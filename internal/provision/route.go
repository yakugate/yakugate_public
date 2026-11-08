package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/netcode"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// A routing rule decides which vendor a message goes to. Without one, an
// account's messages are accepted and then have nowhere to go, which is the
// most common reason a freshly built gateway does nothing. [SCOPE P14-ROUTE]
//
// The patterns are checked with Go's own regexp — the engine's language, not
// JavaScript's (GW-34). The two disagree in both directions, so a pattern that
// looks fine elsewhere can be one the engine silently drops.

// Route is one rule.
type Route struct {
	Name      string
	Direction string // MT | MO
	AccountID int64  // whose rule this is — required: rules for every account are retired (Saleem 2026-09-30)
	VendorID  int64  // MO: the vendor they arrive from, 0 = any (PO 2026-09-24). MT: set from Plan
	Priority  int    // lower is tried first
	Type      string // MT: set to direct (the plan says how); MO: direct

	// Plan is an MT rule's "Where to send": Main and backups
	// (internal/sendplan, config/0131). Retry nil = the gateway's default.
	Plan  *sendplan.Plan
	Retry *sendplan.Retry
	// AcceptUnknown also catches numbers whose network could not be found
	// (accept_undetected_network). It was always written as 1 before
	// 2026-09-30 — looser than the portal, whose default is off.
	AcceptUnknown bool

	// What it matches. Empty means "anything". Each is one field's Ozeki
	// text (ParseCondition): a pattern, or /pattern/replacement/ to change
	// the field too. FieldToText writes them from condition and change rows.
	ToPattern   string // MO: the short code. MT: a change to the receiver only — a destination is chosen instead (2026-09-30)
	FromPattern string // the sender id (MO: the subscriber's number)
	Keyword     string // MT: the text; MO: the first word of the message
	// PriorityPattern is an MT rule's priority condition, e.g. ^(1|2)$ (a
	// message sent without one counts as 9). Never a change.
	PriorityPattern string
	// Networks is the destination as routes.match_mccmncs: none = any
	// destination, ["410"] a country, ["410004"] one network
	// (Destination.Networks). One destination per rule since 2026-09-30.
	Networks []string

	// ID is the rule's id (read by GetRoute; CreateRoute ignores it).
	ID int64
	// Disabled switches the rule off (is_disabled). A new rule is on.
	Disabled bool
	// The rule's own cards; 0 = the default applies (billing.RateFull): the
	// account's card for what the customer pays, each vendor's for what it
	// costs. The receipt pair is for MT rules only; an MT rule with Main and
	// backups prices each vendor's buy on the vendor (sendplan.Vendor), so
	// BuyCard is read by old rules only.
	SellCard, BuyCard, DLRSellCard, DLRBuyCard int64
}

// Destination is the rule's one destination; ok=false for an old rule that
// names several.
func (r Route) Destination() (Destination, bool) { return DestinationOf(r.Networks) }

// Directions and RouteTypes are the real vocabularies. DLR rules were never
// read by the engine and are no longer offered.
func Directions() []string { return []string{"MT", "MO"} }
func RouteTypes() []string {
	return []string{"direct", "failover", "loadbalance", "lcr", "capacity", "quality"}
}

// RouteTypeWhat explains each kind in one line, so a menu can teach as it asks.
func RouteTypeWhat(t string) string {
	switch strings.ToLower(t) {
	case "direct":
		return "always this vendor"
	case "failover":
		return "this vendor, and the next rule when it is down"
	case "loadbalance":
		return "shared across matching vendors by weight"
	case "lcr":
		return "whichever matching vendor is cheapest"
	case "capacity":
		return "whichever matching vendor has room"
	case "quality":
		return "whichever matching vendor is delivering best on that network"
	}
	return ""
}

// ValidateRoute applies everything that does not need the store, for a NEW
// rule: an outgoing rule has one destination and no receiver condition
// (Saleem 2026-09-30).
func ValidateRoute(r Route) error { return validateRoute(r, nil) }

// validateRoute is ValidateRoute for a rule being changed: a shape no longer
// made (several destinations, a receiver condition) is kept when it is left
// exactly as it was, so an old rule can still be edited in other ways.
func validateRoute(r Route, old *Route) error {
	if strings.TrimSpace(r.Name) == "" {
		return refusef("a rule needs a name, so it can be recognised in a list")
	}
	if n := len([]rune(strings.TrimSpace(r.Name))); n > 128 {
		return refusef("a rule's name is at most 128 characters (this one has %d)", n)
	}
	if !oneOf(r.Direction, Directions()) {
		return refusef("direction %q is not one of %s", r.Direction, strings.Join(Directions(), ", "))
	}
	if !oneOf(r.Type, RouteTypes()) {
		return refusef("%q is not a kind of rule: %s", r.Type, strings.Join(RouteTypes(), ", "))
	}
	if r.AccountID <= 0 {
		return refusef("a rule must name its account: every account has its own rules, and a rule for every account is no longer read")
	}
	if strings.EqualFold(r.Direction, "MT") {
		if r.Plan == nil {
			return refusef("an outgoing rule must say where to send: at least a Main level with one vendor, or vendors for one network")
		}
		// The way of choosing is the rule's, not a level's (Saleem
		// 2026-09-30): the doors write only that form. A level repeating the
		// rule's way is fine; one naming another is refused by Validate.
		if r.Plan.Method == "" {
			return refusef("an outgoing rule must say how it chooses among its vendors, once for the whole rule: %s", strings.Join(sendplan.Methods, ", "))
		}
		if err := r.Plan.Validate(); err != nil {
			return refusef("%v", err)
		}
	} else if r.Plan != nil {
		return refusef("an incoming rule has no vendors to send through; only outgoing rules have Main and backups")
	}
	if r.Retry != nil {
		// Retries are rounds of Main and backups: an incoming rule has none
		// (the Admin API refuses them too, resources.js applyRetry). One
		// stored before is kept while it is left as it was.
		if strings.EqualFold(r.Direction, "MO") && (old == nil || old.Retry == nil || *old.Retry != *r.Retry) {
			return refusef("retries are for outgoing rules: an incoming rule delivers what arrives and has no rounds")
		}
		if err := r.Retry.Validate(); err != nil {
			return refusef("%v", err)
		}
	}
	if r.Priority < 0 {
		return refusef("priority cannot be negative: lower numbers are tried first, so 1 is the most urgent")
	}
	// Every condition, in the ENGINE's regex language. A pattern JavaScript
	// accepts and Go does not was silently dropped before GW-34, and a rule
	// with a dropped filter matches MORE traffic than it was meant to.
	//
	// Parsed FIRST (ParseCondition): compiling "/^ACME$/ACMEPK/" whole would
	// succeed and store a pattern that means something else entirely. The
	// part that is checked is the part that is stored.
	// The receiver field is an incoming rule's short code; on an outgoing
	// rule it is a change to the receiver (or an old receiver condition).
	receiver := "receiver"
	if strings.EqualFold(r.Direction, "MO") {
		receiver = "short code"
	}
	for _, p := range []struct{ what, raw string }{
		{receiver, r.ToPattern}, {"sender", r.FromPattern}, {"keyword", r.Keyword}, {"priority", r.PriorityPattern},
	} {
		match, replace, err := ParseCondition(p.raw)
		if err != nil {
			return refusef("the %s condition: %v", p.what, err)
		}
		if match == "" {
			continue
		}
		if _, err := regexp.Compile(match); err != nil {
			return refusef("the %s pattern cannot be read by this gateway: %v. It uses Go's regular expressions, which have no look-behind or back-references",
				p.what, err)
		}
		// An MO rule with a replacement changes the incoming message it wins
		// (4 Nov 2026, internal/mo Service.rewrite), as an MT rule changes
		// what it sends; until then the engine only matched on MO and this
		// refused it. The Admin API has always saved it.
		if p.what == "priority" {
			// The engine judges priority on outgoing messages only, and
			// never rewrites it (stage_route applyRoute).
			if strings.EqualFold(r.Direction, "MO") {
				return refusef("a priority condition is for outgoing rules: an incoming message has no priority")
			}
			if replace != "" {
				return refusef("the priority cannot be changed: a priority condition only decides which messages the rule catches")
			}
		}
	}
	if strings.EqualFold(r.Direction, "MT") {
		if err := assertNewShape(r, old); err != nil {
			return err
		}
	}
	for _, n := range r.Networks {
		// 3 digits is a whole country (an MCC, like 410), which the engine
		// has always read; 4 to 6 is a country and network (41001).
		if !regexp.MustCompile(`^[0-9]{3,6}$`).MatchString(strings.TrimSpace(n)) {
			return refusef("%q is not a network code: a country code (410) or a country and network code together (41001)", n)
		}
	}
	if r.Plan != nil {
		for code := range r.Plan.Exceptions {
			if !networksCover(r.Networks, code) {
				return refusef("the exception for network %s is for a network this rule does not catch", code)
			}
		}
		// A network-only rule carries only the networks that have vendors: a
		// network ticked without any would be caught by nothing, silently.
		if r.Plan.NetworksOnly() {
			for _, n := range r.Networks {
				n = strings.TrimSpace(n)
				if len(n) < 4 {
					continue // a whole country: the rule carries the networks of it that have vendors
				}
				covered := false
				for code := range r.Plan.Exceptions {
					covered = covered || networksCover([]string{code}, n)
				}
				if !covered {
					return refusef("network %s is ticked but has no vendors: this rule sends only for the networks that have their own vendors", n)
				}
			}
		}
	}
	return nil
}

// assertNewShape refuses, on an outgoing rule, what is no longer made
// (Saleem 2026-09-30; rule-creation-ux.md "Must change together"): more than
// one destination — a rule per network instead, which Copy makes quickly —
// and a condition on the receiver, which the destination replaces. A
// network-only rule keeps its networks (each has its own vendors). On an
// edit, either is kept when it is exactly as stored.
func assertNewShape(r Route, old *Route) error {
	nets := cleanCodes(r.Networks)
	if len(nets) > 1 && !r.Plan.NetworksOnly() && (old == nil || !sameCodes(nets, cleanCodes(old.Networks))) {
		return refusef("a rule sends to one destination: any destination, one country or one network. Make one rule for each (copy the rule to the others), so each can be seen, tested and changed on its own")
	}
	if c, _, err := TextToField(FieldReceiver, r.ToPattern); err == nil && c != nil &&
		(old == nil || strings.TrimSpace(old.ToPattern) != strings.TrimSpace(r.ToPattern)) {
		return refusef("the receiver is not a condition any more: choose a destination (a country or a network) instead. A change to the receiver (add at start, remove from start) is still fine")
	}
	// "Also numbers whose network is not found" is for a whole country only:
	// a network rule takes the numbers placed on that network, and one whose
	// network is not found goes to the country's rule or 'Everything else'.
	if d, ok := DestinationOf(nets); ok && d.Kind == DestNetwork && r.AcceptUnknown && !r.Plan.NetworksOnly() &&
		(old == nil || !old.AcceptUnknown || !sameCodes(nets, cleanCodes(old.Networks))) {
		return refusef("a network rule cannot also take numbers whose network is not found: they go to the country's rule or 'Everything else'. Choose the whole country for that")
	}
	return nil
}

func sameCodes(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]string(nil), a...)
	y := append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// networksCover reports whether a rule's networks catch one network code (no
// networks = every network). The engine's own comparison (netcode).
func networksCover(nets []string, code string) bool {
	if len(nets) == 0 {
		return true
	}
	if len(code) < 4 {
		return false
	}
	for _, n := range nets {
		if netcode.Matches(strings.TrimSpace(n), code[:3], code[3:]) {
			return true
		}
	}
	return false
}

// MOConds is what an incoming rule matches, for the short code clash check:
// the stored match patterns (no Ozeki slashes), the networks as
// routes.match_mccmncs, and the vendor (0 = any).
type MOConds struct {
	Receiver, Keyword string
	Networks          []string
	VendorID          int64
}

// MOClash reports whether two incoming rules of two accounts could take one
// message, which one account may not do to another: "a shortcode, with the
// same keyword and network, may belong to only one account" (PO 2026-09-14,
// D-109). The Admin API's moOverlap (services/routeShadow.js) word for word —
// change together; both read sqa/e2e/shared/mo-clash-vectors.json. They clash
// only when every condition could take one message:
//   - the vendor (PO 2026-09-24): two rules that each name a DIFFERENT
//     vendor never meet — the engine only offers a rule the messages its own
//     vendor delivers (internal/mo.Service.match). A rule naming no vendor
//     takes every vendor and meets them all.
//   - the keyword: the SAME keyword pattern, or both none. A rule with a
//     keyword beside another account's rule with none is how a shared code
//     is split (D-032: "SUB" to one account, other text to the next): the
//     engine tries the keyword rule first (mo.Service.match), so the rule
//     with none never takes its messages. Both doors refused that pair until
//     the full go-live retest of 19 Oct 2026 (P6 B7), and this door did not
//     read the keyword or the network at all.
//   - the network: either names none, or one network both could name.
//   - the short code (receiversOverlap).
func MOClash(a, b MOConds) bool {
	if a.VendorID > 0 && b.VendorID > 0 && a.VendorID != b.VendorID {
		return false // different vendors: different messages
	}
	if strings.TrimSpace(a.Keyword) != strings.TrimSpace(b.Keyword) {
		return false // another keyword, or a keyword beside none: another service
	}
	if !networksOverlap(cleanCodes(a.Networks), cleanCodes(b.Networks)) {
		return false
	}
	return receiversOverlap(strings.TrimSpace(a.Receiver), strings.TrimSpace(b.Receiver))
}

// shortcodeFree keeps one short code to one account (MOClash). Two accounts
// owning the same code means one silently takes the other's incoming
// messages, and which one depends on the order rules happen to be read in.
func (s Store) shortcodeFree(ctx context.Context, r Route, ignoreID int64) error {
	// The rule's short code and keyword as stored: the match of their Ozeki text.
	myPat, _, _ := ParseCondition(r.ToPattern)
	myPat = strings.TrimSpace(myPat)
	myKw, _, _ := ParseCondition(r.Keyword)
	mine := MOConds{Receiver: myPat, Keyword: myKw, Networks: r.Networks, VendorID: r.VendorID}
	return s.DB.EachOtherMORule(ctx, r.AccountID, ignoreID, func(o dal.MORuleRow) error {
		id, acct, vendor := o.ID, o.AccountID, o.VendorID
		name, pat, kw, nets := o.Name, o.Receiver, o.Keyword, o.Nets
		theirs := strings.TrimSpace(patternOf(pat))
		if !MOClash(mine, MOConds{Receiver: theirs, Keyword: patternOf(kw), Networks: storedNetworks(nets), VendorID: vendor}) {
			return nil
		}
		who, _ := s.DB.RuleAccountName(ctx, acct)
		switch {
		case theirs == "":
			// A rule with no short code owns EVERY code, so nothing else can
			// be added while it exists. Say which rule, or the refusal is a
			// mystery.
			return refusef("account %q already takes every short code (rule #%d %q has no short code pattern), so no other account can be given one with the same keyword until that rule names its codes",
				orUnknown(who), id, name)
		case myPat == "":
			// And the other way round: a rule with no short code would take
			// every code, the other account's included.
			return refusef("with no short code this rule would take every short code, and account %q already has %s with the same keyword (rule #%d %q). Give this rule its own short code",
				orUnknown(who), codeWords(theirs), id, name)
		}
		code := literalCode(myPat)
		if code == "" {
			code = literalCode(theirs)
		}
		if code == "" {
			code = myPat
		}
		return refusef("short code %s already belongs to account %q (rule #%d %q). One code with the same keyword, network and vendor can belong to one account only, or whichever rule is read first quietly takes the other's messages — to share it, give each account its own keyword (a rule with a keyword is tried before another account's rule with none), or pin each rule to the vendor that delivers it",
			code, orUnknown(who), id, name)
	})
}

// storedNetworks reads routes.match_mccmncs as stored (a JSON list; NULL or
// empty = any network).
func storedNetworks(raw string) []string {
	var n []any
	if strings.TrimSpace(raw) == "" || json.Unmarshal([]byte(raw), &n) != nil {
		return nil
	}
	out := make([]string, 0, len(n))
	for _, v := range n {
		if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// networksOverlap reports whether two rules' networks could both take one
// message: no networks = every network; else one entry of each that could
// name the same network (an MCC alone covers its country; MNCs compare as
// numbers). The Admin API's networksOverlap / entriesOverlap.
func networksOverlap(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return true
	}
	for _, x := range a {
		xm, xn := netcode.Split(x)
		for _, y := range b {
			ym, yn := netcode.Split(y)
			if xm == "" || xm != ym {
				continue
			}
			if xn == "" || yn == "" || netcode.SameMNC(xn, yn) {
				return true
			}
		}
	}
	return false
}

// receiversOverlap reports whether two MO short code patterns could take
// one message — the Admin API's rule, word for word (services/routeShadow.js
// receiversOverlap): no pattern takes every code; the same pattern twice
// overlaps; a plain code overlaps any pattern that matches it.
func receiversOverlap(a, b string) bool {
	if a == "" || b == "" || a == b {
		return true
	}
	if al := literalCode(a); al != "" && matches(b, al) {
		return true
	}
	if bl := literalCode(b); bl != "" && matches(a, bl) {
		return true
	}
	return false
}

// codeWords names a short code pattern for a message: "short code 8005",
// or "the short codes ^80(05|06)$".
func codeWords(p string) string {
	if c := literalCode(p); c != "" {
		return "short code " + c
	}
	return "the short codes " + p
}

// RouteRow is one line of the rule list.
type RouteRow struct {
	ID        int64
	Name      string
	Direction string
	Priority  int
	Account   string
	Vendor    string
	Matches   string
	Disabled  bool
	Trouble   string
}

// ListRoutes shows the rules in the order they are tried, and says when one
// points at a vendor that cannot carry anything — a rule to a broken vendor
// looks healthy and sends every matching message into a queue nobody drains.
func (s Store) ListRoutes(ctx context.Context, limit int) ([]RouteRow, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.DB.RuleList(ctx, limit)
	if err != nil {
		if re, ok := err.(dal.RowsError); ok {
			return nil, re.Err
		}
		return nil, fmt.Errorf("list rules: %w", err)
	}
	var out []RouteRow
	var cids map[int64]string
	for _, row := range rows {
		r := RouteRow{ID: row.ID, Name: row.Name, Direction: row.Direction, Priority: row.Priority, Account: row.Account, Vendor: row.Vendor}
		to, from, kw, transport, adapter, status, planJSON := row.Receiver, row.Sender, row.Keyword, row.Transport, row.AdapterConfig, row.Status, row.Plan
		dis, global := row.Disabled, row.Global
		if plan, err := sendplan.Parse([]byte(planJSON)); err == nil && plan != nil {
			if cids == nil {
				cids = s.vendorCIDs(ctx)
			}
			r.Vendor = planSummary(plan, cids)
		}
		r.Disabled = dis != 0
		if r.Direction == "MO" && transport == "" && r.Vendor == "—" {
			r.Vendor = "any vendor" // an MO rule's vendor is optional (PO 2026-09-24)
		}
		r.Matches = describeMatch(patternOf(to), patternOf(from), patternOf(kw))
		if transport != "" {
			if t := s.vendorTrouble(ctx, transport, adapter); t != "" {
				r.Trouble = "its vendor will not load: " + t
			} else if status != "active" {
				r.Trouble = "its vendor is " + status
			}
		}
		if global != 0 {
			r.Trouble = "a rule for every account: retired, no longer used (give each account its own rules)"
		}
		out = append(out, r)
	}
	return out, nil
}

// vendorCIDs maps vendor ids to names for the rule list.
func (s Store) vendorCIDs(ctx context.Context) map[int64]string {
	out, _ := s.DB.RuleVendorCIDs(ctx)
	return out
}

// SetRouteDisabled switches a rule off or on.
func (s Store) SetRouteDisabled(ctx context.Context, id int64, disabled bool) error {
	n, err := s.DB.SetRuleDisabled(ctx, id, boolInt(disabled))
	if err != nil {
		return fmt.Errorf("update rule %d: %w", id, err)
	}
	if n == 0 {
		return refusef("there is no rule #%d", id)
	}
	return nil
}

// DeleteRoute removes a rule.
func (s Store) DeleteRoute(ctx context.Context, id int64) error {
	n, err := s.DB.DeleteRuleRow(ctx, id)
	if err != nil {
		return fmt.Errorf("delete rule %d: %w", id, err)
	}
	if n == 0 {
		return refusef("there is no rule #%d", id)
	}
	return nil
}

// ── helpers ────────────────────────────────────────────────────────────────

// patternJSON stores a condition the way the engine reads it: {"match",
// "replace"}. The raw text is parsed Ozeki-style first (ParseCondition), so
// "/^0(\d+)/92$1/" becomes a match and a rewrite rather than one odd pattern.
// Validation has already refused anything that does not parse.
func patternJSON(raw string) any {
	match, replace, err := ParseCondition(raw)
	if err != nil || (match == "" && replace == "") {
		return nil
	}
	doc := map[string]string{"match": match}
	if replace != "" {
		doc["replace"] = replace
	}
	b, _ := json.Marshal(doc)
	return string(b)
}

func networksJSON(n []string) any {
	if len(n) == 0 {
		return nil
	}
	clean := make([]string, 0, len(n))
	for _, v := range n {
		if v = strings.TrimSpace(v); v != "" {
			clean = append(clean, v)
		}
	}
	if len(clean) == 0 {
		return nil
	}
	b, _ := json.Marshal(clean)
	return string(b)
}

// patternOf pulls the pattern back out of a stored {"match": "..."}.
func patternOf(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var m struct {
		Match string `json:"match"`
	}
	if json.Unmarshal([]byte(raw), &m) == nil {
		return m.Match
	}
	return ""
}

// literalCode reads a plain short code out of a pattern like ^8005$, so a
// clash can be described in the operator's own terms.
func literalCode(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "^")
	p = strings.TrimSuffix(p, "$")
	if p != "" && regexp.MustCompile(`^[0-9]+$`).MatchString(p) {
		return p
	}
	return ""
}

func matches(pattern, s string) bool {
	re, err := regexp.Compile(pattern)
	return err == nil && re.MatchString(s)
}

func describeMatch(to, from, kw string) string {
	var parts []string
	if to != "" {
		parts = append(parts, "to "+to)
	}
	if from != "" {
		parts = append(parts, "from "+from)
	}
	if kw != "" {
		parts = append(parts, "keyword "+kw)
	}
	if len(parts) == 0 {
		return "anything"
	}
	return strings.Join(parts, ", ")
}

func orUnknown(s string) string {
	if strings.TrimSpace(s) == "" {
		return "another account"
	}
	return s
}

// ── rate cards on a rule ──────────────────────────────────────────────────

// RouteCardSlot is one of the four cards a rule may name. A rule's own card
// always beats the account's or vendor's default (billing.RateFull): it is
// how a per-route deal is priced.
type RouteCardSlot struct {
	Key    string // buy | sell | dlr_buy | dlr_sell
	Column string
	Kind   string // the card side the slot takes
	Label  string
}

// RouteCardSlots lists the slots a rule of this direction has. The receipt
// pair exists on outgoing rules only: a receipt is priced by the MT rule of
// the message it reports on (GW-38).
func RouteCardSlots(direction string) []RouteCardSlot {
	slots := []RouteCardSlot{
		{"sell", "selling_rate_card_id", "sell", "what the customer pays for these messages"},
		{"buy", "buying_rate_card_id", "buy", "what the vendor charges us for them"},
	}
	if strings.EqualFold(direction, "MT") {
		slots = append(slots,
			RouteCardSlot{"dlr_sell", "dlr_selling_rate_card_id", "sell", "what the customer pays for their delivery receipts"},
			RouteCardSlot{"dlr_buy", "dlr_buying_rate_card_id", "buy", "what the vendor charges us for the receipts"})
	}
	return slots
}

// RouteCards is what a rule prices by now; 0 = the default applies.
type RouteCards struct {
	Direction string
	Cards     map[string]int64  // slot key -> card id
	Names     map[string]string // slot key -> card name
}

// GetRouteCards reads a rule's four card columns.
func (s Store) GetRouteCards(ctx context.Context, routeID int64) (RouteCards, error) {
	out := RouteCards{Cards: map[string]int64{}, Names: map[string]string{}}
	row, err := s.DB.RuleCards(ctx, routeID)
	if err == dal.ErrNoRows {
		return out, refusef("there is no rule #%d", routeID)
	}
	if err != nil {
		return out, fmt.Errorf("read rule %d: %w", routeID, err)
	}
	out.Direction = row.Direction
	sell, buy, dsell, dbuy := row.Sell, row.Buy, row.DLRSell, row.DLRBuy
	for k, v := range map[string]int64{"sell": sell, "buy": buy, "dlr_sell": dsell, "dlr_buy": dbuy} {
		out.Cards[k] = v
		if v != 0 {
			if name, err := s.DB.RuleCardName(ctx, v); err == nil {
				out.Names[k] = name
			} else {
				out.Names[k] = fmt.Sprintf("#%d — MISSING, prices nothing", v)
			}
		}
	}
	return out, nil
}

// SetRouteCard puts a card in one of a rule's slots, or clears it (cardID 0).
//
// STRICTER THAN THE PORTAL, deliberately: the Admin API checks card side and
// traffic on a vendor's defaults but not on a rule. Here a rule's sell slot
// takes a sell card and its buy slot a buy card, each for the rule's own
// traffic (receipt slots: DLR). A buy card in a sell slot charges the
// customer our cost; an MO card in an MT rule prices outgoing messages from a
// list nobody meant for them — both look perfectly normal on every screen.
func (s Store) SetRouteCard(ctx context.Context, routeID int64, slot string, cardID int64) error {
	direction, plan, accountID, err := s.DB.RuleShape(ctx, routeID)
	if err != nil {
		return refusef("there is no rule #%d", routeID)
	}
	// A rule with Main and backups prices each vendor's buy on the vendor:
	// the Admin API stores buying_rate_card_id NULL on such a rule
	// (resources.js applySendPlan), so a card put here would be dropped by
	// the portal's next save of it — and the price with it.
	if slot == "buy" && cardID != 0 && strings.EqualFold(direction, "MT") && strings.TrimSpace(plan) != "" && plan != "null" {
		return refusef("a rule with Main and backups has no buy card of its own: give each vendor its own buy price for this rule (Where to send → Prices for this rule, per vendor)")
	}
	var want *RouteCardSlot
	for _, sl := range RouteCardSlots(direction) {
		if sl.Key == slot {
			sl := sl
			want = &sl
		}
	}
	if want == nil {
		if strings.HasPrefix(slot, "dlr_") {
			return refusef("only an outgoing (MT) rule prices receipts: a receipt is charged by the rule of the message it reports on")
		}
		return refusef("%q is not a card slot on a rule", slot)
	}

	var value int64
	if cardID != 0 {
		kind, typ, err := s.DB.RuleCardKind(ctx, cardID)
		if err != nil {
			return refusef("there is no rate card #%d", cardID)
		}
		if kind != want.Kind {
			return refusef("card #%d is a %s card; this slot is %s, so it takes a %s card", cardID, kind, want.Label, want.Kind)
		}
		traffic := strings.ToUpper(direction)
		if strings.HasPrefix(slot, "dlr_") {
			traffic = "DLR"
		}
		if typ != "" && !strings.EqualFold(typ, traffic) {
			return refusef("card #%d prices %s traffic, and this slot prices %s", cardID, typ, traffic)
		}
		if want.Kind == "sell" {
			if err := s.assertSellCardCompany(ctx, cardID, accountID); err != nil {
				return err
			}
		}
		value = cardID
	}
	if err := s.DB.SetRuleCard(ctx, want.Column, value, routeID); err != nil {
		return fmt.Errorf("set the card on rule %d: %w", routeID, err)
	}
	return nil
}

// assertRouteDeployments refuses a rule whose account may submit on a gateway
// its vendor does not run on.
//
// That gateway would accept a message it has no route out of: the submit
// succeeds, the message lands in the vendor's queue, and it waits for a
// vendor role nothing there is running. Under one shared store another
// deployment usually drains it, so nothing is lost today — but the capacity
// planned per gateway is a fiction, and the day the stores are split it IS
// lost.
//
// Read as the set relation it is, only ONE empty case passes: an account
// naming no deployment has the empty set, a subset of anything. A VENDOR
// naming none IS the empty set, and {lhr-1} is not a subset of {} — an
// unconfigured vendor is not one that runs everywhere, it is one nobody has
// placed yet, and a placed account must not be routed into it (PO
// 2026-09-24). That keeps it safe in the direction that matters: a rule whose
// ACCOUNT has never been narrowed still saves, which is every rule written
// before any of this existed.
//
// Stricter than the engine's CLAIM filter on purpose. At design time we
// insist, because a choice is being made and we can ask for it; at run time
// an unassigned vendor is still claimed, because refusing there would stop
// draining a queue that already has traffic in it.
//
// VendorFits says whether a rule of this direction may pair the account with
// the vendor, and why not. The console uses it to leave out of a vendor list
// what the save would refuse, as the portal's picker does. A blackhole vendor
// is judged like any other (2026-10-02).
func (s Store) VendorFits(ctx context.Context, accountID, vendorID int64, direction string) error {
	return s.assertRouteDeployments(ctx, accountID, vendorID, direction)
}

func (s Store) assertRouteDeployments(ctx context.Context, accountID, vendorID int64, direction string) error {
	if accountID <= 0 || vendorID <= 0 {
		return nil // "any account", or no vendor: nothing to compare
	}
	acctJSON, err := s.DB.RuleAccountDeployments(ctx, accountID)
	if err != nil {
		return nil // cannot tell; do not block on our own uncertainty
	}
	vendJSON, err := s.DB.RuleVendorDeployments(ctx, vendorID)
	if err != nil {
		return nil
	}
	// A blackhole is placed and chosen like any other vendor (Saleem
	// 2026-10-02; until then it counted as placed everywhere, and a rule could
	// name one no running blackhole role would drain).
	acct, _ := deployment.Parse(acctJSON)
	vend, _ := deployment.Parse(vendJSON)
	if len(acct) == 0 {
		return nil // the empty set is a subset of anything
	}
	mo := strings.EqualFold(direction, "MO")
	if len(vend) == 0 {
		if mo {
			return refusef("the vendor has no deployment set, and the account is served on %s: a vendor nobody has placed cannot be paired with it. Give the vendor the deployments it runs on",
				strings.Join(idsOf(acct), ", "))
		}
		return refusef("the vendor has no deployment set, and the account is served on %s: a vendor nobody has placed cannot carry it. Give the vendor the deployments it runs on",
			strings.Join(idsOf(acct), ", "))
	}
	has := make(map[string]bool, len(vend))
	for _, e := range vend {
		has[e.ID] = true
	}
	var missing []string
	for _, e := range acct {
		if !has[e.ID] {
			missing = append(missing, e.ID)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if mo {
		return refusef("the account is served on %s and the vendor does not run there. Add %s to the vendor, or remove it from the account",
			strings.Join(missing, ", "), strings.Join(missing, ", "))
	}
	return refusef("the account is served on %s and the vendor does not run there: a submit on %s would be accepted with no vendor to carry it. Add %s to the vendor, or remove it from the account",
		strings.Join(missing, ", "), missing[0], strings.Join(missing, ", "))
}

// idsOf is the deployment ids of a list, for a message.
func idsOf(l deployment.List) []string {
	out := make([]string, 0, len(l))
	for _, e := range l {
		out = append(out, e.ID)
	}
	return out
}

// assertPlan applies the checks an MT rule's plan needs from the store — the
// same the Admin API applies (services/sendPlan.js, resources.js routes):
// every vendor exists and runs everywhere the account is served; every card a
// vendor carries for this rule exists and fits (a buy card where buy, a sell
// card where sell, for outgoing traffic); and the rule's priority is its own
// within the account's outgoing rules, since the first rule that catches a
// message decides.
func (s Store) assertPlan(ctx context.Context, r Route, ignoreID int64) error {
	for _, vid := range r.Plan.VendorIDs() {
		ok, err := s.DB.VendorExists(ctx, vid)
		if err != nil {
			return err
		}
		if !ok {
			return refusef("there is no vendor #%d", vid)
		}
		if err := s.assertRouteDeployments(ctx, r.AccountID, vid, "MT"); err != nil {
			return err
		}
	}
	check := func(cardID int64, kind string) error {
		if cardID == 0 {
			return nil
		}
		k, typ, err := s.DB.RuleCardKind(ctx, cardID)
		if err != nil {
			return refusef("there is no rate card #%d", cardID)
		}
		if k != kind {
			return refusef("card #%d is a %s card; a vendor's %s price for this rule takes a %s card", cardID, k, kind, kind)
		}
		if typ != "" && !strings.EqualFold(typ, "MT") {
			return refusef("card #%d prices %s traffic, and an outgoing rule prices MT", cardID, typ)
		}
		return nil
	}
	levels := [][]sendplan.Level{r.Plan.Levels}
	for _, lv := range r.Plan.Exceptions {
		levels = append(levels, lv)
	}
	for _, list := range levels {
		for _, lv := range list {
			for _, v := range lv.Vendors {
				if err := check(v.BuyRateCardID, "buy"); err != nil {
					return err
				}
				if err := check(v.SellRateCardID, "sell"); err != nil {
					return err
				}
				if err := s.assertSellCardCompany(ctx, v.SellRateCardID, r.AccountID); err != nil {
					return err
				}
			}
		}
	}
	// Priority 0 = placed for the operator (PlaceRules): nothing to clash.
	if r.Priority <= 0 {
		return nil
	}
	clash, name, err := s.DB.RulePriorityTaken(ctx, "MT", r.AccountID, r.Priority, ignoreID)
	if err == nil {
		return refusef("priority %d is already rule #%d %q of this account: the first rule that catches a message decides, so each rule needs its own priority", r.Priority, clash, name)
	}
	if err != dal.ErrNoRows {
		return err
	}
	return nil
}

// planNames lists a plan's vendors for display: "jazz, zong + 2 backups".
func planSummary(p *sendplan.Plan, cid map[int64]string) string {
	if p.NetworksOnly() {
		return fmt.Sprintf("only its %d network(s), each with its own vendors", len(p.Exceptions))
	}
	if p == nil || len(p.Levels) == 0 {
		return ""
	}
	var main []string
	for _, v := range p.Levels[0].Vendors {
		n := cid[v.VendorID]
		if n == "" {
			n = fmt.Sprintf("#%d", v.VendorID)
		}
		if p.Levels[0].Share {
			n += fmt.Sprintf(" %d%%", v.Pct)
		}
		main = append(main, n)
	}
	out := strings.Join(main, ", ")
	if b := len(p.Levels) - 1; b == 1 {
		out += " + 1 backup"
	} else if b > 1 {
		out += fmt.Sprintf(" + %d backups", b)
	}
	if n := len(p.Exceptions); n > 0 {
		out += fmt.Sprintf(" (%d exception", n)
		if n > 1 {
			out += "s"
		}
		out += ")"
	}
	return out
}
