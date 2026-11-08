// Package sendplan is the "Where to send" part of a routing rule (Saleem,
// 2026-09-30): the rule's way of picking a vendor (ONE per rule — Saleem's
// later ruling the same day: "that should be rule wise not option wise"),
// its Main level and its backups, each with its vendors, an optional share
// by % and per-vendor price overrides; plus exceptions (other levels for one
// network) and the rule's retries.
//
// It is stored on routes.send_plan (config/0131) as JSON and read by the
// engine (refdata), the console (provision) and — mirrored in
// p13-backend/src/services/sendPlan.js, CHANGE TOGETHER — the Admin API.
// Parse is strict: an unknown key or a bad value is refused, so the two doors
// cannot store what the engine would read differently.
package sendplan

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"time"
)

// The ways a rule picks a vendor inside a level.
const (
	Direct   = "direct" // the vendors in the operator's order
	Cheapest = "cheap"  // lowest buy price for the message's network first
	Best     = "best"   // best recent delivery rate to the network first
	Value    = "smart"  // 60% price, 40% delivery
	Least    = "least"  // fewest messages waiting first
)

// Methods lists the ways in the order the screens offer them.
var Methods = []string{Direct, Cheapest, Best, Value, Least}

// ScoreMethod reports whether a way orders vendors by a score. Sharing by %
// on one of these overrides the score (the screens warn about it).
func ScoreMethod(m string) bool { return m == Cheapest || m == Best || m == Value || m == Least }

// ValidMethod reports whether m is one of the ways.
func ValidMethod(m string) bool {
	for _, w := range Methods {
		if m == w {
			return true
		}
	}
	return false
}

// Vendor is one vendor in a level.
type Vendor struct {
	VendorID int64 `json:"vendor_id"`
	// Pct is its share when the level shares by % (0..100; the level's
	// shares add up to 100). Ignored when the level does not share.
	Pct int `json:"pct,omitempty"`
	// BuyRateCardID / SellRateCardID price this vendor for THIS rule only.
	// 0 = the vendor's own default buy card / the rule's sell card.
	BuyRateCardID  int64 `json:"buy_rate_card_id,omitempty"`
	SellRateCardID int64 `json:"sell_rate_card_id,omitempty"`
}

// Level is Main or one backup.
type Level struct {
	// Method is LEGACY ONLY: before the ruling of 2026-09-30 each level had
	// its own way. A plan with Plan.Method set uses that in every level; a
	// level may then repeat it or leave it out, never differ. The doors write
	// it on no level.
	Method  string   `json:"method,omitempty"`
	Share   bool     `json:"share,omitempty"`
	Vendors []Vendor `json:"vendors"`
}

// Plan is a rule's whole "Where to send".
//
// A plan may be NETWORK-ONLY (Saleem 2026-09-30: "only network specific
// routing"): no levels of its own, only exceptions. Such a rule carries only
// the networks it names; a number on any other network is not caught by it,
// and the account's next rule gets it.
type Plan struct {
	// Method is the rule's way of choosing, used by Main, every backup and
	// every exception level. Empty only on a plan stored before 2026-09-30,
	// whose levels each carry their own (read as they always were).
	Method string  `json:"method,omitempty"`
	Levels []Level `json:"levels"`
	// Exceptions gives one network (MCC+MNC, e.g. "41004") its own levels.
	Exceptions map[string][]Level `json:"exceptions,omitempty"`
}

var netRe = regexp.MustCompile(`^[0-9]{5,6}$`)

// Parse reads a stored plan strictly. Empty input (NULL column) = nil plan.
func Parse(raw []byte) (*Plan, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var p Plan
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("send plan: %w", err)
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

// Validate applies the rules the engine reads by. It still takes the old
// per-level form, so rules stored before 2026-09-30 keep sending as they did;
// the doors write only the new one (Unified) and require Method.
func (p *Plan) Validate() error {
	if p == nil {
		return fmt.Errorf("send plan: missing")
	}
	if len(p.Levels) == 0 && len(p.Exceptions) == 0 {
		return fmt.Errorf("send plan: needs at least a Main level, or vendors for one network")
	}
	if p.Method != "" && !ValidMethod(p.Method) {
		return fmt.Errorf("send plan: unknown way of choosing %q (direct, cheap, best, smart, least)", p.Method)
	}
	if len(p.Levels) > 0 {
		if err := validLevels("", p.Method, p.Levels); err != nil {
			return err
		}
	}
	for net, lv := range p.Exceptions {
		if !netRe.MatchString(net) {
			return fmt.Errorf("send plan: exception %q is not a network code (MCC+MNC, 5 or 6 digits)", net)
		}
		if err := validLevels("exception "+net, p.Method, lv); err != nil {
			return err
		}
	}
	return nil
}

// MethodFor is the way a level chooses: the rule's, else (a plan stored
// before 2026-09-30) the level's own.
func (p *Plan) MethodFor(lv Level) string {
	if p != nil && p.Method != "" {
		return p.Method
	}
	return lv.Method
}

// PerLevel reports whether the plan is in the old form: no way on the rule,
// each level with its own.
func (p *Plan) PerLevel() bool { return p != nil && p.Method == "" }

// Unified returns the plan in the form every door writes: the way once on
// the rule and on no level. The rule keeps its own way when it has one;
// else it takes Main's, else (a network-only plan) that of the first level
// of the lowest network code. Notes names each level of MORE than one vendor
// that chose another way, because its order changes; a level of one vendor
// sends the same whatever the way. The Admin API applies the same rule to
// what it is given (services/sendPlan.js) — CHANGE TOGETHER.
func (p *Plan) Unified() (Plan, []string) {
	if p == nil {
		return Plan{}, nil
	}
	out := Plan{Method: p.Method}
	if out.Method == "" {
		out.Method = p.firstLevelMethod()
	}
	var notes []string
	strip := func(where string, levels []Level) []Level {
		if levels == nil {
			return nil
		}
		res := make([]Level, len(levels))
		for i, lv := range levels {
			if lv.Method != "" && lv.Method != out.Method && len(lv.Vendors) > 1 {
				notes = append(notes, fmt.Sprintf("%s chose by %s; the rule now chooses by %s in every level.",
					levelIn(where, i), MethodName(lv.Method), MethodName(out.Method)))
			}
			lv.Method = ""
			lv.Vendors = append([]Vendor(nil), lv.Vendors...)
			res[i] = lv
		}
		return res
	}
	out.Levels = strip("", p.Levels)
	if out.Levels == nil {
		// Stored as [] (not null), as the Admin API stores a network-only
		// plan: the portal reads a plan with no list as an old-style rule.
		out.Levels = []Level{}
	}
	if len(p.Exceptions) > 0 {
		out.Exceptions = make(map[string][]Level, len(p.Exceptions))
		for _, code := range sortedCodes(p.Exceptions) {
			out.Exceptions[code] = strip("exception "+code, p.Exceptions[code])
		}
	}
	return out, notes
}

// firstLevelMethod is Main's way, else that of the first level of the lowest
// network code ("" when none has one).
func (p *Plan) firstLevelMethod() string {
	if len(p.Levels) > 0 {
		return p.Levels[0].Method
	}
	for _, code := range sortedCodes(p.Exceptions) {
		if lv := p.Exceptions[code]; len(lv) > 0 && lv[0].Method != "" {
			return lv[0].Method
		}
	}
	return ""
}

func sortedCodes(m map[string][]Level) []string {
	codes := make([]string, 0, len(m))
	for c := range m {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	return codes
}

// NetworksOnly reports whether the plan sends only for the networks it names.
func (p *Plan) NetworksOnly() bool { return p != nil && len(p.Levels) == 0 && len(p.Exceptions) > 0 }

// FirstVendor is the vendor older readers of routes.vendor_id see: Main's
// first, or — on a network-only plan — the first vendor of the lowest network
// code, so the answer never depends on map order. 0 when there is none.
func (p *Plan) FirstVendor() int64 {
	if p == nil {
		return 0
	}
	if len(p.Levels) > 0 && len(p.Levels[0].Vendors) > 0 {
		return p.Levels[0].Vendors[0].VendorID
	}
	for _, c := range sortedCodes(p.Exceptions) {
		if lv := p.Exceptions[c]; len(lv) > 0 && len(lv[0].Vendors) > 0 {
			return lv[0].Vendors[0].VendorID
		}
	}
	return 0
}

// LevelName is how screens and logs name a level: Main, Backup 1, Backup 2…
func LevelName(i int) string {
	if i == 0 {
		return "Main"
	}
	return fmt.Sprintf("Backup %d", i)
}

// levelIn names level i of a list: "Backup 1" in the rule's own list,
// "exception 41004 Backup 1" in an exception's.
func levelIn(where string, i int) string {
	if where == "" {
		return LevelName(i)
	}
	return where + " " + LevelName(i)
}

// validLevels checks one list of levels. planMethod is the rule's way: when
// set, a level may repeat it or leave it out, never name another; when empty
// (a plan stored before 2026-09-30) every level needs a way of its own.
func validLevels(where, planMethod string, levels []Level) error {
	if len(levels) == 0 {
		if where == "" {
			return fmt.Errorf("send plan: needs at least a Main level")
		}
		return fmt.Errorf("send plan (%s): needs at least a Main level", where)
	}
	seen := map[int64]string{}
	for i, lv := range levels {
		name := levelIn(where, i)
		switch {
		case planMethod != "":
			if lv.Method != "" && lv.Method != planMethod {
				return fmt.Errorf("send plan (%s): chooses by %q but the rule chooses by %q — the way is set once, for the whole rule", name, lv.Method, planMethod)
			}
		case lv.Method == "":
			return fmt.Errorf("send plan: say how the rule chooses a vendor (method: direct, cheap, best, smart or least)")
		case !ValidMethod(lv.Method):
			return fmt.Errorf("send plan (%s): unknown way of choosing %q (direct, cheap, best, smart, least)", name, lv.Method)
		}
		if len(lv.Vendors) == 0 {
			return fmt.Errorf("send plan (%s): add at least one vendor", name)
		}
		total := 0
		for _, v := range lv.Vendors {
			if v.VendorID <= 0 {
				return fmt.Errorf("send plan (%s): a vendor has no id", name)
			}
			if prev, dup := seen[v.VendorID]; dup {
				return fmt.Errorf("send plan (%s): vendor %d is already in %s — a vendor can be in one level only", name, v.VendorID, prev)
			}
			seen[v.VendorID] = name
			if v.Pct < 0 || v.Pct > 100 {
				return fmt.Errorf("send plan (%s): vendor %d share %d%% is outside 0..100", name, v.VendorID, v.Pct)
			}
			if v.BuyRateCardID < 0 || v.SellRateCardID < 0 {
				return fmt.Errorf("send plan (%s): vendor %d has a bad rate card id", name, v.VendorID)
			}
			total += v.Pct
		}
		if lv.Share && total != 100 {
			return fmt.Errorf("send plan (%s): shares add up to %d%%; they must add up to 100%%", name, total)
		}
	}
	return nil
}

// VendorIDs lists every vendor the plan names (Main, backups, exceptions).
func (p *Plan) VendorIDs() []int64 {
	if p == nil {
		return nil
	}
	var out []int64
	seen := map[int64]bool{}
	add := func(levels []Level) {
		for _, lv := range levels {
			for _, v := range lv.Vendors {
				if !seen[v.VendorID] {
					seen[v.VendorID] = true
					out = append(out, v.VendorID)
				}
			}
		}
	}
	add(p.Levels)
	for _, lv := range p.Exceptions {
		add(lv)
	}
	return out
}

// FromLegacy is the plan an old rule (routes.send_plan NULL) behaves as: one
// Main level with its one vendor, the rule choosing the way its route_type
// said. loadbalance becomes a shared Direct level; its weight is kept as the
// share so a tier of several such rules still splits the same way.
func FromLegacy(routeType string, vendorID int64, weight int) Plan {
	lv := Level{Vendors: []Vendor{{VendorID: vendorID}}}
	if routeType == "loadbalance" {
		lv.Share = true
		lv.Vendors[0].Pct = weight
	}
	return Plan{Method: MethodFromRouteType(routeType), Levels: []Level{lv}}
}

// MethodFromRouteType maps the old route types onto the new ways.
func MethodFromRouteType(rt string) string {
	switch rt {
	case "lcr":
		return Cheapest
	case "quality":
		return Best
	case "capacity":
		return Least
	default: // direct, failover, loadbalance
		return Direct
	}
}

// Retry is a rule's retries (config/0131). A round is Main, then every
// backup; a round in which no vendor took the message sends it back to the
// queue to wait, and the next round starts again from Main.
type Retry struct {
	Rounds     int    `json:"rounds"`       // retries after the first round
	FirstSec   int    `json:"first_sec"`    // wait before the second round
	Backoff    string `json:"backoff"`      // "double" | "same"
	MaxWaitSec int    `json:"max_wait_sec"` // longest wait when doubling
}

// DefaultRetry is today's gateway behaviour (YAKU_MAX_ATTEMPTS 5, 30 s
// doubling up to 30 min).
var DefaultRetry = Retry{Rounds: 5, FirstSec: 30, Backoff: "double", MaxWaitSec: 1800}

// Validate checks a rule's retries.
func (r Retry) Validate() error {
	if r.Rounds < 0 || r.Rounds > 50 {
		return fmt.Errorf("retries: %d is outside 0..50", r.Rounds)
	}
	if r.FirstSec < 1 || r.FirstSec > 86400 {
		return fmt.Errorf("retries: first wait %d s is outside 1 s..1 day", r.FirstSec)
	}
	if r.Backoff != "double" && r.Backoff != "same" {
		return fmt.Errorf("retries: backoff %q must be double or same", r.Backoff)
	}
	if r.MaxWaitSec < r.FirstSec || r.MaxWaitSec > 86400 {
		return fmt.Errorf("retries: longest wait %d s must be between the first wait and 1 day", r.MaxWaitSec)
	}
	return nil
}

// Wait is how long a message waits before round n+1, after n failed rounds
// (n >= 1).
func (r Retry) Wait(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	s := r.FirstSec
	if r.Backoff == "double" {
		for i := 1; i < n && s < r.MaxWaitSec; i++ {
			s *= 2
		}
		if s > r.MaxWaitSec {
			s = r.MaxWaitSec
		}
	}
	return time.Duration(s) * time.Second
}

// WithDefaults fills unset fields from DefaultRetry (a rule stores NULL for
// "the gateway's default").
func (r Retry) WithDefaults(set bool) Retry {
	if !set {
		return DefaultRetry
	}
	if r.FirstSec == 0 {
		r.FirstSec = DefaultRetry.FirstSec
	}
	if r.Backoff == "" {
		r.Backoff = DefaultRetry.Backoff
	}
	if r.MaxWaitSec == 0 {
		r.MaxWaitSec = DefaultRetry.MaxWaitSec
		if r.MaxWaitSec < r.FirstSec {
			r.MaxWaitSec = r.FirstSec
		}
	}
	return r
}

// ShareWarning is what the screens say when a level shares by % and the
// rule's way orders by a score (Saleem 2026-09-30); "" for Direct. Pass the
// rule's way (Plan.MethodFor on an older plan). The portal carries the same
// words.
func ShareWarning(method string) string {
	switch method {
	case Cheapest:
		return "Sharing is on, so Cheapest is not fully used. Traffic is split by your %, so a more expensive vendor can get more messages than a cheaper one. Cheapest only decides who takes the share of a vendor that cannot send. Switch sharing off to always send to the cheapest."
	case Best:
		return "Sharing is on, so Best delivery is not fully used. Traffic is split by your %, so a vendor that delivers worse can get more messages than the best one. Best delivery only decides who takes the share of a vendor that cannot send. Switch sharing off to always send to the best-delivering vendor."
	case Value:
		return "Sharing is on, so Best value is not fully used. Traffic is split by your %, so a vendor with a worse price and delivery can get more messages than the best-value one. Best value only decides who takes the share of a vendor that cannot send. Switch sharing off to always send to the best value."
	case Least:
		return "Sharing is on, so Least busy is not fully used. Traffic is split by your %, so a busy vendor keeps getting its share while a less busy one has room. Least busy only decides who takes the share of a vendor that cannot send. Switch sharing off to always send to the least busy vendor."
	}
	return ""
}

// MethodName is a way's name on screen.
func MethodName(m string) string {
	switch m {
	case Direct:
		return "Direct"
	case Cheapest:
		return "Cheapest (LCR)"
	case Best:
		return "Best delivery"
	case Value:
		return "Best value"
	case Least:
		return "Least busy"
	}
	return m
}

// MethodWhat explains a way in one line.
func MethodWhat(m string) string {
	switch m {
	case Direct:
		return "your vendors, in your order"
	case Cheapest:
		return "lowest buy price for the number's network first"
	case Best:
		return "best recent delivery to the number's network first"
	case Value:
		return "price and delivery together (60% price, 40% delivery)"
	case Least:
		return "fewest messages waiting first"
	}
	return ""
}
