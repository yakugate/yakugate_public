package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/netcode"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// The plan read from and written to the store: loading an account's rules,
// keeping them in order (PlaceRules), the plan screen's view, the list of
// plans. [SCOPE P14-ROUTE]

// routeRow is one routes row as stored.
type routeRow struct {
	id                             int64
	name                           string
	priority                       int
	routeType                      string
	weight                         int
	direction                      string
	accountID                      int64
	nets                           string
	aun                            int
	sender, receiver, keyword, pri string
	vendorID                       int64
	plan                           string
	rounds, first, maxWait         dal.NullInt64
	backoff                        dal.NullString
	sell, buy, dsell, dbuy         int64
	disabled                       int
}

// routeRowOf is the row the store read (dal.RuleRow), as the plan reads it.
func routeRowOf(r dal.RuleRow) routeRow {
	return routeRow{
		id: r.ID, name: r.Name, priority: r.Priority, routeType: r.RouteType, weight: r.Weight,
		direction: r.Direction, accountID: r.AccountID, nets: r.Networks, aun: r.AcceptUnknown,
		sender: r.Sender, receiver: r.Receiver, keyword: r.Keyword, pri: r.Pri,
		vendorID: r.VendorID, plan: r.Plan, rounds: r.Rounds, first: r.First, maxWait: r.MaxWait,
		backoff: r.Backoff, sell: r.Sell, buy: r.Buy, dsell: r.DLRSell, dbuy: r.DLRBuy, disabled: r.Disabled,
	}
}

// storedPattern reads a stored {"match","replace"}.
func storedPattern(raw string) (match, replace string) {
	if strings.TrimSpace(raw) == "" {
		return "", ""
	}
	var m struct {
		Match   string `json:"match"`
		Replace string `json:"replace"`
	}
	if json.Unmarshal([]byte(raw), &m) != nil {
		return "", ""
	}
	return m.Match, m.Replace
}

func (rr routeRow) networks() []string {
	var n []string
	if strings.TrimSpace(rr.nets) != "" {
		_ = json.Unmarshal([]byte(rr.nets), &n)
	}
	return cleanCodes(n)
}

func (rr routeRow) fields() map[string]string {
	return map[string]string{FieldSender: rr.sender, FieldReceiver: rr.receiver, FieldText: rr.keyword, FieldPriority: rr.pri}
}

// fieldOrder is the order conditions are listed in.
var fieldOrder = []string{FieldSender, FieldText, FieldPriority, FieldReceiver}

func (rr routeRow) retry() *sendplan.Retry {
	if !rr.rounds.Valid && !rr.first.Valid && !rr.backoff.Valid && !rr.maxWait.Valid {
		return nil
	}
	r := sendplan.Retry{Rounds: int(rr.rounds.Int64), FirstSec: int(rr.first.Int64), Backoff: rr.backoff.String, MaxWaitSec: int(rr.maxWait.Int64)}
	if !rr.rounds.Valid {
		r.Rounds = sendplan.DefaultRetry.Rounds
	}
	r = r.WithDefaults(true)
	if r.Validate() != nil {
		d := sendplan.DefaultRetry
		return &d
	}
	return &r
}

// planRule reads a row the way the plan does.
func (rr routeRow) planRule() PlanRule {
	pr := PlanRule{
		ID: rr.id, Name: rr.name, Priority: rr.priority, On: rr.disabled == 0,
		Direction: strings.ToUpper(rr.direction), Networks: rr.networks(), AcceptUnknown: rr.aun != 0,
		RouteType: rr.routeType, VendorID: rr.vendorID, Weight: rr.weight, Retry: rr.retry(),
		SellCard: rr.sell, BuyCard: rr.buy, DLRSellCard: rr.dsell, DLRBuyCard: rr.dbuy,
		Stored: map[string][2]string{},
	}
	pr.Dest, _ = DestinationOf(pr.Networks)
	raw := rr.fields()
	for _, f := range fieldOrder {
		m, rep := storedPattern(raw[f])
		if m == "" && rep == "" {
			continue
		}
		pr.Stored[f] = [2]string{m, rep}
		c, ch := ReadField(f, m, rep)
		if c != nil {
			pr.Conds = append(pr.Conds, *c)
		}
		if ch != nil {
			pr.Changes = append(pr.Changes, *ch)
		}
	}
	if strings.TrimSpace(rr.plan) == "" || rr.plan == "null" {
		pr.Legacy = pr.Direction == "MT"
	} else if p, err := sendplan.Parse([]byte(rr.plan)); err != nil {
		pr.PlanError = err.Error()
	} else {
		pr.Plan = p
	}
	return pr
}

// route reads a row back as the Route a screen edits.
func (rr routeRow) route() Route {
	r := Route{
		ID: rr.id, Name: rr.name, Direction: strings.ToUpper(rr.direction), AccountID: rr.accountID,
		VendorID: rr.vendorID, Priority: rr.priority, Type: rr.routeType, AcceptUnknown: rr.aun != 0,
		Networks: rr.networks(), Disabled: rr.disabled != 0, Retry: rr.retry(),
		SellCard: rr.sell, BuyCard: rr.buy, DLRSellCard: rr.dsell, DLRBuyCard: rr.dbuy,
	}
	r.FromPattern = OzekiText(storedPattern(rr.sender))
	r.ToPattern = OzekiText(storedPattern(rr.receiver))
	r.Keyword = OzekiText(storedPattern(rr.keyword))
	r.PriorityPattern = OzekiText(storedPattern(rr.pri))
	return r
}

// loadRules reads an account's rules of one direction in the order the
// gateway reads them (priority, then id). lock takes the rows FOR UPDATE.
func loadRules(ctx context.Context, q dal.ConfigQueries, accountID int64, direction string, lock bool) ([]PlanRule, error) {
	rows, err := q.RuleRows(ctx, accountID, direction, lock)
	if err != nil {
		if re, ok := err.(dal.RowsError); ok {
			return nil, re.Err
		}
		return nil, fmt.Errorf("read the rules of account %d: %w", accountID, err)
	}
	var out []PlanRule
	for _, r := range rows {
		out = append(out, routeRowOf(r).planRule())
	}
	return out, nil
}

// nameRules fills the destination names from the operators table.
func (s Store) nameRules(ctx context.Context, rules []PlanRule) error {
	c, err := operatorCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	for i := range rules {
		r := &rules[i]
		if r.Dest.Kind == DestCountry || r.Dest.Kind == DestNetwork {
			r.Dest, _ = c.fill(r.Dest)
			continue
		}
		codes := cleanCodes(r.Networks)
		if r.Plan != nil && r.Plan.NetworksOnly() {
			codes = r.reach().codes
		}
		if len(codes) == 0 {
			continue
		}
		mcc, mnc := netcode.Split(codes[0])
		if co, ok := c.countries[mcc]; ok {
			r.CountryName = co.Country
		}
		if mnc != "" {
			if d, ok := c.fill(Destination{Kind: DestNetwork, MCC: mcc, MNC: mnc}); ok {
				r.NetworkName = d.Network
			}
		}
	}
	return nil
}

// vendorInfo is what the plan's words need of a vendor.
type vendorInfo struct {
	cid, transport, status string
}

func (s Store) vendorInfos(ctx context.Context) (VendorNames, map[int64]vendorInfo, error) {
	rows, err := s.DB.PlanVendors(ctx)
	if err != nil {
		if re, ok := err.(dal.RowsError); ok {
			return nil, nil, re.Err
		}
		return nil, nil, fmt.Errorf("read vendors: %w", err)
	}
	names := VendorNames{}
	info := map[int64]vendorInfo{}
	for _, r := range rows {
		v := vendorInfo{cid: r.CID, transport: r.Transport, status: r.Status}
		names[r.ID] = v.cid
		info[r.ID] = v
	}
	return names, info, nil
}

func (s Store) cardNames(ctx context.Context) map[int64]string {
	out, _ := s.DB.PlanCardNames(ctx)
	return out
}

// planAccount is what a plan shows of its account.
type planAccount struct {
	ID            int64
	Username      string
	CompanyID     int64
	Company       string
	Deployments   []string
	DefaultSender string
	HLRCheck      bool
	LossCheck     bool
}

func (s Store) planAccount(ctx context.Context, id int64) (planAccount, error) {
	a := planAccount{ID: id}
	row, err := s.DB.PlanAccount(ctx, id)
	if err == dal.ErrNoRows {
		return a, refusef("there is no account #%d", id)
	}
	if err != nil {
		return a, fmt.Errorf("read account %d: %w", id, err)
	}
	a.Username, a.CompanyID, a.Company, a.DefaultSender = row.Username, row.CompanyID, row.Company, row.DefaultSender
	deps, hlr, loss := row.Deployments, row.HLRCheck, row.LossCheck
	if l, err := deployment.Parse(deps); err == nil {
		a.Deployments = idsOf(l)
	}
	a.HLRCheck, a.LossCheck = hlr != 0, loss != 0
	return a, nil
}

// ── keeping the plan in order ─────────────────────────────────────────────

// Reorder is what putting a plan in order did (or would do).
type Reorder struct {
	AccountID int64
	Moves     []Move   // rules whose place changed
	Effects   []string // what changes for real traffic: rules no longer hidden
	Written   bool     // priorities were rewritten
}

// orderEffects says what putting a plan in order changes for real traffic:
// each rule hidden before and not after (the prototype's orderModal).
func orderEffects(before, after []PlanRule, n VendorNames) []string {
	hb, ha := Hidden(before), Hidden(after)
	var out []string
	for _, r := range before {
		j, was := hb[r.ID]
		if _, still := ha[r.ID]; !was || still {
			continue
		}
		c := before[j]
		if r.Dest.Kind == DestNetwork && !r.Special() {
			rl, cl := r.Levels(), c.Levels()
			if len(rl) > 0 && len(cl) > 0 {
				out = append(out, fmt.Sprintf("%s numbers: %s instead of %s.", r.Dest.Short(),
					n.LevelPlain(rl[0], r.Method()), n.LevelPlain(cl[0], c.Method())))
				continue
			}
		}
		out = append(out, fmt.Sprintf("'%s' is used again.", r.Name))
	}
	return out
}

// writeOrder gives the rules of order the priorities 10, 20, 30… (old rules
// sharing a priority keep sharing one). Nothing is written when the rules
// already stand in this order with rising priorities, unless force.
// priorityWriter writes one rule's priority: a dal.ConfigTx (or a test's
// recorder).
type priorityWriter interface {
	SetRulePriority(ctx context.Context, id int64, priority int) error
}

func writeOrder(ctx context.Context, q priorityWriter, current, order []PlanRule, force bool) (bool, error) {
	ts := tiersOf(order)
	need := force || len(current) != len(order)
	for i := range current {
		if need {
			break
		}
		need = current[i].ID != order[i].ID
	}
	if !need {
		last := -1 << 31
		for _, t := range ts {
			p := order[t[0]].Priority
			for _, i := range t {
				need = need || order[i].Priority != p
			}
			need = need || p <= last
			last = p
		}
	}
	if !need {
		return false, nil
	}
	for k, t := range ts {
		p := 10 * (k + 1)
		for _, i := range t {
			if order[i].Priority == p {
				continue
			}
			if err := q.SetRulePriority(ctx, order[i].ID, p); err != nil {
				return false, fmt.Errorf("put the rules in order: %w", err)
			}
			order[i].Priority = p
		}
	}
	return true, nil
}

// placeTx puts an account's outgoing rules in order inside tx: reads them
// (locked), orders them (CanonicalOrder) and writes the priorities. It
// returns the order read and the order written.
func (s Store) placeTx(ctx context.Context, tx dal.ConfigTx, accountID int64) (current, order []PlanRule, err error) {
	current, err = loadRules(ctx, tx, accountID, "MT", true)
	if err != nil {
		return nil, nil, err
	}
	if err := s.nameRules(ctx, current); err != nil {
		return nil, nil, err
	}
	order = CanonicalOrder(current)
	if _, err := writeOrder(ctx, tx, current, order, false); err != nil {
		return nil, nil, err
	}
	return current, order, nil
}

// PlaceRules puts an account's outgoing rules in the usual order ("Put in
// order"): special cases first, then each country's network rules above
// its whole-country rule, then any destination; priorities renumbered 10,
// 20, 30… in one transaction. It says what moved and what that changes.
func (s Store) PlaceRules(ctx context.Context, accountID int64) (Reorder, error) {
	out := Reorder{AccountID: accountID}
	names, _, err := s.vendorInfos(ctx)
	if err != nil {
		return out, err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	current, err := loadRules(ctx, tx, accountID, "MT", true)
	if err != nil {
		return out, err
	}
	if err := s.nameRules(ctx, current); err != nil {
		return out, err
	}
	before := append([]PlanRule(nil), current...)
	order := CanonicalOrder(current)
	out.Moves = Moves(before, order)
	out.Effects = orderEffects(before, order, names)
	if out.Written, err = writeOrder(ctx, tx, current, order, false); err != nil {
		return out, err
	}
	return out, tx.Commit()
}

// MoveSpecial moves a special case one place up or down among the special
// cases — the only rules whose order is the operator's. A move that would
// put a rule above one it catches every message of is refused: that rule
// would be used only when every vendor of the moved one is down.
func (s Store) MoveSpecial(ctx context.Context, routeID int64, up bool) (Reorder, error) {
	accountID, err := s.DB.OutgoingRuleAccount(ctx, routeID)
	if err != nil {
		return Reorder{}, refusef("there is no outgoing rule #%d", routeID)
	}
	out := Reorder{AccountID: accountID}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	current, err := loadRules(ctx, tx, accountID, "MT", true)
	if err != nil {
		return out, err
	}
	if err := s.nameRules(ctx, current); err != nil {
		return out, err
	}
	order := CanonicalOrder(current)
	ts := tiersOf(order)
	k := -1
	for i, t := range ts {
		for _, j := range t {
			if order[j].ID == routeID {
				k = i
			}
		}
	}
	if k < 0 {
		return out, refusef("there is no outgoing rule #%d", routeID)
	}
	me := order[ts[k][0]]
	if !me.Special() {
		return out, refusef("'%s' is placed by where it sends: only special cases (rules with a sender, text or priority condition) are moved by hand", me.Name)
	}
	other := k - 1
	if !up {
		other = k + 1
	}
	if other < 0 || other >= len(ts) || !order[ts[other][0]].Special() {
		where := "first"
		if !up {
			where = "last"
		}
		return out, refusef("'%s' is already the %s special case", me.Name, where)
	}
	upper, lower := me, order[ts[other][0]]
	if !up {
		upper, lower = lower, me
	}
	if Covers(upper, lower) && !Covers(lower, upper) {
		return out, refusef("can't move '%s' above '%s': it catches every message '%s' catches, so '%s' would be used only when every vendor of '%s' is down",
			upper.Name, lower.Name, lower.Name, lower.Name, upper.Name)
	}
	ts[k], ts[other] = ts[other], ts[k]
	var moved []PlanRule
	for _, t := range ts {
		for _, j := range t {
			moved = append(moved, order[j])
		}
	}
	out.Moves = Moves(current, moved)
	if out.Written, err = writeOrder(ctx, tx, current, moved, true); err != nil {
		return out, err
	}
	return out, tx.Commit()
}

// ── one rule's place ─────────────────────────────────────────────────────

// Placement is where a rule stands in its plan: before saving ("Its place")
// and after.
type Placement struct {
	RouteID    int64
	Name       string
	Position   int    // its place (1 = read first); 0 for an incoming rule
	Was        int    // its place before; 0 = a new rule
	Of         int    // rules in the plan
	Why        string // why it goes there
	Moves      []Move // OTHER rules whose place changes
	WasInOrder bool   // false: the save also put the plan in the usual order
	Text       string // "Its place": "Rule 3 of 6. A network rule goes above its country's rule ('Pakistan'). Moves: …"
	Saved      string // after saving: "Added 'Zong' as rule 2, above 'Pakistan'. CMPAK (Zong) numbers now go to Zong-B."
}

// placement describes rule id in the order after, against the order before
// (which does not hold it when it is new).
func placement(account string, before, after []PlanRule, id int64, isNew bool, n VendorNames) Placement {
	p := Placement{RouteID: id, Of: len(after), WasInOrder: len(before) == 0 || InOrder(before)}
	p.Position = posOf(after, id)
	if !isNew {
		p.Was = posOf(before, id)
	}
	if p.Position == 0 {
		return p
	}
	r := after[p.Position-1]
	p.Name = r.Name
	var others []PlanRule
	for _, x := range after {
		if x.ID != id {
			others = append(others, x)
		}
	}
	var prev []PlanRule
	for _, x := range before {
		if x.ID != id {
			prev = append(prev, x)
		}
	}
	for _, m := range Moves(before, after) {
		if m.ID != id && m.From != 0 {
			p.Moves = append(p.Moves, m)
		}
	}
	g, mcc := r.group()
	switch {
	case len(after) == 1:
		p.Why = "The first rule of " + account + "'s plan."
	case g == grpSpecial:
		var below *PlanRule
		for j := p.Position; j < len(after); j++ {
			x := after[j]
			if x.Special() && Covers(x, r) && !Covers(r, x) {
				below = &after[j]
				break
			}
		}
		switch {
		case below != nil:
			p.Why = fmt.Sprintf("Special cases are checked first. Placed above '%s', which catches every message this rule catches.", below.Name)
		case isNew:
			p.Why = "Special cases are checked first. A new one goes after the others."
		default:
			p.Why = "Special cases are checked first, in your order."
		}
	case g == grpNetwork:
		p.Why = "A network rule goes above 'Everything else'."
		for _, x := range after {
			if !x.Special() && x.Dest.Kind == DestCountry && x.Dest.MCC == mcc {
				p.Why = fmt.Sprintf("A network rule goes above its country's rule ('%s').", x.Name)
				break
			}
		}
	case g == grpCountry:
		p.Why = "A country rule goes after its network rules and above 'Everything else'."
	case g == grpAny:
		p.Why = "'Any destination' is checked last."
	default:
		p.Why = "A rule for several countries goes after the country rules and above 'Everything else'."
	}
	head := fmt.Sprintf("Rule %d of %d.", p.Position, p.Of)
	if !isNew && p.Was != p.Position && p.Was != 0 {
		head = fmt.Sprintf("Now rule %d. After saving: rule %d.", p.Was, p.Position)
	}
	p.Text = head + " " + p.Why
	if len(p.Moves) > 0 {
		pre := ""
		if !p.WasInOrder {
			pre = account + "'s plan is not in the usual order. Saving puts it in order. "
		}
		if len(p.Moves) <= 4 {
			var ws []string
			for _, m := range p.Moves {
				ws = append(ws, fmt.Sprintf("'%s' (%d → %d)", m.Name, m.From, m.To))
			}
			p.Text += " " + pre + "Moves: " + strings.Join(ws, ", ") + "."
		} else {
			p.Text += " " + pre + strconv.Itoa(len(p.Moves)) + " rules change place."
		}
	}
	main := "no vendor"
	if lv := r.Levels(); len(lv) > 0 {
		main = n.LevelPlain(lv[0], r.Method())
	}
	if isNew {
		p.Saved = fmt.Sprintf("Added '%s' as rule %d", r.Name, p.Position)
		if len(after) == 1 {
			if r.Dest.Kind == DestAny && !r.Special() {
				p.Saved = fmt.Sprintf("%s's plan is ready. Every message goes to %s.", account, main)
			} else {
				p.Saved = fmt.Sprintf("%s's plan is ready: rule 1 '%s' sends to %s. Messages it does not catch are refused (503 NO_ROUTE) until a rule catches them.", account, r.Name, main)
			}
		} else if g == grpNetwork && p.Position < len(after) && after[p.Position].Dest.Kind == DestCountry && !after[p.Position].Special() {
			p.Saved += fmt.Sprintf(", above '%s'. %s numbers now go to %s.", after[p.Position].Name, r.Dest.Short(), main)
		} else {
			p.Saved += "."
		}
	} else {
		p.Saved = fmt.Sprintf("Saved '%s'.", r.Name)
		if p.Was != 0 && p.Was != p.Position {
			p.Saved += fmt.Sprintf(" It moved from rule %d to rule %d.", p.Was, p.Position)
		}
		if !p.WasInOrder {
			p.Saved += " " + account + "'s plan was put in the usual order."
		}
	}
	return p
}

// mergeDraft is the plan with a draft rule in it, in the order the gateway
// would read it before placing: an edited rule replaces itself; a new one
// (id 0, given id -1 here) goes at the priority it names, else last.
func mergeDraft(rules []PlanRule, draft PlanRule, editingID int64) []PlanRule {
	out := make([]PlanRule, 0, len(rules)+1)
	maxP := 0
	for _, r := range rules {
		if r.Priority > maxP {
			maxP = r.Priority
		}
	}
	if editingID > 0 {
		draft.ID = editingID
		for _, r := range rules {
			if r.ID == editingID {
				if draft.Priority <= 0 {
					draft.Priority = r.Priority
				}
				out = append(out, draft)
				continue
			}
			out = append(out, r)
		}
	} else {
		draft.ID = -1
		if draft.Priority <= 0 {
			draft.Priority = maxP + 10
		}
		out = append(out, rules...)
		out = append(out, draft)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Priority < out[j].Priority })
	return out
}

// PreviewPlacement is "Its place" before saving: where the rule would go
// in its account's plan, why, and which rules would move. editingID is the
// rule being edited (0 for a new one).
func (s Store) PreviewPlacement(ctx context.Context, r Route, editingID int64) (Placement, error) {
	if !strings.EqualFold(r.Direction, "MT") && r.Direction != "" {
		return Placement{}, nil
	}
	acct, err := s.planAccount(ctx, r.AccountID)
	if err != nil {
		return Placement{}, err
	}
	rules, err := loadRules(ctx, s.DB, r.AccountID, "MT", false)
	if err != nil {
		return Placement{}, err
	}
	draft, err := r.planRule()
	if err != nil {
		return Placement{}, err
	}
	all := append(append([]PlanRule(nil), rules...), draft)
	if err := s.nameRules(ctx, all); err != nil {
		return Placement{}, err
	}
	draft = all[len(all)-1]
	names, _, err := s.vendorInfos(ctx)
	if err != nil {
		return Placement{}, err
	}
	merged := mergeDraft(all[:len(all)-1], draft, editingID)
	id := editingID
	if id <= 0 {
		id = -1
	}
	p := placement(acct.Username, all[:len(all)-1], CanonicalOrder(merged), id, editingID <= 0, names)
	if editingID <= 0 {
		p.RouteID = 0
	}
	return p, nil
}

// RouteInWords is a rule "In words", before it is saved: "Every message to
// CMPAK (Zong) (Pakistan 410-004) goes to zong-b. If zong-b can't send,
// Backup 1 sends to telenor-g."
func (s Store) RouteInWords(ctx context.Context, r Route) (string, error) {
	pr, err := r.planRule()
	if err != nil {
		return "", err
	}
	one := []PlanRule{pr}
	if err := s.nameRules(ctx, one); err != nil {
		return "", err
	}
	names, _, err := s.vendorInfos(ctx)
	if err != nil {
		return "", err
	}
	cards := map[int64]string{}
	if r.SellCard != 0 {
		cards = s.cardNames(ctx)
	}
	return one[0].Sentence(names, cards), nil
}

// ── the plan screen ─────────────────────────────────────────────────────

// RuleView is one rule on the plan screen.
type RuleView struct {
	PlanRule
	Position   int
	Special    bool
	Where      string   // "CMPAK (Zong) (Pakistan 410-004)", "Any destination", "Pakistan"
	Conditions string   // "sender is MyBank · text contains OTP"
	Way        string   // "Cheapest (LCR)"
	Sends      string   // "Direct: Jazz-A", "Shared: A 70%, B 30%" (+ " · +1 backup")
	Levels     []string // "Main: Jazz-A", "Backup 1: Telenor-G"
	FallsBack  string   // "→ Backup 1: Telenor-G → rule 3 'Pakistan' → accepted and retried"
	HiddenBy   int64    // the rule above that catches every message this one catches (0 = none)
	HiddenNote string   // "Used only when every vendor of rule 1 'Pakistan' is down: …"
	Overlap    string   // "A message that fits both goes by rule 1 'Urgent' (above)."
	Changes    []string // what it changes before sending, in words
	Badges     []string // Off, Hidden, Changes the sender, Own retries, Old rule: …
	Sentence   string   // "In words"
	Retries    string   // "5 · 30 s · double · 30 min", or "the usual"
}

// PlanGroup is a heading on the plan screen and the rules under it.
type PlanGroup struct {
	Title     string
	Note      string
	Rules     []int         // indexes into PlanView.Rules
	Uncovered []Destination // networks of this country with no rule of their own
	UsesNote  string        // "They use rule 3 'Pakistan'." / "No rule catches them: refused (503 NO_ROUTE)."
	Missing   string        // under "Everything else" when no rule sends to any destination
}

// PlanView is an account's routing plan as the screen shows it.
type PlanView struct {
	AccountID     int64
	Account       string
	CompanyID     int64
	Company       string
	Deployments   []string
	DefaultSender string
	HLRCheck      bool
	LossCheck     bool

	Rules    []RuleView // outgoing rules, in the order the gateway reads them
	InOrder  bool
	Moves    []Move   // what "Put in order" would move (none when in order)
	Effects  []string // and what that changes for real traffic
	Hidden   int
	Status   string // Ready | Not ready: no rule | Not ready: no rule is on | Not in the usual order | 1 rule hidden
	Level    string // ok | warn | bad
	Coverage string
	Banners  []string
	Groups   []PlanGroup
	MO       []RuleView // incoming rules, as they were

	rules []PlanRule
	names VendorNames
}

// planStatus is a plan's badge (the prototype's planStatus, without prices).
func planStatus(rules []PlanRule) (status, level string) {
	on := 0
	for _, r := range rules {
		if r.On {
			on++
		}
	}
	switch {
	case len(rules) == 0:
		return "Not ready: no rule", "bad"
	case on == 0:
		return "Not ready: no rule is on", "bad"
	case !InOrder(rules):
		return "Not in the usual order", "warn"
	}
	if h := len(Hidden(rules)); h > 0 {
		return plural(h, "1 rule hidden", strconv.Itoa(h)+" rules hidden"), "warn"
	}
	return "Ready", "ok"
}

// coversShort says what a plan reaches: "Pakistan (+1 network) · UAE ·
// everything else", or "Only Pakistan".
func coversShort(rules []PlanRule) string {
	var rs []PlanRule
	for _, r := range rules {
		if r.On && !r.Special() && r.PlanError == "" {
			rs = append(rs, r)
		}
	}
	if len(rs) == 0 {
		return "Nothing yet"
	}
	var mccs []string
	label := map[string]string{}
	anyRule := false
	for _, r := range rs {
		g, mcc := r.group()
		if g == grpAny {
			anyRule = true
		}
		if mcc != "" && label[mcc] == "" {
			label[mcc] = r.countryLabel(mcc)
			mccs = append(mccs, mcc)
		}
	}
	sort.SliceStable(mccs, func(i, j int) bool { return strings.ToLower(label[mccs[i]]) < strings.ToLower(label[mccs[j]]) })
	var parts []string
	for _, m := range mccs {
		nets, hasC := 0, false
		for _, r := range rs {
			g, mcc := r.group()
			if mcc != m {
				continue
			}
			if g == grpNetwork {
				nets++
			} else {
				hasC = true
			}
		}
		nw := plural(nets, "1 network", strconv.Itoa(nets)+" networks")
		if hasC {
			if nets > 0 {
				parts = append(parts, label[m]+" (+"+nw+")")
			} else {
				parts = append(parts, label[m])
			}
		} else {
			parts = append(parts, label[m]+": "+nw)
		}
	}
	for _, r := range rs {
		if g, _ := r.group(); g == grpSeveral {
			parts = append(parts, "networks "+strings.Join(cleanCodes(r.Networks), ", "))
		}
	}
	if anyRule {
		return strings.Join(append(parts, "everything else"), " · ")
	}
	return "Only " + strings.Join(parts, ", ")
}

// coverageLine says, from the rules, where each country's numbers go.
func coverageLine(rules []PlanRule) string {
	var rs []PlanRule
	special := 0
	for _, r := range rules {
		if !r.On || r.PlanError != "" {
			continue
		}
		if r.Special() {
			special++
			continue
		}
		rs = append(rs, r)
	}
	var anyRule *PlanRule
	for i := range rs {
		if rs[i].Dest.Kind == DestAny && anyRule == nil {
			anyRule = &rs[i]
		}
	}
	var mccs []string
	label := map[string]string{}
	for _, r := range rs {
		if _, mcc := r.group(); mcc != "" && label[mcc] == "" {
			label[mcc] = r.countryLabel(mcc)
			mccs = append(mccs, mcc)
		}
	}
	sort.SliceStable(mccs, func(i, j int) bool { return strings.ToLower(label[mccs[i]]) < strings.ToLower(label[mccs[j]]) })
	var parts []string
	for _, m := range mccs {
		var own []string
		var cr *PlanRule
		for i := range rs {
			g, mcc := rs[i].group()
			if mcc != m {
				continue
			}
			if g == grpNetwork {
				own = append(own, rs[i].networkLabel())
			} else if cr == nil {
				cr = &rs[i]
			}
		}
		other := "are refused"
		switch {
		case cr != nil:
			other = "use '" + cr.Name + "'"
		case anyRule != nil:
			other = "use '" + anyRule.Name + "'"
		}
		if len(own) == 0 {
			if cr != nil {
				parts = append(parts, label[m]+": uses '"+cr.Name+"'")
			} else {
				parts = append(parts, label[m]+": refused")
			}
			continue
		}
		verb := " has its own rule"
		if len(own) > 1 {
			verb = " have their own rules"
		}
		parts = append(parts, label[m]+": "+andWords(own)+verb+", other networks "+other)
	}
	if anyRule != nil {
		parts = append(parts, "every other country, and numbers whose country is not found, use '"+anyRule.Name+"'")
	} else {
		var ls []string
		for _, m := range mccs {
			ls = append(ls, label[m])
		}
		if len(ls) == 0 {
			parts = append(parts, "numbers are refused (503 NO_ROUTE): no rule sends to any destination")
		} else {
			parts = append(parts, "numbers outside "+andWords(ls)+" are refused (503 NO_ROUTE): no rule sends to any destination")
		}
	}
	s := strings.Join(parts, " · ") + "."
	if special > 0 {
		s = plural(special, "1 special case", strconv.Itoa(special)+" special cases") + " first. " + s
	}
	return s
}

func ruleView(rules []PlanRule, i int, hidden map[int64]int, n VendorNames, cards map[int64]string) RuleView {
	r := rules[i]
	v := RuleView{PlanRule: r, Position: i + 1, Special: r.Special()}
	switch {
	case r.Dest.Kind == "":
		v.Where = "Networks " + strings.Join(cleanCodes(r.Networks), ", ")
	default:
		v.Where = r.Dest.Label()
	}
	if r.Plan != nil && r.Plan.NetworksOnly() {
		v.Where = "Only networks " + strings.Join(r.reach().codes, ", ")
	}
	var cs []string
	for _, c := range r.Conds {
		cs = append(cs, c.Words())
	}
	v.Conditions = strings.Join(cs, " · ")
	for _, ch := range r.Changes {
		v.Changes = append(v.Changes, ch.Words())
	}
	if r.Direction == "MO" {
		return v
	}
	v.Way = sendplan.MethodName(r.Method())
	levels := r.Levels()
	if len(levels) > 0 {
		v.Sends = n.LevelShort(levels[0], r.Method())
		if b := len(levels) - 1; b > 0 {
			v.Sends += " · +" + plural(b, "1 backup", strconv.Itoa(b)+" backups")
		}
	} else if r.Plan != nil && r.Plan.NetworksOnly() {
		v.Sends = "Each of its networks' own vendors"
	} else if r.PlanError != "" {
		v.Sends = "Unreadable: " + r.PlanError
	}
	for k, lv := range levels {
		v.Levels = append(v.Levels, sendplan.LevelName(k)+": "+n.LevelPlain(lv, r.Method()))
	}
	if r.Plan != nil {
		for _, code := range sortedExceptionCodes(r.Plan) {
			for k, lv := range r.Plan.Exceptions[code] {
				v.Levels = append(v.Levels, "Network "+code+" "+sendplan.LevelName(k)+": "+n.LevelPlain(lv, r.Method()))
			}
		}
	}
	v.FallsBack = fallbackText(rules, i, n)
	if j, ok := hidden[r.ID]; ok {
		c := rules[j]
		v.HiddenBy = c.ID
		v.HiddenNote = fmt.Sprintf("Used only when every vendor of rule %d '%s' is down: that rule catches every message this one catches.", j+1, c.Name)
		if SameCatch(r, c) {
			v.HiddenNote += " Both catch exactly the same messages: add its vendors to that rule as a backup, or turn it off."
		}
	}
	if r.Special() && v.HiddenBy == 0 {
		for j := i - 1; j >= 0; j-- {
			x := rules[j]
			if x.On && x.Special() && CanOverlap(x, r) {
				v.Overlap = fmt.Sprintf("A message that fits both goes by rule %d '%s' (above).", j+1, x.Name)
				break
			}
		}
	}
	if !r.On {
		v.Badges = append(v.Badges, "Off")
	}
	if v.HiddenBy != 0 {
		v.Badges = append(v.Badges, "Hidden")
	}
	for _, ch := range r.Changes {
		if ch.Field == FieldSender {
			v.Badges = append(v.Badges, "Changes the sender")
		}
	}
	if r.Retry != nil {
		v.Badges = append(v.Badges, "Own retries")
		v.Retries = RetryShort(r.Retry)
	} else {
		v.Retries = "the usual (" + RetryShort(nil) + ")"
	}
	if r.PlanError != "" {
		v.Badges = append(v.Badges, "Skipped by the gateway: its send plan cannot be read")
	}
	for _, o := range r.OldShape() {
		v.Badges = append(v.Badges, "Old rule: "+o)
	}
	for _, lv := range levels {
		if lv.Share && len(lv.Vendors) > 1 && sendplan.ScoreMethod(r.Method()) {
			v.Badges = append(v.Badges, "Shares by %, so "+sendplan.MethodName(r.Method())+" is not fully used")
			break
		}
	}
	v.Sentence = r.Sentence(n, cards)
	return v
}

// buildPlanView puts the screen together from the rules (in the order the
// gateway reads them). netsOf lists a country's networks (nil: not shown).
func buildPlanView(acct planAccount, rules, mo []PlanRule, n VendorNames, cards map[int64]string, netsOf func(string) []Destination) PlanView {
	v := PlanView{AccountID: acct.ID, Account: acct.Username, CompanyID: acct.CompanyID, Company: acct.Company,
		Deployments: acct.Deployments, DefaultSender: acct.DefaultSender, HLRCheck: acct.HLRCheck, LossCheck: acct.LossCheck,
		rules: rules, names: n}
	hidden := Hidden(rules)
	v.Hidden = len(hidden)
	v.InOrder = InOrder(rules)
	if !v.InOrder {
		order := CanonicalOrder(rules)
		v.Moves = Moves(rules, order)
		v.Effects = orderEffects(rules, order, n)
	}
	v.Status, v.Level = planStatus(rules)
	for i := range rules {
		v.Rules = append(v.Rules, ruleView(rules, i, hidden, n, cards))
	}
	for i := range mo {
		v.MO = append(v.MO, ruleView(mo, i, nil, n, cards))
	}
	on, old, placed := 0, 0, 0
	var shapes []string // what the old rules have, named once each
	seenShape := map[string]bool{}
	for _, r := range rules {
		if r.On {
			on++
		}
		if len(r.OldShape()) > 0 && !r.Legacy {
			old++
			for _, sh := range r.OldShape() {
				if !seenShape[sh] {
					seenShape[sh] = true
					shapes = append(shapes, sh)
				}
			}
		}
		if g, _ := r.group(); g == grpNetwork || g == grpCountry {
			placed++
		}
	}
	switch {
	case len(rules) == 0:
		v.Banners = append(v.Banners, acct.Username+" has no rule yet, so every message is refused (503 NO_ROUTE).")
	case on == 0:
		v.Banners = append(v.Banners, acct.Username+" has no rule that is on, so every message is refused (503 NO_ROUTE).")
	}
	if !v.InOrder {
		why := "Its rules are not in the order the screen keeps."
		for i, r := range rules {
			if j, ok := hidden[r.ID]; ok {
				why = fmt.Sprintf("Rule %d '%s' is hidden: rule %d '%s' sits above it and catches every message it catches.", i+1, r.Name, j+1, rules[j].Name)
				break
			}
		}
		v.Banners = append(v.Banners, "This plan is not in the usual order. "+why+" Put it in order to fix it.")
	} else if v.Hidden > 0 {
		v.Banners = append(v.Banners, plural(v.Hidden, "1 rule is", strconv.Itoa(v.Hidden)+" rules are")+" hidden: see the notes on the rules.")
	}
	if !acct.HLRCheck && placed > 0 {
		v.Banners = append(v.Banners, acct.Username+" does not look up numbers (HLR check off), so every number counts as 'country not found': country and network rules are not used, unless the client sends the network with the message.")
	}
	if old > 0 {
		v.Banners = append(v.Banners, plural(old, "1 rule uses", strconv.Itoa(old)+" rules use")+" a shape that is no longer made ("+orWords(shapes)+"). They send as before.")
	}
	if len(rules) > 0 && v.InOrder {
		v.Coverage = coverageLine(rules)
	}
	v.Groups = planGroups(rules, v.InOrder, netsOf)
	return v
}

func planGroups(rules []PlanRule, inOrder bool, netsOf func(string) []Destination) []PlanGroup {
	if len(rules) == 0 {
		return nil
	}
	if !inOrder {
		g := PlanGroup{Title: "In the order the gateway reads them", Note: "This plan was saved in another order. Put it in order to group it."}
		for i := range rules {
			g.Rules = append(g.Rules, i)
		}
		return []PlanGroup{g}
	}
	var out []PlanGroup
	sp := PlanGroup{Title: "Special cases", Note: "Checked first, top to bottom. Move them up or down to change their order."}
	for i, r := range rules {
		if r.Special() {
			sp.Rules = append(sp.Rules, i)
		}
	}
	if len(sp.Rules) > 0 {
		out = append(out, sp)
	}
	var mccs []string
	byMCC := map[string][]int{}
	var several, anyIdx []int
	for i, r := range rules {
		g, mcc := r.group()
		switch g {
		case grpNetwork, grpCountry:
			if byMCC[mcc] == nil {
				mccs = append(mccs, mcc)
			}
			byMCC[mcc] = append(byMCC[mcc], i)
		case grpSeveral:
			several = append(several, i)
		case grpAny:
			anyIdx = append(anyIdx, i)
		}
	}
	for _, m := range mccs {
		first := rules[byMCC[m][0]]
		d := Destination{Kind: DestCountry, MCC: m, Country: first.countryLabel(m)}
		if first.Dest.MCC == m {
			d.CCC = first.Dest.CCC
		}
		note := "MCC " + m + " · " + plural(len(byMCC[m]), "1 rule", strconv.Itoa(len(byMCC[m]))+" rules")
		if d.CCC != "" {
			note = "+" + d.CCC + " · " + note
		}
		g := PlanGroup{Title: d.countryName(), Note: note, Rules: byMCC[m]}
		if netsOf != nil {
			for _, nw := range netsOf(m) {
				has := false
				for _, r := range rules {
					if !r.Special() && r.Dest.Kind == DestNetwork && r.Dest.Same(nw) {
						has = true
					}
				}
				if !has {
					g.Uncovered = append(g.Uncovered, nw)
				}
			}
			if len(g.Uncovered) > 0 {
				if by := inheritFor(rules, d); by != nil {
					g.UsesNote = fmt.Sprintf("They use rule %d '%s'.", posOf(rules, by.ID), by.Name)
				} else {
					g.UsesNote = "No rule catches them: refused (503 NO_ROUTE)."
				}
			}
		}
		out = append(out, g)
	}
	if len(several) > 0 {
		out = append(out, PlanGroup{Title: "Several countries", Note: "Old rules that send to networks of more than one country.", Rules: several})
	}
	ev := PlanGroup{Title: "Everything else", Note: "Other countries, and numbers whose country is not found. Checked last.", Rules: anyIdx}
	if len(anyIdx) == 0 {
		ev.Missing = "Not set up. Messages no rule catches are refused (503 NO_ROUTE)."
	}
	return append(out, ev)
}

// DestinationTag says what a destination uses in this plan now: "rule 3
// 'Pakistan'" (it has its own), "uses rule 5 'Everything else'", or "no
// rule: refused". editingID leaves that rule out (its own destination is
// free to keep).
func (v PlanView) DestinationTag(d Destination, editingID int64) string {
	var base []PlanRule
	for _, r := range v.rules {
		if r.ID != editingID {
			base = append(base, r)
		}
	}
	for _, r := range base {
		if !r.Special() && r.Dest.Kind != "" && r.Dest.Same(d) {
			return fmt.Sprintf("rule %d '%s'", posOf(v.rules, r.ID), r.Name)
		}
	}
	if by := inheritFor(base, d); by != nil {
		return fmt.Sprintf("uses rule %d '%s'", posOf(v.rules, by.ID), by.Name)
	}
	return "no rule: refused"
}

// DeleteEffect says where a rule's messages go when it is deleted.
func (v PlanView) DeleteEffect(id int64) string {
	for _, r := range v.MO {
		if r.ID == id {
			return deleteEffect(v.Account, []PlanRule{r.PlanRule}, id, v.names)
		}
	}
	return deleteEffect(v.Account, v.rules, id, v.names)
}

// SwitchEffect says what switching a rule off or on does; ask=true: ask
// first (the last rule that is on).
func (v PlanView) SwitchEffect(id int64, on bool) (text string, ask bool) {
	for _, r := range v.MO {
		if r.ID == id {
			return switchEffect(v.Account, []PlanRule{r.PlanRule}, id, on)
		}
	}
	return switchEffect(v.Account, v.rules, id, on)
}

// Rule finds a rule of the plan (outgoing or incoming) by id.
func (v PlanView) Rule(id int64) (RuleView, bool) {
	for _, r := range v.Rules {
		if r.ID == id {
			return r, true
		}
	}
	for _, r := range v.MO {
		if r.ID == id {
			return r, true
		}
	}
	return RuleView{}, false
}

// Plan reads an account's routing plan for the screen.
func (s Store) Plan(ctx context.Context, accountID int64) (PlanView, error) {
	acct, err := s.planAccount(ctx, accountID)
	if err != nil {
		return PlanView{}, err
	}
	rules, err := loadRules(ctx, s.DB, accountID, "MT", false)
	if err != nil {
		return PlanView{}, err
	}
	mo, err := loadRules(ctx, s.DB, accountID, "MO", false)
	if err != nil {
		return PlanView{}, err
	}
	if err := s.nameRules(ctx, rules); err != nil {
		return PlanView{}, err
	}
	names, _, err := s.vendorInfos(ctx)
	if err != nil {
		return PlanView{}, err
	}
	c, err := operatorCatalog(ctx, s.DB)
	if err != nil {
		return PlanView{}, err
	}
	return buildPlanView(acct, rules, mo, names, s.cardNames(ctx), c.networksOf), nil
}

// DeleteEffect says where a rule's messages go when it is deleted — what
// the console asks before deleting ("Delete 'Zong'? CMPAK (Zong) numbers
// will go to rule 3 'Pakistan' (the cheapest of C and D).").
func (s Store) DeleteEffect(ctx context.Context, routeID int64) (string, error) {
	accountID, err := s.DB.RuleAccount(ctx, routeID)
	if err != nil {
		return "", refusef("there is no rule #%d", routeID)
	}
	v, err := s.Plan(ctx, accountID)
	if err != nil {
		return "", err
	}
	return v.DeleteEffect(routeID), nil
}

// SwitchEffect says what switching a rule off or on does, and whether to
// ask first.
func (s Store) SwitchEffect(ctx context.Context, routeID int64, on bool) (string, bool, error) {
	accountID, err := s.DB.RuleAccount(ctx, routeID)
	if err != nil {
		return "", false, refusef("there is no rule #%d", routeID)
	}
	v, err := s.Plan(ctx, accountID)
	if err != nil {
		return "", false, err
	}
	t, ask := v.SwitchEffect(routeID, on)
	return t, ask, nil
}

// ── the list of plans ───────────────────────────────────────────────────

// PlanSummary is one line of the plans list.
type PlanSummary struct {
	AccountID   int64
	Account     string
	CompanyID   int64
	Company     string
	Deployments []string
	Rules       int // outgoing rules
	Special     int
	Off         int
	Covers      string // "Pakistan (+1 network) · UAE · everything else"
	Status      string
	Level       string // ok | warn | bad
	Vendors     []int64
}

func (s Store) planSummaries(ctx context.Context) (withPlan, without []PlanSummary, err error) {
	rows, err := s.DB.OutgoingRuleRows(ctx)
	if err != nil {
		if re, ok := err.(dal.RowsError); ok {
			return nil, nil, re.Err
		}
		return nil, nil, fmt.Errorf("read the rules: %w", err)
	}
	byAcct := map[int64][]PlanRule{}
	for _, r := range rows {
		rr := routeRowOf(r)
		byAcct[rr.accountID] = append(byAcct[rr.accountID], rr.planRule())
	}
	arows, err := s.DB.PlanAccounts(ctx)
	if err != nil {
		if re, ok := err.(dal.RowsError); ok {
			return nil, nil, re.Err
		}
		return nil, nil, fmt.Errorf("read the accounts: %w", err)
	}
	for _, a := range arows {
		p := PlanSummary{AccountID: a.ID, Account: a.Username, CompanyID: a.CompanyID, Company: a.Company}
		deps := a.Deployments
		if l, err := deployment.Parse(deps); err == nil {
			p.Deployments = idsOf(l)
		}
		rules := byAcct[p.AccountID]
		if len(rules) == 0 {
			without = append(without, p)
			continue
		}
		if err := s.nameRules(ctx, rules); err != nil {
			return nil, nil, err
		}
		p.Rules = len(rules)
		seen := map[int64]bool{}
		for _, r := range rules {
			if r.Special() {
				p.Special++
			}
			if !r.On {
				p.Off++
			}
			for _, lv := range r.Levels() {
				for _, vv := range lv.Vendors {
					if !seen[vv.VendorID] {
						seen[vv.VendorID] = true
						p.Vendors = append(p.Vendors, vv.VendorID)
					}
				}
			}
		}
		p.Covers = coversShort(rules)
		p.Status, p.Level = planStatus(rules)
		withPlan = append(withPlan, p)
	}
	return withPlan, without, nil
}

// ListPlans lists the accounts that have a routing plan (at least one
// outgoing rule), with what each covers and its status.
func (s Store) ListPlans(ctx context.Context) ([]PlanSummary, error) {
	with, _, err := s.planSummaries(ctx)
	return with, err
}

// AccountsWithoutPlan lists the accounts with no outgoing rule — the ones
// "New plan" offers. Every message of theirs is refused (503 NO_ROUTE).
func (s Store) AccountsWithoutPlan(ctx context.Context) ([]PlanSummary, error) {
	_, without, err := s.planSummaries(ctx)
	for i := range without {
		without[i].Status, without[i].Level = "Not ready: no rule", "bad"
		without[i].Covers = "Nothing yet"
	}
	return without, err
}
