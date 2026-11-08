package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// Writing a rule: create, change, and the checks both share. Every save of
// an outgoing rule leaves its account's plan in the usual order
// (PlaceRules), and says where the rule went. [SCOPE P14-ROUTE]

// planRule reads a Route (a draft) the way the plan reads a stored rule.
func (r Route) planRule() (PlanRule, error) {
	dir := strings.ToUpper(strings.TrimSpace(r.Direction))
	if dir == "" {
		dir = "MT"
	}
	pr := PlanRule{
		ID: r.ID, Name: strings.TrimSpace(r.Name), Priority: r.Priority, On: !r.Disabled, Direction: dir,
		Networks: cleanCodes(r.Networks), AcceptUnknown: r.AcceptUnknown, VendorID: r.VendorID,
		RouteType: r.Type, Weight: 100, Retry: r.Retry,
		SellCard: r.SellCard, BuyCard: r.BuyCard, DLRSellCard: r.DLRSellCard, DLRBuyCard: r.DLRBuyCard,
		Stored: map[string][2]string{},
	}
	pr.Dest, _ = DestinationOf(pr.Networks)
	texts := map[string]string{FieldSender: r.FromPattern, FieldReceiver: r.ToPattern, FieldText: r.Keyword, FieldPriority: r.PriorityPattern}
	for _, f := range fieldOrder {
		m, rep, err := ParseCondition(texts[f])
		if err != nil {
			return pr, refusef("the %s condition: %v", fieldWord(f), Reason(err))
		}
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
	if r.Plan != nil {
		u, _ := r.Plan.Unified()
		pr.Plan = &u
	} else if dir == "MT" {
		pr.Legacy = true
	}
	return pr, nil
}

// GetRoute reads one rule as the Route a screen edits, with notes about it:
// an old rule's shape, or that its plan cannot be read. An outgoing rule
// made before 2026-09-30 (no send plan) comes back with the plan the engine
// reads it as — one Main level with its vendor, the way its rule type said
// — and stays an old rule if saved with the same vendor and way.
func (s Store) GetRoute(ctx context.Context, id int64) (Route, []string, error) {
	row, err := s.DB.RuleByID(ctx, id)
	if err == dal.ErrNoRows {
		return Route{}, nil, refusef("there is no rule #%d", id)
	}
	if err != nil {
		return Route{}, nil, fmt.Errorf("read rule %d: %w", id, err)
	}
	rr := routeRowOf(row)
	r := rr.route()
	pr := rr.planRule()
	var notes []string
	if r.Direction == "MT" {
		switch {
		case pr.PlanError != "":
			notes = append(notes, "Its send plan cannot be read ("+pr.PlanError+"), so the gateway skips it. Give it Main again to repair it.")
		case pr.Plan != nil:
			u, un := pr.Plan.Unified()
			r.Plan = &u
			for _, n := range un {
				notes = append(notes, n+" Saving makes that the rule's way.")
			}
		case pr.Legacy:
			if lp := legacyPlan(rr); lp != nil {
				r.Plan = lp
				notes = append(notes, fmt.Sprintf("An old rule: one vendor, chosen as '%s' (%s). It keeps working as it is; saving it with other vendors or another way makes it a rule with Main and backups.",
					rr.routeType, sendplan.MethodName(lp.Method)))
			}
		}
		if len(pr.Networks) > 1 && !(pr.Plan != nil && pr.Plan.NetworksOnly()) {
			notes = append(notes, "It sends to several destinations ("+strings.Join(pr.Networks, ", ")+"), a shape no longer made. It can be kept as it is; a new rule has one destination.")
		}
		if pr.Plan != nil && len(pr.Plan.Exceptions) > 0 {
			notes = append(notes, "It has network tabs (other vendors for some networks), which are no longer made. They are kept when it is saved.")
		}
		if c := pr.cond(FieldReceiver); c != nil {
			notes = append(notes, "It has a receiver condition ("+c.Words()+"), kept from before; a new rule chooses a destination instead.")
		}
		if pr.Dest.Kind == DestNetwork && pr.AcceptUnknown {
			notes = append(notes, "It also takes numbers whose country is not found (kept from before); a new network rule does not — those go to 'Everything else'.")
		}
	}
	return r, notes, nil
}

// legacyPlan is the plan an old rule is read as, one vendor at Main.
func legacyPlan(rr routeRow) *sendplan.Plan {
	if rr.vendorID <= 0 {
		return nil
	}
	return &sendplan.Plan{Method: sendplan.MethodFromRouteType(rr.routeType),
		Levels: []sendplan.Level{{Vendors: []sendplan.Vendor{{VendorID: rr.vendorID}}}}}
}

func planJSON(p *sendplan.Plan) string {
	if p == nil {
		return ""
	}
	u, _ := p.Unified()
	b, _ := json.Marshal(u)
	return string(b)
}

// CreateRoute writes a new rule, after the checks that need the store, and
// puts its account's outgoing rules in order. It returns the rule's id.
func (s Store) CreateRoute(ctx context.Context, r Route) (int64, error) {
	p, err := s.CreateRoutePlaced(ctx, r)
	return p.RouteID, err
}

// CreateRoutePlaced is CreateRoute, saying where the rule went: an outgoing
// rule is placed in its plan (a new special case after the other special
// cases unless a rule it covers must stay above it; a Priority given is
// honoured among the special cases), and the other rules renumbered.
func (s Store) CreateRoutePlaced(ctx context.Context, r Route) (Placement, error) {
	r.Direction = strings.ToUpper(strings.TrimSpace(r.Direction))
	r.Type = "direct"
	if v := r.Plan.FirstVendor(); v > 0 {
		// Main's first vendor (a network-only plan: its first network's), for
		// the older readers of vendor_id.
		r.VendorID = v
	}
	anyTakesUnknown(&r)
	foldRuleBuy(&r)
	if err := ValidateRoute(r); err != nil {
		return Placement{}, err
	}
	if err := s.checkRoute(ctx, r, nil); err != nil {
		return Placement{}, err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return Placement{}, err
	}
	defer tx.Rollback()
	var before []PlanRule
	prio := r.Priority
	if r.Direction == "MT" {
		if before, err = loadRules(ctx, tx, r.AccountID, "MT", true); err != nil {
			return Placement{}, err
		}
		if prio <= 0 {
			prio = 10
			for _, x := range before {
				if x.Priority+10 > prio {
					prio = x.Priority + 10
				}
			}
		}
	}
	var plan any
	if r.Plan != nil {
		plan = planJSON(r.Plan)
	}
	id, err := tx.InsertRule(ctx, dal.RuleWrite{
		Name: strings.TrimSpace(r.Name), Direction: r.Direction, Type: r.Type, Priority: prio,
		AccountID: r.AccountID, VendorID: r.VendorID,
		Receiver: textOrNull(patternJSON(r.ToPattern)), Sender: textOrNull(patternJSON(r.FromPattern)),
		Keyword: textOrNull(patternJSON(r.Keyword)), PriorityMatch: textOrNull(patternJSON(r.PriorityPattern)),
		Networks: textOrNull(networksJSON(r.Networks)), Disabled: boolInt(r.Disabled), AcceptUnknown: boolInt(r.AcceptUnknown),
		Plan: textOrNull(plan), Retry: ruleRetry(r.Retry),
		Sell: r.SellCard, Buy: r.BuyCard, DLRSell: r.DLRSellCard, DLRBuy: r.DLRBuyCard,
	})
	if err != nil {
		return Placement{}, fmt.Errorf("create the rule: %w", err)
	}
	out := Placement{RouteID: id, Name: strings.TrimSpace(r.Name)}
	out.Saved = fmt.Sprintf("Added '%s'.", out.Name)
	if r.Direction == "MT" {
		if err := s.nameRules(ctx, before); err != nil {
			return out, err
		}
		_, order, err := s.placeTx(ctx, tx, r.AccountID)
		if err != nil {
			return out, err
		}
		acct, _ := s.planAccount(ctx, r.AccountID)
		names, _, err := s.vendorInfos(ctx)
		if err != nil {
			return out, err
		}
		out = placement(acct.Username, before, order, id, true, names)
	}
	if err := tx.Commit(); err != nil {
		return Placement{}, err
	}
	return out, nil
}

// foldRuleBuy moves an outgoing plan rule's own buy card (buying_rate_card_id,
// an old rule's) onto every vendor of the plan that has no buy price of its
// own for the rule, and clears it. A rule with Main and backups prices each
// vendor's buy on the vendor, and the Admin API stores buying_rate_card_id
// NULL on such a rule (resources.js applySendPlan) — so a card left in the
// column would be dropped by the portal's next save, and the price with it.
// What is charged does not change: the engine's ladder is the vendor's card
// for this rule, then the rule's, then the vendor's default (refdata
// resolveLevels, billing.Rater).
func foldRuleBuy(r *Route) {
	if r.Direction != "MT" || r.Plan == nil || r.BuyCard == 0 {
		return
	}
	p := clonePlan(r.Plan)
	set := func(levels []sendplan.Level) {
		for i := range levels {
			for j := range levels[i].Vendors {
				if levels[i].Vendors[j].BuyRateCardID == 0 {
					levels[i].Vendors[j].BuyRateCardID = r.BuyCard
				}
			}
		}
	}
	set(p.Levels)
	for code := range p.Exceptions {
		set(p.Exceptions[code])
	}
	r.Plan, r.BuyCard = p, 0
}

// anyTakesUnknown stores accept_undetected_network ON for an outgoing rule
// for any destination, as the console always wrote it: the engine reads no
// network filter as every number anyway, and the portal's hidden-rule check
// reads the flag.
func anyTakesUnknown(r *Route) {
	if r.Direction == "MT" && len(cleanCodes(r.Networks)) == 0 {
		r.AcceptUnknown = true
	}
}

// ruleRetry is a rule's own retries as its columns take them (nil = the
// four columns NULL).
func ruleRetry(r *sendplan.Retry) *dal.RuleRetry {
	if r == nil {
		return nil
	}
	return &dal.RuleRetry{Rounds: r.Rounds, FirstSec: r.FirstSec, Backoff: r.Backoff, MaxWaitSec: r.MaxWaitSec}
}

// textOrNull is a value of patternJSON, networksJSON or a plan as its column
// takes it: text, or NULL for nil.
func textOrNull(v any) dal.NullString {
	if s, ok := v.(string); ok {
		return dal.NullString{String: s, Valid: true}
	}
	return dal.NullString{}
}

// checkRoute applies the checks that need the store, to a new rule (old
// nil) or a changed one: vendors, deployments, cards, the account, a short
// code free for an incoming rule, and no second rule for exactly the same
// messages.
func (s Store) checkRoute(ctx context.Context, r Route, old *Route) error {
	var id int64
	if old != nil {
		id = old.ID
	}
	if r.Direction == "MT" && r.Plan != nil {
		pr := r
		if old != nil && pr.Priority == old.Priority {
			pr.Priority = 0 // unchanged: an old clash never blocks an unrelated edit
		}
		if err := s.assertPlan(ctx, pr, id); err != nil {
			return err
		}
	}
	if r.VendorID > 0 {
		ok, err := s.DB.VendorExists(ctx, r.VendorID)
		if err != nil {
			return err
		}
		if !ok {
			return refusef("there is no vendor #%d", r.VendorID)
		}
	}
	ok, err := s.DB.AccountExists(ctx, r.AccountID)
	if err != nil {
		return err
	}
	if !ok {
		return refusef("there is no account #%d", r.AccountID)
	}
	if r.Direction == "MO" {
		// An edit that leaves who owns what alone is not checked, so a clash
		// saved before this rule existed never blocks an unrelated change
		// (the Admin API's assertShortcodeFree).
		same := old != nil && old.AccountID == r.AccountID && old.ToPattern == r.ToPattern && old.Keyword == r.Keyword &&
			sameCodes(cleanCodes(old.Networks), cleanCodes(r.Networks)) && old.VendorID == r.VendorID
		if !same {
			if err := s.shortcodeFree(ctx, r, id); err != nil {
				return err
			}
		}
		// An incoming rule's priority is its own among the account's
		// incoming rules, as the Admin API has it (resources.js
		// assertPriorityFree: a direct rule never shares a priority).
		// Checked when it is new or changed, so an old clash never blocks an
		// unrelated edit.
		if old == nil || old.Priority != r.Priority || old.AccountID != r.AccountID {
			clash, name, err := s.DB.RulePriorityTaken(ctx, "MO", r.AccountID, r.Priority, id)
			if err == nil {
				return refusef("priority %d is already incoming rule #%d %q of this account: the first rule that matches decides, so each rule needs its own priority. Next free: %d",
					r.Priority, clash, name, s.nextMOPriority(ctx, r.AccountID))
			}
			if err != dal.ErrNoRows {
				return err
			}
		}
		// The account must not be served where its vendor does not run
		// (config/0122) — MO too (PO 2026-09-24); an MO rule naming no vendor
		// has nothing to compare.
		if err := s.assertRouteDeployments(ctx, r.AccountID, r.VendorID, r.Direction); err != nil {
			return err
		}
	}
	if err := s.assertRouteCards(ctx, r); err != nil {
		return err
	}
	if r.Direction == "MT" {
		if err := s.assertNoTwin(ctx, r, old); err != nil {
			return err
		}
	}
	return nil
}

// nextMOPriority is the first priority after the account's last incoming
// rule, for a refusal to suggest.
func (s Store) nextMOPriority(ctx context.Context, accountID int64) int {
	max, err := s.DB.MaxIncomingPriority(ctx, accountID)
	if err != nil || !max.Valid {
		return 10
	}
	return int(max.Int64) + 10
}

// assertRouteCards checks the rule's own cards the way SetRouteCard does:
// each exists, is the slot's side (sell or buy), and prices the slot's
// traffic (the rule's direction; DLR for the receipt pair, MT rules only).
func (s Store) assertRouteCards(ctx context.Context, r Route) error {
	for _, c := range []struct {
		slot string
		id   int64
	}{{"sell", r.SellCard}, {"buy", r.BuyCard}, {"dlr_sell", r.DLRSellCard}, {"dlr_buy", r.DLRBuyCard}} {
		if c.id == 0 {
			continue
		}
		var want *RouteCardSlot
		for _, sl := range RouteCardSlots(r.Direction) {
			if sl.Key == c.slot {
				sl := sl
				want = &sl
			}
		}
		if want == nil {
			return refusef("only an outgoing (MT) rule prices receipts: a receipt is charged by the rule of the message it reports on")
		}
		kind, typ, err := s.DB.RuleCardKind(ctx, c.id)
		if err != nil {
			return refusef("there is no rate card #%d", c.id)
		}
		if kind != want.Kind {
			return refusef("card #%d is a %s card; the %s slot is %s, so it takes a %s card", c.id, kind, c.slot, want.Label, want.Kind)
		}
		traffic := strings.ToUpper(r.Direction)
		if strings.HasPrefix(c.slot, "dlr_") {
			traffic = "DLR"
		}
		if typ != "" && !strings.EqualFold(typ, traffic) {
			return refusef("card #%d prices %s traffic, and the %s slot prices %s", c.id, typ, c.slot, traffic)
		}
		if want.Kind == "sell" {
			if err := s.assertSellCardCompany(ctx, c.id, r.AccountID); err != nil {
				return err
			}
		}
	}
	return nil
}

// assertNoTwin refuses a second rule for exactly the same messages (same
// destination, same conditions), even when the other is off: the lower one
// would only ever be a backup nobody can see (rule-creation-ux.md, section
// 5). On an edit, only a change to what the rule catches is checked.
func (s Store) assertNoTwin(ctx context.Context, r Route, old *Route) error {
	me, err := r.planRule()
	if err != nil {
		return err
	}
	var id int64
	if old != nil {
		id = old.ID
		was, err := old.planRule()
		if err == nil && SameCatch(was, me) && was.AcceptUnknown == me.AcceptUnknown {
			return nil
		}
	}
	rules, err := loadRules(ctx, s.DB, r.AccountID, "MT", false)
	if err != nil {
		return err
	}
	for i, x := range rules {
		if x.ID == id || x.PlanError != "" {
			continue
		}
		if SameCatch(x, me) {
			acct, _ := s.planAccount(ctx, r.AccountID)
			return refusef("%s already has a rule for exactly these messages: rule %d '%s'. Change that rule, or add a condition here",
				orUnknown(acct.Username), i+1, x.Name)
		}
	}
	return nil
}

// UpdateRoute changes every part of a rule — name, on/off, destination,
// conditions and changes, the way of choosing, Main, backups, vendors,
// shares and per-vendor prices, the rule's cards and retries — with the
// same checks as CreateRoute. A rule's account and direction never change
// (copy it to another account instead). An outgoing rule is then re-placed
// in its plan, and the answer says where it moved. A shape no longer made
// (several destinations, a receiver condition, network tabs) is kept when
// left exactly as it was; an old rule saved with its one vendor and way
// unchanged stays an old rule.
func (s Store) UpdateRoute(ctx context.Context, id int64, r Route) (Placement, error) {
	old, _, err := s.GetRoute(ctx, id)
	if err != nil {
		return Placement{}, err
	}
	r.ID = id
	if r.AccountID == 0 {
		r.AccountID = old.AccountID
	}
	r.Direction = strings.ToUpper(strings.TrimSpace(r.Direction))
	if r.Direction == "" {
		r.Direction = old.Direction
	}
	if r.Direction != old.Direction {
		return Placement{}, refusef("an %s rule cannot become an %s one: make a new rule", old.Direction, r.Direction)
	}
	// An outgoing rule belongs to its account's plan: it is copied to
	// another account, never moved. An incoming rule may be given to another
	// account (a short code handed to another customer), with every check a
	// new rule of that account gets (checkRoute).
	if r.AccountID != old.AccountID && r.Direction != "MO" {
		return Placement{}, refusef("a rule belongs to its account's plan: copy it to the other account instead of moving it")
	}
	r.Type = "direct"
	if r.Priority <= 0 {
		r.Priority = old.Priority // unchanged: the plan keeps it in its place
	}
	if r.Direction == "MT" {
		if v := r.Plan.FirstVendor(); v > 0 {
			r.VendorID = v
		}
	}
	anyTakesUnknown(&r)
	if err := validateRoute(r, &old); err != nil {
		return Placement{}, err
	}
	if err := s.checkRoute(ctx, r, &old); err != nil {
		return Placement{}, err
	}

	// An old rule (no send plan) saved with the vendor and way it was read as
	// stays old: its route_type and weight keep meaning what they meant, and
	// an old shared tier is not broken by a rename.
	storedPlan, routeType, weight, err := s.DB.RuleStoredPlan(ctx, id)
	if err != nil {
		return Placement{}, err
	}
	keepOld := r.Direction == "MT" && strings.TrimSpace(storedPlan) == "" && old.Plan != nil && planJSON(r.Plan) == planJSON(old.Plan)
	if !keepOld {
		foldRuleBuy(&r) // a rule with Main and backups: the buy card goes on its vendors
	}

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return Placement{}, err
	}
	defer tx.Rollback()
	var before []PlanRule
	if r.Direction == "MT" {
		if before, err = loadRules(ctx, tx, r.AccountID, "MT", true); err != nil {
			return Placement{}, err
		}
	}
	var plan any
	switch {
	case keepOld:
		r.Type, plan = routeType, nil
		r.VendorID = old.VendorID
	case r.Plan != nil:
		plan = planJSON(r.Plan)
		weight = 100
	}
	if r.Direction == "MO" {
		plan = nil
	}
	if err := tx.UpdateRule(ctx, id, dal.RuleWrite{
		Name: strings.TrimSpace(r.Name), Type: r.Type, Weight: weight, Priority: r.Priority,
		AccountID: r.AccountID, VendorID: r.VendorID,
		Receiver: textOrNull(patternJSON(r.ToPattern)), Sender: textOrNull(patternJSON(r.FromPattern)),
		Keyword: textOrNull(patternJSON(r.Keyword)), PriorityMatch: textOrNull(patternJSON(r.PriorityPattern)),
		Networks: textOrNull(networksJSON(r.Networks)), Disabled: boolInt(r.Disabled), AcceptUnknown: boolInt(r.AcceptUnknown),
		Plan: textOrNull(plan), Retry: ruleRetry(r.Retry),
		Sell: r.SellCard, Buy: r.BuyCard, DLRSell: r.DLRSellCard, DLRBuy: r.DLRBuyCard,
	}); err != nil {
		return Placement{}, fmt.Errorf("update rule %d: %w", id, err)
	}
	out := Placement{RouteID: id, Name: strings.TrimSpace(r.Name)}
	if r.Direction == "MT" {
		if err := s.nameRules(ctx, before); err != nil {
			return out, err
		}
		_, order, err := s.placeTx(ctx, tx, r.AccountID)
		if err != nil {
			return out, err
		}
		acct, _ := s.planAccount(ctx, r.AccountID)
		names, _, err := s.vendorInfos(ctx)
		if err != nil {
			return out, err
		}
		out = placement(acct.Username, before, order, id, false, names)
	} else {
		out.Saved = fmt.Sprintf("Saved '%s'.", out.Name)
	}
	if err := tx.Commit(); err != nil {
		return Placement{}, err
	}
	return out, nil
}
