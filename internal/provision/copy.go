package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/sendplan"
)

// Copying a rule (rule-creation-ux.md section 6): to other destinations of
// the same plan and/or into another account's plan. Each copy is its own
// rule. Into another account, a vendor that does not run where it runs is
// dropped and named, and a card of another company goes back to the
// account's default. [SCOPE P14-ROUTE]

// CopyRequest names the copies to make.
type CopyRequest struct {
	RouteID   int64
	ToAccount int64         // 0 = the rule's own account
	To        []Destination // one copy per destination; none = the rule's own destination (another account only)
}

// CopyResult says what was made and what was left behind.
type CopyResult struct {
	Made    []Placement // one per copy made, with its place
	Notes   []string    // what the copies lost on the way, and why
	Skipped []string    // copies not made, and why
}

// CopyTarget is a destination a copy can be made for; Taken says why not.
type CopyTarget struct {
	Destination
	Taken string // "has its own rule: rule 2 'Jazz'" ("" = free)
}

func clonePlan(p *sendplan.Plan) *sendplan.Plan {
	if p == nil {
		return nil
	}
	b, _ := json.Marshal(p)
	var out sendplan.Plan
	_ = json.Unmarshal(b, &out)
	return &out
}

func evenShares(lv *sendplan.Level) {
	n := len(lv.Vendors)
	if n == 0 {
		return
	}
	base := 100 / n
	for i := range lv.Vendors {
		lv.Vendors[i].Pct = base
		if i < 100-base*n {
			lv.Vendors[i].Pct++
		}
	}
}

// CopyDraft is a copy of a rule for another destination and/or account,
// not saved — "Copy and edit", and each copy CopyRoute makes. to.Kind ""
// keeps the rule's own destination. The notes say what the copy lost.
func (s Store) CopyDraft(ctx context.Context, routeID, toAccount int64, to Destination) (Route, []string, error) {
	src, _, err := s.GetRoute(ctx, routeID)
	if err != nil {
		return Route{}, nil, err
	}
	if src.Direction != "MT" {
		return Route{}, nil, refusef("an incoming rule is not copied: a short code belongs to one account. Add an incoming rule for the other account instead")
	}
	if src.Plan == nil {
		return Route{}, nil, refusef("'%s' has no vendors that can be read, so there is nothing to copy. Repair it first", src.Name)
	}
	if toAccount <= 0 {
		toAccount = src.AccountID
	}
	from, err := s.planAccount(ctx, src.AccountID)
	if err != nil {
		return Route{}, nil, err
	}
	tgt, err := s.planAccount(ctx, toAccount)
	if err != nil {
		return Route{}, nil, err
	}
	cat, err := operatorCatalog(ctx, s.DB)
	if err != nil {
		return Route{}, nil, err
	}
	var notes []string
	c := src
	c.ID, c.AccountID, c.Priority = 0, toAccount, 0
	c.Networks = append([]string(nil), src.Networks...)
	p := clonePlan(src.Plan)

	// Where the copy sends.
	srcDest, single := DestinationOf(src.Networks)
	if single {
		srcDest, _ = cat.fill(srcDest)
	}
	srcPR, err := src.planRule()
	if err != nil {
		return Route{}, nil, err
	}
	newDest := srcDest
	moved := false
	if to.Kind != "" {
		if to, err = s.ResolveDestination(ctx, to); err != nil {
			return Route{}, nil, err
		}
		moved = !single || !to.Same(srcDest)
		newDest = to
		c.Networks = to.Networks()
		if to.Kind == DestNetwork {
			c.AcceptUnknown = false
		}
		wasOnly := p.NetworksOnly()
		for _, code := range sortedExceptionCodes(p) {
			if !networksCover(c.Networks, code) {
				delete(p.Exceptions, code)
				notes = append(notes, fmt.Sprintf("its own vendors for network %s are left out: %s does not take that network", code, to.Short()))
			}
		}
		if wasOnly && len(p.Exceptions) == 0 {
			return Route{}, nil, refusef("'%s' sends only for its own networks, and none of them is in %s", src.Name, to.Short())
		}
		// "Remove the country code" was for the old country's numbers.
		if moved {
			m, rep, _ := ParseCondition(src.ToPattern)
			if _, ch := ReadField(FieldReceiver, m, rep); ch != nil && ch.Action == ChangeRemovePrefix && srcDest.CCC != "" && ch.Value == srcDest.CCC {
				if to.CCC != "" {
					ch.Value = to.CCC
					c.ToPattern, _ = FieldToText(FieldReceiver, nil, ch)
				} else {
					c.ToPattern = ""
					notes = append(notes, fmt.Sprintf("the receiver change (remove %s at the start) is left out: it was for %s numbers", srcDest.CCC, srcDest.Short()))
				}
			}
		}
	}

	// Its vendors, where the account runs.
	if toAccount != src.AccountID {
		deps := strings.Join(tgt.Deployments, ", ")
		if deps == "" {
			deps = "every deployment"
		}
		var fitErr error
		keep := func(levels []sendplan.Level) []sendplan.Level {
			for i := range levels {
				var vs []sendplan.Vendor
				for _, v := range levels[i].Vendors {
					if err := s.VendorFits(ctx, toAccount, v.VendorID, "MT"); Refused(err) {
						notes = append(notes, fmt.Sprintf("%s doesn't run where %s runs (%s)", s.vendorLabel(ctx, v.VendorID), tgt.Username, deps))
						continue
					} else if err != nil && fitErr == nil {
						fitErr = err
					}
					vs = append(vs, v)
				}
				levels[i].Vendors = vs
			}
			return levels
		}
		hadMain := len(p.Levels) > 0
		p.Levels = keep(p.Levels)
		mainGone := hadMain && len(p.Levels[0].Vendors) == 0
		for code := range p.Exceptions {
			p.Exceptions[code] = dropEmpty(keep(p.Exceptions[code]))
			if len(p.Exceptions[code]) == 0 {
				delete(p.Exceptions, code)
			}
		}
		p.Levels = dropEmpty(p.Levels)
		if fitErr != nil {
			return Route{}, nil, fitErr
		}
		if len(p.Levels) == 0 && len(p.Exceptions) == 0 {
			return Route{}, notes, refusef("can't copy '%s' into %s: no vendor of it runs where %s runs (%s)", src.Name, tgt.Username, tgt.Username, deps)
		}
		if mainGone && len(p.Levels) > 0 {
			notes = append(notes, fmt.Sprintf("Backup 1 of '%s' becomes its Main", src.Name))
		}
	}

	// Cards of another company go back to the account's default.
	other := func(card int64) bool {
		if card == 0 || tgt.CompanyID == 0 {
			return false
		}
		co, err := s.DB.RuleCardCompany(ctx, card)
		if err != nil {
			return false
		}
		return co != 0 && co != tgt.CompanyID
	}
	coName := from.Company
	if coName == "" {
		coName = "the other company's"
	}
	dropped := false
	if other(c.SellCard) {
		c.SellCard, dropped = 0, true
	}
	if other(c.DLRSellCard) {
		c.DLRSellCard, dropped = 0, true
	}
	fix := func(levels []sendplan.Level) {
		for i := range levels {
			lv := &levels[i]
			for j := range lv.Vendors {
				if other(lv.Vendors[j].SellRateCardID) {
					lv.Vendors[j].SellRateCardID, dropped = 0, true
				}
			}
			if len(lv.Vendors) < 2 {
				lv.Share = false
				for j := range lv.Vendors {
					lv.Vendors[j].Pct = 0
				}
			} else if lv.Share {
				sum := 0
				for _, v := range lv.Vendors {
					sum += v.Pct
				}
				if sum != 100 {
					evenShares(lv)
				}
			}
		}
	}
	fix(p.Levels)
	for code := range p.Exceptions {
		fix(p.Exceptions[code])
	}
	if dropped {
		notes = append(notes, fmt.Sprintf("%s cards go back to %s's default card", coName, tgt.Username))
	}
	several := false
	for _, lv := range p.Levels {
		several = several || len(lv.Vendors) > 1
	}
	for _, lvs := range p.Exceptions {
		for _, lv := range lvs {
			several = several || len(lv.Vendors) > 1
		}
	}
	if !several {
		p.Method = sendplan.Direct // one vendor per level: every way sends the same
	}
	c.Plan = p

	// Its name: made from its choices when the source's was.
	if moved {
		if srcPR.Name == AutoName(srcDest, srcPR.Conds) {
			c.Name = AutoName(newDest, srcPR.Conds)
		} else {
			c.Name = src.Name + " · " + newDest.Short()
		}
	}
	if r := []rune(c.Name); len(r) > 128 {
		c.Name = string(r[:128])
	}
	return c, dedupe(notes), nil
}

func dropEmpty(levels []sendplan.Level) []sendplan.Level {
	var out []sendplan.Level
	for _, lv := range levels {
		if len(lv.Vendors) > 0 {
			out = append(out, lv)
		}
	}
	return out
}

func dedupe(a []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range a {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func (s Store) vendorLabel(ctx context.Context, id int64) string {
	if cid, err := s.DB.RuleVendorCID(ctx, id); err == nil && cid != "" {
		return cid
	}
	return fmt.Sprintf("vendor #%d", id)
}

// CopyTargets lists the destinations a copy of a rule is usually made for
// — the other networks of its country, its country, any destination (a
// special case, or into another account) — each saying when the target plan
// already has the same rule. Any other destination can be named from
// ListDestinations; CopyRoute checks it the same way.
func (s Store) CopyTargets(ctx context.Context, routeID, toAccount int64) ([]CopyTarget, error) {
	src, _, err := s.GetRoute(ctx, routeID)
	if err != nil {
		return nil, err
	}
	if toAccount <= 0 {
		toAccount = src.AccountID
	}
	cat, err := operatorCatalog(ctx, s.DB)
	if err != nil {
		return nil, err
	}
	me, err := src.planRule()
	if err != nil {
		return nil, err
	}
	rules, err := loadRules(ctx, s.DB, toAccount, "MT", false)
	if err != nil {
		return nil, err
	}
	if err := s.nameRules(ctx, rules); err != nil {
		return nil, err
	}
	same := toAccount == src.AccountID
	var dests []Destination
	d, single := DestinationOf(src.Networks)
	if single {
		d, _ = cat.fill(d)
	}
	switch d.Kind {
	case DestNetwork:
		for _, n := range cat.networksOf(d.MCC) {
			if !(same && n.Same(d)) {
				dests = append(dests, n)
			}
		}
		c, _ := cat.fill(Destination{Kind: DestCountry, MCC: d.MCC})
		dests = append(dests, c)
	case DestCountry:
		dests = append(dests, cat.networksOf(d.MCC)...)
		if !same {
			dests = append(dests, d)
		}
	}
	if me.Special() || !same {
		if !(same && d.Kind == DestAny) {
			dests = append(dests, AnyDestination())
		}
	}
	var out []CopyTarget
	for _, dest := range dests {
		cand := me
		cand.Networks = dest.Networks()
		cand.Dest = dest
		t := CopyTarget{Destination: dest}
		for i, x := range rules {
			if x.PlanError == "" && SameCatch(x, cand) {
				what := "has its own rule"
				if me.Special() {
					what = "has the same rule"
				}
				t.Taken = fmt.Sprintf("%s: rule %d '%s'", what, i+1, x.Name)
				break
			}
		}
		out = append(out, t)
	}
	return out, nil
}

// CopyRoute makes the copies: one rule per destination, each placed in its
// plan. A copy the rules refuse (the same rule is already there, no vendor
// runs there) is skipped with the reason; the others are still made.
func (s Store) CopyRoute(ctx context.Context, req CopyRequest) (CopyResult, error) {
	var out CopyResult
	to := req.To
	if len(to) == 0 {
		to = []Destination{{}}
	}
	for _, d := range to {
		label := d.Short()
		if d.Kind == "" {
			label = "the same destination"
		}
		draft, notes, err := s.CopyDraft(ctx, req.RouteID, req.ToAccount, d)
		if err != nil {
			if Refused(err) {
				out.Skipped = append(out.Skipped, label+": "+Reason(err))
				continue
			}
			return out, err
		}
		p, err := s.CreateRoutePlaced(ctx, draft)
		if err != nil {
			if Refused(err) {
				out.Skipped = append(out.Skipped, label+": "+Reason(err))
				continue
			}
			return out, err
		}
		out.Made = append(out.Made, p)
		out.Notes = dedupe(append(out.Notes, notes...))
	}
	return out, nil
}
