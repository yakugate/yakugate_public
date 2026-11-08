package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// Routing plans. [SCOPE P14-ROUTE]
//
// Every account has its own ROUTING PLAN (Saleem 2026-09-30): its rules in
// order, and the first rule that catches a message sends it. The order is
// kept for the operator — special cases first, then each country's network
// rules above its whole-country rule, then "any destination" — so nobody
// types a position; the screen says where each rule went and why. The
// rules themselves are provision's, the same as the Admin API's (one
// database, two doors); these screens only ask and show. The approved
// design is rule-creation-ux.md; the contract routing-plans.md.
//
// A gateway with accounts and vendors and no plan accepts nothing that can
// be sent (503 NO_ROUTE), so this is the last piece of "enough to work".

func (p *Process) routesMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		p.printf("\n  Routing plans\n")
		p.printf("  Each account has its own plan: its rules, tried in order. Open one to see and change it.\n\n")
		opts := []opt{
			item("1", "Plans — the accounts that have one, and whether each is ready"),
			item("2", "New plan — for an account that has none"),
			item("3", "Incoming (MO) rules — which account an incoming message belongs to"),
			item("0", "Back"),
		}
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.plansList(ctx, sc)
		case "2":
			p.newPlan(ctx, sc)
		case "3":
			p.moMenu(ctx, sc)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
		if p.ended {
			return
		}
	}
}

// ── the list of plans ─────────────────────────────────────────────────────

func rulesWords(s provision.PlanSummary) string {
	out := plural(s.Rules, "1 rule", strconv.Itoa(s.Rules)+" rules")
	if s.Special > 0 {
		out += " · " + plural(s.Special, "1 special case", strconv.Itoa(s.Special)+" special cases")
	}
	if s.Off > 0 {
		out += fmt.Sprintf(" · %d off", s.Off)
	}
	return out
}

func (p *Process) plansList(ctx context.Context, sc *bufio.Scanner) {
	var company int64
	companyName := ""
	for {
		plans, err := p.store.ListPlans(ctx)
		if err != nil {
			p.errf("  %v\n", err)
			return
		}
		var shown []provision.PlanSummary
		for _, s := range plans {
			if company == 0 || s.CompanyID == company {
				shown = append(shown, s)
			}
		}
		switch {
		case len(plans) == 0:
			p.printf("\n  No routing plans yet. An account can send only once it has a plan:\n")
			p.printf("  Routing plans → 2 New plan makes one.\n")
			return
		case len(shown) == 0:
			p.printf("\n  No plan of %s's accounts yet.\n", companyName)
		default:
			title := "Routing plans"
			if company != 0 {
				title += " of " + companyName + "'s accounts"
			}
			p.printf("\n  %s — the first rule of a plan that catches a message sends it.\n\n", title)
			p.printf("   %4s  %-16s %-18s %-30s %s\n", "id", "account", "company", "rules", "status")
		}
		var opts []opt
		for _, s := range shown {
			opts = append(opts, row(s.AccountID, fmt.Sprintf("   %4d  %-16s %-18s %-30s %s",
				s.AccountID, trim(s.Account, 16), trim(s.Company, 18), rulesWords(s), s.Status),
				"         covers: "+s.Covers))
		}
		if company == 0 {
			opts = append(opts, opt{Key: "c", Text: "      c  Only one company's plans"})
		} else {
			opts = append(opts, opt{Key: "c", Text: "      c  Every company's plans again"})
		}
		opts = append(opts, opt{Key: "0", Text: "      0  Back"})
		k, ok := p.choose(sc, "Open a plan by account id (c = a company, 0 = back)", "", opts)
		if !ok || k == "0" || k == "" {
			return
		}
		if strings.EqualFold(k, "c") {
			if company != 0 {
				company, companyName = 0, ""
				continue
			}
			companies, err := p.store.ListCompanies(ctx, 500)
			if err != nil {
				p.errf("  %v\n", err)
				continue
			}
			p.printf("\n  Which company's plans?\n")
			id, ok := p.chooseID(sc, "Company id", companyRows(companies))
			if !ok {
				return
			}
			for _, c := range companies {
				if c.ID == id {
					company, companyName = c.ID, c.Name
				}
			}
			continue
		}
		id, err := strconv.ParseInt(k, 10, 64)
		found := false
		for _, s := range shown {
			found = found || s.AccountID == id
		}
		if err != nil || !found {
			p.errf("  There is no plan for account %q in this list.\n", k)
			continue
		}
		p.planScreen(ctx, sc, id)
		if p.ended {
			return
		}
	}
}

// ── a new plan ────────────────────────────────────────────────────────────

func (p *Process) newPlan(ctx context.Context, sc *bufio.Scanner) {
	without, err := p.store.AccountsWithoutPlan(ctx)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(without) == 0 {
		p.printf("\n  Every account here already has a plan. Open it under Plans to add rules.\n")
		return
	}
	p.printf("\n  New plan. These accounts have no rule yet, so every message of theirs is\n")
	p.printf("  refused (503 NO_ROUTE).\n")
	var rows []opt
	for _, a := range without {
		runs := "every deployment"
		if len(a.Deployments) > 0 {
			runs = strings.Join(a.Deployments, ", ")
		}
		rows = append(rows, row(a.AccountID, fmt.Sprintf("   %4d  %-16s %-20s runs on %s", a.AccountID, trim(a.Account, 16), trim(a.Company, 20), runs), ""))
	}
	id, ok := p.chooseID(sc, "Account id", rows)
	if !ok || id == 0 {
		return
	}
	var acct *provision.PlanSummary
	for i := range without {
		if without[i].AccountID == id {
			acct = &without[i]
		}
	}
	if acct == nil {
		p.errf("  Account #%d is not one of the choices.\n", id)
		return
	}
	p.printf("\n  Where should %s's messages go?\n", acct.Account)
	k, ok := p.choose(sc, "Start with", "1", []opt{
		item("1", "One vendor for every destination — the simplest plan"),
		item("2", "A full rule — a country or a network, conditions, backups"),
		item("3", "A copy of another account's plan"),
		item("0", "Back"),
	})
	if !ok {
		return
	}
	saved := false
	switch k {
	case "1":
		saved = p.oneVendorPlan(ctx, sc, acct.AccountID, acct.Account)
	case "2":
		saved = p.addRule(ctx, sc, acct.AccountID, nil)
	case "3":
		saved = p.copyPlan(ctx, sc, acct.AccountID, acct.Account)
	default:
		return
	}
	if saved && !p.ended {
		p.planScreen(ctx, sc, acct.AccountID)
	}
}

// oneVendorPlan is the first-rule card: one vendor, every destination.
// The draft then opens for saving, with every part still open to change.
func (p *Process) oneVendorPlan(ctx context.Context, sc *bufio.Scanner, accountID int64, account string) bool {
	vendors, err := p.store.ListVendors(ctx, 1000)
	if err != nil {
		p.errf("  %v\n", err)
		return false
	}
	fit := p.vendorsThatFit(ctx, vendors, accountID, "MT")
	if len(fit) == 0 {
		p.errf("  No vendor runs where %s runs. Place one on its deployments under Vendors first.\n", account)
		return false
	}
	p.printf("\n  Send with — every message of %s goes to this vendor.\n", account)
	var list []opt
	for _, v := range fit {
		list = append(list, vendorLine(v))
	}
	vid, ok := p.chooseID(sc, "Vendor id", list)
	if !ok || vid == 0 {
		return false
	}
	d := newDraft(accountID, account)
	d.dest, d.unknown = provision.AnyDestination(), true
	d.plan = &sendplan.Plan{Method: sendplan.Direct, Levels: []sendplan.Level{{Vendors: []sendplan.Vendor{{VendorID: vid}}}}}
	d.whereTouched = true
	return p.finishDraft(ctx, sc, d)
}

// copyPlan copies every rule of another account's plan into this one, each
// as its own rule; a vendor that does not run where this account runs is
// left out and named.
func (p *Process) copyPlan(ctx context.Context, sc *bufio.Scanner, accountID int64, account string) bool {
	plans, err := p.store.ListPlans(ctx)
	if err != nil {
		p.errf("  %v\n", err)
		return false
	}
	var rows []opt
	for _, s := range plans {
		if s.AccountID != accountID {
			rows = append(rows, row(s.AccountID, fmt.Sprintf("   %4d  %-16s %-18s %s", s.AccountID, trim(s.Account, 16), trim(s.Company, 18), rulesWords(s)), ""))
		}
	}
	if len(rows) == 0 {
		p.printf("  No other account has a plan to copy.\n")
		return false
	}
	p.printf("\n  Copy whose plan? Every rule is copied as its own rule.\n")
	src, ok := p.chooseID(sc, "Account id", rows)
	if !ok || src == 0 {
		return false
	}
	v, err := p.store.Plan(ctx, src)
	if err != nil {
		p.refusal(err)
		return false
	}
	var made []string
	var notes, skipped []string
	for _, r := range v.Rules {
		res, err := p.store.CopyRoute(ctx, provision.CopyRequest{RouteID: r.ID, ToAccount: accountID})
		if err != nil {
			p.refusal(err)
			return len(made) > 0
		}
		for _, m := range res.Made {
			made = append(made, "'"+m.Name+"'")
		}
		notes = append(notes, res.Notes...)
		for _, s := range res.Skipped {
			skipped = append(skipped, "'"+r.Name+"' ("+s+")")
		}
	}
	if len(made) > 0 {
		p.okf("\n  Copied %s into %s's plan: %s.\n", plural(len(made), "1 rule", strconv.Itoa(len(made))+" rules"), account, strings.Join(made, ", "))
	}
	for _, n := range dedupeWords(notes) {
		p.warnf("  %s.\n", upper(n))
	}
	for _, s := range skipped {
		p.warnf("  Not copied: %s.\n", s)
	}
	return len(made) > 0
}

func dedupeWords(a []string) []string {
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

func upper(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// ── one plan ──────────────────────────────────────────────────────────────

func (p *Process) printPlan(v provision.PlanView) {
	head := "\n  Routing plan for " + v.Account
	if v.Company != "" {
		head += " — " + v.Company
	}
	if len(v.Deployments) > 0 {
		head += " · runs on " + strings.Join(v.Deployments, ", ")
	}
	p.printf("%s\n", head)
	switch v.Level {
	case "ok":
		p.okf("  Status: %s\n", v.Status)
	case "bad":
		p.errf("  Status: %s\n", v.Status)
	default:
		p.warnf("  Status: %s\n", v.Status)
	}
	for _, b := range v.Banners {
		if strings.Contains(b, "refused (503") && v.Level == "bad" {
			p.errf("  %s\n", b)
		} else {
			p.warnf("  %s\n", b)
		}
	}
	if v.Coverage != "" {
		p.printf("  Covers: %s\n", v.Coverage)
	}
	for _, g := range v.Groups {
		p.printf("\n  %s · %s\n", g.Title, g.Note)
		for _, i := range g.Rules {
			p.printRule(v.Rules[i])
		}
		if len(g.Uncovered) > 0 {
			var names []string
			for k, d := range g.Uncovered {
				if k == 6 {
					names = append(names, fmt.Sprintf("and %d more", len(g.Uncovered)-6))
					break
				}
				names = append(names, d.Short())
			}
			p.printf("       No rule of their own: %s. %s\n", strings.Join(names, ", "), g.UsesNote)
		}
		if g.Missing != "" {
			p.warnf("       %s\n", g.Missing)
		}
	}
	if n := len(v.MO); n > 0 {
		p.printf("\n  Incoming rules: %d — under Routing plans → 3 Incoming (MO) rules.\n", n)
	}
}

func (p *Process) printRule(r provision.RuleView) {
	badges := ""
	if len(r.Badges) > 0 {
		badges = "  [" + strings.Join(r.Badges, "] [") + "]"
	}
	p.printf("   %2d  %s%s\n", r.Position, r.Name, badges)
	what := r.Where
	if r.Conditions != "" {
		what = r.Conditions + " · for " + r.Where
	}
	p.printf("       %s → %s\n", what, orDash(r.Sends))
	if r.FallsBack != "" {
		p.printf("       if they can't send %s\n", r.FallsBack)
	}
	if r.HiddenNote != "" {
		p.warnf("       Hidden: %s\n", r.HiddenNote)
	}
	if r.Overlap != "" {
		p.printf("       %s\n", r.Overlap)
	}
}

func specialCount(v provision.PlanView) int {
	n := 0
	for _, r := range v.Rules {
		if r.Special {
			n++
		}
	}
	return n
}

func (p *Process) planScreen(ctx context.Context, sc *bufio.Scanner, accountID int64) {
	for {
		v, err := p.store.Plan(ctx, accountID)
		if err != nil {
			p.refusal(err)
			return
		}
		p.printPlan(v)
		opts := []opt{item("1", "Add a rule")}
		if len(v.Rules) > 0 {
			opts = append(opts,
				item("2", "Edit a rule — any part of it"),
				item("3", "Copy a rule — to other networks, countries or accounts"),
				item("4", "Turn a rule on or off"),
				item("5", "Delete a rule"),
				item("6", "Test a number — which rule catches it, which vendor sends it"))
		}
		if !v.InOrder {
			opts = append(opts, item("7", "Put in order — see the moves first"))
		}
		if specialCount(v) > 1 {
			opts = append(opts, item("8", "Move a special case up or down"))
		}
		opts = append(opts, item("0", "Back"))
		p.printf("\n")
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.addRule(ctx, sc, accountID, nil)
		case "2":
			if r, ok := p.pickRule(sc, v, "Edit which rule"); ok {
				p.editRule(ctx, sc, r.ID)
			}
		case "3":
			if r, ok := p.pickRule(sc, v, "Copy which rule"); ok {
				p.copyRule(ctx, sc, v, r)
			}
		case "4":
			if r, ok := p.pickRule(sc, v, "Turn which rule on or off"); ok {
				p.switchRule(ctx, sc, v, r)
			}
		case "5":
			if r, ok := p.pickRule(sc, v, "Delete which rule"); ok {
				p.deleteRule(ctx, sc, v, r)
			}
		case "6":
			p.testNumber(ctx, sc, v)
		case "7":
			p.putInOrder(ctx, sc, v)
		case "8":
			p.moveSpecial(ctx, sc, v)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
		if p.ended {
			return
		}
	}
}

// pickRule asks for a rule by its number in the plan.
func (p *Process) pickRule(sc *bufio.Scanner, v provision.PlanView, prompt string) (provision.RuleView, bool) {
	var rows []opt
	for _, r := range v.Rules {
		off := ""
		if !r.On {
			off = "  (off)"
		}
		rows = append(rows, row(int64(r.Position), fmt.Sprintf("   %2d  %-28s %s%s", r.Position, trim(r.Name, 28), r.Where, off), ""))
	}
	n, ok := p.chooseID(sc, prompt+" — its number (0 = back)", rows)
	if !ok || n == 0 {
		return provision.RuleView{}, false
	}
	if n < 1 || int(n) > len(v.Rules) {
		p.errf("  There is no rule %d in this plan.\n", n)
		return provision.RuleView{}, false
	}
	return v.Rules[n-1], true
}

// switchRule turns a rule on or off, saying what that does; turning off the
// last rule that is on asks first.
func (p *Process) switchRule(ctx context.Context, sc *bufio.Scanner, v provision.PlanView, r provision.RuleView) {
	on := !r.On
	text, ask := v.SwitchEffect(r.ID, on)
	if ask {
		p.warnf("  %s\n", text)
		yes, ok := p.askYesOK(sc, "Turn it off", false)
		if !ok || !yes {
			p.printf("  Left on.\n")
			return
		}
		text = fmt.Sprintf("Turned off '%s'.", r.Name)
	}
	if err := p.store.SetRouteDisabled(ctx, r.ID, !on); err != nil {
		p.refusal(err)
		return
	}
	p.okf("  %s\n", text)
}

// deleteRule says where the rule's messages go before deleting it, and
// offers to turn it off instead.
func (p *Process) deleteRule(ctx context.Context, sc *bufio.Scanner, v provision.PlanView, r provision.RuleView) {
	p.warnf("\n  Delete '%s'? %s\n", r.Name, v.DeleteEffect(r.ID))
	opts := []opt{item("1", "Delete it")}
	if r.On {
		opts = append(opts, item("2", "Turn it off instead — it can be turned on again"))
	}
	opts = append(opts, item("0", "Keep it"))
	k, ok := p.choose(sc, "Choose", "0", opts)
	if !ok {
		return
	}
	switch k {
	case "1":
		yes, ok := p.askYesOK(sc, fmt.Sprintf("Delete '%s' for good", r.Name), false)
		if !ok || !yes {
			p.printf("  Left alone.\n")
			return
		}
		if err := p.store.DeleteRoute(ctx, r.ID); err != nil {
			p.refusal(err)
			return
		}
		p.okf("  Deleted '%s'.\n", r.Name)
	case "2":
		if r.On {
			p.switchRule(ctx, sc, v, r)
		}
	default:
		p.printf("  Left alone.\n")
	}
}

// putInOrder lists the moves and what they change, then puts the plan in
// the usual order in one go.
func (p *Process) putInOrder(ctx context.Context, sc *bufio.Scanner, v provision.PlanView) {
	if v.InOrder {
		p.printf("  %s's plan is already in the usual order.\n", v.Account)
		return
	}
	p.printf("\n  Put %s's plan in the usual order: special cases first, then each country's\n", v.Account)
	p.printf("  network rules above its whole-country rule, then any destination last.\n")
	for _, m := range v.Moves {
		p.printf("    %s\n", m.Words())
	}
	for _, e := range v.Effects {
		p.warnf("    %s\n", e)
	}
	p.printf("  Nothing else changes.\n")
	yes, ok := p.askYesOK(sc, "Put it in order", true)
	if !ok || !yes {
		p.printf("  Kept as it is.\n")
		return
	}
	res, err := p.store.PlaceRules(ctx, v.AccountID)
	if err != nil {
		p.refusal(err)
		return
	}
	if !res.Written {
		p.printf("  It was already in order.\n")
		return
	}
	p.okf("  Put in order: %s changed place.\n", plural(len(res.Moves), "1 rule", strconv.Itoa(len(res.Moves))+" rules"))
}

// moveSpecial moves a special case one place up or down: the only rules
// whose order is the operator's.
func (p *Process) moveSpecial(ctx context.Context, sc *bufio.Scanner, v provision.PlanView) {
	var rows []opt
	for _, r := range v.Rules {
		if r.Special {
			rows = append(rows, row(int64(r.Position), fmt.Sprintf("   %2d  %-28s %s", r.Position, trim(r.Name, 28), r.Conditions), ""))
		}
	}
	n, ok := p.chooseID(sc, "Move which special case — its number (0 = back)", rows)
	if !ok || n == 0 || int(n) > len(v.Rules) || !v.Rules[n-1].Special {
		return
	}
	r := v.Rules[n-1]
	k, ok := p.choose(sc, "Which way", "1", []opt{item("1", "Up — checked earlier"), item("2", "Down — checked later"), item("0", "Back")})
	if !ok || (k != "1" && k != "2") {
		return
	}
	res, err := p.store.MoveSpecial(ctx, r.ID, k == "1")
	if err != nil {
		p.refusal(err)
		return
	}
	var ws []string
	for _, m := range res.Moves {
		ws = append(ws, m.Words())
	}
	p.okf("  Moved: %s.\n", strings.Join(ws, ", "))
}

// ── test a number ─────────────────────────────────────────────────────────

// planUses says which message fields a plan's conditions test, so a test
// asks only for those.
func planUses(v provision.PlanView) map[string]bool {
	uses := map[string]bool{}
	for _, r := range v.Rules {
		for _, c := range r.Conds {
			uses[c.Field] = true
		}
	}
	return uses
}

// askTestInput asks a number, and the sender, text and priority when a
// rule tests them. ok=false: backed out.
func (p *Process) askTestInput(sc *bufio.Scanner, uses map[string]bool) (provision.TestInput, bool) {
	var in provision.TestInput
	num, ok := p.ask(sc, "Number, with its country code (0 = back)")
	if !ok || num == "0" || num == "" {
		return in, false
	}
	in.Number = num
	if !uses[provision.FieldSender] && !uses[provision.FieldText] && !uses[provision.FieldPriority] {
		// No rule tests them: offered, not asked (the prototype's "+ Sender,
		// text, priority").
		yes, ok := p.askYesOK(sc, "Add a sender, text or priority too", false)
		if !ok {
			return in, false
		}
		if yes {
			uses = map[string]bool{provision.FieldSender: true, provision.FieldText: true, provision.FieldPriority: true}
		}
	}
	if uses[provision.FieldSender] {
		if in.Sender, ok = p.ask(sc, "Sender (Enter = the account's default sender)"); !ok {
			return in, false
		}
	}
	if uses[provision.FieldText] {
		if in.Text, ok = p.ask(sc, "Text (Enter = not typed: a text condition is not judged)"); !ok {
			return in, false
		}
	}
	if uses[provision.FieldPriority] {
		for {
			s, ok := p.ask(sc, "Priority 1 to 9 (Enter = not sent, counts as 9)")
			if !ok {
				return in, false
			}
			if s == "" {
				break
			}
			n, err := strconv.Atoi(s)
			if err != nil || n < 1 || n > 9 {
				p.errf("  A priority is 1 to 9; 1 is the most urgent.\n")
				continue
			}
			in.Priority = n
			break
		}
	}
	return in, true
}

// printTest prints a number test, its headline coloured by what it means.
func (p *Process) printTest(t provision.NumberTest) {
	p.printf("\n")
	for _, l := range t.Lines {
		switch {
		case l == t.Headline && t.Level == "ok":
			p.okf("  %s\n", l)
		case l == t.Headline && t.Level == "bad":
			p.errf("  %s\n", l)
		case l == t.Headline:
			p.warnf("  %s\n", l)
		default:
			p.printf("  %s\n", l)
		}
	}
}

func (p *Process) testNumber(ctx context.Context, sc *bufio.Scanner, v provision.PlanView) {
	p.printf("\n  Test a number against %s's plan: which rule catches it and which vendor sends it.\n", v.Account)
	p.printf("  Nothing is sent. The network comes from the operators' number series.\n")
	in, ok := p.askTestInput(sc, planUses(v))
	if !ok {
		return
	}
	res, err := p.store.TestNumberWith(ctx, v.AccountID, in)
	if err != nil {
		p.refusal(err)
		return
	}
	p.printTest(res)
	if res.AddFor != nil {
		yes, ok := p.askYesOK(sc, "Add a rule for "+res.AddFor.Short()+" now", false)
		if ok && yes {
			p.addRule(ctx, sc, v.AccountID, res.AddFor)
		}
	}
}

// ── copy a rule ───────────────────────────────────────────────────────────

func (p *Process) copyRule(ctx context.Context, sc *bufio.Scanner, v provision.PlanView, r provision.RuleView) {
	p.printf("\n  Copy '%s'. Each copy is its own rule: changing one later does not change the others.\n", r.Name)
	k, ok := p.choose(sc, "Into the plan of", "1", []opt{
		item("1", v.Account+" (this plan)"),
		item("2", "Another account"),
		item("0", "Back"),
	})
	if !ok || (k != "1" && k != "2") {
		return
	}
	into, intoName := v.AccountID, v.Account
	if k == "2" {
		accounts, err := p.store.ListAccounts(ctx, 1000)
		if err != nil {
			p.errf("  %v\n", err)
			return
		}
		var rows []opt
		for _, a := range accounts {
			if a.ID != v.AccountID {
				rows = append(rows, row(a.ID, fmt.Sprintf("   %4d  %-16s %s", a.ID, trim(a.Username, 16), trim(a.Company, 24)), ""))
			}
		}
		id, ok := p.chooseID(sc, "Account id", rows)
		if !ok || id == 0 {
			return
		}
		for _, a := range accounts {
			if a.ID == id {
				into, intoName = a.ID, a.Username
			}
		}
		if into == v.AccountID {
			p.errf("  Account #%d is not one of the choices.\n", id)
			return
		}
	}
	targets, err := p.store.CopyTargets(ctx, r.ID, into)
	if err != nil {
		p.refusal(err)
		return
	}
	// Choosing the destinations is asked again whenever a copy is refused
	// for its destination: nothing chosen is lost until the operator goes
	// back on purpose (Saleem 2026-09-30).
	for {
		dests, ok := p.askCopyDests(ctx, sc, targets, into, intoName)
		if !ok {
			return
		}
		what := plural(len(dests), "Make the copy", "Make "+strconv.Itoa(len(dests))+" copies")
		k, ok = p.choose(sc, "Choose", "1", []opt{
			item("1", what),
			item("2", "Copy and edit — the first one opens in the editor; nothing is saved until you create it"),
			item("0", "Cancel"),
		})
		if !ok {
			return
		}
		switch k {
		case "1":
			again := p.makeCopies(ctx, sc, r, into, intoName, dests)
			if len(again) == 0 || p.ended {
				return
			}
			p.warnf("  Nothing is lost: choose another destination for %s, or 0 to leave it.\n", strings.Join(again, ", "))
			continue
		case "2":
			draft, notes, err := p.store.CopyDraft(ctx, r.ID, into, dests[0])
			if err != nil {
				p.refusal(err)
				if !provision.Refused(err) {
					return
				}
				p.warnf("  Nothing is lost: choose another destination, or 0 to leave it.\n")
				continue
			}
			d := p.draftFromRoute(ctx, draft, intoName, nil)
			d.id, d.copyOf = 0, r.Name
			d.copyNamed(r.Name)
			for _, n := range notes {
				d.notes = append(d.notes, upper(n)+".")
			}
			// A copy is new: every field is written from what it reads as.
			d.whereTouched = true
			p.finishDraft(ctx, sc, d)
		}
		return
	}
}

// askCopyDests asks which destinations to make copies for: numbers from
// the list, or s to search. ok=false: backed out (0) or the input closed.
func (p *Process) askCopyDests(ctx context.Context, sc *bufio.Scanner, targets []provision.CopyTarget, into int64, intoName string) ([]provision.Destination, bool) {
	for {
		p.printf("\n  Make copies for — into %s's plan:\n", intoName)
		for i, t := range targets {
			line := fmt.Sprintf("   %2d  %s", i+1, t.Label())
			if t.Taken != "" {
				line += "   — " + t.Taken
			}
			p.printf("%s\n", line)
		}
		p.printf("    s  Another destination (search)\n")
		s, ok := p.ask(sc, "Which (numbers, comma separated; s = search; 0 = back)")
		if !ok || s == "0" || s == "" {
			return nil, false
		}
		var dests []provision.Destination
		bad := ""
		for _, x := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' }) {
			if strings.EqualFold(x, "s") {
				e := &ruleEditor{p: p, ctx: ctx, sc: sc, d: newDraft(into, intoName), pickOnly: true}
				if pv, err := p.store.Plan(ctx, into); err == nil {
					e.view = pv
				}
				if e.askDest() {
					dests = append(dests, e.d.dest)
				} else if p.ended {
					return nil, false
				}
				continue
			}
			n, err := strconv.Atoi(x)
			if err != nil || n < 1 || n > len(targets) {
				bad = x
				break
			}
			if t := targets[n-1]; t.Taken != "" {
				p.warnf("  %s %s: that copy will be refused.\n", t.Short(), t.Taken)
			}
			dests = append(dests, targets[n-1].Destination)
		}
		if bad != "" {
			p.errf("  %q is not one of the choices.\n", bad)
			continue
		}
		if len(dests) > 0 {
			return dests, true
		}
	}
}

// makeCopies makes one copy per destination, each placed in its plan (as
// provision.CopyRoute does). A copy the rules refuse keeps its draft: it
// opens in the editor on the part the refusal is about, and is dropped
// only when the operator says so twice (Saleem 2026-09-30: "don't lose the
// data"). It returns the destinations no copy could even be drafted for
// (the refusal is about the destination), so they can be chosen again.
func (p *Process) makeCopies(ctx context.Context, sc *bufio.Scanner, r provision.RuleView, into int64, intoName string, dests []provision.Destination) []string {
	type refusedCopy struct {
		label string
		draft provision.Route
		notes []string
		err   error
	}
	var made []string
	var notes []string
	var refused []refusedCopy
	var again []string
	for _, dest := range dests {
		draft, dn, err := p.store.CopyDraft(ctx, r.ID, into, dest)
		if err != nil {
			if !provision.Refused(err) {
				p.refusal(err)
				return nil
			}
			p.errf("  Not copied — %s: %s\n", dest.Short(), provision.Reason(err))
			again = append(again, dest.Short())
			continue
		}
		pl, err := p.store.CreateRoutePlaced(ctx, draft)
		if err != nil {
			if !provision.Refused(err) {
				p.refusal(err)
				return nil
			}
			refused = append(refused, refusedCopy{dest.Short(), draft, dn, err})
			continue
		}
		made = append(made, fmt.Sprintf("'%s' (rule %d)", pl.Name, pl.Position))
		notes = append(notes, dn...)
	}
	if len(made) > 0 {
		p.okf("  Added %s: %s.\n", plural(len(made), "1 rule", strconv.Itoa(len(made))+" rules"), strings.Join(made, ", "))
		p.printf("  A running gateway uses them within 30 seconds.\n")
	}
	for _, n := range dedupeWords(notes) {
		p.warnf("  %s.\n", upper(n))
	}
	for _, c := range refused {
		p.errf("  Not copied — %s: %s\n", c.label, provision.Reason(c.err))
		open, ok := p.keepRefusedCopy(sc, c.label)
		if !ok {
			return nil
		}
		if !open {
			continue
		}
		d := p.draftFromRoute(ctx, c.draft, intoName, nil)
		d.id, d.copyOf, d.whereTouched = 0, r.Name, true
		d.copyNamed(r.Name)
		for _, n := range c.notes {
			d.notes = append(d.notes, upper(n)+".")
		}
		p.finishRefused(ctx, sc, d, c.err)
		if p.ended {
			return nil
		}
	}
	return again
}

// keepRefusedCopy asks whether to open a refused copy in the editor. It is
// dropped only when the operator says no and then confirms it.
// ok=false: the input closed.
func (p *Process) keepRefusedCopy(sc *bufio.Scanner, label string) (open, ok bool) {
	for {
		yes, ok := p.askYesOK(sc, "Open this copy in the editor, to change what was refused and save it", true)
		if !ok {
			return false, false
		}
		if yes {
			return true, true
		}
		drop, ok := p.askYesOK(sc, "Drop the copy for "+label+"? Nothing of it is saved", false)
		if !ok {
			return false, false
		}
		if drop {
			p.printf("  Dropped the copy for %s.\n", label)
			return false, true
		}
	}
}

// ── shared pieces ─────────────────────────────────────────────────────────

// nextFreePriority is the first position after the account's last rule in
// this direction (10 for its first), so the offered default is never taken.
// Outgoing rules are placed for the operator; incoming ones keep a priority.
func (p *Process) nextFreePriority(ctx context.Context, accountID int64, direction string) int {
	max, err := p.store.DB.MaxRulePriority(ctx, accountID, direction)
	if err != nil || !max.Valid {
		return 10
	}
	return int(max.Int64) + 10
}

// askRetry asks for a rule's own retries, until they are ones the gateway
// takes. ok=false: backed out, nothing changed.
func (p *Process) askRetry(sc *bufio.Scanner) (*sendplan.Retry, bool) {
	d := sendplan.DefaultRetry
	for {
		n, ok := p.askInt(sc, "Retries after the first round (0 to 50)", d.Rounds)
		if !ok {
			return nil, false
		}
		first, ok := p.askInt(sc, "First wait, in seconds", d.FirstSec)
		if !ok {
			return nil, false
		}
		r := &sendplan.Retry{Rounds: int(n), FirstSec: int(first), Backoff: "same", MaxWaitSec: int(first)}
		double, ok := p.askYesOK(sc, "Double the wait each round", true)
		if !ok {
			return nil, false
		}
		if double {
			mx, ok := p.askInt(sc, "Longest wait, in seconds", d.MaxWaitSec)
			if !ok {
				return nil, false
			}
			r.Backoff, r.MaxWaitSec = "double", int(mx)
		}
		if err := r.Validate(); err != nil {
			p.errf("  %v\n", err)
			continue
		}
		p.printf("  %s\n", provision.RetryWords(r))
		return r, true
	}
}

// vendorsThatFit leaves out the vendors a rule for this account could not
// name — the account served on a gateway the vendor does not run on — and
// says which and why, as the portal's picker does. A shorter list with no
// explanation reads as a bug. The save refuses them anyway; this only keeps
// the choice that cannot work off the screen. A blackhole vendor is judged
// like any other (2026-10-02).
func (p *Process) vendorsThatFit(ctx context.Context, vendors []provision.VendorRow, accountID int64, direction string) []provision.VendorRow {
	if accountID <= 0 {
		return vendors
	}
	var fit []provision.VendorRow
	var left []string
	for _, v := range vendors {
		if err := p.store.VendorFits(ctx, accountID, v.ID, direction); provision.Refused(err) {
			left = append(left, v.CID)
			continue
		}
		fit = append(fit, v)
	}
	if len(left) > 0 {
		p.warnf("  Not listed — they do not run on every deployment this account is served on: %s\n",
			strings.Join(left, ", "))
	}
	return fit
}
