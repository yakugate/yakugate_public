package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
)

// Incoming (MO) rules: which account a message arriving from a vendor
// belongs to. They are not part of the routing plan's order — they match a
// short code, and one short code belongs to one account (per vendor, PO
// 2026-09-24) — so they keep a priority of their own. Every part of one can
// be changed, and a refused save keeps what was typed, as for an outgoing
// rule. [SCOPE P14-ROUTE]

func (p *Process) moMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		p.printf("\n  Incoming (MO) rules\n")
		p.printf("  Which account an incoming message belongs to, by the short code it arrives on.\n\n")
		opts := []opt{item("1", "Add an incoming rule"), item("2", "List incoming rules"), item("0", "Back")}
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.addMORule(ctx, sc)
		case "2":
			p.listMORules(ctx, sc)
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

// moEditor is an incoming rule being made or changed.
type moEditor struct {
	p    *Process
	ctx  context.Context
	sc   *bufio.Scanner
	r    provision.Route
	orig *provision.Route // as read (an edit)
	acct string

	vendors []provision.VendorRow
	cards   []provision.RateCardRow
}

func (p *Process) addMORule(ctx context.Context, sc *bufio.Scanner) {
	e := &moEditor{p: p, ctx: ctx, sc: sc, r: provision.Route{Direction: "MO", Type: "direct"}}
	p.printf("\n  Add an incoming rule. Each question can be changed again before saving.\n\n")
	if !e.askName() {
		return
	}
	for _, st := range []func() bool{e.askAccount, e.askVendor, e.askMatch, e.askPriority} {
		if st() {
			continue
		}
		if p.ended {
			return
		}
		p.warnf("  Kept what you entered so far.\n")
		break
	}
	e.run()
}

func (p *Process) listMORules(ctx context.Context, sc *bufio.Scanner) {
	for {
		all, err := p.store.ListRoutes(ctx, 1000)
		if err != nil {
			p.errf("  %v\n", err)
			return
		}
		var rows []opt
		for _, r := range all {
			if r.Direction != "MO" {
				continue
			}
			name := r.Name
			if r.Disabled {
				name += " (off)"
			}
			note := ""
			if r.Trouble != "" {
				note = "          ^ " + r.Trouble
			}
			rows = append(rows, row(r.ID, fmt.Sprintf("   %-6d %-5d %-22s %-14s %-14s %s",
				r.ID, r.Priority, trim(name, 22), trim(r.Account, 14), trim(r.Vendor, 14), trim(r.Matches, 30)), note))
		}
		if len(rows) == 0 {
			p.printf("\n  No incoming rules yet: a message arriving from a vendor belongs to no account.\n")
			return
		}
		p.printf("\n   %-6s %-5s %-22s %-14s %-14s %s\n", "id", "prio", "name", "account", "from vendor", "matches")
		id, ok := p.chooseID(sc, "Open a rule by id (0 to go back)", rows)
		if !ok || id == 0 {
			return
		}
		p.moRuleMenu(ctx, sc, id)
		if p.ended {
			return
		}
	}
}

func (p *Process) moRuleMenu(ctx context.Context, sc *bufio.Scanner, id int64) {
	for {
		r, _, err := p.store.GetRoute(ctx, id)
		if err != nil {
			p.refusal(err)
			return
		}
		if r.Direction != "MO" {
			p.errf("  Rule #%d is an outgoing rule: open it from its account's plan.\n", id)
			return
		}
		e := &moEditor{p: p, ctx: ctx, sc: sc, r: r}
		orig := r
		e.orig = &orig
		state := "on"
		if r.Disabled {
			state = "OFF"
		}
		p.printf("\n  Incoming rule #%d '%s' — %s, priority %d, %s\n", r.ID, r.Name, e.accountName(), r.Priority, state)
		p.printf("   %s\n", e.matchWords())
		opts := []opt{item("1", "Change it — any part of it")}
		if r.Disabled {
			opts = append(opts, item("2", "Turn it on"))
		} else {
			opts = append(opts, item("2", "Turn it off — matching messages fall through to the next rule"))
		}
		opts = append(opts, item("3", "Delete it"), item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			e.run()
		case "2":
			if err := p.store.SetRouteDisabled(ctx, r.ID, !r.Disabled); err != nil {
				p.refusal(err)
				return
			}
			if r.Disabled {
				p.okf("  Turned on '%s'.\n", r.Name)
			} else {
				p.okf("  Turned off '%s'.\n", r.Name)
			}
		case "3":
			effect, err := p.store.DeleteEffect(ctx, r.ID)
			if err != nil {
				p.refusal(err)
				return
			}
			p.warnf("\n  Delete '%s'? %s\n", r.Name, effect)
			del := []opt{item("1", "Delete it")}
			if !r.Disabled {
				del = append(del, item("2", "Turn it off instead — it can be turned on again"))
			}
			del = append(del, item("0", "Keep it"))
			k, ok := p.choose(sc, "Choose", "0", del)
			if !ok {
				return
			}
			switch k {
			case "1":
				yes, ok := p.askYesOK(sc, fmt.Sprintf("Delete '%s' for good", r.Name), false)
				if !ok || !yes {
					p.printf("  Left alone.\n")
					continue
				}
				if err := p.store.DeleteRoute(ctx, r.ID); err != nil {
					p.refusal(err)
					return
				}
				p.okf("  Deleted '%s'.\n", r.Name)
				return
			case "2":
				if !r.Disabled {
					if err := p.store.SetRouteDisabled(ctx, r.ID, true); err != nil {
						p.refusal(err)
						return
					}
					p.okf("  Turned off '%s'.\n", r.Name)
				}
			default:
				p.printf("  Left alone.\n")
			}
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

func (e *moEditor) run() bool {
	saveText, leaveText := "Create rule", "Discard this rule"
	if e.r.ID != 0 {
		saveText, leaveText = "Save rule", "Leave without saving"
	}
	return e.p.runEditor(e.sc, edLoop{
		header:    e.header,
		parts:     e.parts,
		saveText:  saveText,
		leaveText: leaveText,
		save:      e.save,
		leave:     e.leave,
		unchanged: func() bool { return e.orig != nil && !e.dirty() },
		snapshot: func() func() {
			before := e.r
			before.Networks = append([]string(nil), e.r.Networks...)
			return func() { e.r = before }
		},
	})
}

func (e *moEditor) save() error {
	r := e.r
	if r.AccountID <= 0 {
		return errDraft("choose whose rule this is: every account has its own rules")
	}
	if r.ID == 0 {
		id, err := e.p.store.CreateRoute(e.ctx, r)
		if err != nil {
			return err
		}
		e.r.ID = id
		e.p.okf("\n  Created rule #%d %q (incoming). A running gateway uses it within 30 seconds.\n", id, r.Name)
		return nil
	}
	pl, err := e.p.store.UpdateRoute(e.ctx, r.ID, r)
	if err != nil {
		return err
	}
	o := e.r
	e.orig = &o
	e.p.okf("\n  %s A running gateway uses it within 30 seconds.\n", pl.Saved)
	return nil
}

func (e *moEditor) leave() bool {
	if e.r.ID == 0 {
		yes, ok := e.p.askYesOK(e.sc, "Discard everything you entered for this rule", false)
		if yes && ok {
			e.p.printf("  Discarded. Nothing was saved.\n")
		}
		return yes || !ok
	}
	if !e.dirty() {
		return true
	}
	yes, ok := e.p.askYesOK(e.sc, fmt.Sprintf("Leave without saving? Your changes to '%s' are lost", e.orig.Name), false)
	return yes || !ok
}

func (e *moEditor) summaries(r provision.Route) []string {
	return []string{r.Name, e.accountOf(r.AccountID), e.vendorWords(r.VendorID), matchWordsOf(r),
		strconv.Itoa(r.Priority), e.cardWords(r), onWords(!r.Disabled)}
}

func (e *moEditor) dirty() bool {
	if e.orig == nil {
		return true
	}
	a, b := e.summaries(*e.orig), e.summaries(e.r)
	for i := range a {
		if a[i] != b[i] {
			return true
		}
	}
	return false
}

func (e *moEditor) header() {
	p := e.p
	if e.r.ID == 0 {
		p.printf("\n  New incoming rule\n")
	} else {
		p.printf("\n  Incoming rule #%d '%s'\n", e.r.ID, e.orig.Name)
	}
	p.printf("  %s\n", e.matchWords())
	if e.orig != nil && e.dirty() {
		labels := []string{"Name", "Account", "Arrives from", "Matches", "Priority", "Cards", "On"}
		a, b := e.summaries(*e.orig), e.summaries(e.r)
		p.printf("  What changes when you save:\n")
		for i := range a {
			if a[i] != b[i] {
				p.printf("    %s: %s → %s\n", labels[i], a[i], b[i])
			}
		}
	}
}

func (e *moEditor) parts() []edPart {
	r := e.r
	// Every part can be changed, the account too: an incoming rule may be
	// handed to another customer, with the checks a new rule gets.
	out := []edPart{
		{key: "2", label: "Name", now: orDash(r.Name), redo: e.askName, rank: 6, hint: []string{"name"}},
		{key: "3", label: "Account", now: e.accountOf(r.AccountID), redo: e.askAccount, rank: 5,
			hint: []string{"account"}},
	}
	out = append(out,
		edPart{key: "4", label: "Arrives from", now: e.vendorWords(r.VendorID), redo: e.askVendor, rank: 1,
			hint: []string{"vendor", "deployment"}},
		edPart{key: "5", label: "What it matches", now: matchWordsOf(r), redo: e.askMatch, rank: 0,
			hint: []string{"short code", "pattern", "condition", "keyword", "sender", "rewrite", "priority condition"}},
		edPart{key: "6", label: "Priority", now: strconv.Itoa(r.Priority), redo: e.askPriority, rank: 2,
			hint: []string{"priority"}},
		edPart{key: "7", label: "Cards", now: e.cardWords(r), redo: e.editCards, rank: 3,
			hint: []string{"card", "slot"}},
		edPart{key: "8", label: "On or off", now: onWords(!r.Disabled), redo: func() bool {
			e.r.Disabled = !e.r.Disabled
			return true
		}, rank: 7},
	)
	return out
}

func (e *moEditor) accountOf(id int64) string {
	if id <= 0 {
		return "[choose an account]"
	}
	if e.acct != "" && e.r.AccountID == id {
		return e.acct
	}
	name, err := e.p.store.DB.AccountUsername(e.ctx, id)
	if err != nil {
		return fmt.Sprintf("account #%d", id)
	}
	if e.r.AccountID == id {
		e.acct = name
	}
	return name
}

func (e *moEditor) accountName() string { return e.accountOf(e.r.AccountID) }

func (e *moEditor) allVendors() []provision.VendorRow {
	if e.vendors == nil {
		v, err := e.p.store.ListVendors(e.ctx, 1000)
		if err != nil {
			e.p.errf("  %v\n", err)
		}
		e.vendors = v
		if e.vendors == nil {
			e.vendors = []provision.VendorRow{}
		}
	}
	return e.vendors
}

func (e *moEditor) vendorWords(id int64) string {
	if id == 0 {
		return "any vendor"
	}
	for _, v := range e.allVendors() {
		if v.ID == id {
			return v.CID
		}
	}
	return fmt.Sprintf("vendor #%d", id)
}

func matchWordsOf(r provision.Route) string {
	var parts []string
	pat := func(s string) string {
		m, rep, err := provision.ParseCondition(s)
		if err != nil {
			return s
		}
		if rep != "" {
			return m + " (changed to " + rep + ")"
		}
		return m
	}
	if r.ToPattern != "" {
		parts = append(parts, "short code "+pat(r.ToPattern))
	}
	if r.FromPattern != "" {
		parts = append(parts, "from "+pat(r.FromPattern))
	}
	if r.Keyword != "" {
		parts = append(parts, "first word "+pat(r.Keyword))
	}
	if len(parts) == 0 {
		return "anything"
	}
	return strings.Join(parts, " · ")
}

func (e *moEditor) matchWords() string {
	return "Messages matching " + matchWordsOf(e.r) + ", arriving from " + e.vendorWords(e.r.VendorID) + ", belong to " + e.accountName() + "."
}

func (e *moEditor) cardName(id int64) string {
	if e.cards == nil {
		c, _ := e.p.store.ListRateCards(e.ctx, 1000)
		e.cards = c
		if e.cards == nil {
			e.cards = []provision.RateCardRow{}
		}
	}
	for _, c := range e.cards {
		if c.ID == id {
			return c.Name
		}
	}
	return fmt.Sprintf("card #%d", id)
}

func (e *moEditor) cardWords(r provision.Route) string {
	if r.SellCard == 0 && r.BuyCard == 0 {
		return "the defaults"
	}
	var out []string
	if r.SellCard != 0 {
		out = append(out, "sell: "+e.cardName(r.SellCard))
	}
	if r.BuyCard != 0 {
		out = append(out, "buy: "+e.cardName(r.BuyCard))
	}
	return strings.Join(out, " · ")
}

// ── the questions ─────────────────────────────────────────────────────────

func (e *moEditor) askName() bool {
	prompt := "A name for this rule"
	if e.r.Name != "" {
		prompt += " [" + e.r.Name + "]"
	}
	for {
		s, ok := e.p.ask(e.sc, prompt)
		if !ok || s == "0" {
			return false
		}
		if s == "" {
			if e.r.Name == "" {
				e.p.errf("  A rule needs a name, so it can be recognised in a list.\n")
				continue
			}
			return true
		}
		e.r.Name = s
		return true
	}
}

func (e *moEditor) askAccount() bool {
	accounts, err := e.p.store.ListAccounts(e.ctx, 1000)
	if err != nil {
		e.p.errf("  %v\n", err)
		return false
	}
	// Every account has its own rules (Saleem 2026-09-30): a rule for every
	// account is no longer read, so it is not offered.
	e.p.printf("\n  Whose rule is this?\n")
	id, ok := e.p.chooseID(e.sc, "Account id", accountRows(accounts))
	if !ok || id == 0 {
		return false
	}
	e.r.AccountID, e.acct = id, ""
	return true
}

// askVendor asks which vendor the messages arrive from (optional — PO
// 2026-09-24). Two accounts may share a short code when each rule names the
// vendor that delivers it; 0 keeps the rule for every vendor.
func (e *moEditor) askVendor() bool {
	p := e.p
	vendors := e.allVendors()
	p.printf("\n  Only messages arriving from one vendor? 0 = from any vendor.\n")
	if e.r.AccountID > 0 {
		vendors = p.vendorsThatFit(e.ctx, vendors, e.r.AccountID, "MO")
	}
	var list []opt
	for _, v := range vendors {
		list = append(list, row(v.ID, fmt.Sprintf("   %4d  %-16s %-10s %s", v.ID, trim(v.CID, 16), v.Transport, trim(v.Company, 18)), ""))
	}
	list = append(list, opt{Key: "0", Text: "      0  any vendor"})
	vid, back, ok := p.chooseWithZero(e.sc, "Vendor id", list)
	if !ok || back {
		return false
	}
	e.r.VendorID = vid
	return true
}

// askOne asks one pattern: Enter keeps what is there, "-" clears it. Each
// is checked in the engine's own regex language when saved.
func (e *moEditor) askOne(prompt string, cur *string) bool {
	if *cur != "" {
		prompt += " [" + *cur + "] (- clears it)"
	}
	s, ok := e.p.ask(e.sc, prompt)
	if !ok {
		return false
	}
	switch s {
	case "":
	case "-":
		*cur = ""
	default:
		*cur = s
	}
	return true
}

func (e *moEditor) askMatch() bool {
	p := e.p
	p.printf("\n  What should this rule match? Enter on its own means anything.\n")
	p.printf("  These are patterns: ^8005$ is exactly 8005, ^92300 starts with 92300.\n")
	p.printf("  They are checked before the rule is saved.\n")
	p.printf("  To change what the client gets, write /pattern/replacement/: /^0(3\\d+)$/92$1/\n")
	p.printf("  changes the matched part ($1 is the first group). The rule still matches on what arrived.\n")
	r := e.r
	if !e.askOne("Short code the messages arrive on, e.g. ^8005$", &r.ToPattern) {
		return false
	}
	// The SUBSCRIBER's own number (2026-09-24): the engine matches it on MO
	// since internal/mo.Service.match took the source address.
	if !e.askOne("Subscriber's number, if this rule is for particular senders", &r.FromPattern) {
		return false
	}
	if !e.askOne("First word of the message, if this rule is for one keyword", &r.Keyword) {
		return false
	}
	e.r = r
	return true
}

func (e *moEditor) askPriority() bool {
	def := e.r.Priority
	if def <= 0 {
		def = e.p.nextFreePriority(e.ctx, e.r.AccountID, "MO")
	}
	n, ok := e.p.askInt(e.sc, "Priority — lower is tried first", def)
	if !ok {
		return false
	}
	e.r.Priority = int(n)
	return true
}

func (e *moEditor) editCards() bool {
	p := e.p
	re := &ruleEditor{p: p, ctx: e.ctx, sc: e.sc, d: newDraft(e.r.AccountID, e.accountName())}
	for {
		name := func(id int64, def string) string {
			if id == 0 {
				return def
			}
			return e.cardName(id)
		}
		opts := []opt{
			item("1", "Sell card — what the account pays for these messages: "+name(e.r.SellCard, "the account's default")),
			item("2", "Buy card — what the vendor charges us for them: "+name(e.r.BuyCard, "the vendor's default")),
			item("0", "Done"),
		}
		k, ok := p.choose(e.sc, "Change which", "0", opts)
		if !ok {
			return false
		}
		switch k {
		case "0", "":
			return true
		case "1":
			if id, ok := re.pickCard("sell", "MO", "the account's default", e.r.SellCard); ok {
				e.r.SellCard = id
			} else if p.ended {
				return false
			}
		case "2":
			if id, ok := re.pickCard("buy", "MO", "the vendor's default", e.r.BuyCard); ok {
				e.r.BuyCard = id
			} else if p.ended {
				return false
			}
		default:
			p.errf("  %q is not one of the choices.\n", k)
		}
	}
}
