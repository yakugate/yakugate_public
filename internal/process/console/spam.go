package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
)

// Spam rules. [SCOPE P14-SPAM] The rulebook is provision/spam.go, the Admin
// API's rules for spam_keywords; this file only asks the questions.

func (p *Process) spamMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		p.printf("\n  Spam rules\n")
		p.printf("  Words or patterns a message may not contain. They apply only to accounts\n")
		if on, total, err := p.store.SpamCheckCounts(ctx); err == nil {
			p.printf("  with the spam check on — %d of %d accounts.\n\n", on, total)
		} else {
			p.printf("  with the spam check on.\n\n")
		}
		choice, ok := p.choose(sc, "Choose", "", []opt{
			item("1", "Add a rule"),
			item("2", "List rules"),
			item("3", "Test a message against the rules"),
			item("0", "Back"),
		})
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.addSpamRule(ctx, sc)
		case "2":
			p.listSpamRules(ctx, sc)
		case "3":
			p.testSpam(ctx, sc)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

func (p *Process) addSpamRule(ctx context.Context, sc *bufio.Scanner) {
	p.printf("\n  Add a spam rule. 0 to abandon.\n")
	var r provision.SpamRule

	companies, err := p.store.ListCompanies(ctx, 200)
	if err != nil {
		p.refusal(err)
		return
	}
	p.printf("\n  Whose messages does it check?\n")
	rows := append(companyRows(companies), opt{Key: "0", Text: "      0  every company — a universal rule"})
	cid, back, ok := p.chooseWithZero(sc, "Company id", rows)
	if !ok || back {
		return
	}
	r.CompanyID = cid

	r.Kind = "blocked"
	if cid != 0 {
		p.printf("\n  A blocked rule catches a message. An allowed rule lets this company send\n")
		p.printf("  wording a universal rule would catch (e.g. a bank that must say \"loan\").\n")
		k, ok := p.askChoice(sc, "Kind", provision.SpamKinds(), "blocked", "blocked = catch it; allowed = let it through")
		if !ok {
			return
		}
		r.Kind = k
	}

	name, ok := p.ask(sc, "Name, e.g. gambling [general]")
	if !ok || name == "0" {
		return
	}
	r.Name = orDefault(name, "general")

	if r.Pattern, ok = p.askPattern(sc); !ok {
		return
	}

	if r.Kind == "blocked" {
		p.printf("\n  What should a match do?\n")
		for _, a := range provision.SpamActions() {
			p.printf("    %-17s %s\n", a, provision.SpamActionWhat(a))
		}
		if r.Action, ok = p.askChoice(sc, "Action", provision.SpamActions(), "reject", ""); !ok {
			return
		}
	}

	p.printf("\n  Only for numbers starting with a dialling prefix, e.g. 92? Enter = every number.\n")
	prefix, ok := p.ask(sc, "Prefix")
	if !ok || prefix == "0" {
		return
	}
	r.Prefix = strings.TrimSpace(prefix)

	id, err := p.store.CreateSpamRule(ctx, r)
	if err != nil {
		p.refusal(err)
		if id == 0 {
			return
		}
	}
	p.okf("\n  Created spam rule #%d %q. A running gateway uses it within 30 seconds.\n", id, r.Name)
}

// askPattern asks for the rule's words or pattern, and offers to try it.
func (p *Process) askPattern(sc *bufio.Scanner) (string, bool) {
	p.printf("\n  Give the words to catch, separated by commas (loan, free credit), or a\n")
	p.printf("  pattern (loan.?offer|free\\s*credit). Case never matters.\n")
	how, ok := p.askChoice(sc, "Words or a pattern", []string{"words", "pattern"}, "words", "")
	if !ok {
		return "", false
	}
	for {
		var pattern string
		if how == "words" {
			w, ok := p.ask(sc, "Words")
			if !ok || w == "0" || w == "" {
				return "", false
			}
			pat, err := provision.WordsToPattern(w)
			if err != nil {
				p.refusal(err)
				continue
			}
			pattern = pat
		} else {
			v, ok := p.ask(sc, "Pattern")
			if !ok || v == "0" || v == "" {
				return "", false
			}
			pattern = v
		}
		if err := provision.CheckSpamPattern(pattern); err != nil {
			p.refusal(err)
			continue
		}
		p.printf("  Pattern: %s\n", pattern)
		p.trySpamPattern(sc, pattern)
		return pattern, true
	}
}

// trySpamPattern lets the operator see what a pattern catches before saving.
func (p *Process) trySpamPattern(sc *bufio.Scanner, pattern string) {
	p.printf("\n  Try it on a message, as many as you like.\n")
	for {
		// The way out is on every prompt, and 0 leaves as it does everywhere
		// else: an operator could not see how to stop (2026-09-21).
		t, ok := p.ask(sc, "Message (Enter or 0 = done)")
		if !ok || strings.TrimSpace(t) == "" || t == "0" {
			return
		}
		if provision.SpamMatches(pattern, t) {
			p.errf("    ✗ caught\n")
		} else {
			p.okf("    ✓ not caught\n")
		}
	}
}

func (p *Process) listSpamRules(ctx context.Context, sc *bufio.Scanner) {
	rules, err := p.store.ListSpamRules(ctx)
	if err != nil {
		p.refusal(err)
		return
	}
	if len(rules) == 0 {
		p.printf("\n  No spam rules yet. Nothing is refused for its wording.\n")
		return
	}
	p.printf("\n   %-6s %-20s %-8s %-14s %-9s %-7s %s\n", "id", "company", "kind", "name", "action", "prefix", "pattern")
	list := []opt{}
	for _, r := range rules {
		action := r.Action
		if r.Kind == "allowed" {
			action = "—"
		}
		pat := r.Pattern
		note := ""
		switch {
		case r.Legacy:
			pat = "(old word list) " + pat
		case pat == "":
			pat = "(empty)"
			note = "          ^ no pattern — this rule catches nothing"
		}
		list = append(list, row(r.ID, fmt.Sprintf("   %-6d %-20s %-8s %-14s %-9s %-7s %s",
			r.ID, trim(r.Company, 20), r.Kind, trim(r.Name, 14), action, orDash(r.Prefix), trim(pat, 40)), note))
	}
	id, ok := p.chooseID(sc, "Open a rule by id (0 to go back)", list)
	if !ok || id == 0 {
		return
	}
	p.spamRuleMenu(ctx, sc, id)
}

func (p *Process) spamRuleMenu(ctx context.Context, sc *bufio.Scanner, id int64) {
	for {
		r, err := p.store.GetSpamRule(ctx, id)
		if err != nil {
			p.refusal(err)
			return
		}
		p.printf("\n  Spam rule #%d %q — %s, %s", r.ID, r.Name, r.Company, r.Kind)
		if r.Kind == "blocked" {
			p.printf(", %s", r.Action)
		}
		if r.Prefix != "" {
			p.printf(", numbers starting %s", r.Prefix)
		}
		p.printf("\n  Pattern: %s\n", orDefault(r.Pattern, "(none)"))
		if r.Legacy {
			p.warnf("  This rule is an old word list; the gateway enforces it as the pattern\n")
			p.warnf("  above. Changing the pattern here replaces the list.\n")
		}
		p.printf("\n")
		choice, ok := p.choose(sc, "Choose", "", []opt{
			item("1", "Change a setting"),
			item("2", "Try it on a message"),
			item("3", "Delete — permanent"),
			item("0", "Back"),
		})
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.editOne(ctx, sc, provision.SpamRuleFields(r.Kind), nil, func(key, val string) error {
				if key == "pattern" {
					if err := provision.CheckSpamPattern(val); err != nil {
						return err
					}
				}
				return p.store.UpdateSpamRule(ctx, id, key, val)
			})
		case "2":
			p.trySpamPattern(sc, r.Pattern)
		case "3":
			p.warnf("\n  Deleting rule %q stops it at once for every account it covered.\n", r.Name)
			if !p.confirm(sc, strconv.FormatInt(r.ID, 10)) {
				p.printf("  Left alone.\n")
				continue
			}
			if err := p.store.DeleteSpamRule(ctx, id); err != nil {
				p.refusal(err)
				continue
			}
			p.okf("  Deleted spam rule #%d. A running gateway drops it within 30 seconds.\n", id)
			return
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

// testSpam lists which rules a message would meet, for one company. It shows
// the rules, not a verdict computed a second way: the engine's own dry run
// (Tools → "Will this send?" in the portal, POST /admin/send/check) is the
// authority on the verdict.
func (p *Process) testSpam(ctx context.Context, sc *bufio.Scanner) {
	companies, err := p.store.ListCompanies(ctx, 200)
	if err != nil {
		p.refusal(err)
		return
	}
	p.printf("\n  Which company's rules? (Universal rules always apply.)\n")
	rows := append(companyRows(companies), opt{Key: "0", Text: "      0  none — universal rules only"})
	cid, back, ok := p.chooseWithZero(sc, "Company id", rows)
	if !ok || back {
		return
	}
	dest, ok := p.ask(sc, "To number (Enter = any)")
	if !ok || dest == "0" {
		return
	}
	text, ok := p.ask(sc, "Message")
	if !ok || strings.TrimSpace(text) == "" {
		return
	}
	rules, err := p.store.ListSpamRules(ctx)
	if err != nil {
		p.refusal(err)
		return
	}
	var allowed, blocked []provision.SpamRule
	for _, r := range rules {
		if r.CompanyID != 0 && r.CompanyID != cid {
			continue
		}
		if r.Prefix != "" && dest != "" {
			if n, err := provision.CleanNumber(dest); err == nil && !strings.HasPrefix(n, r.Prefix) {
				continue
			}
		}
		if r.Pattern == "" || !provision.SpamMatches(r.Pattern, text) {
			continue
		}
		if r.Kind == "allowed" {
			allowed = append(allowed, r)
		} else {
			blocked = append(blocked, r)
		}
	}
	p.printf("\n")
	switch {
	case len(allowed) > 0:
		p.okf("  ✓ An allowed rule matches, so no blocked rule applies:\n")
		for _, r := range allowed {
			p.printf("      #%d %q\n", r.ID, r.Name)
		}
	case len(blocked) == 0:
		p.okf("  ✓ No rule catches it.\n")
	default:
		p.errf("  ✗ Caught by:\n")
		for _, r := range blocked {
			p.printf("      #%d %q (%s) — %s\n", r.ID, r.Name, r.Company, provision.SpamActionWhat(r.Action))
		}
	}
	p.printf("\n  Rules only run for an account with its spam check on. With the strict\n")
	p.printf("  check on, spellings like l0an are caught too; this test does not fold them.\n")
}

// spamCheckLine is the account screen's summary of its spam settings.
func spamCheckLine(d provision.AccountDetail) string {
	switch {
	case !d.SpamCheck:
		return "off — spam rules are not run for this account"
	case d.SpamStrict:
		return "on, strict (also catches l0an, c.a.s.i.n.o)"
	}
	return "on"
}
