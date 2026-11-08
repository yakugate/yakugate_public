package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/provision"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
)

// The gateway-wide default receipt policy (issue 23, 2026-10-27): what a
// client is told where its account's own receipt policy has no rule for the
// case. Gateway → 6. The rules are provision's (receiptdefault.go), which are
// the gateway's own parser and the Admin API's: one database, two doors.
// Rule by rule, never as JSON; a split or a rule that replaces a receipt is
// shown and kept, and set in the portal.

func (p *Process) receiptDefaultMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		def, err := p.store.ReceiptPolicyDefault(ctx)
		if err != nil {
			p.errf("  %v\n", err)
			return
		}
		p.printf("\n  Default receipt policy — what a client is told where its account's own policy has no rule.\n")
		p.printf("  An account's own rule wins; where neither has one the client gets the standard word.\n\n")
		p.printf("  The gateway's own outcomes, as they are told now:\n")
		for _, o := range []struct{ label, code string }{
			{"Validity over before it was sent", receiptpolicy.CodeValidityExpired},
			{"Its tries ran out", receiptpolicy.CodeMaxAttempts},
			{"A vendor refused it", ""},
		} {
			a := dlrsvc.GatewayAnswer((*receiptpolicy.Policy)(nil).Over(def), o.code)
			by := "the standard word"
			if a.Told != nil {
				by = "the default (" + strings.TrimPrefix(a.Told.Rule, receiptpolicy.DefaultPrefix) + ")"
			}
			errText := a.Err
			if errText == "" {
				errText = "000"
				if o.code == "" && a.Told == nil {
					errText = "the vendor's code"
				}
			}
			p.printf("   %-34s %-8s err %-18s %s\n", o.label, a.Stat, errText, by)
		}
		lines := provision.PolicyLines(def)
		if len(lines) == 0 {
			p.printf("\n  No default is set.\n\n")
		} else {
			p.printf("\n  Its rules:\n")
			for _, l := range lines {
				p.printf("   • %s\n", l)
			}
			p.printf("\n")
		}
		opts := []opt{item("1", "Set a rule")}
		if def != nil {
			opts = append(opts, item("2", "Remove a rule"), item("3", "Remove the whole default"))
		}
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok || choice == "0" {
			return
		}
		switch choice {
		case "1":
			p.setDefaultRule(ctx, sc, def)
		case "2":
			if def != nil {
				p.removeDefaultRule(ctx, sc, def)
			}
		case "3":
			if def != nil && p.confirmWord(sc, "Remove the whole default? Type yes") {
				p.saveDefault(ctx, nil)
			}
		case "":
		default:
			p.errf("  %q is not one of the choices.\n\n", choice)
		}
	}
}

// setDefaultRule asks for one rule and saves the default with it.
func (p *Process) setDefaultRule(ctx context.Context, sc *bufio.Scanner, def *receiptpolicy.Policy) {
	rules := provision.PolicyRules()
	opts := make([]opt, 0, len(rules)+1)
	for i, r := range rules {
		opts = append(opts, item(strconv.Itoa(i+1), r.Label))
	}
	opts = append(opts, item("0", "Back"))
	pick, ok := p.choose(sc, "Which rule", "", opts)
	n, _ := strconv.Atoi(pick)
	if !ok || n < 1 || n > len(rules) {
		return
	}
	rule := rules[n-1]
	p.printf("  %s\n", rule.Hint)
	code := ""
	if rule.Key == "submit_code" {
		v, ok := p.askName(sc, "The vendor's refusal code (as the record has it), or * for the rest")
		if !ok || v == "" {
			return
		}
		code = v
	}
	var a provision.PolicyAnswer
	if rule.Key == "no_receipt" {
		m, ok := p.askInt(sc, "Wait this many minutes first", 60)
		if !ok {
			return
		}
		a.AfterMin = int(m)
	}
	word, ok := p.chooseWord(sc, rule.FinalOnly)
	if !ok {
		return
	}
	a.Word = word
	if receiptpolicy.Final(word) && rule.Key != "progress" {
		v, ok := p.ask(sc, "Error code sent (Enter = none, 000)")
		if !ok || v == "0" {
			return
		}
		a.Err = v
	}
	if rule.Key != "no_receipt" {
		lo, ok := p.askInt(sc, "Hold the receipt back at least (seconds)", 0)
		if !ok {
			return
		}
		hi, ok := p.askInt(sc, "…and at most (seconds; 0 = the same)", 0)
		if !ok {
			return
		}
		a.DelayMin, a.DelayMax = int(lo), int(hi)
	}
	next, err := provision.SetPolicyRule(def, rule.Key, code, a)
	if err != nil {
		p.refusal(err)
		return
	}
	p.saveDefault(ctx, next)
}

// chooseWord offers the eight receipt words, or the six final ones.
func (p *Process) chooseWord(sc *bufio.Scanner, finalOnly bool) (string, bool) {
	var opts []opt
	for i, w := range receiptpolicy.Words {
		if finalOnly && !receiptpolicy.Final(w) {
			continue
		}
		label := w + "  (final — closes the message)"
		if !receiptpolicy.Final(w) {
			label = w + "  (progress — the message stays open)"
		}
		opts = append(opts, item(strconv.Itoa(i+1), label))
	}
	opts = append(opts, item("0", "Back"))
	pick, ok := p.choose(sc, "Tell the client", "", opts)
	n, _ := strconv.Atoi(pick)
	if !ok || n < 1 || n > len(receiptpolicy.Words) {
		return "", false
	}
	return receiptpolicy.Words[n-1], true
}

// removeDefaultRule takes one rule out of the default.
func (p *Process) removeDefaultRule(ctx context.Context, sc *bufio.Scanner, def *receiptpolicy.Policy) {
	type target struct {
		key, code string
		index     int
	}
	var targets []target
	var opts []opt
	add := func(t target, label string) {
		targets = append(targets, t)
		opts = append(opts, item(strconv.Itoa(len(targets)), label))
	}
	if def.OnAccepted != nil {
		add(target{key: "on_accepted"}, "When the vendor accepts a message")
	}
	if def.Progress != nil {
		add(target{key: "progress"}, "Progress receipt when the vendor accepts it")
	}
	if def.OnRefused != nil {
		add(target{key: "on_refused"}, "When the vendor refuses a message")
	}
	for _, r := range def.SubmitCodes {
		add(target{key: "submit_code", code: r.Code}, "Refusal code "+r.Code)
	}
	for i, r := range def.Receipts {
		add(target{key: "receipt", index: i}, fmt.Sprintf("Receipt rule %d (%s)", i+1, strings.TrimSpace(r.Stat+" "+r.Err)))
	}
	if def.NoReceipt != nil {
		add(target{key: "no_receipt"}, "When no receipt arrives in time")
	}
	opts = append(opts, item("0", "Back"))
	pick, ok := p.choose(sc, "Remove which rule", "", opts)
	n, _ := strconv.Atoi(pick)
	if !ok || n < 1 || n > len(targets) {
		return
	}
	t := targets[n-1]
	next, err := provision.RemovePolicyRule(def, t.key, t.code, t.index)
	if err != nil {
		p.refusal(err)
		return
	}
	p.saveDefault(ctx, next)
}

func (p *Process) saveDefault(ctx context.Context, next *receiptpolicy.Policy) {
	changed, err := p.store.SaveReceiptPolicyDefault(ctx, next)
	switch {
	case err != nil && changed:
		p.okf("  Saved.\n")
		p.warnf("  %v\n", err)
	case err != nil:
		p.refusal(err)
	case !changed:
		p.printf("  Nothing changed, so nothing was saved.\n")
	default:
		p.okf("  Saved. The gateway reads it within 30 seconds.\n")
	}
}
