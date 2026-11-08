package console

import (
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
)

// The rule editor's last parts: retries, rate cards, the name, on or off,
// and testing a number with the rule.

func (e *ruleEditor) retryLinear() bool {
	p := e.p
	p.printf("\n  Retries: a round is Main, then every backup. When no vendor of a round takes\n")
	p.printf("  the message it waits, then the next round starts again from Main.\n")
	p.printf("  The usual: %s\n", provision.RetryWords(nil))
	yes, ok := p.askYesOK(e.sc, "Give this rule its own retries", false)
	if !ok {
		return false
	}
	if !yes {
		return true
	}
	rt, ok := e.p.askRetry(e.sc)
	if !ok {
		return false
	}
	e.d.retry = rt
	return true
}

func (e *ruleEditor) editRetry() bool {
	p := e.p
	p.printf("\n  Retries now: %s\n", provision.RetryWords(e.d.retry))
	k, ok := p.choose(e.sc, "Choose", "0", []opt{
		item("1", "The usual ("+provision.RetryShort(nil)+"), stored as no value"),
		item("2", "Own retries for this rule"),
		item("0", "Back"),
	})
	if !ok {
		return false
	}
	switch k {
	case "1":
		e.d.retry = nil
	case "2":
		rt, ok := p.askRetry(e.sc)
		if !ok {
			return false
		}
		e.d.retry = rt
	}
	return true
}

func (e *ruleEditor) editCards() bool {
	p, d := e.p, e.d
	for {
		p.printf("\n  Prices for this rule. Empty = the default: what %s pays comes from its own card,\n", d.acct)
		p.printf("  what each vendor costs from the vendor's card (a vendor's own price for this\n")
		p.printf("  rule is under Where to send).\n")
		name := func(id int64, def string) string {
			if id == 0 {
				return def
			}
			return e.cardName(id)
		}
		opts := []opt{
			item("1", "Sell card — what "+d.acct+" pays for these messages: "+name(d.sell, d.acct+"'s default card")),
			item("2", "Receipt sell card — what "+d.acct+" pays for their receipts: "+name(d.dlrSell, d.acct+"'s default receipt card")),
			item("3", "Receipt buy card — what vendors charge for the receipts: "+name(d.dlrBuy, "each vendor's default receipt card")),
		}
		if d.buy != 0 {
			opts = append(opts, item("4", "Buy card (an old rule's) — "+name(d.buy, "")+"; 0 clears it"))
		}
		opts = append(opts, item("0", "Done"))
		k, ok := p.choose(e.sc, "Change which", "0", opts)
		if !ok {
			return false
		}
		var slot *int64
		kind, traffic, zero := "sell", "MT", "the default"
		switch k {
		case "0", "":
			return true
		case "1":
			slot, zero = &d.sell, d.acct+"'s default card"
		case "2":
			slot, traffic, zero = &d.dlrSell, "DLR", d.acct+"'s default receipt card"
		case "3":
			slot, kind, traffic, zero = &d.dlrBuy, "buy", "DLR", "each vendor's default receipt card"
		case "4":
			if d.buy == 0 {
				continue
			}
			slot, kind, zero = &d.buy, "buy", "each vendor's own card"
		default:
			p.errf("  %q is not one of the choices.\n", k)
			continue
		}
		id, ok := e.pickCard(kind, traffic, zero, *slot)
		if !ok {
			if p.ended {
				return false
			}
			continue
		}
		*slot = id
	}
}

func (e *ruleEditor) askName() bool {
	p, d := e.p, e.d
	auto := provision.AutoName(d.dest, d.condList())
	p.printf("\n  Enter keeps the name; 'auto' names it from your choices (%s).\n", auto)
	for {
		s, ok := p.ask(e.sc, fmt.Sprintf("A name for this rule [%s]", d.displayName()))
		if !ok || s == "0" {
			return false
		}
		switch {
		case s == "":
			return true
		case strings.EqualFold(s, "auto"):
			d.autoName, d.name, d.copyBase = true, "", ""
			return true
		}
		if n := len([]rune(s)); n > 128 {
			p.errf("  A name is at most 128 characters (this one has %d).\n", n)
			continue
		}
		d.name, d.autoName, d.copyBase = s, s == auto, ""
		return true
	}
}

func (e *ruleEditor) toggleOn() bool {
	e.d.on = !e.d.on
	if e.d.on {
		e.p.printf("  It will be saved on.\n")
	} else {
		e.p.warnf("  It will be saved off: it catches nothing until it is turned on.\n")
	}
	return true
}

func (e *ruleEditor) test() {
	p, d := e.p, e.d
	if !d.destChosen() {
		p.warnf("  Choose where these messages go first.\n")
		return
	}
	r, _ := d.route(false)
	uses := planUses(e.view)
	for _, c := range d.condList() {
		uses[c.Field] = true
	}
	in, ok := p.askTestInput(e.sc, uses)
	if !ok {
		return
	}
	in.Draft, in.EditingID = &r, d.id
	res, err := p.store.TestNumberWith(e.ctx, d.account, in)
	if err != nil {
		p.refusal(err)
		return
	}
	p.printTest(res)
}
