package console

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/netcode"
	"github.com/yakugate/yakugate/internal/provision"
)

// The rule editor's "Send to" and "Only when" parts: which messages a rule
// catches.

// askDest asks where the rule sends: a search over countries, networks,
// dialling codes, MCC-MNC and the start of a number.
func (e *ruleEditor) askDest() bool {
	p := e.p
	p.printf("\n  Send to: where these messages go. Type a country, a network, a dialling code,\n")
	p.printf("  410-04 or the start of a number. Enter on its own = any destination.\n")
	for {
		q, ok := p.ask(e.sc, "Send to (0 = back)")
		if !ok || q == "0" {
			return false
		}
		var dest provision.Destination
		if q == "" {
			dest = provision.AnyDestination()
		} else {
			list, err := p.store.ListDestinations(e.ctx, q, 20)
			if err != nil {
				p.errf("  %v\n", err)
				return false
			}
			if list.Note != "" {
				p.warnf("  %s\n", list.Note)
			}
			if len(list.Hits) == 0 {
				continue
			}
			opts := make([]opt, 0, len(list.Hits)+1)
			for i, h := range list.Hits {
				opts = append(opts, opt{Key: strconv.Itoa(i + 1),
					Text: fmt.Sprintf("   %2d  %s — %s", i+1, h.What, e.view.DestinationTag(h.Destination, e.d.id))})
			}
			opts = append(opts, opt{Key: "0", Text: "    0  Search again"})
			k, ok := p.choose(e.sc, "Which one", "1", opts)
			if !ok {
				return false
			}
			n, _ := strconv.Atoi(k)
			if n < 1 || n > len(list.Hits) {
				if k != "0" {
					p.errf("  %q is not one of the choices.\n", k)
				}
				continue
			}
			dest = list.Hits[n-1].Destination
		}
		wasCountry := e.d.dest.Kind == provision.DestCountry
		e.setDest(dest)
		p.printf("  %s\n", dest.Reads())
		if dest.Kind == provision.DestCountry && !e.pickOnly {
			def := wasCountry && e.d.unknown
			yes, ok := p.askYesOK(e.sc, "Also take numbers whose country is not found? Normally only 'Everything else' takes them", def)
			if !ok {
				return false
			}
			e.d.unknown = yes
		}
		return true
	}
}

// setDest puts a destination on the draft. Any destination always takes
// numbers whose country is not found; a network rule never does. Network
// tabs of an old rule outside the new destination are left out, and said.
func (e *ruleEditor) setDest(d provision.Destination) {
	e.d.dest, e.d.oldNets = d, nil
	switch d.Kind {
	case provision.DestAny:
		e.d.unknown = true
	case provision.DestNetwork:
		e.d.unknown = false
	case provision.DestCountry:
		e.d.unknown = false
	}
	if e.d.plan != nil {
		for code := range e.d.plan.Exceptions {
			if !destTakesCode(d, code) {
				delete(e.d.plan.Exceptions, code)
				e.d.whereTouched = true
				e.p.warnf("  Its own vendors for network %s are left out: %s does not take that network.\n", code, d.Short())
			}
		}
	}
	if ch := e.d.changes[provision.FieldReceiver]; ch != nil && ch.Action == provision.ChangeRemovePrefix && d.CCC != "" && ch.Value != d.CCC {
		e.p.warnf("  The receiver change removes %s at the start, and %s numbers start with %s. Check it under 'Change before sending'.\n", ch.Value, d.Short(), d.CCC)
	}
}

// destTakesCode: the destination takes numbers of one network code.
func destTakesCode(d provision.Destination, code string) bool {
	mcc, mnc := netcode.Split(code)
	switch d.Kind {
	case provision.DestAny:
		return true
	case provision.DestCountry:
		return mcc == d.MCC
	case provision.DestNetwork:
		return mcc == d.MCC && netcode.SameMNC(mnc, d.MNC)
	}
	return false
}

func (e *ruleEditor) condsLinear() bool {
	yes, ok := e.p.askYesOK(e.sc, "Only for some messages: a sender, text or priority condition", false)
	if !ok {
		return false
	}
	if !yes {
		return true
	}
	return e.editConds()
}

func fieldTitle(f string) string {
	switch f {
	case provision.FieldText:
		return "Text"
	case provision.FieldPriority:
		return "Priority"
	case provision.FieldReceiver:
		return "Receiver"
	}
	return "Sender"
}

func (e *ruleEditor) editConds() bool {
	p, d := e.p, e.d
	p.printf("\n  Only when (optional). With a condition this rule is a special case:\n")
	p.printf("  special cases are checked before the destination rules.\n")
	fields := []string{provision.FieldSender, provision.FieldText, provision.FieldPriority}
	for {
		p.printf("\n  Now: %s\n", condWords(d))
		opts := []opt{}
		for i, f := range fields {
			label := fieldTitle(f) + " — add a condition"
			if c := d.conds[f]; c != nil {
				label = fieldTitle(f) + " — " + c.Words() + " (change or remove)"
			}
			opts = append(opts, item(strconv.Itoa(i+1), label))
		}
		if c := d.conds[provision.FieldReceiver]; c != nil {
			opts = append(opts, item("4", "Receiver — "+c.Words()+" (kept from before; remove it)"))
		}
		opts = append(opts, item("0", "Done"))
		k, ok := p.choose(e.sc, "Choose", "0", opts)
		if !ok {
			return false
		}
		switch k {
		case "0", "":
			return true
		case "4":
			if d.conds[provision.FieldReceiver] != nil {
				delete(d.conds, provision.FieldReceiver)
				d.touched[provision.FieldReceiver] = true
				p.okf("  Removed. The destination decides which numbers the rule catches.\n")
			}
			continue
		}
		n, _ := strconv.Atoi(k)
		if n < 1 || n > len(fields) {
			p.errf("  %q is not one of the choices.\n", k)
			continue
		}
		f := fields[n-1]
		if cur := d.conds[f]; cur != nil {
			act, ok := p.choose(e.sc, "The "+strings.ToLower(fieldTitle(f))+" condition", "1",
				[]opt{item("1", "Change it"), item("2", "Remove it"), item("0", "Back")})
			if !ok {
				return false
			}
			if act == "2" {
				delete(d.conds, f)
				d.touched[f] = true
				e.checkFieldFits(f)
				continue
			}
			if act != "1" {
				continue
			}
		}
		c, ok := e.askCond(f, d.conds[f])
		if !ok {
			if p.ended {
				return false
			}
			continue
		}
		d.conds[f] = c
		d.touched[f] = true
		e.checkFieldFits(f)
	}
}

// checkFieldFits warns when a field's change no longer fits beside its
// condition: one field holds one pattern.
func (e *ruleEditor) checkFieldFits(f string) {
	ch := e.d.changes[f]
	if ch == nil {
		return
	}
	if _, err := provision.FieldToText(f, copyCond(e.d.conds[f]), copyChange(ch)); err != nil {
		e.p.warnf("  %s.\n  Change it under 'Change before sending', or the rule cannot be saved.\n", provision.Reason(err))
	}
}

func condPrompt(f, op string) string {
	w := strings.ToLower(fieldTitle(f))
	switch op {
	case provision.OpIs:
		return "The " + w + " is"
	case provision.OpOneOf:
		return "The " + w + " is one of (comma separated)"
	case provision.OpStarts:
		return "The " + w + " starts with"
	case provision.OpContains:
		return "The " + w + " contains"
	}
	return "The " + w + " matches the pattern (Go regular expression)"
}

// askCond asks one condition. Its test and value are checked at once, in
// the engine's own regex language.
func (e *ruleEditor) askCond(f string, cur *provision.Cond) (*provision.Cond, bool) {
	p := e.p
	if f == provision.FieldPriority {
		p.printf("  1 is the most urgent. A message sent without a priority counts as 9.\n")
		prompt := "Which priorities, 1 to 9 (comma separated)"
		if cur != nil {
			prompt += " [" + cur.Value + "]"
		}
		for {
			s, ok := p.ask(e.sc, prompt)
			if !ok || s == "0" {
				return nil, false
			}
			if s == "" && cur != nil {
				return cur, true
			}
			vals := strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' })
			c := &provision.Cond{Field: f, Op: provision.OpOneOf, Value: strings.Join(vals, ", ")}
			if len(vals) == 1 {
				c.Op = provision.OpIs
			}
			if err := c.Check(); err != nil {
				p.errf("  %s\n", provision.Reason(err))
				continue
			}
			return c, true
		}
	}
	ops := provision.CondOps(f)
	opts := make([]opt, 0, len(ops)+1)
	def := "1"
	for i, o := range ops {
		opts = append(opts, item(strconv.Itoa(i+1), provision.OpWords(o)))
		if cur != nil && cur.Op == o {
			def = strconv.Itoa(i + 1)
		}
	}
	opts = append(opts, item("0", "Back"))
	k, ok := p.choose(e.sc, "The "+strings.ToLower(fieldTitle(f))+"…", def, opts)
	n, _ := strconv.Atoi(k)
	if !ok || n < 1 || n > len(ops) {
		return nil, false
	}
	c := &provision.Cond{Field: f, Op: ops[n-1], IgnoreCase: f == provision.FieldText}
	if cur != nil {
		c.IgnoreCase = cur.IgnoreCase
	}
	for {
		prompt := condPrompt(f, c.Op)
		if cur != nil && cur.Op == c.Op {
			prompt += " [" + cur.Value + "]"
		}
		v, ok := p.ask(e.sc, prompt)
		if !ok || v == "0" {
			return nil, false
		}
		if v == "" && cur != nil && cur.Op == c.Op {
			v = cur.Value
		}
		c.Value = v
		if err := c.Check(); err != nil {
			p.errf("  %s\n", provision.Reason(err))
			continue
		}
		break
	}
	if c.Op != provision.OpPattern {
		yes, ok := p.askYesOK(e.sc, "Ignore capitals", c.IgnoreCase)
		if !ok {
			return nil, false
		}
		c.IgnoreCase = yes
	} else {
		c.IgnoreCase = false
	}
	if err := c.Check(); err != nil {
		p.errf("  %s\n", provision.Reason(err))
		return nil, false
	}
	return c, true
}
