package console

import (
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
)

// The rule editor's "Change before sending" part.

func (e *ruleEditor) changesLinear() bool {
	yes, ok := e.p.askYesOK(e.sc, "Change what the vendor gets (the sender, receiver or text)", false)
	if !ok {
		return false
	}
	if !yes {
		return true
	}
	return e.editChanges()
}

func (e *ruleEditor) editChanges() bool {
	p, d := e.p, e.d
	p.printf("\n  Change before sending (optional). A change alters only what the vendor gets,\n")
	p.printf("  after all checks. It is undone if the message moves to another rule.\n")
	p.printf("  Priority can't be changed.\n")
	fields := []string{provision.FieldSender, provision.FieldReceiver, provision.FieldText}
	for {
		p.printf("\n  Now: %s\n", changeWords(d))
		opts := []opt{}
		for i, f := range fields {
			label := fieldTitle(f) + " — no change"
			if ch := d.changes[f]; ch != nil {
				label = fieldTitle(f) + " — " + strings.TrimSuffix(ch.Words(), ".") + " (change or remove)"
			}
			opts = append(opts, item(strconv.Itoa(i+1), label))
		}
		opts = append(opts, item("0", "Done"))
		k, ok := p.choose(e.sc, "Choose", "0", opts)
		if !ok {
			return false
		}
		if k == "0" || k == "" {
			return true
		}
		n, _ := strconv.Atoi(k)
		if n < 1 || n > len(fields) {
			p.errf("  %q is not one of the choices.\n", k)
			continue
		}
		f := fields[n-1]
		if cur := d.changes[f]; cur != nil {
			act, ok := p.choose(e.sc, "The "+strings.ToLower(fieldTitle(f))+" change", "1",
				[]opt{item("1", "Change it"), item("2", "Remove it"), item("0", "Back")})
			if !ok {
				return false
			}
			if act == "2" {
				delete(d.changes, f)
				d.touched[f] = true
				continue
			}
			if act != "1" {
				continue
			}
		}
		ch, ok := e.askChange(f)
		if !ok {
			if p.ended {
				return false
			}
			continue
		}
		d.changes[f] = ch
		d.touched[f] = true
	}
}

// changeLabel is an action as the menu offers it.
func changeLabel(a string) string {
	switch a {
	case provision.ChangeSet:
		return "Change to"
	case provision.ChangeReplace:
		return "Replace what matched with"
	case provision.ChangeAddPrefix:
		return "Add at start"
	case provision.ChangeAddSuffix:
		return "Add at end"
	case provision.ChangeRemovePrefix:
		return "Remove from the start (the country code, say), then add (optional)"
	}
	return provision.ActionWords(a)
}

// askChange asks one field's change. Beside a condition only "change to"
// fits, and "replace what matched" beside "contains" or "starts with": one
// field holds one pattern.
func (e *ruleEditor) askChange(f string) (*provision.Change, bool) {
	p, d := e.p, e.d
	cond := d.conds[f]
	var acts []string
	if cond != nil {
		acts = provision.ChangeActionsWith(cond)
		p.printf("  The %s has a condition (%s), so the change uses the same pattern.\n", strings.ToLower(fieldTitle(f)), cond.Words())
	} else {
		switch f {
		case provision.FieldSender:
			acts = []string{provision.ChangeSet, provision.ChangeAddPrefix, provision.ChangeRemovePrefix}
		case provision.FieldReceiver:
			acts = []string{provision.ChangeAddPrefix, provision.ChangeRemovePrefix}
		default:
			acts = []string{provision.ChangeSet, provision.ChangeAddPrefix, provision.ChangeAddSuffix}
		}
		if f != provision.FieldReceiver {
			p.printf("  To replace words, add a '%s contains' condition first: a replace only works on\n", fieldTitle(f))
			p.printf("  messages that contain the words, so without it the rule would skip the others.\n")
		}
	}
	if f == provision.FieldReceiver {
		p.printf("  The rule and prices are chosen for the number the client sent.\n")
	}
	if len(acts) == 0 {
		p.errf("  The %s cannot be changed.\n", strings.ToLower(fieldTitle(f)))
		return nil, false
	}
	opts := []opt{}
	for i, a := range acts {
		opts = append(opts, item(strconv.Itoa(i+1), changeLabel(a)))
	}
	opts = append(opts, item("0", "Back"))
	k, ok := p.choose(e.sc, "Which change", "1", opts)
	n, _ := strconv.Atoi(k)
	if !ok || n < 1 || n > len(acts) {
		return nil, false
	}
	ch := &provision.Change{Field: f, Action: acts[n-1]}
	for {
		switch ch.Action {
		case provision.ChangeSet:
			v, ok := p.ask(e.sc, "Change it to")
			if !ok {
				return nil, false
			}
			ch.Value = v
		case provision.ChangeReplace:
			v, ok := p.ask(e.sc, "Replace what matched with (Enter = remove it)")
			if !ok {
				return nil, false
			}
			ch.With = v
		case provision.ChangeAddPrefix, provision.ChangeAddSuffix:
			where := "start"
			if ch.Action == provision.ChangeAddSuffix {
				where = "end"
			}
			v, ok := p.ask(e.sc, "What to add at the "+where)
			if !ok {
				return nil, false
			}
			ch.Value = v
		case provision.ChangeRemovePrefix:
			prompt := "What to remove from the start"
			if d.dest.CCC != "" {
				prompt += " [" + d.dest.CCC + "]"
			}
			v, ok := p.ask(e.sc, prompt)
			if !ok {
				return nil, false
			}
			if v == "" {
				v = d.dest.CCC
			}
			then, ok := p.ask(e.sc, "Then add, if anything (for example 0)")
			if !ok {
				return nil, false
			}
			ch.Value, ch.With = v, then
		}
		if _, err := provision.FieldToText(f, copyCond(cond), copyChange(ch)); err != nil {
			p.errf("  %s\n", provision.Reason(err))
			continue
		}
		return ch, true
	}
}
