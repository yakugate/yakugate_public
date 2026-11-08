package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/provision"
)

// Asking the operator: one line, a name, a number, yes or no, a choice, one
// setting at a time, and which gateways serve a new account or vendor.

// ask prints a prompt and reads one line. ok=false means the terminal closed.
func (p *Process) ask(sc *bufio.Scanner, prompt string) (string, bool) {
	p.printf("%s: ", prompt)
	if !sc.Scan() {
		p.ended = true
		return "", false
	}
	return strings.TrimSpace(sc.Text()), true
}

// askName reads a name a person is giving something — a field they call
// "MSISDN", a rule they call "Jazz peak hours". Everywhere else in this
// console 0 means "go back", so it has to mean that here too: without this, an
// operator typing 0 to escape a long list of questions silently named a field
// "0" (found 2026-09-18, building a dialect). ok=false means abandon.
func (p *Process) askName(sc *bufio.Scanner, prompt string) (string, bool) {
	v, ok := p.ask(sc, prompt)
	if !ok || v == "0" {
		return "", false
	}
	return v, true
}

func (p *Process) askInt(sc *bufio.Scanner, prompt string, def int) (int64, bool) {
	for {
		s, ok := p.ask(sc, fmt.Sprintf("%s [%d]", prompt, def))
		if !ok {
			return 0, false
		}
		if s == "" {
			return int64(def), true
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			p.errf("  %q is not a number.\n", s)
			continue
		}
		return n, true
	}
}

func (p *Process) askYes(sc *bufio.Scanner, prompt string, def bool) bool {
	d := "y/N"
	if def {
		d = "Y/n"
	}
	for {
		s, ok := p.ask(sc, fmt.Sprintf("%s [%s]", prompt, d))
		if !ok {
			return def
		}
		switch strings.ToLower(s) {
		case "":
			return def
		case "y", "yes":
			return true
		case "n", "no":
			return false
		}
		p.errf("  Answer y or n.\n")
	}
}

// askYesOK is askYes that says when the input closed (ok=false), so a flow
// can stop instead of taking the default as an answer.
func (p *Process) askYesOK(sc *bufio.Scanner, prompt string, def bool) (yes, ok bool) {
	yes = p.askYes(sc, prompt, def)
	return yes, !p.ended
}

// askChoice offers the real vocabulary rather than taking free text, because a
// value the engine does not recognise would quietly become the default — which
// is exactly the bug multipart_mode had before 2026-09-18.
func (p *Process) askChoice(sc *bufio.Scanner, prompt string, choices []string, def, hint string) (string, bool) {
	if hint != "" { // no hint, no line: an empty one used to be printed (bug 18)
		p.printf("  %s\n", hint)
	}
	if p.cursor && p.tty != nil {
		// Picked, not typed: the one value that cannot be mistyped is one
		// nobody types. Esc abandons, as 0 did.
		p.printf("%s:\n", prompt)
		opts := make([]opt, 0, len(choices))
		for _, c := range choices {
			o := opt{Key: c, Text: "   " + c}
			if c == def {
				o.Text += "   (default)"
			}
			opts = append(opts, o)
		}
		return p.chooseCursor(sc, prompt, def, opts)
	}
	for {
		s, ok := p.ask(sc, fmt.Sprintf("%s (%s) [%s]", prompt, strings.Join(choices, " / "), def))
		if !ok {
			return "", false
		}
		if s == "" {
			return def, true
		}
		if s == "0" {
			return "", false
		}
		for _, c := range choices {
			if strings.EqualFold(s, c) {
				return c, true
			}
		}
		p.errf("  %q is not one of: %s\n", s, strings.Join(choices, ", "))
	}
}

// editOne draws a numbered list of what may be changed, asks for the new value
// in the right shape, and hands it to the caller's update. The list comes from
// the rulebook, so the console never has to know what a setting means — and a
// setting that cannot be changed is SHOWN with its reason rather than left
// out, because an operator hunting for a missing option assumes the console is
// unfinished.
func (p *Process) editOne(ctx context.Context, sc *bufio.Scanner, fields []provision.Field,
	fixed []provision.FixedField, save func(key, value string) error) {
	p.editOneShowing(ctx, sc, fields, fixed, nil, save)
}

// editOneShowing is editOne that also says what the chosen setting is now,
// for the keys now answers ("" = say nothing).
func (p *Process) editOneShowing(ctx context.Context, sc *bufio.Scanner, fields []provision.Field,
	fixed []provision.FixedField, now func(key string) string, save func(key, value string) error) {
	p.printf("\n  Change a setting\n")
	opts := []opt{}
	for i, f := range fields {
		opts = append(opts, opt{Key: strconv.Itoa(i + 1), Text: fmt.Sprintf("   %2d  %s", i+1, f.Label)})
	}
	for _, f := range fixed {
		opts = append(opts, opt{Text: fmt.Sprintf("    –  %s — cannot be changed", f.Label), Skip: true})
	}
	opts = append(opts, opt{Key: "0", Text: "    0  Back"})
	pick, ok := p.choose(sc, "Which setting", "0", opts)
	n, _ := strconv.ParseInt(pick, 10, 64)
	if !ok || n == 0 {
		if len(fixed) > 0 && n == 0 {
			return
		}
		return
	}
	if n < 1 || int(n) > len(fields) {
		p.errf("  There is no setting %d.\n", n)
		return
	}
	f := fields[n-1]
	if f.Hint != "" {
		p.printf("  %s\n", f.Hint)
	}
	if now != nil {
		if v := now(f.Key); v != "" {
			p.printf("  Now: %s\n", v)
		}
	}
	var val string
	switch f.Kind {
	case "choice":
		val, ok = p.askChoice(sc, f.Label, f.Choices, f.Choices[0], "")
	case "bool":
		val = "no"
		if p.askYes(sc, f.Label, true) {
			val = "yes"
		}
		ok = true
	case "int":
		var i64 int64
		i64, ok = p.askInt(sc, f.Label, 0)
		val = strconv.FormatInt(i64, 10)
	default:
		val, ok = p.ask(sc, "New value")
	}
	if !ok || val == "0" && f.Kind == "text" {
		return
	}
	if err := save(f.Key, val); err != nil {
		p.refusal(err)
		return
	}
	p.okf("  Saved. A running gateway picks it up within 30 seconds.\n")
}

// confirm makes a destructive step deliberate: the name has to be typed back,
// so a stray keypress cannot delete a customer.
func (p *Process) confirm(sc *bufio.Scanner, name string) bool {
	got, ok := p.ask(sc, fmt.Sprintf("Type %q to confirm", name))
	return ok && got == name
}

// askDeployments asks which gateways serve an account and its MT rate on each
// (config/0122 + config/0123). Choosing is mandatory: the Admin API refuses a
// save without one and a provision rule must never be looser.
//
// The register is read from the config store the console already holds, so a
// gateway with no Admin API can still place an account.
func (p *Process) askDeployments(sc *bufio.Scanner) (deployment.List, bool) {
	rows, err := p.store.DB.ActiveDeployments(context.Background())
	if err != nil {
		p.errf("  Could not read the deployments: %v\n", err)
		return nil, false
	}
	type dep struct{ id, name string }
	var known []dep
	for _, d := range rows {
		known = append(known, dep{d.ID, d.Name})
	}

	if len(known) == 0 {
		// Nothing to choose from. An install that has never registered a
		// deployment keeps working exactly as it did before any of this
		// existed — insisting here would make it impossible to create an
		// account at all.
		p.printf("\n  No deployment is registered yet, so this account is served everywhere.\n")
		p.printf("  Register one under Operators → Deployments to set a per-gateway rate.\n")
		return nil, true
	}
	opts := make([]opt, 0, len(known)+1)
	for _, d := range known {
		label := d.id
		if d.name != "" {
			label = d.name + "  (" + d.id + ")"
		}
		opts = append(opts, wideItem(d.id, label))
	}
	p.printf("\n  Which gateway serves this account? Throughput is set on it.\n")
	choice, ok := p.choose(sc, "Deployment", known[0].id, opts)
	if !ok || choice == "" {
		return nil, false
	}
	tp, ok := p.askInt(sc, "Throughput on "+choice+" in segments/sec (0 = uncapped)", 0)
	if !ok {
		return nil, false
	}
	e := deployment.Assignment{ID: choice}
	if tp > 0 {
		e.MTThroughput = int(tp)
	}
	p.printf("  More gateways can be added later from Accounts → edit → Deployments & throughput.\n")
	return deployment.List{e}, true
}

// askVendorDeployments asks which gateways carry a vendor. Same shape as
// askDeployments for an account, and the same conditional rule: with no
// deployment registered there is nothing to choose and the vendor runs
// everywhere, as it always did.
func (p *Process) askVendorDeployments(sc *bufio.Scanner) (deployment.List, bool) {
	rows, err := p.store.DB.ActiveDeployments(context.Background())
	if err != nil {
		p.errf("  Could not read the deployments: %v\n", err)
		return nil, false
	}
	type dep struct{ id, name string }
	var known []dep
	for _, d := range rows {
		known = append(known, dep{d.ID, d.Name})
	}
	if len(known) == 0 {
		p.printf("\n  No deployment is registered yet, so every gateway carries this vendor.\n")
		return nil, true
	}
	opts := make([]opt, 0, len(known))
	for _, d := range known {
		label := d.id
		if d.name != "" {
			label = d.name + "  (" + d.id + ")"
		}
		opts = append(opts, wideItem(d.id, label))
	}
	p.printf("\n  Which gateway carries this vendor? Only it binds and drains the queue.\n")
	choice, ok := p.choose(sc, "Deployment", known[0].id, opts)
	if !ok || choice == "" {
		return nil, false
	}
	tp, ok := p.askInt(sc, "Send rate on "+choice+" in messages/sec (0 = uncapped)", 0)
	if !ok {
		return nil, false
	}
	e := deployment.Assignment{ID: choice}
	if tp > 0 {
		e.MTThroughput = int(tp)
	}
	p.printf("  More gateways can be added later from the portal.\n")
	return deployment.List{e}, true
}
