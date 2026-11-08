package console

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// The outgoing rule editor (rule-creation-ux.md section 3, in console
// form). One rule, one destination; optional conditions make it a special
// case; one way of choosing for the whole rule, Main and as many backups as
// wanted; optional per-vendor prices, changes before sending, retries and
// cards. The same screen adds, edits and finishes a copy. The rules are
// provision's (the same as the Admin API's): this file only asks.
// [SCOPE P14-ROUTE]

// The editor's parts, by menu key. 1 saves, 0 leaves.
const (
	partDest    = "2"
	partConds   = "3"
	partWhere   = "4"
	partChanges = "5"
	partRetry   = "6"
	partCards   = "7"
	partName    = "8"
	partOn      = "9"
	partTest    = "10"
)

type ruleEditor struct {
	p    *Process
	ctx  context.Context
	sc   *bufio.Scanner
	d    *ruleDraft
	orig *ruleDraft // as read, for "What changes when you save" (nil: a new rule)
	view provision.PlanView

	vendors []provision.VendorRow // every vendor, for names
	fit     []provision.VendorRow // those a rule of this account may name (nil: not listed yet)
	names   provision.VendorNames
	cards   []provision.RateCardRow

	saved provision.Placement

	// refused is a refusal the draft had before the editor opened (a copy).
	refused error

	// pickOnly: askDest only picks a destination (a copy's), with no
	// question about the draft's other settings.
	pickOnly bool
}

func (p *Process) newEditor(ctx context.Context, sc *bufio.Scanner, d *ruleDraft) (*ruleEditor, error) {
	e := &ruleEditor{p: p, ctx: ctx, sc: sc, d: d}
	v, err := p.store.Plan(ctx, d.account)
	if err != nil {
		return nil, err
	}
	e.view = v
	if d.acct == "" {
		d.acct = v.Account
	}
	if e.vendors, err = p.store.ListVendors(ctx, 1000); err != nil {
		return nil, err
	}
	e.names = provision.VendorNames{}
	for _, x := range e.vendors {
		e.names[x.ID] = x.CID
	}
	return e, nil
}

// addRule makes a new outgoing rule for an account, step by step, then
// shows it for saving. preset: its destination is already chosen (a test
// result's "Add a rule for Zong"). It returns true when a rule was saved.
func (p *Process) addRule(ctx context.Context, sc *bufio.Scanner, accountID int64, preset *provision.Destination) bool {
	e, err := p.newEditor(ctx, sc, newDraft(accountID, ""))
	if err != nil {
		p.refusal(err)
		return false
	}
	p.printf("\n  New rule for %s. Each question can be changed again before saving.\n", e.d.acct)
	if preset != nil {
		e.setDest(*preset)
		p.printf("  Send to: %s. %s\n", preset.Label(), preset.Reads())
	}
	if !e.linear(preset != nil) {
		return false
	}
	return e.run()
}

// finishDraft shows a draft made elsewhere (one vendor for everything, a
// copy) for saving, with every part open to change.
func (p *Process) finishDraft(ctx context.Context, sc *bufio.Scanner, d *ruleDraft) bool {
	return p.finishRefused(ctx, sc, d, nil)
}

// finishRefused is finishDraft for a draft the rules already refused (a
// copy): the editor opens on the part the refusal is about, and saves again
// once it is answered (Saleem 2026-09-30: "don't lose the data, ask the
// same again").
func (p *Process) finishRefused(ctx context.Context, sc *bufio.Scanner, d *ruleDraft, refused error) bool {
	e, err := p.newEditor(ctx, sc, d)
	if err != nil {
		p.refusal(err)
		return false
	}
	e.refused = refused
	return e.run()
}

// editRule changes an existing outgoing rule: every part of it.
func (p *Process) editRule(ctx context.Context, sc *bufio.Scanner, routeID int64) bool {
	r, notes, err := p.store.GetRoute(ctx, routeID)
	if err != nil {
		p.refusal(err)
		return false
	}
	d := p.draftFromRoute(ctx, r, "", notes)
	e, err := p.newEditor(ctx, sc, d)
	if err != nil {
		p.refusal(err)
		return false
	}
	e.orig = d.clone()
	return e.run()
}

// linear asks a new rule's parts one after the other. Backing out of one
// keeps what was typed and goes to the review, where it can be finished or
// discarded; backing out of the very first question leaves (nothing typed).
func (e *ruleEditor) linear(preset bool) bool {
	steps := []func() bool{e.askDest, e.condsLinear, e.whereLinearOrEdit, e.changesLinear, e.retryLinear, e.askName}
	if preset {
		steps = steps[1:]
	}
	for i, st := range steps {
		if st() {
			continue
		}
		if e.p.ended {
			return false
		}
		if i == 0 && !preset {
			return false
		}
		e.p.warnf("  Kept what you entered so far.\n")
		break
	}
	return true
}

func (e *ruleEditor) run() bool {
	d := e.d
	saveText, leaveText := "Create rule", "Discard this rule"
	if d.id != 0 {
		saveText, leaveText = "Save rule", "Leave without saving"
	} else if len(e.view.Rules) == 0 {
		saveText = "Create plan"
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
			before := e.d.clone()
			return func() { *e.d = *before }
		},
		extra:   []edPart{{key: partTest, label: "Test a number with this rule", redo: func() bool { e.test(); return true }}},
		refused: e.refused,
	})
}

func (e *ruleEditor) save() error {
	r, err := e.d.route(true)
	if err != nil {
		return err
	}
	var pl provision.Placement
	if e.d.id == 0 {
		pl, err = e.p.store.CreateRoutePlaced(e.ctx, r)
	} else {
		pl, err = e.p.store.UpdateRoute(e.ctx, e.d.id, r)
	}
	if err != nil {
		return err
	}
	e.saved = pl
	e.p.okf("\n  %s\n", pl.Saved)
	if len(pl.Moves) > 0 {
		var ws []string
		for _, m := range pl.Moves {
			ws = append(ws, fmt.Sprintf("'%s' (%d → %d)", m.Name, m.From, m.To))
		}
		e.p.printf("  Also moved: %s.\n", strings.Join(ws, ", "))
	}
	e.p.printf("  A running gateway uses it within 30 seconds.\n")
	return nil
}

func (e *ruleEditor) leave() bool {
	if e.d.id == 0 {
		yes, ok := e.p.askYesOK(e.sc, "Discard everything you entered for this rule", false)
		if yes && ok {
			e.p.printf("  Discarded. Nothing was saved.\n")
		}
		return yes || !ok
	}
	if !e.dirty() {
		return true
	}
	yes, ok := e.p.askYesOK(e.sc, fmt.Sprintf("Leave without saving? Your changes to '%s' are lost", e.orig.displayName()), false)
	return yes || !ok
}

// summary is one part in a few words, for the menu and for "What changes".
type summary struct{ label, now string }

func (e *ruleEditor) summaries(d *ruleDraft) []summary {
	return []summary{
		{"Send to", e.destWords(d)},
		{"Only when", condWords(d)},
		{"Where to send", e.whereWords(d)},
		{"Change before sending", changeWords(d)},
		{"Retries", retryWords(d.retry)},
		{"Cards", e.cardWords(d)},
		{"Name", d.displayName()},
		{"On", onWords(d.on)},
	}
}

func (e *ruleEditor) dirty() bool {
	if e.orig == nil {
		return true
	}
	a, b := e.summaries(e.orig), e.summaries(e.d)
	for i := range a {
		if a[i] != b[i] {
			return true
		}
	}
	return false
}

func (e *ruleEditor) parts() []edPart {
	d := e.d
	s := e.summaries(d)
	name := s[6].now
	if d.autoName {
		name += " (made from your choices)"
	}
	return []edPart{
		{key: partDest, label: "Send to", now: s[0].now, redo: e.askDest, rank: 0,
			hint: []string{"exactly these messages", "where these messages go", "destination", "operators table", "network code", "network rule cannot", "exception for network", "country is not found"}},
		{key: partConds, label: "Only when", now: s[1].now, redo: e.editConds, rank: 5,
			hint: []string{"condition", "pattern", "look-behind", "fits every message", "look for", "must be", "priorit"}},
		{key: partWhere, label: "Where to send", now: s[2].now, redo: e.whereLinearOrEdit, rank: 3,
			hint: []string{"vendor", "send plan", "share", "level", "backup", "deployment", "chooses", "card"}},
		{key: partChanges, label: "Change before sending", now: s[3].now, redo: e.editChanges, rank: 4,
			hint: []string{"change", "add to the", "remove from the start", "after removing", "rewrite"}},
		{key: partRetry, label: "Retries", now: s[4].now, redo: e.editRetry, rank: 1,
			hint: []string{"retries", "retry", "wait", "backoff"}},
		{key: partCards, label: "Sell and receipt cards", now: s[5].now, redo: e.editCards, rank: 2,
			hint: []string{"slot", "prices receipts"}},
		{key: partName, label: "Name", now: name, redo: e.askName, rank: 6, hint: []string{"name"}},
		{key: partOn, label: "On or off", now: s[7].now, redo: e.toggleOn, rank: 7},
	}
}

func onWords(on bool) string {
	if on {
		return "on"
	}
	return "off — it catches nothing until it is turned on"
}

func (e *ruleEditor) header() {
	p, d := e.p, e.d
	if d.id == 0 {
		p.printf("\n  New rule for %s\n", d.acct)
	} else {
		p.printf("\n  Rule #%d '%s' of %s's plan\n", d.id, e.orig.displayName(), d.acct)
	}
	if d.copyOf != "" {
		p.printf("  Copy of '%s'. Nothing is saved until you create it.\n", d.copyOf)
	}
	for _, n := range d.notes {
		p.warnf("  %s\n", n)
	}
	if !d.destChosen() {
		p.warnf("  In words: choose where these messages go first (Send to).\n")
	} else {
		r, _ := d.route(false)
		if s, err := p.store.RouteInWords(e.ctx, r); err == nil {
			p.printf("  In words: %s\n", s)
		}
		if pl, err := p.store.PreviewPlacement(e.ctx, r, d.id); err == nil && pl.Text != "" {
			p.printf("  Its place: %s\n", pl.Text)
		}
		if len(d.condList()) == 0 && d.dest.Kind != "" {
			if tag := e.view.DestinationTag(d.dest, d.id); strings.HasPrefix(tag, "rule ") {
				p.warnf("  %s already has a rule for exactly these messages: %s. Change that rule, or add a condition here.\n", d.acct, tag)
			}
		}
	}
	if d.plan != nil && sendplan.ScoreMethod(d.plan.Method) {
		for _, lv := range d.plan.Levels {
			if lv.Share && len(lv.Vendors) > 1 {
				p.warnf("  %s\n", sendplan.ShareWarning(d.plan.Method))
				break
			}
		}
	}
	if e.orig != nil && e.dirty() {
		a, b := e.summaries(e.orig), e.summaries(d)
		p.printf("  What changes when you save:\n")
		for i := range a {
			if a[i] != b[i] {
				p.printf("    %s: %s → %s\n", a[i].label, a[i].now, b[i].now)
			}
		}
	}
}
