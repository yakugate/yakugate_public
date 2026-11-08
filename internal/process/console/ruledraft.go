package console

import (
	"context"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// The rule being made or changed (ruleDraft): copying it, turning it into a
// route, and filling it from a saved rule.

// draftFields are the fields a rule tests or changes, in the order shown.
var draftFields = []string{provision.FieldSender, provision.FieldText, provision.FieldPriority, provision.FieldReceiver}

// ruleDraft is an outgoing rule being made or changed: everything typed so
// far, kept through a refusal until it is saved or discarded on purpose.
type ruleDraft struct {
	id      int64 // 0 = a new rule
	account int64
	acct    string

	dest    provision.Destination // Kind "" = not chosen yet
	oldNets []string              // an old rule's several destinations, kept while Send to is untouched
	unknown bool                  // accept_undetected_network: also numbers whose country is not found

	conds   map[string]*provision.Cond   // sender, text, priority (receiver: old rules only)
	changes map[string]*provision.Change // sender, receiver, text
	stored  map[string]string            // each field's text as read, kept verbatim until the field is touched
	touched map[string]bool

	plan         *sendplan.Plan
	whereTouched bool // the plan was changed here (else it is saved exactly as read)
	wayAsked     bool // the way of choosing has been asked, or was already the rule's
	retry        *sendplan.Retry

	name     string
	autoName bool
	on       bool

	sell, buy, dlrSell, dlrBuy int64

	copyOf string   // the rule this draft copies ("Copy and edit")
	notes  []string // about the rule as read or copied
	// copyBase: a copy named "<its source's name> · <destination>" keeps
	// that name for the destination chosen now, until a name is typed.
	copyBase string
}

func newDraft(accountID int64, account string) *ruleDraft {
	return &ruleDraft{account: accountID, acct: account, on: true, autoName: true,
		conds: map[string]*provision.Cond{}, changes: map[string]*provision.Change{},
		stored: map[string]string{}, touched: map[string]bool{}}
}

func (d *ruleDraft) clone() *ruleDraft {
	c := *d
	c.conds = map[string]*provision.Cond{}
	for k, v := range d.conds {
		x := *v
		c.conds[k] = &x
	}
	c.changes = map[string]*provision.Change{}
	for k, v := range d.changes {
		x := *v
		c.changes[k] = &x
	}
	c.stored = map[string]string{}
	for k, v := range d.stored {
		c.stored[k] = v
	}
	c.touched = map[string]bool{}
	for k, v := range d.touched {
		c.touched[k] = v
	}
	c.oldNets = append([]string(nil), d.oldNets...)
	c.plan = clonePlan(d.plan)
	if d.retry != nil {
		r := *d.retry
		c.retry = &r
	}
	c.notes = append([]string(nil), d.notes...)
	return &c
}

func clonePlan(p *sendplan.Plan) *sendplan.Plan {
	if p == nil {
		return nil
	}
	c := *p
	c.Levels = cloneLevels(p.Levels)
	if p.Exceptions != nil {
		c.Exceptions = map[string][]sendplan.Level{}
		for k, v := range p.Exceptions {
			c.Exceptions[k] = cloneLevels(v)
		}
	}
	return &c
}

func cloneLevels(ls []sendplan.Level) []sendplan.Level {
	if ls == nil {
		return nil
	}
	out := make([]sendplan.Level, len(ls))
	for i, l := range ls {
		l.Vendors = append([]sendplan.Vendor(nil), l.Vendors...)
		out[i] = l
	}
	return out
}

func (d *ruleDraft) condList() []provision.Cond {
	var out []provision.Cond
	for _, f := range draftFields {
		if c := d.conds[f]; c != nil {
			out = append(out, *c)
		}
	}
	return out
}

func (d *ruleDraft) displayName() string {
	if d.autoName || strings.TrimSpace(d.name) == "" {
		return provision.AutoName(d.dest, d.condList())
	}
	if d.copyBase != "" && d.dest.Kind != "" {
		n := d.copyBase + " · " + d.dest.Short()
		if r := []rune(n); len(r) > 128 {
			n = string(r[:128])
		}
		return n
	}
	return d.name
}

// copyNamed marks a copy whose name was made from its source's name and its
// destination ("Zong · Telenor"), so the name follows a destination changed
// in the editor instead of naming the one the copy no longer sends to.
func (d *ruleDraft) copyNamed(src string) {
	if !d.autoName && d.dest.Kind != "" && d.name == src+" · "+d.dest.Short() {
		d.copyBase = src
	}
}

func (d *ruleDraft) destChosen() bool { return d.dest.Kind != "" || len(d.oldNets) > 0 }

// route is the draft as the Route provision saves. strict: refuse what
// cannot be saved (no destination, a field that does not fit together);
// otherwise leave it out, for the "In words" preview.
func (d *ruleDraft) route(strict bool) (provision.Route, error) {
	r := provision.Route{ID: d.id, Name: d.displayName(), Direction: "MT", AccountID: d.account, Type: "direct",
		AcceptUnknown: d.unknown, Disabled: !d.on,
		SellCard: d.sell, BuyCard: d.buy, DLRSellCard: d.dlrSell, DLRBuyCard: d.dlrBuy}
	if d.retry != nil {
		rt := *d.retry
		r.Retry = &rt
	}
	switch {
	case d.dest.Kind != "":
		r.Networks = d.dest.Networks()
	case len(d.oldNets) > 0:
		r.Networks = append([]string(nil), d.oldNets...)
	case strict:
		return r, errDraft("choose where these messages go: any destination, a country or a network")
	}
	for _, f := range draftFields {
		var text string
		if s, ok := d.stored[f]; ok && !d.touched[f] {
			text = s
		} else {
			t, err := provision.FieldToText(f, copyCond(d.conds[f]), copyChange(d.changes[f]))
			if err != nil {
				if strict {
					return r, err
				}
				continue
			}
			text = t
		}
		switch f {
		case provision.FieldSender:
			r.FromPattern = text
		case provision.FieldText:
			r.Keyword = text
		case provision.FieldPriority:
			r.PriorityPattern = text
		case provision.FieldReceiver:
			r.ToPattern = text
		}
	}
	if d.plan != nil {
		pl := clonePlan(d.plan)
		if d.whereTouched {
			tidyPlan(pl)
		}
		r.Plan = pl
	}
	return r, nil
}

func copyCond(c *provision.Cond) *provision.Cond {
	if c == nil {
		return nil
	}
	x := *c
	return &x
}

func copyChange(c *provision.Change) *provision.Change {
	if c == nil {
		return nil
	}
	x := *c
	return &x
}

// tidyPlan writes the plan the way the doors write it (the prototype's
// normalize): the way once, on the rule; no share on a level of one vendor;
// Direct when no level has two vendors to choose between.
func tidyPlan(p *sendplan.Plan) {
	if p.Method == "" {
		u, _ := p.Unified()
		*p = u
	}
	several := false
	for i := range p.Levels {
		lv := &p.Levels[i]
		lv.Method = ""
		if len(lv.Vendors) < 2 {
			lv.Share = false
			for j := range lv.Vendors {
				lv.Vendors[j].Pct = 0
			}
		} else {
			several = true
		}
	}
	for _, ls := range p.Exceptions {
		for _, lv := range ls {
			several = several || len(lv.Vendors) > 1
		}
	}
	if p.Method == "" || !several {
		p.Method = sendplan.Direct
	}
}

// draftFromRoute reads a stored rule (or a copy) as a draft.
func (p *Process) draftFromRoute(ctx context.Context, r provision.Route, account string, notes []string) *ruleDraft {
	d := newDraft(r.AccountID, account)
	d.id, d.on, d.unknown, d.name = r.ID, !r.Disabled, r.AcceptUnknown, r.Name
	d.sell, d.buy, d.dlrSell, d.dlrBuy = r.SellCard, r.BuyCard, r.DLRSellCard, r.DLRBuyCard
	if r.Retry != nil {
		rt := *r.Retry
		d.retry = &rt
	}
	if dest, ok := r.Destination(); ok {
		if full, err := p.store.ResolveDestination(ctx, dest); err == nil {
			dest = full
		}
		d.dest = dest
	} else {
		d.oldNets = append([]string(nil), r.Networks...)
	}
	for f, text := range map[string]string{provision.FieldSender: r.FromPattern, provision.FieldText: r.Keyword,
		provision.FieldPriority: r.PriorityPattern, provision.FieldReceiver: r.ToPattern} {
		if strings.TrimSpace(text) == "" {
			continue
		}
		d.stored[f] = text
		if c, ch, err := provision.TextToField(f, text); err == nil {
			if c != nil {
				d.conds[f] = c
			}
			if ch != nil {
				d.changes[f] = ch
			}
		}
	}
	d.plan = clonePlan(r.Plan)
	if d.plan != nil {
		for _, lv := range d.plan.Levels {
			d.wayAsked = d.wayAsked || len(lv.Vendors) > 1
		}
	}
	d.autoName = r.Name == provision.AutoName(d.dest, d.condList())
	d.notes = notes
	return d
}
