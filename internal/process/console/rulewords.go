package console

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// How the rule editor says each part of a rule in words: destination,
// conditions, changes, retries, where to send and cards.

func (e *ruleEditor) destWords(d *ruleDraft) string {
	switch {
	case d.dest.Kind != "":
		s := d.dest.Label()
		if d.dest.Kind == provision.DestCountry && d.unknown {
			s += " · also numbers whose country is not found"
		}
		if d.dest.Kind == provision.DestNetwork && d.unknown {
			s += " · also numbers whose country is not found (kept from before)"
		}
		return s
	case len(d.oldNets) > 0:
		return "networks " + strings.Join(d.oldNets, ", ") + " (kept from before; a new rule has one destination)"
	}
	return "[choose where these messages go]"
}

func condWords(d *ruleDraft) string {
	var cs []string
	for _, c := range d.condList() {
		w := c.Words()
		if c.Field == provision.FieldReceiver {
			w += " (kept from before)"
		}
		cs = append(cs, w)
	}
	if len(cs) == 0 {
		return "no conditions: every message to this destination"
	}
	return strings.Join(cs, " · ") + " — a special case"
}

func changeWords(d *ruleDraft) string {
	var out []string
	for _, f := range draftFields {
		if ch := d.changes[f]; ch != nil {
			out = append(out, strings.TrimSuffix(ch.Words(), "."))
		}
	}
	if len(out) == 0 {
		return "no changes: the vendor gets what the client sent"
	}
	return strings.Join(out, " · ")
}

func retryWords(r *sendplan.Retry) string {
	if r == nil {
		return "the usual (" + provision.RetryShort(nil) + ")"
	}
	return "own: " + provision.RetryShort(r)
}

func (e *ruleEditor) vname(id int64) string {
	if s := e.names[id]; s != "" {
		return s
	}
	return fmt.Sprintf("#%d", id)
}

func (e *ruleEditor) whereWords(d *ruleDraft) string {
	pl := d.plan
	if pl == nil || (len(pl.Levels) == 0 && len(pl.Exceptions) == 0) || (len(pl.Levels) > 0 && len(pl.Levels[0].Vendors) == 0) {
		return "[choose a vendor]"
	}
	if len(pl.Levels) == 0 {
		return fmt.Sprintf("only its %d network(s), each with its own vendors (kept from before)", len(pl.Exceptions))
	}
	method := pl.Method
	if method == "" {
		method = pl.MethodFor(pl.Levels[0])
	}
	s := e.names.LevelShort(pl.Levels[0], method)
	if b := len(pl.Levels) - 1; b > 0 {
		s += " · +" + plural(b, "1 backup", strconv.Itoa(b)+" backups")
	}
	own := 0
	for _, lv := range pl.Levels {
		for _, v := range lv.Vendors {
			if v.BuyRateCardID != 0 || v.SellRateCardID != 0 {
				own++
			}
		}
	}
	if own > 0 {
		s += " · " + plural(own, "1 vendor with its own price", strconv.Itoa(own)+" vendors with their own price")
	}
	if n := len(pl.Exceptions); n > 0 {
		s += " · " + plural(n, "1 network tab", strconv.Itoa(n)+" network tabs") + " (kept from before)"
	}
	return s
}

func (e *ruleEditor) rateCards() []provision.RateCardRow {
	if e.cards == nil {
		cards, err := e.p.store.ListRateCards(e.ctx, 1000)
		if err != nil {
			e.p.errf("  %v\n", err)
		}
		e.cards = cards
		if e.cards == nil {
			e.cards = []provision.RateCardRow{}
		}
	}
	return e.cards
}

func (e *ruleEditor) cardName(id int64) string {
	for _, c := range e.rateCards() {
		if c.ID == id {
			return c.Name
		}
	}
	return fmt.Sprintf("card #%d", id)
}

func (e *ruleEditor) cardWords(d *ruleDraft) string {
	if d.sell == 0 && d.dlrSell == 0 && d.dlrBuy == 0 && d.buy == 0 {
		return "the defaults (the account's cards)"
	}
	var out []string
	add := func(what string, id int64) {
		if id != 0 {
			out = append(out, what+": "+e.cardName(id))
		}
	}
	add("sell", d.sell)
	add("receipt sell", d.dlrSell)
	add("receipt buy", d.dlrBuy)
	add("buy (old rule)", d.buy)
	return strings.Join(out, " · ")
}
