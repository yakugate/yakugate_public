package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/provision"
)

// The rate cards screen. [SCOPE P14-RATES]
//
// Two things make this different from every other screen in the console.
//
// FIRST, there is no "edit this price". Prices are versioned: the row that
// charged a message last month must still be there, unchanged, or that charge
// can never be explained again. So the screen offers "set the price for a
// destination", and the rulebook decides whether that means a first price, a
// new version, or correcting a schedule that has not started. What it did is
// then said out loud, because "saved" would hide the one thing an operator
// needs to know — whether the old price is still charging.
//
// SECOND, the failure here is silent by nature. A message with no matching
// price is not refused: it is accepted, sent, and charged nothing, and every
// screen downstream looks completely normal. So this screen keeps saying which
// cards have no prices, and which have no any-destination price to catch what
// the listed ones miss.

func (p *Process) rateCardsMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		p.printf("\n  Rate cards\n")
		p.printf("  What a customer pays us, and what a vendor costs us.\n\n")
		opts := []opt{}
		opts = append(opts, item("1", "Add a card"))
		opts = append(opts, item("2", "List cards"))
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.addRateCard(ctx, sc)
		case "2":
			p.listRateCards(ctx, sc)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

func (p *Process) addRateCard(ctx context.Context, sc *bufio.Scanner) {
	p.printf("\n  Add a rate card. A card is a list of prices for one kind of traffic.\n\n")

	var c provision.RateCard
	var ok bool

	side, ok := p.choose(sc, "Which side", "1", []opt{
		item("1", "sell — "+provision.RateKindWhat("sell")),
		item("2", "buy  — "+provision.RateKindWhat("buy")),
	})
	n, _ := strconv.ParseInt(side, 10, 64)
	if !ok || n == 0 {
		return
	}
	switch n {
	case 1:
		c.Kind = "sell"
	case 2:
		c.Kind = "buy"
	default:
		p.errf("  There is no side %d.\n", n)
		return
	}

	p.printf("\n  Which traffic does it price?\n")
	types := []opt{}
	for i, t := range provision.RateTypes() {
		types = append(types, item(strconv.Itoa(i+1), fmt.Sprintf("%-4s %s", t, provision.RateTypeWhat(t))))
	}
	kk, ok := p.choose(sc, "Choose", "1", types)
	k, _ := strconv.ParseInt(kk, 10, 64)
	if !ok || k < 1 || int(k) > len(provision.RateTypes()) {
		return
	}
	c.Type = provision.RateTypes()[k-1]
	if c.Kind == "buy" && c.Type == "HLR" {
		p.warnf("\n  A number lookup is something we charge a customer for, not something a\n")
		p.warnf("  vendor bills us for here — a buy card of this kind can be made, but\n")
		p.warnf("  nothing will ever read it.\n")
		if !p.askYes(sc, "Make it anyway", false) {
			return
		}
	}

	if c.Name, ok = p.askName(sc, "A name for this card, e.g. Jazz MT sell"); !ok || c.Name == "" {
		return
	}
	if c.Currency, ok = p.ask(sc, "Currency (three letters, e.g. PKR) [PKR]"); !ok || c.Currency == "0" {
		return
	}
	if c.Currency == "" {
		c.Currency = "PKR"
	}

	companies, err := p.store.ListCompanies(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(companies) == 0 {
		p.printf("\n  There are no companies yet, and every rate card belongs to one.\n")
		p.printf("  Companies → Add a company, first.\n")
		return
	}
	p.printf("\n  Whose card is this? Every card belongs to one company.\n")
	cos := []opt{}
	for _, co := range companies {
		cos = append(cos, row(co.ID, fmt.Sprintf("   %4d  %s", co.ID, trim(co.Name, 40)), ""))
	}
	cid, ok := p.chooseID(sc, "Company id", cos)
	if !ok || cid == 0 {
		return
	}
	c.CompanyID = cid

	c.Notify = true
	if c.Kind == "sell" {
		c.Notify = p.askYes(sc, "Tell the customer when a price on this card changes", true)
	}

	if err := provision.ValidateRateCard(c); err != nil {
		p.refusal(err)
		return
	}
	id, err := p.store.CreateRateCard(ctx, c)
	if err != nil {
		p.refusal(err)
		return
	}
	p.okf("\n  Created rate card #%d %q.\n", id, c.Name)
	p.warnf("  It has no prices yet, so anything charged from it is charged nothing —\n")
	p.warnf("  and that looks perfectly normal everywhere else. Let us add the first one.\n\n")
	if p.askYes(sc, "Set a price now", true) {
		p.setPrice(ctx, sc, id, c.Kind, c.Currency)
	}
}

func (p *Process) listRateCards(ctx context.Context, sc *bufio.Scanner) {
	rows, err := p.store.ListRateCards(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(rows) == 0 {
		p.printf("\n  No rate cards yet. Until there is one, nothing is charged for anything.\n")
		return
	}
	p.printf("\n   %-6s %-26s %-5s %-5s %-5s %-18s %-7s %s\n",
		"id", "name", "side", "for", "cur", "company", "prices", "used by")
	list := []opt{}
	for _, r := range rows {
		name := r.Name
		if r.Disabled {
			name += " (off)"
		}
		note := ""
		if r.Trouble != "" {
			note = "          ^ " + r.Trouble
		}
		list = append(list, row(r.ID, fmt.Sprintf("   %-6d %-26s %-5s %-5s %-5s %-18s %-7d %d",
			r.ID, trim(name, 26), r.Kind, r.Type, r.Currency, trim(r.Company, 18), r.Prices, r.UsedBy), note))
	}
	id, ok := p.chooseID(sc, "Open a card by id (0 to go back)", list)
	if !ok || id == 0 {
		return
	}
	for _, r := range rows {
		if r.ID == id {
			p.rateCardMenu(ctx, sc, r)
			return
		}
	}
	p.errf("  There is no rate card #%d in this list.\n", id)
}

func (p *Process) rateCardMenu(ctx context.Context, sc *bufio.Scanner, r provision.RateCardRow) {
	for {
		state := "on"
		if r.Disabled {
			state = "OFF"
		}
		p.printf("\n  Rate card #%d %q — %s, %s, %s, %s\n", r.ID, r.Name, r.Kind, r.Type, r.Currency, state)
		p.printf("   %s · %d price(s) in force · used by %d account(s), vendor(s) or rule(s)\n",
			r.Company, r.Prices, r.UsedBy)
		if r.Trouble != "" {
			p.printf("   ^ %s\n", r.Trouble)
		}
		opts := []opt{}
		opts = append(opts, item("1", "Prices — what this card charges"))
		opts = append(opts, item("2", "Set a price"))
		opts = append(opts, item("3", "Retire a price"))
		opts = append(opts, item("4", "Put this card on an account or a vendor"))
		if r.Disabled {
			opts = append(opts, item("5", "Switch on"))
		} else {
			opts = append(opts, item("5", "Switch off — traffic priced from it stops being charged"))
		}
		opts = append(opts, item("6", "Delete"))
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.listPrices(ctx, sc, r.ID, r.Currency)
		case "2":
			p.setPrice(ctx, sc, r.ID, r.Kind, r.Currency)
			r = p.refreshCard(ctx, r)
		case "3":
			p.retirePrice(ctx, sc, r.ID, r.Currency)
			r = p.refreshCard(ctx, r)
		case "4":
			p.assignCard(ctx, sc, r)
		case "5":
			if err := p.store.SetRateCardDisabled(ctx, r.ID, !r.Disabled); err != nil {
				p.refusal(err)
				return
			}
			r.Disabled = !r.Disabled
			p.okf("  Done. A running gateway acts on it within 30 seconds.\n")
		case "6":
			p.printf("\n  A card is only deleted when it never held a price: the prices are what\n")
			p.printf("  past charges point at to explain themselves.\n")
			if !p.confirm(sc, r.Name) {
				p.printf("  Left alone.\n")
				continue
			}
			if err := p.store.DeleteRateCard(ctx, r.ID); err != nil {
				p.refusal(err)
				continue
			}
			p.okf("  Deleted rate card #%d %q.\n", r.ID, r.Name)
			return
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

// refreshCard re-reads the card's counters after a price changed, so the menu
// above it is not still claiming the card has no prices.
func (p *Process) refreshCard(ctx context.Context, r provision.RateCardRow) provision.RateCardRow {
	rows, err := p.store.ListRateCards(ctx, 500)
	if err != nil {
		return r
	}
	for _, c := range rows {
		if c.ID == r.ID {
			return c
		}
	}
	return r
}

func (p *Process) listPrices(ctx context.Context, sc *bufio.Scanner, cardID int64, currency string) {
	rows, err := p.store.ListRates(ctx, cardID, false)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(rows) == 0 {
		p.warnf("\n  No prices on this card. Traffic charged from it is charged nothing,\n")
		p.warnf("  and no screen anywhere will say so.\n")
		return
	}
	p.printf("\n  Prices in force, and anything waiting to start. All times UTC.\n\n")
	p.printf("   %-6s %-20s %-4s %-14s %-6s %s\n", "id", "destination", "ver", "per segment", "tax", "state")
	for _, r := range rows {
		p.printf("   %-6d %-20s %-4d %-14s %-6s %s\n",
			r.ID, trim(r.Destination, 20), r.Version, r.Price+" "+currency, r.Tax+"%", r.State)
	}
	p.printf("\n")
	if !p.askYes(sc, "Show every past version too", false) {
		return
	}
	all, err := p.store.ListRates(ctx, cardID, true)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	p.printf("\n  Every version, including the retired ones. A retired row is kept on\n")
	p.printf("  purpose: the charges it made point at it to say what they cost.\n\n")
	p.printf("   %-6s %-20s %-4s %-14s %-6s %-22s %s\n",
		"id", "destination", "ver", "per segment", "tax", "state", "replaced by")
	for _, r := range all {
		by := "—"
		if r.Replaces != 0 {
			by = "#" + strconv.FormatInt(r.Replaces, 10)
		}
		p.printf("   %-6d %-20s %-4d %-14s %-6s %-22s %s\n",
			r.ID, trim(r.Destination, 20), r.Version, r.Price+" "+currency, r.Tax+"%", r.State, by)
	}
	p.printf("\n")
}

// setPrice is the only way a price is written. What happens to the price
// already there is decided by the rulebook, and reported afterwards.
func (p *Process) setPrice(ctx context.Context, sc *bufio.Scanner, cardID int64, kind, currency string) {
	r := provision.Rate{RateCardID: cardID}
	var ok bool

	p.printf("\n  Which messages does this price cover?\n")
	dest, ok := p.choose(sc, "Choose", "1", []opt{
		item("1", "Any destination — the fallback that catches everything else"),
		item("2", "One network — a country code, and a network within it"),
		item("3", "One country — by dialling prefix, e.g. 92"),
	})
	n, _ := strconv.ParseInt(dest, 10, 64)
	if !ok || n == 0 {
		return
	}
	switch n {
	case 1:
		// all three match fields stay empty
	case 2:
		p.printf("  The country code is three digits (410 = Pakistan). Leave the network\n")
		p.printf("  empty to price every network in that country.\n")
		if r.MCC, ok = p.ask(sc, "Country code"); !ok || r.MCC == "0" || r.MCC == "" {
			return
		}
		if r.MNC, ok = p.ask(sc, "Network code (empty = any network there)"); !ok {
			return
		}
	case 3:
		p.printf("  The dialling prefix as it appears at the front of the number, with no +.\n")
		if r.CountryCode, ok = p.ask(sc, "Dialling prefix"); !ok || r.CountryCode == "0" || r.CountryCode == "" {
			return
		}
	default:
		p.errf("  There is no choice %d.\n", n)
		return
	}

	p.printf("\n  The price is per message part, in %s. A long message is several parts\n", currency)
	p.printf("  and is charged for each of them. Free traffic is 0.00, not 0 — on every\n")
	p.printf("  other screen here 0 means go back, and it has to mean that here too.\n")
	if r.Price, ok = p.ask(sc, "Price per segment"); !ok || r.Price == "0" || r.Price == "" {
		return
	}
	if r.TaxPercent, ok = p.ask(sc, "Tax percentage on top (empty = none)"); !ok {
		return
	}

	p.printf("\n  When should it start charging? Enter on its own means now. A date in\n")
	p.printf("  the future schedules it: the price in force keeps charging until then.\n")
	when, ok := p.askWhen(sc, "Starts (YYYY-MM-DD, or YYYY-MM-DD HH:MM, UTC)")
	if !ok {
		return
	}
	r.EffectiveAt = when

	if err := provision.ValidateRate(r); err != nil {
		p.refusal(err)
		return
	}
	out, err := p.store.SetRate(ctx, r)
	if err != nil {
		p.refusal(err)
		return
	}

	// Say which rule applied. "Saved" would hide the one thing that matters:
	// whether the old price is still charging.
	switch {
	case out.First && out.Dropped:
		p.warnf("\n  Set as price #%d — and it is charging NOW, not on the date you gave.\n", out.RateID)
		p.warnf("  This is the first price for that destination, so there is no earlier\n")
		p.warnf("  version to charge the traffic in the meantime; a start date would have\n")
		p.warnf("  left it free until then.\n")
	case out.First:
		p.okf("\n  Set as price #%d. It is charging now.\n", out.RateID)
	case out.InPlace && out.Scheduled:
		p.okf("\n  Changed the price that was waiting to start (#%d). It now starts %s UTC.\n",
			out.RateID, out.When.Format("2006-01-02 15:04"))
		p.printf("  It had not charged anything yet, so it was corrected rather than versioned.\n")
	case out.InPlace:
		p.warnf("\n  Price #%d was waiting to start; it is charging NOW instead, and the\n", out.RateID)
		p.warnf("  version it replaces has stopped.\n")
	case out.Scheduled:
		p.okf("\n  Scheduled as version %d (price #%d), starting %s UTC.\n",
			out.Version, out.RateID, out.When.Format("2006-01-02 15:04"))
		p.printf("  The price in force keeps charging until then, and the gateway switches\n")
		p.printf("  over by itself — nothing to run on the day.\n")
	default:
		p.okf("\n  Saved as version %d (price #%d), charging now.\n", out.Version, out.RateID)
		p.printf("  The version it replaces is closed, not deleted: the charges it already\n")
		p.printf("  made still point at it to say what they cost.\n")
	}
	if kind == "sell" {
		p.printf("  A customer notice is recorded as pending; the portal sends it, if one is\n")
		p.printf("  running. This card can be set never to notify.\n")
	}
	p.printf("  A running gateway charges by it within 30 seconds.\n")
}

func (p *Process) retirePrice(ctx context.Context, sc *bufio.Scanner, cardID int64, currency string) {
	rows, err := p.store.ListRates(ctx, cardID, false)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(rows) == 0 {
		p.printf("\n  There are no prices on this card to retire.\n")
		return
	}
	p.printf("\n  Retiring a price stops it charging. It is never deleted.\n\n")
	p.printf("   %-6s %-20s %-14s %s\n", "id", "destination", "per segment", "state")
	list := []opt{}
	for _, r := range rows {
		list = append(list, row(r.ID, fmt.Sprintf("   %-6d %-20s %-14s %s", r.ID, trim(r.Destination, 20), r.Price+" "+currency, r.State), ""))
	}
	id, ok := p.chooseID(sc, "Retire which price (0 to go back)", list)
	if !ok || id == 0 {
		return
	}
	var chosen provision.RateRow
	for _, r := range rows {
		if r.ID == id {
			chosen = r
		}
	}
	if chosen.ID == 0 {
		p.errf("  There is no price #%d on this card.\n", id)
		return
	}
	p.printf("\n  %s at %s %s — %s\n", chosen.Destination, chosen.Price, currency, chosen.State)
	if !p.confirm(sc, strconv.FormatInt(chosen.ID, 10)) {
		p.printf("  Left alone.\n")
		return
	}
	if err := p.store.CloseRate(ctx, cardID, chosen.ID); err != nil {
		p.refusal(err)
		return
	}
	p.okf("  Retired price #%d. It stays in the table so its past charges can still\n", chosen.ID)
	p.printf("  be explained.\n")
}

// assignCard puts the card where something will actually read it. A card
// nothing points at prices nothing, which is the quietest way to find out at
// the end of a month that a customer was never charged.
func (p *Process) assignCard(ctx context.Context, sc *bufio.Scanner, r provision.RateCardRow) {
	what := "account"
	if r.Kind == "buy" {
		what = "vendor"
	}
	p.printf("\n  This is a %s card, so it goes on %s %s.\n", r.Kind, article(what), what)

	var id int64
	var ok bool
	if what == "account" {
		accounts, err := p.store.ListAccounts(ctx, 200)
		if err != nil {
			p.errf("  %v\n", err)
			return
		}
		if len(accounts) == 0 {
			p.printf("  There are no accounts yet.\n")
			return
		}
		p.printf("\n")
		if id, ok = p.chooseID(sc, "Account id", accountRows(accounts)); !ok || id == 0 {
			return
		}
	} else {
		vendors, err := p.store.ListVendors(ctx, 200)
		if err != nil {
			p.errf("  %v\n", err)
			return
		}
		if len(vendors) == 0 {
			p.printf("  There are no vendors yet.\n")
			return
		}
		p.printf("\n")
		if id, ok = p.chooseID(sc, "Vendor id", vendorRows(vendors)); !ok || id == 0 {
			return
		}
	}

	if err := p.store.AssignRateCard(ctx, what, id, r.ID, r.Type); err != nil {
		p.refusal(err)
		return
	}
	p.okf("\n  Done. %s #%d now prices its %s traffic from %q, unless a routing rule\n",
		title(what), id, r.Type, r.Name)
	p.printf("  names a card of its own — a rule always wins, because a per-rule deal is\n")
	p.printf("  deliberate.\n")
}

// askWhen reads a start time. Empty means now, which is the common answer, so
// it must not be a special word anyone has to know.
func (p *Process) askWhen(sc *bufio.Scanner, prompt string) (time.Time, bool) {
	for {
		s, ok := p.ask(sc, prompt)
		if !ok {
			return time.Time{}, false
		}
		s = strings.TrimSpace(s)
		switch s {
		case "":
			return time.Time{}, true // now
		case "0":
			return time.Time{}, false
		}
		for _, layout := range []string{"2006-01-02 15:04", "2006-01-02T15:04", "2006-01-02"} {
			if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
				return t, true
			}
		}
		p.errf("  %q is not a date. Write it as 2026-10-01, or 2026-10-01 09:30.\n", s)
	}
}

func article(s string) string {
	if s == "" {
		return "a"
	}
	if strings.ContainsRune("aeiou", rune(s[0])) {
		return "an"
	}
	return "a"
}

func title(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
