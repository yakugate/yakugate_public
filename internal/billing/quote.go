package billing

import (
	"encoding/json"
	"strings"
)

// A Quote is what a message costs, fixed at the moment it is ACCEPTED and
// carried on the message to its record (model.Message.Quote, queue/0038,
// hot/0059).
//
// Saleem, 2026-10-13: "we don't need to record per message" and "older
// messages are not charged; the charge applies once the rate card is there,
// previously sent messages are not touched at all".
//
// Until then the billing role priced a record when it RATED it, with the
// cards of that moment, and knew that a prepaid message had been charged at
// acceptance only from a row in balance_history under the message id. Two
// things followed: a card set while records were waiting priced all of them,
// and every message cost a ledger row (written while the client waited).
//
// Now the accept path prices the message once — the message itself, one
// delivery receipt, one number lookup — and says whether the message's price
// was taken from a prepaid balance there and then (Held). The billing role
// charges by the quote:
//
//   - no price in the quote: never charged, whatever cards exist later;
//   - Held: nothing more to take (only a difference in parts);
//   - a price, not held: charged by the billing role, at this price.
//
// A record with no quote at all (accepted before this, or by a gateway with
// no prices wired) is priced by the rates of its accept time, as before.
type Quote struct {
	V int `json:"v"`
	// MT is the price of the message for Parts parts; nil = no card or no
	// rate priced it when it was accepted.
	MT    *Price `json:"mt,omitempty"`
	Parts int    `json:"p,omitempty"`
	// Held says MT.Charge was taken from a prepaid balance at acceptance.
	Held bool `json:"held,omitempty"`
	// DLR and HLR are the price of ONE delivery receipt and ONE number
	// lookup for this message; nil = not priced.
	DLR *Price `json:"dlr,omitempty"`
	HLR *Price `json:"hlr,omitempty"`

	// Fixed at acceptance with the price, for every later moment that books
	// money (events.go; Saleem 26 Oct 2026): the company's currency (what a
	// charge is in), the account's charge point and its receipt charging
	// (dlr_charge_mode). "" in a quote written before: the account's terms
	// as they stand are read instead (Terms).
	Cur     string `json:"cc,omitempty"`
	Point   string `json:"pt,omitempty"`
	DLRMode string `json:"dm,omitempty"`
	// Booked says the accept path made this message's acceptance events (the
	// message's sell side when it is charged at acceptance, and its lookup).
	// A quote without it was accepted before the billing queue: the first
	// later moment makes them (AcceptEvents).
	Booked bool `json:"bk,omitempty"`
}

// Price is one selling price as the rater gave it: Charge in the company's
// currency (what a balance is moved by), Sell in the card's own.
type Price struct {
	Charge   float64         `json:"c"`
	Sell     float64         `json:"s"`
	Net      float64         `json:"n,omitempty"`
	Tax      float64         `json:"t,omitempty"`
	TaxPct   float64         `json:"tp,omitempty"`
	Currency string          `json:"cur,omitempty"`
	Detail   json.RawMessage `json:"d,omitempty"` // the rating's own audit JSON (card, rate, fx)
	// FX is what one unit of the card's currency was charged as in the
	// company's (1 = the same currency); B what one unit of the company's
	// currency is worth in the base currency. Fixed at acceptance with the
	// price (27 Oct 2026), so every later moment books with the same rates.
	// 0 = not known (a quote written before, or no rate to base).
	FX float64 `json:"x,omitempty"`
	B  float64 `json:"b,omitempty"`
}

// Conv is how this price converts, as a side carries it.
func (p *Price) Conv() *Conv {
	if p == nil {
		return nil
	}
	fx := p.FX
	if fx == 0 {
		fx = fxOf(p.Detail)
	}
	return &Conv{CardCur: strings.ToUpper(p.Currency), FX: fx, Base: p.B}
}

// LookupCharged says whether a message's number lookup is one that is
// charged: our HLR data ("db") and a live lookup that answered ("dip")
// always; a live lookup that found nothing ("dip_miss") only when the account
// says so (charge_failed_hlr). Number ranges, the country code and the
// client's own code are free. The accept path quotes only such a lookup, and
// the billing role charges by the same rule.
func LookupCharged(source string, chargeFailed bool) bool {
	switch source {
	case "db", "dip":
		return true
	case "dip_miss":
		return chargeFailed
	}
	return false
}

// QuoteVersion is the shape written today.
const QuoteVersion = 1

// PriceOf is the selling side of a rating as a Price; nil when the rating
// priced nothing to charge.
func PriceOf(r Rated) *Price {
	if r.SellCharge == nil || *r.SellCharge <= 0 || r.Sell == nil {
		return nil
	}
	p := &Price{Charge: *r.SellCharge, Sell: *r.Sell, Currency: r.Currency, FX: r.SellFX, B: r.SellBase}
	if r.SellNet != nil {
		p.Net = *r.SellNet
	}
	if r.SellTax != nil {
		p.Tax = *r.SellTax
	}
	if r.SellTaxPct != nil {
		p.TaxPct = *r.SellTaxPct
	}
	p.Detail = sellDetail(r.Detail)
	return p
}

// sellDetail keeps only the selling side of a rating's audit JSON — the card
// and rate that priced it and the exchange rate used. The quote rides the
// queue row of every message: the buying side is the billing role's to work
// out when the vendor that really carried the message is known, and is no
// business of the quote.
func sellDetail(detail string) json.RawMessage {
	if detail == "" {
		return nil
	}
	var m map[string]json.RawMessage
	if json.Unmarshal([]byte(detail), &m) != nil {
		return nil
	}
	keep := map[string]json.RawMessage{}
	for _, k := range []string{"sell", "fx", "base"} {
		if v, ok := m[k]; ok {
			keep[k] = v
		}
	}
	if len(keep) == 0 {
		return nil
	}
	b, err := json.Marshal(keep)
	if err != nil {
		return nil
	}
	return b
}

// String is the quote as it rides the queue row; "" for the zero quote.
func (q Quote) String() string {
	if q.V == 0 {
		return ""
	}
	b, err := json.Marshal(q)
	if err != nil {
		return ""
	}
	return string(b)
}

// ParseQuote reads a quote off a record. ok=false = no quote there (an older
// record) or one this build cannot read: the caller prices the record itself.
func ParseQuote(s string) (Quote, bool) {
	if s == "" {
		return Quote{}, false
	}
	var q Quote
	if err := json.Unmarshal([]byte(s), &q); err != nil || q.V != QuoteVersion {
		return Quote{}, false
	}
	if q.Parts < 1 {
		q.Parts = 1
	}
	return q, true
}

// ChargeFor is the message's price for `parts` parts, in the company's
// currency and in whole ledger units: the price per part that was quoted,
// times the parts. 0 when the quote has no price.
func (q Quote) ChargeFor(parts int) float64 {
	if q.MT == nil {
		return 0
	}
	if parts < 1 {
		parts = 1
	}
	return RoundLedger(q.MT.Charge * float64(parts) / float64(q.partsOr1()))
}

func (q Quote) partsOr1() int {
	if q.Parts < 1 {
		return 1
	}
	return q.Parts
}

// OnRating puts a quoted price onto a rating's selling side, scaled from the
// parts it was quoted for to the parts given (1 and 1 for a receipt or a
// lookup). The buying side of r is left as it is. A nil price clears the
// selling side: nothing priced it.
func (p *Price) OnRating(r Rated, parts, quotedParts int) Rated {
	if p == nil {
		r.Sell, r.SellCharge, r.SellNet, r.SellTax, r.SellTaxPct = nil, nil, nil, nil, nil
		return r
	}
	if parts < 1 {
		parts = 1
	}
	if quotedParts < 1 {
		quotedParts = 1
	}
	k := float64(parts) / float64(quotedParts)
	sell, charge := p.Sell*k, RoundLedger(p.Charge*k)
	r.Sell, r.SellCharge = &sell, &charge
	if p.Net != 0 || p.Tax != 0 {
		net, tax := p.Net*k, p.Tax*k
		r.SellNet, r.SellTax = &net, &tax
	} else {
		r.SellNet, r.SellTax = nil, nil
	}
	if p.TaxPct != 0 {
		pct := p.TaxPct
		r.SellTaxPct = &pct
	} else {
		r.SellTaxPct = nil
	}
	if p.Currency != "" {
		r.Currency = p.Currency
	}
	return r
}
