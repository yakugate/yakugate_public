package billing

import (
	"encoding/json"

	"github.com/yakugate/yakugate/internal/model"
)

// RecordRates is what a log row keeps of a message's price (issue 16, Saleem
// 22 Oct 2026: "it was decided to just keep the rate card version id"): the
// rate version ids that priced it, and the rate its selling price was turned
// into the company's currency at. A rate_card_rates row is never changed (a
// new price is a new version), so the ids say the exact price for ever; the
// amounts are worked out when read. 0 = no rate priced that side.
type RecordRates struct {
	Sell    int64 // the message's selling price
	Buy     int64 // the vendor's price
	DLRSell int64 // one receipt's selling price, fixed at acceptance
	HLRSell int64 // one lookup's selling price, fixed at acceptance
	// FX is selling card currency → company currency; 0 = no conversion (the
	// card is in the company's currency, or nothing was priced).
	FX float64
	// The rest of the chain (27 Oct 2026), fixed when priced; 0 = not known:
	// BaseRate is the customer company's currency → the base currency;
	// BuyFX the buying card's currency → the vendor's own company's;
	// BuyBase the vendor's company's currency → the base currency.
	BaseRate, BuyFX, BuyBase float64
}

// RatesOf reads a message's rate ids off the price its record is written
// with (Msg.Priced: the audit JSON of the rating) and the quote fixed when it
// was accepted (Msg.Quote: the receipt's and the lookup's own prices).
func RatesOf(m model.Message) RecordRates {
	var r RecordRates
	if m.Priced != nil && m.Priced.Detail != "" {
		d := []byte(m.Priced.Detail)
		r.Sell, r.Buy, r.FX = RateIDOf(d, "sell"), RateIDOf(d, "buy"), fxOf(d)
		r.BaseRate, r.BuyFX, r.BuyBase = rateAt(d, "base"), rateAt(d, "buy_fx"), rateAt(d, "buy_base")
	}
	if q, ok := ParseQuote(m.Quote); ok {
		if r.Sell == 0 && q.MT != nil {
			r.Sell = RateIDOf(q.MT.Detail, "sell")
			if r.FX == 0 {
				r.FX = fxOf(q.MT.Detail)
			}
		}
		// The base rate is the one fixed at acceptance, where the quote has it.
		if q.MT != nil && q.MT.B > 0 {
			r.BaseRate = q.MT.B
		} else if q.MT != nil && r.BaseRate == 0 {
			r.BaseRate = rateAt(q.MT.Detail, "base")
		}
		if q.DLR != nil {
			r.DLRSell = RateIDOf(q.DLR.Detail, "sell")
		}
		if q.HLR != nil {
			r.HLRSell = RateIDOf(q.HLR.Detail, "sell")
		}
	}
	return r
}

// rateAt is the rate an audit JSON names under key ({"rate": …}); 0 when
// none.
func rateAt(detail []byte, key string) float64 {
	if len(detail) == 0 {
		return 0
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(detail, &m) != nil || len(m[key]) == 0 {
		return 0
	}
	var v struct {
		Rate float64 `json:"rate"`
	}
	if json.Unmarshal(m[key], &v) != nil || v.Rate <= 0 {
		return 0
	}
	return v.Rate
}

// fxOf is the conversion rate an audit JSON names ("fx": {"rate": …}); 0
// when it names none.
func fxOf(detail []byte) float64 {
	if len(detail) == 0 {
		return 0
	}
	var v struct {
		FX *FXRate `json:"fx"`
	}
	if json.Unmarshal(detail, &v) != nil || v.FX == nil || v.FX.Rate <= 0 {
		return 0
	}
	return v.FX.Rate
}

// BookedRates is what a receipt's bookings were priced by: the receipt fee's
// selling rate and the vendor's receipt cost (the DLR events among bs). 0 =
// that side was not booked by this receipt.
func BookedRates(bs []Booking) (sell, buy int64) {
	for _, b := range bs {
		for _, e := range b.Events {
			if e.Kind != EventDLR {
				continue
			}
			if e.Sell != nil && e.Sell.RateID != 0 && e.Sell.Units > 0 {
				sell = e.Sell.RateID
			}
			if e.Buy != nil && e.Buy.RateID != 0 && e.Buy.Units > 0 {
				buy = e.Buy.RateID
			}
		}
	}
	return sell, buy
}
