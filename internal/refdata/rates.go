// Rate cards: the rate row and the cache's buy and sell price lookups.

package refdata

import (
	"time"

	"github.com/yakugate/yakugate/internal/ratematch"
)

// RateRow is one priced destination in a buying rate card (LCR). [SCOPE P1-15 LCR]
type RateRow struct {
	MCC     string
	MNC     string
	Country string // destination_country_code (prefix match fallback)
	Price   float64
	// Kind/Currency/ConvRate/CRPRef exist for loss detection (S-03, PO
	// 2026-09-03) — LoadRates used to load ONLY kind='buy' rows (LCR's one
	// consumer), so these were never needed. Widened to load both kinds; the
	// pre-existing LCR path (RateFor) still filters to Kind=="buy" explicitly
	// now, so its behaviour is unchanged.
	Kind     string // "buy" | "sell" — rate_cards.kind
	Currency string // rate_cards.currency, the CARD's own currency
	ConvRate float64
	CRPRef   string // thirdparty_crp_id; "" = pinned ConvRate only
	// ConvTo is the currency ConvRate (and the row's provider) converts INTO:
	// the currency of the company that owns the card. For a selling card that
	// is the customer; for a buying card it is the VENDOR's company — so the
	// row's own rate says nothing about the customer's currency unless the two
	// are the same (QA billing report #2, 2026-10-07). "" = not known (a card
	// with no company, or a snapshot written before this field).
	ConvTo string `json:",omitempty"`
	// OwnerBaseRate / OwnerBaseCRP are the owning company's base conversion
	// rate and base currency provider (see Account.CompanyBaseRate): the last
	// step of card currency -> owner's currency -> system base currency.
	OwnerBaseRate float64 `json:",omitempty"`
	OwnerBaseCRP  string  `json:",omitempty"`
	// ID, TaxPct and EffectiveFrom let the match below be billing's own
	// (billing.bestRate, QA EP-017/RR-001/SMPP-003, 2026-10-01): ties go to
	// the later effective_from, then the lower id; a row whose
	// effective_from is still ahead is skipped at match time; and a price is
	// GROSS (net + tax%), as billing records both sides.
	ID            int64
	TaxPct        float64
	EffectiveFrom time.Time // zero = always
}

// Gross is the total for parts parts: net price plus the row's tax — the
// amount billing records for this side (GW-13).
func (r RateRow) Gross(parts int) float64 {
	if parts < 1 {
		parts = 1
	}
	net := r.Price * float64(parts)
	return net + net*r.TaxPct/100
}

// RateFor returns the buying price-per-segment for a rate card and destination,
// matched the way billing matches (rateMatch: most specific row, the "Any
// destination" row included). ok=false when the card has no matching BUY row.
// [SCOPE P1-15 LCR]
//
// Filters to Kind=="buy" explicitly (2026-09-03): LoadRates used to load only
// buy-kind rows via its SQL WHERE clause, so every row here was implicitly a
// buy row; it was widened to also carry sell rows for loss detection (S-03),
// so this filter is what keeps LCR's behaviour byte-for-byte unchanged.
func (m *Mem) RateFor(cardID int64, mcc, mnc, dest string) (float64, bool) {
	row, ok := m.RateRowFor(cardID, "buy", mcc, mnc, dest)
	return row.Price, ok
}

// SellRateFor mirrors RateFor for a card's sell-kind rows (S-03, PO
// 2026-09-03: loss detection). ok=false when the card has no matching SELL
// row — including when cardID is 0 (no sell card configured), which the PO
// ruled must be treated as "unpriced," not "a loss."
func (m *Mem) SellRateFor(cardID int64, mcc, mnc, dest string) (float64, bool) {
	row, ok := m.RateRowFor(cardID, "sell", mcc, mnc, dest)
	return row.Price, ok
}

// RateRowFor is RateFor/SellRateFor's full-fidelity form: the whole matched
// row, not just its price. Loss detection (S-03) needs the row's own
// Currency/ConvRate/CRPRef — the SAME per-row FX pinning billing.Rater uses
// for the real charge — not only the price, so this is the one place both
// RateFor and SellRateFor actually delegate to.
func (m *Mem) RateRowFor(cardID int64, kind, mcc, mnc, dest string) (RateRow, bool) {
	return rateMatch(m.rateRows(cardID, kind), mcc, mnc, dest)
}

// CardRateFor is the row billing charges for this card and number: the same
// match, over every row of the card whatever its kind — billing.Rater prices
// whichever card the rule names, so LCR and the loss check do too.
func (m *Mem) CardRateFor(cardID int64, mcc, mnc, dest string) (RateRow, bool) {
	if cardID == 0 {
		return RateRow{}, false
	}
	m.mu.RLock()
	rows := m.rates[cardID]
	m.mu.RUnlock()
	return rateMatch(rows, mcc, mnc, dest)
}

// PricedCard is the first of ids that prices anything — enabled and holding
// at least one open rate, billing.Rater's "usable" — or 0 when none does. A
// card the operator disabled, or one with no rate yet, is stepped past the
// way billing steps past it, so LCR and the loss check read the card the
// charge will really come from.
func (m *Mem) PricedCard(ids ...int64) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, id := range ids {
		if id > 0 && len(m.rates[id]) > 0 {
			return id
		}
	}
	return 0
}

// CardCurrency returns a rate card's own currency (rate_cards.currency,
// uniform across every row of one card) — ok=false when the card has no
// loaded rows at all (disabled, or an id that does not exist).
func (m *Mem) CardCurrency(cardID int64) (string, bool) {
	m.mu.RLock()
	rows := m.rates[cardID]
	m.mu.RUnlock()
	if len(rows) == 0 {
		return "", false
	}
	return rows[0].Currency, true
}

// rateRows returns cardID's rows of the given kind ("buy" | "sell").
func (m *Mem) rateRows(cardID int64, kind string) []RateRow {
	if cardID == 0 {
		return nil
	}
	m.mu.RLock()
	all := m.rates[cardID]
	m.mu.RUnlock()
	out := make([]RateRow, 0, len(all))
	for _, r := range all {
		if r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

// rateMatch is billing's own row choice — internal/ratematch, which
// billing.Rater prices by too — so the price LCR and the loss check read is
// the price the billing role charges (QA EP-017/RR-001/SMPP-003, 2026-10-01).
// It used to be written here a second time and knew only network and
// dialling-code rows, so a card priced by its "Any destination" row looked
// unpriced: LCR fell back to the first rule and the loss check never ran.
// The most specific row wins, the "Any destination" row prices what nothing
// more specific does, and a row whose effective_from is still ahead is
// skipped (the rule itself is in internal/ratematch).
func rateMatch(rows []RateRow, mcc, mnc, dest string) (RateRow, bool) {
	i, ok := ratematch.Best(rows, func(r *RateRow) ratematch.Key {
		return ratematch.Key{MCC: r.MCC, MNC: r.MNC, Prefix: r.Country, From: r.EffectiveFrom, ID: r.ID}
	}, mcc, mnc, dest, time.Now().UTC())
	if !ok {
		return RateRow{}, false
	}
	return rows[i], true
}
