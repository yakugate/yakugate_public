package billing

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// RatePrice is one rate version's gross price for one part, in its card's
// own currency (net + tax, GW-13): what a log row's rate id means.
type RatePrice struct {
	PerPart  float64
	Currency string
}

// RatePrices reads the gross price per part of the rate versions named,
// with their cards' currency. A rate_card_rates row is never changed (a new
// price is a new version), so what is read once stays true; Prices keeps
// them in memory.
func RatePrices(ctx context.Context, db dal.RateTables, ids []int64) (map[int64]RatePrice, error) {
	out := map[int64]RatePrice{}
	rows, err := db.RatePrices(ctx, ids)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		ppm, tax := r.PricePerPart, r.TaxPct
		out[r.ID] = RatePrice{PerPart: ppm + ppm*tax/100, Currency: strings.ToUpper(r.Currency)}
	}
	return out, nil
}

// Prices is RatePrices kept in memory: a rate version's price never changes,
// so each is read once (and a version not found is asked again after a
// minute — it may be one made a moment ago on another machine).
type Prices struct {
	db     dal.RateTables
	mu     sync.Mutex
	known  map[int64]RatePrice
	missed map[int64]time.Time
}

// NewPrices reads rate versions from the config store db.
func NewPrices(db dal.RateTables) *Prices {
	return &Prices{db: db, known: map[int64]RatePrice{}, missed: map[int64]time.Time{}}
}

// KnownPrices is Prices that answers only the prices given (a test's, or a
// caller that read them itself).
func KnownPrices(known map[int64]RatePrice) *Prices {
	p := &Prices{known: map[int64]RatePrice{}, missed: map[int64]time.Time{}}
	for id, v := range known {
		p.known[id] = v
	}
	return p
}

// Of is the price of each id it can find.
func (p *Prices) Of(ctx context.Context, ids []int64) map[int64]RatePrice {
	out := map[int64]RatePrice{}
	if p == nil {
		return out
	}
	var ask []int64
	p.mu.Lock()
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if v, ok := p.known[id]; ok {
			out[id] = v
		} else if t, ok := p.missed[id]; !ok || time.Since(t) > time.Minute {
			ask = append(ask, id)
		}
	}
	p.mu.Unlock()
	if len(ask) == 0 || p.db == nil {
		return out
	}
	got, err := RatePrices(ctx, p.db, ask)
	if err != nil {
		return out
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, id := range ask {
		if v, ok := got[id]; ok {
			p.known[id], out[id] = v, v
			delete(p.missed, id)
		} else {
			p.missed[id] = time.Now()
		}
	}
	return out
}
