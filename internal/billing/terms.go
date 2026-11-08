package billing

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Terms is every account's and every vendor's billing terms, as the moments
// that book money need them away from the accept path (a vendor's answer, a
// receipt, an incoming message): an account's company, currency, charge
// point, receipt charging and model; a vendor's charge point. It was the
// billing role's per-sweep read (loadAcctBilling); each process that books
// now keeps it, read again every termsEvery, so a change applies within half
// a minute as the reference cache's does.
type Terms struct {
	db dal.BillingTerms

	mu      sync.RWMutex
	accts   map[int64]AccountTerms
	vendors map[int64]string
	at      time.Time
}

// termsEvery is how often Terms is read again.
const termsEvery = 30 * time.Second

// NewTerms reads terms from the config store.
func NewTerms(cfgDB dal.BillingTerms) *Terms {
	if cfgDB == nil {
		return nil
	}
	return &Terms{db: cfgDB, accts: map[int64]AccountTerms{}, vendors: map[int64]string{}}
}

// Account is an account's terms; the zero value (company 0) when unknown.
func (t *Terms) Account(id int64) AccountTerms {
	if t == nil {
		return AccountTerms{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.accts[id]
}

// VendorPoint is a vendor's charge point (acceptance when unknown).
func (t *Terms) VendorPoint(id int64) string {
	if t == nil {
		return VendorAcceptance
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return NormalVendorPoint(t.vendors[id])
}

// Loaded says the terms have been read at least once.
func (t *Terms) Loaded() bool {
	if t == nil {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return !t.at.IsZero()
}

// Run reads the terms now and every termsEvery until ctx ends. A failed read
// keeps the last terms (logged).
func (t *Terms) Run(ctx context.Context, logf func(string, ...any)) {
	if t == nil {
		return
	}
	tick := time.NewTicker(termsEvery)
	defer tick.Stop()
	for {
		if err := t.Reload(ctx); err != nil && logf != nil && ctx.Err() == nil {
			logf("billing terms: %v (the last ones read are kept)", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// Reload reads every account's and vendor's terms.
func (t *Terms) Reload(ctx context.Context) error {
	accts := map[int64]AccountTerms{}
	rows, err := t.db.AccountTerms(ctx)
	if err != nil {
		return err
	}
	for _, r := range rows {
		a := AccountTerms{Company: r.Company, Currency: r.Currency, Point: r.Point, DLRMode: r.DLRMode,
			Prepaid: r.Prepaid, MOSellCard: r.MOSellCard, MTSellCard: r.MTSellCard, DLRSellCard: r.DLRSellCard}
		a.Currency = strings.ToUpper(a.Currency)
		a.Point = NormalPoint(a.Point)
		accts[r.ID] = a
	}
	vendors := map[int64]string{}
	// A store without config/0150 yet answers none: every vendor charges on
	// acceptance.
	points, err := t.db.VendorPoints(ctx)
	if err != nil {
		return err
	}
	for id, p := range points {
		vendors[id] = NormalVendorPoint(p)
	}
	t.mu.Lock()
	t.accts, t.vendors, t.at = accts, vendors, time.Now()
	t.mu.Unlock()
	return nil
}

// ModelOf is the model of a sell side for an account: prepaid or postpaid.
func (a AccountTerms) ModelOf() string {
	if a.Prepaid {
		return ModelPrepaid
	}
	return ModelPostpaid
}
