package pipeline

import (
	"context"
	"sync"
	"time"
)

// --- stage 13/18: credit / balance --- [SCOPE P1-12]
type creditStage struct {
	// fresh reads the balance as it stands now, asked only when the
	// reference cache's copy says nothing is left. nil = the copy decides.
	fresh *freshBalance
}

func (creditStage) Name() string { return "credit" }

// Settles against the TENANT balance — the only balance there is now
// (2026-07-28, migration 0044/0045 removed the per-account fallback). An
// account with no company link can never be Prepaid()==true, so it always
// passes this gate (treated as postpaid, not free of charge — P9 still rates
// and invoices it). Charging itself is P9's job (D-033) — this is only the gate.
//
// The reference cache's copy of the balance is up to 30 s old. A refusal is
// checked against the balance as it stands (fresh) before it is given:
// after a top-up from 0 every send was refused 402 for 12–30 s, the customer
// having just paid (go-live test 18 Oct, P1 C3, P9 C3). A send the copy lets
// through is not read again: the hold at acceptance reads the live balance
// itself, and a balance may go below zero by what billing takes later.
func (s creditStage) Run(ctx context.Context, e *Envelope) *Reject {
	if !e.Account.Prepaid() || e.Account.EffectiveBalance() > 0 {
		return nil
	}
	if b, ok := s.fresh.balance(ctx, e.Account.CompanyID); ok && b > 0 {
		return nil
	}
	return reject(402, "INSUFFICIENT_BALANCE")
}

// BalanceReader says a company's balance as it stands now. Both billing
// reservers do: the live Redis counter when there is one, else
// companies.balance.
type BalanceReader interface {
	Balance(ctx context.Context, companyID int64) (float64, error)
}

// freshBalance asks a BalanceReader, at most once a second per company: a
// customer at 0 that keeps sending must not cost a read per refusal.
type freshBalance struct {
	r   BalanceReader
	ttl time.Duration

	mu sync.Mutex
	m  map[int64]freshEntry
}

type freshEntry struct {
	v  float64
	at time.Time
}

// freshBalanceEvery is how long one answer of the balance is used.
const freshBalanceEvery = time.Second

// newFreshBalance is a freshBalance over r when r can say a balance, else nil.
func newFreshBalance(r any) *freshBalance {
	br, ok := r.(BalanceReader)
	if !ok || br == nil {
		return nil
	}
	return &freshBalance{r: br, ttl: freshBalanceEvery, m: map[int64]freshEntry{}}
}

// balance is the company's balance now, or within the last ttl. ok=false
// when it cannot be read (the cached copy then decides, as before).
func (f *freshBalance) balance(ctx context.Context, companyID int64) (float64, bool) {
	if f == nil || companyID == 0 {
		return 0, false
	}
	f.mu.Lock()
	if e, hit := f.m[companyID]; hit && time.Since(e.at) < f.ttl {
		f.mu.Unlock()
		return e.v, true
	}
	f.mu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	v, err := f.r.Balance(cctx, companyID)
	if err != nil {
		return 0, false
	}
	f.mu.Lock()
	f.m[companyID] = freshEntry{v: v, at: time.Now()}
	f.mu.Unlock()
	return v, true
}
