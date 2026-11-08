package monitor

import (
	"sort"
	"sync"
	"time"
)

// What we pushed TO each customer, per account (PO 2026-09-16).
//
// Delivery (delivery.go) answers "is this leg healthy" for the whole gateway.
// This answers the question an operator actually gets asked — "is MY traffic
// reaching me" — which is per customer, and which nothing could answer before:
// a single customer whose endpoint was refusing everything was invisible
// beside nine healthy ones, because their failures were summed into the same
// number.
//
// Counts only, no latency ring: a Counter is 300 int64s, so the three here
// cost about seven kilobytes per active account, while a latency ring would
// cost thirty per account for a figure the gateway-wide section already gives.
// Accounts idle for an hour are dropped on read, so the map tracks who is
// actually sending rather than everyone who ever has.

const pushAccountIdle = time.Hour

// PushAccounts is one role's per-account view of its push leg. The zero value
// is ready to use; MO and receipts each have their own.
type PushAccounts struct {
	mu sync.Mutex
	by map[int64]*accountPush
}

type accountPush struct {
	delivered, failed, gaveUp Counter
	lastOK, lastErrAt         time.Time
	lastErr                   string
}

// tally returns the account's counters, creating them on first sight.
// Caller holds mu.
func (p *PushAccounts) tally(accountID int64) *accountPush {
	if p.by == nil {
		p.by = map[int64]*accountPush{}
	}
	a := p.by[accountID]
	if a == nil {
		a = &accountPush{}
		p.by[accountID] = a
	}
	return a
}

// NoteOK records one message that reached this customer's endpoint.
func (p *PushAccounts) NoteOK(accountID int64) {
	if accountID == 0 {
		return
	}
	p.mu.Lock()
	a := p.tally(accountID)
	a.lastOK = time.Now()
	p.mu.Unlock()
	a.delivered.Add(1)
}

// NoteFailed records one failed attempt for this customer. The message may
// still be retried — NoteGaveUp is what says it was abandoned.
func (p *PushAccounts) NoteFailed(accountID int64, reason string) {
	if accountID == 0 {
		return
	}
	if len(reason) > 160 {
		reason = reason[:160] + "…"
	}
	p.mu.Lock()
	a := p.tally(accountID)
	a.lastErr, a.lastErrAt = reason, time.Now()
	p.mu.Unlock()
	a.failed.Add(1)
}

// NoteGaveUp records one message abandoned for this customer for good.
func (p *PushAccounts) NoteGaveUp(accountID int64) {
	if accountID == 0 {
		return
	}
	p.mu.Lock()
	a := p.tally(accountID)
	p.mu.Unlock()
	a.gaveUp.Add(1)
}

// Section lists one row per account with traffic in the window: totals, rates,
// and the last thing that went wrong. nil when there has been none, so a role
// that pushes nothing adds nothing to its snapshot.
func (p *PushAccounts) Section() []map[string]any {
	now := time.Now()
	p.mu.Lock()
	ids := make([]int64, 0, len(p.by))
	for id, a := range p.by {
		// Idle for an hour: forget them rather than carry every account that
		// has ever sent for as long as the process lives.
		if last := latest(a.lastOK, a.lastErrAt); !last.IsZero() && now.Sub(last) > pushAccountIdle {
			delete(p.by, id)
			continue
		}
		ids = append(ids, id)
	}
	rows := make([]map[string]any, 0, len(ids))
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		a := p.by[id]
		row := map[string]any{
			"account_id": id,
			"delivered":  a.delivered.Snapshot(0),
			"failed":     a.failed.Snapshot(0),
			"gave_up":    a.gaveUp.Snapshot(0),
		}
		if !a.lastOK.IsZero() {
			row["last_delivered_at"] = a.lastOK.UTC().Format(time.RFC3339)
		}
		if a.lastErr != "" {
			row["last_error"] = a.lastErr
			row["last_error_at"] = a.lastErrAt.UTC().Format(time.RFC3339)
		}
		rows = append(rows, row)
	}
	p.mu.Unlock()
	if len(rows) == 0 {
		return nil
	}
	return rows
}

func latest(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
