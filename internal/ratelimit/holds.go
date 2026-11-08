package ratelimit

import (
	"strconv"
	"sync"
	"time"
)

// Holds counts, per account, the checks where its MO or receipt rate on this
// gateway gave 0, so a customer queue held by its rate can be told from one
// that nothing takes out (4 Nov 2026: tw-2 held yaku_test's MO, and the
// queue read "nothing is being taken out"). Published as rate_held in the
// role's status; Note says when to log, at most once a minute an account.
type Holds struct {
	mu   sync.Mutex
	rows map[int64]*held
}

type held struct {
	count  int64
	lastAt int64 // unix seconds of the newest hold
	saidAt int64 // unix seconds of the newest log line
}

// Note counts one hold of acct and reports whether to log it now.
func (h *Holds) Note(acct int64) bool {
	now := time.Now().Unix()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rows == nil {
		h.rows = map[int64]*held{}
	}
	r := h.rows[acct]
	if r == nil {
		r = &held{}
		h.rows[acct] = r
	}
	r.count++
	r.lastAt = now
	if now-r.saidAt >= 60 {
		r.saidAt = now
		return true
	}
	return false
}

// HeldRow is one account's holds as the status shows them.
type HeldRow struct {
	Count  int64  `json:"count"`
	LastAt string `json:"last_at"` // RFC 3339 UTC
}

// Section is a copy of the counts keyed by account id; nil when none.
func (h *Holds) Section() map[string]HeldRow {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.rows) == 0 {
		return nil
	}
	out := make(map[string]HeldRow, len(h.rows))
	for a, r := range h.rows {
		out[strconv.FormatInt(a, 10)] = HeldRow{Count: r.count, LastAt: time.Unix(r.lastAt, 0).UTC().Format(time.RFC3339)}
	}
	return out
}
