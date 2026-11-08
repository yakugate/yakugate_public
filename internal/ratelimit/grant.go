package ratelimit

import (
	"fmt"
	"time"
)

// Granter hands out up to limit units per second for a key, part of a
// request when that is all that is left (2026-09-14, per-account MO and DLR
// throughput). Unlike Allow, a refused request spends nothing, and units that
// were granted but not used can be handed back — so the queue drainers can ask
// "how many may I take now?", take that many, and return the rest.
type Granter interface {
	Grant(key string, limit, want int) int
	Return(key string, n int)
}

// Grant returns how many of want units key may use in the current second.
// limit <= 0 means no limit: all of want.
func (rl *RateLimiter) Grant(key string, limit, want int) int {
	if want < 1 {
		return 0
	}
	if limit <= 0 {
		return want
	}
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	w := rl.windows[key]
	if w == nil || now.Sub(w.start) >= time.Second {
		w = &window{start: now}
		rl.windows[key] = w
	}
	left := limit - w.count
	if left <= 0 {
		return 0
	}
	if want < left {
		left = want
	}
	w.count += left
	return left
}

// Return hands back n granted units that were not used, in the same second.
func (rl *RateLimiter) Return(key string, n int) {
	if n < 1 {
		return
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if w := rl.windows[key]; w != nil && time.Since(w.start) < time.Second {
		w.count -= n
		if w.count < 0 {
			w.count = 0
		}
	}
}

// MOKey and DLRKey name an account's incoming-message and receipt buckets
// on one gateway: mo:<acct>@<gateway>, dlr:<acct>@<gateway>. They are shared
// by that gateway's bind leg (smpp-server) and its webhook leg (mo and dlr
// roles), so the limit covers the account ON ONE GATEWAY, both legs together
// — the same as the MT limit, which is per deployment too (config/0123).
// Before 4 Nov 2026 the key was the account alone, so two gateways drew from
// one bucket and the first to ask each second could hold every unit with
// nothing to deliver (tw-2 held yaku_test's MO). An empty gateway keeps the
// old key.
func MOKey(accountID int64, gateway string) string  { return paceKey("mo", accountID, gateway) }
func DLRKey(accountID int64, gateway string) string { return paceKey("dlr", accountID, gateway) }

func paceKey(kind string, accountID int64, gateway string) string {
	if gateway == "" {
		return fmt.Sprintf("%s:%d", kind, accountID)
	}
	return fmt.Sprintf("%s:%d@%s", kind, accountID, gateway)
}
