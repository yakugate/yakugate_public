package ratelimit

import (
	"sync"
	"time"
)

// Usage is the quota-usage counter (chunk-6 ruling 6.1, 2026-08-04): before
// it existed, quota enforcement read only the DB-stored daily_used/
// monthly_used JSON — which NOTHING ever incremented, so no quota could ever
// trip on live traffic. Keys are period-stamped by the caller (e.g.
// "qd:co:7:20260804", "qm:cc:92:202608"), which makes day/month rollover
// automatic: a new period is simply a new key. Units are SEGMENTS, matching
// rate-limiting and billing (flagged in D-095 — flip to messages is a one-line
// change at the call site if the PO rules otherwise).
//
// In-memory and single-node; SharedUsage mirrors it cluster-wide under the
// YAKU_REDIS_ADDR gate, same split as RateLimiter/Shared. A process restart
// loses the in-memory counts (quota becomes approximate); the Redis variant
// persists across restarts and instances.
type Usage struct {
	mu        sync.Mutex
	counts    map[string]*usageEntry
	lastSweep time.Time
	idle      time.Duration    // 0 = usageIdle
	now       func() time.Time // injectable for tests
}

type usageEntry struct {
	n     int
	touch time.Time
}

// usageIdle is how long an untouched key survives before the sweep drops it —
// long enough that a monthly key quiet for a few days is never lost, short
// enough that dead period keys don't accumulate forever.
const usageIdle = 45 * 24 * time.Hour

// DestCapKeyLife is how long a per-destination frequency-cap key survives
// (config/0096). Two days comfortably outlives the longest of its windows (a
// UTC day) with room for clock skew, and is short enough that the huge number
// of destination keys never accumulates. Exported because both the HTTP and
// SMPP servers build the counter and must agree on it.
const DestCapKeyLife = 48 * time.Hour

// NewUsageIdle is NewUsage with a shorter key lifetime.
//
// The 45-day default suits quota, whose keys are one per account (or company,
// or country) per period — a handful. The per-destination frequency cap keys
// on the DESTINATION too, so at any real volume that is millions of keys, and
// an hourly key is dead a few hours later. Holding those for 45 days would be
// a slow memory leak, so that counter passes its own idle here.
func NewUsageIdle(idle time.Duration) *Usage {
	u := NewUsage()
	u.idle = idle
	return u
}

// NewUsage returns an empty in-memory usage counter.
func NewUsage() *Usage {
	return &Usage{counts: map[string]*usageEntry{}, now: time.Now}
}

// Add records n units of usage against key.
func (u *Usage) Add(key string, n int) {
	if key == "" || n <= 0 {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	now := u.now()
	u.sweep(now)
	e := u.counts[key]
	if e == nil {
		e = &usageEntry{}
		u.counts[key] = e
	}
	e.n += n
	e.touch = now
}

// Get returns the live count for key (0 if never seen).
func (u *Usage) Get(key string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	if e := u.counts[key]; e != nil {
		return e.n
	}
	return 0
}

// sweep drops keys idle past this counter's idle window (usageIdle unless
// NewUsageIdle set a shorter one) — dead period stamps (yesterday's daily
// keys, last month's monthly keys) must not accumulate for the life of the
// process. Paced hourly; caller holds the mutex.
func (u *Usage) sweep(now time.Time) {
	if now.Sub(u.lastSweep) < time.Hour {
		return
	}
	u.lastSweep = now
	idle := u.idle
	if idle <= 0 {
		idle = usageIdle
	}
	for k, e := range u.counts {
		if now.Sub(e.touch) >= idle {
			delete(u.counts, k)
		}
	}
}
