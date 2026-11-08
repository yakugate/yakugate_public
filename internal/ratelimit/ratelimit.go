package ratelimit

import (
	"sync"
	"time"
)

// RateLimiter is a fixed-window (1-second) SEGMENT counter per string key
// (PO 2026-07-26): "co:<id>" = company-shared bucket (every account under one
// company draws from the same limit), "acct:<id>" = unlinked account,
// "cc:<prefix>" = per-destination-country bucket. A limit <= 0 means
// unlimited. Safe for concurrent use. Single-node and in-memory; Shared (the
// DAL's buckets) backs a clustered deployment. [SCOPE P1-03]
type RateLimiter struct {
	mu        sync.Mutex
	windows   map[string]*window
	lastSweep time.Time
}

type window struct {
	start time.Time
	count int
}

// New returns an empty RateLimiter.
func New() *RateLimiter {
	return &RateLimiter{windows: map[string]*window{}}
}

// Allow reports whether spending n segments against key stays within its
// per-second limit, recording the spend either way (the request that tips
// over the cap is rejected and its segments still occupy the window).
func (rl *RateLimiter) Allow(key string, limit, n int) bool {
	if limit <= 0 {
		return true
	}
	if n < 1 {
		n = 1
	}
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	// Evict windows whose second has long passed (chunk-6 ruling 6.5, same
	// hygiene as the lockout sweep): keys are bounded by tenants × countries,
	// so this is slow-growth cleanup, paced to stay off the hot path.
	if now.Sub(rl.lastSweep) >= time.Minute {
		rl.lastSweep = now
		for k, old := range rl.windows {
			if now.Sub(old.start) >= 2*time.Second {
				delete(rl.windows, k)
			}
		}
	}
	w := rl.windows[key]
	if w == nil || now.Sub(w.start) >= time.Second {
		w = &window{start: now}
		rl.windows[key] = w
	}
	w.count += n
	return w.count <= limit
}
