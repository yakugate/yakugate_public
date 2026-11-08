package lockout

import (
	"sync"
	"time"
)

// SprayGuard detects a credential-stuffing/spray pattern: one source IP
// trying many DISTINCT identities (usernames/tokens/JWT subjects — whatever
// string internal/auth passes to Fail) within a short window. PO 2026-07-27:
// "if IP is different and person is using a different username, it must be
// blocked by first try" — a rotating-identity pattern from one IP IS the
// attack signature, so crossing the distinct-identity threshold trips an
// escalating ladder IMMEDIATELY (rung 1 on the first crossing, rung 2 on the
// next NEW distinct identity, rung 3/final after that), rather than waiting
// for the normal per-identity failure-count grace those ladders use. This is
// a THIRD, independent gate — separate from the identity ladder (5 fails/min,
// one username) and the IP volume throttle (20 fails/min, any username).
//
// In-memory and single-node; SharedSprayGuard mirrors the same policy across
// instances under the YAKU_REDIS_ADDR gate, same as Ladder/Shared. [SCOPE P1-AUTH]
type SprayGuard struct {
	mu        sync.Mutex
	seen      map[string]map[string]time.Time // ip -> identity -> last seen
	threshold int
	ladder    *Ladder // threshold=1: any Fail() call IS a trip
	now       func() time.Time
	lastSweep time.Time
}

// NewSprayGuard builds a guard that trips once distinctThreshold different
// identities have failed from the same IP inside the 1-minute window.
func NewSprayGuard(distinctThreshold int) *SprayGuard {
	return &SprayGuard{
		seen:      map[string]map[string]time.Time{},
		threshold: distinctThreshold,
		ladder:    New(1),
		now:       time.Now,
	}
}

// Locked mirrors Ladder.Locked, keyed by IP.
func (g *SprayGuard) Locked(ip string) (bool, time.Duration) {
	return g.ladder.Locked(ip)
}

// Observe records one failed-auth identity from ip. When a NEW distinct
// identity within the window pushes ip's distinct count to the threshold, it
// trips the escalating ladder immediately. A repeat of an identity already
// counted this window is not "distinct" growth and never re-trips on its own.
func (g *SprayGuard) Observe(ip, identity string) (tripped bool, retryAfter time.Duration, level int) {
	if ip == "" || identity == "" {
		return false, 0, 0
	}
	g.mu.Lock()
	now := g.now()
	// Cross-IP eviction sweep (chunk-2 ruling 2.4): the per-call prune below
	// only touches THIS ip's identity set — an IP that stops failing would
	// otherwise leave its set behind forever. Same pacing/cost trade-off as
	// Ladder.sweep; its own escalation ladder is swept by Ladder.sweep itself.
	if now.Sub(g.lastSweep) >= sweepEvery {
		g.lastSweep = now
		for oip, ids := range g.seen {
			for id, t := range ids {
				if now.Sub(t) >= window {
					delete(ids, id)
				}
			}
			if len(ids) == 0 {
				delete(g.seen, oip)
			}
		}
	}
	m := g.seen[ip]
	if m == nil {
		m = map[string]time.Time{}
		g.seen[ip] = m
	}
	for id, t := range m { // prune to the sliding window
		if now.Sub(t) >= window {
			delete(m, id)
		}
	}
	_, already := m[identity]
	m[identity] = now
	count := len(m)
	g.mu.Unlock()

	if already || count < g.threshold {
		return false, 0, 0
	}
	return g.ladder.Fail(ip)
}

// Clear is the operator's manual unblock for a sprayed-from IP (P1 scope
// finalize 2026-08-09 §7): drops the distinct-identity window and the block
// itself.
func (g *SprayGuard) Clear(ip string) {
	if ip == "" {
		return
	}
	g.mu.Lock()
	delete(g.seen, ip)
	g.mu.Unlock()
	g.ladder.Clear(ip)
}
