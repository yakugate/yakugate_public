// Package lockout provides Yakugate's shared authentication-failure ladders.
//
// Two ladders, same escalation mechanics, different thresholds (PO 2026-07-26):
//
//	Identity lock (threshold 5 fails / 1 min): per username / JWT subject /
//	token digest. Trip → 423 AUTH_LOCKED + Retry-After.
//	IP throttle (threshold 20 fails / 1 min): per source IP. Trip → pre-auth
//	429 + Retry-After — a THROTTLE, never a hard block: enterprise clients
//	share NAT egress IPs, so one hard block would punish every legitimate
//	account behind it; the looser threshold (20, not 5) is the honest floor
//	for an address that legitimately carries many accounts' traffic.
//
// Escalation rungs: 1 min → 5 min → 30 min → 60 min. A re-trip within 15
// minutes of the previous lock RELEASING climbs one rung; later re-trips
// repeat the current rung ("30–60 min cycles"). Permanent disable is never
// automatic — that is is_disabled=1, a human (or P10) decision. After 1 hour
// with no failures the escalation level decays back to the first rung. A
// successful auth clears the failure counter but KEEPS the escalation level —
// otherwise one lucky guess every few attempts would reset the defence.
//
// In-memory and single-node; lockout.Shared mirrors the same policy across
// instances under the YAKU_REDIS_ADDR gate. [SCOPE P1-AUTH]
package lockout

import (
	"sync"
	"time"
)

// rungs are the escalating lock durations. Beyond the last rung the ladder
// stays at the final duration (the "30–60 min cycles" regime).
var rungs = []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute, 60 * time.Minute}

// window is the sliding failure-count window; escalateWithin is how soon after
// a lock's RELEASE a re-trip must happen to climb a rung; decayAfter is how
// long an identity must stay failure-free for its escalation level to reset.
const (
	window         = time.Minute
	escalateWithin = 15 * time.Minute
	decayAfter     = time.Hour
	// sweepEvery paces the eviction sweep (chunk-2 ruling 2.4, 2026-08-03) —
	// see Ladder.sweep.
	sweepEvery = 30 * time.Second
)

// Ladder tracks failed-auth counts per identity and locks each one, for an
// escalating duration, once the threshold is hit within the window. Safe for
// concurrent use.
type Ladder struct {
	mu        sync.Mutex
	entries   map[string]*entry
	threshold int
	now       func() time.Time // injectable for tests
	lastSweep time.Time
}

type entry struct {
	fails    []time.Time
	level    int       // escalation rung already reached (0 = none / decayed)
	until    time.Time // locked until
	lastFail time.Time
}

// New returns a Ladder that trips once an identity reaches threshold failures
// inside the 1-minute window. Identity ladder: New(5); IP ladder: New(20).
func New(threshold int) *Ladder {
	return &Ladder{entries: map[string]*entry{}, threshold: threshold, now: time.Now}
}

// decay resets the escalation level after a clean hour. The hour counts from
// the LATER of the last failure and the lock's release — time spent locked is
// not "clean" (the 60-minute rung would otherwise always decay before it
// could ever repeat, contradicting the 30–60-min-cycles regime). Caller holds
// the mutex.
func (l *Ladder) decay(e *entry, now time.Time) {
	if e.level == 0 || !now.After(e.until) {
		return
	}
	since := e.lastFail
	if e.until.After(since) {
		since = e.until
	}
	if !since.IsZero() && now.Sub(since) >= decayAfter {
		e.level = 0
	}
}

// sweep drops entries holding no state worth keeping — unlocked, escalation
// decayed, no failure inside the window — so the map cannot grow without
// bound (chunk-2 ruling 2.4, 2026-08-03: every unique bad token/username/IP
// used to leave a permanent entry until restart, an easy memory exhaustion
// under a random-identity flood). Deleting such an entry is lossless: it is
// indistinguishable from one that never existed. Full scan, at most every
// sweepEvery, under the mutex the caller already holds — cost is proportional
// to the live attack rate (a few ms per 100k entries, at most twice a
// minute). The Redis mirror needs none of this: every key it writes carries
// its own TTL. The per-IP BLOCKING of such floods already exists and is not
// this sweep's job — the spray guard trips an IP on its 3rd distinct failing
// identity, and the volume ladder throttles it at 20 fails/min; the sweep
// only reclaims the memory those (and many-IP botnet floods, which no per-IP
// gate can prevent) leave behind.
func (l *Ladder) sweep(now time.Time) {
	if now.Sub(l.lastSweep) < sweepEvery {
		return
	}
	l.lastSweep = now
	for id, e := range l.entries {
		l.decay(e, now)
		if e.level != 0 || e.until.After(now) {
			continue
		}
		stale := true
		for _, t := range e.fails {
			if now.Sub(t) < window {
				stale = false
				break
			}
		}
		if stale {
			delete(l.entries, id)
		}
	}
}

// Locked reports whether the identity is currently locked, and for how much
// longer (the Retry-After value).
func (l *Ladder) Locked(id string) (bool, time.Duration) {
	if id == "" {
		return false, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.entries[id]
	if e == nil {
		return false, 0
	}
	now := l.now()
	l.decay(e, now)
	if e.until.After(now) {
		return true, e.until.Sub(now)
	}
	return false, 0
}

// Fail records a failed attempt. When the threshold trips it locks the
// identity for the current rung's duration and reports (locked, retryAfter,
// level) — level lets the caller fire an audit/alert at the third rung.
func (l *Ladder) Fail(id string) (locked bool, retryAfter time.Duration, level int) {
	if id == "" {
		return false, 0, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.sweep(now) // growth happens only here, so here is where eviction paces
	e := l.entries[id]
	if e == nil {
		e = &entry{}
		l.entries[id] = e
	}
	l.decay(e, now)

	// prune to the sliding window, then record this failure
	keep := e.fails[:0]
	for _, t := range e.fails {
		if now.Sub(t) < window {
			keep = append(keep, t)
		}
	}
	e.fails = append(keep, now)
	e.lastFail = now

	if len(e.fails) < l.threshold {
		return false, 0, e.level
	}

	// Trip. Climb a rung only when this trip lands within escalateWithin of
	// the previous lock RELEASING; otherwise repeat the current rung.
	switch {
	case e.level == 0:
		e.level = 1
	case now.Sub(e.until) <= escalateWithin && e.level < len(rungs):
		e.level++
	}
	dur := rungs[e.level-1]
	e.until = now.Add(dur)
	e.fails = nil // the lock replaces the counter
	return true, dur, e.level
}

// Reset clears the failure counter after a successful auth — but KEEPS the
// escalation level, so a lucky guess between lockouts does not reset the
// ladder. The level decays on its own after a clean hour.
func (l *Ladder) Reset(id string) {
	if id == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.entries[id]; e != nil {
		e.fails = nil
	}
}

// Clear removes id's entry entirely — active lock, failure count, AND
// escalation level. This is the operator's manual unblock (P1 scope finalize
// 2026-08-09 §7), deliberately stronger than Reset: the operator has decided
// the block is wrong or resolved, so nothing should survive.
func (l *Ladder) Clear(id string) {
	if id == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.entries, id)
}
