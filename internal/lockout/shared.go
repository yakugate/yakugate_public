package lockout

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Shared is the cross-instance ladder under the YAKU_REDIS_ADDR gate
// (2026-07-24): failure counts, escalation levels and lock flags are kept by
// the DAL (dal.Blocks: the main Redis, else the SQL database), so an attacker spraying
// guesses across N gateway instances climbs ONE ladder instead of N. Same
// policy as the in-memory Ladder (rungs 1m/5m/30m/60m, escalate on re-trip
// within 15 min of release, decay after a clean hour, Reset keeps the
// level). It was lockout.Redis until 29 Oct 2026; its scripts moved into
// internal/dal/redis unchanged and the rules are handed to them from here.
type Shared struct {
	b     dal.Blocks
	rules dal.LadderRules
}

// NewShared mirrors New's threshold policy over the DAL's blocks.
// Identity ladder: NewShared(b, 5); IP ladder: NewShared(b, 20).
func NewShared(b dal.Blocks, threshold int) *Shared {
	return &Shared{b: b, rules: dal.LadderRules{
		Threshold:      threshold,
		Rungs:          [4]time.Duration{rungs[0], rungs[1], rungs[2], rungs[3]},
		DecayAfter:     decayAfter,
		EscalateWithin: escalateWithin,
	}}
}

// callFor is how long one call to the store may take.
const callFor = 2 * time.Second

// Locked reports whether the identity is currently locked, and for how much
// longer. Fails OPEN on store errors — auth still verifies credentials; only
// the brute-force throttle degrades during a blip.
func (r *Shared) Locked(id string) (bool, time.Duration) {
	if id == "" {
		return false, 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	left, err := r.b.LockedFor(ctx, id)
	if err != nil || left <= 0 {
		return false, 0
	}
	return true, left
}

// Fail records a failed attempt; on a trip it reports the lock duration and
// the escalation level reached. Fails OPEN on store errors.
func (r *Shared) Fail(id string) (locked bool, retryAfter time.Duration, level int) {
	if id == "" {
		return false, 0, 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	locked, retryAfter, level, err := r.b.Fail(ctx, id, r.rules, time.Now())
	if err != nil {
		return false, 0, 0
	}
	return locked, retryAfter, level
}

// Reset clears the failure counter after a successful auth — the escalation
// level is deliberately left to expire on its own (Reset keeps the level).
func (r *Shared) Reset(id string) {
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	_ = r.b.ResetFails(ctx, id)
}

// Clear is the operator's manual unblock (P1 scope finalize 2026-08-09 §7):
// removes the active lock, the failure counter, the escalation level AND the
// escalation window — cluster-wide, since every instance shares them. The
// id leaves the list of who is locked with them.
func (r *Shared) Clear(id string) {
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	_ = r.b.ClearBlock(ctx, id)
}

// SharedSprayGuard is SprayGuard's cross-instance mirror (YAKU_REDIS_ADDR
// gate): the distinct identities seen from an address are kept by the DAL
// for the window, and escalation reuses Shared with threshold=1 exactly like
// the in-memory guard reuses Ladder. It was SprayGuardRedis until 29 Oct
// 2026.
type SharedSprayGuard struct {
	b         dal.Blocks
	threshold int
	ladder    *Shared
}

// NewSharedSprayGuard mirrors NewSprayGuard's policy over the DAL's blocks.
func NewSharedSprayGuard(b dal.Blocks, distinctThreshold int) *SharedSprayGuard {
	return &SharedSprayGuard{b: b, threshold: distinctThreshold, ladder: NewShared(b, 1)}
}

func (g *SharedSprayGuard) Locked(ip string) (bool, time.Duration) {
	return g.ladder.Locked(ip)
}

// Observe notes identity as failed from ip; when it is NEW and brings ip's
// distinct count to the threshold, the ladder trips. A repeat of an
// already-counted identity never re-trips on its own.
func (g *SharedSprayGuard) Observe(ip, identity string) (tripped bool, retryAfter time.Duration, level int) {
	if ip == "" || identity == "" {
		return false, 0, 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	added, count, err := g.b.SeeIdentity(ctx, ip, identity, window)
	if err != nil {
		return false, 0, 0
	}
	if !added || count < g.threshold {
		return false, 0, 0
	}
	return g.ladder.Fail(ip)
}

// Clear mirrors SprayGuard.Clear cluster-wide.
func (g *SharedSprayGuard) Clear(ip string) {
	if ip == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	_ = g.b.ClearIdentities(ctx, ip)
	g.ladder.Clear(ip)
}
