package ratelimit

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Shared is the cluster-accurate rate limiter (2026-07-24): every gateway
// instance draws from one per-key per-second bucket kept by the DAL
// (dal.Limits: the main Redis, else the SQL database), so N instances enforce ONE limit
// instead of N — the in-memory limiter's documented single-node gap. Keys
// and units follow the in-memory limiter (segments per second per string
// key, PO 2026-07-26). It was ratelimit.Redis until 29 Oct 2026; the
// scripts moved into internal/dal/redis unchanged.
//
// It FAILS OPEN on store errors: a transient blip must degrade rate
// enforcement, not availability — the same posture the HLR store takes.
type Shared struct{ l dal.Limits }

// NewShared draws from the DAL's buckets.
func NewShared(l dal.Limits) *Shared { return &Shared{l: l} }

// callFor is how long one bucket call may take.
const callFor = 2 * time.Second

// Allow reports whether spending n segments against key stays within its
// per-second limit, spending them either way.
func (r *Shared) Allow(key string, limit, n int) bool {
	if limit <= 0 {
		return true
	}
	if n < 1 {
		n = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	got, err := r.l.Spend(ctx, key, time.Now().Unix(), n)
	if err != nil {
		return true
	}
	return got <= limit
}

// Take spends up to max against key and returns how much fitted under its
// per-second limit — ONE call for a whole batch. A sender asking for its
// budget one unit at a time (Allow in a loop) paid a round trip per message:
// two thousand a second for a vendor allowed two thousand a second (load
// test 2026-10-07). Fails open like Allow.
func (r *Shared) Take(key string, limit, max int) int {
	if limit <= 0 || max <= 0 {
		return max
	}
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	got, err := r.l.Take(ctx, key, time.Now().Unix(), limit, max)
	if err != nil {
		return max
	}
	return got
}

// Grant is the cluster-wide Grant: every gateway instance draws from one
// per-second bucket, and two can never both take the last units. It fails
// OPEN on store errors, like Allow.
func (r *Shared) Grant(key string, limit, want int) int {
	if want < 1 {
		return 0
	}
	if limit <= 0 {
		return want
	}
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	got, err := r.l.Grant(ctx, key, time.Now().Unix(), limit, want)
	if err != nil {
		return want
	}
	return got
}

// Return hands back unused units in the current second's bucket, only to a
// bucket that is still there. A return that comes a second late is simply
// dropped: the second it belonged to is over.
func (r *Shared) Return(key string, n int) {
	if n < 1 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	_ = r.l.Return(ctx, key, time.Now().Unix(), n)
}

// SharedUsage is Usage's cluster-accurate mirror: period-stamped keys kept
// by the DAL (dal.Usage: the main Redis, else the SQL database) with a life comfortably past
// their period, so counts survive restarts and are shared across instances —
// exactly the properties quota enforcement wants and the in-memory counter
// can't give. It was ratelimit.UsageRedis until 29 Oct 2026. FAILS OPEN on
// store errors (Get 0 / best-effort Add), the same posture as Shared: a blip
// degrades quota accuracy, never availability.
type SharedUsage struct {
	u    dal.Usage
	life time.Duration // 0 = usageTTL
}

// NewSharedUsage counts on the DAL's usage keys.
func NewSharedUsage(u dal.Usage) *SharedUsage { return &SharedUsage{u: u} }

// NewSharedUsageTTL is NewSharedUsage with a shorter key life — the shared
// half of NewUsageIdle's reasoning. Per-destination cap keys are one per
// (account, destination, period), so they must expire soon after their
// period ends rather than sit for the 45 days a monthly quota key needs.
func NewSharedUsageTTL(u dal.Usage, life time.Duration) *SharedUsage {
	return &SharedUsage{u: u, life: life}
}

// usageTTL outlives any monthly period key (31d) with margin; period stamps
// in the key mean an expired-then-recreated key simply starts at zero for a
// period that is already over.
const usageTTL = 45 * 24 * time.Hour

func (s *SharedUsage) Add(key string, n int) {
	if key == "" || n <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	life := s.life
	if life <= 0 {
		life = usageTTL
	}
	_ = s.u.AddUsage(ctx, key, n, life)
}

func (s *SharedUsage) Get(key string) int {
	ctx, cancel := context.WithTimeout(context.Background(), callFor)
	defer cancel()
	n, err := s.u.GetUsage(ctx, key)
	if err != nil {
		return 0
	}
	return n
}
