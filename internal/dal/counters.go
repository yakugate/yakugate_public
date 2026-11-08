package dal

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// The shared counters and blocks of the accept path and the inbound doors:
// rate limits, quota and per-destination counts, the login ladders and the
// guessing guard, the duplicate check, the live campaign window and the abuse
// monitor's counters. Each is one call a message at most (one script, one
// statement, or one pipeline a second from a buffer in memory).
//
// What is not storage stays in its package (internal/ratelimit, lockout,
// dedupe, campaignwin, abusecount): the in-memory versions used when no
// database keeps an area, the ladder's rules, the window arithmetic, the
// fail-open posture.
//
// They are opened together (OpenCounters) because a role kept them on ONE
// connection — one Redis client and one pool for the rate limits, the
// duplicate check, the ladders, … — and a role that wants an area on a
// connection of its own opens it alone (OpenLimits, …).

// Limits is the per-second buckets every process of a gateway draws from
// together: an account's, a company's, a country's, a vendor's rate. A bucket
// is a key and a second (unix seconds); it lasts no longer than about two
// seconds, so a second that is over is simply a bucket nobody asks again.
type Limits interface {
	// Spend adds n to the bucket and answers its count after the add. A
	// spend over the limit still counts (Allow: the request that tips over
	// the cap is refused and its segments still occupy the window).
	Spend(ctx context.Context, key string, sec int64, n int) (int, error)
	// Take takes as much of max as still fits under limit and answers how
	// much that was: ONE call for a whole batch.
	Take(ctx context.Context, key string, sec int64, limit, max int) (int, error)
	// Grant is Take for the queue drainers (up to want, part of it when
	// that is all that is left); what is not used goes back with Return.
	Grant(ctx context.Context, key string, sec int64, limit, want int) (int, error)
	// Return hands back up to n units to a bucket that is still there,
	// never below zero. A return to a bucket that is gone does nothing.
	Return(ctx context.Context, key string, sec int64, n int) error
}

// Usage is the counts kept per period: quota use (a day's, a month's) and
// the per-destination cap. The caller stamps the period into the key, so a
// new period is a new key; life is how long a key is kept after it was made.
type Usage interface {
	AddUsage(ctx context.Context, key string, n int, life time.Duration) error
	// GetUsage answers the count of a key, 0 for one never counted.
	GetUsage(ctx context.Context, key string) (int, error)
}

// LadderRules are the numbers a login ladder runs by (internal/lockout):
// Threshold failures inside the window trip a lock; the lock lasts the rung
// reached (Rungs, shortest first, four of them); a trip within
// EscalateWithin of the last lock's end climbs a rung; the level falls back
// to none after DecayAfter clean.
type LadderRules struct {
	Threshold      int
	Rungs          [4]time.Duration
	DecayAfter     time.Duration
	EscalateWithin time.Duration
}

// Block is one id a ladder holds locked now.
type Block struct {
	ID        string        // as the ladder keys it
	Level     int           // the rung it is on, 1 to 4
	Remaining time.Duration // until it lets go by itself
}

// Blocks is the login ladders and the guessing guard, shared by every
// process: a lock set by one is seen by all.
type Blocks interface {
	// LockedFor answers how much longer id is locked; 0 = not locked.
	LockedFor(ctx context.Context, id string) (time.Duration, error)
	// Fail counts one failed attempt of id and, when that trips the ladder,
	// locks it: locked, for how long, and the level reached (the level so
	// far when it did not trip). now stamps the list of who is locked.
	Fail(ctx context.Context, id string, r LadderRules, now time.Time) (locked bool, retryAfter time.Duration, level int, err error)
	// ResetFails clears id's failure count; its level and lock stay.
	ResetFails(ctx context.Context, id string) error
	// ClearBlock removes everything of id: lock, count, level.
	ClearBlock(ctx context.Context, id string) error
	// Blocked lists ids locked now, at least limit of them when there are
	// that many, in no set order; more = there may be more than were read.
	// It never answers "nobody" for a list it could not read.
	Blocked(ctx context.Context, limit int) (blocks []Block, more bool, err error)
	// SeeIdentity notes that identity failed from addr, kept for window
	// after the last one: added = it was not yet among them, distinct = how
	// many different ones there are now.
	SeeIdentity(ctx context.Context, addr, identity string, window time.Duration) (added bool, distinct int, err error)
	// ClearIdentities forgets the identities noted for addr.
	ClearIdentities(ctx context.Context, addr string) error
}

// Dedupe is the duplicate check: one key a message (pipeline.DedupKey).
type Dedupe interface {
	// Check creates key holding msgID for window: firstID "" = created (a
	// new message), else the id it already holds (a duplicate of it). The
	// same msgID asked twice is "created". A DAL may fail open by itself
	// (Redis does: an error reads as created).
	Check(ctx context.Context, key, msgID string, window time.Duration) (firstID string, err error)
	// Release removes key only while it holds msgID.
	Release(ctx context.Context, key, msgID string) error
}

// DedupeSweeper is a Dedupe whose keys do not end by themselves (a table):
// SweepDedupe removes some of those long expired and answers how many.
type DedupeSweeper interface {
	SweepDedupe(ctx context.Context) (int64, error)
}

// CampaignCount is one account's messages of one text signature in one
// bucket of the campaign window.
type CampaignCount struct {
	Hash    string
	Bucket  int64
	Account int64
	N       int
}

// CampaignWindow is the live campaign window (internal/campaignwin).
type CampaignWindow interface {
	// AddCampaignCounts adds the counts; every bucket written is kept for
	// keep. One call (one pipeline) for a whole flush.
	AddCampaignCounts(ctx context.Context, counts []CampaignCount, keep time.Duration) error
	// CampaignBuckets reads one signature's buckets: per bucket asked, in
	// that order, messages by account.
	CampaignBuckets(ctx context.Context, hash string, buckets []int64) ([]map[int64]int, error)
}

// The abuse monitor's counters (internal/abusecount). A count is a field of
// a bucket of a kind: AbuseMinute, AbuseHour and AbusePrefix buckets are
// minutes or hours (unix time / 60 or / 3600), AbuseDay buckets are UTC days
// (unix time / 86400). The package names the fields.
const (
	AbuseMinute  byte = 'm'
	AbuseHour    byte = 'h'
	AbusePrefix  byte = 'p'
	AbuseDay     byte = 'd'
	AbuseNumbers byte = 'u' // the numbers an account sent to in a minute
	AbuseTexts   byte = 't' // an account's newest texts
)

// AbuseCount is one count to add.
type AbuseCount struct {
	Kind   byte
	Bucket int64
	Field  string
	N      int64
}

// AbuseNumberSet is the numbers an account sent to in one minute.
type AbuseNumberSet struct {
	Account, Minute int64
	Numbers         []string
}

// AbuseSample is one kept text.
type AbuseSample struct {
	At     int64  `json:"t"` // unix seconds
	Dest   string `json:"d,omitempty"`
	Sender string `json:"s,omitempty"`
	Body   string `json:"b"`
}

// AbuseTextSet is an account's texts of one flush, oldest first.
type AbuseTextSet struct {
	Account int64
	Samples []AbuseSample
}

// AbuseFlush is what the accept path writes in one go.
type AbuseFlush struct {
	// Since marks when the counters became whole; it is set only when
	// nothing marks it yet.
	Since   time.Time
	Counts  []AbuseCount
	Numbers []AbuseNumberSet
	Texts   []AbuseTextSet
	// Keep is how long each kind is kept after its last write.
	Keep map[byte]time.Duration
	// SampleKeep is how many texts an account keeps: the newest.
	SampleKeep int
}

// AbuseCounts is what the accept path counts for the abuse monitor, and
// what the monitor reads back.
type AbuseCounts interface {
	// FlushAbuse writes a flush, in one call (one pipeline).
	FlushAbuse(ctx context.Context, f AbuseFlush) error
	// AbusePrefixLen is the prefix length the monitor asked for (0 and an
	// error when it has asked nothing).
	AbusePrefixLen(ctx context.Context) (int, error)
	// AskAbusePrefixLen asks for n and answers since when n has been asked.
	AskAbusePrefixLen(ctx context.Context, n int) (time.Time, error)
	// AbuseEpoch is since when the counters are whole; ok=false = nothing
	// marks it (nothing counted yet, or the counts were lost).
	AbuseEpoch(ctx context.Context) (since time.Time, ok bool, err error)
	// AbuseBuckets reads buckets of one kind: per bucket asked, in that
	// order, its fields (a value that is not a number is left out).
	AbuseBuckets(ctx context.Context, kind byte, buckets []int64) ([]map[string]int64, error)
	// AbuseDistinct estimates how many different numbers an account sent to
	// over the minutes given.
	AbuseDistinct(ctx context.Context, account int64, minutes []int64) (int64, error)
	// AbuseSamples is an account's kept texts, newest first as written, at
	// most keep (one that cannot be read is left out).
	AbuseSamples(ctx context.Context, account int64, keep int) ([]AbuseSample, error)
}

// Counters is the areas above as one database keeps them for a role, on one
// connection. An area the database does not keep (yet) is nil: the caller
// uses its in-memory version, as it did without Redis.
type Counters struct {
	Limits    Limits
	Usage     Usage
	Blocks    Blocks
	Dedupe    Dedupe
	Campaigns CampaignWindow
	Abuse     AbuseCounts
	// Conn closes the connection they share; nil = nothing to close.
	Conn io.Closer
}

// Close closes the connection the areas share.
func (c *Counters) Close() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

// CountersOn says which DAL keeps the counters and blocks: the main Redis
// whenever one is set (cfg.RedisEnabled — the roles' gate since 2026-07-24),
// else the install's SQL database. MySQL keeps all six since step 3 of the
// DAL plan (30 Oct 2026: limit_buckets, usage_counts, blocks, dedupe_keys,
// campaign_counts, the abuse_* tables); a database that does not keep an area
// leaves it nil and each process keeps that area in memory.
func CountersOn(cfg config.Config) config.Backend {
	if cfg.RedisEnabled() {
		return config.BackendRedis
	}
	return SQL(cfg)
}

// OpenCounters opens the counters and blocks on the DAL CountersOn picks,
// on one new connection. ErrNotServed = that database keeps none of them.
func OpenCounters(cfg config.Config) (*Counters, config.Backend, error) {
	on := CountersOn(cfg)
	d, ok := Lookup(on)
	if !ok {
		return nil, on, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.Counters == nil {
		return nil, on, fmt.Errorf("dal: counters on %s: %w", on, ErrNotServed)
	}
	c, err := d.Counters(cfg)
	if err != nil {
		return nil, on, fmt.Errorf("dal: counters on %s: %w", on, err)
	}
	return c, on, nil
}

// openOne opens the counters and keeps one area of them; ErrNotServed when
// the database picked does not keep that area.
func openOne[T any](cfg config.Config, area string, pick func(*Counters) T, none func(T) bool) (T, error) {
	var zero T
	c, on, err := OpenCounters(cfg)
	if err != nil {
		return zero, err
	}
	v := pick(c)
	if none(v) {
		c.Close()
		return zero, fmt.Errorf("dal: %s on %s: %w", area, on, ErrNotServed)
	}
	return v, nil
}

// OpenLimits opens the rate-limit buckets alone, on a connection of their
// own (a role whose limits must not wait behind its other calls).
func OpenLimits(cfg config.Config) (Limits, error) {
	return openOne(cfg, "limits", func(c *Counters) Limits { return c.Limits }, func(l Limits) bool { return l == nil })
}

// OpenAbuseCounts opens the abuse monitor's counters alone.
func OpenAbuseCounts(cfg config.Config) (AbuseCounts, error) {
	return openOne(cfg, "abuse counters", func(c *Counters) AbuseCounts { return c.Abuse }, func(a AbuseCounts) bool { return a == nil })
}
