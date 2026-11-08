package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limits is the per-second buckets in the main Redis (dal.Limits), moved
// from internal/ratelimit (redis.go, grant.go) on 29 Oct 2026, scripts
// unchanged: one INCRBY-with-expiry per request against a shared per-key
// per-second bucket, so N gateway instances enforce ONE limit instead of N.
// A bucket is rl:<key>:<unix second>.
type Limits struct{ c *redis.Client }

// NewLimits keeps the buckets on a Redis client.
func NewLimits(c *redis.Client) *Limits { return &Limits{c: c} }

func bucketKey(key string, sec int64) string { return fmt.Sprintf("rl:%s:%d", key, sec) }

// incrScript counts the spend and sets the window's expiry exactly once
// (2s TTL comfortably outlives the 1s window without accumulating keys).
var incrScript = redis.NewScript(`
local n = redis.call('INCRBY', KEYS[1], ARGV[1])
if n == tonumber(ARGV[1]) then redis.call('EXPIRE', KEYS[1], 2) end
return n
`)

// Spend adds n to the bucket and answers its count after the add.
func (l *Limits) Spend(ctx context.Context, key string, sec int64, n int) (int, error) {
	return incrScript.Run(ctx, l.c, []string{bucketKey(key, sec)}, n).Int()
}

// takeScript takes as much of ARGV[2] as still fits under the per-second
// limit ARGV[1], and says how much that was.
var takeScript = redis.NewScript(`
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
local room = tonumber(ARGV[1]) - cur
if room <= 0 then return 0 end
local take = math.min(room, tonumber(ARGV[2]))
local n = redis.call('INCRBY', KEYS[1], take)
if n == take then redis.call('EXPIRE', KEYS[1], 2) end
return take
`)

// Take takes up to max under limit, in one script.
func (l *Limits) Take(ctx context.Context, key string, sec int64, limit, max int) (int, error) {
	return takeScript.Run(ctx, l.c, []string{bucketKey(key, sec)}, limit, max).Int()
}

// grantScript takes up to ARGV[2] units from a limit of ARGV[1] in one step,
// so two instances can never both take the last units.
var grantScript = redis.NewScript(`
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local left = tonumber(ARGV[1]) - used
if left <= 0 then return 0 end
local want = tonumber(ARGV[2])
if want < left then left = want end
local n = redis.call('INCRBY', KEYS[1], left)
if n == left then redis.call('EXPIRE', KEYS[1], 2) end
return left
`)

// Grant takes up to want under limit, in one script.
func (l *Limits) Grant(ctx context.Context, key string, sec int64, limit, want int) (int, error) {
	return grantScript.Run(ctx, l.c, []string{bucketKey(key, sec)}, limit, want).Int()
}

// returnScript hands up to ARGV[1] units back to a bucket that is still
// there, and never takes it below zero.
var returnScript = redis.NewScript(`
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
if used <= 0 then return 0 end
local back = math.min(used, tonumber(ARGV[1]))
redis.call('DECRBY', KEYS[1], back)
return back
`)

// Return hands back unused units to a bucket that exists.
//
// Only to a bucket that exists. A plain DECRBY made one when the second had
// turned between the grant and the return: a key with a negative count and no
// expiry, which the next grant in that second then read as extra room and —
// finding a count that was not its own — never gave an expiry either. Those
// keys stayed for good (2,419 of them after one afternoon's load test on the
// dev box, 2026-10-07).
func (l *Limits) Return(ctx context.Context, key string, sec int64, n int) error {
	return returnScript.Run(ctx, l.c, []string{bucketKey(key, sec)}, n).Err()
}

// Usage is the period counts in the main Redis (dal.Usage), moved from
// internal/ratelimit (usage_redis.go): period-stamped keys qu:<key>
// INCRBY'd with a TTL past their period, so counts survive restarts and are
// shared across instances.
type Usage struct{ c *redis.Client }

// NewUsage keeps the counts on a Redis client.
func NewUsage(c *redis.Client) *Usage { return &Usage{c: c} }

var usageIncrScript = redis.NewScript(`
local n = redis.call('INCRBY', KEYS[1], ARGV[1])
if n == tonumber(ARGV[1]) then redis.call('EXPIRE', KEYS[1], ARGV[2]) end
return n
`)

// AddUsage adds n to key; a new key lives for life.
func (u *Usage) AddUsage(ctx context.Context, key string, n int, life time.Duration) error {
	return usageIncrScript.Run(ctx, u.c, []string{"qu:" + key}, n, int(life.Seconds())).Err()
}

// GetUsage reads key's count, 0 for one never counted.
func (u *Usage) GetUsage(ctx context.Context, key string) (int, error) {
	n, err := u.c.Get(ctx, "qu:"+key).Int()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return n, err
}
