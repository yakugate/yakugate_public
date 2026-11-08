package redis

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
)

// Blocks is the login ladders and the guessing guard in the main Redis
// (dal.Blocks), moved from internal/lockout (redis.go, spray.go, blocked.go)
// on 29 Oct 2026, scripts unchanged. Failure counts, escalation levels and
// lock flags live in shared keys, so an attacker spraying guesses across N
// gateway instances climbs ONE ladder instead of N. The rules (threshold,
// rungs, decay) are the caller's (dal.LadderRules).
//
//	lkf:<id>   failure counter (60s window via EXPIRE-on-first)
//	lku:<id>   lock flag; its TTL IS the Retry-After
//	lkl:<id>   escalation level; TTL = lock + decay hour
//	lke:<id>   escalation window marker; TTL = lock + 15 min
//	lkidx      who is locked now (below): written on a trip only
//	spr:<ip>   the distinct identities that failed from one address
type Blocks struct{ c *redis.Client }

// NewBlocks keeps the ladders on a Redis client.
func NewBlocks(c *redis.Client) *Blocks { return &Blocks{c: c} }

// Who is locked right now (2026-10-09): the locks are one key each among
// millions and the gateway does not walk Redis's key space to find them, so
// the script that sets a lock also files it in an index:
//
//	lkidx   sorted set: member = the locked id, score = when its lock ends
//	        (unix seconds)
//
// trimmed there too, so it holds one entry per id locked now and no more.
// The lock key stays the truth: a reader asks each listed id for its real
// time left. Every ladder of a gateway writes this ONE index; the kinds are
// kept apart by the form of the id ("u:alice", "ip:203.0.113.7", a bare
// address).
const (
	indexKey = "lkidx"
	// indexSlack keeps an entry a little past its lock's end, so two gateways
	// whose clocks differ do not trim each other's live entries.
	indexSlack = 2 * time.Minute
	// indexLife outlives the longest lock plus the slack: the index goes away
	// by itself once nothing has been locked for a while.
	indexLife = 2 * time.Hour
	// indexMargin is read past the limit, for entries whose lock has just
	// ended or was lifted.
	indexMargin = 64
)

// failScript implements one Fail() atomically.
//
//	KEYS[1] lkf:<id>    failure counter (60s window via EXPIRE-on-first)
//	KEYS[2] lku:<id>    lock flag; its TTL IS the Retry-After
//	KEYS[3] lkl:<id>    escalation level; TTL = lock + decay hour
//	KEYS[4] lke:<id>    escalation window marker; TTL = lock + 15 min —
//	                    existing at the NEXT trip means "within 15 min of the
//	                    previous lock releasing", so climb a rung
//	KEYS[5] lkidx       who is locked now: written on a trip only
//	ARGV[1] threshold   ARGV[2..5] rung seconds   ARGV[6] decay seconds
//	ARGV[7] escalate-within seconds
//	ARGV[8] now, unix seconds   ARGV[9] the id   ARGV[10] index slack seconds
//	ARGV[11] index life seconds
//
// returns {locked(0/1), retryAfterSeconds, level}
var failScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('EXPIRE', KEYS[1], 60) end
local lvl = tonumber(redis.call('GET', KEYS[3]) or '0')
if n < tonumber(ARGV[1]) then return {0, 0, lvl} end
redis.call('DEL', KEYS[1])
if lvl == 0 then
  lvl = 1
elseif redis.call('EXISTS', KEYS[4]) == 1 and lvl < 4 then
  lvl = lvl + 1
end
local dur = tonumber(ARGV[1 + lvl])
redis.call('SET', KEYS[2], '1', 'EX', dur)
redis.call('SET', KEYS[3], lvl, 'EX', dur + tonumber(ARGV[6]))
redis.call('SET', KEYS[4], '1', 'EX', dur + tonumber(ARGV[7]))
local now = tonumber(ARGV[8])
redis.call('ZADD', KEYS[5], now + dur, ARGV[9])
redis.call('ZREMRANGEBYSCORE', KEYS[5], '-inf', now - tonumber(ARGV[10]))
redis.call('EXPIRE', KEYS[5], tonumber(ARGV[11]))
return {1, dur, lvl}
`)

func ladderKeys(id string) []string {
	return []string{"lkf:" + id, "lku:" + id, "lkl:" + id, "lke:" + id, indexKey}
}

// LockedFor answers the lock's time left; 0 = not locked.
func (b *Blocks) LockedFor(ctx context.Context, id string) (time.Duration, error) {
	ttl, err := b.c.TTL(ctx, "lku:"+id).Result()
	if err != nil || ttl <= 0 {
		return 0, err
	}
	return ttl, nil
}

// Fail counts one failure of id under r, in one script.
func (b *Blocks) Fail(ctx context.Context, id string, r dal.LadderRules, now time.Time) (bool, time.Duration, int, error) {
	res, err := failScript.Run(ctx, b.c, ladderKeys(id),
		r.Threshold,
		int(r.Rungs[0].Seconds()), int(r.Rungs[1].Seconds()), int(r.Rungs[2].Seconds()), int(r.Rungs[3].Seconds()),
		int(r.DecayAfter.Seconds()), int(r.EscalateWithin.Seconds()),
		now.Unix(), id, int(indexSlack.Seconds()), int(indexLife.Seconds()),
	).Int64Slice()
	if err != nil {
		return false, 0, 0, err
	}
	if len(res) != 3 {
		return false, 0, 0, errors.New("lockout: the fail script answered " + strconv.Itoa(len(res)) + " values, not 3")
	}
	return res[0] == 1, time.Duration(res[1]) * time.Second, int(res[2]), nil
}

// ResetFails clears the failure counter — the escalation level key is
// deliberately left to expire on its own.
func (b *Blocks) ResetFails(ctx context.Context, id string) error {
	return b.c.Del(ctx, "lkf:"+id).Err()
}

// ClearBlock removes the active lock, the failure counter, the escalation
// level AND the escalation window — cluster-wide, since every instance
// shares these keys. The id leaves the list of who is locked with them.
func (b *Blocks) ClearBlock(ctx context.Context, id string) error {
	pipe := b.c.Pipeline()
	pipe.Del(ctx, "lkf:"+id, "lku:"+id, "lkl:"+id, "lke:"+id)
	pipe.ZRem(ctx, indexKey, id)
	_, err := pipe.Exec(ctx)
	return err
}

// Blocked reads the index — limit entries and a margin, longest locked first
// — then each listed id's real time left and level in one pipeline, and
// leaves out an id whose lock has gone. more = the read was cut.
func (b *Blocks) Blocked(ctx context.Context, limit int) ([]dal.Block, bool, error) {
	ask := limit + indexMargin
	since := time.Now().Add(-indexSlack).Unix()
	ids, err := b.c.ZRevRangeByScore(ctx, indexKey, &redis.ZRangeBy{
		Min: strconv.FormatInt(since, 10), Max: "+inf", Count: int64(ask),
	}).Result()
	if err != nil {
		return nil, false, err
	}
	if len(ids) == 0 {
		return nil, false, nil
	}
	pipe := b.c.Pipeline()
	left := make([]*redis.DurationCmd, len(ids))
	rung := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		left[i] = pipe.TTL(ctx, "lku:"+id)
		rung[i] = pipe.Get(ctx, "lkl:"+id)
	}
	// A level key that is gone answers redis.Nil; that is not a failed read.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, false, err
	}
	out := make([]dal.Block, 0, len(ids))
	for i, id := range ids {
		ttl := left[i].Val()
		if left[i].Err() != nil || ttl <= 0 {
			continue // ended, or lifted
		}
		level, _ := strconv.Atoi(rung[i].Val())
		if level < 1 {
			level = 1
		}
		out = append(out, dal.Block{ID: id, Level: level, Remaining: ttl})
	}
	return out, len(ids) == ask, nil
}

// spraySetScript adds identity to ip's distinct-set (refreshing its TTL to
// the window on every call, so a sustained spray keeps the set alive) and
// reports both whether the add was genuinely new and the resulting distinct
// count — so a repeat of an already-counted identity never re-trips on its
// own.
var spraySetScript = redis.NewScript(`
local added = redis.call('SADD', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
local count = redis.call('SCARD', KEYS[1])
return {added, count}
`)

// SeeIdentity notes a failed identity from addr, in one script.
func (b *Blocks) SeeIdentity(ctx context.Context, addr, identity string, window time.Duration) (bool, int, error) {
	res, err := spraySetScript.Run(ctx, b.c, []string{"spr:" + addr}, identity, int(window.Seconds())).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	if len(res) != 2 {
		return false, 0, errors.New("lockout: the spray script answered " + strconv.Itoa(len(res)) + " values, not 2")
	}
	return res[0] == 1, int(res[1]), nil
}

// ClearIdentities drops addr's distinct-identity window.
func (b *Blocks) ClearIdentities(ctx context.Context, addr string) error {
	return b.c.Del(ctx, "spr:"+addr).Err()
}
