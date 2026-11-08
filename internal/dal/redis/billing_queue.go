package redis

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The billing queues (dal.BillingQueue), moved here from internal/billing
// (booker.go, events.go) and internal/billingq on 29 Oct 2026, unchanged:
// the same keys, the same scripts, the same pipelines.

// --- the queue's keys -------------------------------------------------------

// Every deployment books its own events (issue 25: "queues:<dep>:<what>:<who>
// :ready|delayed"). The scope is billing.QueueScope: the deployment id, else
// "shared". The side is "sell", "buy", or "" for the one mixed queue of the
// build of 26 Oct.

func sidePath(side string) string {
	if side == "" {
		return ":billing:db:"
	}
	return ":billing:" + side + ":db:"
}

func queueKey(scope, side string, lane int, what string) string {
	return "queues:" + scope + sidePath(side) + strconv.Itoa(lane) + ":" + what
}

// BillingLaneKey is one of a lane's keys: ready, delayed, doing, doing:id, lock.
func BillingLaneKey(scope, side string, lane int, what string) string {
	return queueKey(scope, side, lane, what)
}

// BillingReadyKey is the lane's waiting events (sorted set, scored by the
// event's ms).
func BillingReadyKey(scope, side string, lane int) string {
	return queueKey(scope, side, lane, "ready")
}

// BillingDelayedKey is the lane's events MySQL refused, by the time they may
// be tried again.
func BillingDelayedKey(scope, side string, lane int) string {
	return queueKey(scope, side, lane, "delayed")
}

// BillingLanesKey lists every lane anything was put on, so a lane is never
// left unread when the lane count changes.
func BillingLanesKey(scope, side string) string { return "queues:" + scope + sidePath(side) + "lanes" }

func doingKey(l dal.Lane) string   { return queueKey(l.Scope, l.Side, l.Lane, "doing") }
func doingIDKey(l dal.Lane) string { return queueKey(l.Scope, l.Side, l.Lane, "doing:id") }
func lockKey(l dal.Lane) string    { return queueKey(l.Scope, l.Side, l.Lane, "lock") }
func readyKey(l dal.Lane) string   { return BillingReadyKey(l.Scope, l.Side, l.Lane) }
func delayedKey(l dal.Lane) string { return BillingDelayedKey(l.Scope, l.Side, l.Lane) }

// QueueClient is the client of the deployment's queue Redis — where the sell
// and buy queues live since 29 Oct 2026 (Saleem: "an event is sent to the
// selling queue in local redis"): YAKU_QUEUE_REDIS_*, else the main Redis.
// One client a process, shared by every booker in it.
func QueueClient(cfg config.Config) *redis.Client {
	queueRedisOnce.Do(func() {
		queueRedis = FromConfig(cfg.ForQueueRedis()).Client()
	})
	return queueRedis
}

var (
	queueRedisOnce sync.Once
	queueRedis     *redis.Client
)

// mainBillingClient is the main Redis's client for the live balances and
// the earlier builds' queues: one a process, as QueueClient. Its calls wait
// BalanceTimeout for Redis, so a deduction under load is not refused at 4 s.
func mainBillingClient(cfg config.Config) *redis.Client {
	mainBillingOnce.Do(func() {
		mainBilling = fromConfig(cfg, BalanceTimeout(cfg)).Client()
	})
	return mainBilling
}

var (
	mainBillingOnce sync.Once
	mainBilling     *redis.Client
)

// BillingQueue is a deployment's billing queues on one Redis.
type BillingQueue struct {
	rc *redis.Client
}

// NewBillingQueue keeps the billing queues on rc.
func NewBillingQueue(rc *redis.Client) *BillingQueue {
	if rc == nil {
		return nil
	}
	return &BillingQueue{rc: rc}
}

// Client is the Redis the queues are in.
func (q *BillingQueue) Client() *redis.Client { return q.rc }

var _ dal.BillingQueue = (*BillingQueue)(nil)

// Put puts each lane's ZADD (and SADD of its name, where asked) on one
// pipeline: one round trip. Nothing to add = no call.
func (q *BillingQueue) Put(ctx context.Context, adds []dal.LaneAdd) error {
	pipe := q.rc.Pipeline()
	n := 0
	for _, a := range adds {
		if len(a.Events) == 0 {
			continue
		}
		zs := make([]redis.Z, len(a.Events))
		for i, e := range a.Events {
			zs[i] = redis.Z{Score: e.Score, Member: e.Member}
		}
		pipe.ZAdd(ctx, readyKey(a.Lane), zs...)
		if a.Name {
			pipe.SAdd(ctx, BillingLanesKey(a.Scope, a.Side), a.Lane.Lane)
		}
		n++
	}
	if n == 0 {
		return nil
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Waiting is how many events wait on the ready lists of the sides and how
// long the oldest has waited (ms), read from the queues' own lengths.
func (q *BillingQueue) Waiting(ctx context.Context, scope string, sides []string, lanes int) (n, oldestMS int64, err error) {
	pipe := q.rc.Pipeline()
	type one struct {
		n     *redis.IntCmd
		first *redis.ZSliceCmd
	}
	var qs []one
	for _, side := range sides {
		ls := map[int]bool{}
		for i := 0; i < lanes; i++ {
			ls[i] = true
		}
		if ms, err := q.rc.SMembers(ctx, BillingLanesKey(scope, side)).Result(); err == nil {
			for _, m := range ms {
				if l, err := strconv.Atoi(m); err == nil && l >= 0 && l < 1024 {
					ls[l] = true
				}
			}
		}
		for l := range ls {
			k := BillingReadyKey(scope, side, l)
			qs = append(qs, one{n: pipe.ZCard(ctx, k), first: pipe.ZRangeWithScores(ctx, k, 0, 0)})
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, 0, err
	}
	now := time.Now().UnixMilli()
	for _, x := range qs {
		n += x.n.Val()
		if f := x.first.Val(); len(f) == 1 {
			if age := now - int64(f[0].Score); age > oldestMS {
				oldestMS = age
			}
		}
	}
	return n, oldestMS, nil
}

// Lanes is the side's lanes set.
func (q *BillingQueue) Lanes(ctx context.Context, scope, side string) ([]string, error) {
	return q.rc.SMembers(ctx, BillingLanesKey(scope, side)).Result()
}

// takeTurnScript: the lane's turn, taken when free or renewed when ours.
var takeTurnScript = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if not v then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
if v == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`)

// claimScript gives the batch to book: the one left in "doing" (a repeat),
// else up to ARGV[2] events — those in "delayed" whose time has come first,
// then the oldest in "ready" — moved into "doing" under the id ARGV[3]. Only
// while the turn is ours.
//
// KEYS: ready, delayed, doing, doing:id, lock. ARGV: token, max, new id, now ms.
// Answers {id, repeat (0|1), member...}, or {} when nothing waits or the turn
// is lost.
var claimScript = redis.NewScript(`
if redis.call('GET', KEYS[5]) ~= ARGV[1] then return {} end
if redis.call('EXISTS', KEYS[3]) == 1 then
  local out = {redis.call('GET', KEYS[4]) or '', '1'}
  for _, m in ipairs(redis.call('LRANGE', KEYS[3], 0, -1)) do table.insert(out, m) end
  return out
end
local max = tonumber(ARGV[2])
local got = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[4], 'LIMIT', 0, max)
if #got > 0 then redis.call('ZREM', KEYS[2], unpack(got)) end
if #got < max then
  local more = redis.call('ZRANGE', KEYS[1], 0, max - #got - 1)
  if #more > 0 then
    redis.call('ZREM', KEYS[1], unpack(more))
    for _, m in ipairs(more) do table.insert(got, m) end
  end
end
if #got == 0 then return {} end
redis.call('RPUSH', KEYS[3], unpack(got))
redis.call('SET', KEYS[4], ARGV[3])
local out = {ARGV[3], '0'}
for _, m in ipairs(got) do table.insert(out, m) end
return out
`)

// doneScript ends a booked batch: "doing" is cleared, and the events MySQL
// refused (ARGV[3..], each a score then a member) go to "delayed". Only while
// the turn is ours.
//
// KEYS: doing, doing:id, delayed, lock. ARGV: token, k, then k (score, member).
var doneScript = redis.NewScript(`
if redis.call('GET', KEYS[4]) ~= ARGV[1] then return 0 end
local k = tonumber(ARGV[2])
for i = 3, 2 + 2 * k, 2 do
  redis.call('ZADD', KEYS[3], ARGV[i], ARGV[i + 1])
end
redis.call('DEL', KEYS[1], KEYS[2])
return 1
`)

var dropIfMineScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0`)

// TakeTurn runs takeTurnScript.
func (q *BillingQueue) TakeTurn(ctx context.Context, l dal.Lane, token string, ttl time.Duration) (bool, error) {
	mine, err := takeTurnScript.Run(ctx, q.rc, []string{lockKey(l)}, token, int(ttl/time.Millisecond)).Int()
	return err == nil && mine == 1, err
}

// DropTurn runs dropIfMineScript.
func (q *BillingQueue) DropTurn(ctx context.Context, l dal.Lane, token string) error {
	return dropIfMineScript.Run(ctx, q.rc, []string{lockKey(l)}, token).Err()
}

// Claim runs claimScript.
func (q *BillingQueue) Claim(ctx context.Context, l dal.Lane, token string, max int, newID string, nowMS int64) ([]string, error) {
	keys := []string{readyKey(l), delayedKey(l), doingKey(l), doingIDKey(l), lockKey(l)}
	return claimScript.Run(ctx, q.rc, keys, token, max, newID, nowMS).StringSlice()
}

// SetBatchID writes doing:id.
func (q *BillingQueue) SetBatchID(ctx context.Context, l dal.Lane, id string) error {
	return q.rc.Set(ctx, doingIDKey(l), id, 0).Err()
}

// Done runs doneScript; each of later is a (score, member) pair.
func (q *BillingQueue) Done(ctx context.Context, l dal.Lane, token string, later []dal.Scored) (int, error) {
	args := []any{token, len(later)}
	for _, s := range later {
		args = append(args, int64(s.Score), s.Member)
	}
	return doneScript.Run(ctx, q.rc, []string{doingKey(l), doingIDKey(l), delayedKey(l), lockKey(l)}, args...).Int()
}

// DropIfEmpty says nothing is left on a lane (ready, delayed or doing); the
// lane is then taken off the lanes set (the set goes with its last lane).
func (q *BillingQueue) DropIfEmpty(ctx context.Context, l dal.Lane) (bool, error) {
	n, err := q.rc.Exists(ctx, readyKey(l), delayedKey(l), doingKey(l)).Result()
	if err != nil || n > 0 {
		return false, err
	}
	_ = q.rc.SRem(ctx, BillingLanesKey(l.Scope, l.Side), strconv.Itoa(l.Lane)).Err()
	return true, nil
}

// Count reads a lane in one pipeline.
func (q *BillingQueue) Count(ctx context.Context, l dal.Lane) (dal.LaneCount, error) {
	pipe := q.rc.Pipeline()
	zc := pipe.ZCard(ctx, readyKey(l))
	dc := pipe.ZCard(ctx, delayedKey(l))
	gc := pipe.LLen(ctx, doingKey(l))
	first := pipe.ZRangeWithScores(ctx, readyKey(l), 0, 0)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return dal.LaneCount{}, err
	}
	c := dal.LaneCount{Ready: zc.Val(), Delayed: dc.Val(), Doing: gc.Val()}
	if f := first.Val(); len(f) == 1 {
		c.Oldest, c.HasOldest = f[0].Score, true
	}
	return c, nil
}

// Unsent says Redis certainly did not run the call.
func (q *BillingQueue) Unsent(err error) bool { return unsent(err) }
