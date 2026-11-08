package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The live view in the main Redis — moved here unchanged on 29 Oct 2026 (the
// DAL plan) from internal/monitor (push.go, storeturn.go, reportnow.go,
// verdicts.go).
//
// Every role pushes its own report to `status:<role>:<instance>` on an
// interval; the key carries a TTL of 3× the interval so a dead instance
// disappears from the cluster view by itself instead of showing stale numbers
// forever.
//
// The keys are found through a LIST of them, never by walking the key space
// (2026-10-08). ClusterStatus used to SCAN for `status:*`, which reads every
// key Redis holds: with five million receipts waiting in the same Redis that
// took six seconds and more, so the console's Monitor, the health page and
// /status/cluster ran out of time and showed nothing — on exactly the day
// somebody needs them. Every push now also files its key in statusIndex
// (scored by the time of the push), and a reader takes the keys from there.

// openLiveStatus is the live view's opener: the main Redis, as cmd/yakugate
// built it (FromConfig(cfg).Client()).
func openLiveStatus(cfg config.Config) (dal.LiveStatus, error) {
	return NewLiveStatus(FromConfig(cfg).Client()), nil
}

// LiveStatus is the live view on one Redis client.
type LiveStatus struct{ c *redis.Client }

var _ dal.LiveStatus = (*LiveStatus)(nil)

// NewLiveStatus wraps a client a role already has. nil client = nil view.
func NewLiveStatus(c *redis.Client) *LiveStatus {
	if c == nil {
		return nil
	}
	return &LiveStatus{c: c}
}

// statusIndex lists the status keys that were pushed lately. Its name is
// deliberately NOT under `status:` — a role older than this still SCANs that
// pattern and GETs whatever it finds, and a sorted set there would fail its
// whole read.
const statusIndex = "statuskeys"

// statusIndexKeep is how long a key stays listed after its last push. The
// status key itself is gone after three intervals; the list only has to stay
// short.
const statusIndexKeep = time.Hour

// Publish writes one snapshot and lists its key.
func (l *LiveStatus) Publish(ctx context.Context, key string, snap []byte, every time.Duration) error {
	now := time.Now()
	pipe := l.c.Pipeline()
	pipe.Set(ctx, key, snap, 3*every)
	pipe.ZAdd(ctx, statusIndex, redis.Z{Score: float64(now.Unix()), Member: key})
	pipe.ZRemRangeByScore(ctx, statusIndex, "-inf", strconv.FormatInt(now.Add(-statusIndexKeep).Unix(), 10))
	pipe.Expire(ctx, statusIndex, 24*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

// Statuses reads every live instance's pushed snapshot — the gateway-wide
// view, by status key.
//
// The keys come from statusIndex. Only when that list is empty — nothing has
// pushed since this build, so the roles are older than it, or none runs — is
// the key space walked the old way. A gateway whose roles are partly older
// than this shows only the newer ones until the rest are restarted.
func (l *LiveStatus) Statuses(ctx context.Context) (map[string]json.RawMessage, error) {
	rdb := l.c
	since := strconv.FormatInt(time.Now().Add(-statusIndexKeep).Unix(), 10)
	keys, err := rdb.ZRangeByScore(ctx, statusIndex, &redis.ZRangeBy{Min: since, Max: "+inf"}).Result()
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		iter := rdb.Scan(ctx, 0, "status:*", 1000).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if err := iter.Err(); err != nil {
			return nil, err
		}
	}
	out := make(map[string]json.RawMessage, len(keys))
	const chunk = 200
	for i := 0; i < len(keys); i += chunk {
		part := keys[i:min(i+chunk, len(keys))]
		vals, err := rdb.MGet(ctx, part...).Result()
		if err != nil {
			return nil, err
		}
		for j, v := range vals {
			if s, ok := v.(string); ok { // nil = gone since it was listed
				out[part[j]] = json.RawMessage(s)
			}
		}
	}
	return out, nil
}

// storeTurnScript: the key is ours when it was free or already ours, and its
// life is renewed; someone else's live key is left alone.
var storeTurnScript = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if (not v) or v == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0`)

func storeTurnRedisKey(scope string) string { return "storewatch:turn@" + scope }

// TakeStoreTurn takes the turn when it is free, or keeps it when it is ours.
func (l *LiveStatus) TakeStoreTurn(ctx context.Context, scope, me string, hold time.Duration) (bool, error) {
	n, err := storeTurnScript.Run(ctx, l.c, []string{storeTurnRedisKey(scope)}, me, hold.Milliseconds()).Int()
	return err == nil && n == 1, err
}

// askRedisKey is where the ask to report now is written beside the hot
// store's copy (monitor_state report_now).
const askRedisKey = "mon:report_now"

// Ask is the current ask (redis.Nil when none).
func (l *LiveStatus) Ask(ctx context.Context) (string, error) {
	return l.c.Get(ctx, askRedisKey).Result()
}

// SetAsk writes an ask, kept for keep.
func (l *LiveStatus) SetAsk(ctx context.Context, value string, keep time.Duration) error {
	return l.c.Set(ctx, askRedisKey, value, keep).Err()
}

// The waiting sets of builds before 2026-10-15: each vendor's receipts owed
// (await:v:<id>) and the list of those vendors (await:vendors).
const (
	awaitKeyPrefix = "await:v:"
	awaitIndexKey  = "await:vendors"
)

// DropAwaitSets removes the waiting sets an older build kept (await:v:<id>,
// await:vendors). UNLINK: a set of a million members is freed off Redis's
// main thread. Safe to call again: nothing writes them any more.
func (l *LiveStatus) DropAwaitSets(ctx context.Context) (int, error) {
	rdb := l.c
	ids, err := rdb.SMembers(ctx, awaitIndexKey).Result()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	keys := make([]string, 0, len(ids)+1)
	for _, id := range ids {
		keys = append(keys, awaitKeyPrefix+id)
	}
	keys = append(keys, awaitIndexKey)
	n, err := rdb.Unlink(ctx, keys...).Result()
	return int(n), err
}
