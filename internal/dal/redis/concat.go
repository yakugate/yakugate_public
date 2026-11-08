package redis

// The multi-part buffer in Redis (internal/concat's RedisAssembler; moved
// here unchanged on 29 Oct 2026, the DAL plan) — the assemble-first buffer
// for deployments whose parts wait in the MAIN Redis (PO 2026-08-09:
// "buffer mirrors the queue backend"; YAKU_CONCAT_*_STORE since 28 Oct).
//
// Key layout (one Redis, single-node semantics — the deployment's optional
// standby is a dial-level failover, not a cluster, so multi-key Lua is safe):
//
//	concat:h:<group-key>   HASH  seq -> part payload (JSON); one field per part
//	concat:idx             ZSET  group-key scored by the OLDEST part's arrival (unix)
//
// and concat:mo:h: / concat:mo:idx for incoming messages (2026-09-14), so
// each reaper sees only its own groups. The hash also carries a TTL as a
// self-cleaning safety net; the ZSET index is what the reaper scans to find
// groups that timed out. Durability of in-flight partials is the deployment's
// responsibility (persistent/durable Redis is assumed — AOF + standby).

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// openConcatBuffer is the buffer's opener: long messages' parts wait in the
// MAIN Redis (YAKU_CONCAT_*_STORE).
func openConcatBuffer(cfg config.Config, mo bool) (dal.ConcatBuffer, error) {
	return NewConcatBuffer(FromConfig(cfg).Client(), mo), nil
}

const (
	concatHashPrefix = "concat:h:" // + group-key
	concatIndexKey   = "concat:idx"
	// concatTTLGrace pads the hash TTL beyond the assembly timeout so the
	// reaper (which records the group as expired) normally wins the race
	// against Redis reclaiming the key silently. If the reaper is down long
	// enough for the TTL to fire anyway, the hash self-cleans — we lose the
	// expiry RECORD, never correctness.
	concatTTLGrace = 30 * time.Second
)

// ConcatBuffer buffers parts in Redis. Safe for concurrent use across binds
// and instances: completion and expiry are each a single atomic Lua script,
// so exactly one caller assembles or expires any given group.
type ConcatBuffer struct {
	c                    *redis.Client
	hashPrefix, indexKey string
}

var (
	_ dal.ConcatBuffer  = (*ConcatBuffer)(nil)
	_ dal.ConcatPending = (*ConcatBuffer)(nil)
)

// NewConcatBuffer keeps one side's parts on c: MT (concat:h:, concat:idx) or
// MO (concat:mo:h:, concat:mo:idx).
func NewConcatBuffer(c *redis.Client, mo bool) *ConcatBuffer {
	if mo {
		return &ConcatBuffer{c: c, hashPrefix: "concat:mo:h:", indexKey: "concat:mo:idx"}
	}
	return &ConcatBuffer{c: c, hashPrefix: concatHashPrefix, indexKey: concatIndexKey}
}

func (r *ConcatBuffer) hashKey(groupKey string) string { return r.hashPrefix + groupKey }

// concatAddScript buffers one part and, when the group is complete, claims it
// atomically. Doing add + completeness-check + claim in ONE script is what
// makes exactly-once assembly hold across racing final-part arrivals: the
// loser sees the hash already deleted and returns "still waiting".
//
//	KEYS[1] = hash key            KEYS[2] = index key
//	ARGV[1] = group key           ARGV[2] = seq          ARGV[3] = payload
//	ARGV[4] = total parts         ARGV[5] = now (unix)   ARGV[6] = hash TTL (sec)
//
// Returns [] while waiting, or the flat [seq, payload, ...] of a complete group.
var concatAddScript = redis.NewScript(`
local isNew = (redis.call('EXISTS', KEYS[1]) == 0)
redis.call('HSET', KEYS[1], ARGV[2], ARGV[3])   -- idempotent on a retransmitted seq
if isNew then
  redis.call('ZADD', KEYS[2], ARGV[5], ARGV[1])  -- index by first-part arrival
  redis.call('EXPIRE', KEYS[1], ARGV[6])         -- self-cleaning safety net
end
if redis.call('HLEN', KEYS[1]) < tonumber(ARGV[4]) then
  return {}
end
local fields = redis.call('HGETALL', KEYS[1])
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return fields
`)

// AddPart runs the add script.
func (r *ConcatBuffer) AddPart(ctx context.Context, gk string, _ int64, seq, total int, payload string, timeout time.Duration) ([]*model.Message, error) {
	ttl := int((timeout + concatTTLGrace).Seconds())
	raw, err := concatAddScript.Run(ctx, r.c,
		[]string{r.hashKey(gk), r.indexKey},
		gk, seq, payload, total, time.Now().Unix(), ttl,
	).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("buffer part: %w", err)
	}
	if len(raw) == 0 {
		return nil, nil // still waiting for more parts
	}
	return concatPartsFromHash(raw)
}

// concatClaimScript atomically hands a single expired group to one reaper:
// it reads the parts, then deletes the hash and its index entry. A second
// reaper racing the same group gets an empty result and skips it.
//
//	KEYS[1] = hash key   KEYS[2] = index key   ARGV[1] = group key
var concatClaimScript = redis.NewScript(`
local fields = redis.call('HGETALL', KEYS[1])
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return fields
`)

// ClaimExpired reads the index for groups whose oldest part is older than
// the timeout (at most 100 a call; the reaper ticks again for the rest) and
// claims each one atomically.
func (r *ConcatBuffer) ClaimExpired(ctx context.Context, timeout time.Duration, each func(string, []*model.Message, error)) error {
	cutoff := time.Now().Add(-timeout).Unix()
	groupKeys, err := r.c.ZRangeByScore(ctx, r.indexKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", cutoff),
		Count: 100, // bound the batch; the reaper ticks again for the rest
	}).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	for _, gk := range groupKeys {
		raw, err := concatClaimScript.Run(ctx, r.c,
			[]string{r.hashKey(gk), r.indexKey}, gk).StringSlice()
		if err != nil && err != redis.Nil {
			return err
		}
		parts, err := concatPartsFromHash(raw)
		if err != nil {
			each(gk, nil, err)
			continue
		}
		if len(parts) == 0 {
			continue // already claimed by another reaper
		}
		each(gk, parts, nil)
	}
	return nil
}

// concatPartsFromHash decodes a flat Redis HGETALL result ([seq, payload,
// ...]) into the buffered part messages. Seq keys are ignored — Assemble
// re-sorts by the ConcatSeq carried inside each payload.
func concatPartsFromHash(flat []string) ([]*model.Message, error) {
	parts := make([]*model.Message, 0, len(flat)/2)
	for i := 1; i < len(flat); i += 2 {
		var m model.Message
		if err := json.Unmarshal([]byte(flat[i]), &m); err != nil {
			return nil, fmt.Errorf("unmarshal buffered part: %w", err)
		}
		parts = append(parts, &m)
	}
	return parts, nil
}

// Pending reports how many groups are waiting for parts and how long the
// oldest has waited — for monitoring, read from the index ZSET (score = the
// oldest part's arrival, unix seconds). Two cheap reads, no scan.
func (r *ConcatBuffer) Pending(ctx context.Context) (groups, oldestAgeSec int64, err error) {
	pipe := r.c.Pipeline()
	card := pipe.ZCard(ctx, r.indexKey)
	head := pipe.ZRangeWithScores(ctx, r.indexKey, 0, 0)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, 0, err
	}
	groups = card.Val()
	if h := head.Val(); len(h) > 0 {
		oldestAgeSec = time.Now().Unix() - int64(h[0].Score)
	}
	return groups, oldestAgeSec, nil
}
