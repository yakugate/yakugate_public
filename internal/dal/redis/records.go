package redis

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// openRecordLanes is the record queue's opener: the queue Redis (else the
// main one), a new client each open, as recordq.FromConfig made it.
func openRecordLanes(cfg config.Config, owner string) (dal.RecordLanes, error) {
	return NewRecordLanes(FromConfig(cfg.ForQueueRedis()).Client(), owner), nil
}

// The record queue's lists in Redis (internal/recordq; moved here unchanged
// on 29 Oct 2026, the DAL plan). The keys of a deployment (owner) end in
// "@<owner>"; an install that names no deployment has none.
//
//	recq:<lane>         a lane: the tasks, first in first out
//	recq:lanes          the set of lanes anything was put on
//	recq:lock:<lane>    whose turn it is to write the lane
//	recq:park:<lane>    the lane's tasks MySQL refused, in their order
//	recq:held:<lane>    per message, how many it has parked
//	recq:pend:<lane>    per message, what a status pull may be told
//	recq:doing:<lane>   set while a batch of the lane is written
//	recq:alive          a DB worker's sign of life

// RecordLanes is one deployment's record queue on one Redis.
type RecordLanes struct {
	c     *redis.Client
	owner string
}

var _ dal.RecordLanes = (*RecordLanes)(nil)

// NewRecordLanes names the lists of the deployment owner on c.
func NewRecordLanes(c *redis.Client, owner string) *RecordLanes {
	return &RecordLanes{c: c, owner: owner}
}

// Owner is the deployment whose lists these are.
func (q *RecordLanes) Owner() string { return q.owner }

// Client is the Redis the lists are in.
func (q *RecordLanes) Client() *redis.Client { return q.c }

func recSuffix(owner string) string {
	if owner == "" {
		return ""
	}
	return "@" + owner
}
func recLaneKey(owner string, lane int) string {
	return "recq:" + strconv.Itoa(lane) + recSuffix(owner)
}
func recLockKey(owner string, lane int) string {
	return "recq:lock:" + strconv.Itoa(lane) + recSuffix(owner)
}
func recParkKey(owner string, lane int) string {
	return "recq:park:" + strconv.Itoa(lane) + recSuffix(owner)
}
func recHeldKey(owner string, lane int) string {
	return "recq:held:" + strconv.Itoa(lane) + recSuffix(owner)
}
func recPendKey(owner string, lane int) string {
	return "recq:pend:" + strconv.Itoa(lane) + recSuffix(owner)
}
func recDoingKey(owner string, lane int) string {
	return "recq:doing:" + strconv.Itoa(lane) + recSuffix(owner)
}
func recAliveKey(owner string) string { return "recq:alive" + recSuffix(owner) }
func recLanesKey(owner string) string { return "recq:lanes" + recSuffix(owner) }

// recPendFor is how long a pending entry lives if nothing removes it.
const recPendFor = 48 * time.Hour

// addRecordPut puts a RecordPut's commands on a transaction: the lanes named,
// the tasks pushed, the pending entries written with their life.
func addRecordPut(ctx context.Context, pipe redis.Pipeliner, p dal.RecordPut) {
	for _, l := range p.Name {
		pipe.SAdd(ctx, recLanesKey(p.Owner), l)
	}
	for l, vals := range p.Tasks {
		args := make([]any, len(vals))
		for i, v := range vals {
			args[i] = v
		}
		pipe.RPush(ctx, recLaneKey(p.Owner, l), args...)
	}
	for l, kv := range p.Pending {
		args := make([]any, len(kv))
		for i, v := range kv {
			args[i] = v
		}
		pipe.HSet(ctx, recPendKey(p.Owner, l), args...)
		pipe.Expire(ctx, recPendKey(p.Owner, l), recPendFor)
	}
}

// Put queues tasks in ONE transaction.
func (q *RecordLanes) Put(ctx context.Context, p dal.RecordPut) error {
	pipe := q.c.TxPipeline()
	addRecordPut(ctx, pipe, p)
	_, err := pipe.Exec(ctx)
	return err
}

// AckWithRecords is AckWith carrying a put of record tasks: the messages'
// records are queued and the messages leave their queue in ONE transaction
// (recordq.Client.RecordAndAck).
func (s *Store) AckWithRecords(ctx context.Context, vendor string, msgIDs []string, p dal.RecordPut) error {
	return s.AckWith(ctx, vendor, msgIDs, func(pipe redis.Pipeliner) { addRecordPut(ctx, pipe, p) })
}

// EnqueueWithRecords is EnqueueWith carrying a put of record tasks: the
// message's mt_ task (recordq.Client.EnqueueAccepted).
func (s *Store) EnqueueWithRecords(ctx context.Context, m model.Message, p dal.RecordPut) error {
	return s.EnqueueWith(ctx, m, func(pipe redis.Pipeliner) { addRecordPut(ctx, pipe, p) })
}

// EnqueueClientDLRsWithRecords is EnqueueClientDLRsWith carrying a put of
// record tasks: the receipts' database work (recordq.ReceiptQueue).
func (s *Store) EnqueueClientDLRsWithRecords(ctx context.Context, ds []model.ClientDLR, p dal.RecordPut) error {
	return s.EnqueueClientDLRsWith(ctx, ds, func(pipe redis.Pipeliner) { addRecordPut(ctx, pipe, p) })
}

// NamedLanes is every lane anything was ever put on, as written.
func (q *RecordLanes) NamedLanes(ctx context.Context) ([]string, error) {
	members, err := q.c.SMembers(ctx, recLanesKey(q.owner)).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	return members, nil
}

// Look counts what waits in the lanes and reads the worker's sign of life,
// in one round trip.
func (q *RecordLanes) Look(ctx context.Context, lanes []int) (waiting int64, alive bool, err error) {
	pipe := q.c.Pipeline()
	cmds := make([]*redis.IntCmd, 0, 2*len(lanes))
	for _, l := range lanes {
		cmds = append(cmds, pipe.LLen(ctx, recLaneKey(q.owner, l)), pipe.LLen(ctx, recParkKey(q.owner, l)))
	}
	aliveCmd := pipe.Exists(ctx, recAliveKey(q.owner))
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, false, err
	}
	for _, cmd := range cmds {
		waiting += cmd.Val()
	}
	return waiting, aliveCmd.Val() == 1, nil
}

// Heads reads the task at the head of each lane, in one round trip.
func (q *RecordLanes) Heads(ctx context.Context, lanes []int) ([]string, error) {
	pipe := q.c.Pipeline()
	cmds := make([]*redis.StringCmd, len(lanes))
	for i, l := range lanes {
		cmds[i] = pipe.LIndex(ctx, recLaneKey(q.owner, l), 0)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	out := make([]string, len(cmds))
	for i, cmd := range cmds {
		out[i] = cmd.Val()
	}
	return out, nil
}

// PendingOf reads msgID's pending entry on each lane, in one round trip.
func (q *RecordLanes) PendingOf(ctx context.Context, lanes []int, msgID string) ([]string, error) {
	pipe := q.c.Pipeline()
	cmds := make([]*redis.StringCmd, len(lanes))
	for i, l := range lanes {
		cmds[i] = pipe.HGet(ctx, recPendKey(q.owner, l), msgID)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	out := make([]string, len(cmds))
	for i, cmd := range cmds {
		out[i] = cmd.Val()
	}
	return out, nil
}

// HeldIn says whether msgID has a task parked on any lane, in one round trip.
func (q *RecordLanes) HeldIn(ctx context.Context, lanes []int, msgID string) (bool, error) {
	pipe := q.c.Pipeline()
	cmds := make([]*redis.BoolCmd, len(lanes))
	for i, l := range lanes {
		cmds[i] = pipe.HExists(ctx, recHeldKey(q.owner, l), msgID)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return false, err
	}
	for _, c := range cmds {
		if c.Val() {
			return true, nil
		}
	}
	return false, nil
}

// recDropIfMineScript deletes a key only while it still holds this worker's
// token: another worker of the deployment may have written it since.
var recDropIfMineScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0`)

var recTakeTurnScript = redis.NewScript(`
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

// recDoneScript takes the n tasks just handled off the head of a lane and
// parks the ones that must be tried again — only while the turn is still
// ours, so a writer whose turn ran out cannot cut tasks another writer has
// not read.
//
// It also clears the lane's "doing" mark: the batch ended cleanly, so the
// next one is not a repeat.
//
// And it removes what a status pull was told of the messages whose task is
// now written (pend): from here the record itself answers.
//
// KEYS: lane, lock, park, held, doing, pend. ARGV: token, n, k, then k
// (message id, task) pairs to park, then the message ids that were written.
var recDoneScript = redis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
local k = tonumber(ARGV[3])
redis.call('LTRIM', KEYS[1], tonumber(ARGV[2]), -1)
for i = 4, 3 + 2 * k, 2 do
  redis.call('RPUSH', KEYS[3], ARGV[i + 1])
  redis.call('HINCRBY', KEYS[4], ARGV[i], 1)
end
redis.call('DEL', KEYS[5])
if #ARGV > 3 + 2 * k then
  redis.call('HDEL', KEYS[6], unpack(ARGV, 4 + 2 * k))
end
return 1
`)

// recParkDoneScript takes the n parked tasks just tried off the head of the
// park list, puts the k that failed again back at the HEAD in their order (so
// the list keeps its order), and counts down the messages whose task is gone.
//
// KEYS: park, lock, held, pend. ARGV: token, n, k, the k tasks, then the
// message id of every task that was written or let go.
var recParkDoneScript = redis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
local k = tonumber(ARGV[3])
redis.call('LTRIM', KEYS[1], tonumber(ARGV[2]), -1)
for i = 3 + k, 4, -1 do redis.call('LPUSH', KEYS[1], ARGV[i]) end
for i = 4 + k, #ARGV do
  if redis.call('HINCRBY', KEYS[3], ARGV[i], -1) <= 0 then
    redis.call('HDEL', KEYS[3], ARGV[i])
    redis.call('HDEL', KEYS[4], ARGV[i])
  end
end
return 1
`)

// Beat writes the worker's token as its sign of life, for ttl.
func (q *RecordLanes) Beat(ctx context.Context, token string, ttl time.Duration) error {
	return q.c.Set(ctx, recAliveKey(q.owner), token, ttl).Err()
}

// Leave deletes the sign of life while it is still token's.
func (q *RecordLanes) Leave(ctx context.Context, token string) error {
	return recDropIfMineScript.Run(ctx, q.c, []string{recAliveKey(q.owner)}, token).Err()
}

// TakeTurn takes or renews a lane's turn: 1 = token's.
func (q *RecordLanes) TakeTurn(ctx context.Context, lane int, token string, ttl time.Duration) (int, error) {
	return recTakeTurnScript.Run(ctx, q.c, []string{recLockKey(q.owner, lane)}, token, int(ttl/time.Millisecond)).Int()
}

// DropTurn lets a lane's turn go while it is token's.
func (q *RecordLanes) DropTurn(ctx context.Context, lane int, token string) error {
	return q.c.Eval(ctx, `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0`,
		[]string{recLockKey(q.owner, lane)}, token).Err()
}

// Batch reads the lane's head, its held messages and its "doing" mark in one
// round trip.
func (q *RecordLanes) Batch(ctx context.Context, lane, most int) ([]string, map[string]string, bool, error) {
	pipe := q.c.Pipeline()
	rawCmd := pipe.LRange(ctx, recLaneKey(q.owner, lane), 0, int64(most)-1)
	heldCmd := pipe.HGetAll(ctx, recHeldKey(q.owner, lane))
	doingCmd := pipe.Exists(ctx, recDoingKey(q.owner, lane))
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, nil, false, err
	}
	return rawCmd.Val(), heldCmd.Val(), doingCmd.Val() == 1, nil
}

// MarkDoing sets the lane's "doing" mark.
func (q *RecordLanes) MarkDoing(ctx context.Context, lane int, token string) error {
	return q.c.Set(ctx, recDoingKey(q.owner, lane), token, 0).Err()
}

// Done runs the script that ends a written batch.
func (q *RecordLanes) Done(ctx context.Context, lane int, token string, n int, park [][2]string, written []string) (int, error) {
	args := make([]any, 0, 3+2*len(park)+len(written))
	args = append(args, token, n, len(park))
	for _, p := range park {
		args = append(args, p[0], p[1])
	}
	for _, id := range written {
		args = append(args, id)
	}
	return recDoneScript.Run(ctx, q.c, []string{recLaneKey(q.owner, lane), recLockKey(q.owner, lane), recParkKey(q.owner, lane),
		recHeldKey(q.owner, lane), recDoingKey(q.owner, lane), recPendKey(q.owner, lane)}, args...).Int()
}

// Len is how many tasks wait on the lane.
func (q *RecordLanes) Len(ctx context.Context, lane int) (int64, error) {
	return q.c.LLen(ctx, recLaneKey(q.owner, lane)).Result()
}

// Parked reads the head of the lane's park list.
func (q *RecordLanes) Parked(ctx context.Context, lane, most int) ([]string, error) {
	raw, err := q.c.LRange(ctx, recParkKey(q.owner, lane), 0, int64(most)-1).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	return raw, nil
}

// ParkDone runs the script that ends a try of the parked tasks.
func (q *RecordLanes) ParkDone(ctx context.Context, lane int, token string, n int, back, gone []string) (int, error) {
	args := make([]any, 0, 3+len(back)+len(gone))
	args = append(args, token, n, len(back))
	for _, b := range back {
		args = append(args, b)
	}
	for _, g := range gone {
		args = append(args, g)
	}
	return recParkDoneScript.Run(ctx, q.c, []string{recParkKey(q.owner, lane), recLockKey(q.owner, lane),
		recHeldKey(q.owner, lane), recPendKey(q.owner, lane)}, args...).Int()
}

// ParkedLen is how many tasks are parked on the lane.
func (q *RecordLanes) ParkedLen(ctx context.Context, lane int) (int64, error) {
	return q.c.LLen(ctx, recParkKey(q.owner, lane)).Result()
}

// AppendOnly asks Redis whether it keeps an append-only file.
func (q *RecordLanes) AppendOnly(ctx context.Context) (string, bool) {
	v, err := q.c.ConfigGet(ctx, "appendonly").Result()
	if err != nil {
		return "", false
	}
	got, ok := v["appendonly"]
	return got, ok
}

// Unsent: NotQueued.
func (q *RecordLanes) Unsent(err error) bool { return NotQueued(err) }

// NotQueued reports whether err leaves nothing in Redis: Redis could not be
// reached at all (no connection made, a closed client, no free connection in
// the pool), or Redis answered and refused the transaction whole — EXECABORT
// (the transaction was discarded), LOADING (a Redis started again, still
// reading its file: the drill of 2026-10-14 met 108 of these in the second
// after the restart), READONLY (a replica). Moved from recordq.NotQueued.
func NotQueued(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, redis.ErrClosed) {
		return true
	}
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return true
	}
	msg := err.Error()
	for _, refused := range []string{
		"EXECABORT",                      // the transaction was discarded: nothing in it ran
		"LOADING ",                       // Redis is loading its data and runs no command
		"READONLY ",                      // a replica: it writes nothing
		"redis: connection pool timeout", // go-redis keeps this one unexported (internal/pool.ErrPoolTimeout)
	} {
		if strings.Contains(msg, refused) {
			return true
		}
	}
	return false
}
