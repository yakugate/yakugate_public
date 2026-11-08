package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// openReceipts is the receipts' opener: the facts and the write-behind list
// on the main Redis, on one client (the vendor roles kept them on one).
func openReceipts(cfg config.Config, owner string) (*dal.Receipts, error) {
	c := FromConfig(cfg).Client()
	return &dal.Receipts{Facts: NewReceiptFacts(c), Writes: NewReceiptWrites(c, owner)}, nil
}

// openHandOffs is the hand-off's opener: the main Redis, with the queue Redis
// when the queues are there (the clients handoff.FromConfig made).
func openHandOffs(cfg config.Config) (dal.HandOffs, error) {
	var queue *redis.Client
	if cfg.QueuesInRedis() {
		queue = FromConfig(cfg.ForQueueRedis()).Client()
	}
	return NewHandOffs(FromConfig(cfg).Client(), queue), nil
}

// The receipt facts and the receipts' write-behind list in the main Redis
// (internal/receiptfacts; moved here unchanged on 29 Oct 2026, the DAL plan).
//
//	rf:<scope>:<vendor msg id>   a message's facts, one value (dlr.FactsKey)
//	rfp:<msg id>                 a long message's part counter
//	rcptw[@<owner>]              a deployment's receipts' database work
//	rcptw:lock[@<owner>]         whose turn it is to write that list

// ReceiptFacts is the receipt facts on one Redis.
type ReceiptFacts struct{ c *redis.Client }

var _ dal.ReceiptFacts = (*ReceiptFacts)(nil)

// NewReceiptFacts keeps the facts on c.
func NewReceiptFacts(c *redis.Client) *ReceiptFacts { return &ReceiptFacts{c: c} }

// File files many facts in one pipeline: one SET a message (APPEND and
// EXPIRE for a group member).
func (s *ReceiptFacts) File(ctx context.Context, puts []dal.FactPut) error {
	p := s.c.Pipeline()
	for _, e := range puts {
		if e.Add {
			p.Append(ctx, e.Key, e.Value)
			p.Expire(ctx, e.Key, e.Life)
			continue
		}
		p.Set(ctx, e.Key, e.Value, e.Life)
	}
	_, err := p.Exec(ctx)
	return err
}

// Get reads a key's value (GET).
func (s *ReceiptFacts) Get(ctx context.Context, key string) (string, bool, error) {
	raw, err := s.c.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return raw, true, nil
}

// Take reads a key's value and deletes it in one call (GETDEL).
func (s *ReceiptFacts) Take(ctx context.Context, key string) (string, bool, error) {
	raw, err := s.c.GetDel(ctx, key).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return raw, true, nil
}

// Set files a value for life (SET … EX).
func (s *ReceiptFacts) Set(ctx context.Context, key, value string, life time.Duration) error {
	return s.c.Set(ctx, key, value, life).Err()
}

// Looks is GETDEL for an ask that takes, GET for one that does not, in one
// pipeline, answered in the order asked.
func (s *ReceiptFacts) Looks(ctx context.Context, looks []dal.FactLook) ([]string, []bool, error) {
	vals := make([]string, len(looks))
	found := make([]bool, len(looks))
	if len(looks) == 0 {
		return vals, found, nil
	}
	pipe := s.c.Pipeline()
	cmds := make([]*redis.StringCmd, len(looks))
	for i, a := range looks {
		if a.Take {
			cmds[i] = pipe.GetDel(ctx, a.Key)
		} else {
			cmds[i] = pipe.Get(ctx, a.Key)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, nil, err
	}
	for i, c := range cmds {
		raw, err := c.Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		vals[i], found[i] = raw, true
	}
	return vals, found, nil
}

// CountPart is INCRBY on the counter, and EXPIRE once when this call made it.
func (s *ReceiptFacts) CountPart(ctx context.Context, key string, n int64, life time.Duration) (int64, error) {
	got, err := s.c.IncrBy(ctx, key, n).Result()
	if err != nil {
		return 0, err
	}
	if got == n {
		_ = s.c.Expire(ctx, key, life).Err() // made just now: it lives as long as the message's facts
	}
	return got, nil
}

// Gone is one EXISTS a key, in one round trip.
func (s *ReceiptFacts) Gone(ctx context.Context, keys []string) (map[string]bool, error) {
	return factsGone(ctx, s.c, keys)
}

// factsGone says which keys are not there: one EXISTS a key, one pipeline.
func factsGone(ctx context.Context, c redis.Cmdable, keys []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(keys) == 0 {
		return out, nil
	}
	pipe := c.Pipeline()
	cmds := make([]*redis.IntCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.Exists(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return out, err
	}
	for i, cmd := range cmds {
		if cmd.Val() == 0 {
			out[keys[i]] = true
		}
	}
	return out, nil
}

// factsOldKeysDone marks that the keys of the build before 29 Oct 2026 were
// let go.
const factsOldKeysDone = "rf:upgraded:2026-10-29"

// DropOldKeys lets go, once for the whole gateway, of what the receipt facts
// kept before 29 Oct 2026 that nothing reads any more: the facts hashes
// (rfm:), the waiting sets and their list (wait:), the told marks (told:), the
// groups' sets (rf:…:g) and the pointers by number (rf:…#…). One SCAN of the
// keyspace, by the first dlr role to start on this build; the others see the
// mark and skip it. The old one-message pointers (rf:<scope>:<id>, holding a
// message id) are left to run out: a receipt that takes one reads nothing.
func (s *ReceiptFacts) DropOldKeys(ctx context.Context) (int64, error) {
	ok, err := s.c.SetNX(ctx, factsOldKeysDone, time.Now().UTC().Format(time.RFC3339), 0).Result()
	if err != nil || !ok {
		return 0, err
	}
	var dropped int64
	for _, pattern := range []string{"rfm:*", "wait:*", "told:*", "rf:*:g", "rf:*#*"} {
		iter := s.c.Scan(ctx, 0, pattern, 1000).Iterator()
		var keys []string
		flush := func() error {
			if len(keys) == 0 {
				return nil
			}
			n, err := s.c.Unlink(ctx, keys...).Result()
			dropped += n
			keys = keys[:0]
			return err
		}
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
			if len(keys) == 500 {
				if err := flush(); err != nil {
					return dropped, err
				}
			}
		}
		if err := iter.Err(); err != nil {
			return dropped, err
		}
		if err := flush(); err != nil {
			return dropped, err
		}
	}
	return dropped, nil
}

// ---- the write-behind list ----

// ReceiptWrites is one deployment's rcptw list on one Redis.
type ReceiptWrites struct {
	c     *redis.Client
	owner string
	key   string
	lock  string
}

var _ dal.ReceiptWrites = (*ReceiptWrites)(nil)

func rcptwKey(owner string) string {
	if owner == "" {
		return "rcptw"
	}
	return "rcptw@" + owner
}

// NewReceiptWrites names the list of the deployment owner on c.
func NewReceiptWrites(c *redis.Client, owner string) *ReceiptWrites {
	suffix := ""
	if owner != "" {
		suffix = "@" + owner
	}
	return &ReceiptWrites{c: c, owner: owner, key: "rcptw" + suffix, lock: "rcptw:lock" + suffix}
}

// Owner is the deployment whose list this is.
func (q *ReceiptWrites) Owner() string { return q.owner }

func strArgs(vals []string) []any {
	out := make([]any, len(vals))
	for i, v := range vals {
		out[i] = v
	}
	return out
}

// Push is one RPUSH on this deployment's list.
func (q *ReceiptWrites) Push(ctx context.Context, values []string) error {
	return q.c.RPush(ctx, q.key, strArgs(values)...).Err()
}

// PushTo is one RPUSH a list, in one transaction.
func (q *ReceiptWrites) PushTo(ctx context.Context, owners []string, values map[string][]string) error {
	pipe := q.c.TxPipeline()
	for _, o := range owners {
		pipe.RPush(ctx, rcptwKey(o), strArgs(values[o])...)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Len is LLEN of this deployment's list.
func (q *ReceiptWrites) Len(ctx context.Context) (int64, error) {
	return q.c.LLen(ctx, q.key).Result()
}

// rcptwTakeTurnScript takes the writer's turn, or keeps it: true while it is
// ours.
var rcptwTakeTurnScript = redis.NewScript(`
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

// rcptwDoneScript removes the n rows just handled from the head and adds back
// what is still owed — only while the turn is still ours, so a writer whose
// turn ran out cannot cut rows another writer has not read.
var rcptwDoneScript = redis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
redis.call('LTRIM', KEYS[1], tonumber(ARGV[2]), -1)
for i = 3, #ARGV do redis.call('RPUSH', KEYS[1], ARGV[i]) end
return 1
`)

// TakeTurn runs the turn script.
func (q *ReceiptWrites) TakeTurn(ctx context.Context, token string, ttl time.Duration) (int, error) {
	return rcptwTakeTurnScript.Run(ctx, q.c, []string{q.lock}, token, int(ttl/time.Millisecond)).Int()
}

// DropTurn drops the turn if it is still token's.
func (q *ReceiptWrites) DropTurn(ctx context.Context, token string) error {
	return q.c.Eval(ctx, `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0`,
		[]string{q.lock}, token).Err()
}

// Head is LRANGE of the head.
func (q *ReceiptWrites) Head(ctx context.Context, most int) ([]string, error) {
	return q.c.LRange(ctx, q.key, 0, int64(most)-1).Result()
}

// Done runs the script that ends a written batch.
func (q *ReceiptWrites) Done(ctx context.Context, token string, n int, back []string) (int, error) {
	args := make([]any, 0, len(back)+2)
	args = append(args, token, n)
	for _, b := range back {
		args = append(args, b)
	}
	return rcptwDoneScript.Run(ctx, q.c, []string{q.key, q.lock}, args...).Int()
}

// ---- the hand-off lists ----

// HandOffs is the hand-off lists in the main Redis (internal/handoff; moved
// here unchanged on 29 Oct 2026), with this gateway's queue Redis when its
// queues are there.
//
//	queues:<owner>:handoff:<kind>          the items parked for owner
//	queues:<owner>:handoff:<kind>:lock     whose turn it is to import them
//	queues:<owner>:handoff:<kind>:strays   the one-time walk's lock
//	handoff:stores                         deployment -> its queue store's id
//	qstore:id (in the queue Redis)         the queue store's own id
type HandOffs struct {
	main  *redis.Client
	queue *redis.Client // nil = this gateway's queues are in MySQL
}

var _ dal.HandOffs = (*HandOffs)(nil)

// NewHandOffs keeps the lists on main; queue is this gateway's queue Redis,
// nil when its queues are in MySQL.
func NewHandOffs(main, queue *redis.Client) *HandOffs {
	if main == nil {
		return nil
	}
	return &HandOffs{main: main, queue: queue}
}

const (
	handoffStoresKey  = "handoff:stores"
	handoffStoreIDKey = "qstore:id"
)

func handoffListKey(owner, kind string) string  { return "queues:" + owner + ":handoff:" + kind }
func handoffLockKey(owner, kind string) string  { return handoffListKey(owner, kind) + ":lock" }
func handoffStrayKey(owner, kind string) string { return handoffListKey(owner, kind) + ":strays" }

// QueuesHere says this gateway's queues are in Redis.
func (h *HandOffs) QueuesHere() bool { return h.queue != nil }

// QueueStoreID is SET NX then GET of the queue store's id.
func (h *HandOffs) QueueStoreID(ctx context.Context, newID string) (string, error) {
	if err := h.queue.SetNX(ctx, handoffStoreIDKey, newID, 0).Err(); err != nil {
		return "", err
	}
	return h.queue.Get(ctx, handoffStoreIDKey).Result()
}

// NameStore is HSET of me's id.
func (h *HandOffs) NameStore(ctx context.Context, me, id string) error {
	return h.main.HSet(ctx, handoffStoresKey, me, id).Err()
}

// UnnameStore is HDEL of me.
func (h *HandOffs) UnnameStore(ctx context.Context, me string) error {
	return h.main.HDel(ctx, handoffStoresKey, me).Err()
}

// Stores is HGETALL of the stores' ids.
func (h *HandOffs) Stores(ctx context.Context) (map[string]string, error) {
	return h.main.HGetAll(ctx, handoffStoresKey).Result()
}

// handoffParkScript appends items to an owner's list, each stamped with
// Redis's own time ({"p":<ms>,"x":<item>}), all or none.
var handoffParkScript = redis.NewScript(`
local t = redis.call('TIME')
local ms = string.format('%d', t[1] * 1000 + math.floor(t[2] / 1000))
for i = 1, #ARGV do
  redis.call('RPUSH', KEYS[1], '{"p":' .. ms .. ',"x":' .. ARGV[i] .. '}')
end
return redis.call('LLEN', KEYS[1])
`)

// Park runs the park script.
func (h *HandOffs) Park(ctx context.Context, owner, kind string, items []string) error {
	return handoffParkScript.Run(ctx, h.main, []string{handoffListKey(owner, kind)}, strArgs(items)...).Err()
}

// handoffTakeTurnScript takes the importer's turn, or keeps it: 1 while it is
// ours (the receipt writer's, rcptwTakeTurnScript).
var handoffTakeTurnScript = redis.NewScript(`
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

// handoffTrimScript removes the n items just filed from the head — only while
// the turn is still ours, so an importer whose turn ran out cannot cut items
// another has not filed. Parkers only add at the tail.
var handoffTrimScript = redis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
redis.call('LTRIM', KEYS[1], tonumber(ARGV[2]), -1)
return 1
`)

// TakeTurn runs the turn script.
func (h *HandOffs) TakeTurn(ctx context.Context, owner, kind, token string, ttl time.Duration) (int, error) {
	return handoffTakeTurnScript.Run(ctx, h.main, []string{handoffLockKey(owner, kind)}, token, int(ttl/time.Millisecond)).Int()
}

// DropTurn drops the turn if it is still token's.
func (h *HandOffs) DropTurn(ctx context.Context, owner, kind, token string) error {
	return h.main.Eval(ctx, `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0`,
		[]string{handoffLockKey(owner, kind)}, token).Err()
}

// Head is LRANGE of the head.
func (h *HandOffs) Head(ctx context.Context, owner, kind string, most int) ([]string, error) {
	return h.main.LRange(ctx, handoffListKey(owner, kind), 0, int64(most)-1).Result()
}

// Now is TIME.
func (h *HandOffs) Now(ctx context.Context) (time.Time, error) { return h.main.Time(ctx).Result() }

// Trim runs the trim script.
func (h *HandOffs) Trim(ctx context.Context, owner, kind, token string, n int) (int, error) {
	return handoffTrimScript.Run(ctx, h.main, []string{handoffListKey(owner, kind), handoffLockKey(owner, kind)}, token, n).Int()
}

// TakeStrays is SET NX of the walk's lock.
func (h *HandOffs) TakeStrays(ctx context.Context, owner, kind, token string, ttl time.Duration) (bool, error) {
	return h.main.SetNX(ctx, handoffStrayKey(owner, kind), token, ttl).Result()
}

// DropStrays drops the walk's lock if it is still token's.
func (h *HandOffs) DropStrays(ctx context.Context, owner, kind, token string) error {
	return h.main.Eval(ctx, `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0`,
		[]string{handoffStrayKey(owner, kind)}, token).Err()
}

// Waiting is LLEN, LINDEX 0 and TIME in one pipeline.
func (h *HandOffs) Waiting(ctx context.Context, owner, kind string) (int64, string, time.Time, error) {
	key := handoffListKey(owner, kind)
	pipe := h.main.Pipeline()
	ln := pipe.LLen(ctx, key)
	head := pipe.LIndex(ctx, key, 0)
	now := pipe.Time(ctx)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, "", time.Time{}, err
	}
	s, err := head.Result()
	if err != nil {
		s = ""
	}
	return ln.Val(), s, now.Val(), nil
}

// FactsGone is one EXISTS a key on the main Redis, in one round trip.
func (h *HandOffs) FactsGone(ctx context.Context, keys []string) (map[string]bool, error) {
	return factsGone(ctx, h.main, keys)
}
