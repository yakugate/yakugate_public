package redis

// The outgoing (MT) queues in Redis — issue 22 of the design audit with issue
// 25's names (Saleem and the PO, 23–26 Oct 2026). Package queues has the
// rules both stores share; this is how Redis keeps them:
//
//	queues:<dep>:mt:<cid>:<sub>:ready    ZSET  the item, scored by its priority
//	queues:<dep>:mt:<cid>:<sub>:delayed  ZSET  the item, scored by its pick time
//	queues:<dep>:mt:<cid>:list           SET   the vendor's sub-queues with items
//	queues:<dep>:mt:<cid>:group:<g>      SET   the queued parts of one long message
//	queues:<dep>:mt:vendors              SET   the vendors with queues here
//
// <dep> is the deployment that sends the message (the accepting gateway,
// D-121), "shared" when no deployment is named; <sub> is "common" or one of
// the vendor's short codes. The item is the whole message (queues.Item), so a
// message has no keys of its own: no payload, no attempt counter, no lease.
// A take removes what it takes.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queueflow"
	"github.com/yakugate/yakugate/internal/queues"
	"github.com/yakugate/yakugate/internal/store"
)

// scopes are the deployments whose queues this process takes from: its own,
// then the shared one (messages queued by a gateway that names no
// deployment). Only the shared one when this process names none.
func (s *Store) scopes() []string {
	own := queues.Scope(s.owner)
	if own == queues.Scope("") {
		return []string{own}
	}
	return []string{own, queues.Scope("")}
}

// subOf is the sub-queue a message waits in.
func subOf(m model.Message) string {
	if m.ShortCode != "" {
		return m.ShortCode
	}
	return queues.Common
}

// groupOf names the queued parts of one long message: they are found by the
// same five things the MySQL store matches (D-036).
func groupOf(accountID int64, sender, dest string, ref, total int) string {
	return fmt.Sprintf("%d|%s|%s|%d|%d", accountID, sender, dest, ref, total)
}

// itemOf is the message as its queue keeps it.
func itemOf(m model.Message) (string, error) {
	if m.Encoding == "" {
		m.Encoding = "gsm7"
	}
	if m.Status == "" {
		m.Status = model.StatusQueued
	}
	m.QueueItem = ""
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return queues.Item(m.ID, m.Attempts, string(b)), nil
}

// fromItem reads a message back from its item; its attempt is the item's.
func fromItem(item, vendor string) (model.Message, bool) {
	_, att, payload, ok := queues.ParseItem(item)
	if !ok {
		return model.Message{}, false
	}
	var m model.Message
	if json.Unmarshal([]byte(payload), &m) != nil {
		return model.Message{}, false
	}
	m.Attempts, m.Vendor, m.QueueItem = att, vendor, item
	return m, true
}

// addItem puts one item on its queue in pipe: ready now, or delayed until
// m.NextAttemptAt when that is still to come.
func addItem(ctx context.Context, pipe redis.Pipeliner, m model.Message, item string) {
	sc, sub, prio := queues.Scope(m.QueueOwner), subOf(m), queues.Priority(m.Priority)
	if at := m.NextAttemptAt; !at.IsZero() && at.After(time.Now()) {
		pipe.ZAdd(ctx, queues.MTDelayed(sc, m.Vendor, sub), redis.Z{Score: queues.DelayedScore(at, prio), Member: item})
	} else {
		pipe.ZAdd(ctx, queues.MTReady(sc, m.Vendor, sub), redis.Z{Score: float64(prio), Member: item})
	}
	pipe.SAdd(ctx, queues.MTList(sc, m.Vendor), sub)
	pipe.SAdd(ctx, queues.MTVendors(sc), m.Vendor)
	if m.ConcatTotal > 1 {
		pipe.SAdd(ctx, queues.MTGroup(sc, m.Vendor, groupOf(m.AccountID, m.Sender, m.Destination, m.ConcatRef, m.ConcatTotal)), item)
	}
}

// Enqueue puts a message on its vendor's queue (m.Vendor, the deployment
// m.QueueOwner, the sub-queue m.ShortCode): ready at once, or delayed until
// m.NextAttemptAt (a scheduled send, a retry after its gap). Its attempt is
// m.Attempts. One transaction.
func (s *Store) Enqueue(ctx context.Context, m model.Message) error {
	item, err := itemOf(m)
	if err != nil {
		return err
	}
	pipe := s.c.TxPipeline()
	addItem(ctx, pipe, m, item)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	queueflow.In(queueflow.MT, m.Vendor, 1) // monitoring: the queue's rate in (D-127)
	return nil
}

// EnqueueWith is Enqueue with more commands in the same transaction: the
// accept path's mt_ task on the record queue (recordq.Client.EnqueueAccepted,
// the three-table design of 27 Oct 2026), so a message is queued for its
// vendor and for its record together, or not at all.
func (s *Store) EnqueueWith(ctx context.Context, m model.Message, more func(pipe redis.Pipeliner)) error {
	item, err := itemOf(m)
	if err != nil {
		return err
	}
	pipe := s.c.TxPipeline()
	addItem(ctx, pipe, m, item)
	if more != nil {
		more(pipe)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	queueflow.In(queueflow.MT, m.Vendor, 1)
	return nil
}

// Return puts messages taken but not sent back on their queues, ready at
// once and with the attempt they had: a session that ended before it could
// send them.
func (s *Store) Return(ctx context.Context, ms ...model.Message) error {
	if len(ms) == 0 {
		return nil
	}
	pipe := s.c.TxPipeline()
	for _, m := range ms {
		m.NextAttemptAt = time.Time{}
		item, err := itemOf(m)
		if err != nil {
			return err
		}
		addItem(ctx, pipe, m, item)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// prepScript moves a vendor's due items from delayed to ready and counts what
// each of its sub-queues has ready (with ARGV[2] = 1, by priority as well).
// A sub-queue with nothing left is taken off the vendor's list.
//
// ARGV: the highest due delayed score, 1 = count by priority, the vendor,
// then the scopes. The answer: scope, sub, ready[, ready of priority 1…9],
// for every sub-queue with something ready.
var prepScript = redis.NewScript(`
local due = ARGV[1]
local byPrio = ARGV[2] == '1'
local vendor = ARGV[3]
local out = {}
for i = 4, #ARGV do
  local base = 'queues:' .. ARGV[i] .. ':mt:' .. vendor
  local subs = redis.call('SMEMBERS', base .. ':list')
  for _, sub in ipairs(subs) do
    local ready = base .. ':' .. sub .. ':ready'
    local delayed = base .. ':' .. sub .. ':delayed'
    local moving = redis.call('ZRANGEBYSCORE', delayed, '-inf', due, 'WITHSCORES', 'LIMIT', 0, 1000)
    for k = 1, #moving, 2 do
      redis.call('ZREM', delayed, moving[k])
      redis.call('ZADD', ready, tonumber(moving[k+1]) % 10, moving[k])
    end
    local n = redis.call('ZCARD', ready)
    if n > 0 then
      out[#out+1] = ARGV[i]
      out[#out+1] = sub
      out[#out+1] = n
      if byPrio then
        for p = 1, 9 do out[#out+1] = redis.call('ZCOUNT', ready, p, p) end
      end
    elseif redis.call('ZCARD', delayed) == 0 then
      redis.call('SREM', base .. ':list', sub)
    end
  end
end
return out
`)

// popScript takes ARGV[i] items, lowest score (priority) first and, within a
// priority, oldest first, from each ready set KEYS[i]. The answer is the
// items, each after the place of its key.
var popScript = redis.NewScript(`
local out = {}
for i = 1, #KEYS do
  local got = redis.call('ZPOPMIN', KEYS[i], ARGV[i])
  for k = 1, #got, 2 do
    out[#out+1] = i
    out[#out+1] = got[k]
  end
end
return out
`)

// Claim takes up to n messages of a vendor, its rate split fairly between
// its queues. [store.Queue]
func (s *Store) Claim(ctx context.Context, vendor string, n int) ([]model.Message, error) {
	return s.ClaimSplit(ctx, vendor, n, queues.Fair)
}

// ClaimSplit takes up to n messages of a vendor from its queues in this
// process's scopes: due delayed items are made ready, n is split between the
// sub-queues by mode (queues.Split), and each gives its share. What it takes
// is gone from the queue: nothing is acknowledged.
func (s *Store) ClaimSplit(ctx context.Context, vendor string, n int, mode queues.Mode) ([]model.Message, error) {
	if n <= 0 {
		return nil, nil
	}
	scs := s.scopes()
	args := []any{queues.DueScore(time.Now()), boolArg(mode == queues.ByPriority), vendor}
	for _, sc := range scs {
		args = append(args, sc)
	}
	raw, err := prepScript.Run(ctx, s.c, nil, args...).Slice()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	var keys []string
	var waits []queues.Waiting
	step := 3
	if mode == queues.ByPriority {
		step = 12
	}
	for i := 0; i+step <= len(raw); i += step {
		sc, _ := raw[i].(string)
		sub, _ := raw[i+1].(string)
		w := queues.Waiting{Ready: toInt(raw[i+2])}
		if mode == queues.ByPriority {
			for p := 1; p <= 9; p++ {
				w.ByPrio[p] = toInt(raw[i+2+p])
			}
		}
		keys = append(keys, queues.MTReady(sc, vendor, sub))
		waits = append(waits, w)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	share := queues.Split(n, waits, mode, queues.Turn(vendor))
	var popKeys []string
	var counts []any
	for i, k := range keys {
		if share[i] > 0 {
			popKeys = append(popKeys, k)
			counts = append(counts, share[i])
		}
	}
	if len(popKeys) == 0 {
		return nil, nil
	}
	got, err := popScript.Run(ctx, s.c, popKeys, counts...).Slice()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	out := make([]model.Message, 0, len(got)/2)
	var parted []model.Message
	for i := 0; i+1 < len(got); i += 2 {
		item, _ := got[i+1].(string)
		m, ok := fromItem(item, vendor)
		if !ok {
			continue // not an item of ours: dropped by the take
		}
		out = append(out, m)
		if m.ConcatTotal > 1 {
			parted = append(parted, m)
		}
	}
	if len(parted) > 0 {
		// A part taken is no longer one of its group's queued parts.
		pipe := s.c.Pipeline()
		for _, m := range parted {
			pipe.SRem(ctx, queues.MTGroup(queues.Scope(m.QueueOwner), vendor,
				groupOf(m.AccountID, m.Sender, m.Destination, m.ConcatRef, m.ConcatTotal)), m.QueueItem)
		}
		_, _ = pipe.Exec(ctx)
	}
	queueflow.Out(queueflow.MT, vendor, int64(len(out))) // monitoring: the queue's rate out (D-127)
	return out, nil
}

func boolArg(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func toInt(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

// Ack is nothing for an outgoing message: a take removed it. [store.Queue]
func (s *Store) Ack(ctx context.Context, vendor string, msgIDs ...string) error { return nil }

// AckWith runs the commands `more` adds in one transaction (the DB worker's
// record of a message that was sent, recordq). The message itself left its
// queue when it was taken.
func (s *Store) AckWith(ctx context.Context, vendor string, msgIDs []string, more func(pipe redis.Pipeliner)) error {
	if more == nil {
		return nil
	}
	pipe := s.c.TxPipeline()
	more(pipe)
	_, err := pipe.Exec(ctx)
	return err
}

// groupTakeScript takes the queued parts of one long message off their
// queues: KEYS[1] the group, ARGV[1] the vendor's MTBase. The answer is the
// items taken; the group is emptied.
var groupTakeScript = redis.NewScript(`
local items = redis.call('SMEMBERS', KEYS[1])
local subs = redis.call('SMEMBERS', ARGV[1] .. ':list')
local out = {}
for _, it in ipairs(items) do
  for _, sub in ipairs(subs) do
    local base = ARGV[1] .. ':' .. sub
    if redis.call('ZREM', base .. ':ready', it) == 1 or redis.call('ZREM', base .. ':delayed', it) == 1 then
      out[#out+1] = it
      break
    end
  end
end
redis.call('DEL', KEYS[1])
return out
`)

// FetchConcatGroup takes the still-queued parts of one long message, in part
// order (D-036): an HTTP vendor that took one part sends them as one call.
// They are gone from the queue, as a take's are.
func (s *Store) FetchConcatGroup(ctx context.Context, vendor string, accountID int64,
	sender, dest string, ref, total int) ([]model.Message, error) {
	g := groupOf(accountID, sender, dest, ref, total)
	var out []model.Message
	for _, sc := range s.scopes() {
		items, err := groupTakeScript.Run(ctx, s.c, []string{queues.MTGroup(sc, vendor, g)}, queues.MTBase(sc, vendor)).StringSlice()
		if err != nil && err != redis.Nil {
			return out, err
		}
		for _, it := range items {
			if m, ok := fromItem(it, vendor); ok {
				out = append(out, m)
			}
		}
	}
	queueflow.Out(queueflow.MT, vendor, int64(len(out)))
	sort.Slice(out, func(i, j int) bool { return out[i].ConcatSeq < out[j].ConcatSeq })
	return out, nil
}

// readyKeys are a vendor's ready sets in this process's scopes, in a fixed
// order, with how many each holds.
func (s *Store) readyKeys(ctx context.Context, vendor string) ([]string, []int64, error) {
	var keys []string
	for _, sc := range s.scopes() {
		subs, err := s.c.SMembers(ctx, queues.MTList(sc, vendor)).Result()
		if err != nil && err != redis.Nil {
			return nil, nil, err
		}
		sort.Strings(subs)
		for _, sub := range subs {
			keys = append(keys, queues.MTReady(sc, vendor, sub))
		}
	}
	if len(keys) == 0 {
		return nil, nil, nil
	}
	pipe := s.c.Pipeline()
	cards := make([]*redis.IntCmd, len(keys))
	for i, k := range keys {
		cards[i] = pipe.ZCard(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, nil, err
	}
	n := make([]int64, len(keys))
	for i := range keys {
		n[i] = cards[i].Val()
	}
	return keys, n, nil
}

// PeekMovable pages through a vendor's ready messages — this deployment's
// queues, then the shared ones — for the re-route mover, without taking them:
// up to n from position `after`, and the position to ask from next. A part
// of a long message is passed over (its parts go together, D-036). Each
// message carries its item (QueueItem) for MoveQueued.
func (s *Store) PeekMovable(ctx context.Context, vendor string, after int64, n int) ([]model.Message, int64, error) {
	keys, cards, err := s.readyKeys(ctx, vendor)
	if err != nil || len(keys) == 0 {
		return nil, after, err
	}
	off := after
	k := 0
	for k < len(keys) && off >= cards[k] {
		off -= cards[k]
		k++
	}
	if k >= len(keys) {
		return nil, after, nil
	}
	items, err := s.c.ZRange(ctx, keys[k], off, off+int64(n)-1).Result()
	if err != nil && err != redis.Nil {
		return nil, after, err
	}
	next := after + int64(len(items))
	var out []model.Message
	for _, it := range items {
		m, ok := fromItem(it, vendor)
		if !ok || m.ConcatTotal > 1 {
			continue
		}
		out = append(out, m)
	}
	return out, next, nil
}

// moveScript moves one item: KEYS[1] the ready set it is in, KEYS[2] the
// ready set it goes to, KEYS[3] the new vendor's list, KEYS[4] the vendors
// set; ARGV the old item, the new item, its priority, its sub-queue, the new
// vendor. 0 = it was not there any more (taken or moved meanwhile).
var moveScript = redis.NewScript(`
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 0 end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
redis.call('SADD', KEYS[3], ARGV[4])
redis.call('SADD', KEYS[4], ARGV[5])
return 1
`)

// MoveQueued moves one waiting message (as PeekMovable read it, QueueItem
// set) from vendor `from` to m.Vendor's common queue, with what the mover
// changed. false = it was taken or moved meanwhile.
func (s *Store) MoveQueued(ctx context.Context, from string, m model.Message) (bool, error) {
	if from == m.Vendor || m.QueueItem == "" {
		return false, nil
	}
	old := m.QueueItem
	sc := queues.Scope(m.QueueOwner)
	fromKey := queues.MTReady(sc, from, subOf(m))
	// A short code is the old vendor's: on the new vendor it waits in the
	// common queue.
	m.ShortCode = ""
	m.NextAttemptAt = time.Time{}
	item, err := itemOf(m)
	if err != nil {
		return false, err
	}
	n, err := moveScript.Run(ctx, s.c,
		[]string{fromKey, queues.MTReady(sc, m.Vendor, queues.Common), queues.MTList(sc, m.Vendor), queues.MTVendors(sc)},
		old, item, queues.Priority(m.Priority), queues.Common, m.Vendor).Int()
	if err != nil || n == 0 {
		return false, err
	}
	queueflow.Out(queueflow.MT, from, 1) // it left one vendor's queue for another's (D-127)
	queueflow.In(queueflow.MT, m.Vendor, 1)
	return true, nil
}

// findScript looks for one message's item in a vendor's queues: ARGV the
// message id, the vendor, then the scopes. The answer: the item, "r" or
// "d" (ready or delayed), its score; nothing when it is not there.
var findScript = redis.NewScript(`
local pat = ARGV[1] .. '|*'
for i = 3, #ARGV do
  local base = 'queues:' .. ARGV[i] .. ':mt:' .. ARGV[2]
  local subs = redis.call('SMEMBERS', base .. ':list')
  for _, sub in ipairs(subs) do
    for _, part in ipairs({'ready', 'delayed'}) do
      local key = base .. ':' .. sub .. ':' .. part
      local cur = '0'
      repeat
        local r = redis.call('ZSCAN', key, cur, 'MATCH', pat, 'COUNT', 1000)
        cur = r[1]
        if #r[2] > 0 then return {r[2][1], string.sub(part, 1, 1), r[2][2]} end
      until cur == '0'
    end
  end
end
return {}
`)

// FindQueued finds one message still waiting in the queues of the vendors
// named, for its account only (store.QueuedFinder). A message being sent at
// this moment has left its queue and is not found.
func (s *Store) FindQueued(ctx context.Context, msgID string, accountID int64, vendors []string) (model.Message, bool, error) {
	if msgID == "" {
		return model.Message{}, false, nil
	}
	seen := map[string]bool{}
	for _, v := range vendors {
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		args := []any{msgID, v}
		for _, sc := range s.scopes() {
			args = append(args, sc)
		}
		r, err := findScript.Run(ctx, s.c, nil, args...).StringSlice()
		if err != nil && err != redis.Nil {
			return model.Message{}, false, err
		}
		if len(r) < 3 {
			continue
		}
		m, ok := fromItem(r[0], v)
		if !ok || m.AccountID != accountID {
			continue // someone else's: not found, by the same road as an unknown id
		}
		m.NextAttemptAt = time.Time{}
		if r[1] == "d" {
			if sc, err := strconv.ParseFloat(r[2], 64); err == nil {
				if at := time.UnixMilli(int64(sc) / 10); at.After(time.Now()) {
					m.NextAttemptAt = at
				}
			}
		}
		return m, true, nil
	}
	return model.Message{}, false, nil
}

// depthScript reads every vendor's queues in the scopes ARGV: for each vendor
// its ready and delayed counts and the oldest ready item's id (the oldest of
// each priority's first).
var depthScript = redis.NewScript(`
local out = {}
for i = 1, #ARGV do
  local vendors = redis.call('SMEMBERS', 'queues:' .. ARGV[i] .. ':mt:vendors')
  for _, v in ipairs(vendors) do
    local base = 'queues:' .. ARGV[i] .. ':mt:' .. v
    local subs = redis.call('SMEMBERS', base .. ':list')
    local ready, delayed, oldest = 0, 0, ''
    for _, sub in ipairs(subs) do
      local r = redis.call('ZCARD', base .. ':' .. sub .. ':ready')
      ready = ready + r
      delayed = delayed + redis.call('ZCARD', base .. ':' .. sub .. ':delayed')
      if r > 0 then
        for p = 1, 9 do
          local h = redis.call('ZRANGEBYSCORE', base .. ':' .. sub .. ':ready', p, p, 'LIMIT', 0, 1)
          if #h > 0 then
            local id = string.sub(h[1], 1, 26)
            if oldest == '' or id < oldest then oldest = id end
          end
        end
      end
    end
    out[#out+1] = v
    out[#out+1] = ready
    out[#out+1] = delayed
    out[#out+1] = oldest
  end
end
return out
`)

// queueDepths is every vendor's queues in this process's scopes: Ready may be
// taken now, Held waits for its pick time, OldestAgeSec is how long the
// oldest ready message has waited since it was accepted.
func (s *Store) queueDepths(ctx context.Context) (map[string]store.VendorQueueStat, error) {
	args := []any{}
	for _, sc := range s.scopes() {
		args = append(args, sc)
	}
	raw, err := depthScript.Run(ctx, s.c, nil, args...).Slice()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	now := time.Now()
	out := map[string]store.VendorQueueStat{}
	for i := 0; i+3 < len(raw); i += 4 {
		v, _ := raw[i].(string)
		st := out[v]
		st.Ready += toInt(raw[i+1])
		st.Held += toInt(raw[i+2])
		st.Depth = st.Ready + st.Held
		if id, _ := raw[i+3].(string); id != "" {
			if u, err := ulid.ParseStrict(id); err == nil {
				if age := int64(now.Sub(ulid.Time(u.Time())) / time.Second); age > st.OldestAgeSec {
					st.OldestAgeSec = age
				}
			}
		}
		out[v] = st
	}
	return out, nil
}

// QueuedForVendor is how many messages wait for a vendor in EVERY scope —
// every deployment's and the shared ones (store.VendorQueueCounter): a
// vendor is not deleted while any wait.
func (s *Store) QueuedForVendor(ctx context.Context, vendor string) (int64, error) {
	var total int64
	iter := s.c.Scan(ctx, 0, "queues:*:mt:"+vendor+":list", 100).Iterator()
	for iter.Next(ctx) {
		list := iter.Val()
		base := list[:len(list)-len(":list")]
		subs, err := s.c.SMembers(ctx, list).Result()
		if err != nil {
			return 0, err
		}
		for _, sub := range subs {
			r, err := s.c.ZCard(ctx, base+":"+sub+":ready").Result()
			if err != nil {
				return 0, err
			}
			d, err := s.c.ZCard(ctx, base+":"+sub+":delayed").Result()
			if err != nil {
				return 0, err
			}
			total += r + d
		}
	}
	return total, iter.Err()
}
