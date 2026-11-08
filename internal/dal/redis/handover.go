package redis

// Handing the queues over to MySQL (Saleem 2026-10-02, D-122).
//
// YAKU_QUEUE_BACKEND decides where the queues run. An install that ran them
// in Redis and now says `mysql` — every install that had Redis set before this
// rule, the live one included — still has items waiting in Redis when it
// restarts. Outgoing messages have their MySQL copy already, but client
// receipts, incoming messages (MO) and SMPP part ids exist ONLY in Redis: left
// there, they would never be delivered. HandOverTo moves every one of them to
// the MySQL queue store and clears the Redis queue keys, so a later switch
// back to Redis cannot send an old message twice.

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queues"
)

// QueueSink is where queued items are handed over to: the MySQL queue store,
// or another Redis (the queues' own, YAKU_QUEUE_REDIS_*).
type QueueSink interface {
	// HandInQueued adds an outgoing message unless the sink already holds it;
	// false = it was already there.
	HandInQueued(ctx context.Context, m model.Message) (bool, error)
	EnqueueClientDLR(ctx context.Context, d model.ClientDLR) error
	StageMO(ctx context.Context, m model.Message) error
	SaveClientPartIDs(ctx context.Context, msgID string, partIDs []string) error
}

// HandOverCounts says what a hand-over moved.
type HandOverCounts struct {
	MT       int // outgoing messages the sink did not already hold (the rest were cleared)
	Receipts int
	MO       int
	PartIDs  int
}

// Any reports whether anything was moved.
func (c HandOverCounts) Any() bool { return c.MT+c.Receipts+c.MO+c.PartIDs > 0 }

const handOverPage = 500

// HandOverTo moves every queued item in Redis — every deployment's and the
// shared ones, owners kept — into sink, deleting each from Redis once sink
// has it. An error stops the hand-over where it is; what was not moved stays
// in Redis for the next start.
func (s *Store) HandOverTo(ctx context.Context, sink QueueSink) (HandOverCounts, error) {
	var c HandOverCounts
	var err error
	if c.MT, err = s.handOverMT(ctx, sink); err != nil {
		return c, err
	}
	if c.Receipts, err = s.handOverAccounts(ctx, "cdlr:accts", nil, func(member string, b []byte) error {
		var p cdlrPayload
		if json.Unmarshal(b, &p) != nil {
			return nil // unreadable: dropped with its entry
		}
		return sink.EnqueueClientDLR(ctx, model.ClientDLR{AccountID: p.AccountID, ClientMsgID: p.ClientMsgID,
			Status: p.Status, Stat: p.Stat, Receipt: p.Receipt, BindSlot: p.BindSlot, Src: p.Src, Dst: p.Dst,
			Owner: p.Owner, DoneAt: p.doneAt(), MsgID: p.Msg})
	}, func(acct int64, owner string) (string, string) {
		return cdlrAcctKey(acct, owner), cdlrAgeKey(acct, owner)
	},
		func(member string) string { id, _ := strconv.ParseInt(member, 10, 64); return cdlrJobKey(id) }); err != nil {
		return c, err
	}
	if c.MO, err = s.handOverAccounts(ctx, "mo:accts", nil, func(member string, b []byte) error {
		var p moPayload
		if json.Unmarshal(b, &p) != nil {
			return nil
		}
		return sink.StageMO(ctx, p.M)
	}, func(acct int64, owner string) (string, string) { return moAcctKey(acct, owner), moAgeKey(acct, owner) },
		moJobKey); err != nil {
		return c, err
	}
	c.PartIDs, err = s.handOverPartIDs(ctx, sink)
	return c, err
}

// handOverMT empties the outgoing queues into sink: what is left under the
// keys used before 27 Oct 2026 (oldMT), then every scope's queues of today.
// Each message is handed to sink before it is removed here.
func (s *Store) handOverMT(ctx context.Context, sink QueueSink) (int, error) {
	moved, err := s.oldMT(ctx, sink)
	if err != nil {
		return moved, err
	}
	var lists []string
	iter := s.c.Scan(ctx, 0, "queues:*:mt:*:list", 1000).Iterator()
	for iter.Next(ctx) {
		lists = append(lists, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return moved, err
	}
	for _, list := range lists {
		n, err := s.handOverVendor(ctx, list, sink)
		moved += n
		if err != nil {
			return moved, err
		}
	}
	return moved, nil
}

// handOverVendor empties one vendor's queues in one scope (its list key) into
// sink: ready items as they are, delayed ones with their pick time.
func (s *Store) handOverVendor(ctx context.Context, list string, sink QueueSink) (int, error) {
	base := strings.TrimSuffix(list, ":list")
	vendor := base[strings.LastIndexByte(base, ':')+1:]
	subs, err := s.c.SMembers(ctx, list).Result()
	if err != nil {
		return 0, err
	}
	moved := 0
	for _, sub := range subs {
		for _, part := range []string{"ready", "delayed"} {
			key := base + ":" + sub + ":" + part
			for {
				got, err := s.c.ZRangeWithScores(ctx, key, 0, handOverPage-1).Result()
				if err != nil {
					return moved, err
				}
				if len(got) == 0 {
					break
				}
				for _, z := range got {
					item, _ := z.Member.(string)
					if m, ok := fromItem(item, vendor); ok {
						m.NextAttemptAt = time.Time{}
						if part == "delayed" {
							m.NextAttemptAt = time.UnixMilli(int64(z.Score) / 10)
						}
						added, err := sink.HandInQueued(ctx, m)
						if err != nil {
							return moved, err
						}
						if added {
							moved++
						}
					}
					if err := s.c.ZRem(ctx, key, item).Err(); err != nil {
						return moved, err
					}
				}
			}
		}
	}
	s.c.Del(ctx, list)
	return moved, nil
}

// UpgradeOld moves what waits under the keys used before 27 Oct 2026 into
// today's queues of this Redis (issue 22): run at a role's start, once, under
// the hand-over lock. A message a sender held then (its lease) waits until
// the lease would have ended.
func (s *Store) UpgradeOld(ctx context.Context) (int, error) { return s.oldMT(ctx, selfSink{s}) }

// selfSink hands old items to the same Redis's queues of today.
type selfSink struct{ *Store }

func (s selfSink) HandInQueued(ctx context.Context, m model.Message) (bool, error) {
	return true, s.Store.Enqueue(ctx, m)
}

// handOverAccounts moves the per-account jobs of one kind (receipts or MO):
// every account set ("<setPrefix>" and "<setPrefix>@<deployment>") whose
// owner `which` picks (nil = all), every job of every account. An entry whose
// job is gone is just dropped.
func (s *Store) handOverAccounts(ctx context.Context, setPrefix string, which func(owner string) bool,
	move func(member string, payload []byte) error,
	keys func(acct int64, owner string) (zset, age string), jobKey func(member string) string) (int, error) {
	var sets []string
	iter := s.c.Scan(ctx, 0, setPrefix+"*", 1000).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		if k != setPrefix && !strings.HasPrefix(k, setPrefix+"@") {
			continue
		}
		if which == nil || which(strings.TrimPrefix(strings.TrimPrefix(k, setPrefix), "@")) {
			sets = append(sets, k)
		}
	}
	if err := iter.Err(); err != nil {
		return 0, err
	}
	moved := 0
	for _, set := range sets {
		owner := strings.TrimPrefix(strings.TrimPrefix(set, setPrefix), "@")
		accts, err := s.c.SMembers(ctx, set).Result()
		if err != nil {
			return moved, err
		}
		for _, a := range accts {
			acct, err := strconv.ParseInt(a, 10, 64)
			if err != nil {
				s.c.SRem(ctx, set, a)
				continue
			}
			zset, age := keys(acct, owner)
			for {
				members, err := s.c.ZRange(ctx, zset, 0, handOverPage-1).Result()
				if err != nil {
					return moved, err
				}
				if len(members) == 0 {
					break
				}
				jobKeys := make([]string, len(members))
				for i, m := range members {
					jobKeys[i] = jobKey(m)
				}
				vals, err := s.c.MGet(ctx, jobKeys...).Result()
				if err != nil {
					return moved, err
				}
				for i, m := range members {
					if v, ok := vals[i].(string); ok {
						if err := move(m, []byte(v)); err != nil {
							return moved, err
						}
						moved++
					}
					pipe := s.c.TxPipeline()
					pipe.Del(ctx, jobKeys[i])
					pipe.ZRem(ctx, zset, m)
					pipe.ZRem(ctx, age, m)
					if _, err := pipe.Exec(ctx); err != nil {
						return moved, err
					}
				}
			}
			s.c.Del(ctx, zset, age)
			s.c.SRem(ctx, set, a)
		}
	}
	return moved, nil
}

// handOverPartIDs moves the part ids saved for long messages sent in parts.
func (s *Store) handOverPartIDs(ctx context.Context, sink QueueSink) (int, error) {
	moved := 0
	iter := s.c.Scan(ctx, 0, "cparts:*", 1000).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		v, err := s.c.Get(ctx, k).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return moved, err
		}
		if err := sink.SaveClientPartIDs(ctx, strings.TrimPrefix(k, "cparts:"), strings.Split(v, ",")); err != nil {
			return moved, err
		}
		s.c.Del(ctx, k)
		moved++
	}
	return moved, iter.Err()
}

// clientDLR is the receipt a job holds, as it was given to EnqueueClientDLR.
// Delay is what is left of its hold-back: At is when it may be claimed.
func (p cdlrPayload) clientDLR() model.ClientDLR {
	d := model.ClientDLR{AccountID: p.AccountID, ClientMsgID: p.ClientMsgID, Status: p.Status, Stat: p.Stat,
		Receipt: p.Receipt, BindSlot: p.BindSlot, Src: p.Src, Dst: p.Dst, Owner: p.Owner, DoneAt: p.doneAt(), MsgID: p.Msg}
	if wait := time.Until(time.Unix(p.At, 0)); wait > 0 {
		d.Delay = wait
	}
	return d
}

// HandOverReceiptsOf moves the client receipts waiting here for the owners
// `which` picks, each to fn, and removes each once fn has it (an error stops
// the walk; the rest stay). For receipts filed in this Redis for a gateway
// that reads another queue Redis, where nobody would ever take them (issue 5
// of the design audit, internal/handoff). It walks the keyspace: once at a
// role's start, never on a timer.
func (s *Store) HandOverReceiptsOf(ctx context.Context, which func(owner string) bool, fn func(d model.ClientDLR) error) (int, error) {
	return s.handOverAccounts(ctx, "cdlr:accts", which, func(member string, b []byte) error {
		var p cdlrPayload
		if json.Unmarshal(b, &p) != nil {
			return nil // unreadable: dropped with its entry
		}
		return fn(p.clientDLR())
	}, func(acct int64, owner string) (string, string) {
		return cdlrAcctKey(acct, owner), cdlrAgeKey(acct, owner)
	},
		func(member string) string { id, _ := strconv.ParseInt(member, 10, 64); return cdlrJobKey(id) })
}

// HandOverMOOf is HandOverReceiptsOf for incoming messages.
func (s *Store) HandOverMOOf(ctx context.Context, which func(owner string) bool, fn func(m model.Message) error) (int, error) {
	return s.handOverAccounts(ctx, "mo:accts", which, func(member string, b []byte) error {
		var p moPayload
		if json.Unmarshal(b, &p) != nil {
			return nil
		}
		return fn(p.M)
	}, func(acct int64, owner string) (string, string) { return moAcctKey(acct, owner), moAgeKey(acct, owner) },
		moJobKey)
}

// HandInQueued adds an outgoing message handed over from MySQL or from
// another Redis to its queue here, unless this Redis holds it already
// (false): a message is never queued twice.
func (s *Store) HandInQueued(ctx context.Context, m model.Message) (bool, error) {
	// One this Redis holds already (queued under its owner) is left alone.
	r, err := findScript.Run(ctx, s.c, nil, m.ID, m.Vendor, queues.Scope(m.QueueOwner)).StringSlice()
	if err != nil && err != redis.Nil {
		return false, err
	}
	if len(r) >= 3 {
		return false, nil
	}
	return true, s.Enqueue(ctx, m)
}

// Pending reports whether Redis holds any queued item at all — the check
// before a hand-over. A SCAN page can come back empty while the cursor goes
// on, so each pattern is followed to its first match or to the end.
func (s *Store) Pending(ctx context.Context) (bool, error) {
	if n, err := s.c.SCard(ctx, oldVendorsKey).Result(); err != nil || n > 0 {
		return n > 0, err
	}
	for _, p := range []string{"queues:*:mt:*:list", "cdlr:accts*", "mo:accts*", "cparts:*"} {
		iter := s.c.Scan(ctx, 0, p, 1000).Iterator()
		if iter.Next(ctx) {
			return true, nil
		}
		if err := iter.Err(); err != nil {
			return false, err
		}
	}
	return false, nil
}
