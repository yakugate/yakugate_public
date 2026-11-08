package redis

// The keys an outgoing message had before 27 Oct 2026 (issue 22 of the
// design audit): read only to hand what is left in them over to the queues of
// today (UpgradeOld at a role's start, HandOverTo on a switch of store).
//
//	qvendors                 SET   every queue name that ever enqueued
//	q:<queue name>           ZSET  msg_id scored by visible-at (+ priority fraction)
//	qmsg:<queue name>:<id>   STR   the message (JSON)
//	qatt:<queue name>        HASH  msg_id -> times claimed
//	qgrp:<queue name>:<g>    SET   msg ids of one long message's parts
//	qgrpof:<queue name>      HASH  msg_id -> its group key
//
// A queue name is the vendor cid (shared), or "<cid>@<deployment>".

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/model"
)

const oldVendorsKey = "qvendors"

func qKey(qn string) string           { return "q:" + qn }
func payloadKey(qn, id string) string { return "qmsg:" + qn + ":" + id }
func attKey(qn string) string         { return "qatt:" + qn }
func grpOfKey(qn string) string       { return "qgrpof:" + qn }

// oldMT empties the old outgoing queues into sink: each message with the
// times it was claimed as its attempt and, when it was leased or scheduled,
// that time as its pick time; its keys are removed once sink has it.
func (s *Store) oldMT(ctx context.Context, sink interface {
	HandInQueued(ctx context.Context, m model.Message) (bool, error)
}) (int, error) {
	names, err := s.c.SMembers(ctx, oldVendorsKey).Result()
	if err != nil {
		return 0, err
	}
	moved := 0
	for _, qn := range names {
		vendor, owner := splitQueueName(qn)
		for {
			ids, err := s.c.ZRange(ctx, qKey(qn), 0, handOverPage-1).Result()
			if err != nil {
				return moved, err
			}
			if len(ids) == 0 {
				break
			}
			for _, id := range ids {
				b, err := s.c.Get(ctx, payloadKey(qn, id)).Bytes()
				if err != nil && err != redis.Nil {
					return moved, err
				}
				if err == nil {
					var m model.Message
					if json.Unmarshal(b, &m) == nil {
						m.Vendor, m.NextAttemptAt = vendor, time.Time{}
						if m.QueueOwner == "" {
							m.QueueOwner = owner
						}
						if a, err := s.c.HGet(ctx, attKey(qn), id).Int(); err == nil {
							m.Attempts = a
						}
						if sc, err := s.c.ZScore(ctx, qKey(qn), id).Result(); err == nil {
							if at := time.Unix(int64(sc), 0); at.After(time.Now()) {
								m.NextAttemptAt = at
							}
						}
						if _, err := sink.HandInQueued(ctx, m); err != nil {
							return moved, err
						}
						moved++
					}
				}
				pipe := s.c.TxPipeline()
				pipe.Del(ctx, payloadKey(qn, id))
				pipe.ZRem(ctx, qKey(qn), id)
				pipe.HDel(ctx, attKey(qn), id)
				if gk, err := s.c.HGet(ctx, grpOfKey(qn), id).Result(); err == nil && gk != "" {
					pipe.SRem(ctx, gk, id)
				}
				if _, err := pipe.Exec(ctx); err != nil {
					return moved, err
				}
			}
		}
		s.c.Del(ctx, qKey(qn), attKey(qn), grpOfKey(qn))
		s.c.SRem(ctx, oldVendorsKey, qn)
	}
	return moved, nil
}
