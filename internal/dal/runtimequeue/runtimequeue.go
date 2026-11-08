// Package runtimequeue builds the Redis queue store for the roles when
// YAKU_QUEUE_BACKEND=redis, and hands queued items over when that setting
// changes.
//
// Redis is the ONLY place the queues live then (Saleem 2026-10-02: "if I set
// redis then no need to store in mysql", amending D-122). Until that day a
// role under the Redis gate also wrote every outgoing message to a MySQL
// "truth" copy and refilled Redis from it at start (P1 scope §18); that copy is
// gone. What keeps queued messages safe is Redis itself: persistence (AOF,
// appendfsync everysec), maxmemory-policy noeviction and, for failover,
// Sentinel with a replica. The MySQL queue database stays, for what is not a
// queue: HLR data and HTTP vendor sessions.
//
// Moved from internal/store/runtimequeue on 30 Oct 2026, unchanged: it works
// on two DALs' stores at once (the MySQL queue database and the queue
// Redis), so it lives in the DAL.
package runtimequeue

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/dal/mysql"
	"github.com/yakugate/yakugate/internal/dal/redis"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// repopTries × repopWait covers a start-up schema upgrade with room to spare.
const (
	repopTries = 10
	repopWait  = 3 * time.Second
)

// Build constructs the Redis queue store for a role: the queues' own Redis
// (YAKU_QUEUE_REDIS_* — one address, a standby or Sentinel) when it is set,
// else the main Redis; never a MySQL copy.
func Build(cfg config.Config, logf func(string, ...any)) *redis.Store {
	return redis.FromConfig(cfg.ForQueueRedis())
}

// HandOver moves queued items to where the settings now say the queues run
// (Saleem 2026-10-02, D-122), once, in the background:
//
//   - `mysql`: everything still waiting in Redis — the queues' own Redis and
//     the main one — moves into the MySQL queue store (outgoing messages,
//     client receipts, MO, SMPP part ids), and the Redis queue keys are
//     cleared (left, a later switch back would send old messages twice).
//   - `redis`: everything waiting in MySQL moves into the queue Redis and
//     leaves MySQL; and when the queues have their own Redis, what is still
//     queued in the MAIN Redis (from before it was set) moves over too. An
//     outgoing message the destination already holds (queued or in flight)
//     is not touched there, only removed from where it was.
//
// One process does it (a lock in the queue Redis); every role that delivers
// or takes in traffic calls it, so a deployment without http-server still
// hands over. Retried for a while: it can run before this start's migrations
// have added the columns it writes.
func HandOver(cfg config.Config, logf func(string, ...any)) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if !cfg.RedisEnabled() && !cfg.QueueRedisSet() {
		// No Redis anywhere: nothing to hand over, but rows queued in MySQL
		// before queue/0044 still need their pick time (issue 22). The
		// update is the same however many processes make it.
		if cfg.QueueStoreMySQL() {
			go func() {
				my, err := mysql.New(cfg.QueueDSN)
				if err != nil {
					return
				}
				defer my.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				defer cancel()
				if n, err := my.MTUpgrade(ctx); err != nil {
					logf("queue upgrade (issue 22): %v — tried again at the next start", err)
				} else if n > 0 {
					logf("queue upgrade (issue 22): %d message(s) queued in MySQL before this build were given their pick time", n)
				}
			}()
		}
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		qrs := redis.FromConfig(cfg.ForQueueRedis()) // the queue Redis, else the main one
		defer qrs.Close()
		lock := "queue:handover:lock"
		got, err := qrs.Client().SetNX(ctx, lock, "1", 30*time.Minute).Result()
		if err != nil || !got {
			return // another process is doing it, or Redis is down (next start tries again)
		}
		defer qrs.Client().Del(context.WithoutCancel(ctx), lock)
		var main *redis.Store // the main Redis, when it is another Redis than the queues'
		if cfg.RedisEnabled() && cfg.QueueRedisSeparate() {
			main = redis.FromConfig(cfg)
			defer main.Close()
		}
		var my *mysql.Store
		if cfg.QueueStoreMySQL() {
			if my, err = mysql.New(cfg.QueueDSN); err != nil {
				logf("queue hand-over: MySQL queue store unavailable: %v", err)
				my = nil
			} else {
				defer my.Close()
			}
		}
		for try := 1; try <= repopTries; try++ {
			if err = handOverOnce(ctx, cfg, qrs, main, my, logf); err == nil {
				return
			}
			if ctx.Err() != nil {
				break
			}
			if try < repopTries {
				logf("queue hand-over: try %d failed (%v) — trying again in %s", try, err, repopWait)
				time.Sleep(repopWait)
			}
		}
		logf("queue hand-over FAILED: %v — what was not moved stays where it is until the next start", err)
	}()
}

// handOverOnce does one pass: qrs is the queue Redis (or the main one when the
// queues have none of their own), main the main Redis when it is a different
// one, my the MySQL queue database when there is one.
func handOverOnce(ctx context.Context, cfg config.Config, qrs, main *redis.Store, my *mysql.Store, logf func(string, ...any)) error {
	// The outgoing queues of before 27 Oct 2026 (issue 22): rows with no pick
	// time get one, and what waits under the old Redis keys moves into
	// today's queues.
	if my != nil {
		n, err := my.MTUpgrade(ctx)
		if err != nil {
			return err
		}
		if n > 0 {
			logf("queue upgrade (issue 22): %d message(s) queued in MySQL before this build were given their pick time", n)
		}
	}
	if cfg.QueuesInRedis() {
		n, err := qrs.UpgradeOld(ctx)
		if err != nil {
			return err
		}
		if n > 0 {
			logf("queue upgrade (issue 22): %d message(s) waiting under the old Redis keys moved into today's queues", n)
		}
	}
	if cfg.QueuesInRedis() {
		if my != nil {
			c, err := my.HandOverTo(ctx, qrs)
			if err != nil {
				return err
			}
			if c.Any() {
				logf("queue hand-over to Redis (YAKU_QUEUE_BACKEND=redis, %s): moved %d outgoing message(s), %d receipt(s), %d MO, %d part-id list(s) from MySQL; MySQL holds no queued item now",
					cfg.ForQueueRedis().RedisWhere(), c.MT, c.Receipts, c.MO, c.PartIDs)
			}
		}
		if main != nil {
			return drain(ctx, main, qrs, "the main Redis ("+cfg.RedisWhere()+")", "the queues' own Redis ("+cfg.ForQueueRedis().RedisWhere()+")", logf)
		}
		return nil
	}
	// `mysql`: every Redis that may hold queued items empties into MySQL.
	if my == nil {
		return nil
	}
	if cfg.QueueRedisSet() {
		if err := drain(ctx, qrs, my, "the queues' own Redis ("+cfg.ForQueueRedis().RedisWhere()+")", "MySQL", logf); err != nil {
			return err
		}
		if main != nil {
			return drain(ctx, main, my, "the main Redis ("+cfg.RedisWhere()+")", "MySQL", logf)
		}
		return nil
	}
	return drain(ctx, qrs, my, "Redis ("+cfg.RedisWhere()+")", "MySQL", logf)
}

// drain moves everything queued in from into to, when from holds anything.
func drain(ctx context.Context, from *redis.Store, to redis.QueueSink, fromName, toName string, logf func(string, ...any)) error {
	pending, err := from.Pending(ctx)
	if err != nil || !pending {
		return err
	}
	logf("queue hand-over: %s still holds queued items — moving them to %s", fromName, toName)
	c, err := from.HandOverTo(ctx, to)
	if err != nil {
		return err
	}
	logf("queue hand-over from %s to %s done: %d outgoing message(s), %d receipt(s), %d MO, %d part-id list(s); its queue keys are cleared",
		fromName, toName, c.MT, c.Receipts, c.MO, c.PartIDs)
	return nil
}
