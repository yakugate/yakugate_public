// Package redis is the Redis backend of the queues: the outgoing (MT) queues
// of every vendor (mt.go, issue 22 of the design audit with issue 25's names:
// queues:<dep>:mt:<cid>:<sub>:ready|delayed), the client receipts and MO
// waiting for their clients (moredis.go), and a minimal record store used only
// when the WHOLE queue store runs on Redis.
//
// Before 27 Oct 2026 an outgoing message had keys of its own (qmsg:, qatt:,
// qgrp:, qgrpof:, a lease in q:<vendor>, the list qvendors); oldkeys.go reads
// them only to hand what is left in them over to the new queues at start.
package redis

import (
	"context"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// Store is the Redis queue backend.
type Store struct {
	c *redis.Client
	// owner is the deployment whose queues this process serves
	// (deployment.Owner, D-121); "" = the shared queues only, as before.
	owner string

	// The most times a message may be sent (attemptcap): a take hands a
	// message that has had its tries to its vendor role, which writes it off
	// (MaxAttempts).
	capMu     sync.RWMutex
	capGlobal int            // 0 = no cap, as before
	capVendor map[string]int // by vendor cid; wins over capGlobal
}

// SetAttemptCap sets the most times a message may be sent: global for every
// queue, perVendor (by vendor cid) where a vendor has its own. 0 = no cap.
// [attemptcap.Setter]
func (s *Store) SetAttemptCap(global int, perVendor map[string]int) {
	s.capMu.Lock()
	s.capGlobal, s.capVendor = global, perVendor
	s.capMu.Unlock()
}

// MaxAttempts is the most times a vendor's message may be sent (0 = no cap).
func (s *Store) MaxAttempts(vendor string) int { return s.attemptCap(vendor) }

func (s *Store) attemptCap(vendor string) int {
	s.capMu.RLock()
	defer s.capMu.RUnlock()
	if n, ok := s.capVendor[vendor]; ok && n > 0 {
		return n
	}
	return s.capGlobal
}

// SetQueueOwner points the store at another deployment's queues — tests only;
// a role always serves deployment.Owner().
func (s *Store) SetQueueOwner(owner string) { s.owner = owner }

// queueName is where one owner's messages for a vendor are kept: the vendor
// cid for the shared queue, "<cid>@<owner>" for a deployment's own. Every
// per-queue key is built from it, so an owned queue is a separate set of keys.
func queueName(vendor, owner string) string {
	if owner == "" {
		return vendor
	}
	return vendor + "@" + owner
}

// splitQueueName is the vendor and owner of a queue name.
func splitQueueName(qn string) (vendor, owner string) {
	if i := strings.LastIndexByte(qn, '@'); i >= 0 {
		return qn[:i], qn[i+1:]
	}
	return qn, ""
}

// myQueues are the queue names this process drains for a vendor: its own
// first, then the shared one (messages queued before separate queues, or by
// a gateway that names no deployment). Only the shared one when this process
// names no deployment.
func (s *Store) myQueues(vendor string) []string {
	if s.owner == "" {
		return []string{vendor}
	}
	return []string{queueName(vendor, s.owner), vendor}
}

// mine reports whether this process drains a queue name.
func (s *Store) mine(qn string) bool {
	_, o := splitQueueName(qn)
	return o == "" || o == s.owner
}

// New builds a Redis client from a redis:// URL.
func New(url string) (*Store, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return &Store{c: redis.NewClient(opt), owner: deployment.Owner()}, nil
}

// NewAddr builds a Redis client from host:port + password + db — the shape the
// YAKU_REDIS_ADDR hot-path gate provides. [2026-07-23]
//
// An optional FAILOVER address (PO 2026-07-26: "redis primary and redis
// failover only") makes every NEW connection try the primary first and fall
// back to the standby when the primary is unreachable. Because go-redis
// re-dials on connection failure and pooled connections are per-attempt, this
// gives automatic failover AND automatic fail-back (recovered primary wins
// the next dial) with no client-side state machine. The failover instance
// must share the primary's password/db and, for data continuity, should
// replicate the primary (and be promotable/writable) — a cold empty standby
// only preserves availability, not in-flight queue data.
func NewAddr(addr, password string, db int, failover ...string) *Store {
	return &Store{c: redis.NewClient(addrOptions(addr, password, db, failover...)), owner: deployment.Owner()}
}

// addrOptions are NewAddr's client options.
func addrOptions(addr, password string, db int, failover ...string) *redis.Options {
	opts := &redis.Options{Addr: addr, Password: password, DB: db}
	if len(failover) > 0 && failover[0] != "" {
		standby := failover[0]
		opts.Dialer = func(ctx context.Context, network, _ string) (net.Conn, error) {
			d := net.Dialer{Timeout: 2 * time.Second}
			conn, err := d.DialContext(ctx, network, addr)
			if err == nil {
				return conn, nil
			}
			return d.DialContext(ctx, network, standby)
		}
	}
	return opts
}

// FromConfig is the one way the engine reaches its Redis: Sentinel when the
// settings name Sentinels (2026-10-02, Saleem), else the one address with its
// optional standby (NewAddr).
//
// With Sentinel, go-redis asks the Sentinels which server is the main one,
// connects only to it, and on a failover (Sentinel's +switch-master) drops
// its connections and follows the new main server — so a demoted server is
// never written to, which the standby dial of NewAddr cannot promise (it
// prefers the first address whenever it answers, even as a replica). Writes
// made while the Sentinels are still choosing fail like any Redis outage and
// are handled where they are made.
func FromConfig(cfg config.Config) *Store { return fromConfig(cfg, 0) }

// fromConfig is FromConfig whose calls, when wait > 0, wait up to wait for a
// free connection and as long again for each answer, in place of go-redis's
// 4 s and 3 s.
func fromConfig(cfg config.Config, wait time.Duration) *Store {
	if !cfg.RedisSentinel() {
		opts := addrOptions(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.RedisFailoverAddr)
		if cfg.RedisPoolSize > 0 || cfg.RedisPoolTimeoutMS > 0 {
			opts.PoolSize, opts.PoolTimeout = PoolSize(cfg), PoolTimeout(cfg)
		}
		if wait > 0 {
			opts.PoolTimeout, opts.ReadTimeout, opts.WriteTimeout = wait, wait, wait
		}
		return &Store{c: redis.NewClient(opts), owner: deployment.Owner()}
	}
	o := &redis.FailoverOptions{
		MasterName:       cfg.RedisMasterName,
		SentinelAddrs:    cfg.RedisSentinelAddrs,
		SentinelPassword: cfg.RedisSentinelPassword,
		Password:         cfg.RedisPassword,
		DB:               cfg.RedisDB,
	}
	if cfg.RedisPoolSize > 0 || cfg.RedisPoolTimeoutMS > 0 {
		o.PoolSize, o.PoolTimeout = PoolSize(cfg), PoolTimeout(cfg)
	}
	if wait > 0 {
		o.PoolTimeout, o.ReadTimeout, o.WriteTimeout = wait, wait, wait
	}
	return &Store{owner: deployment.Owner(), c: redis.NewFailoverClient(o)}
}

// PoolSize is how many connections one client of FromConfig keeps:
// YAKU_REDIS_POOL_SIZE, else go-redis's own 10 a CPU.
func PoolSize(cfg config.Config) int {
	if cfg.RedisPoolSize > 0 {
		return cfg.RedisPoolSize
	}
	return 10 * runtime.GOMAXPROCS(0)
}

// PoolTimeout is how long a call waits for a free connection before it fails:
// YAKU_REDIS_POOL_TIMEOUT_MS, else go-redis's own (the 3 s read timeout + 1 s).
func PoolTimeout(cfg config.Config) time.Duration {
	if cfg.RedisPoolTimeoutMS > 0 {
		return time.Duration(cfg.RedisPoolTimeoutMS) * time.Millisecond
	}
	return 4 * time.Second
}

// BalanceTimeout is how long a call on the live balances' client waits for
// Redis: YAKU_REDIS_BALANCE_TIMEOUT_MS, else 30 s.
func BalanceTimeout(cfg config.Config) time.Duration {
	if cfg.RedisBalanceTimeoutMS > 0 {
		return time.Duration(cfg.RedisBalanceTimeoutMS) * time.Millisecond
	}
	return 30 * time.Second
}

// Client exposes the underlying connection for sibling hot-path features that
// share the gated Redis instance (the prepaid balance reserver) — one client,
// one pool, instead of every feature dialing its own.
func (s *Store) Client() *redis.Client { return s.c }

func (s *Store) Close() error { return s.c.Close() }

// Ping verifies the connection — called once at wiring time so a bad
// YAKU_REDIS_ADDR fails loudly at startup instead of 503ing every request.
func (s *Store) Ping(ctx context.Context) error { return s.c.Ping(ctx).Err() }

func recordKey(id string) string { return "record:" + id }

// claimChunk is how many payloads one read brings, and how many group
// lookups one read of Ack does.
const claimChunk = 500

// Write stores a record as a hash (whole-store-on-Redis mode only).
func (s *Store) Write(ctx context.Context, m model.Message) error {
	if m.Status == "" {
		m.Status = model.StatusQueued
	}
	return s.c.HSet(ctx, recordKey(m.ID),
		"msg_id", m.ID, "account_id", m.AccountID, "username", m.Username,
		"sender", m.Sender, "destination", m.Destination, "mcc", m.MCC, "mnc", m.MNC,
		"transport", string(m.Transport), "segments", m.Segments, "status", string(m.Status),
		"submit_date", time.Now().UTC().Format(time.RFC3339),
	).Err()
}

// UpdateStatus updates a record's final status (whole-store-on-Redis mode only).
func (s *Store) UpdateStatus(ctx context.Context, msgID string, st model.Status, vendorMsgID, errorCode string) error {
	return s.c.HSet(ctx, recordKey(msgID),
		"status", string(st), "vendor_msg_id", vendorMsgID, "error_code", errorCode,
		"delivery_date", time.Now().UTC().Format(time.RFC3339),
	).Err()
}

// Archive handled by the retention process; no-op here.
func (s *Store) Archive(ctx context.Context, ms []model.Message) error { return nil }
