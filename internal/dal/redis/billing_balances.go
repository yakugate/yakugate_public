package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
)

// The live balance counters (dal.LiveBalances), moved here from
// internal/billing (redisreserver.go, booker.go) on 29 Oct 2026, unchanged:
// the same keys, the same scripts, the same pipelines.
//
// Values are stored as INTEGER MICRO-UNITS (balance × 10000, matching the
// DECIMAL(14,4) columns and the codebase's money-as-integer convention) so
// DECRBY/INCRBY stay exact — no float drift ever accumulates in the counter.

// LiveBalances is the live balance counters on one Redis (the main one).
type LiveBalances struct {
	rc *redis.Client
}

// NewLiveBalances keeps the live balances on rc.
func NewLiveBalances(rc *redis.Client) *LiveBalances {
	if rc == nil {
		return nil
	}
	return &LiveBalances{rc: rc}
}

// Client is the Redis the counters are in.
func (l *LiveBalances) Client() *redis.Client { return l.rc }

var _ dal.LiveBalances = (*LiveBalances)(nil)

// BalanceIndexKey is a Redis set of the live balance keys, so the write-back
// reads the few keys there are and not the whole key space.
//
// The write-back used to SCAN for bal:* every 30 seconds in every accept
// process: on the dev Redis of four million keys one pass was about 40,000
// round trips and 14 seconds (2026-10-13). A key is added to the set where
// it is SEEDED from MySQL (seedPrepaid, seed, Booker.seedOnTop,
// creditScript) and nowhere else: since 29 Oct 2026 the set also says which
// counters are seeded (Booker.move), so nothing adds a key it merely found.
const BalanceIndexKey = "balidx"

// BalanceKey is the live counter of a company (or, companyID 0, of an
// unlinked account).
func BalanceKey(accountID, companyID int64) string {
	if companyID != 0 {
		return fmt.Sprintf("bal:co:%d", companyID)
	}
	return fmt.Sprintf("bal:acct:%d", accountID)
}

// counterOf reads a counter's key back into its owner (0, 0 = neither).
func counterOf(key string) dal.Counter {
	c := dal.Counter{Key: key}
	switch {
	case strings.HasPrefix(key, "bal:co:"):
		if id, err := strconv.ParseInt(strings.TrimPrefix(key, "bal:co:"), 10, 64); err == nil {
			c.Company = id
		}
	case strings.HasPrefix(key, "bal:acct:"):
		if id, err := strconv.ParseInt(strings.TrimPrefix(key, "bal:acct:"), 10, 64); err == nil {
			c.Account = id
		}
	}
	return c
}

// reserveScript: -2 = key missing (caller must seed), 0 = insufficient,
// 1 = reserved.
var reserveScript = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if not v then return -2 end
if tonumber(v) < tonumber(ARGV[1]) then return 0 end
redis.call('DECRBY', KEYS[1], ARGV[1])
return 1
`)

// reserveLeftScript is reserveScript that also returns what the hold left:
// {1, left} reserved, {0} insufficient, {-2} key missing (seed first).
var reserveLeftScript = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if not v then return {-2} end
if tonumber(v) < tonumber(ARGV[1]) then return {0} end
return {1, redis.call('DECRBY', KEYS[1], ARGV[1])}
`)

// refundScript adds ARGV[1] back to a counter that exists (1), and leaves a
// missing one alone (0) — the caller seeds it first.
var refundScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('INCRBY', KEYS[1], ARGV[1])
  return 1
end
return 0
`)

// seedOnTopScript adds the MySQL balance (ARGV[1]) to a counter that is not
// yet in the balance index, and adds it there — once, whoever asks.
// Answers the counter after.
var seedOnTopScript = redis.NewScript(`
if redis.call('SISMEMBER', KEYS[2], KEYS[1]) == 1 then
  return tonumber(redis.call('GET', KEYS[1]) or '0')
end
redis.call('SADD', KEYS[2], KEYS[1])
return redis.call('INCRBY', KEYS[1], ARGV[1])
`)

// creditScript applies a manual balance change to the live counter (PO
// 2026-09-18). KEYS[1] = the company key; ARGV[1] = the change in micro-units;
// ARGV[2] = the new total in micro-units, as MySQL will hold it.
//
// It exists because a MySQL-only change is LOST under this reserver: the
// write-back loop copies the live counter over companies.balance every 30 s,
// so a top-up written to MySQL alone is overwritten by the older live figure,
// while its balance_history row still says the money arrived.
//
// Both branches are needed, and neither may be a plain INCRBY or SET:
//   - counter present: it is the live truth and MySQL lags it, so ADD the
//     change — setting the MySQL total would undo every reservation made since
//     the last write-back;
//   - counter absent: MySQL is the truth, so SET the new total. Leaving it
//     absent is not safe: a reservation could seed from the old committed
//     MySQL figure before our transaction commits, and the write-back would
//     then erase the top-up; adding to a counter seeded after our commit would
//     count it twice.
//
// The caller runs this while holding the company row lock, before its commit.
var creditScript = redis.NewScript(`
redis.call('SADD', KEYS[2], KEYS[1])
if redis.call('EXISTS', KEYS[1]) == 1 then
  return redis.call('INCRBY', KEYS[1], ARGV[1])
end
redis.call('SET', KEYS[1], ARGV[2])
return tonumber(ARGV[2])
`)

// CreditLive applies a manual change of deltaMicro (signed) to a company's live
// balance counter, or creates it at newTotalMicro. See creditScript.
func CreditLive(ctx context.Context, rc *redis.Client, companyID, deltaMicro, newTotalMicro int64) error {
	if companyID == 0 {
		return fmt.Errorf("a live balance change needs a company")
	}
	return creditScript.Run(ctx, rc, []string{BalanceKey(0, companyID), BalanceIndexKey}, deltaMicro, newTotalMicro).Err()
}

// UndoLive reverses a CreditLive whose MySQL transaction then failed to
// commit, so the live counter does not hold money the ledger never recorded.
func UndoLive(ctx context.Context, rc *redis.Client, companyID, deltaMicro int64) error {
	return rc.DecrBy(ctx, BalanceKey(0, companyID), deltaMicro).Err()
}

// Deduct is the one call on the main Redis: DECRBY of the company's live
// balance by d (negative = given back). It answers the balance after.
func (l *LiveBalances) Deduct(ctx context.Context, company, d int64) (int64, error) {
	return l.rc.DecrBy(ctx, BalanceKey(0, company), d).Result()
}

// DeductMany is every DECRBY in one pipeline. Each answer is read from its
// own command (the pipeline's own error is the first of them).
func (l *LiveBalances) DeductMany(ctx context.Context, ds []dal.Deduction) []dal.Deducted {
	out := make([]dal.Deducted, len(ds))
	if len(ds) == 0 {
		return out
	}
	pipe := l.rc.Pipeline()
	cmds := make([]*redis.IntCmd, len(ds))
	for i, d := range ds {
		cmds[i] = pipe.DecrBy(ctx, BalanceKey(0, d.Company), d.Units)
	}
	_, _ = pipe.Exec(ctx)
	for i, c := range cmds {
		out[i].Left, out[i].Err = c.Result()
	}
	return out
}

// IsSeeded says the counter is on the balance index.
func (l *LiveBalances) IsSeeded(ctx context.Context, company int64) (bool, error) {
	return l.rc.SIsMember(ctx, BalanceIndexKey, BalanceKey(0, company)).Result()
}

// SeedOnTop runs seedOnTopScript.
func (l *LiveBalances) SeedOnTop(ctx context.Context, company, units int64) (int64, error) {
	return seedOnTopScript.Run(ctx, l.rc, []string{BalanceKey(0, company), BalanceIndexKey}, units).Int64()
}

// SeedMissing is one pipeline: SET NX of each counter, and every name on the
// balance index.
func (l *LiveBalances) SeedMissing(ctx context.Context, bs []dal.CounterSeed) (int, error) {
	if len(bs) == 0 {
		return 0, nil
	}
	pipe := l.rc.Pipeline()
	sets := make([]*redis.BoolCmd, len(bs))
	members := make([]any, len(bs))
	for i, c := range bs {
		key := BalanceKey(0, c.Company)
		sets[i] = pipe.SetNX(ctx, key, c.Units, 0)
		members[i] = key
	}
	pipe.SAdd(ctx, BalanceIndexKey, members...)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	made := 0
	for _, s := range sets {
		if s.Val() {
			made++
		}
	}
	return made, nil
}

// Seed copies a balance into Redis once (SET NX). Safe to call when the key
// already exists — NX makes it a no-op, so a concurrent seeder can never
// clobber decrements that happened since the other seeder read MySQL.
func (l *LiveBalances) Seed(ctx context.Context, accountID, companyID, units int64) error {
	key := BalanceKey(accountID, companyID)
	if err := l.rc.SetNX(ctx, key, units, 0).Err(); err != nil {
		return err
	}
	_ = l.rc.SAdd(ctx, BalanceIndexKey, key).Err() // best effort: the full round finds it too
	return nil
}

// Hold runs reserveScript.
func (l *LiveBalances) Hold(ctx context.Context, accountID, companyID, units int64) (int, error) {
	return reserveScript.Run(ctx, l.rc, []string{BalanceKey(accountID, companyID)}, units).Int()
}

// HoldLeft runs reserveLeftScript.
func (l *LiveBalances) HoldLeft(ctx context.Context, accountID, companyID, units int64) ([]int64, error) {
	return reserveLeftScript.Run(ctx, l.rc, []string{BalanceKey(accountID, companyID)}, units).Int64Slice()
}

// AddIfThere runs refundScript.
func (l *LiveBalances) AddIfThere(ctx context.Context, accountID, companyID, units int64) (bool, error) {
	done, err := refundScript.Run(ctx, l.rc, []string{BalanceKey(accountID, companyID)}, units).Int()
	if err != nil {
		return false, err
	}
	return done == 1, nil
}

// Add is INCRBY.
func (l *LiveBalances) Add(ctx context.Context, accountID, companyID, units int64) error {
	return l.rc.IncrBy(ctx, BalanceKey(accountID, companyID), units).Err()
}

// Current reads a counter.
func (l *LiveBalances) Current(ctx context.Context, accountID, companyID int64) (int64, bool, error) {
	micro, err := l.rc.Get(ctx, BalanceKey(accountID, companyID)).Int64()
	if err == nil {
		return micro, true, nil
	}
	if err == redis.Nil {
		return 0, false, nil
	}
	return 0, false, err
}

// Drop deletes the live counter and takes it off the balance index, in one
// transaction.
func (l *LiveBalances) Drop(ctx context.Context, accountID, companyID int64) error {
	key := BalanceKey(accountID, companyID)
	pipe := l.rc.TxPipeline()
	pipe.Del(ctx, key)
	pipe.SRem(ctx, BalanceIndexKey, key)
	_, err := pipe.Exec(ctx)
	return err
}

// DropAll drops every live balance counter (SCAN, non-blocking).
func (l *LiveBalances) DropAll(ctx context.Context) (int, error) {
	var dropped int
	iter := l.rc.Scan(ctx, 0, "bal:*", 100).Iterator()
	for iter.Next(ctx) {
		pipe := l.rc.TxPipeline()
		pipe.Del(ctx, iter.Val())
		pipe.SRem(ctx, BalanceIndexKey, iter.Val())
		if _, err := pipe.Exec(ctx); err == nil {
			dropped++
		}
	}
	return dropped, iter.Err()
}

// Seeded is the balance index: the live balance keys, and nothing else.
func (l *LiveBalances) Seeded(ctx context.Context) ([]dal.Counter, error) {
	keys, err := l.rc.SMembers(ctx, BalanceIndexKey).Result()
	if err != nil {
		return nil, err
	}
	out := make([]dal.Counter, len(keys))
	for i, k := range keys {
		out[i] = counterOf(k)
	}
	return out, nil
}

// Read is one counter of the index: found=false when the key is gone; a
// value that is not a number is an error.
func (l *LiveBalances) Read(ctx context.Context, c dal.Counter) (int64, bool, error) {
	raw, err := l.rc.Get(ctx, c.Key).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	micro, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return micro, true, nil
}

// Forget takes a counter off the balance index.
func (l *LiveBalances) Forget(ctx context.Context, c dal.Counter) error {
	return l.rc.SRem(ctx, BalanceIndexKey, c.Key).Err()
}

// Credit is CreditLive on this Redis.
func (l *LiveBalances) Credit(ctx context.Context, company, delta, total int64) error {
	return CreditLive(ctx, l.rc, company, delta, total)
}

// Undo is UndoLive on this Redis.
func (l *LiveBalances) Undo(ctx context.Context, company, delta int64) error {
	return UndoLive(ctx, l.rc, company, delta)
}

// Unsent says Redis certainly did not run the call.
func (l *LiveBalances) Unsent(err error) bool { return unsent(err) }

// unsent says Redis certainly did not run a call: it was refused before it
// was sent, or refused whole.
func unsent(err error) bool {
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
	for _, refused := range []string{"EXECABORT", "LOADING ", "READONLY ", "redis: connection pool timeout", "MASTERDOWN"} {
		if strings.Contains(msg, refused) {
			return true
		}
	}
	return false
}
