package billing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
)

// RedisReserver satisfies internal/pipeline.Reserver against a shared Redis
// instance instead of MySQL — the second half of the YAKU_REDIS_ADDR hot-path
// offload (2026-07-23). Same contract as DBReserver, different physics: the
// atomic check-and-decrement is a Lua script on one key, so concurrent
// prepaid accepts against ONE account contend on a sub-millisecond in-memory
// operation instead of an InnoDB row lock held across a network round trip.
// Correct across any number of gateway instances because they all share the
// one Redis.
//
// Lifecycle of a balance (PO decision 2026-07-23, "lazy seed + write-back"):
//   - MySQL (accounts/companies.balance) remains where top-ups land and what
//     reports read.
//   - The FIRST reservation for an account copies the MySQL balance into
//     Redis exactly once (SET NX — a concurrent seeder can never overwrite
//     live decrements).
//   - Redis is then the live number; every reserve/refund moves it.
//   - A write-back loop copies live Redis values back to MySQL every
//     writeBackEvery so dashboards stay near-live.
//   - After a manual top-up in MySQL, POST /admin/balance/refresh (Resync
//     below) deletes the Redis key so the next reservation re-seeds it with
//     the topped-up value. Without that call the top-up becomes visible only
//     to MySQL readers, never to the live counter.
//
// Values are stored as INTEGER MICRO-UNITS (balance × 10000, matching the
// DECIMAL(14,4) columns and the codebase's money-as-integer convention) so
// DECRBY/INCRBY stay exact — no float drift ever accumulates in the counter.
type RedisReserver struct {
	live dal.LiveBalances    // the live counters (the main Redis)
	db   dal.CompanyBalances // config store — seed source and write-back target
	logf func(string, ...any)
	// shown answers Balance for a company with no live key (see BalanceCache).
	shown *BalanceCache
	// wrote is what the write-back last put in MySQL for each key, so a
	// balance that did not move is not written again (writeBack).
	wroteMu sync.Mutex
	wrote   map[string]int64
	// book queues the money events of what this reserver takes (Booker).
	book *Booker
}

const microFactor = 10000

func toMicro(amount float64) int64 { return int64(amount*microFactor + 0.5) }

// NewRedisReserver wires the reserver — balances on the live counters (the
// main Redis), the money events on the deployment's queue q — seeds every
// prepaid company's live balance, and starts its write-back loop, which runs
// until ctx is cancelled (pass the process context; for a server role that is
// effectively process lifetime).
func NewRedisReserver(ctx context.Context, live dal.LiveBalances, q dal.BillingQueue, cfgDB dal.CompanyBalances, logf func(string, ...any)) *RedisReserver {
	r := &RedisReserver{live: live, db: cfgDB, logf: logf, shown: NewBalanceCache(cfgDB), wrote: map[string]int64{},
		book: NewBooker(live, q, cfgDB, deployment.Owner(), LanesFromEnv())}
	go r.seedPrepaid(ctx)
	go r.writeBackLoop(ctx, 30*time.Second)
	return r
}

// seedPrepaid makes every prepaid company's live balance (SET NX from
// companies.balance, and its name on the balance index) when the role
// starts, tried again every 10 s until it has: a deduction is ONE DECRBY
// and must find its counter there (Saleem, 29 Oct 2026). A company made
// prepaid later is seeded by its first deduction (Booker.move) or by the
// Admin API's credit call.
func (r *RedisReserver) seedPrepaid(ctx context.Context) {
	for {
		n, err := r.SeedPrepaid(ctx)
		if err == nil {
			if r.logf != nil && n > 0 {
				r.logf("live balances: %d prepaid compan(ies) seeded", n)
			}
			return
		}
		if r.logf != nil {
			r.logf("live balances: prepaid companies not seeded yet: %v (tried again in 10 s)", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}
}

// SeedPrepaid seeds the live balance of every prepaid company that has none
// (SET NX: a counter that is there is the live truth and is left alone). It
// answers how many it made.
func (r *RedisReserver) SeedPrepaid(ctx context.Context) (int, error) {
	if r.db == nil {
		return 0, fmt.Errorf("no config store")
	}
	cos, err := r.db.Prepaid(ctx)
	if err != nil {
		return 0, err
	}
	if len(cos) == 0 {
		return 0, nil
	}
	seeds := make([]dal.CounterSeed, len(cos))
	for i, c := range cos {
		seeds[i] = dal.CounterSeed{Company: c.Company, Units: toMicro(c.Balance)}
	}
	return r.live.SeedMissing(ctx, seeds)
}

// Reserve atomically deducts amount iff the live balance covers it, lazily
// seeding the key from MySQL on first touch.
func (r *RedisReserver) Reserve(ctx context.Context, accountID, companyID int64, amount float64) (bool, error) {
	micro := toMicro(amount)
	for attempt := 0; attempt < 2; attempt++ {
		res, err := r.live.Hold(ctx, accountID, companyID, micro)
		if err != nil {
			return false, onStore(StoreMainRedis, "balance", err)
		}
		switch res {
		case 1:
			return true, nil
		case 0:
			return false, nil
		case -2:
			if err := r.seed(ctx, accountID, companyID); err != nil {
				return false, err
			}
			// loop: retry the script against the freshly seeded key
		}
	}
	return false, fmt.Errorf("redis reserve: key %s missing after seed", counterName(accountID, companyID))
}

// Refund adds a reservation back. Seeds first if the key is somehow absent —
// a bare INCRBY on a missing key would mint a balance out of nothing.
//
// The counter the hold came from is normally still there, and then nothing
// but Redis is needed (review, 2026-10-01): seeding first used to read MySQL
// on every refund, so a refund made BECAUSE MySQL was down (the hold's ledger
// row or the queue write failed) failed too, and the money stayed held for a
// message that was refused.
func (r *RedisReserver) Refund(ctx context.Context, accountID, companyID int64, amount float64) error {
	micro := toMicro(amount)
	done, err := r.live.AddIfThere(ctx, accountID, companyID, micro)
	if err != nil {
		return err
	}
	if done {
		return nil
	}
	if err := r.seed(ctx, accountID, companyID); err != nil {
		return err
	}
	return r.live.Add(ctx, accountID, companyID, micro)
}

// seed copies the MySQL balance into Redis once (SET NX). Safe to call when
// the key already exists — NX makes it a no-op, so a concurrent seeder can
// never clobber decrements that happened since the other seeder read MySQL.
func (r *RedisReserver) seed(ctx context.Context, accountID, companyID int64) error {
	var balance float64
	var err error
	if companyID != 0 {
		balance, err = r.db.Balance(ctx, companyID)
	} else {
		balance, err = r.db.AccountBalance(ctx, accountID)
	}
	if err != nil {
		return onStore(StoreSQL, "balance to seed", err)
	}
	if err := r.live.Seed(ctx, accountID, companyID, toMicro(balance)); err != nil {
		return onStore(StoreMainRedis, "balance seed", err)
	}
	return nil
}

// Resync deletes the live counter so the NEXT reservation re-seeds it from
// MySQL — the hook to call after a top-up or manual balance edit. companyID=0
// resyncs an unlinked account's key.
//
// The key leaves the balance index too: a deduction that then finds no
// counter (Booker.move) seeds it from MySQL on top of what it took.
func (r *RedisReserver) Resync(ctx context.Context, accountID, companyID int64) error {
	return r.live.Drop(ctx, accountID, companyID)
}

// ResyncAll drops every live balance counter (SCAN, non-blocking) — the
// blunt "I changed several balances in MySQL" hook.
func (r *RedisReserver) ResyncAll(ctx context.Context) (int, error) {
	return r.live.DropAll(ctx)
}

// writeBackLoop copies live Redis balances to MySQL on an interval so
// dashboards/reports read near-live numbers. Best-effort by design: a missed
// cycle only delays visibility, never correctness — Redis stays the live
// counter regardless.
func (r *RedisReserver) writeBackLoop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	round := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			round++
			r.writeBack(ctx, round%writeBackAllEvery == 1) // the first round, then every 20th
		}
	}
}

// writeBackAllEvery: on the first round and every this many after it (ten
// minutes) the whole key space is walked for balance keys, and every live
// balance is written whether it moved or not — so a key the set does not
// know is found, and MySQL is put right if anything else ever changed it.
const writeBackAllEvery = 20

// liveKeys is the live balance keys: the balance index, and nothing else.
//
// Since 29 Oct 2026 a key is in the index only once it was SEEDED from
// MySQL (seedPrepaid, seed, seedOnTop, creditScript). A DECRBY on a counter
// a Redis had lost makes the key from nothing, negative; until it is seeded
// it must never be copied over companies.balance. The walk of the key space
// that used to add every `bal:*` it found to the index is gone for that
// reason (every build since 13 Oct adds its keys to the index itself).
func (r *RedisReserver) liveKeys(ctx context.Context, _ bool) []dal.Counter {
	keys, err := r.live.Seeded(ctx)
	if err != nil {
		return nil
	}
	return keys
}

// writeBack copies the live balances that MOVED since this process last
// wrote them over MySQL (all = every one). It used to write every key on
// every round from every accept process, changed or not — which also moved
// companies.updated_at and sms_service_config.updated_at every 30 seconds,
// and with them every fingerprint that watches those tables (2026-10-13).
func (r *RedisReserver) writeBack(ctx context.Context, all bool) {
	for _, c := range r.liveKeys(ctx, all) {
		key := c.Key
		micro, found, err := r.live.Read(ctx, c)
		if err == nil && !found {
			// The counter is gone (a resync): forget it until it is made again.
			_ = r.live.Forget(ctx, c)
			r.wroteMu.Lock()
			delete(r.wrote, key)
			r.wroteMu.Unlock()
			continue
		}
		if err != nil {
			continue
		}
		r.wroteMu.Lock()
		last, known := r.wrote[key]
		r.wroteMu.Unlock()
		if known && last == micro && !all {
			continue
		}
		bal := float64(micro) / microFactor
		var werr error
		switch {
		case c.Company != 0:
			if werr = r.db.SetBalance(ctx, c.Company, bal); werr != nil && r.logf != nil {
				r.logf("balance write-back company %d: %v", c.Company, werr)
			}
		case c.Account != 0:
			if werr = r.db.SetAccountBalance(ctx, c.Account, bal); werr != nil && r.logf != nil {
				r.logf("balance write-back account %d: %v", c.Account, werr)
			}
		default:
			continue
		}
		if werr == nil {
			r.wroteMu.Lock()
			if r.wrote == nil {
				r.wrote = map[string]int64{}
			}
			r.wrote[key] = micro
			r.wroteMu.Unlock()
		}
	}
}

// ReserveFor is Reserve plus the ledger row (PO 2026-09-17, see
// DBReserver.ReserveFor). The balance lives in Redis here, so the row's
// before/after figures are read from the key after the decrement.
func (r *RedisReserver) ReserveFor(ctx context.Context, accountID, companyID int64, amount float64, msgID string) (bool, error) {
	ok, _, err := r.ReserveForBalance(ctx, accountID, companyID, amount, msgID)
	return ok, err
}

// ReserveForBalance is ReserveFor that also says the live balance right after
// this hold (nil when nothing was held) — what the send reply shows as
// remainingBalance (QA API-004, BILL-005).
//
// The hold and its ledger row stand or fall together. The row used to be
// best-effort ("balance already held" in a log line), but the billing role
// finds an acceptance charge ONLY by that row: without it the message was
// charged a second time at rating, and the ledger never showed the first
// debit. A row that cannot be written now gives the hold back and refuses the
// message, as the MySQL reserver's single transaction always has.
//
// Exact throughout (review, 2026-10-01): the amount is taken in whole ledger
// units, the balance it left is the one the hold's own script returned (a
// read afterwards could already include another message's hold, so two rows
// said the same before/after), and the row is written as exact decimals. The
// row is written on a context the client's going away cannot cancel, and a
// write that reports an error is looked for before the hold is given back: an
// insert can land and still lose its answer, and giving back a hold whose row
// is there would show a charge the balance never took.
func (r *RedisReserver) ReserveForBalance(ctx context.Context, accountID, companyID int64, amount float64, msgID string) (bool, *float64, error) {
	if companyID == 0 {
		// An unlinked account's own counter: held, never ledgered (the
		// billing role charges no unlinked account), as before.
		ok, err := r.Reserve(ctx, accountID, companyID, amount)
		return ok, nil, err
	}
	amt := toUnits(amount)
	if amt <= 0 {
		return true, nil, nil
	}
	ok, left, err := r.reserveUnits(ctx, accountID, companyID, amt)
	if err != nil || !ok {
		return ok, nil, err
	}
	// No row for the message (Saleem 2026-10-13: "we don't need to record per
	// message"): the hold above is the charge, and the message itself says so
	// (model.Message.Quote, Held) — nothing is written to MySQL while the
	// client waits. LedgerPerMessage keeps the old row for the length of an
	// upgrade, when an earlier build's billing role may still rate this
	// message.
	if !LedgerPerMessage() {
		after := float64(left) / microFactor
		return true, &after, nil
	}
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	lerr := r.db.AddHoldRow(wctx, dal.LedgerRow{Account: accountID, Company: companyID,
		Units: -amt, Prev: left + amt, After: left, Reference: msgID, Reason: ReasonAtAcceptance})
	if lerr != nil {
		if n, qerr := r.db.HoldRows(wctx, msgID); qerr == nil && n > 0 {
			after := float64(left) / microFactor
			return true, &after, nil // the row is there: the hold stands
		}
		if rerr := r.Refund(wctx, accountID, companyID, float64(amt)/microFactor); rerr != nil && r.logf != nil {
			r.logf("ledger row for %s failed and the hold of %s could not be given back: %v — needs manual reconciliation", msgID, unitsStr(amt), rerr)
		}
		return false, nil, onStore(StoreSQL, "ledger row for "+msgID, lerr)
	}
	after := float64(left) / microFactor
	return true, &after, nil
}

// reserveUnits holds amt micro-units (= ledger units) iff the live balance
// covers it, seeding the key from MySQL on first touch, and says the balance
// the hold left — atomically, inside the one script.
func (r *RedisReserver) reserveUnits(ctx context.Context, accountID, companyID, amt int64) (bool, int64, error) {
	for attempt := 0; attempt < 2; attempt++ {
		res, err := r.live.HoldLeft(ctx, accountID, companyID, amt)
		if err != nil {
			return false, 0, onStore(StoreMainRedis, "balance", err)
		}
		switch {
		case len(res) == 2 && res[0] == 1:
			return true, res[1], nil
		case len(res) >= 1 && res[0] == 0:
			return false, 0, nil
		case len(res) >= 1 && res[0] == -2:
			if err := r.seed(ctx, accountID, companyID); err != nil {
				return false, 0, err
			}
		default:
			return false, 0, fmt.Errorf("redis reserve: unexpected answer %v", res)
		}
	}
	return false, 0, fmt.Errorf("redis reserve: key %s missing after seed", counterName(accountID, companyID))
}

// Balance is a company's balance as it stands now: the live counter when
// Redis holds one, else companies.balance (nothing has touched it in Redis,
// so MySQL is current).
func (r *RedisReserver) Balance(ctx context.Context, companyID int64) (float64, error) {
	micro, found, err := r.live.Current(ctx, 0, companyID)
	if err != nil {
		return 0, err
	}
	if found {
		return float64(micro) / microFactor, nil
	}
	if r.shown != nil {
		return r.shown.Balance(ctx, companyID)
	}
	return DBReserver{DB: r.db}.Balance(ctx, companyID)
}

// RefundFor is Refund plus the ledger row (type refund, reference refund:<id>).
func (r *RedisReserver) RefundFor(ctx context.Context, accountID, companyID int64, amount float64, msgID string) error {
	if err := r.Refund(ctx, accountID, companyID, amount); err != nil {
		return err
	}
	if companyID == 0 || amount <= 0 {
		return nil
	}
	if !LedgerPerMessage() {
		return nil
	}
	amt := toUnits(amount)
	newBal := toUnits(r.balanceAfter(ctx, accountID, companyID))
	lerr := r.db.AddGiveBackRow(ctx, dal.LedgerRow{Account: accountID, Company: companyID,
		Units: amt, Prev: newBal - amt, After: newBal, Reference: "refund:" + msgID})
	if lerr != nil && r.logf != nil {
		r.logf("refund ledger row for %s failed: %v (balance already returned)", msgID, lerr)
	}
	return nil
}

// balanceAfter reads the live balance from Redis (0 when unreadable).
func (r *RedisReserver) balanceAfter(ctx context.Context, accountID, companyID int64) float64 {
	micro, found, err := r.live.Current(ctx, accountID, companyID)
	if err != nil || !found {
		return 0
	}
	return float64(micro) / microFactor
}

// MicroFactor is the number of micro-units in one unit of balance
// (DECIMAL(14,4)), shared with anything that must speak the counter's units.
const MicroFactor = microFactor

// Credit applies a manual change of deltaMicro (signed) to a company's live
// balance counter, or creates it at newTotalMicro — the engine's
// POST /admin/balance/credit, which the Admin API calls under its company row
// lock so a portal top-up is not erased by the write-back.
func (r *RedisReserver) Credit(ctx context.Context, companyID, deltaMicro, newTotalMicro int64) error {
	return r.live.Credit(ctx, companyID, deltaMicro, newTotalMicro)
}

// Undo reverses a Credit whose MySQL transaction then failed to commit, so
// the live counter does not hold money the ledger never recorded.
func (r *RedisReserver) Undo(ctx context.Context, companyID, deltaMicro int64) error {
	return r.live.Undo(ctx, companyID, deltaMicro)
}

// Booker is this reserver's billing queue: the same Redis, this deployment.
func (r *RedisReserver) Booker() *Booker { return r.book }

// TakeEvents is the accept path's hold with its events (pipeline.EventBooker):
// what the events take from a prepaid balance is deducted (ONE DECRBY on the
// main Redis), refused and given back when the balance after would be below
// zero, and the events are queued on the deployment's queue Redis.
func (r *RedisReserver) TakeEvents(ctx context.Context, companyID int64, evs []Event) (bool, *float64, error) {
	o, left, err := r.book.Take(ctx, companyID, evs)
	if err != nil || o == Short {
		return false, nil, err
	}
	if left < 0 {
		return true, nil, nil
	}
	after := float64(left) / microFactor
	return true, &after, nil
}

// BookEvents queues events that move no balance (postpaid: no call on the
// main Redis).
func (r *RedisReserver) BookEvents(ctx context.Context, companyID int64, evs []Event) error {
	return r.book.Book(ctx, companyID, evs)
}

// GiveBackEvents books the opposite of evs.
func (r *RedisReserver) GiveBackEvents(ctx context.Context, companyID int64, evs []Event) error {
	return r.book.GiveBack(ctx, companyID, evs)
}

// Live is the live counters this reserver holds balances in.
func (r *RedisReserver) Live() dal.LiveBalances { return r.live }

// counterName names a live counter in an error, as its key did.
func counterName(accountID, companyID int64) string {
	if companyID != 0 {
		return fmt.Sprintf("bal:co:%d", companyID)
	}
	return fmt.Sprintf("bal:acct:%d", accountID)
}
