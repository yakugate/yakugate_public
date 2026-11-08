package billing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/yakugate/yakugate/internal/dal"
)

// Booker takes money from a company's live balance and logs the events that
// say why (Saleem, 29 Oct 2026: "a balance is deducted on the go and wherever
// required and an event is sent to the selling queue in local redis").
//
//   - The balance is ONE call on the MAIN Redis: DECRBY of the live counter
//     (`bal:co:<company>`, in ledger units). Its answer is the balance after:
//     zero or more = taken; below zero = low balance. Only the accept path's
//     hold (Take) is refused then — the amount is given back with INCRBY.
//     Every later moment (submit, delivery, a receipt or an MO handed to the
//     client) deducts even into the negative: the message was accepted. A
//     postpaid event moves no balance and makes no call there.
//   - The events go to this deployment's sell and buy queues in the LOCAL
//     queue Redis (cfg.ForQueueRedis()): one ZADD per side, in one round
//     trip, for its db-worker to book. Nothing about money waits in the main
//     Redis any more.
//
// The two are two Redis servers, so they are two steps. The balance goes
// first; events that could not be queued after it are kept and queued again
// (an event's member is its own id, so queueing it twice adds it once).
//
// Until 29 Oct one Lua script read the balance, decremented it, added the
// key to the balance index, put each half on its queue, added the lane to
// the lanes sets and set once-only marks — about nine commands a booking on
// the main Redis, three bookings a message. The marks (`billonce:`) are gone:
// each moment happens once by itself (a message is queued once, taken by a
// vendor once — queues are taken, not leased — and its receipt is handed
// once).
//
// With no Redis (NewSQLBooker, plan of 29 Oct 2026 step 3) the balance is the
// balance of record and the queue a table of the same database: a move and
// its events are ONE transaction (dal.BalanceBook), with the same rules.
type Booker struct {
	live  dal.LiveBalances    // MAIN Redis: the live balances
	q     dal.BillingQueue    // LOCAL queue Redis: the sell and buy queues
	db    dal.CompanyBalances // config store: where a balance is seeded from
	scope string
	lanes int

	// book is the balance of record and the queue in one SQL database (no
	// Redis, plan of 29 Oct 2026 step 3): a move and its events are ONE
	// transaction there, so live is nil and every booking is one call.
	book dal.BalanceBook

	// The lanes sets are written once per process and lane, not per event.
	lanesSaid sync.Map

	// balanceOf reads a company's balance as MySQL holds it (nil = db's
	// balance of record); a test seam.
	balanceOf func(ctx context.Context, company int64) (float64, error)
}

// NewBooker books balances on live (the MAIN Redis) and events on q (this
// deployment's queue Redis) for the deployment owner ("" = shared), seeding
// balances from cfgDB (the balance of record). With no live balances or no
// queue there is no booker.
func NewBooker(live dal.LiveBalances, q dal.BillingQueue, cfgDB dal.CompanyBalances, owner string, lanes int) *Booker {
	if live == nil || q == nil {
		return nil
	}
	if lanes < 1 {
		lanes = DefaultLanes
	}
	return &Booker{live: live, q: q, db: cfgDB, scope: QueueScope(owner), lanes: lanes}
}

// NewSQLBooker books on a gateway with no Redis (plan of 29 Oct 2026, step
// 3): the balance of record moves and the events are written in ONE
// transaction of the SQL database (book), on its billing queue q, for the
// deployment owner ("" = shared). The rules are the Redis booker's: a hold
// at acceptance is refused when the balance after would be below zero,
// every later moment deducts even into the negative, a postpaid event moves
// nothing. With no book or no queue there is no booker.
func NewSQLBooker(book dal.BalanceBook, q dal.BillingQueue, owner string, lanes int) *Booker {
	if book == nil || q == nil {
		return nil
	}
	if lanes < 1 {
		lanes = DefaultLanes
	}
	return &Booker{book: book, q: q, scope: QueueScope(owner), lanes: lanes}
}

// LanesFromEnv is YAKU_DB_QUEUE_LANES (issue 25: "ask in config, number of
// lanes"), 1..64, DefaultLanes when unset or out of range. Every process of
// a deployment must use the same number; the lanes set keeps a lane named by
// an older count readable.
func LanesFromEnv() int {
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_DB_QUEUE_LANES"))); err == nil && n >= 1 && n <= 64 {
		return n
	}
	return DefaultLanes
}

// Scope is the deployment part of the queue's keys.
func (b *Booker) Scope() string { return b.scope }

// Lanes is how many lanes this booker spreads companies over.
func (b *Booker) Lanes() int { return b.lanes }

// Outcome is what a booking did.
type Outcome int

const (
	Booked Outcome = iota // the balance moved (if any) and the events are queued
	Short                 // take only: the balance does not cover it; nothing done
)

// Take is the accept path's hold: what the events take from a prepaid
// balance — refused (Short, and given back) when the balance after would be
// below zero — and the events queued. left is the balance after (ledger
// units; -1 when nothing moved).
func (b *Booker) Take(ctx context.Context, company int64, evs []Event) (Outcome, int64, error) {
	if b == nil {
		return Booked, -1, fmt.Errorf("no billing queue in this process")
	}
	delta, err := b.delta(company, evs)
	if err != nil {
		return Booked, -1, err
	}
	if b.book != nil {
		// One transaction: the hold (refused when short) and the events.
		ok, left, err := b.book.Move(ctx, company, delta, true, b.add(company, evs))
		switch {
		case err != nil:
			return Booked, -1, onStore(StoreSQL, "balance and billing events", err)
		case !ok:
			return Short, -1, nil
		case delta == 0:
			return Booked, -1, nil
		}
		return Booked, left, nil
	}
	left := int64(-1)
	if delta != 0 {
		if left, err = b.move(ctx, company, delta); err != nil {
			return Booked, -1, err
		}
		if delta > 0 && left < 0 {
			// Low balance: give it back. Nothing is queued.
			if _, err := b.move(ctx, company, -delta); err != nil {
				return Short, -1, fmt.Errorf("a refused hold of company %d could not be given back: %w", company, err)
			}
			return Short, -1, nil
		}
	}
	if err := b.queue(ctx, company, evs); err != nil {
		// The client is refused: what was taken goes back.
		if delta != 0 {
			if _, gerr := b.move(context.WithoutCancel(ctx), company, -delta); gerr != nil {
				return Booked, -1, fmt.Errorf("events not queued (%v), and the hold of company %d not given back: %w", err, company, gerr)
			}
		}
		return Booked, -1, err
	}
	return Booked, left, nil
}

// Book deducts what the events take (never refused: a balance may go below
// zero after acceptance) and queues them.
func (b *Booker) Book(ctx context.Context, company int64, evs []Event) error {
	if b == nil {
		return fmt.Errorf("no billing queue in this process")
	}
	delta, err := b.delta(company, evs)
	if err != nil {
		return err
	}
	if b.book != nil {
		// One transaction: both or neither. A booking that moves no balance
		// is its events alone, which may always be put again.
		if _, _, err := b.book.Move(ctx, company, delta, false, b.add(company, evs)); err != nil {
			return &stepError{err: onStore(StoreSQL, "balance and billing events", err), moved: delta == 0}
		}
		return nil
	}
	if delta != 0 {
		if _, err := b.move(ctx, company, delta); err != nil {
			return &stepError{err: err}
		}
	}
	if err := b.queue(ctx, company, evs); err != nil {
		return &stepError{err: err, moved: true}
	}
	return nil
}

// Queue puts events on the queues and moves no balance: a booking whose
// balance has moved already (balanceMoved) and whose events are left.
func (b *Booker) Queue(ctx context.Context, company int64, evs []Event) error {
	if b == nil {
		return fmt.Errorf("no billing queue in this process")
	}
	return b.queue(ctx, company, evs)
}

// GiveBack books the opposite of evs: the hold of a message that never
// reached the queue.
func (b *Booker) GiveBack(ctx context.Context, company int64, evs []Event) error {
	back := make([]Event, 0, len(evs))
	for _, e := range evs {
		back = append(back, e.Negated())
	}
	return b.Book(ctx, company, back)
}

// stepError says which step of a booking failed: moved = the balance step
// is done (moved, or nothing to move) and only the events are left to queue
// — queueing them again is always safe (an event's member is its own id).
type stepError struct {
	err   error
	moved bool
}

func (e *stepError) Error() string { return e.err.Error() }
func (e *stepError) Unwrap() error { return e.err }

// The stores a booking can fail on, as StoreError names them: the balance on
// the main Redis, the events on the box's queue Redis, or both in the SQL
// database of a gateway with no Redis.
const (
	StoreMainRedis  = "main redis"
	StoreQueueRedis = "queue redis"
	StoreSQL        = "sql"
)

// StoreError says which store a booking failed on, and what it was doing
// there (load test of 30 Oct 2026: "balance reservation failed" could not
// say whether the main Redis or the queue Redis had failed).
type StoreError struct {
	Store string // StoreMainRedis, StoreQueueRedis or StoreSQL
	What  string // "balance", "billing events", ...
	Err   error
}

func (e *StoreError) Error() string { return e.Store + ": " + e.What + ": " + e.Err.Error() }
func (e *StoreError) Unwrap() error { return e.Err }

// onStore names the store err came from.
func onStore(store, what string, err error) error {
	return &StoreError{Store: store, What: what, Err: err}
}

// StoreOf is the store a booking's error names ("" = none).
func StoreOf(err error) string {
	var se *StoreError
	if errors.As(err, &se) {
		return se.Store
	}
	return ""
}

// balanceMoved says a failed booking's balance step is done: only its
// events are left.
func balanceMoved(err error) bool {
	se, ok := err.(*stepError)
	return ok && se.moved
}

// delta is what the events move on a prepaid balance (positive = taken).
func (b *Booker) delta(company int64, evs []Event) (int64, error) {
	var d int64
	for _, e := range evs {
		if e.Empty() || e.Company == 0 {
			continue
		}
		if e.Company != company {
			return 0, fmt.Errorf("one booking is one company's: event %s is company %d, not %d", e.ID, e.Company, company)
		}
		d += e.Prepaid()
	}
	return d, nil
}

// move is the one call on the main Redis: DECRBY of the company's live
// balance by d (negative = given back). It answers the balance after.
//
// A counter that answers exactly -d was 0 before — or was not there, and the
// DECRBY just made it from nothing (a Redis that lost its data; balances are
// seeded when a role starts and never expire). The balance index says which:
// a seeded key is in it. A key that was not there is seeded now from MySQL,
// on top of whatever was already taken from the new key (seedOnTop), so no
// deduction is lost and none is taken twice.
func (b *Booker) move(ctx context.Context, company, d int64) (int64, error) {
	left, err := b.live.Deduct(ctx, company, d)
	if err != nil {
		return 0, onStore(StoreMainRedis, "balance", err)
	}
	if left != -d {
		return left, nil
	}
	known, err := b.live.IsSeeded(ctx, company)
	if err != nil || known {
		return left, nil // an index that cannot be read: the counter stands
	}
	return b.seedOnTop(ctx, company)
}

// seedOnTop adds the MySQL balance to a counter that is not yet in the
// balance index, and adds it there — once, whoever asks (the live store's
// one script). Answers the counter after.
func (b *Booker) seedOnTop(ctx context.Context, company int64) (int64, error) {
	var balance float64
	switch {
	case b.balanceOf != nil:
		v, err := b.balanceOf(ctx, company)
		if err != nil {
			return 0, onStore(StoreSQL, "balance to seed", err)
		}
		balance = v
	case b.db == nil:
		return 0, fmt.Errorf("no config store to read the balance of company %d from", company)
	default:
		v, err := b.db.Balance(ctx, company)
		if err != nil {
			return 0, onStore(StoreSQL, "balance to seed", err)
		}
		balance = v
	}
	left, err := b.live.SeedOnTop(ctx, company, toMicro(balance))
	if err != nil {
		return 0, onStore(StoreMainRedis, "balance seed", err)
	}
	return left, nil
}

// queue puts each event's halves on this deployment's sell and buy queues in
// the local Redis: one ZADD per side, one round trip.
func (b *Booker) queue(ctx context.Context, company int64, evs []Event) error {
	adds := b.add(company, evs)
	if len(adds) == 0 {
		return nil
	}
	if err := b.q.Put(ctx, adds); err != nil {
		store := StoreQueueRedis
		if b.book != nil {
			store = StoreSQL // the SQL booker's queue is a table
		}
		return onStore(store, "billing events", err)
	}
	return nil
}

// add is the lane adds of evs, a side's sell or buy half each (and a lane's
// name on the lanes set the first time this process uses it); none =
// nothing to queue.
func (b *Booker) add(company int64, evs []Event) []dal.LaneAdd {
	var sells, buys []dal.Scored
	for _, e := range evs {
		if e.Empty() || e.Company == 0 {
			continue
		}
		// Each side to its own queue: the halves keep the event's id, its
		// moment, its message and its counts, and each carries one side.
		if s, ok := half(e, true); ok {
			sells = append(sells, dal.Scored{Score: float64(e.At), Member: s.Member()})
		}
		if bh, ok := half(e, false); ok {
			buys = append(buys, dal.Scored{Score: float64(e.At), Member: bh.Member()})
		}
	}
	if len(sells) == 0 && len(buys) == 0 {
		return nil
	}
	lane := LaneOf(company, b.lanes)
	var adds []dal.LaneAdd
	for _, x := range []struct {
		side string
		zs   []dal.Scored
	}{{SideSell, sells}, {SideBuy, buys}} {
		if len(x.zs) == 0 {
			continue
		}
		a := dal.LaneAdd{Lane: dal.Lane{Scope: b.scope, Side: x.side, Lane: lane}, Events: x.zs}
		said := x.side + ":" + strconv.Itoa(lane)
		if _, ok := b.lanesSaid.Load(said); !ok {
			a.Name = true
			b.lanesSaid.Store(said, true)
		}
		adds = append(adds, a)
	}
	return adds
}

// half is e with one side kept: its sell side (sell) or its buy side; ok is
// false when e has no such side.
func half(e Event, sell bool) (Event, bool) {
	if sell {
		e.Buy = nil
	} else {
		e.Sell = nil
	}
	return e, !e.Empty()
}

// Item is one booking of one company, for BookMany. Moved: its balance was
// moved already (an earlier try) and only its events are left to queue.
type Item struct {
	Company int64
	Booking Booking
	Moved   bool
}

// BookMany books many bookings in TWO round trips: every balance that moves
// in one pipeline on the main Redis (one DECRBY each), then every event in
// one pipeline on the local Redis. The answer is an error per item; an error
// that balanceMoved says yes to left only its events unqueued.
func (b *Booker) BookMany(ctx context.Context, items []Item) []error {
	errs := make([]error, len(items))
	if b == nil {
		for i := range errs {
			errs[i] = fmt.Errorf("no billing queue in this process")
		}
		return errs
	}
	deltas := make([]int64, len(items))
	moved := make([]bool, len(items))
	var ds []dal.Deduction
	var idx []int
	for i, it := range items {
		d, err := b.delta(it.Company, it.Booking.Events)
		if err != nil {
			errs[i] = err
			continue
		}
		deltas[i], moved[i] = d, it.Moved
		if d != 0 && !it.Moved {
			ds = append(ds, dal.Deduction{Company: it.Company, Units: d})
			idx = append(idx, i)
		}
	}
	if b.book != nil {
		// ONE transaction for every balance and every event of the batch.
		var adds []dal.LaneAdd
		for i, it := range items {
			if errs[i] == nil {
				adds = append(adds, b.add(it.Company, it.Booking.Events)...)
			}
		}
		if len(ds) == 0 && len(adds) == 0 {
			return errs
		}
		if _, err := b.book.MoveMany(ctx, ds, adds); err != nil {
			err = onStore(StoreSQL, "balance and billing events", err)
			for i := range items {
				if errs[i] == nil {
					// Nothing moved with it (one transaction); a booking that
					// moves no balance has only events, and putting an event
					// twice puts it once.
					errs[i] = &stepError{err: err, moved: moved[i] || deltas[i] == 0}
				}
			}
		}
		return errs
	}
	if len(ds) > 0 {
		for k, r := range b.live.DeductMany(ctx, ds) {
			i := idx[k]
			if r.Err != nil {
				errs[i] = &stepError{err: onStore(StoreMainRedis, "balance", r.Err)}
				continue
			}
			moved[i] = true
			if r.Left == -deltas[i] {
				// Was 0, or made from nothing: the index says which (move).
				// A seed that fails leaves the deduction on the counter; the
				// next deduction that finds the key unseeded seeds it.
				if known, err := b.live.IsSeeded(ctx, items[i].Company); err == nil && !known {
					_, _ = b.seedOnTop(ctx, items[i].Company)
				}
			}
		}
	}
	var adds []dal.LaneAdd
	var queued []int
	for i, it := range items {
		if errs[i] != nil {
			continue
		}
		if a := b.add(it.Company, it.Booking.Events); len(a) > 0 {
			adds = append(adds, a...)
			queued = append(queued, i)
		}
	}
	if len(queued) > 0 {
		if err := b.q.Put(ctx, adds); err != nil {
			err = onStore(StoreQueueRedis, "billing events", err)
			for _, i := range queued {
				errs[i] = &stepError{err: err, moved: moved[i] || deltas[i] == 0}
			}
		}
	}
	return errs
}

// BookAll books each booking of one company, one at a time.
func (b *Booker) BookAll(ctx context.Context, company int64, bs []Booking) error {
	for _, bk := range bs {
		if err := b.Book(ctx, company, bk.Events); err != nil {
			return err
		}
	}
	return nil
}

// Waiting is how many events wait on this deployment's billing queues (both
// sides) and how long the oldest has waited (ms): what a role that books says
// on its status, so events nobody books are seen even with no db-worker
// reporting. Read from the queues' own lengths in the local Redis.
func (b *Booker) Waiting(ctx context.Context) (n, oldestMS int64, err error) {
	return b.q.Waiting(ctx, b.scope, Sides, b.lanes)
}

// unsent says a failed booking certainly never ran on either Redis.
func (b *Booker) unsent(err error) bool {
	if b == nil {
		return false
	}
	if b.book != nil {
		return b.book.Unsent(err) || b.q.Unsent(err)
	}
	return b.live.Unsent(err) || b.q.Unsent(err)
}
