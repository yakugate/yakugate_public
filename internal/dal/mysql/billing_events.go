package mysql

import (
	"context"
	"crypto/md5"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// The billing queue and the balance moves in MySQL, for an install with no
// Redis (plan of 29 Oct 2026, step 3; config/0167_billing_events).
//
// With Redis a balance is one DECRBY on the main Redis and its events one
// ZADD a side on the queue Redis: two servers, two steps. Here both are the
// config store, so they are ONE transaction (dal.BalanceBook): the company's
// row locked, the balance moved, the events written, committed — both or
// neither. The accept path's hold already was a transaction on that row
// (dal.CompanyBalances HoldUnits, one a message); here the moves of a
// process are committed together (Move: group commit), so under load one
// lock and one commit carry many messages.
//
// The queue (dal.BillingQueue) is the table billing_events: a row is an
// event's half (sell or buy) on a lane, in one of four states — 0 ready,
// 1 delayed, 2 and 3 doing (claimed from delayed, claimed from ready: the
// claim hands out the due delayed ones first, as the Redis script does). Its
// key (scope, side, lane, state, score) serves every read of a lane; an
// event put twice is there once (the unique key on its member's MD5, as a
// sorted-set member is once in its set). The db-worker books it with the
// same code as the Redis queues (internal/billingq): claim, book under the
// batch's id, done.
//
// A lane's turn is a row of queue_turns (turns.go). A claim and a done check
// the turn inside their own transaction, so a worker whose turn ran out
// changes nothing, as the Redis scripts did.

// BillingEvents is the billing queue and the balance moves on the config
// store's pool.
type BillingEvents struct {
	db    *sql.DB
	turns turns

	// The writers that commit the moves of this process together (Move).
	once sync.Once
	in   chan *moveReq
}

// NewBillingEvents keeps the billing queue on db (the config store).
func NewBillingEvents(db *sql.DB) *BillingEvents {
	if db == nil {
		return nil
	}
	return &BillingEvents{db: db, turns: turns{db: db}}
}

var (
	_ dal.BillingQueue = (*BillingEvents)(nil)
	_ dal.BalanceBook  = (*BillingEvents)(nil)
)

// The states of a row.
const (
	evReady       = 0
	evDelayed     = 1
	evDoingLater  = 2 // claimed from delayed
	evDoingNow    = 3 // claimed from ready
	evPutColumns  = "(scope, side, lane, state, score, mhash, member)"
	evPutRow      = "(?,?,?,?,?,?,?)"
	evPutOnRepeat = " ON DUPLICATE KEY UPDATE mhash = mhash"
)

var (
	// The accept path and every later moment: one INSERT of every half,
	// inside the balance's transaction.
	stEvPut = BillingStmts.Add("billing.events_put",
		"INSERT INTO billing_events "+evPutColumns+" VALUES "+evPutRow+evPutOnRepeat, true)
	// The balance moves: amounts are exact decimals (unitsStr), cast so the
	// arithmetic stays DECIMAL (a quoted value would be read as a double).
	// One statement for every company of a batch; the rows are locked first,
	// in id order (events_lock), so two batches never wait in a circle.
	stEvLock = BillingStmts.Add("billing.events_lock",
		"SELECT id, balance FROM companies WHERE id IN (?) ORDER BY id FOR UPDATE", true)
	stEvMoveMany = BillingStmts.Add("billing.events_move_many",
		"UPDATE companies SET balance = balance - CASE id WHEN ? THEN CAST(? AS DECIMAL(18,4)) END, updated_at = updated_at WHERE id IN (?)", true)
	stEvBalances = BillingStmts.Add("billing.events_balances",
		"SELECT id, balance FROM companies WHERE id IN (?)", true)

	// The db-worker's side.
	stEvDoing = BillingStmts.Add("billing.events_doing", `SELECT COALESCE(batch_id,''), member FROM billing_events
		WHERE scope = ? AND side = ? AND lane = ? AND state IN (2,3) ORDER BY state, score, id`, false)
	stEvClaimDelayed = BillingStmts.Add("billing.events_claim_delayed", `UPDATE billing_events SET state = 2, batch_id = ?
		WHERE scope = ? AND side = ? AND lane = ? AND state = 1 AND score <= ? ORDER BY score, id LIMIT ?`, false)
	stEvClaimReady = BillingStmts.Add("billing.events_claim_ready", `UPDATE billing_events SET state = 3, batch_id = ?
		WHERE scope = ? AND side = ? AND lane = ? AND state = 0 ORDER BY score, id LIMIT ?`, false)
	stEvSetBatch = BillingStmts.Add("billing.events_set_batch", `UPDATE billing_events SET batch_id = ?
		WHERE scope = ? AND side = ? AND lane = ? AND state IN (2,3)`, false)
	stEvDone = BillingStmts.Add("billing.events_done",
		"DELETE FROM billing_events WHERE scope = ? AND side = ? AND lane = ? AND state IN (2,3)", false)
	stEvLater = BillingStmts.Add("billing.events_later",
		"INSERT INTO billing_events "+evPutColumns+" VALUES "+evPutRow+
			" ON DUPLICATE KEY UPDATE state = 1, score = VALUES(score), batch_id = NULL", false)
	stEvLanes = BillingStmts.Add("billing.events_lanes",
		"SELECT DISTINCT lane FROM billing_events WHERE scope = ? AND side = ? LIMIT 1024", false)
	stEvAny = BillingStmts.Add("billing.events_any",
		"SELECT 1 FROM billing_events WHERE scope = ? AND side = ? AND lane = ? LIMIT 1", false)
	stEvCount = BillingStmts.Add("billing.events_count", `SELECT state, COUNT(*), COALESCE(MIN(score), 0) FROM billing_events
		WHERE scope = ? AND side = ? AND lane = ? GROUP BY state`, false)
	stEvWaiting = BillingStmts.Add("billing.events_waiting", `SELECT COUNT(*), MIN(score) FROM billing_events
		WHERE scope = ? AND side IN (?) AND state = 0`, false)
)

// evRow is one half to write.
type evRow struct {
	scope, side string
	lane        int
	score       int64
	member      string
}

// rowsOf is the halves of adds, in order.
func eventRowsOf(adds []dal.LaneAdd) []evRow {
	var out []evRow
	for _, a := range adds {
		for _, e := range a.Events {
			out = append(out, evRow{scope: a.Scope, side: a.Side, lane: a.Lane.Lane, score: int64(e.Score), member: e.Member})
		}
	}
	return out
}

// putSQL is one INSERT of rows in state st, and its arguments.
func putSQL(rows []evRow, st int, tail string) (string, []any) {
	var sb strings.Builder
	sb.WriteString("INSERT INTO billing_events " + evPutColumns + " VALUES ")
	args := make([]any, 0, len(rows)*7)
	for i, r := range rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(evPutRow)
		h := md5.Sum([]byte(r.member))
		args = append(args, r.scope, r.side, r.lane, st, r.score, h[:], r.member)
	}
	sb.WriteString(tail)
	return sb.String(), args
}

// execer is a pool or a transaction.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// put writes rows as ready events (a row there already stays as it is).
func put(ctx context.Context, x execer, rows []evRow) error {
	if len(rows) == 0 {
		return nil
	}
	q, args := putSQL(rows, evReady, evPutOnRepeat)
	_, err := x.ExecContext(ctx, q, args...)
	return err
}

// ---- dal.BalanceBook --------------------------------------------------------

// errNoCompany: a hold on a company that is not there.
var errNoCompany = errors.New("no such company")

// Move moves a company's balance and writes the events in ONE transaction,
// both or neither: refused (ok=false, nothing moved, nothing written) when
// refuse says so and the balance does not cover units. updated_at is left
// as it is: a balance move is not a change of the company's settings, which
// the reference cache watches.
//
// The moves of a process are COMMITTED TOGETHER (group commit): a call is
// handed to one of moveWriters writers, which takes it with every call
// waiting behind it (at most moveBatch) and books them in one transaction —
// the companies' rows locked in id order (one SELECT … FOR UPDATE), each
// call judged in the order it came against the running balance, one UPDATE
// for every company, one INSERT of every half, one commit. A call that comes
// alone is booked at once: nobody waits to fill a batch. Every message's
// money is still committed before its caller is answered; under load one
// commit carries many messages, where one a message held the company's row
// (and a pooled connection) for a commit each.
//
// The caller waits for its answer even when its own context ends (a client
// that went away): the batch may already have committed its hold, and the
// caller then gives it back the way a refused message is (the accept path's
// give-back). The batch runs on a context of its own, bounded by moveFor.
//
// A company that is not there: refused with an error at a hold (refuse);
// at a later moment its events are still written (the money was decided)
// and nothing moves, as Redis's DECRBY made a counter from nothing.
func (b *BillingEvents) Move(_ context.Context, company, units int64, refuse bool, adds []dal.LaneAdd) (bool, int64, error) {
	rows := eventRowsOf(adds)
	if units == 0 && len(rows) == 0 {
		return true, 0, nil
	}
	b.startWriters()
	r := &moveReq{company: company, units: units, refuse: refuse, rows: rows, done: make(chan moveAns, 1)}
	b.in <- r
	a := <-r.done
	return a.ok, a.left, a.err
}

const (
	moveWriters = 2                // transactions a process has in flight
	moveBatch   = 256              // calls one transaction books at most
	moveFor     = 10 * time.Second // the most one batch may take
)

// moveReq is one Move waiting for its writer; moveAns its answer.
type moveReq struct {
	company, units int64
	refuse         bool
	rows           []evRow
	done           chan moveAns
}

type moveAns struct {
	ok   bool
	left int64
	err  error
}

func (b *BillingEvents) startWriters() {
	b.once.Do(func() {
		b.in = make(chan *moveReq, 4096)
		for i := 0; i < moveWriters; i++ {
			go b.writeMoves()
		}
	})
}

// writeMoves books what waits, a batch at a time, until the process ends.
func (b *BillingEvents) writeMoves() {
	for first := range b.in {
		batch := []*moveReq{first}
	more:
		for len(batch) < moveBatch {
			select {
			case r := <-b.in:
				batch = append(batch, r)
			default:
				break more
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), moveFor)
		ans, err := b.moveBatch(ctx, batch)
		if err != nil && len(batch) > 1 {
			// One call's events may be what the store refused: each alone,
			// so only that one fails.
			for i, r := range batch {
				one, e := b.moveBatch(ctx, []*moveReq{r})
				if e != nil {
					ans[i] = moveAns{err: e}
				} else {
					ans[i] = one[0]
				}
			}
		} else if err != nil {
			ans[0] = moveAns{err: err}
		}
		cancel()
		for i, r := range batch {
			r.done <- ans[i]
		}
	}
}

// moveBatch books calls in one transaction (see Move) and answers each.
// err is the transaction's (every call failed with it).
func (b *BillingEvents) moveBatch(ctx context.Context, batch []*moveReq) ([]moveAns, error) {
	ans := make([]moveAns, len(batch))
	seen := map[int64]bool{}
	var ids []int64
	for _, r := range batch {
		if r.units != 0 && !seen[r.company] {
			seen[r.company] = true
			ids = append(ids, r.company)
		}
	}
	if len(ids) == 0 {
		// Nothing moves (postpaid, buy sides): the INSERT alone.
		var rows []evRow
		for i, r := range batch {
			rows = append(rows, r.rows...)
			ans[i] = moveAns{ok: true}
		}
		if err := againIfVictimN(ctx, enqueueTries, func() error { return put(ctx, b.db, rows) }); err != nil {
			return ans, err
		}
		return ans, nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	err := rcTx(ctx, b.db, enqueueTries, func(tx *sql.Tx) error {
		in := placeholders(len(ids))
		args := make([]any, len(ids))
		for i, id := range ids {
			args[i] = id
		}
		rs, err := tx.QueryContext(ctx, strings.Replace(stEvLock.SQL, "IN (?)", "IN ("+in+")", 1), args...)
		if err != nil {
			return err
		}
		bal := map[int64]int64{}
		for rs.Next() {
			var id int64
			var v float64
			if err := rs.Scan(&id, &v); err != nil {
				rs.Close()
				return err
			}
			bal[id] = toUnits(v)
		}
		rs.Close()
		if err := rs.Err(); err != nil {
			return err
		}
		net := map[int64]int64{}
		var rows []evRow
		for i, r := range batch {
			ans[i] = moveAns{}
			cur, there := bal[r.company]
			switch {
			case r.units == 0:
				ans[i].ok = true
			case !there && r.refuse:
				ans[i].err = &notCommitted{fmt.Errorf("company %d: %w", r.company, errNoCompany)}
				continue
			case !there:
				ans[i].ok, ans[i].left = true, -r.units // its events stand; nothing moved
			case r.refuse && r.units > 0 && cur-r.units < 0:
				ans[i].left = cur // short: nothing moves, nothing is written
				continue
			default:
				cur -= r.units
				bal[r.company], net[r.company] = cur, net[r.company]+r.units
				ans[i].ok, ans[i].left = true, cur
			}
			rows = append(rows, r.rows...)
		}
		var sb strings.Builder
		var uargs, inArgs []any
		for _, id := range ids {
			if net[id] == 0 {
				continue
			}
			if len(uargs) == 0 {
				sb.WriteString("UPDATE companies SET balance = balance - CASE id")
			}
			sb.WriteString(" WHEN ? THEN CAST(? AS DECIMAL(18,4))")
			uargs = append(uargs, id, unitsStr(net[id]))
			inArgs = append(inArgs, id)
		}
		if len(uargs) > 0 {
			sb.WriteString(" END, updated_at = updated_at WHERE id IN (" + placeholders(len(inArgs)) + ")")
			if _, err := tx.ExecContext(ctx, sb.String(), append(uargs, inArgs...)...); err != nil {
				return err
			}
		}
		return put(ctx, tx, rows)
	})
	return ans, err
}

// MoveMany moves many companies' balances and writes every event in ONE
// transaction: one INSERT of every half first, then one UPDATE for all the
// companies (a CASE by id; it locks their rows in id order, so two bookers
// never wait on each other in a circle), then one read of the balances
// after, committed. Each answer is the balance after its own move, in the
// order asked. Never refused: these are later moments.
func (b *BillingEvents) MoveMany(ctx context.Context, moves []dal.Deduction, adds []dal.LaneAdd) ([]dal.Deducted, error) {
	rows := eventRowsOf(adds)
	net := map[int64]int64{}
	for _, m := range moves {
		if m.Units != 0 {
			net[m.Company] += m.Units
		}
	}
	ids := make([]int64, 0, len(net))
	for id, u := range net {
		if u != 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	bal := map[int64]int64{}
	var err error
	if len(ids) == 0 {
		err = againIfVictimN(ctx, 8, func() error { return put(ctx, b.db, rows) })
	} else {
		err = rcTx(ctx, b.db, 8, func(tx *sql.Tx) error {
			clear(bal)
			if err := put(ctx, tx, rows); err != nil {
				return err
			}
			in := placeholders(len(ids))
			args := make([]any, len(ids))
			for i, id := range ids {
				args[i] = id
			}
			var sb strings.Builder
			uargs := make([]any, 0, len(ids)*3)
			sb.WriteString("UPDATE companies SET balance = balance - CASE id")
			for _, id := range ids {
				sb.WriteString(" WHEN ? THEN CAST(? AS DECIMAL(18,4))")
				uargs = append(uargs, id, unitsStr(net[id]))
			}
			sb.WriteString(" END, updated_at = updated_at WHERE id IN (" + in + ")")
			uargs = append(uargs, args...)
			if _, err := tx.ExecContext(ctx, sb.String(), uargs...); err != nil {
				return err
			}
			rs, err := tx.QueryContext(ctx, strings.Replace(stEvBalances.SQL, "IN (?)", "IN ("+in+")", 1), args...)
			if err != nil {
				return err
			}
			defer rs.Close()
			for rs.Next() {
				var id int64
				var v float64
				if err := rs.Scan(&id, &v); err != nil {
					return err
				}
				bal[id] = toUnits(v) + net[id] // the balance before the moves
			}
			return rs.Err()
		})
	}
	if err != nil {
		return nil, err
	}
	out := make([]dal.Deducted, len(moves))
	for i, m := range moves {
		bal[m.Company] -= m.Units
		out[i] = dal.Deducted{Left: bal[m.Company]}
	}
	return out, nil
}

// Unsent says the call certainly changed nothing (turns.go unsentErr).
func (b *BillingEvents) Unsent(err error) bool { return unsentErr(err) }

// ---- dal.BillingQueue -------------------------------------------------------

func turnOf(l dal.Lane) string {
	return "billing:" + l.Scope + ":" + l.Side + ":" + strconv.Itoa(l.Lane)
}

// Put writes every half as a ready event, one INSERT for all of them (an
// event put twice is there once). The lanes need no naming: a lane is named
// by the rows on it.
func (b *BillingEvents) Put(ctx context.Context, adds []dal.LaneAdd) error {
	rows := eventRowsOf(adds)
	if len(rows) == 0 {
		return nil
	}
	return againIfVictimN(ctx, enqueueTries, func() error { return put(ctx, b.db, rows) })
}

// Waiting counts the ready events of the sides of scope, every lane, and
// how long the oldest has waited (ms).
func (b *BillingEvents) Waiting(ctx context.Context, scope string, sides []string, _ int) (int64, int64, error) {
	if len(sides) == 0 {
		return 0, 0, nil
	}
	args := []any{scope}
	for _, s := range sides {
		args = append(args, s)
	}
	q := strings.Replace(stEvWaiting.SQL, "IN (?)", "IN ("+placeholders(len(sides))+")", 1)
	var n int64
	var first sql.NullInt64
	if err := b.db.QueryRowContext(ctx, q, args...).Scan(&n, &first); err != nil {
		return 0, 0, err
	}
	var oldest int64
	if first.Valid {
		if age := time.Now().UnixMilli() - first.Int64; age > 0 {
			oldest = age
		}
	}
	return n, oldest, nil
}

// Lanes is every lane of a side that holds an event.
func (b *BillingEvents) Lanes(ctx context.Context, scope, side string) ([]string, error) {
	rows, err := b.db.QueryContext(ctx, stEvLanes.SQL, scope, side)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var l int
		if err := rows.Scan(&l); err != nil {
			return nil, err
		}
		out = append(out, strconv.Itoa(l))
	}
	return out, rows.Err()
}

// TakeTurn takes a lane's turn when it is free or run out, or renews it when
// it is token's.
func (b *BillingEvents) TakeTurn(ctx context.Context, l dal.Lane, token string, ttl time.Duration) (bool, error) {
	return b.turns.take(ctx, turnOf(l), token, ttl, false)
}

// DropTurn lets a lane's turn go when it is token's.
func (b *BillingEvents) DropTurn(ctx context.Context, l dal.Lane, token string) error {
	return b.turns.drop(ctx, turnOf(l), token)
}

// Claim gives the batch to book while the turn is token's, in one
// transaction: the batch left in doing (repeat, whole whatever max says),
// else up to max events — the delayed ones whose time has come first, then
// the oldest ready — marked doing under newID. Answers {id, "1"|"0",
// member…}, or nothing.
func (b *BillingEvents) Claim(ctx context.Context, l dal.Lane, token string, max int, newID string, nowMS int64) ([]string, error) {
	var out []string
	err := rcTx(ctx, b.db, 8, func(tx *sql.Tx) error {
		out = nil
		mine, err := turnHeld(ctx, tx, turnOf(l), token)
		if err != nil || !mine {
			return err
		}
		read := func() ([]string, string, error) {
			rows, err := tx.QueryContext(ctx, stEvDoing.SQL, l.Scope, l.Side, l.Lane)
			if err != nil {
				return nil, "", err
			}
			defer rows.Close()
			var ms []string
			id := ""
			for rows.Next() {
				var bid, m string
				if err := rows.Scan(&bid, &m); err != nil {
					return nil, "", err
				}
				if id == "" {
					id = bid
				}
				ms = append(ms, m)
			}
			return ms, id, rows.Err()
		}
		ms, id, err := read()
		if err != nil {
			return err
		}
		if len(ms) > 0 {
			out = append([]string{id, "1"}, ms...)
			return nil
		}
		if max <= 0 {
			return nil
		}
		res, err := tx.ExecContext(ctx, stEvClaimDelayed.SQL, newID, l.Scope, l.Side, l.Lane, nowMS, max)
		if err != nil {
			return err
		}
		got, _ := res.RowsAffected()
		if int(got) < max {
			if _, err := tx.ExecContext(ctx, stEvClaimReady.SQL, newID, l.Scope, l.Side, l.Lane, max-int(got)); err != nil {
				return err
			}
		}
		if ms, _, err = read(); err != nil {
			return err
		}
		if len(ms) > 0 {
			out = append([]string{newID, "0"}, ms...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		out = []string{}
	}
	return out, nil
}

// SetBatchID names the batch in doing.
func (b *BillingEvents) SetBatchID(ctx context.Context, l dal.Lane, id string) error {
	_, err := b.db.ExecContext(ctx, stEvSetBatch.SQL, id, l.Scope, l.Side, l.Lane)
	return err
}

// Done ends a booked batch while the turn is token's, in one transaction:
// the doing rows deleted, those booked later written as delayed. 1 = done,
// 0 = the turn was lost (nothing changed).
func (b *BillingEvents) Done(ctx context.Context, l dal.Lane, token string, later []dal.Scored) (int, error) {
	done := 0
	err := rcTx(ctx, b.db, 8, func(tx *sql.Tx) error {
		done = 0
		mine, err := turnHeld(ctx, tx, turnOf(l), token)
		if err != nil || !mine {
			return err
		}
		if _, err := tx.ExecContext(ctx, stEvDone.SQL, l.Scope, l.Side, l.Lane); err != nil {
			return err
		}
		if len(later) > 0 {
			rows := make([]evRow, len(later))
			for i, s := range later {
				rows[i] = evRow{scope: l.Scope, side: l.Side, lane: l.Lane, score: int64(s.Score), member: s.Member}
			}
			q, args := putSQL(rows, evDelayed, " ON DUPLICATE KEY UPDATE state = 1, score = VALUES(score), batch_id = NULL")
			if _, err := tx.ExecContext(ctx, q, args...); err != nil {
				return err
			}
		}
		done = 1
		return nil
	})
	return done, err
}

// DropIfEmpty says nothing is left on a lane. The lane is then off its
// side's list by itself (the list is the lanes that hold rows).
func (b *BillingEvents) DropIfEmpty(ctx context.Context, l dal.Lane) (bool, error) {
	var one int
	err := b.db.QueryRowContext(ctx, stEvAny.SQL, l.Scope, l.Side, l.Lane).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	return false, err
}

// Count is what a lane holds now, in one statement.
func (b *BillingEvents) Count(ctx context.Context, l dal.Lane) (dal.LaneCount, error) {
	rows, err := b.db.QueryContext(ctx, stEvCount.SQL, l.Scope, l.Side, l.Lane)
	if err != nil {
		return dal.LaneCount{}, err
	}
	defer rows.Close()
	var c dal.LaneCount
	for rows.Next() {
		var st int
		var n, first int64
		if err := rows.Scan(&st, &n, &first); err != nil {
			return dal.LaneCount{}, err
		}
		switch st {
		case evReady:
			c.Ready, c.Oldest, c.HasOldest = n, float64(first), n > 0
		case evDelayed:
			c.Delayed = n
		case evDoingLater, evDoingNow:
			c.Doing += n
		}
	}
	return c, rows.Err()
}

// With no Redis (step 3): the balance moves and the billing queue in the
// config store, one transaction.
var _ = serves(func(d *dal.Driver) {
	d.BalanceBook = openBillingEvents
	d.BillingQueue = openBillingQueue
})

// openBillingEvents and openBillingQueue open the balance moves and the
// billing queue on the config store's one pool a process (dal.OpenBilling
// asks for them only with no Redis). The main Redis's queues of earlier
// builds (main) are not this database's.
func openBillingEvents(cfg config.Config) (dal.BalanceBook, error) {
	db, err := dbpool.Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return NewBillingEvents(db), nil
}

func openBillingQueue(cfg config.Config, main bool) (dal.BillingQueue, error) {
	if main {
		return nil, dal.ErrNotServed
	}
	db, err := dbpool.Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return NewBillingEvents(db), nil
}
