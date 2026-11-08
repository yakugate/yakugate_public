package mssql

import (
	"context"
	"crypto/md5"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The billing queue and the balance moves on SQL Server, for an install with
// no Redis. It mirrors internal/dal/mysql/billing_events.go (and the lane
// turns of internal/dal/mysql/turns.go): the table billing_events, a row an
// event's half in one of four states (0 ready, 1 delayed, 2 and 3 doing —
// claimed from delayed, claimed from ready), the turns in queue_turns, and
// the balance moves of a process committed together (group commit: two
// writers, at most 256 moves a transaction).
//
// Written for SQL Server:
//
//   - The companies' rows are locked in id order with one statement: the
//     ids, sorted, as a JSON array, each row sought and locked
//     WITH (UPDLOCK, ROWLOCK) in the array's order (FORCE ORDER), as MySQL's
//     SELECT … ORDER BY id FOR UPDATE.
//   - The halves of a batch go as one JSON parameter (OPENJSON): a batch of
//     256 moves passes SQL Server's 2,100 parameters, and the schema has no
//     table type for a TVP. An event put twice is stored once: the INSERT
//     leaves out a half whose (scope, side, mhash) is there, the batch's own
//     repeats are taken out before, and two writers that put one new event
//     at once meet the unique key (2601): the transaction is run again and
//     the second finds it there.
//   - Deadlocks (1205) and lock waits are run again where MySQL ran its
//     lock victims again.

// BillingEvents is the billing queue and the balance moves on the config
// store's pool.
type BillingEvents struct {
	db *sql.DB

	// The writers that commit the moves of this process together (Move).
	once sync.Once
	in   chan *moveReq
}

// NewBillingEvents keeps the billing queue on db (the config store).
func NewBillingEvents(db *sql.DB) *BillingEvents {
	if db == nil {
		return nil
	}
	return &BillingEvents{db: db}
}

var (
	_ dal.BillingQueue = (*BillingEvents)(nil)
	_ dal.BalanceBook  = (*BillingEvents)(nil)
)

// The states of a row.
const (
	evReady      = 0
	evDelayed    = 1
	evDoingLater = 2 // claimed from delayed
	evDoingNow   = 3 // claimed from ready
)

// How often a write is tried when it meets a deadlock, a lock wait or a
// repeat put by another writer: the accept path's (MySQL enqueueTries) and
// the db-worker's.
const (
	billEnqueueTries = 4
	billWorkerTries  = 8
)

// evJSON is the halves' JSON as the statements read them: c scope, s side,
// l lane, o score, h the member's MD5 (hex), m the member.
const evJSON = `WITH (c NVARCHAR(4000) '$.c', s NVARCHAR(4000) '$.s', l INT '$.l', o BIGINT '$.o', h VARCHAR(32) '$.h', m NVARCHAR(MAX) '$.m')`

var (
	// The accept path and every later moment: one INSERT of every half. The
	// text columns are read wide, so a value too long for its column is
	// refused (2628), never cut.
	stEvPut = BillingStmts.Add("billing.events_put", `INSERT INTO billing_events (scope, side, lane, state, score, mhash, member)
		SELECT j.c, j.s, j.l, 0, j.o, CONVERT(BINARY(16), j.h, 2), j.m FROM OPENJSON(@rows) `+evJSON+` AS j
		 WHERE NOT EXISTS (SELECT 1 FROM billing_events AS t
		                    WHERE t.scope = j.c AND t.side = j.s AND t.mhash = CONVERT(BINARY(16), j.h, 2))`, true)
	// The companies' rows locked in id order (@ids sorted), with their
	// balance in ledger units.
	stEvLock = BillingStmts.Add("billing.events_lock", `SELECT c.id, CAST(c.balance * 10000 AS BIGINT)
		FROM OPENJSON(@ids) WITH (id BIGINT '$') AS j
		CROSS APPLY (SELECT x.id, x.balance FROM companies AS x WITH (UPDLOCK, ROWLOCK, FORCESEEK) WHERE x.id = j.id) AS c
		OPTION (FORCE ORDER)`, true)
	// The balance moves of a batch (@moves: id, units as exact decimal
	// text) and its halves, in one round trip. updated_at is left as it is:
	// a balance move is not a change of the company's settings, which the
	// reference cache watches.
	stEvMove = BillingStmts.Add("billing.events_move", `SET NOCOUNT ON; SET XACT_ABORT ON;
		UPDATE c SET balance = c.balance - j.u
		  FROM OPENJSON(@moves) WITH (i BIGINT '$.i', u DECIMAL(18,4) '$.u') AS j
		 INNER LOOP JOIN companies AS c WITH (ROWLOCK, FORCESEEK) ON c.id = j.i
		OPTION (FORCE ORDER);
		`+stEvPutText, true)
	// MoveMany's round trip: every half, then the moves, answering each
	// company's balance after (ledger units).
	stEvMoveMany = BillingStmts.Add("billing.events_move_many", `SET NOCOUNT ON; SET XACT_ABORT ON;
		`+stEvPutText+`;
		UPDATE c SET balance = c.balance - j.u
		OUTPUT inserted.id, CAST(inserted.balance * 10000 AS BIGINT)
		  FROM OPENJSON(@moves) WITH (i BIGINT '$.i', u DECIMAL(18,4) '$.u') AS j
		 INNER LOOP JOIN companies AS c WITH (ROWLOCK, FORCESEEK) ON c.id = j.i
		OPTION (FORCE ORDER);`, true)

	// The db-worker's side. A claim and a done hold the lane's turn row
	// (UPDLOCK) for their transaction, so a takeover waits for them.
	stEvClaim = BillingStmts.Add("billing.events_claim", `SET NOCOUNT ON;
DECLARE @mine INT = 0, @repeat INT = 0, @got INT = 0;
IF EXISTS (SELECT 1 FROM queue_turns WITH (UPDLOCK, ROWLOCK) WHERE name = @turn AND token = @token AND until_at >= SYSUTCDATETIME())
  SET @mine = 1;
IF @mine = 1 AND EXISTS (SELECT 1 FROM billing_events WHERE scope = @scope AND side = @side AND lane = @lane AND state IN (2, 3))
  SET @repeat = 1;
IF @mine = 1 AND @repeat = 0 AND @max > 0
BEGIN
  ;WITH d AS (SELECT TOP (@max) state, batch_id FROM billing_events WITH (ROWLOCK, UPDLOCK, READPAST)
     WHERE scope = @scope AND side = @side AND lane = @lane AND state = 1 AND score <= @now ORDER BY score, id)
  UPDATE d SET state = 2, batch_id = @id;
  SET @got = @@ROWCOUNT;
  IF @got < @max
  BEGIN
    ;WITH r AS (SELECT TOP (@max - @got) state, batch_id FROM billing_events WITH (ROWLOCK, UPDLOCK, READPAST)
       WHERE scope = @scope AND side = @side AND lane = @lane AND state = 0 ORDER BY score, id)
    UPDATE r SET state = 3, batch_id = @id;
  END
END
SELECT @repeat, COALESCE(batch_id, N''), member FROM billing_events
 WHERE @mine = 1 AND scope = @scope AND side = @side AND lane = @lane AND state IN (2, 3)
 ORDER BY state, score, id;`, false)
	stEvSetBatch = BillingStmts.Add("billing.events_set_batch", `UPDATE billing_events SET batch_id = @p1
		WHERE scope = @p2 AND side = @p3 AND lane = @p4 AND state IN (2, 3)`, false)
	// Done: the doing rows deleted and those booked later written as
	// delayed (a later half that is there already, ready on any lane of its
	// side, is made delayed with its new score, as MySQL's ON DUPLICATE KEY
	// UPDATE did). Answers 1, or 0 when the turn was lost (nothing changed).
	stEvDone = BillingStmts.Add("billing.events_done", `SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @mine INT = 0;
IF EXISTS (SELECT 1 FROM queue_turns WITH (UPDLOCK, ROWLOCK) WHERE name = @turn AND token = @token AND until_at >= SYSUTCDATETIME())
  SET @mine = 1;
IF @mine = 1
BEGIN
  DELETE FROM billing_events WHERE scope = @scope AND side = @side AND lane = @lane AND state IN (2, 3);
  UPDATE t SET state = 1, score = j.o, batch_id = NULL
    FROM billing_events AS t INNER JOIN OPENJSON(@later) `+evJSON+` AS j
      ON t.scope = @scope AND t.side = @side AND t.mhash = CONVERT(BINARY(16), j.h, 2);
  INSERT INTO billing_events (scope, side, lane, state, score, mhash, member)
  SELECT @scope, @side, @lane, 1, j.o, CONVERT(BINARY(16), j.h, 2), j.m FROM OPENJSON(@later) `+evJSON+` AS j
   WHERE NOT EXISTS (SELECT 1 FROM billing_events AS t
                      WHERE t.scope = @scope AND t.side = @side AND t.mhash = CONVERT(BINARY(16), j.h, 2));
END
SELECT @mine;`, false)
	stEvLanes = BillingStmts.Add("billing.events_lanes",
		"SELECT DISTINCT TOP (1024) lane FROM billing_events WHERE scope = @p1 AND side = @p2", false)
	stEvAny = BillingStmts.Add("billing.events_any",
		"SELECT TOP (1) 1 FROM billing_events WHERE scope = @p1 AND side = @p2 AND lane = @p3", false)
	stEvCount = BillingStmts.Add("billing.events_count", `SELECT state, COUNT_BIG(*), COALESCE(MIN(score), 0) FROM billing_events
		WHERE scope = @p1 AND side = @p2 AND lane = @p3 GROUP BY state`, false)
	stEvWaiting = BillingStmts.Add("billing.events_waiting", `SELECT COUNT_BIG(*), MIN(score) FROM billing_events
		WHERE scope = @p1 AND state = 0 AND side IN (@p2)`, false)

	// A lane's turn (MySQL turns.go): taken when free or run out, renewed
	// while it is the token's; answers whether it is the token's now. One
	// round trip: SQL Server's SET sees every column as it was, so both
	// assignments test the same condition.
	stBillTurnTake = BillingStmts.Add("billing.turn_take", `SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @until DATETIME2(6) = DATEADD(MICROSECOND, CAST(@us % 1000000 AS INT), DATEADD(SECOND, CAST(@us / 1000000 AS INT), SYSUTCDATETIME()));
BEGIN TRAN;
UPDATE queue_turns WITH (UPDLOCK, SERIALIZABLE)
   SET token = CASE WHEN token = @token OR until_at < SYSUTCDATETIME() THEN @token ELSE token END,
       until_at = CASE WHEN token = @token OR until_at < SYSUTCDATETIME() THEN @until ELSE until_at END
 WHERE name = @name;
IF @@ROWCOUNT = 0 INSERT INTO queue_turns (name, token, until_at) VALUES (@name, @token, @until);
COMMIT;
SELECT CASE WHEN token = @token AND until_at >= SYSUTCDATETIME() THEN 1 ELSE 0 END FROM queue_turns WHERE name = @name;`, false)
	stBillTurnDrop = BillingStmts.Add("billing.turn_drop", "DELETE FROM queue_turns WHERE name = @p1 AND token = @p2", false)
)

// stEvPutText is the put's INSERT, for the batches that carry it.
var stEvPutText = stEvPut.SQL

// evRow is one half to write.
type evRow struct {
	scope, side string
	lane        int
	score       int64
	member      string
}

// evRowJSON is one half as evJSON reads it.
type evRowJSON struct {
	C string `json:"c"`
	S string `json:"s"`
	L int    `json:"l"`
	O int64  `json:"o"`
	H string `json:"h"`
	M string `json:"m"`
}

// eventRowsOf is the halves of adds, in order.
func eventRowsOf(adds []dal.LaneAdd) []evRow {
	var out []evRow
	for _, a := range adds {
		for _, e := range a.Events {
			out = append(out, evRow{scope: a.Scope, side: a.Side, lane: a.Lane.Lane, score: int64(e.Score), member: e.Member})
		}
	}
	return out
}

// evRowsJSON is rows as one JSON array, each event once: a repeat of a
// (scope, side, member) keeps the first one's place; last says the repeat's
// score wins (Done's later), else the first's (a put).
func evRowsJSON(rows []evRow, last bool) (string, error) {
	type key struct {
		scope, side string
		h           [16]byte
	}
	at := make(map[key]int, len(rows))
	out := make([]evRowJSON, 0, len(rows))
	for _, r := range rows {
		h := md5.Sum([]byte(r.member))
		k := key{r.scope, r.side, h}
		if i, seen := at[k]; seen {
			if last {
				out[i].O = r.score
			}
			continue
		}
		at[k] = len(out)
		out = append(out, evRowJSON{C: r.scope, S: r.side, L: r.lane, O: r.score, H: hex.EncodeToString(h[:]), M: r.member})
	}
	b, err := json.Marshal(out)
	return string(b), err
}

// putRows writes rows as ready events (a row there already stays as it is).
func putRows(ctx context.Context, q Querier, rows []evRow) error {
	if len(rows) == 0 {
		return nil
	}
	body, err := evRowsJSON(rows, false)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, stEvPut.SQL, sql.Named("rows", body))
	return err
}

// ---- dal.BalanceBook --------------------------------------------------------

// errNoCompany: a hold on a company that is not there.
var errNoCompany = errors.New("no such company")

// Move moves a company's balance and writes the events in ONE transaction,
// both or neither: refused (ok=false, nothing moved, nothing written) when
// refuse says so and the balance does not cover units.
//
// The moves of a process are COMMITTED TOGETHER (group commit), as on MySQL:
// a call is handed to one of moveWriters writers, which takes it with every
// call waiting behind it (at most moveBatch) and books them in one
// transaction — the companies' rows locked in id order, each call judged in
// the order it came against the running balance, one UPDATE for every
// company and one INSERT of every half (one round trip), one commit. A call
// that comes alone is booked at once.
//
// The caller waits for its answer even when its own context ends: the batch
// may already have committed its hold, and the caller then gives it back.
// The batch runs on a context of its own, bounded by moveFor.
//
// A company that is not there: refused with an error at a hold (refuse); at
// a later moment its events are still written and nothing moves.
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

// moveJSON is one company's net move: id, units as exact decimal text.
type moveJSON struct {
	I int64  `json:"i"`
	U string `json:"u"`
}

func idsJSON(ids []int64) string {
	b, _ := json.Marshal(ids)
	return string(b)
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
		if err := billAgain(ctx, billEnqueueTries, func() error { return putRows(ctx, b.db, rows) }); err != nil {
			return ans, err
		}
		return ans, nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	err := billTx(ctx, b.db, billEnqueueTries, func(tx *sql.Tx) error {
		rs, err := tx.QueryContext(ctx, stEvLock.SQL, sql.Named("ids", idsJSON(ids)))
		if err != nil {
			return err
		}
		bal := map[int64]int64{}
		for rs.Next() {
			var id, v int64
			if err := rs.Scan(&id, &v); err != nil {
				rs.Close()
				return err
			}
			bal[id] = v
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
				ans[i].err = &billNotCommitted{fmt.Errorf("company %d: %w", r.company, errNoCompany)}
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
		moves := make([]moveJSON, 0, len(ids))
		for _, id := range ids {
			if net[id] != 0 {
				moves = append(moves, moveJSON{I: id, U: unitsStr(net[id])})
			}
		}
		if len(moves) == 0 {
			return putRows(ctx, tx, rows)
		}
		mb, err := json.Marshal(moves)
		if err != nil {
			return err
		}
		evs, err := evRowsJSON(rows, false)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, stEvMove.SQL, sql.Named("moves", string(mb)), sql.Named("rows", evs))
		return err
	})
	return ans, err
}

// MoveMany moves many companies' balances and writes every event in ONE
// transaction: one round trip writes every half, then moves every company
// (their rows locked in id order, so two bookers never wait on each other in
// a circle) and answers their balances after, committed. Each answer is the
// balance after its own move, in the order asked. Never refused: these are
// later moments.
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
		err = billAgain(ctx, billWorkerTries, func() error { return putRows(ctx, b.db, rows) })
	} else {
		mv := make([]moveJSON, len(ids))
		for i, id := range ids {
			mv[i] = moveJSON{I: id, U: unitsStr(net[id])}
		}
		mb, _ := json.Marshal(mv)
		evs, jerr := evRowsJSON(rows, false)
		if jerr != nil {
			return nil, jerr
		}
		err = billTx(ctx, b.db, billWorkerTries, func(tx *sql.Tx) error {
			clear(bal)
			rs, err := tx.QueryContext(ctx, stEvMoveMany.SQL, sql.Named("rows", evs), sql.Named("moves", string(mb)))
			if err != nil {
				return err
			}
			defer rs.Close()
			for rs.Next() {
				var id, v int64
				if err := rs.Scan(&id, &v); err != nil {
					return err
				}
				bal[id] = v + net[id] // the balance before the moves
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

// Unsent says the call certainly changed nothing (billUnsent).
func (b *BillingEvents) Unsent(err error) bool { return billUnsent(err) }

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
	return billAgain(ctx, billEnqueueTries, func() error { return putRows(ctx, b.db, rows) })
}

// Waiting counts the ready events of the sides of scope, every lane, and
// how long the oldest has waited (ms).
func (b *BillingEvents) Waiting(ctx context.Context, scope string, sides []string, _ int) (int64, int64, error) {
	if len(sides) == 0 {
		return 0, 0, nil
	}
	args := []any{scope}
	marks := make([]string, len(sides))
	for i, s := range sides {
		args = append(args, s)
		marks[i] = "@p" + strconv.Itoa(i+2)
	}
	q := strings.Replace(stEvWaiting.SQL, "IN (@p2)", "IN ("+strings.Join(marks, ", ")+")", 1)
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
	var mine int
	err := b.db.QueryRowContext(ctx, stBillTurnTake.SQL, sql.Named("name", turnOf(l)), sql.Named("token", token),
		sql.Named("us", ttl.Microseconds())).Scan(&mine)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return mine == 1, err
}

// DropTurn lets a lane's turn go when it is token's.
func (b *BillingEvents) DropTurn(ctx context.Context, l dal.Lane, token string) error {
	_, err := b.db.ExecContext(ctx, stBillTurnDrop.SQL, turnOf(l), token)
	return err
}

// laneArgs is a lane's named parameters.
func laneArgs(l dal.Lane, token string) []any {
	return []any{sql.Named("turn", turnOf(l)), sql.Named("token", token),
		sql.Named("scope", l.Scope), sql.Named("side", l.Side), sql.Named("lane", l.Lane)}
}

// Claim gives the batch to book while the turn is token's, in one
// transaction and one round trip: the batch left in doing (repeat, whole
// whatever max says), else up to max events — the delayed ones whose time
// has come first, then the oldest ready — marked doing under newID.
// Answers {id, "1"|"0", member…}, or nothing.
func (b *BillingEvents) Claim(ctx context.Context, l dal.Lane, token string, max int, newID string, nowMS int64) ([]string, error) {
	var out []string
	err := billTx(ctx, b.db, billWorkerTries, func(tx *sql.Tx) error {
		out = nil
		args := append(laneArgs(l, token), sql.Named("max", max), sql.Named("id", newID), sql.Named("now", nowMS))
		rows, err := tx.QueryContext(ctx, stEvClaim.SQL, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		var ms []string
		id, repeat := "", 0
		for rows.Next() {
			var bid, m string
			if err := rows.Scan(&repeat, &bid, &m); err != nil {
				return err
			}
			if id == "" {
				id = bid
			}
			ms = append(ms, m)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		switch {
		case len(ms) == 0:
		case repeat == 1:
			out = append([]string{id, "1"}, ms...)
		default:
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
	rows := make([]evRow, len(later))
	for i, s := range later {
		rows[i] = evRow{scope: l.Scope, side: l.Side, lane: l.Lane, score: int64(s.Score), member: s.Member}
	}
	lj, err := evRowsJSON(rows, true) // "[]": nothing booked later
	if err != nil {
		return 0, err
	}
	done := 0
	err = billTx(ctx, b.db, billWorkerTries, func(tx *sql.Tx) error {
		done = 0
		args := append(laneArgs(l, token), sql.Named("later", lj))
		return tx.QueryRowContext(ctx, stEvDone.SQL, args...).Scan(&done)
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

// openBillingEvents and openBillingQueue open the balance moves and the
// billing queue on the config store's one pool a process (dal.OpenBilling
// asks for them only with no Redis). The main Redis's queues of earlier
// builds (main) are not this database's.
func openBillingEvents(cfg config.Config) (dal.BalanceBook, error) {
	db, err := Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return NewBillingEvents(db), nil
}

func openBillingQueue(cfg config.Config, main bool) (dal.BillingQueue, error) {
	if main {
		return nil, dal.ErrNotServed
	}
	db, err := Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return NewBillingEvents(db), nil
}

// ---- transactions that stand in for one Redis script ------------------------

// billNotCommitted marks an error met before a transaction's commit: it was
// rolled back, so nothing it did is there and doing it again cannot do it
// twice.
type billNotCommitted struct{ err error }

func (e *billNotCommitted) Error() string { return e.err.Error() }
func (e *billNotCommitted) Unwrap() error { return e.err }

// billUnsent says a failed call certainly changed nothing: an error before
// its commit, an answer of the server itself (it refused the statement), a
// connection the driver had not sent on, or a dial that failed.
func billUnsent(err error) bool {
	if err == nil {
		return false
	}
	var nc *billNotCommitted
	if errors.As(err, &nc) {
		return true
	}
	if _, ok := ErrorNumber(err); ok {
		return true
	}
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// billTx runs fn in one transaction on db (READ COMMITTED, which is
// snapshot reads here; the rows a statement must see as they stand it reads
// under UPDLOCK), again when it meets a deadlock, a lock wait or another
// writer's repeat of an event, at most tries runs. An error before the
// commit comes back as a billNotCommitted. fn may run more than once: it
// must start from nothing each time.
func billTx(ctx context.Context, db *sql.DB, tries int, fn func(tx *sql.Tx) error) error {
	return billAgain(ctx, tries, func() error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return &billNotCommitted{err}
		}
		defer tx.Rollback() //nolint:errcheck // no-op after Commit
		if err := fn(tx); err != nil {
			return &billNotCommitted{err}
		}
		return tx.Commit()
	})
}

// billAgain is Again (helpers.go), also running again a write that met
// another writer's repeat of one of its events (2601): the second run finds
// it there and leaves it out.
func billAgain(ctx context.Context, tries int, f func() error) error {
	var err error
	for i := 1; ; i++ {
		if err = f(); err == nil || !(IsRetryable(err) || IsDuplicate(err)) || i >= tries {
			return err
		}
		wait := time.Duration(15*i)*time.Millisecond + time.Duration(rand.IntN(20))*time.Millisecond
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
	}
}
