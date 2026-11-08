package mysql

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

// How the queue tables are locked (go-live retest of 19 Oct 2026).
//
// With the queues in MySQL the accept INSERT waited on the vendor roles'
// claims, and the two deadlocked. Read on rig 62 from MySQL's own lock
// tables while 200 messages a second went through (the old code):
//
//   - A claim reads its queue with SELECT … FOR UPDATE SKIP LOCKED. On a
//     queue of a few dozen rows MySQL reads the whole table (it is cheaper
//     than the index) and sorts it. Under REPEATABLE READ every row it reads
//     is locked until the claim commits — the messages it does not take too,
//     including the ones out with a vendor — and so is the gap at the end of
//     the table. Every accept INSERT lands in that gap, so it waited for
//     whichever claim was running (124 of 220 waits seen in one run; the
//     queue pools of http-server and smpp-server waited thousands of times a
//     run).
//   - Opposite orders, one each way: a receipt INSERT (client_dlr) holds its
//     new row and waits for the end-of-index gap the claim holds; the claim's
//     lease UPDATE reads on into the new row and waits for it. An ack DELETE
//     holds the rows it has deleted and waits for one the claim read; the
//     claim waits for one the DELETE holds. Two claims' lease UPDATEs each
//     wait for a gap of idx_claim the other holds. MySQL breaks each cycle
//     by refusing one statement (1213), and that was the client's
//     QUEUE_UNAVAILABLE, the dlr's "refused a client receipt (Deadlock)" and
//     smpp-vendor's "refused an ack (Deadlock)".
//
// So a claim (and every other transaction that takes queue rows: the MO and
// receipt claims, the expiry and retry sweeps, the re-route mover) runs at
// READ COMMITTED: it
// locks only the rows it takes, a row it reads and passes over is let go at
// once, no gap is locked, and an UPDATE does not wait for a row someone else
// is still inserting (it reads the last committed version, which a new row
// does not have). SKIP LOCKED still keeps two claims apart, and a locking
// read sees the latest committed row at any level, so nothing is handed out
// twice.
//
// And a statement MySQL refuses as a deadlock or a lock wait victim is run
// again (againIfVictim, a little later each time): it was rolled back whole,
// so running it again is the same write.

// queueTxOptions is the isolation of a transaction that takes queue rows.
var queueTxOptions = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

// readCommittedRefused is set when the server will not write at READ
// COMMITTED: binlog_format = STATEMENT refuses it (1665). Every queue
// transaction of the process then runs at the server's own level, as before.
var readCommittedRefused atomic.Bool

// enqueueTries bounds the tries of a write a client or a vendor is waiting
// on (an accept, a receipt to queue): three waits of 15, 30 and 45 ms plus
// up to 20 ms each.
const enqueueTries = 4

// beginQueueTx starts a transaction that takes queue rows.
func (s *Store) beginQueueTx(ctx context.Context) (*sql.Tx, error) {
	if readCommittedRefused.Load() {
		return s.db.BeginTx(ctx, nil)
	}
	return s.db.BeginTx(ctx, queueTxOptions)
}

// queueTx runs fn in one transaction that takes queue rows (beginQueueTx),
// again when MySQL makes it a lock victim. fn may run more than once: it must
// start from nothing each time.
func (s *Store) queueTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	run := func() error {
		return againIfVictim(ctx, func() error {
			tx, err := s.beginQueueTx(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback() //nolint:errcheck
			if err := fn(tx); err != nil {
				return err
			}
			return tx.Commit()
		})
	}
	err := run()
	if isReadCommittedRefused(err) && readCommittedRefused.CompareAndSwap(false, true) {
		log.Printf("[store] the server will not write at READ COMMITTED (%v): queue claims run at its own isolation level, and may wait on one another", err)
		err = run()
	}
	return err
}

// isReadCommittedRefused: binlog_format = STATEMENT does not take a write at
// READ COMMITTED (1665, ER_BINLOG_STMT_MODE_AND_ROW_ENGINE).
func isReadCommittedRefused(err error) bool {
	if err == nil {
		return false
	}
	m := err.Error()
	return strings.Contains(m, "Error 1665") || strings.Contains(m, "BINLOG_FORMAT = STATEMENT")
}

// againIfVictimN is againIfVictim with at most tries runs.
func againIfVictimN(ctx context.Context, tries int, write func() error) error {
	var err error
	for attempt := 0; attempt < tries; attempt++ {
		if err = write(); !isLockVictim(err) {
			return err
		}
		if attempt == tries-1 {
			break
		}
		wait := time.Duration(15*(attempt+1))*time.Millisecond + time.Duration(time.Now().UnixNano()%int64(20*time.Millisecond))
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
	}
	return err
}

// writeOne runs one statement on a queue table on its own (autocommit, one
// round trip), again when MySQL makes it a lock victim — at most enqueueTries
// runs, since a client or a vendor is waiting on it. Only for an INSERT (it
// locks no gap, only its own new rows) or a change of ONE row found by its
// primary key (it locks that row only); anything that reads by another key or
// a list goes through execQueue.
func (s *Store) writeOne(ctx context.Context, q string, args ...any) (sql.Result, error) {
	var res sql.Result
	err := againIfVictimN(ctx, enqueueTries, func() error {
		var err error
		res, err = s.db.ExecContext(ctx, q, args...)
		return err
	})
	return res, err
}

// execQueue runs one UPDATE or DELETE on a queue table in a transaction of
// its own at READ COMMITTED (queueTx), again when MySQL makes it a lock
// victim. On its own at REPEATABLE READ an ack (`msg_id IN (…)`, which MySQL
// may read as a scan of a small table) held every row it read and the gap
// at the end until it finished: accept INSERTs waited on acks too (45 of 190
// waits on rig 62 once the claims ran at READ COMMITTED), and two acks
// deadlocked on each other's rows.
func (s *Store) execQueue(ctx context.Context, q string, args ...any) (sql.Result, error) {
	var res sql.Result
	err := s.queueTx(ctx, func(tx *sql.Tx) error {
		var err error
		res, err = tx.ExecContext(ctx, q, args...)
		return err
	})
	return res, err
}
