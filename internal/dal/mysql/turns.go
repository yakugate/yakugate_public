package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log"
	"net"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/dal/stmt"
)

// Whose turn it is, without Redis (plan of 29 Oct 2026, step 3;
// config/0168_queue_turns). With Redis a lane of the billing queue or a
// deployment's hand-off list is worked by one process at a time through a
// key SET with a time to live. Here the turn is a row of queue_turns: taken
// when free or run out, renewed while it is the token's, let go by its
// token. Every time is the database's own UTC clock, so the hosts' clocks do
// not matter.
//
// A transaction that may only run while the turn is its own (a claim, the
// end of a batch, a trim) reads the turn row FOR UPDATE first (turnHeld): a
// takeover then waits for that transaction, as a Redis script runs whole.

// TurnStmts is every statement of the turns, by name.
var TurnStmts = stmt.New("mysql")

var (
	// The two assignments run left to right: once token is set, the second
	// one sees the new token, so until_at moves only when the turn is (now)
	// the caller's.
	stTurnTake = TurnStmts.Add("turn.take", `INSERT INTO queue_turns (name, token, until_at)
		VALUES (?, ?, UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND)
		ON DUPLICATE KEY UPDATE
		  token = IF(token = VALUES(token) OR until_at < UTC_TIMESTAMP(6), VALUES(token), token),
		  until_at = IF(token = VALUES(token), VALUES(until_at), until_at)`, false)
	// takeFree is SET NX: taken only when free or run out, never renewed.
	stTurnTakeFree = TurnStmts.Add("turn.take_free", `INSERT INTO queue_turns (name, token, until_at)
		VALUES (?, ?, UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND)
		ON DUPLICATE KEY UPDATE
		  token = IF(until_at < UTC_TIMESTAMP(6), VALUES(token), token),
		  until_at = IF(token = VALUES(token), VALUES(until_at), until_at)`, false)
	stTurnMine = TurnStmts.Add("turn.mine",
		"SELECT token = ? AND until_at >= UTC_TIMESTAMP(6) FROM queue_turns WHERE name = ?", false)
	stTurnHeld = TurnStmts.Add("turn.held",
		"SELECT 1 FROM queue_turns WHERE name = ? AND token = ? AND until_at >= UTC_TIMESTAMP(6) FOR UPDATE", false)
	stTurnDrop = TurnStmts.Add("turn.drop", "DELETE FROM queue_turns WHERE name = ? AND token = ?", false)
)

// turns is the turns table on one pool (the config store).
type turns struct{ db *sql.DB }

// take takes the turn name for token when it is free, run out or already
// token's, for ttl; free = only when free or run out (SET NX). It answers
// whether the turn is token's now: two statements (the upsert cannot say it
// whatever the DSN's row-count setting).
func (t turns) take(ctx context.Context, name, token string, ttl time.Duration, free bool) (bool, error) {
	q := stTurnTake.SQL
	if free {
		q = stTurnTakeFree.SQL
	}
	if _, err := t.db.ExecContext(ctx, q, name, token, ttl.Microseconds()); err != nil {
		return false, err
	}
	var mine bool
	if err := t.db.QueryRowContext(ctx, stTurnMine.SQL, token, name).Scan(&mine); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return mine, nil
}

// drop lets the turn go when it is token's.
func (t turns) drop(ctx context.Context, name, token string) error {
	_, err := t.db.ExecContext(ctx, stTurnDrop.SQL, name, token)
	return err
}

// turnHeld says, inside tx, that the turn is token's, and holds its row
// until tx ends.
func turnHeld(ctx context.Context, tx *sql.Tx, name, token string) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx, stTurnHeld.SQL, name, token).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// ---- transactions that stand in for one Redis script ------------------------

// notCommitted marks an error met before a transaction's commit: it was
// rolled back, so nothing it did is there and doing it again cannot do it
// twice.
type notCommitted struct{ err error }

func (e *notCommitted) Error() string { return e.err.Error() }
func (e *notCommitted) Unwrap() error { return e.err }

// unsentErr says a failed call certainly changed nothing: an error before
// its commit, an answer of the server itself (it refused the statement), a
// connection the driver had not sent on, or a dial that failed.
func unsentErr(err error) bool {
	if err == nil {
		return false
	}
	var nc *notCommitted
	if errors.As(err, &nc) {
		return true
	}
	var me *mysqldrv.MySQLError
	if errors.As(err, &me) {
		return true
	}
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// rcTx runs fn in one READ COMMITTED transaction on db (as the queue tables'
// claims do: no gap locks, a row read and passed over is let go at once),
// again when MySQL makes it a lock victim, at most tries runs. An error
// before the commit comes back as a notCommitted. fn may run more than once:
// it must start from nothing each time.
func rcTx(ctx context.Context, db *sql.DB, tries int, fn func(tx *sql.Tx) error) error {
	run := func() error {
		return againIfVictimN(ctx, tries, func() error {
			opts := queueTxOptions
			if readCommittedRefused.Load() {
				opts = nil
			}
			tx, err := db.BeginTx(ctx, opts)
			if err != nil {
				return &notCommitted{err}
			}
			defer tx.Rollback() //nolint:errcheck // no-op after Commit
			if err := fn(tx); err != nil {
				return &notCommitted{err}
			}
			return tx.Commit()
		})
	}
	err := run()
	if isReadCommittedRefused(err) && readCommittedRefused.CompareAndSwap(false, true) {
		log.Printf("[store] the server will not write at READ COMMITTED (%v): queue transactions run at its own isolation level", err)
		err = run()
	}
	return err
}
