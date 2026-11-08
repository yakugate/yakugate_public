package mysql

// The outgoing (MT) queues in MySQL — issue 22 of the design audit with issue
// 25's rulings (Saleem and the PO, 23–26 Oct 2026); package queues has the
// rules both stores share. A vendor's queue is its table queue_<cid>; a row
// is in the sub-queue of its short code ('' = common) and ready once pick_at
// has come (queue/0044). A take reads and deletes its rows in one
// transaction: nothing is leased and nothing is acknowledged.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queueflow"
	"github.com/yakugate/yakugate/internal/queues"
	"github.com/yakugate/yakugate/internal/store"
)

// owners are the queue_owner values this process takes: its own deployment,
// then the shared rows (NULL). Only the shared ones when it names none.
func (s *Store) owners() []sql.NullString {
	if s.owner == "" {
		return []sql.NullString{{}}
	}
	return []sql.NullString{{String: s.owner, Valid: true}, {}}
}

// readyCond is a row's "may be taken now".
const readyCond = "pick_at <= NOW(3)"

// subQueue is one (owner, short code) of a vendor's table.
type subQueue struct {
	owner sql.NullString
	sc    string
}

// Claim takes up to n messages of a vendor, its rate split fairly between
// its short codes. [store.Queue]
func (s *Store) Claim(ctx context.Context, vendor string, n int) ([]model.Message, error) {
	return s.ClaimSplit(ctx, vendor, n, queues.Fair)
}

// ClaimSplit takes up to n messages of a vendor from this process's rows:
// how many each (owner, short code) has ready is counted (up to n), n is
// split between them by mode (queues.Split, as the Redis store does), and
// each gives its share in priority order, oldest first. The rows taken are
// deleted in the same transaction, at READ COMMITTED (queuelocks.go), locked
// as they are read so two takers never share one.
func (s *Store) ClaimSplit(ctx context.Context, vendor string, n int, mode queues.Mode) ([]model.Message, error) {
	if n <= 0 {
		return nil, nil
	}
	t := queueTable(vendor)
	var out []model.Message
	err := s.queueTx(ctx, func(tx *sql.Tx) error {
		out = nil
		subs, waits, err := s.countReady(ctx, tx, t, n, mode)
		if err != nil || len(subs) == 0 {
			return err
		}
		share := queues.Split(n, waits, mode, queues.Turn(vendor))
		var ids []int64
		for i, sq := range subs {
			if share[i] <= 0 {
				continue
			}
			start, took := time.Now(), len(out)
			rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT %s FROM %s
				WHERE queue_owner <=> ? AND shortcode = ? AND %s
				ORDER BY priority, pick_at, id LIMIT ? FOR UPDATE SKIP LOCKED`, queueSelectCols, t, readyCond),
				sq.owner, sq.sc, share[i])
			if err != nil {
				return err
			}
			for rows.Next() {
				m, id, err := scanQueueRow(rows, vendor)
				if err != nil {
					rows.Close()
					return err
				}
				out = append(out, m)
				ids = append(ids, id)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			stMTTake.Observe(time.Since(start), int64(len(out)-took))
		}
		return deleteRows(ctx, tx, t, ids)
	})
	if err != nil {
		return nil, err
	}
	queueflow.Out(queueflow.MT, vendor, int64(len(out))) // monitoring: the queue's rate out (D-127)
	return out, nil
}

// countReady is how much each (owner, short code) of a vendor's table has
// ready, counted up to n (by priority too, for the priority split). A table
// not made yet is an empty queue.
func (s *Store) countReady(ctx context.Context, tx *sql.Tx, t string, n int, mode queues.Mode) ([]subQueue, []queues.Waiting, error) {
	subs, err := s.subQueues(ctx, tx, t)
	if err != nil || len(subs) == 0 {
		return nil, nil, err
	}
	var kept []subQueue
	var waits []queues.Waiting
	for _, sq := range subs {
		var w queues.Waiting
		start := time.Now()
		rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT priority, COUNT(*) FROM
			(SELECT priority FROM %s WHERE queue_owner <=> ? AND shortcode = ? AND %s ORDER BY priority LIMIT ?) r
			GROUP BY priority`, t, readyCond), sq.owner, sq.sc, n)
		if err != nil {
			return nil, nil, err
		}
		for rows.Next() {
			var p int
			var c int64
			if err := rows.Scan(&p, &c); err != nil {
				rows.Close()
				return nil, nil, err
			}
			w.Ready += c
			w.ByPrio[queues.Priority(p)] += c
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, nil, err
		}
		stMTReady.Observe(time.Since(start), w.Ready)
		if w.Ready > 0 {
			kept = append(kept, sq)
			waits = append(waits, w)
		}
	}
	return kept, waits, nil
}

// subQueues lists the (owner, short code) pairs of a vendor's table that hold
// rows, in the order a take reads them: this process's own owner first, then
// the shared rows (s.owners()), each owner's short codes in the index's order.
// ONE statement read by idx_pick's first two columns (stMTSubQueues: a dive
// per pair, "Using index for group-by"). It was one DISTINCT a owner, and
// each read every queued row of that owner (2 million index rows for 2,072
// takes in a 3-minute rig run, 30 Oct 2026). A table not made yet is an
// empty queue.
func (s *Store) subQueues(ctx context.Context, tx *sql.Tx, t string) ([]subQueue, error) {
	q, args := strings.Replace(mtSubQueuesShared, "`queue_<cid>`", t, 1), []any(nil)
	if s.owner != "" {
		q, args = strings.Replace(mtSubQueuesOwn, "`queue_<cid>`", t, 1), []any{s.owner}
	}
	start := time.Now()
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		if isNoSuchTable(err) {
			return nil, nil
		}
		return nil, err
	}
	byOwner := map[bool][]subQueue{} // Valid = own rows, else shared
	n := 0
	for rows.Next() {
		var o sql.NullString
		var sc string
		if err := rows.Scan(&o, &sc); err != nil {
			rows.Close()
			return nil, err
		}
		byOwner[o.Valid] = append(byOwner[o.Valid], subQueue{o, sc})
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	stMTSubQueues.Observe(time.Since(start), int64(n))
	var subs []subQueue
	for _, o := range s.owners() {
		subs = append(subs, byOwner[o.Valid]...)
	}
	return subs, nil
}

// deleteRows deletes taken rows by their row id, in tx.
func deleteRows(ctx context.Context, tx *sql.Tx, t string, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	start := time.Now()
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id IN (%s)`, t, placeholders(len(ids))), args...)
	stMTDelete.Observe(time.Since(start), int64(len(ids)))
	return err
}

// Ack is nothing for an outgoing message: a take removed it. [store.Queue]
func (s *Store) Ack(ctx context.Context, vendor string, msgIDs ...string) error { return nil }

// Return puts messages taken but not sent back on their queues, ready at
// once and with the attempt they had: a session that ended before it could
// send them.
func (s *Store) Return(ctx context.Context, ms ...model.Message) error {
	for _, m := range ms {
		m.NextAttemptAt = time.Time{}
		if err := s.Enqueue(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// FetchConcatGroup takes the still-queued parts of one long message, in part
// order (D-036): an HTTP vendor that took one part sends them as one call
// (HTTP has no UDH). They are deleted as a take's rows are.
func (s *Store) FetchConcatGroup(ctx context.Context, vendor string, accountID int64,
	sender, dest string, ref, total int) ([]model.Message, error) {
	t := queueTable(vendor)
	own, ownArgs := s.ownerCond() // the group's parts are all one deployment's (D-121)
	var out []model.Message
	err := s.queueTx(ctx, func(tx *sql.Tx) error {
		out = nil
		rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT %s FROM %s
			WHERE account_id=? AND sender=? AND destination=? AND udh_ref=? AND udh_total=?%s
			ORDER BY udh_seq FOR UPDATE SKIP LOCKED`, queueSelectCols, t, own),
			append([]any{accountID, sender, dest, ref, total}, ownArgs...)...)
		if err != nil {
			if isNoSuchTable(err) {
				return nil
			}
			return err
		}
		var ids []int64
		for rows.Next() {
			m, id, err := scanQueueRow(rows, vendor)
			if err != nil {
				rows.Close()
				return err
			}
			out = append(out, m)
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		return deleteRows(ctx, tx, t, ids)
	})
	if err != nil {
		return nil, err
	}
	queueflow.Out(queueflow.MT, vendor, int64(len(out)))
	return out, nil
}

// MaxAttempts is the most times a vendor's message may be sent (0 = no cap).
func (s *Store) MaxAttempts(vendor string) int {
	s.capMu.RLock()
	defer s.capMu.RUnlock()
	if v, ok := s.capVendor[vendor]; ok && v > 0 {
		return v
	}
	return s.capGlobal
}

// queueStat reads one queue table: this deployment's rows and the shared ones
// (D-121 — another deployment's backlog is not ours to report, nor to steer
// Least busy routing by). Ready may be taken now, Held waits for its pick
// time, Tried has been sent before (-1: not counted, a large queue).
// OldestAgeSec is how long the oldest ready message has been ready, worked
// out by MySQL on its own clock (pick_at is written relative to NOW(3)).
func (s *Store) queueStat(ctx context.Context, t string) (store.VendorQueueStat, error) {
	own, ownArgs := s.ownerCond()
	var st store.VendorQueueStat
	var depth, readyN sql.NullInt64
	if err := s.db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT COUNT(*), SUM(%s) FROM `%s` WHERE 1=1%s", readyCond, t, own), ownArgs...).Scan(&depth, &readyN); err != nil {
		return st, err
	}
	st.Depth, st.Ready = depth.Int64, readyN.Int64
	st.Held = st.Depth - st.Ready
	if st.Ready > 0 {
		var age sql.NullInt64
		if err := s.db.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT TIMESTAMPDIFF(SECOND, MIN(pick_at), NOW(3)) FROM `%s` WHERE %s%s", t, readyCond, own), ownArgs...).Scan(&age); err != nil {
			return st, err
		}
		if age.Valid && age.Int64 > 0 {
			st.OldestAgeSec = age.Int64
		}
	}
	st.Tried = -1
	if st.Depth <= triedReadMax {
		var tried sql.NullInt64
		if err := s.db.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT COUNT(*) FROM `%s` WHERE attempts>0%s", t, own), ownArgs...).Scan(&tried); err != nil {
			return st, err
		}
		st.Tried = tried.Int64
	}
	return st, nil
}

// MTUpgrade gives every queued row written before queue/0044 its pick time:
// its next_attempt_at (a lease's end, a scheduled send) or now. Run once at a
// role's start, under the hand-over lock; a table with none left costs one
// indexed look.
func (s *Store) MTUpgrade(ctx context.Context) (int64, error) {
	tables, err := s.listQueueTables(ctx)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, t := range tables {
		res, err := s.db.ExecContext(ctx, fmt.Sprintf(
			"UPDATE `%s` SET pick_at = COALESCE(next_attempt_at, NOW(3)) WHERE pick_at IS NULL", t))
		if err != nil {
			if isUnknownColumnErr(err) || isNoSuchTable(err) {
				continue // a table not given queue/0044's columns yet: the next start
			}
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
	}
	return total, nil
}

func isUnknownColumnErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Unknown column")
}
