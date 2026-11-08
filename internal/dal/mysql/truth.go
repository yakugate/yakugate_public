package mysql

// Truth-copy support for the Redis-runtime queue model (P1 scope finalize
// 2026-08-09 §18): "MySQL is the durable system of record for EVERYTHING. The
// running/live queues execute on Redis only (hot path); a copy lives on
// MySQL; on every restart Redis is repopulated from MySQL."
//
// These methods are the MySQL half: a table-ensuring enqueue for the
// write-through copy, and the two readers repopulation needs (vendor list +
// full-fidelity pending rows). The Redis half lives in
// internal/store/redis/truth.go; the wiring in internal/store/runtimequeue.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/yakugate/yakugate/internal/model"
)

// isNoSuchTable reports MySQL ER_NO_SUCH_TABLE (1146).
func isNoSuchTable(err error) bool {
	var myErr *gomysql.MySQLError
	return errors.As(err, &myErr) && myErr.Number == 1146
}

// EnqueueTruth writes the durable queue copy, creating the vendor's queue
// table on first use. The plain Enqueue deliberately does NOT auto-create
// (a typo'd vendor cid should fail loudly in MySQL-queue mode), but the truth
// copy must never be the reason an already-routed message is lost.
func (s *Store) EnqueueTruth(ctx context.Context, m model.Message) error {
	err := s.Enqueue(ctx, m)
	if err != nil && isNoSuchTable(err) {
		if e2 := s.ensureCloneTable(ctx, queueTable(m.Vendor), "queue_template"); e2 != nil {
			return err
		}
		return s.Enqueue(ctx, m)
	}
	return err
}

// QueueVendors lists the vendors that have a queue table — repopulation's
// iteration set.
func (s *Store) QueueVendors(ctx context.Context) ([]string, error) {
	tables, err := s.listQueueTables(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		if v := t[len("queue_"):]; v != "" && v != "template" {
			out = append(out, v)
		}
	}
	return out, nil
}

// PendingMessages pages through EVERY row of one vendor's queue table
// (leased or not) in id order, returning full-fidelity messages plus the last
// internal id seen (the next call's afterID). Repopulation uses it after a
// Redis loss: rows whose payload still exists in Redis are skipped NX-style
// on the Redis side, so including leased rows here is safe — and necessary,
// because after a flush the lease state is gone with everything else.
func (s *Store) PendingMessages(ctx context.Context, vendor string, afterID int64, limit int) ([]model.Message, int64, error) {
	return s.pendingMessages(ctx, vendor, afterID, limit, false)
}

// pendingMessages is PendingMessages; allOwners reads every deployment's rows
// too (the hand-over to Redis moves them all, owners kept).
func (s *Store) pendingMessages(ctx context.Context, vendor string, afterID int64, limit int, allOwners bool) ([]model.Message, int64, error) {
	t := queueTable(vendor)
	// The four columns appended here are the ones queueSelectCols does not
	// carry because a CLAIM does not need them — a claimed message is by
	// definition queued and unsent. Repopulation does need them: it is
	// rebuilding rows as they stand, mid-flight leases and all.
	//
	// Everything else is scanned by scanQueueRow, the one reader of
	// queueSelectCols. This function used to repeat that scan by hand, and
	// went stale the moment columns were added to the list — Redis
	// repopulation then failed on every start with an argument-count error,
	// so a flushed Redis was never refilled from MySQL truth.
	// This deployment's rows and the shared ones (D-121): each deployment
	// refills its own queues; another's wait for it.
	own, ownArgs := s.ownerCond()
	if allOwners {
		own, ownArgs = "", nil
	}
	q := fmt.Sprintf(`SELECT %s, COALESCE(vendor_id,0), status, valid_until,
	        COALESCE(TIMESTAMPDIFF(MICROSECOND, NOW(3), pick_at), 0)
	        FROM %s WHERE id > ?%s ORDER BY id LIMIT %d`, queueSelectCols, t, own, limit)
	rows, err := s.db.QueryContext(ctx, q, append([]any{afterID}, ownArgs...)...)
	if err != nil {
		if isNoSuchTable(err) {
			return nil, afterID, nil
		}
		return nil, afterID, err
	}
	defer rows.Close()
	var out []model.Message
	last := afterID
	for rows.Next() {
		var (
			vendorID   sql.NullInt64
			status     sql.NullString
			validUntil sql.NullTime
			wait       int64
		)
		m, id, err := scanQueueRow(rows, vendor, &vendorID, &status, &validUntil, &wait)
		if err != nil {
			return out, last, err
		}
		// scanQueueRow already reproduced the client's receipt preference
		// exactly — a Redis loss recovering to "send everything" would be a
		// silent widening of what the client asked for. [PO 2026-09-03]
		m.VendorID = vendorID.Int64
		m.Status = model.Status(status.String)
		if validUntil.Valid {
			m.ValidUntil = validUntil.Time
		}
		// Its pick time, worked out on the SQL clock (pick_at is written
		// NOW(3)-relative): a later one rides the hand-over as NextAttemptAt.
		if wait > 0 {
			m.NextAttemptAt = time.Now().Add(time.Duration(wait) * time.Microsecond)
		}
		last = id
		out = append(out, m)
	}
	return out, last, rows.Err()
}
