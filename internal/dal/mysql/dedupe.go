package mysql

import (
	"context"
	"database/sql"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// The duplicate check's statements, moved word for word from
// internal/dedupe (mysql.go) on 29 Oct 2026.
var (
	stmtDedupeCheck = counterStmts.Add("dedupe.check", `
		INSERT INTO dedupe_keys (dkey, fingerprint, msg_id, expires_at, pending)
		VALUES (?, '', ?, DATE_ADD(NOW(), INTERVAL ? SECOND), 0)
		ON DUPLICATE KEY UPDATE
		  msg_id = IF(expires_at <= NOW(), VALUES(msg_id), msg_id),
		  expires_at = IF(expires_at <= NOW(), VALUES(expires_at), expires_at)`, true)
	stmtDedupeFirst   = counterStmts.Add("dedupe.first", `SELECT msg_id FROM dedupe_keys WHERE dkey=?`, true)
	stmtDedupeRelease = counterStmts.Add("dedupe.release", `DELETE FROM dedupe_keys WHERE dkey=? AND msg_id=?`, true)
	stmtDedupeSweep   = counterStmts.Add("dedupe.sweep",
		`DELETE FROM dedupe_keys WHERE expires_at < DATE_SUB(NOW(), INTERVAL 1 DAY) LIMIT 5000`, false)
)

// Dedupe is the duplicate check for a gateway without Redis (dal.Dedupe; PO
// 2026-07-27: "in other dbs, it will use an expiry time"), on the config
// store's dedupe_keys (config/0028), shared by every process on that store.
// One statement a new message: an INSERT whose ON DUPLICATE KEY UPDATE takes
// over a row only when it has expired. MySQL answers 1 row (inserted) or 2
// (an expired row taken over) for a new message, 0 (nothing changed) for a
// duplicate; only then is the first id read.
type Dedupe struct{ db *sql.DB }

// NewDedupe keeps the duplicate check on the config store's pool.
func NewDedupe(db *sql.DB) *Dedupe { return &Dedupe{db: db} }

// Check creates key holding msgID for window: firstID "" = new, else the id
// of the message it duplicates. The expiry is judged by MySQL's own clock
// (NOW() on both sides). `fingerprint` (config/0028) and `pending`
// (config/0148) are no longer read: the key is the whole check.
//
// msg_id is assigned before expires_at on purpose: MySQL applies the
// assignments left to right, so the second IF must still see the row's old
// expires_at. Nothing else is assigned, so a live row is left exactly as it
// was and MySQL answers 0 rows (the driver's default, clientFoundRows off).
func (d *Dedupe) Check(ctx context.Context, key, msgID string, window time.Duration) (string, error) {
	start := time.Now()
	res, err := d.db.ExecContext(ctx, stmtDedupeCheck.SQL, key, msgID, int(window/time.Second))
	if err != nil {
		return "", err
	}
	n, err := res.RowsAffected()
	stmtDedupeCheck.Observe(time.Since(start), n)
	if err != nil || n > 0 {
		return "", err
	}
	var first string
	start = time.Now()
	if err := d.db.QueryRowContext(ctx, stmtDedupeFirst.SQL, key).Scan(&first); err != nil {
		return "", err
	}
	stmtDedupeFirst.Observe(time.Since(start), 1)
	if first == msgID {
		return "", nil // the same request asked twice
	}
	return first, nil
}

// Release removes the key of a message that was never queued, only while it
// holds that message's id.
func (d *Dedupe) Release(ctx context.Context, key, msgID string) error {
	start := time.Now()
	res, err := d.db.ExecContext(ctx, stmtDedupeRelease.SQL, key, msgID)
	if err == nil {
		n, _ := res.RowsAffected()
		stmtDedupeRelease.Observe(time.Since(start), n)
	}
	return err
}

// SweepDedupe deletes up to 5,000 rows expired more than a day ago, bounding
// table growth from one-off keys nobody ever looks up again (an expired row
// is taken over in place by Check regardless — this is housekeeping, not a
// correctness dependency).
func (d *Dedupe) SweepDedupe(ctx context.Context) (int64, error) {
	start := time.Now()
	res, err := d.db.ExecContext(ctx, stmtDedupeSweep.SQL)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	stmtDedupeSweep.Observe(time.Since(start), n)
	return n, nil
}

var (
	_ dal.Dedupe        = (*Dedupe)(nil)
	_ dal.DedupeSweeper = (*Dedupe)(nil)
)
