package mssql

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// The duplicate check on SQL Server (mirrors internal/dal/mysql/dedupe.go):
// dedupe_keys in the config store, shared by every process on that store.

// dedupeNow is the database's clock to the second, as MySQL's NOW(): the
// column is DATETIME2(0), and a time put in it is rounded, not cut.
const dedupeNow = `DATEADD(SECOND, DATEDIFF(SECOND, '20000101', SYSUTCDATETIME()), CAST('20000101' AS DATETIME2(0)))`

var (
	// dedupe.check takes over a row only when it has expired, else leaves
	// it as it was and answers the id it holds (NULL = created: new, or an
	// expired row taken over). One round trip, where MySQL needs a second
	// statement to read the first id of a duplicate.
	stDedupeCheck = CounterStmts.Add("dedupe.check", `SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @yk_now DATETIME2(0) = `+dedupeNow+`;
DECLARE @yk_first VARCHAR(26) = NULL;
BEGIN TRAN;
UPDATE t SET
  @yk_first = CASE WHEN t.expires_at <= @yk_now THEN NULL ELSE t.msg_id END,
  msg_id = CASE WHEN t.expires_at <= @yk_now THEN @msg ELSE t.msg_id END,
  expires_at = CASE WHEN t.expires_at <= @yk_now THEN DATEADD(SECOND, @window, @yk_now) ELSE t.expires_at END
  FROM dbo.dedupe_keys AS t WITH (UPDLOCK, SERIALIZABLE) WHERE t.dkey = @key;
IF @@ROWCOUNT = 0
  INSERT INTO dbo.dedupe_keys (dkey, fingerprint, msg_id, expires_at, pending)
  VALUES (@key, N'', @msg, DATEADD(SECOND, @window, @yk_now), 0);
COMMIT;
SELECT @yk_first;`, true)
	stDedupeRelease = CounterStmts.Add("dedupe.release", `DELETE FROM dbo.dedupe_keys WHERE dkey = @p1 AND msg_id = @p2`, true)
	stDedupeSweep   = CounterStmts.Add("dedupe.sweep",
		`DELETE TOP (5000) FROM dbo.dedupe_keys WHERE expires_at < DATEADD(DAY, -1, SYSUTCDATETIME())`, false)
)

// Dedupe is the duplicate check for an install without Redis (dal.Dedupe):
// an expiry time on each key. `fingerprint` and `pending` are not read: the
// key is the whole check.
type Dedupe struct{ db *sql.DB }

// NewDedupe keeps the duplicate check on the config store's pool.
func NewDedupe(db *sql.DB) *Dedupe { return &Dedupe{db: db} }

// Check creates key holding msgID for window: firstID "" = new, else the id
// of the message it duplicates. The expiry is judged by the database's own
// clock.
func (d *Dedupe) Check(ctx context.Context, key, msgID string, window time.Duration) (string, error) {
	start := time.Now()
	var first sql.NullString
	err := d.db.QueryRowContext(ctx, stDedupeCheck.SQL,
		sql.Named("key", key), sql.Named("msg", Varchar(msgID)), sql.Named("window", int64(window/time.Second)),
	).Scan(&first)
	if err != nil {
		return "", err
	}
	observe(stDedupeCheck, start, 1)
	// msg_id is CHAR(26): SQL Server hands it back padded.
	id := strings.TrimRight(first.String, " ")
	if !first.Valid || id == msgID {
		return "", nil // created, or the same request asked twice
	}
	return id, nil
}

// Release removes the key of a message that was never queued, only while it
// holds that message's id.
func (d *Dedupe) Release(ctx context.Context, key, msgID string) error {
	_, err := counterExec(ctx, d.db, stDedupeRelease, key, Varchar(msgID))
	return err
}

// SweepDedupe deletes up to 5,000 rows expired more than a day ago (an
// expired row is taken over in place by Check regardless — this is
// housekeeping, not a correctness dependency).
func (d *Dedupe) SweepDedupe(ctx context.Context) (int64, error) {
	return counterExec(ctx, d.db, stDedupeSweep)
}
