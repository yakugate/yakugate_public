package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// Long messages' parts (the queue database).
var _ = serves(func(d *dal.Driver) { d.ConcatBuffer = openConcatBuffer })

// openConcatBuffer is the buffer's opener: concat_assembly in the queue
// database, on its one pool a process, when the queue DSN is a MySQL one.
func openConcatBuffer(cfg config.Config, mo bool) (dal.ConcatBuffer, error) {
	if !cfg.QueueStoreMySQL() {
		return nil, fmt.Errorf("no MySQL queue database (YAKU_QUEUE_DSN) to keep concat_assembly in")
	}
	db, err := dbpool.Get(cfg.QueueDSN)
	if err != nil {
		return nil, err
	}
	return NewConcatBuffer(db, mo), nil
}

// The multi-part buffer in the queue database's concat_assembly (internal/
// concat's Assembler; moved here unchanged on 29 Oct 2026, the DAL plan).
// MT and MO groups share the table; an MO group's key starts "mo|".

var (
	stConcatInsert = TrafficStmts.Add("concat.insert", `
		INSERT INTO concat_assembly (account_id, group_key, seq, total, payload)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE received_at = received_at`, true)
	stConcatGroup = TrafficStmts.Add("concat.group",
		`SELECT payload FROM concat_assembly WHERE group_key = ? ORDER BY seq FOR UPDATE`, true)
	stConcatDelete  = TrafficStmts.Add("concat.delete", `DELETE FROM concat_assembly WHERE group_key = ?`, true)
	stConcatExpired = TrafficStmts.Add("concat.expired_mt", concatExpiredSQL("group_key NOT LIKE 'mo|%'"), false)
	stConcatMOExp   = TrafficStmts.Add("concat.expired_mo", concatExpiredSQL("group_key LIKE 'mo|%'"), false)
)

func concatExpiredSQL(kind string) string {
	return `
		SELECT group_key FROM concat_assembly WHERE ` + kind + `
		GROUP BY group_key HAVING MIN(received_at) < (NOW(3) - INTERVAL ? SECOND)
		LIMIT 100`
}

// ConcatBuffer buffers one side's parts in concat_assembly. Instance-safe:
// a group is claimed under row locks and deleted before it is returned.
type ConcatBuffer struct {
	db *sql.DB
	mo bool
}

var _ dal.ConcatBuffer = (*ConcatBuffer)(nil)

// NewConcatBuffer keeps one side's parts on db (the queue database).
func NewConcatBuffer(db *sql.DB, mo bool) *ConcatBuffer { return &ConcatBuffer{db: db, mo: mo} }

// AddPart durably buffers one part; a retransmitted duplicate part is
// idempotent (unique group+seq). The completion check runs under row locks:
// of two racing final-part arrivals, one blocks on the other's locks and
// finds the rows already deleted.
func (a *ConcatBuffer) AddPart(ctx context.Context, key string, accountID int64, seq, total int, payload string, _ time.Duration) ([]*model.Message, error) {
	// The payload as text: bytes are sent as a binary string once the driver
	// fills the arguments in (dbpool.Interpolate), and MySQL refuses that for
	// the JSON column.
	if _, err := a.db.ExecContext(ctx, stConcatInsert.SQL, accountID, key, seq, total, payload); err != nil {
		return nil, fmt.Errorf("buffer part: %w", err)
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	parts, err := concatSelectGroup(ctx, tx, key)
	if err != nil {
		return nil, err
	}
	if len(parts) < total {
		return nil, tx.Commit() // still waiting; commit releases locks
	}
	if _, err := tx.ExecContext(ctx, stConcatDelete.SQL, key); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return parts, nil
}

// ClaimExpired claims every group (at most 100 a call) whose OLDEST part has
// waited past the timeout, each under its locks, deleted before it is handed
// over.
func (a *ConcatBuffer) ClaimExpired(ctx context.Context, timeout time.Duration, each func(string, []*model.Message, error)) error {
	q := stConcatExpired
	if a.mo {
		q = stConcatMOExp
	}
	rows, err := a.db.QueryContext(ctx, q.SQL, int(timeout.Seconds()))
	if err != nil {
		return err
	}
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, k)
	}
	rows.Close()

	for _, key := range keys {
		tx, err := a.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		parts, err := concatSelectGroup(ctx, tx, key)
		if err == nil && len(parts) > 0 {
			_, err = tx.ExecContext(ctx, stConcatDelete.SQL, key)
		}
		if err != nil {
			tx.Rollback() //nolint:errcheck
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		if len(parts) > 0 {
			each(key, parts, nil)
		}
	}
	return nil
}

// concatSelectGroup reads (and locks) a group's parts in seq order.
func concatSelectGroup(ctx context.Context, tx *sql.Tx, key string) ([]*model.Message, error) {
	rows, err := tx.QueryContext(ctx, stConcatGroup.SQL, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var parts []*model.Message
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var m model.Message
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, err
		}
		parts = append(parts, &m)
	}
	return parts, rows.Err()
}
