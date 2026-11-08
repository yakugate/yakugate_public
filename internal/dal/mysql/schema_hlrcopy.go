package mysql

// Filling a separate HLR store (Saleem 2026-10-02, D-124) — moved here
// unchanged on 30 Oct 2026 from internal/platform/bootstrap/hlrcopy.go. When
// it runs (in the background, once a separate HLR store is set) is still the
// bootstrap's.
//
// The HLR shards, HTTP vendor sessions and JWT revocations lived in the queue
// database from the start. Once YAKU_HLR_DSN names a database of their own,
// that database is created with the hlr/ migration set and what the queue
// database held is copied in — once, in the background, so a large HLR does
// not hold a role's start. Rows already in the new database win (INSERT
// IGNORE): a lookup kept or a session made since the switch is newer than the
// copy. The queue database's tables are left as they are.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	driver "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/dal"
)

// hlrCopiedMarker is the schema_migrations row a finished copy leaves in the
// HLR store; with it there, nothing is copied again.
const hlrCopiedMarker = "hlr/copied-from-queue"

var (
	stHLRCopied      = SchemaStmts.Add("schema.hlr_copied", "SELECT COUNT(*) FROM schema_migrations WHERE version = ?", false)
	stHLRCopyLock    = SchemaStmts.Add("schema.hlr_copy_lock", "SELECT GET_LOCK('yaku_hlr_copy', 0)", false)
	stHLRCopyRelease = SchemaStmts.Add("schema.hlr_copy_release", "SELECT RELEASE_LOCK('yaku_hlr_copy')", false)
	stTablesLike     = SchemaStmts.Add("schema.tables_like", `SELECT table_name FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name LIKE ? ORDER BY table_name`, false)
)

// hlrCopyTables are the non-shard tables copied, with their key and columns.
var hlrCopyTables = []struct {
	name, key string
	cols      []string
}{
	{"vendor_sessions", "vendor_id", []string{"vendor_id", "cid", "vars", "expires_at", "updated_at"}},
	{"jwt_revocations", "jti", []string{"jti", "account_id", "expires_at", "revoked_at", "reason"}},
}

var hlrShardCols = []string{"msisdn", "mcc", "mnc", "ported", "last_check", "is_deleted"}

// CopyHLRFrom copies the HLR shards, vendor sessions and JWT revocations
// from the queue database (queueDSN) into this store (the HLR store), once: a
// finished copy leaves a marker, and one process copies at a time. Returns
// the rows inserted.
func (s *schema) CopyHLRFrom(ctx context.Context, queueDSN string, log dal.Printer) (int64, error) {
	return copyHLRFromQueue(ctx, queueDSN, s.dsn, log)
}

func copyHLRFromQueue(ctx context.Context, queueDSN, hlrDSN string, log dal.Printer) (int64, error) {
	src, err := rawDB(queueDSN)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	dst, err := rawDB(hlrDSN)
	if err != nil {
		return 0, err
	}
	defer dst.Close()

	done := func() (bool, error) {
		var n int
		err := dst.QueryRowContext(ctx, stHLRCopied.SQL, hlrCopiedMarker).Scan(&n)
		return n > 0, err
	}
	if ok, err := done(); err != nil || ok {
		return 0, err
	}
	// One copier: a pinned connection holds the lock.
	conn, err := dst.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, stHLRCopyLock.SQL).Scan(&got); err != nil || got.Int64 != 1 {
		return 0, err // another process is copying
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), stHLRCopyRelease.SQL)
	if ok, err := done(); err != nil || ok {
		return 0, err
	}

	var total int64
	for _, t := range hlrCopyTables {
		n, err := copyTable(ctx, src, dst, t.name, t.key, t.cols)
		total += n
		if err != nil {
			return total, fmt.Errorf("%s: %w", t.name, err)
		}
	}
	shards, err := tablesLike(ctx, src, `hlr\_%`)
	if err != nil {
		return total, err
	}
	for _, sh := range shards {
		if sh == "hlr_template" {
			continue
		}
		if _, err := dst.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS `"+sh+"` LIKE hlr_template"); err != nil {
			return total, fmt.Errorf("%s: %w", sh, err)
		}
		n, err := copyTable(ctx, src, dst, sh, "msisdn", hlrShardCols)
		total += n
		if err != nil {
			return total, fmt.Errorf("%s: %w", sh, err)
		}
	}
	if _, err := dst.ExecContext(ctx, stSchemaMark.SQL, hlrCopiedMarker); err != nil {
		return total, err
	}
	if log != nil {
		log.Printf("HLR store: the queue database's HLR data and vendor sessions are copied (%d shard(s)); its tables are left as they were", len(shards))
	}
	return total, nil
}

// rawDB opens a DSN with times left as text, so every value is copied exactly
// as it is stored — no time zone or driver conversion on the way through.
func rawDB(dsn string) (*sql.DB, error) {
	c, err := driver.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c.ParseTime = false
	return sql.Open("mysql", c.FormatDSN())
}

func tablesLike(ctx context.Context, db *sql.DB, pattern string) ([]string, error) {
	rows, err := db.QueryContext(ctx, stTablesLike.SQL, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// copyTable pages through src.table by key and INSERT IGNOREs each page into
// dst. A table src does not have is nothing to copy.
func copyTable(ctx context.Context, src, dst *sql.DB, table, key string, cols []string) (int64, error) {
	if have, err := tablesLike(ctx, src, table); err != nil || len(have) == 0 {
		return 0, err
	}
	const page = 1000
	list := "`" + strings.Join(cols, "`,`") + "`"
	keyAt := -1
	for i, c := range cols {
		if c == key {
			keyAt = i
		}
	}
	var after any
	var total int64
	for {
		q := "SELECT " + list + " FROM `" + table + "`"
		args := []any{}
		if after != nil {
			q += " WHERE `" + key + "` > ?"
			args = append(args, after)
		}
		q += " ORDER BY `" + key + "` LIMIT " + fmt.Sprint(page)
		rows, err := src.QueryContext(ctx, q, args...)
		if err != nil {
			return total, err
		}
		var vals []any
		n := 0
		for rows.Next() {
			row := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range row {
				ptrs[i] = &row[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return total, err
			}
			vals = append(vals, row...)
			after = row[keyAt]
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return total, err
		}
		if n == 0 {
			return total, nil
		}
		ph := "(" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"
		ins := "INSERT IGNORE INTO `" + table + "` (" + list + ") VALUES " +
			strings.TrimSuffix(strings.Repeat(ph+",", n), ",")
		res, err := dst.ExecContext(ctx, ins, vals...)
		if err != nil {
			return total, err
		}
		if a, err := res.RowsAffected(); err == nil {
			total += a
		}
		if n < page {
			return total, nil
		}
	}
}
