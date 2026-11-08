package mssql

// Filling a separate HLR store (D-124) — made like
// internal/dal/mysql/schema_hlrcopy.go. Once YAKU_HLR_DSN names a database
// of its own, what the queue database held (HLR shards, HTTP vendor
// sessions, JWT revocations) is copied in once, in the background. Rows
// already in the new database win: a lookup kept or a session made since the
// switch is newer than the copy. The queue database's tables are left as
// they are.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// hlrCopiedMarker is the schema_migrations row a finished copy leaves.
const hlrCopiedMarker = "hlr/copied-from-queue"

var (
	stHLRCopied  = SchemaStmts.Add("schema.hlr_copied", "SELECT COUNT(*) FROM schema_migrations WHERE version = @p1", false)
	stTablesLike = SchemaStmts.Add("schema.tables_like", `SELECT name FROM sys.tables
		WHERE name LIKE @p1 ESCAPE '\' ORDER BY name`, false)
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

// CopyHLRFrom copies from the queue database (queueDSN) into this store,
// once: a finished copy leaves a marker, and one process copies at a time.
func (s *schema) CopyHLRFrom(ctx context.Context, queueDSN string, log dal.Printer) (int64, error) {
	src, err := schemaOpenStore(queueDSN)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	dst, err := schemaOpenStore(s.dsn)
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
	conn, err := dst.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	const lock = "yaku_hlr_copy"
	var got int
	if err := conn.QueryRowContext(ctx, stSchemaLock.SQL, lock, 0).Scan(&got); err != nil || got < 0 {
		return 0, err // another process is copying
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), stSchemaRelease.SQL, lock)
	if ok, err := done(); err != nil || ok {
		return 0, err
	}

	var total int64
	for _, t := range hlrCopyTables {
		n, err := hlrCopyTable(ctx, src, dst, t.name, t.key, t.cols)
		total += n
		if err != nil {
			return total, fmt.Errorf("%s: %w", t.name, err)
		}
	}
	shards, err := hlrTablesLike(ctx, src, `hlr\_%`)
	if err != nil {
		return total, err
	}
	clones := NewCloner(dst)
	n := 0
	for _, sh := range shards {
		if sh == "hlr_template" {
			continue
		}
		n++
		if err := clones.CreateLike(ctx, "hlr_template", sh); err != nil {
			return total, fmt.Errorf("%s: %w", sh, err)
		}
		c, err := hlrCopyTable(ctx, src, dst, sh, "msisdn", hlrShardCols)
		total += c
		if err != nil {
			return total, fmt.Errorf("%s: %w", sh, err)
		}
	}
	if _, err := dst.ExecContext(ctx, stSchemaMark.SQL, hlrCopiedMarker); err != nil {
		return total, err
	}
	if log != nil {
		log.Printf("HLR store: the queue database's HLR data and vendor sessions are copied (%d shard(s)); its tables are left as they were", n)
	}
	return total, nil
}

func hlrTablesLike(ctx context.Context, db *sql.DB, pattern string) ([]string, error) {
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

// copyPage is the rows a page copies: SQL Server takes at most 2,100
// parameters a statement, and a shard row has six.
const copyPage = 300

// copyTable pages through src.table by key and inserts each page into dst,
// leaving a row dst already has as it is. A table src does not have is
// nothing to copy.
func hlrCopyTable(ctx context.Context, src, dst *sql.DB, table, key string, cols []string) (int64, error) {
	esc := strings.NewReplacer(`\`, `\\`, `_`, `\_`, `%`, `\%`).Replace(table)
	if have, err := hlrTablesLike(ctx, src, esc); err != nil || len(have) == 0 {
		return 0, err
	}
	list := make([]string, len(cols))
	for i, c := range cols {
		list[i] = schemaQuote(c)
	}
	keyAt := -1
	for i, c := range cols {
		if c == key {
			keyAt = i
		}
	}
	var after any
	var total int64
	for {
		q := fmt.Sprintf("SELECT TOP (%d) %s FROM %s", copyPage, strings.Join(list, ", "), schemaQuote(table))
		var args []any
		if after != nil {
			q += " WHERE " + schemaQuote(key) + " > @p1"
			args = append(args, after)
		}
		q += " ORDER BY " + schemaQuote(key)
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
		tuples := make([]string, n)
		p := 1
		for r := 0; r < n; r++ {
			ph := make([]string, len(cols))
			for i := range cols {
				ph[i] = fmt.Sprintf("@p%d", p)
				p++
			}
			tuples[r] = "(" + strings.Join(ph, ", ") + ")"
		}
		ins := "INSERT INTO " + schemaQuote(table) + " (" + strings.Join(list, ", ") + ") SELECT " +
			"v." + strings.Join(list, ", v.") + " FROM (VALUES " + strings.Join(tuples, ", ") + ") AS v (" +
			strings.Join(list, ", ") + ") WHERE NOT EXISTS (SELECT 1 FROM " + schemaQuote(table) + " x WHERE x." +
			schemaQuote(key) + " = v." + schemaQuote(key) + ")"
		res, err := dst.ExecContext(ctx, ins, vals...)
		if err != nil {
			return total, err
		}
		if a, err := res.RowsAffected(); err == nil {
			total += a
		}
		if n < copyPage {
			return total, nil
		}
	}
}
