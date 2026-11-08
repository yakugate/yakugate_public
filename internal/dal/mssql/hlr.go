// MSSQL HLR shards, optionally memory-optimized (register rows 1.6.1 + 1.6.2).
// Moved here unchanged on 29 Oct 2026 (the DAL plan) from
// internal/hlr/store_mssql.go; internal/hlr reads it through dal.HLRShards
// (hlr.FromShards).
//
// This lived in internal/hlr rather than in internal/refdata because HLR is a QUEUE-store
// resident (the four-store model puts hlr_<first4> alongside the per-vendor
// queues), while refdata's loader is a CONFIG-store reader. Tying the two
// together is what made "MSSQL HLR" look like it needed a whole MSSQL
// reference loader: it does not. A deployment can perfectly well keep its
// configuration in MySQL and its queue plus HLR in SQL Server, and this type
// is what makes that combination work.
//
// It satisfies dal.HLRShards (hlr.DeltaSource through hlr.FromShards), so the ordinary in-memory hlr.Cache sits on
// top exactly as it does for MySQL: one full load at start, deltas by
// last_check thereafter. Memory-optimized tables make the DATABASE side of a
// lookup fast; the cache is still what keeps a lookup off the network
// entirely, and both are worth having.

package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/hlr"
)

// openHLRShards is the HLR shards' opener.
func openHLRShards(dsn string) (dal.HLRShards, error) {
	s, err := NewHLRShards(dsn)
	if err != nil {
		return nil, err
	}
	return s, nil
}

var _ dal.HLRShards = (*HLRShards)(nil)

// HLRShards reads HLR shards from a SQL Server queue store.
type HLRShards struct {
	db *sql.DB
	// bucketCount sizes the hash index on a shard this type creates or
	// upgrades. See Prepare for why it is not a constant.
	bucketCount int
}

// DefaultHLRBucketCount is the hash-index size used for a shard whose row
// count we have no better information about.
//
// A hash index is a fixed array of buckets, so this is a real capacity
// decision and not a tuning knob to leave alone: too few buckets and lookups
// degrade into scanning collision chains, too many and the memory is simply
// wasted. Microsoft's guidance is one to two times the number of DISTINCT key
// values, rounded up to a power of two. 2^20 suits a shard holding up to about
// a million numbers, which is the usual size of a four-digit prefix.
const DefaultHLRBucketCount = 1 << 20

// NewHLRShards opens the queue-store connection.
func NewHLRShards(dsn string) (*HLRShards, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return &HLRShards{db: db, bucketCount: DefaultHLRBucketCount}, nil
}

// WithBucketCount overrides the hash-index size for shards created or upgraded
// from here on. Rounded up to a power of two, because SQL Server rounds it up
// anyway and silently getting a different number than you asked for is worse
// than being told.
func (l *HLRShards) WithBucketCount(n int) *HLRShards {
	if n > 0 {
		l.bucketCount = roundPow2(n)
	}
	return l
}

func roundPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// Close releases the connection.
func (l *HLRShards) Close() error { return l.db.Close() }

// DB exposes the handle for callers that need the same connection (tests, and
// anything wiring a second consumer onto one pool).
func (l *HLRShards) DB() *sql.DB { return l.db }

// Lookup resolves one MSISDN from its first-4-digit shard. Soft-deleted rows
// are misses. Used directly only in cache passthrough mode; normally Cache
// serves lookups from memory.
func (l *HLRShards) Lookup(msisdn string) (dal.HLRRow, bool) {
	table := "hlr_" + hlr.Shard(msisdn)
	var e dal.HLRRow
	var ported bool
	var lc sql.NullTime
	err := l.db.QueryRow(
		"SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), ported, last_check FROM "+quoteIdent(table)+" WHERE msisdn = @p1 AND is_deleted = 0",
		msisdn).Scan(&e.MSISDN, &e.MCC, &e.MNC, &e.OperatorName, &e.CCC, &ported, &lc)
	if err != nil {
		return dal.HLRRow{}, false
	}
	e.Ported = ported
	if lc.Valid {
		e.CheckedAt = lc.Time
	}
	return e, true
}

// shards lists the live hlr_<first4> tables, template excluded.
func (l *HLRShards) shards(ctx context.Context) ([]string, error) {
	rows, err := l.db.QueryContext(ctx, `
		SELECT name FROM sys.tables
		WHERE name LIKE 'hlr[_]%' AND name <> 'hlr_template'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Prepare brings every live shard up to the cache schema (is_deleted plus an
// indexed last_check), the same job refdata.MySQL.Prepare does on the other
// backend. Idempotent: it checks the catalog before altering, so a restart
// costs one query per shard.
//
// It deliberately does NOT convert a disk-based shard into a memory-optimized
// one. SQL Server cannot do that in place — it means creating a new table,
// copying every row and swapping the names, which on a shard of any size is a
// long, memory-hungry, interruptible operation. Doing that silently inside a
// process start is the wrong place for it: a half-finished conversion during a
// deploy would take HLR down. New shards are born memory-optimized by cloning
// hlr_template (migration queue/mssql/0004); converting an existing one is an
// operator's deliberate, scheduled act.
func (l *HLRShards) Prepare(ctx context.Context) error {
	shards, err := l.shards(ctx)
	if err != nil {
		return err
	}
	for _, t := range shards {
		memOpt, err := l.isMemoryOptimized(ctx, t)
		if err != nil {
			return fmt.Errorf("hlr prepare %s: %w", t, err)
		}
		if err := l.ensureColumn(ctx, t, "is_deleted", "BIT NOT NULL DEFAULT 0"); err != nil {
			return err
		}
		if err := l.ensureColumn(ctx, t, "last_check", "DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()"); err != nil {
			return err
		}
		// A memory-optimized table cannot take an index after creation, so
		// there is nothing to add and nothing to fix: it either has the index
		// from the template or it does not, and rebuilding it is the operator
		// conversion described above.
		if memOpt {
			continue
		}
		if err := l.ensureIndex(ctx, t, "idx_hlr_last_check", "last_check"); err != nil {
			return err
		}
	}
	return nil
}

func (l *HLRShards) isMemoryOptimized(ctx context.Context, table string) (bool, error) {
	var b bool
	err := l.db.QueryRowContext(ctx,
		"SELECT is_memory_optimized FROM sys.tables WHERE name = @p1", table).Scan(&b)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return b, err
}

// ensureColumn adds a column when the catalog says it is missing.
func (l *HLRShards) ensureColumn(ctx context.Context, table, column, def string) error {
	var n int
	if err := l.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sys.columns
		WHERE object_id = OBJECT_ID(@p1) AND name = @p2`, table, column).Scan(&n); err != nil {
		return fmt.Errorf("hlr prepare %s.%s: %w", table, column, err)
	}
	if n > 0 {
		return nil
	}
	if _, err := l.db.ExecContext(ctx,
		"ALTER TABLE "+quoteIdent(table)+" ADD "+quoteIdent(column)+" "+def); err != nil {
		// Every accept role prepares the shards at its start: another one
		// may have added it between the look and the ALTER (2705).
		if n, ok := ErrorNumber(err); ok && n == 2705 {
			return nil
		}
		return fmt.Errorf("hlr prepare %s.%s: %w", table, column, err)
	}
	return nil
}

// ensureIndex adds an index led by col when the shard has none, whatever its
// name (a shard made from hlr_template has idx_last_check). Disk-based shards
// only.
func (l *HLRShards) ensureIndex(ctx context.Context, table, name, col string) error {
	var n int
	if err := l.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sys.index_columns ic
		JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		WHERE ic.object_id = OBJECT_ID(@p1) AND ic.key_ordinal = 1 AND c.name = @p2`, table, col).Scan(&n); err != nil {
		return fmt.Errorf("hlr prepare %s index: %w", table, err)
	}
	if n > 0 {
		return nil
	}
	if _, err := l.db.ExecContext(ctx,
		"CREATE INDEX "+quoteIdent(name)+" ON "+quoteIdent(table)+" ("+quoteIdent(col)+")"); err != nil {
		// Another accept role made it between the look and the CREATE (1913).
		if n, ok := ErrorNumber(err); ok && n == 1913 {
			return nil
		}
		return fmt.Errorf("hlr prepare %s index: %w", table, err)
	}
	return nil
}

// Count totals HLR rows across all shards — the input to the cache's memory
// precheck, so it must be the real number rather than an estimate.
func (l *HLRShards) Count(ctx context.Context) (int64, error) {
	shards, err := l.shards(ctx)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, t := range shards {
		var n int64
		if err := l.db.QueryRowContext(ctx, "SELECT COUNT_BIG(*) FROM "+quoteIdent(t)).Scan(&n); err != nil {
			return 0, fmt.Errorf("hlr count %s: %w", t, err)
		}
		total += n
	}
	return total, nil
}

// LoadSince returns entries changed at or after `since` across every shard,
// plus the highest last_check seen. A zero `since` is the initial full load
// and skips soft-deleted rows; a delta load includes them, because that is how
// the cache learns to evict.
//
// SQL Server has no ON UPDATE CURRENT_TIMESTAMP, so last_check is only as
// truthful as the writers: the Admin API sets it on every HLR upsert, and so
// does the engine's own import. A writer that forgets will have its rows
// missed by every delta until a full reload — which is why the column carries
// a NOT NULL default rather than being nullable.
func (l *HLRShards) LoadSince(ctx context.Context, since time.Time) ([]dal.HLRRow, time.Time, error) {
	shards, err := l.shards(ctx)
	if err != nil {
		return nil, since, err
	}
	maxTS := since
	var out []dal.HLRRow
	for _, t := range shards {
		q := "SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), ported, is_deleted, last_check FROM " + quoteIdent(t)
		var rows *sql.Rows
		if since.IsZero() {
			rows, err = l.db.QueryContext(ctx, q+" WHERE is_deleted = 0")
		} else {
			rows, err = l.db.QueryContext(ctx, q+" WHERE last_check >= @p1", since)
		}
		if err != nil {
			return nil, maxTS, fmt.Errorf("hlr load %s: %w", t, err)
		}
		for rows.Next() {
			var e dal.HLRRow
			var ported, deleted bool
			var lc sql.NullTime
			if err := rows.Scan(&e.MSISDN, &e.MCC, &e.MNC, &e.OperatorName, &e.CCC, &ported, &deleted, &lc); err != nil {
				rows.Close()
				return nil, maxTS, fmt.Errorf("hlr load %s: %w", t, err)
			}
			e.Ported, e.Deleted = ported, deleted
			if lc.Valid {
				e.CheckedAt = lc.Time
			}
			if lc.Valid && lc.Time.After(maxTS) {
				maxTS = lc.Time
			}
			out = append(out, e)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, maxTS, fmt.Errorf("hlr load %s: %w", t, err)
		}
		rows.Close()
	}
	return out, maxTS, nil
}

// EnsureShard creates hlr_<first4> as a memory-optimized table if it is
// absent. Shard names come from the first four digits of an MSISDN and are
// therefore always digits — quoteIdent enforces that rather than trusting it,
// because these names reach SQL as text.
func (l *HLRShards) EnsureShard(ctx context.Context, msisdn string) error {
	table := "hlr_" + hlr.Shard(msisdn)
	var n int
	if err := l.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sys.tables WHERE name = @p1", table).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err := l.db.ExecContext(ctx, fmt.Sprintf(`
CREATE TABLE %s (
  msisdn        VARCHAR(20) COLLATE Latin1_General_100_BIN2 NOT NULL,
  mcc           VARCHAR(3)  NOT NULL,
  mnc           VARCHAR(3)  NOT NULL,
  ported        BIT         NOT NULL DEFAULT 0,
  operator_name NVARCHAR(128) NULL,
  ccc           NVARCHAR(12) NULL,
  block_senders VARCHAR(25) NOT NULL DEFAULT '',
  is_deleted    BIT         NOT NULL DEFAULT 0,
  last_check    DATETIME2   NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT %s PRIMARY KEY NONCLUSTERED HASH (msisdn) WITH (BUCKET_COUNT = %d),
  INDEX idx_hlr_network NONCLUSTERED (mcc, mnc),
  INDEX idx_hlr_last_check NONCLUSTERED (last_check)
) WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA)`,
		quoteIdent(table), quoteIdent("pk_"+table), l.bucketCount))
	return err
}

// quoteIdent brackets an identifier this package builds itself. Shard names
// are derived from digits and table names from the catalog, so nothing here is
// user input — but these strings are concatenated into SQL, and a bracketed,
// escaped identifier costs nothing and removes the question.
func quoteIdent(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}
