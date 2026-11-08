package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	mssqldrv "github.com/microsoft/go-mssqldb"

	"github.com/yakugate/yakugate/internal/dal/stmt"
)

// Upserts on SQL Server (plan, section 7): what MySQL's INSERT … ON
// DUPLICATE KEY UPDATE does, written as an UPDATE that holds the key — row
// or gap — under UPDLOCK and SERIALIZABLE, then an INSERT when it found no
// row, in one transaction. Never MERGE: it has a long list of known faults
// under concurrency and takes the same locks no better.
//
// Two sessions that upsert one new key at once: the first one's UPDATE takes
// the key range's update lock and the second waits on it; the first inserts
// and commits; the second's UPDATE then finds the row. Nobody meets a
// duplicate key.
//
// An assignment is written for both shapes at once: "hits = t.hits + $hits"
// — t is the table's row, $col the value given for col (a parameter @col
// for one row, the column s.col of a table-valued parameter's rows).

// UpsertSpec says what an upsert writes.
type UpsertSpec struct {
	// Table, e.g. "dbo.limit_buckets".
	Table string
	// Keys are the columns that find the row; each is given as $key.
	Keys []string
	// Set are the assignments made to a row that is there:
	// "hits = t.hits + $hits", "updated_at = SYSUTCDATETIME()".
	Set []string
	// Insert are the columns a new row is given, each from $col. Empty = the
	// keys and every column Set assigns. InsertValues, when given, holds one
	// expression per Insert column instead ("SYSUTCDATETIME()", "$hits").
	Insert       []string
	InsertValues []string
	// Type is the table type of the rows a batch upsert is given
	// (UpsertBatch only), e.g. "dbo.limit_bucket_rows": CREATE TYPE … AS
	// TABLE with the columns the statement reads, made by the area's
	// migration. Its rows must have distinct keys.
	Type string
}

func (s UpsertSpec) insertCols() []string {
	if len(s.Insert) > 0 {
		return s.Insert
	}
	cols := append([]string(nil), s.Keys...)
	for _, a := range s.Set {
		cols = append(cols, target(a))
	}
	return cols
}

func (s UpsertSpec) insertValues(prefix string) []string {
	cols := s.insertCols()
	out := make([]string, len(cols))
	for i, c := range cols {
		if i < len(s.InsertValues) && s.InsertValues[i] != "" {
			out[i] = withValues(s.InsertValues[i], prefix)
		} else {
			out[i] = prefix + c
		}
	}
	return out
}

func (s UpsertSpec) keyMatch(prefix string) string {
	parts := make([]string, len(s.Keys))
	for i, k := range s.Keys {
		parts[i] = "t." + quoteIdent(k) + " = " + prefix + k
	}
	return strings.Join(parts, " AND ")
}

func (s UpsertSpec) assignments(prefix string) string {
	parts := make([]string, len(s.Set))
	for i, a := range s.Set {
		parts[i] = withValues(a, prefix)
	}
	return strings.Join(parts, ", ")
}

func quoteAll(cols []string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = quoteIdent(c)
	}
	return strings.Join(q, ", ")
}

// Upsert is one row's upsert, a registered statement.
type Upsert struct {
	Stmt *stmt.Stmt
}

// UpsertSQL is the text of one row's upsert. It answers one row: 1 when a
// row was there and was changed, 0 when it was inserted.
func UpsertSQL(s UpsertSpec) string {
	return fmt.Sprintf(`SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @yk_found INT;
BEGIN TRAN;
UPDATE t SET %s FROM %s AS t WITH (UPDLOCK, SERIALIZABLE) WHERE %s;
SET @yk_found = @@ROWCOUNT;
IF @yk_found = 0 INSERT INTO %s (%s) VALUES (%s);
COMMIT;
SELECT @yk_found;`,
		s.assignments("@"), Ident(s.Table), s.keyMatch("@"),
		Ident(s.Table), quoteAll(s.insertCols()), strings.Join(s.insertValues("@"), ", "))
}

// AddUpsert registers one row's upsert under name.
func AddUpsert(r *stmt.Registry, name string, s UpsertSpec, hot bool) *Upsert {
	return &Upsert{Stmt: r.Add(name, UpsertSQL(s), hot)}
}

// Exec upserts one row: args are sql.Named(col, value) for every $col of
// the spec. inserted says the row was new. It is its own transaction, or a
// part of the caller's when q is a *sql.Tx (XACT_ABORT is then on for the
// rest of it: an error rolls the whole of it back).
func (u *Upsert) Exec(ctx context.Context, q Querier, args ...any) (inserted bool, err error) {
	start := time.Now()
	var found int
	err = q.QueryRowContext(ctx, u.Stmt.SQL, args...).Scan(&found)
	observe(u.Stmt, start, 1)
	return err == nil && found == 0, err
}

// UpsertBatch is many rows' upsert in one statement, the rows in a
// table-valued parameter: one set-based UPDATE of the rows that are there,
// one INSERT of the rest, one transaction.
type UpsertBatch struct {
	Stmt *stmt.Stmt
	Type string
}

// UpsertBatchSQL is the text of a batch upsert. It answers one row: how many
// rows were changed, and how many inserted.
func UpsertBatchSQL(s UpsertSpec) string {
	join := s.keyMatch("s.")
	return fmt.Sprintf(`SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @yk_changed INT, @yk_added INT;
BEGIN TRAN;
UPDATE t SET %s FROM %s AS t WITH (UPDLOCK, SERIALIZABLE) INNER JOIN @rows AS s ON %s;
SET @yk_changed = @@ROWCOUNT;
INSERT INTO %s (%s) SELECT %s FROM @rows AS s
 WHERE NOT EXISTS (SELECT 1 FROM %s AS t WITH (UPDLOCK, SERIALIZABLE) WHERE %s);
SET @yk_added = @@ROWCOUNT;
COMMIT;
SELECT @yk_changed, @yk_added;`,
		s.assignments("s."), Ident(s.Table), join,
		Ident(s.Table), quoteAll(s.insertCols()), strings.Join(s.insertValues("s."), ", "),
		Ident(s.Table), join)
}

// AddUpsertBatch registers a batch upsert under name.
func AddUpsertBatch(r *stmt.Registry, name string, s UpsertSpec, hot bool) *UpsertBatch {
	if s.Type == "" {
		panic("mssql: batch upsert " + name + " names no table type")
	}
	return &UpsertBatch{Stmt: r.Add(name, UpsertBatchSQL(s), hot), Type: s.Type}
}

// Exec upserts rows, a slice of structs whose fields are the table type's
// columns in its order (go-mssqldb's TVP rules; `tvp:"-"` skips a field).
// An empty slice writes nothing. Two rows with one key in one call are a
// duplicate key: merge them first.
func (b *UpsertBatch) Exec(ctx context.Context, q Querier, rows any) (changed, added int64, err error) {
	if empty(rows) {
		return 0, 0, nil
	}
	start := time.Now()
	err = q.QueryRowContext(ctx, b.Stmt.SQL, sql.Named("rows", mssqldrv.TVP{TypeName: b.Type, Value: rows})).Scan(&changed, &added)
	observe(b.Stmt, start, changed+added)
	return changed, added, err
}
