package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	mssqldrv "github.com/microsoft/go-mssqldb"

	"github.com/yakugate/yakugate/internal/dal/stmt"
)

// Inserts of many rows, and an insert that answers its new id (plan,
// section 7: "table-valued parameter + one set-based statement",
// "mssql.CopyIn for large batches", "BIGINT IDENTITY + OUTPUT INSERTED.id").
//
// Which one: a batch a role writes as it goes (a few to a few thousand rows)
// is InsertBatch — one statement, one round trip, in the caller's
// transaction if it has one. A load of many thousands (an import, a copy of
// a day's table) is BulkCopy, the server's bulk load: much less work a row,
// but its own transaction and no answer row by row. BulkFrom is the line
// between the two the areas use.

// BulkFrom is the batch size from which BulkCopy is the cheaper way in.
const BulkFrom = 5000

// InsertSpec says what an insert of many rows writes.
type InsertSpec struct {
	Table   string   // "dbo.mo_out_rows"
	Columns []string // the table's columns written, in the table type's order
	Type    string   // the table type of the rows, "dbo.mo_out_rows_tvp"
	// Ignore leaves out a row whose key is there already (the MySQL DAL's
	// INSERT IGNORE on a unique key): Key names the columns of that key.
	Key []string
}

// InsertBatch is many rows' insert from a table-valued parameter.
type InsertBatch struct {
	Stmt *stmt.Stmt
	Type string
}

// InsertBatchSQL is the text of a batch insert.
func InsertBatchSQL(s InsertSpec) string {
	cols := quoteAll(s.Columns)
	sel := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		sel[i] = "s." + quoteIdent(c)
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM @rows AS s", Ident(s.Table), cols, strings.Join(sel, ", "))
	if len(s.Key) > 0 {
		match := make([]string, len(s.Key))
		for i, k := range s.Key {
			match[i] = "t." + quoteIdent(k) + " = s." + quoteIdent(k)
		}
		q += fmt.Sprintf(" WHERE NOT EXISTS (SELECT 1 FROM %s AS t WITH (UPDLOCK, SERIALIZABLE) WHERE %s)",
			Ident(s.Table), strings.Join(match, " AND "))
	}
	return q
}

// AddInsertBatch registers a batch insert under name.
func AddInsertBatch(r *stmt.Registry, name string, s InsertSpec, hot bool) *InsertBatch {
	if s.Type == "" {
		panic("mssql: batch insert " + name + " names no table type")
	}
	return &InsertBatch{Stmt: r.Add(name, InsertBatchSQL(s), hot), Type: s.Type}
}

// Exec inserts rows (a slice of structs in the table type's column order)
// and answers how many went in. An empty slice writes nothing.
func (b *InsertBatch) Exec(ctx context.Context, q Querier, rows any) (int64, error) {
	if empty(rows) {
		return 0, nil
	}
	start := time.Now()
	res, err := q.ExecContext(ctx, b.Stmt.SQL, sql.Named("rows", mssqldrv.TVP{TypeName: b.Type, Value: rows}))
	var n int64
	if err == nil {
		n, err = res.RowsAffected()
	}
	observe(b.Stmt, start, n)
	return n, err
}

// empty says a slice is nil or has no rows (a TVP of none is refused by the
// driver).
func empty(rows any) bool {
	v := reflect.ValueOf(rows)
	return !v.IsValid() || (v.Kind() == reflect.Slice && v.Len() == 0)
}

// BulkCopy loads rows into table with the server's bulk load
// (mssql.CopyIn), in a transaction of its own on db. Each row holds one
// value per column, in cols' order. Constraints are checked and triggers
// fired, as an INSERT would: a bulk load does neither unless asked, and a
// row a check refuses must be refused here too.
func BulkCopy(ctx context.Context, db *sql.DB, table string, cols []string, rows [][]any) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	st, err := tx.PrepareContext(ctx, mssqldrv.CopyIn(table, mssqldrv.BulkOptions{CheckConstraints: true, FireTriggers: true}, cols...))
	if err != nil {
		return 0, err
	}
	for _, r := range rows {
		if _, err := st.ExecContext(ctx, r...); err != nil {
			st.Close()
			return 0, err
		}
	}
	res, err := st.ExecContext(ctx) // no arguments: send what is buffered
	if err != nil {
		st.Close()
		return 0, err
	}
	if err := st.Close(); err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, tx.Commit()
}

// InsertID is an insert into a table keyed by a BIGINT IDENTITY that
// answers the new row's id (MySQL's LastInsertId): INSERT … OUTPUT
// INSERTED.id VALUES …, one round trip.
type InsertID struct {
	Stmt *stmt.Stmt
}

// InsertIDSQL is the text of an insert that answers idCol: the values are
// @p1, @p2 … in cols' order, or the expressions given in values.
func InsertIDSQL(table, idCol string, cols []string, values ...string) string {
	v := make([]string, len(cols))
	for i := range cols {
		if i < len(values) && values[i] != "" {
			v[i] = values[i]
		} else {
			v[i] = fmt.Sprintf("@p%d", i+1)
		}
	}
	return fmt.Sprintf("INSERT INTO %s (%s) OUTPUT INSERTED.%s VALUES (%s)",
		Ident(table), quoteAll(cols), quoteIdent(idCol), strings.Join(v, ", "))
}

// AddInsertID registers an insert that answers its id.
func AddInsertID(r *stmt.Registry, name, table, idCol string, cols []string, hot bool, values ...string) *InsertID {
	return &InsertID{Stmt: r.Add(name, InsertIDSQL(table, idCol, cols, values...), hot)}
}

// Exec inserts one row and answers its id.
func (i *InsertID) Exec(ctx context.Context, q Querier, args ...any) (int64, error) {
	start := time.Now()
	var id int64
	err := q.QueryRowContext(ctx, i.Stmt.SQL, args...).Scan(&id)
	observe(i.Stmt, start, 1)
	return id, err
}
