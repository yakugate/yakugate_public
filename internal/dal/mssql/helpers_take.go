package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal/stmt"
)

// Taking rows off a queue table on SQL Server (plan, section 7): what MySQL's
// SELECT … FOR UPDATE SKIP LOCKED with its UPDATE or DELETE does, in ONE
// statement. The rows are read WITH (ROWLOCK, UPDLOCK, READPAST): a row
// another taker holds is passed over, never waited for, and the ones taken
// are changed (leased) or removed in the same statement, which OUTPUTs them.
// So two takers never take one row, and a take is one round trip.
//
// UPDATE TOP (n) takes any n rows; a queue that must be taken in order
// (oldest, or priority first) gives OrderBy, and the statement reads the
// TOP (n) in that order in a CTE and changes the CTE.
//
// This is for queues on disk tables. The plan puts the queues of an install
// with no Redis in memory-optimized tables with natively compiled
// procedures (those never take locks, so READPAST means nothing there);
// that is the queue area's own work.

// TakeSpec says what a take reads and does.
type TakeSpec struct {
	Table string
	// Columns are what the take answers, in order (OUTPUT inserted.… or
	// deleted.…). They must include every column Set reads or assigns, and
	// OrderBy's.
	Columns []string
	// Where picks the rows that may be taken, with named parameters:
	// "vendor_id = @vendor AND pick_at <= SYSUTCDATETIME()".
	Where string
	// OrderBy, when given, takes in that order: "priority, pick_at, id".
	OrderBy string
	// Set leases what is taken: "leased_until = DATEADD(SECOND, @lease,
	// SYSUTCDATETIME())", "attempts = attempts + 1". Empty with Delete.
	Set []string
	// Delete removes what is taken instead (taken, not leased).
	Delete bool
}

// Take is a registered take. @yk_n is how many it takes at most.
type Take struct {
	Stmt *stmt.Stmt
}

// TakeSQL is the text of a take.
func TakeSQL(s TakeSpec) string {
	out := make([]string, len(s.Columns))
	side := "inserted."
	if s.Delete {
		side = "deleted."
	}
	for i, c := range s.Columns {
		out[i] = side + quoteIdent(c)
	}
	output := strings.Join(out, ", ")
	where := ""
	if strings.TrimSpace(s.Where) != "" {
		where = " WHERE " + s.Where
	}
	if s.OrderBy == "" {
		if s.Delete {
			return fmt.Sprintf("DELETE TOP (@yk_n) FROM %s WITH (ROWLOCK, UPDLOCK, READPAST) OUTPUT %s%s",
				Ident(s.Table), output, where)
		}
		return fmt.Sprintf("UPDATE TOP (@yk_n) %s WITH (ROWLOCK, UPDLOCK, READPAST) SET %s OUTPUT %s%s",
			Ident(s.Table), strings.Join(s.Set, ", "), output, where)
	}
	cte := fmt.Sprintf("WITH yk_take AS (SELECT TOP (@yk_n) %s FROM %s WITH (ROWLOCK, UPDLOCK, READPAST)%s ORDER BY %s)",
		quoteAll(s.Columns), Ident(s.Table), where, s.OrderBy)
	if s.Delete {
		return cte + " DELETE FROM yk_take OUTPUT " + output
	}
	return cte + fmt.Sprintf(" UPDATE yk_take SET %s OUTPUT %s", strings.Join(s.Set, ", "), output)
}

// AddTake registers a take under name.
func AddTake(r *stmt.Registry, name string, s TakeSpec, hot bool) *Take {
	if !s.Delete && len(s.Set) == 0 {
		panic("mssql: take " + name + " neither leases (Set) nor removes (Delete)")
	}
	return &Take{Stmt: r.Add(name, TakeSQL(s), hot)}
}

// Run takes at most n rows and hands each to row, which scans it (the
// spec's Columns, in order). args are the Where's and Set's named
// parameters (sql.Named). It answers how many it took. The rows come back
// in no promised order, even from an ordered take (OUTPUT keeps none): sort
// them if it matters.
func (t *Take) Run(ctx context.Context, q Querier, n int, row func(scan func(dest ...any) error) error, args ...any) (int, error) {
	if n <= 0 {
		return 0, nil
	}
	start := time.Now()
	all := append([]any{sql.Named("yk_n", n)}, args...)
	rows, err := q.QueryContext(ctx, t.Stmt.SQL, all...)
	if err != nil {
		observe(t.Stmt, start, 0)
		return 0, err
	}
	defer rows.Close()
	got := 0
	for rows.Next() {
		if err := row(rows.Scan); err != nil {
			observe(t.Stmt, start, int64(got))
			return got, err
		}
		got++
	}
	observe(t.Stmt, start, int64(got))
	return got, rows.Err()
}
