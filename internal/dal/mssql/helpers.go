package mssql

import (
	"context"
	"database/sql"
	"errors"
	"math/rand/v2"
	"regexp"
	"strings"
	"time"

	"github.com/golang-sql/civil"
	mssqldrv "github.com/microsoft/go-mssqldb"

	"github.com/yakugate/yakugate/internal/dal/stmt"
)

// The small things every SQL Server area uses (DAL plan, section 7, "SQL
// Server choices"): the clock, times, parameter types, the error numbers the
// areas act on, and running a statement of the registry.

// Querier is a *sql.DB, a *sql.Tx or a *sql.Conn.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Now is the database's clock, in UTC, for a statement's text: what
// UTC_TIMESTAMP(3) is on MySQL. Every time is stored DATETIME2(3) in UTC; a
// DATETIME2 carries no zone, so the one rule is that it is always UTC.
const Now = "SYSUTCDATETIME()"

// Time is a time as a DATETIME2 parameter, in UTC, to the millisecond the
// columns keep.
//
// go-mssqldb sends a time.Time as DATETIMEOFFSET. Compared with a
// DATETIME2 column, SQL Server converts the COLUMN to the parameter's
// type (DATETIMEOFFSET ranks higher), so an index on it can no longer be
// sought — the same trap as Varchar's. A civil.DateTime goes as DATETIME2.
func Time(t time.Time) civil.DateTime {
	return civil.DateTimeOf(t.UTC().Truncate(time.Millisecond))
}

// Varchar is a string as a VARCHAR parameter (rule 8 of the plan).
//
// go-mssqldb sends a Go string as NVARCHAR. Compared with a CHAR or
// VARCHAR column — the ULID message id CHAR(26), a role or deployment name —
// SQL Server converts the COLUMN to NVARCHAR, and an index on it is
// scanned instead of sought: a lookup by message id reads the whole table.
// Pass every value of a CHAR/VARCHAR column through Varchar. A value for an
// NVARCHAR column (text a person wrote) stays a plain string.
func Varchar(s string) mssqldrv.VarChar { return mssqldrv.VarChar(s) }

// ErrorNumber is SQL Server's own number for an error, when err is one.
func ErrorNumber(err error) (int32, bool) {
	var e mssqldrv.Error
	if errors.As(err, &e) {
		return e.Number, true
	}
	var p *mssqldrv.Error
	if errors.As(err, &p) && p != nil {
		return p.Number, true
	}
	return 0, false
}

// IsDuplicate says err is a row refused by a primary key or unique
// constraint (2627) or a unique index (2601): MySQL's 1062.
func IsDuplicate(err error) bool {
	n, ok := ErrorNumber(err)
	return ok && (n == 2627 || n == 2601)
}

// MissingTable says err is "Invalid object name" (208): a table (or view)
// that is not there. MySQL's 1146.
func MissingTable(err error) bool {
	n, ok := ErrorNumber(err)
	return ok && n == 208
}

// IsRetryable says err is one a statement or transaction may simply be run
// again after: chosen as a deadlock victim (1205), a lock wait or snapshot
// conflict (1222, 3960), or a memory-optimized table's write conflict or
// failed validation (41301, 41302, 41305, 41325). The whole transaction was
// rolled back, so running it again is the same write.
func IsRetryable(err error) bool {
	n, ok := ErrorNumber(err)
	if !ok {
		return false
	}
	switch n {
	case 1205, 1222, 3960, 41301, 41302, 41305, 41325:
		return true
	}
	return false
}

// Again runs f until it succeeds, fails with an error IsRetryable does not
// know, or has run `tries` times — a little later each time (15, 30, 45 ms …
// plus up to 20 ms), as the MySQL DAL's againIfVictimN does.
func Again(ctx context.Context, tries int, f func() error) error {
	var err error
	for i := 1; ; i++ {
		if err = f(); err == nil || !IsRetryable(err) || i >= tries {
			return err
		}
		wait := time.Duration(15*i)*time.Millisecond + time.Duration(rand.IntN(20))*time.Millisecond
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
	}
}

// Ident brackets an identifier this DAL writes into a statement, part by
// part ("dbo.queue_mt" → [dbo].[queue_mt]). Names come from the code or the
// catalog, never from a request.
func Ident(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = quoteIdent(strings.Trim(p, "[]"))
	}
	return strings.Join(parts, ".")
}

// observe runs one call of a registered statement and counts it.
func observe(s *stmt.Stmt, start time.Time, rows int64) {
	if s != nil {
		s.Observe(time.Since(start), rows)
	}
}

// newValue is "$col" in the shapes' assignments: the value given for col.
var newValue = regexp.MustCompile(`\$([A-Za-z_][A-Za-z0-9_]*)`)

// withValues writes each "$col" of an expression as prefix+col: "@col" for
// one row's parameters, "s.col" for a table-valued parameter's rows.
func withValues(expr, prefix string) string {
	return newValue.ReplaceAllString(expr, prefix+"$1")
}

// target is the column an assignment "col = …" sets.
func target(assign string) string {
	col, _, _ := strings.Cut(assign, "=")
	col = strings.TrimSpace(col)
	if i := strings.LastIndex(col, "."); i >= 0 {
		col = col[i+1:]
	}
	return strings.Trim(col, "[] ")
}
