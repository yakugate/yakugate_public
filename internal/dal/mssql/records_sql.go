package mssql

// The record store's small parts on SQL Server: its statement registry, the
// day tables named into a statement, rows given as one JSON parameter, and
// the values MySQL's helpers wrote (mysql.go's nz, nsStr, ackAtArg …,
// records.go's valuesFor), written for SQL Server.
//
// How the record store is written for SQL Server, and why:
//
//   - A day table (mt_<company>_<day> …) is named into the text at the call:
//     a registered statement holds {mt}, {sub}, {dlr} … and rOn puts the
//     bracketed name there. Each day's table is its own text, as on MySQL.
//   - A batch of rows is ONE parameter: a JSON array of arrays, read by
//     OPENJSON … WITH (col type '$[i]'). So a batch of any size is one
//     statement and one plan (the schema has no table types). A message id
//     is read as CHAR(26) in its column's collation, so the unique index on
//     msg_id is sought; every other value is read wide and the column itself
//     refuses a value too long for it, as MySQL's strict mode does.
//   - "Insert if not there" (MySQL's read-then-insert, and INSERT IGNORE) is
//     INSERT … SELECT … WHERE NOT EXISTS, and OUTPUT INSERTED.msg_id says
//     which rows it made — the read and the insert in one statement. Two
//     writers of one new row: the second meets the unique key (2627/2601)
//     and the write is made again, as MySQL's errRaced is.
//   - Times go as text "2006-01-02T15:04:05.000" (UTC) inside the JSON, or as
//     Time(t) on their own: a DATETIME2, never a DATETIMEOFFSET.
//   - A decimal (a conversion rate) goes as its shortest decimal text, as
//     MySQL rounds a double into DECIMAL; JSON's exponent form is not read
//     as a DECIMAL by SQL Server.
//   - A text value on its own goes as NVARCHAR(MAX) (rText), a message id as
//     VARCHAR (Varchar): one declaration a statement, and the index sought.

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	mssqldrv "github.com/microsoft/go-mssqldb"

	"github.com/yakugate/yakugate/internal/dal/stmt"
)

// RecordStmts is the record store's statements (records*.go), by name. A day
// table is written {mt}, {sub}, {dlr}, {rej}, {mo}, {out}, {hlr} or {orph}
// in a registered text; rOn names it at the call.
var RecordStmts = newRegistry()

// RecordStatements is the registry of the record store's statements.
func RecordStatements() *stmt.Registry { return RecordStmts }

// rOn is a registered text with its day tables named: pairs of placeholder
// ("mt") and table ("mt_12_20261030").
func rOn(text string, pairs ...string) string {
	if len(pairs) == 0 {
		return text
	}
	r := make([]string, 0, len(pairs))
	for i := 0; i+1 < len(pairs); i += 2 {
		r = append(r, "{"+pairs[i]+"}", quoteIdent(pairs[i+1]))
	}
	return strings.NewReplacer(r...).Replace(text)
}

// rExec runs a registered statement (its text as given) and counts it.
func rExec(ctx context.Context, x Querier, st *stmt.Stmt, text string, args ...any) (sql.Result, error) {
	start := time.Now()
	res, err := x.ExecContext(ctx, text, args...)
	var n int64
	if err == nil {
		n, _ = res.RowsAffected()
	}
	observe(st, start, n)
	return res, err
}

// rQuery runs a registered read and counts it.
func rQuery(ctx context.Context, x Querier, st *stmt.Stmt, text string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	rows, err := x.QueryContext(ctx, text, args...)
	observe(st, start, 0)
	return rows, err
}

// rRow runs a registered read of one row and counts it.
func rRow(ctx context.Context, x Querier, st *stmt.Stmt, text string, args ...any) *sql.Row {
	start := time.Now()
	row := x.QueryRowContext(ctx, text, args...)
	observe(st, start, 1)
	return row
}

// rText is a text value as NVARCHAR(MAX): one declaration for every call.
func rText(s string) mssqldrv.NVarCharMax { return mssqldrv.NVarCharMax(s) }

// rJSON is rows as one JSON parameter: an array of arrays, each a row's
// values in its WITH clause's order, then the row's place (jRows).
func rJSON(rows [][]any) mssqldrv.NVarCharMax {
	placed := make([][]any, len(rows))
	for i, r := range rows {
		placed[i] = append(r[:len(r):len(r)], i)
	}
	b, _ := json.Marshal(placed)
	return mssqldrv.NVarCharMax(b)
}

// rIDs is message ids as one JSON parameter (an array of strings).
func rIDs(ids []string) mssqldrv.NVarCharMax {
	if ids == nil {
		ids = []string{}
	}
	b, _ := json.Marshal(ids)
	return mssqldrv.NVarCharMax(b)
}

// rIDsFrom is the ids read with OPENJSON, driving a loop join into the table
// that follows: one seek an id, in the list's order.
const rIDsFrom = "OPENJSON(@p1) WITH (v CHAR(26) COLLATE Latin1_General_100_BIN2 '$') AS j INNER LOOP JOIN "

// jcol is one column of a JSON row: its name and the type it is read as.
type jcol struct{ name, typ string }

// The types JSON values are read as.
const (
	jID   = "CHAR(26) COLLATE Latin1_General_100_BIN2" // a message id: the index is sought
	jTxt  = "NVARCHAR(4000)"
	jMax  = "NVARCHAR(MAX)"
	jBig  = "BIGINT"
	jInt  = "INT"
	jTime = "DATETIME2(3)"
	jSec  = "DATETIME2(0)"
	jDec  = "DECIMAL(24,12)"
)

// jWith is the WITH clause reading cols from each row array, by position.
func jWith(cols []jcol) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = c.name + " " + c.typ + " '$[" + strconv.Itoa(i) + "]'"
	}
	return "WITH (" + strings.Join(parts, ", ") + ")"
}

// jRows is the JSON rows of @p1 read with cols; jInOrder, at the end of an
// INSERT … SELECT, numbers the new rows' IDENTITY ids in the array's order,
// as MySQL numbers a multi-row VALUES list: rJSON gives every row its place
// as one more value at its end (yk_i), which a WITH clause that does not
// name it leaves alone.
func jRows(cols []jcol) string {
	return "OPENJSON(@p1) " + jWith(append(append([]jcol(nil), cols...), jcol{"yk_i", jInt})) + " AS j"
}

const jInOrder = " ORDER BY j.yk_i"

// jNames is the columns' names, joined; with prefix "j." the JSON's values.
func jNames(cols []jcol, prefix string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = prefix + c.name
	}
	return strings.Join(parts, ", ")
}

// jcols is a column list "a,b,c" with one type each: a shorthand for jcol
// lists whose types follow the names.
func jcols(spec ...string) []jcol {
	out := make([]jcol, 0, len(spec)/2)
	for i := 0; i+1 < len(spec); i += 2 {
		out = append(out, jcol{spec[i], spec[i+1]})
	}
	return out
}

// ---- values, as MySQL's helpers wrote them ----

// rTimeText is a time as its JSON text (UTC, to the millisecond); the zero
// time is null (MySQL's ackAtArg).
func rTimeText(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000")
}

// rSecText is a Unix second as its JSON text (UTC): a summary's bucket.
func rSecText(unix int64) string {
	return time.Unix(unix, 0).UTC().Format("2006-01-02T15:04:05")
}

// rDec is a conversion rate as decimal text, null when there is none
// (MySQL's nsFx).
func rDec(v float64) any {
	if v <= 0 {
		return nil
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// rNZ is a number, null for 0 (MySQL's nz).
func rNZ(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

// rNZi is rNZ for an int (MySQL's nzInt).
func rNZi(v int) any {
	if v == 0 {
		return nil
	}
	return v
}

// rStr is a text, null for "" (MySQL's nsStr).
func rStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// rBool is a flag as 0/1 (MySQL's boolInt).
func rBool(b bool) int {
	if b {
		return 1
	}
	return 0
}

// rOr is v, or def when v is "" (MySQL's orStr).
func rOr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// rTrunc bounds a value to a column width in bytes (MySQL's trunc).
func rTrunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// rRunes cuts s to n characters, as LEFT() does (MySQL's truncRunes).
func rRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// rTrim is a CHAR column read back without its padding.
func rTrim(s string) string { return strings.TrimRight(s, " ") }

// rChunk is the most rows one statement carries: the receipts' 500, as on
// MySQL.
const rChunk = 500
