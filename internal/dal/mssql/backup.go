package mssql

import (
	"compress/gzip"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/golang-sql/civil"

	"github.com/yakugate/yakugate/internal/dal"
)

// A database's tables as the backup role reads them into a run's files
// (dal.TableDumps), made like the MySQL DAL's internal/dal/mysql/backup.go.
// The restore's side is backup_restore.go.
//
// The files are the MySQL DAL's: one gzipped JSON object a row (values as
// text or null, binary as {"b64": …}), the table's CREATE statement beside
// it, the database's triggers in _triggers.json. What differs, and why:
//   - A table is read a page at a time by its primary key (TOP (n) … WHERE
//     key > the last … ORDER BY key), not in one SELECT *: each statement is
//     short, so the read never holds the row versions of a whole table's
//     read in tempdb (the database reads under READ_COMMITTED_SNAPSHOT and
//     blocks nobody either way). The columns are named from the catalog. A
//     table with no primary key (the hot store's summary_* tables) is read
//     in one statement.
//   - SQL Server has no SHOW CREATE TABLE: the statement is written from the
//     catalog (sys.columns, sys.default_constraints, sys.check_constraints,
//     sys.indexes, sys.computed_columns, sys.foreign_keys) with the
//     database's own constraint names. A foreign key cannot name a table not
//     made yet, and a restore makes the tables in name order, so the keys
//     are written after the table as statements of their own, under
//     fkMarker; the restore adds each once the table it names is there
//     (backup_restore.go).
//   - A value is written as SQL Server prints it: a time 2006-01-02
//     15:04:05.9999999 (a date 2006-01-02, a time of day 15:04:05.9999999,
//     an offset time RFC 3339), a bit 1 or 0, a float in Go's shortest form.
//
// A native backup (BACKUP DATABASE … WITH COMPRESSION, CHECKSUM, and
// RESTORE) would be the SQL Server way to back a database up whole; it is
// not what dal.TableDumps / dal.Restorer are (one table a file, readable and
// restorable table by table), so it is not here.

// BackupStmts is every statement of backup and restore, by name. A statement
// built at run time (a table's columns and key, an ALTER of a key) is
// registered in one representative form.
var BackupStmts = newRegistry()

// dumpPage is how many rows one read of a table takes.
const dumpPage = 5000

// fkMarker starts a saved CREATE statement's foreign keys: one ALTER TABLE …
// ADD CONSTRAINT … FOREIGN KEY a line after it.
const fkMarker = "-- yakugate: foreign keys, each added once the table it names is there"

var (
	stBackupTables = BackupStmts.Add("backup.tables", `SELECT name FROM sys.tables
		WHERE is_ms_shipped = 0 AND schema_id = SCHEMA_ID(N'dbo') ORDER BY name`, false)
	// A table's columns, their types and their place in the primary key
	// (0 = none).
	stBackupColumns = BackupStmts.Add("backup.columns", `SELECT c.name, TYPE_NAME(c.system_type_id), COALESCE(k.key_ordinal, 0)
		  FROM sys.columns c
		  LEFT JOIN (SELECT ic.column_id, ic.key_ordinal FROM sys.indexes i
		               JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
		              WHERE i.object_id = OBJECT_ID(@p1, N'U') AND i.is_primary_key = 1) k ON k.column_id = c.column_id
		 WHERE c.object_id = OBJECT_ID(@p1, N'U') ORDER BY c.column_id`, false)
	stBackupRows = BackupStmts.Add("backup.rows",
		"SELECT TOP (@yk_n) [<column>], [<column>] FROM [<table>] WHERE [<key>] > @k1 ORDER BY [<key>]", false)
	stBackupObject   = BackupStmts.Add("backup.object", "SELECT TOP (1) COALESCE(OBJECT_ID(@p1, N'U'), 0)", false)
	stBackupComputed = BackupStmts.Add("backup.computed", `SELECT name, definition, is_persisted FROM sys.computed_columns
		WHERE object_id = @p1`, false)
	stBackupForeign = BackupStmts.Add("backup.foreign_keys", `SELECT fk.name, OBJECT_NAME(fk.referenced_object_id), pc.name, rc.name,
		       fk.delete_referential_action_desc, fk.update_referential_action_desc
		  FROM sys.foreign_keys fk
		  JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
		  JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
		  JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
		 WHERE fk.parent_object_id = @p1 ORDER BY fk.name, fkc.constraint_column_id`, false)
	stBackupTriggers = BackupStmts.Add("backup.triggers", `SELECT tr.name, OBJECT_NAME(tr.parent_id), OBJECT_DEFINITION(tr.object_id)
		  FROM sys.triggers tr WHERE tr.parent_class = 1 AND tr.is_ms_shipped = 0 ORDER BY tr.name`, false)
)

var _ = serves(func(d *dal.Driver) {
	d.TableDumps = openTableDumps
	d.Restorer = openRestorer
})

// openTableDumps is the backup's opener: the process's one pool on the
// database, and its read instance when one is set. A read instance that
// cannot be had is no error: the backup reads where it writes.
func openTableDumps(dsn string) (dal.TableDumps, error) {
	db, err := Get(dsn)
	if err != nil {
		return nil, err
	}
	rd, _ := Reader(dsn)
	return &TableDumps{db: db, rd: rd}, nil
}

// TableDumps is one SQL Server database as a backup reads it.
type TableDumps struct {
	db *sql.DB
	// rd is where the database is read when it has a read instance (an
	// Availability Group's readable secondary): a backup a moment behind is
	// still a whole backup.
	rd *Read
}

// NewTableDumps is TableDumps on a pool the caller holds (rd may be nil).
func NewTableDumps(db *sql.DB, rd *Read) *TableDumps { return &TableDumps{db: db, rd: rd} }

func (t *TableDumps) reads() *sql.DB {
	if t.rd != nil {
		if d := t.rd.DB(); d != nil {
			return d
		}
	}
	return t.db
}

// Reads is the pool one dump reads through, taken now.
func (t *TableDumps) Reads() dal.TableReads { return tableReads{db: t.reads()} }

// Ping asks the database where a dump reads it.
func (t *TableDumps) Ping(ctx context.Context) error { return t.reads().PingContext(ctx) }

// ConnLost says an error is the connection going away rather than something
// about the table being read.
func (t *TableDumps) ConnLost(err error) bool { return backupConnLost(err) }

// backupConnLost: the driver's own (a bad connection, the stream cut), the
// network's, and SQL Server's numbers for a server going or gone: 233 / 10053
// / 10054 / 10060 / 64 (the connection broken), 6005 (shutting down), 17142
// (paused), 40613 / 4060 (the database not reachable now), 17809 (too many
// connections).
func backupConnLost(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	if n, ok := ErrorNumber(err); ok {
		switch n {
		case 64, 233, 4060, 6005, 10053, 10054, 10060, 17142, 17809, 40613:
			return true
		}
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	s := strings.ToLower(err.Error())
	for _, w := range []string{"bad connection", "connection refused", "connection reset", "broken pipe",
		"i/o timeout", "use of closed network connection", "unable to open tcp connection"} {
		if strings.Contains(s, w) {
			return true
		}
	}
	return false
}

// tableReads is one pool a dump reads through.
type tableReads struct{ db *sql.DB }

// Tables returns the database's base tables, in name order.
func (r tableReads) Tables(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, stBackupTables.SQL)
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

// dumpBinaryKey marks a value written as base64 because JSON cannot carry it.
const dumpBinaryKey = "b64"

// dumpColumn is one column as a dump reads it.
type dumpColumn struct {
	name, typ string
	key       int // its place in the primary key, from 1; 0 = none
}

func dumpIsBinary(typ string) bool {
	switch typ {
	case "binary", "varbinary", "image", "timestamp":
		return true
	}
	return false
}

// dumpColumnsOf reads a table's columns; none = no such table.
func dumpColumnsOf(ctx context.Context, q Querier, table string) ([]dumpColumn, error) {
	rows, err := q.QueryContext(ctx, stBackupColumns.SQL, Ident(table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dumpColumn
	for rows.Next() {
		var c dumpColumn
		if err := rows.Scan(&c.name, &c.typ, &c.key); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("table %s does not exist", table)
	}
	return out, nil
}

// dumpPageSQL is the read of one page of a table after the key given (none on
// the first page), and of the whole table when it has no key.
func dumpPageSQL(table string, cols []dumpColumn, keys []dumpColumn, first bool) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quoteIdent(c.name)
	}
	q := "SELECT "
	if len(keys) > 0 {
		q += "TOP (@yk_n) "
	}
	q += strings.Join(names, ", ") + " FROM " + Ident(table)
	if len(keys) == 0 {
		return q
	}
	if !first {
		// (k1 > @k1) OR (k1 = @k1 AND k2 > @k2) OR …
		var ors []string
		for i := range keys {
			var ands []string
			for j := 0; j < i; j++ {
				ands = append(ands, fmt.Sprintf("%s = @k%d", quoteIdent(keys[j].name), j+1))
			}
			ands = append(ands, fmt.Sprintf("%s > @k%d", quoteIdent(keys[i].name), i+1))
			ors = append(ors, "("+strings.Join(ands, " AND ")+")")
		}
		q += " WHERE " + strings.Join(ors, " OR ")
	}
	order := make([]string, len(keys))
	for i, k := range keys {
		order[i] = quoteIdent(k.name)
	}
	return q + " ORDER BY " + strings.Join(order, ", ")
}

// dumpKeyArg is a key value read back as a parameter of its column's own type
// (a string of a CHAR/VARCHAR column as VARCHAR, a time as DATETIME2 to the
// tick, a date as DATE), so the next page is a seek on the key.
func dumpKeyArg(typ string, v any) any {
	switch x := v.(type) {
	case string:
		if typ == "char" || typ == "varchar" {
			return Varchar(x)
		}
	case time.Time:
		switch typ {
		case "date":
			return civil.DateOf(x)
		case "datetimeoffset":
			return x
		}
		return civil.DateTimeOf(x)
	case []byte:
		if !dumpIsBinary(typ) {
			return string(x) // a decimal's text
		}
	}
	return v
}

// DumpTable writes every row of the table to path as gzipped JSON, one
// object per row, a page at a time in its key's order. Binary values (and
// any that are not valid UTF-8) are written as {"b64": …}; a message body is
// sealed by sb.Seal before it is written (D-104), as on MySQL.
func (r tableReads) DumpTable(ctx context.Context, tbl, path string, mode os.FileMode, sb dal.BodySeal) (n int, err error) {
	cols, err := dumpColumnsOf(ctx, r.db, tbl)
	if err != nil {
		return 0, err
	}
	var keys []dumpColumn
	for k := 1; ; k++ {
		found := false
		for _, c := range cols {
			if c.key == k {
				keys, found = append(keys, c), true
			}
		}
		if !found {
			break
		}
	}
	bodyIdx, acctIdx := -1, -1
	for i, c := range cols {
		switch c.name {
		case "body":
			bodyIdx = i
		case "account_id":
			acctIdx = i
		}
	}
	seal := sb.On
	if bodyIdx >= 0 && !seal && sb.Plain != nil {
		sb.Plain()
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return 0, err
	}
	gz := gzip.NewWriter(f)
	// A close that fails is a file that is not whole: it fails the table.
	defer func() {
		if cerr := gz.Close(); err == nil {
			err = cerr
		}
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	enc := json.NewEncoder(gz)

	keyAt := make([]int, len(keys))
	for i, k := range keys {
		for j, c := range cols {
			if c.name == k.name {
				keyAt[i] = j
			}
		}
	}
	var last []any
	for first := true; ; first = false {
		args := []any{sql.Named("yk_n", dumpPage)}
		for i, v := range last {
			args = append(args, sql.Named(fmt.Sprintf("k%d", i+1), dumpKeyArg(keys[i].typ, v)))
		}
		start := time.Now()
		rows, err := r.db.QueryContext(ctx, dumpPageSQL(tbl, cols, keys, first), args...)
		if err != nil {
			return n, err
		}
		got := 0
		vals := make([]any, len(cols))
		scan := make([]any, len(cols))
		for i := range vals {
			scan[i] = &vals[i]
		}
		for rows.Next() {
			if err := rows.Scan(scan...); err != nil {
				rows.Close()
				return n, err
			}
			rec := make(map[string]any, len(cols))
			for i, c := range cols {
				rec[c.name] = dumpValue(vals[i], c.typ)
			}
			if seal && bodyIdx >= 0 {
				if v, ok := rec[cols[bodyIdx].name].(string); ok && v != "" {
					var acct int64
					if acctIdx >= 0 {
						if a, ok := rec[cols[acctIdx].name].(string); ok {
							acct, _ = strconv.ParseInt(a, 10, 64)
						}
					}
					ct, err := sb.Seal(ctx, n+1, acct, v)
					if err != nil {
						rows.Close()
						return n, err
					}
					rec[cols[bodyIdx].name] = ct
				}
			}
			if err := enc.Encode(rec); err != nil {
				rows.Close()
				return n, err
			}
			n++
			got++
			if len(keys) > 0 {
				last = last[:0]
				for _, at := range keyAt {
					last = append(last, vals[at])
				}
			}
		}
		rows.Close()
		observe(stBackupRows, start, int64(got))
		if err := rows.Err(); err != nil {
			return n, err
		}
		if len(keys) == 0 || got < dumpPage {
			return n, nil
		}
	}
}

// dumpValue is one value as a dump writes it: text, null, or {"b64": …}.
func dumpValue(v any, typ string) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		if dumpIsBinary(typ) || !utf8.Valid(x) {
			return map[string]string{dumpBinaryKey: base64.StdEncoding.EncodeToString(x)}
		}
		return string(x)
	case string:
		if !utf8.ValidString(x) {
			return map[string]string{dumpBinaryKey: base64.StdEncoding.EncodeToString([]byte(x))}
		}
		return x
	case bool:
		if x {
			return "1"
		}
		return "0"
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		if typ == "real" {
			return strconv.FormatFloat(x, 'g', -1, 32)
		}
		return strconv.FormatFloat(x, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case time.Time:
		switch typ {
		case "date":
			return x.Format("2006-01-02")
		case "time":
			return x.Format("15:04:05.9999999")
		case "datetimeoffset":
			return x.Format(time.RFC3339Nano)
		}
		return x.Format("2006-01-02 15:04:05.9999999")
	}
	return fmt.Sprint(v)
}

// DumpSchema writes the table's CREATE TABLE statement (and its indexes'),
// from the catalog, with its foreign keys after fkMarker.
func (r tableReads) DumpSchema(ctx context.Context, tbl, path string, mode os.FileMode) error {
	ddl, err := createStatement(ctx, r.db, tbl)
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(ddl+"\n"), mode)
}

// createStatement is a table's CREATE statement as the catalog has it.
func createStatement(ctx context.Context, q catalogQuerier, tbl string) (string, error) {
	var id int64
	if err := q.QueryRowContext(ctx, stBackupObject.SQL, Ident(tbl)).Scan(&id); err != nil {
		return "", err
	}
	if id == 0 {
		return "", fmt.Errorf("table %s does not exist", tbl)
	}
	sh, err := readShape(ctx, q, id)
	if err != nil {
		return "", err
	}
	computed := map[string]string{}
	rows, err := q.QueryContext(ctx, stBackupComputed.SQL, id)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var name, def string
		var persisted bool
		if err := rows.Scan(&name, &def, &persisted); err != nil {
			rows.Close()
			return "", err
		}
		computed[name] = schemaQuote(name) + " AS " + def
		if persisted {
			computed[name] += " PERSISTED"
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", err
	}

	var lines []string
	for _, c := range sh.columns {
		if def, ok := computed[c.name]; ok {
			lines = append(lines, "  "+def)
			continue
		}
		s := schemaQuote(c.name) + " " + c.typeSQL()
		if c.isText() && c.collation != "" {
			s += " COLLATE " + c.collation
		}
		if c.identity {
			s += " IDENTITY(1,1)"
		}
		if c.nullable {
			s += " NULL"
		} else {
			s += " NOT NULL"
		}
		if c.defSQL != "" {
			s += " CONSTRAINT " + schemaQuote(c.defName) + " DEFAULT " + c.defSQL
		}
		lines = append(lines, "  "+s)
	}
	for _, ix := range sh.indexes {
		if ix.primary {
			kind := " NONCLUSTERED"
			if ix.clustered {
				kind = " CLUSTERED"
			}
			lines = append(lines, "  CONSTRAINT "+schemaQuote(ix.name)+" PRIMARY KEY"+kind+" ("+ix.keyList()+")")
		}
	}
	for _, ck := range sh.checks {
		lines = append(lines, "  CONSTRAINT "+schemaQuote(ck[0])+" CHECK "+ck[1])
	}
	var b strings.Builder
	b.WriteString("CREATE TABLE " + schemaQuote(tbl) + " (\n" + strings.Join(lines, ",\n") + "\n);\n")
	for _, ix := range sh.indexes {
		if !ix.primary {
			b.WriteString(ix.createSQL(tbl))
		}
	}

	fks, err := foreignKeys(ctx, q, id, tbl)
	if err != nil {
		return "", err
	}
	if len(fks) > 0 {
		b.WriteString(fkMarker + "\n")
		for _, fk := range fks {
			b.WriteString(fk + "\n")
		}
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

// foreignKeys are a table's foreign keys, each as one ALTER TABLE … WITH
// NOCHECK ADD CONSTRAINT statement on one line.
func foreignKeys(ctx context.Context, q catalogQuerier, id int64, tbl string) ([]string, error) {
	rows, err := q.QueryContext(ctx, stBackupForeign.SQL, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type fk struct {
		name, ref, onDelete, onUpdate string
		cols, refCols                 []string
	}
	var all []*fk
	for rows.Next() {
		var name, ref, col, refCol, del, upd string
		if err := rows.Scan(&name, &ref, &col, &refCol, &del, &upd); err != nil {
			return nil, err
		}
		if len(all) == 0 || all[len(all)-1].name != name {
			all = append(all, &fk{name: name, ref: ref, onDelete: del, onUpdate: upd})
		}
		k := all[len(all)-1]
		k.cols, k.refCols = append(k.cols, schemaQuote(col)), append(k.refCols, schemaQuote(refCol))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]string, len(all))
	for i, k := range all {
		s := fmt.Sprintf("ALTER TABLE %s WITH NOCHECK ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
			schemaQuote(tbl), schemaQuote(k.name), strings.Join(k.cols, ", "), schemaQuote(k.ref), strings.Join(k.refCols, ", "))
		if a := strings.ReplaceAll(k.onDelete, "_", " "); a != "NO ACTION" {
			s += " ON DELETE " + a
		}
		if a := strings.ReplaceAll(k.onUpdate, "_", " "); a != "NO ACTION" {
			s += " ON UPDATE " + a
		}
		out[i] = s + ";"
	}
	return out, nil
}

// savedTrigger is one trigger as restore makes it again (after the rows are
// in, so it does not fire for them).
type savedTrigger struct {
	Name   string `json:"name"`
	Table  string `json:"table"`
	Create string `json:"create"`
}

// DumpTriggers writes the database's triggers (their CREATE TRIGGER text as
// the catalog keeps it); no triggers, no file.
func (r tableReads) DumpTriggers(ctx context.Context, path string, mode os.FileMode) error {
	rows, err := r.db.QueryContext(ctx, stBackupTriggers.SQL)
	if err != nil {
		return err
	}
	var trs []savedTrigger
	for rows.Next() {
		var t savedTrigger
		var def sql.NullString
		if err := rows.Scan(&t.Name, &t.Table, &def); err != nil {
			rows.Close()
			return err
		}
		if !def.Valid || def.String == "" {
			rows.Close()
			return fmt.Errorf("trigger %s: no CREATE statement returned", t.Name)
		}
		t.Create = def.String
		trs = append(trs, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(trs) == 0 {
		return err
	}
	body, err := json.MarshalIndent(trs, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(body, '\n'), mode)
}

var _ dal.TableDumps = (*TableDumps)(nil)
