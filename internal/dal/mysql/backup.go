package mysql

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
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"unicode/utf8"

	mysqldrv "github.com/go-sql-driver/mysql"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// A database's tables as the backup role reads them into a run's files
// (TableDumps) — moved here unchanged on 30 Oct 2026 (the DAL plan) from
// internal/process/backup/workings.go (listTables, dumpTable, dumpSchema,
// dumpTriggers, isConnErr, target.reads). The schedule, the run's names, the
// manifest and the body sealing's keys stayed in the role. The restore's side
// is backup_restore.go.

// BackupStmts is every statement of backup and restore, by name. A statement
// built at run time (a table's name, a trigger's, an INSERT's column list) is
// registered in one representative form; the text a call sends is built the
// same way. SELECT * is the backup's nature: a dump is every column of every
// row, whatever the table.
var BackupStmts = stmt.New("mysql")

// BackupStatements is the registry of these statements (the lint test).
func BackupStatements() *stmt.Registry { return BackupStmts }

var (
	stBackupTables = BackupStmts.Add("backup.tables",
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = DATABASE() AND table_type='BASE TABLE' ORDER BY table_name`, false)
	stBackupRows     = BackupStmts.Add("backup.rows", "SELECT * FROM `<table>`", false)
	stBackupCreate   = BackupStmts.Add("backup.show_create_table", "SHOW CREATE TABLE `<table>`", false)
	stBackupTriggers = BackupStmts.Add("backup.triggers", `SELECT trigger_name, event_object_table FROM information_schema.triggers
		 WHERE trigger_schema = DATABASE() ORDER BY trigger_name`, false)
	stBackupTrigger = BackupStmts.Add("backup.show_create_trigger", "SHOW CREATE TRIGGER `<trigger>`", false)
)

// Backup's reads of a database (the process's one pool and its read
// instance).
var _ = serves(func(d *dal.Driver) { d.TableDumps = openTableDumps })

// openTableDumps is the backup's opener: the process's one pool on the
// database, and its read instance when one is set (dbpool.Reader,
// 2026-10-13). A read instance that cannot be had is no error: the backup
// reads where it writes.
func openTableDumps(dsn string) (dal.TableDumps, error) {
	db, err := dbpool.Get(dsn)
	if err != nil {
		return nil, err
	}
	rd, _ := dbpool.Reader(dsn)
	return &TableDumps{db: db, rd: rd}, nil
}

// TableDumps is one MySQL database as a backup reads it.
type TableDumps struct {
	db *sql.DB
	// rd is where the database is READ for a backup when it has a read
	// instance (dbpool.Reader, 2026-10-13). A backup reads every row of
	// every table; that is the work a read instance is for, and a backup a
	// moment behind the write instance is still a whole backup.
	rd *dbpool.Read
}

// NewTableDumps is TableDumps on a pool the caller holds (rd may be nil:
// no read instance).
func NewTableDumps(db *sql.DB, rd *dbpool.Read) *TableDumps {
	return &TableDumps{db: db, rd: rd}
}

// reads is the pool a backup run reads this database through.
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

// ConnLost says an error is the database connection going away (MySQL
// restarted, refusing connections, a network cut) rather than something
// about the table being read.
func (t *TableDumps) ConnLost(err error) bool { return isConnErr(err) }

// isConnErr says an error is the database connection going away (MySQL
// restarted, refusing connections, a network cut) rather than something
// about the table being read. A body that could not be sealed is the role's
// to tell apart, before it asks.
func isConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysqldrv.ErrInvalidConn) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var me *mysqldrv.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1040, 1053, 1927, 2006, 2013: // too many connections, shutting down, connection killed, gone away, lost
			return true
		}
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	s := strings.ToLower(err.Error())
	for _, w := range []string{"invalid connection", "bad connection", "connection refused", "connection reset",
		"broken pipe", "server has gone away", "lost connection", "too many connections"} {
		if strings.Contains(s, w) {
			return true
		}
	}
	return false
}

// tableReads is one pool a dump reads through.
type tableReads struct{ db *sql.DB }

// Tables returns the base tables of the connected schema.
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

// binaryKey marks a value written as base64 because JSON cannot carry it.
const binaryKey = "b64"

func isBinaryType(dbType string) bool {
	switch strings.ToUpper(dbType) {
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY", "BIT", "GEOMETRY":
		return true
	}
	return false
}

// DumpTable streams SELECT * FROM tbl as one JSON object per row into a gzip file.
// Values are emitted as strings (or null) — a faithful snapshot; restore tooling
// coerces types from the target schema. The one exception is BINARY data (a
// BLOB/BINARY column, or any value that is not valid UTF-8): JSON cannot carry
// raw bytes — the encoder swaps them for U+FFFD, which silently ruined wrapped
// data keys — so those are written as {"b64":"<base64>"} (2026-09-17). Message BODY columns are sealed with the
// gateway crypto envelope before writing (D-104, body-only artifact encryption):
// a value already encrypted at rest passes through untouched (no double-seal),
// everything else is exactly as protected as the filesystem — deliberately, per
// the PO's "body only" ruling. The sealing itself is the role's (seal).
func (r tableReads) DumpTable(ctx context.Context, tbl, path string, mode os.FileMode, sb dal.BodySeal) (n int, err error) {
	rows, err := r.db.QueryContext(ctx, "SELECT * FROM `"+tbl+"`")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	binary := make([]bool, len(cols))
	if cts, err := rows.ColumnTypes(); err == nil {
		for i, ct := range cts {
			binary[i] = isBinaryType(ct.DatabaseTypeName())
		}
	}
	bodyIdx, acctIdx := -1, -1
	for i, c := range cols {
		switch c {
		case "body":
			bodyIdx = i
		case "account_id":
			acctIdx = i
		}
	}
	// Sealing needs the gateway key. Without one (a deployment whose companies
	// each bring their own KMS), bodies already encrypted under a company key
	// stay encrypted in the dump and plain ones stay plain, with a warning —
	// the same as having no crypto at all, rather than failing every backup.
	seal := sb.On
	if bodyIdx >= 0 && !seal && sb.Plain != nil {
		sb.Plain()
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return 0, err
	}
	gz := gzip.NewWriter(f)
	// A close that fails is a file that is not whole: it must fail the table,
	// not be hashed into the manifest as if it were good.
	defer func() {
		if cerr := gz.Close(); err == nil {
			err = cerr
		}
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	enc := json.NewEncoder(gz)

	raw := make([]sql.RawBytes, len(cols))
	scan := make([]any, len(cols))
	for i := range raw {
		scan[i] = &raw[i]
	}
	for rows.Next() {
		if err := rows.Scan(scan...); err != nil {
			return n, err
		}
		rec := make(map[string]any, len(cols))
		for i, c := range cols {
			switch {
			case raw[i] == nil:
				rec[c] = nil
			case binary[i] || !utf8.Valid(raw[i]):
				rec[c] = map[string]string{binaryKey: base64.StdEncoding.EncodeToString(raw[i])}
			default:
				rec[c] = string(raw[i])
			}
		}
		if seal && bodyIdx >= 0 {
			if v, ok := rec[cols[bodyIdx]].(string); ok && v != "" {
				var acct int64
				if acctIdx >= 0 {
					if a, ok := rec[cols[acctIdx]].(string); ok {
						acct, _ = strconv.ParseInt(a, 10, 64)
					}
				}
				ct, err := sb.Seal(ctx, n+1, acct, v)
				if err != nil {
					return n, err
				}
				rec[cols[bodyIdx]] = ct
			}
		}
		if err := enc.Encode(rec); err != nil {
			return n, err
		}
		n++
	}
	return n, rows.Err()
}

// DumpSchema writes the table's CREATE TABLE statement next to its data.
func (r tableReads) DumpSchema(ctx context.Context, tbl, path string, mode os.FileMode) error {
	var name, ddl string
	if err := r.db.QueryRowContext(ctx, "SHOW CREATE TABLE `"+tbl+"`").Scan(&name, &ddl); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(ddl+"\n"), mode)
}

// savedTrigger is one trigger as restore re-creates it (after the rows are in,
// so it does not fire for them).
type savedTrigger struct {
	Name   string `json:"name"`
	Table  string `json:"table"`
	Create string `json:"create"`
}

// definerRe drops "DEFINER=`user`@`host`": the account may not exist where the
// backup is restored, and creating a trigger for someone else needs SUPER.
var definerRe = regexp.MustCompile("(?i)\\s+DEFINER\\s*=\\s*(`[^`]*`|\\S+?)@(`[^`]*`|\\S+)")

// DumpTriggers writes the database's triggers; no triggers, no file.
func (r tableReads) DumpTriggers(ctx context.Context, path string, mode os.FileMode) error {
	db := r.db
	rows, err := db.QueryContext(ctx, stBackupTriggers.SQL)
	if err != nil {
		return err
	}
	var trs []savedTrigger
	for rows.Next() {
		var t savedTrigger
		if err := rows.Scan(&t.Name, &t.Table); err != nil {
			rows.Close()
			return err
		}
		trs = append(trs, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(trs) == 0 {
		return err
	}
	for i := range trs {
		r, err := db.QueryContext(ctx, "SHOW CREATE TRIGGER `"+trs[i].Name+"`")
		if err != nil {
			return err
		}
		cols, _ := r.Columns()
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for j := range vals {
			ptrs[j] = &vals[j]
		}
		if r.Next() {
			if err := r.Scan(ptrs...); err != nil {
				r.Close()
				return err
			}
			for j, c := range cols {
				if c == "SQL Original Statement" {
					trs[i].Create = definerRe.ReplaceAllString(string(vals[j]), "")
				}
			}
		}
		r.Close()
		if trs[i].Create == "" {
			return fmt.Errorf("trigger %s: no CREATE statement returned", trs[i].Name)
		}
	}
	body, err := json.MarshalIndent(trs, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(body, '\n'), mode)
}

var _ dal.TableDumps = (*TableDumps)(nil)
