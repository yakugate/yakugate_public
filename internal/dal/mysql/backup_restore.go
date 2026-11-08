package mysql

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// A target database as the restore loads a backup run into it (Restorer) —
// moved here unchanged on 30 Oct 2026 (the DAL plan) from
// internal/process/backup/restore.go (the connection, tableState,
// tableTriggers, tableColumns, restoreTable, columnValue, mysqlTime,
// restoreTriggers). Which tables, the manifest's checks and the refusals
// stayed in the role.

var (
	stRestoreDatabase = BackupStmts.Add("restore.database", "SELECT COALESCE(DATABASE(),'')", false)
	stRestoreNoFKs    = BackupStmts.Add("restore.no_foreign_keys", "SET FOREIGN_KEY_CHECKS=0", false)
	stRestoreSQLMode  = BackupStmts.Add("restore.sql_mode", "SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES'", false)
	stRestoreTableHas = BackupStmts.Add("restore.table_exists", `SELECT COUNT(*) FROM information_schema.tables
		 WHERE table_schema = DATABASE() AND table_name = ?`, false)
	stRestoreTableRows = BackupStmts.Add("restore.table_has_rows", "SELECT 1 FROM `<table>` LIMIT 1", false)
	stRestoreInsertTrs = BackupStmts.Add("restore.insert_triggers", `SELECT trigger_name FROM information_schema.triggers
		 WHERE trigger_schema = DATABASE() AND event_object_table = ? AND event_manipulation = 'INSERT'`, false)
	stRestoreColumns = BackupStmts.Add("restore.columns", `SELECT column_name, data_type, extra FROM information_schema.columns
		 WHERE table_schema = DATABASE() AND table_name = ?`, false)
	stRestoreCreate  = BackupStmts.Add("restore.create_table", "CREATE TABLE `<table>` (<the run's saved CREATE TABLE>)", false)
	stRestoreInsert  = BackupStmts.Add("restore.insert_rows", "INSERT INTO `<table>` (`<column>`,`<column>`) VALUES (?,?),(?,?)", false)
	stRestoreTrigger = BackupStmts.Add("restore.trigger_exists", `SELECT COUNT(*) FROM information_schema.triggers
			 WHERE trigger_schema = DATABASE() AND trigger_name = ?`, false)
	stRestoreMakeTrigger = BackupStmts.Add("restore.create_trigger", "CREATE TRIGGER `<trigger>` <the run's saved statement>", false)
)

const (
	restoreBatchRows  = 500
	restoreBatchBytes = 1 << 20 // well under any max_allowed_packet
	maxPlaceholders   = 60000   // MySQL allows 65535 per statement
)

// RestoreBatchRows is the most rows one INSERT of a restore carries (for the
// role's tests, which cross a batch's end).
const RestoreBatchRows = restoreBatchRows

// A restore's target (a connection of its own).
var _ = serves(func(d *dal.Driver) { d.Restorer = openRestorer })

// openRestorer is the restore's opener: a database handle of its own on the
// target DSN (never the process's pool — the restore is a command, not a
// role) and ONE connection of it for the whole restore, since the session
// settings must hold for every statement.
func openRestorer(ctx context.Context, dsn string) (dal.Restorer, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// One connection for the whole restore: the session settings below must
	// hold for every statement.
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	r := &Restorer{db: db, conn: conn}
	if err := conn.QueryRowContext(ctx, stRestoreDatabase.SQL).Scan(&r.target); err != nil {
		r.Close()
		return nil, err
	}
	if r.target == "" {
		r.Close()
		return nil, errors.New("the DSN names no database")
	}
	for _, q := range []string{
		// Tables load in name order, not dependency order.
		stRestoreNoFKs.SQL,
		// Keep an id of 0 as 0; stay strict so nothing is cut short silently;
		// zero dates in old rows are allowed back in.
		stRestoreSQLMode.SQL,
	} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			r.Close()
			return nil, fmt.Errorf("%s: %w", q, err)
		}
	}
	return r, nil
}

// Restorer is one MySQL target of a restore, on one connection.
type Restorer struct {
	db     *sql.DB
	conn   *sql.Conn
	target string
}

// Target is the database's name on the target server.
func (r *Restorer) Target() string { return r.target }

// Close lets go of the connection, then of the handle.
func (r *Restorer) Close() error {
	cerr := r.conn.Close()
	if err := r.db.Close(); err != nil {
		return err
	}
	return cerr
}

// CreateTable runs a CREATE TABLE statement saved in the run.
func (r *Restorer) CreateTable(ctx context.Context, ddl string) error {
	_, err := r.conn.ExecContext(ctx, ddl)
	return err
}

// TableState says whether the table exists and has rows.
func (r *Restorer) TableState(ctx context.Context, t string) (exists, hasRows bool, err error) {
	conn := r.conn
	var n int
	if err = conn.QueryRowContext(ctx, stRestoreTableHas.SQL, t).Scan(&n); err != nil || n == 0 {
		return false, false, err
	}
	var one int
	err = conn.QueryRowContext(ctx, "SELECT 1 FROM `"+t+"` LIMIT 1").Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return true, false, nil
	}
	return true, err == nil, err
}

// InsertTriggers names the table's triggers that fire on INSERT.
func (r *Restorer) InsertTriggers(ctx context.Context, t string) ([]string, error) {
	rows, err := r.conn.QueryContext(ctx, stRestoreInsertTrs.SQL, t)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// column is what restore needs to know about one target column.
type column struct {
	dataType  string // information_schema DATA_TYPE, lower case
	generated bool   // computed by the server; cannot be inserted
}

func tableColumns(ctx context.Context, conn *sql.Conn, t string) (map[string]column, error) {
	rows, err := conn.QueryContext(ctx, stRestoreColumns.SQL, t)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]column{}
	for rows.Next() {
		var name, typ, extra string
		if err := rows.Scan(&name, &typ, &extra); err != nil {
			return nil, err
		}
		extra = strings.ToUpper(extra)
		// Not DEFAULT_GENERATED: that is an ordinary column with an expression default.
		gen := strings.Contains(extra, "VIRTUAL GENERATED") || strings.Contains(extra, "STORED GENERATED") ||
			strings.Contains(extra, "PERSISTENT")
		out[name] = column{dataType: strings.ToLower(typ), generated: gen}
	}
	return out, rows.Err()
}

// RestoreTable loads one data file inside one transaction.
func (r *Restorer) RestoreTable(ctx context.Context, t, path string, want *int64) (n int64, err error) {
	conn := r.conn
	cols, err := tableColumns(ctx, conn, t)
	if err != nil {
		return 0, err
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return 0, err
	}
	defer gz.Close()
	dec := json.NewDecoder(gz)

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	var names []string // insert column list, fixed by the first row
	var args []any
	var batchRows, batchBytes int
	flush := func() error {
		if batchRows == 0 {
			return nil
		}
		one := "(" + strings.TrimSuffix(strings.Repeat("?,", len(names)), ",") + ")"
		q := "INSERT INTO `" + t + "` (`" + strings.Join(names, "`,`") + "`) VALUES " +
			strings.TrimSuffix(strings.Repeat(one+",", batchRows), ",")
		_, err := tx.ExecContext(ctx, q, args...)
		args, batchRows, batchBytes = args[:0], 0, 0
		return err
	}
	maxRows := restoreBatchRows
	for {
		var rec map[string]any
		if err = dec.Decode(&rec); err == io.EOF {
			break
		} else if err != nil {
			return n, fmt.Errorf("row %d: %w", n+1, err)
		}
		if names == nil {
			for c := range rec {
				col, ok := cols[c]
				if !ok {
					return n, fmt.Errorf("column %q is in the backup but not in the target table", c)
				}
				if !col.generated {
					names = append(names, c)
				}
			}
			sort.Strings(names)
			if len(names) == 0 {
				return n, errors.New("no insertable columns")
			}
			if m := maxPlaceholders / len(names); m < maxRows {
				maxRows = max(m, 1)
			}
		}
		for _, c := range names {
			raw, ok := rec[c]
			if !ok {
				return n, fmt.Errorf("row %d: column %q is missing", n+1, c)
			}
			v, size, err := columnValue(raw, cols[c].dataType)
			if err != nil {
				return n, fmt.Errorf("row %d column %q: %w", n+1, c, err)
			}
			args = append(args, v)
			batchBytes += size
		}
		n++
		batchRows++
		if batchRows >= maxRows || batchBytes >= restoreBatchBytes {
			if err = flush(); err != nil {
				return n, err
			}
		}
	}
	if err = flush(); err != nil {
		return n, err
	}
	if want != nil && n != *want {
		err = fmt.Errorf("read %d rows, the manifest says %d", n, *want)
		return n, err
	}
	err = tx.Commit()
	return n, err
}

// columnValue turns one dumped JSON value back into an INSERT argument. It is
// the inverse of DumpTable: null = NULL, {"b64":…} = bytes, everything else is
// the text MySQL itself printed, which MySQL reads back for any column type
// (numbers, decimals, JSON, enums). The one text form that is NOT MySQL's own
// is a date: a DSN with parseTime=true makes the driver hand DumpTable a
// time.Time, which lands in the dump as RFC 3339 ("2026-09-17T10:00:00Z").
func columnValue(raw any, dataType string) (any, int, error) {
	switch v := raw.(type) {
	case nil:
		return nil, 0, nil
	case string:
		switch dataType {
		case "datetime", "timestamp", "date":
			v = mysqlTime(v, dataType == "date")
		case "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary":
			return []byte(v), len(v), nil // a dump older than the b64 form
		}
		return v, len(v), nil
	case map[string]any:
		s, ok := v[binaryKey].(string)
		if !ok || len(v) != 1 {
			return nil, 0, errors.New("unknown value form")
		}
		b, err := base64.StdEncoding.DecodeString(s)
		return b, len(b), err
	}
	return nil, 0, fmt.Errorf("unexpected JSON value %T", raw)
}

// mysqlTime rewrites an RFC 3339 stamp as MySQL's own, keeping the wall clock
// exactly as dumped (the offset the driver printed is its `loc`, not a fact
// about the column). Anything else passes through untouched.
func mysqlTime(s string, dateOnly bool) string {
	if len(s) < 20 || s[10] != 'T' {
		return s
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return s
	}
	if t.Year() <= 1 { // the driver's reading of MySQL's zero date
		if dateOnly {
			return "0000-00-00"
		}
		return "0000-00-00 00:00:00"
	}
	if dateOnly {
		return t.Format("2006-01-02")
	}
	return t.Format("2006-01-02 15:04:05.999999")
}

// RestoreTriggers creates the saved triggers the target does not have yet.
// The file's name is the role's; errors name it as the role always did.
func (r *Restorer) RestoreTriggers(ctx context.Context, path string) ([]string, error) {
	conn := r.conn
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var trs []savedTrigger
	if err := json.Unmarshal(body, &trs); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	var created []string
	for _, tr := range trs {
		var n int
		if err := conn.QueryRowContext(ctx, stRestoreTrigger.SQL, tr.Name).Scan(&n); err != nil {
			return created, err
		}
		if n > 0 {
			continue
		}
		if _, err := conn.ExecContext(ctx, tr.Create); err != nil {
			return created, fmt.Errorf("create trigger %s: %w", tr.Name, err)
		}
		created = append(created, tr.Name)
	}
	return created, nil
}

var _ dal.Restorer = (*Restorer)(nil)
