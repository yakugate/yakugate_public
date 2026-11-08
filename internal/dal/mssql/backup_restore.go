package mssql

import (
	"bufio"
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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	mssqldrv "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/yakugate/yakugate/internal/dal"
)

// A target database as the restore loads a backup run into it
// (dal.Restorer), made like the MySQL DAL's
// internal/dal/mysql/backup_restore.go: one connection the whole restore
// long, a table's rows in one transaction, the triggers last.
//
// What differs, and why:
//   - The rows go in with the server's bulk load (INSERT BULK, go-mssqldb's
//     CopyIn), a batch of loadBatch rows a statement, all in the table's
//     one transaction. Checks are checked and triggers fired, as MySQL's
//     INSERT does. An identity column's values are the run's (a bulk load
//     keeps the values it is given; MySQL's NO_AUTO_VALUE_ON_ZERO), and the
//     identity carries on after the highest.
//   - Foreign keys off (MySQL's FOREIGN_KEY_CHECKS=0) is per table here: the
//     table's own keys are switched off for its load (NOCHECK CONSTRAINT),
//     on again in the same transaction, and then checked against the rows
//     (WITH CHECK CHECK CONSTRAINT) after it commits. A key whose rows do
//     not all have their parent yet (a table loaded before the table it
//     names) is checked again at the end (RestoreTriggers, Close); one that
//     still fails stays on but "not trusted", as the rows MySQL loaded with
//     its checks off were never checked either.
//   - A saved CREATE statement's foreign keys (after fkMarker) are added
//     once the table each names is there: at once, or at a later
//     CreateTable, RestoreTriggers or Close (the role does not read Close's
//     answer: a key whose table never came is left out silently).
//   - Strict is SQL Server's own way (a value that does not fit is an
//     error).

const loadBatch = 5000

var (
	stRestoreDatabase = BackupStmts.Add("restore.database", "SELECT TOP (1) COALESCE(DB_NAME(), '')", false)
	stRestoreSession  = BackupStmts.Add("restore.session", "SET XACT_ABORT ON; SET ANSI_WARNINGS ON", false)
	stRestoreTableHas = BackupStmts.Add("restore.table_exists",
		"SELECT TOP (1) CASE WHEN OBJECT_ID(@p1, N'U') IS NULL THEN 0 ELSE 1 END", false)
	stRestoreTableRows = BackupStmts.Add("restore.table_has_rows", "SELECT TOP (1) 1 FROM [<table>]", false)
	stRestoreInsertTrs = BackupStmts.Add("restore.insert_triggers", `SELECT tr.name FROM sys.triggers tr
		  JOIN sys.trigger_events te ON te.object_id = tr.object_id
		 WHERE tr.parent_id = OBJECT_ID(@p1, N'U') AND te.type_desc = N'INSERT'`, false)
	stRestoreColumns = BackupStmts.Add("restore.columns", `SELECT c.name, TYPE_NAME(c.system_type_id),
		       CAST(CASE WHEN c.is_computed = 1 OR TYPE_NAME(c.system_type_id) = 'timestamp' THEN 1 ELSE 0 END AS BIT), c.is_identity
		  FROM sys.columns c WHERE c.object_id = OBJECT_ID(@p1, N'U')`, false)
	stRestoreForeign = BackupStmts.Add("restore.foreign_keys", `SELECT name FROM sys.foreign_keys
		WHERE parent_object_id = OBJECT_ID(@p1, N'U') AND is_disabled = 0`, false)
	stRestoreKeyOff   = BackupStmts.Add("restore.key_off", "ALTER TABLE [<table>] NOCHECK CONSTRAINT [<key>]", false)
	stRestoreKeyOn    = BackupStmts.Add("restore.key_on", "ALTER TABLE [<table>] CHECK CONSTRAINT [<key>]", false)
	stRestoreKeyCheck = BackupStmts.Add("restore.key_checked", "ALTER TABLE [<table>] WITH CHECK CHECK CONSTRAINT [<key>]", false)
	stRestoreCreate   = BackupStmts.Add("restore.create_table", "CREATE TABLE [<table>] (<the run's saved CREATE TABLE>)", false)
	stRestoreLoad     = BackupStmts.Add("restore.load_rows",
		"INSERT BULK [<table>] ([<column>], [<column>]) WITH (CHECK_CONSTRAINTS, FIRE_TRIGGERS, KEEP_NULLS)", false)
	stRestoreIdentityOff = BackupStmts.Add("restore.identity_off", "SET IDENTITY_INSERT [<table>] OFF", false)
	stRestoreTrigger     = BackupStmts.Add("restore.trigger_exists",
		"SELECT COUNT(*) FROM sys.triggers WHERE name = @p1", false)
	stRestoreMakeTrigger = BackupStmts.Add("restore.create_trigger", "CREATE TRIGGER [<trigger>] <the run's saved statement>", false)
	stRestoreAddKey      = BackupStmts.Add("restore.add_foreign_key",
		"ALTER TABLE [<table>] WITH NOCHECK ADD CONSTRAINT [<key>] FOREIGN KEY (<columns>) REFERENCES [<table>] (<columns>)", false)
)

// RestoreBatchRows is the most rows one bulk statement of a restore carries
// (for tests that cross a batch's end).
const RestoreBatchRows = loadBatch

// openRestorer is the restore's opener: a handle of its own on the target
// DSN (the restore is a command, not a role) and ONE connection of it for
// the whole restore.
func openRestorer(ctx context.Context, dsn string) (dal.Restorer, error) {
	c, err := msdsn.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if c.Database == "" {
		return nil, errors.New("the DSN names no database")
	}
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
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
	if _, err := conn.ExecContext(ctx, stRestoreSession.SQL); err != nil {
		r.Close()
		return nil, fmt.Errorf("%s: %w", stRestoreSession.SQL, err)
	}
	return r, nil
}

// Restorer is one SQL Server target of a restore, on one connection.
type Restorer struct {
	db     *sql.DB
	conn   *sql.Conn
	target string
	// pending are saved foreign keys not added yet: the table they name is
	// not there yet.
	pending []pendingKey
	// unchecked are the foreign keys this restore added or switched off and
	// on whose rows were not checked yet (table, key).
	unchecked [][2]string
}

// pendingKey is one saved foreign key, its table and the table it names.
type pendingKey struct {
	table, name, ref, ddl string
}

// checkKeys checks each foreign key this restore touched against the rows
// now in, so the server may trust it; one whose rows are not all matched
// (a parent not restored) stays on, not trusted.
func (r *Restorer) checkKeys(ctx context.Context) {
	var left [][2]string
	for _, k := range r.unchecked {
		if _, err := r.conn.ExecContext(ctx, restoreKeySQL(stRestoreKeyCheck.SQL, k[0], k[1])); err != nil {
			left = append(left, k)
		}
	}
	r.unchecked = left
}

// Target is the database's name on the target server.
func (r *Restorer) Target() string { return r.target }

// Close adds the foreign keys still waiting whose tables came, checks the
// keys the restore touched, then lets go of the connection and the handle.
func (r *Restorer) Close() error {
	if len(r.pending) > 0 || len(r.unchecked) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		_ = r.addPending(ctx)
		r.checkKeys(ctx)
		cancel()
	}
	cerr := r.conn.Close()
	if err := r.db.Close(); err != nil {
		return err
	}
	return cerr
}

// restoreRefRe finds the table a saved foreign key names; restoreKeyNameRe its own table
// and its name.
var (
	restoreRefRe     = regexp.MustCompile(`REFERENCES \[((?:[^\]]|\]\])+)\]`)
	restoreKeyNameRe = regexp.MustCompile(`^ALTER TABLE \[((?:[^\]]|\]\])+)\] WITH NOCHECK ADD CONSTRAINT \[((?:[^\]]|\]\])+)\]`)
)

// bracketText is a bracketed name's text.
func bracketText(s string) string { return strings.ReplaceAll(s, "]]", "]") }

// CreateTable runs a CREATE statement saved in the run: the table and its
// indexes, then its foreign keys as the tables they name are there.
func (r *Restorer) CreateTable(ctx context.Context, ddl string) error {
	create, keys, _ := strings.Cut(ddl, fkMarker)
	if _, err := r.conn.ExecContext(ctx, create); err != nil {
		return err
	}
	sc := bufio.NewScanner(strings.NewReader(keys))
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		m, n := restoreRefRe.FindStringSubmatch(line), restoreKeyNameRe.FindStringSubmatch(line)
		if m == nil || n == nil {
			return fmt.Errorf("a saved foreign key names no table: %s", line)
		}
		r.pending = append(r.pending, pendingKey{table: bracketText(n[1]), name: bracketText(n[2]), ref: bracketText(m[1]), ddl: line})
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return r.addPending(ctx)
}

// addPending adds each waiting foreign key whose table is there now.
func (r *Restorer) addPending(ctx context.Context) error {
	var left []pendingKey
	for i, k := range r.pending {
		var has int
		if err := r.conn.QueryRowContext(ctx, stRestoreTableHas.SQL, Ident(k.ref)).Scan(&has); err != nil {
			r.pending = append(left, r.pending[i:]...)
			return err
		}
		if has == 0 {
			left = append(left, k)
			continue
		}
		if _, err := r.conn.ExecContext(ctx, k.ddl); err != nil {
			r.pending = append(left, r.pending[i+1:]...)
			return fmt.Errorf("foreign key: %w", err)
		}
		r.unchecked = append(r.unchecked, [2]string{k.table, k.name})
	}
	r.pending = left
	return nil
}

// TableState says whether the table exists and has rows.
func (r *Restorer) TableState(ctx context.Context, t string) (exists, hasRows bool, err error) {
	var n int
	if err = r.conn.QueryRowContext(ctx, stRestoreTableHas.SQL, Ident(t)).Scan(&n); err != nil || n == 0 {
		return false, false, err
	}
	var one int
	err = r.conn.QueryRowContext(ctx, "SELECT TOP (1) 1 FROM "+Ident(t)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return true, false, nil
	}
	return true, err == nil, err
}

// InsertTriggers names the table's triggers that fire on INSERT.
func (r *Restorer) InsertTriggers(ctx context.Context, t string) ([]string, error) {
	rows, err := r.conn.QueryContext(ctx, stRestoreInsertTrs.SQL, Ident(t))
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

// restoreColumn is what restore needs to know about one target column.
type restoreColumn struct {
	dataType  string // TYPE_NAME of its system type
	generated bool   // computed, or a rowversion: the server writes it
	identity  bool
}

func (r *Restorer) tableColumns(ctx context.Context, t string) (map[string]restoreColumn, error) {
	rows, err := r.conn.QueryContext(ctx, stRestoreColumns.SQL, Ident(t))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]restoreColumn{}
	for rows.Next() {
		var name, typ string
		var gen, ident bool
		if err := rows.Scan(&name, &typ, &gen, &ident); err != nil {
			return nil, err
		}
		out[name] = restoreColumn{dataType: typ, generated: gen, identity: ident}
	}
	return out, rows.Err()
}

func (r *Restorer) foreignKeysOn(ctx context.Context, t string) ([]string, error) {
	rows, err := r.conn.QueryContext(ctx, stRestoreForeign.SQL, Ident(t))
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

// restoreKeySQL is an ALTER of one of a table's foreign keys (a registered form).
func restoreKeySQL(form, t, key string) string {
	return strings.NewReplacer("[<table>]", Ident(t), "[<key>]", quoteIdent(key)).Replace(form)
}

// RestoreTable loads one data file inside one transaction.
func (r *Restorer) RestoreTable(ctx context.Context, t, path string, want *int64) (n int64, err error) {
	cols, err := r.tableColumns(ctx, t)
	if err != nil {
		return 0, err
	}
	if len(cols) == 0 {
		return 0, fmt.Errorf("table %s does not exist", t)
	}
	for _, c := range cols {
		if c.identity {
			// A bulk load given an identity column's values turns the
			// session's IDENTITY_INSERT on for the table, and a load that
			// fails part way leaves it on — and a session may have it on
			// for one table only. Off again after each table, whatever
			// happened.
			defer r.conn.ExecContext(context.Background(), strings.Replace(stRestoreIdentityOff.SQL, "[<table>]", Ident(t), 1))
			break
		}
	}
	keys, err := r.foreignKeysOn(ctx, t)
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

	tx, err := r.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	for _, k := range keys {
		if _, err = tx.ExecContext(ctx, restoreKeySQL(stRestoreKeyOff.SQL, t, k)); err != nil {
			return 0, err
		}
	}

	var names []string // the load's columns, fixed by the first row
	var batch *sql.Stmt
	inBatch := 0
	flush := func() error {
		if batch == nil {
			return nil
		}
		start := time.Now()
		_, err := batch.ExecContext(ctx) // no arguments: the batch is done
		observe(stRestoreLoad, start, int64(inBatch))
		cerr := batch.Close()
		batch, inBatch = nil, 0
		if err != nil {
			return err
		}
		return cerr
	}
	defer func() {
		if batch != nil {
			batch.Close()
		}
	}()
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
		}
		vals := make([]any, len(names))
		for i, c := range names {
			raw, ok := rec[c]
			if !ok {
				return n, fmt.Errorf("row %d: column %q is missing", n+1, c)
			}
			if vals[i], err = restoreValue(raw, cols[c].dataType); err != nil {
				return n, fmt.Errorf("row %d column %q: %w", n+1, c, err)
			}
		}
		if batch == nil {
			batch, err = tx.PrepareContext(ctx, mssqldrv.CopyIn(Ident(t),
				mssqldrv.BulkOptions{CheckConstraints: true, FireTriggers: true, KeepNulls: true}, names...))
			if err != nil {
				return n, err
			}
		}
		if _, err = batch.ExecContext(ctx, vals...); err != nil {
			return n, err
		}
		n++
		if inBatch++; inBatch >= loadBatch {
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
	for _, k := range keys {
		if _, err = tx.ExecContext(ctx, restoreKeySQL(stRestoreKeyOn.SQL, t, k)); err != nil {
			return n, err
		}
	}
	if err = tx.Commit(); err != nil {
		return n, err
	}
	// Each key checked against the rows now in, so the server may trust it;
	// one whose parent rows are not all in yet is checked again at the end
	// (RestoreTriggers, Close).
	for _, k := range keys {
		if _, err := r.conn.ExecContext(ctx, restoreKeySQL(stRestoreKeyCheck.SQL, t, k)); err != nil {
			r.unchecked = append(r.unchecked, [2]string{t, k})
		}
	}
	return n, nil
}

// restoreValue turns one dumped JSON value back into a bulk load's value for a
// column of the type given: the inverse of dumpValue. null = NULL, {"b64":…}
// = bytes, text = the value as SQL Server printed it (a time as written by
// dumpValue, or RFC 3339).
func restoreValue(raw any, typ string) (any, error) {
	switch v := raw.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		s, ok := v[dumpBinaryKey].(string)
		if !ok || len(v) != 1 {
			return nil, errors.New("unknown value form")
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, err
		}
		if !dumpIsBinary(typ) {
			return string(b), nil // text that was not valid UTF-8
		}
		return b, nil
	case string:
		switch typ {
		case "tinyint", "smallint", "int", "bigint":
			return strconv.ParseInt(v, 10, 64)
		case "bit":
			switch strings.ToLower(v) {
			case "1", "true":
				return true, nil
			case "0", "false":
				return false, nil
			}
			return nil, fmt.Errorf("%q is not a bit", v)
		case "float", "real":
			return strconv.ParseFloat(v, 64)
		case "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time":
			return restoreTime(v, typ)
		case "binary", "varbinary", "image":
			return []byte(v), nil // a dump older than the b64 form
		}
		return v, nil // text, decimals, JSON
	}
	return nil, fmt.Errorf("unexpected JSON value %T", raw)
}

// restoreTime reads a time as dumpValue wrote it, or RFC 3339.
func restoreTime(s, typ string) (time.Time, error) {
	layouts := []string{"2006-01-02 15:04:05.9999999", time.RFC3339Nano, "2006-01-02T15:04:05.9999999", "2006-01-02"}
	if typ == "time" {
		layouts = []string{"15:04:05.9999999"}
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, s, time.UTC); err == nil {
			if typ == "time" {
				return time.Date(1, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC), nil
			}
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("%q is not a time", s)
}

// RestoreTriggers creates the saved triggers the target does not have yet,
// after the foreign keys still waiting.
func (r *Restorer) RestoreTriggers(ctx context.Context, path string) ([]string, error) {
	if err := r.addPending(ctx); err != nil {
		return nil, err
	}
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
		if err := r.conn.QueryRowContext(ctx, stRestoreTrigger.SQL, tr.Name).Scan(&n); err != nil {
			return created, err
		}
		if n > 0 {
			continue
		}
		if _, err := r.conn.ExecContext(ctx, tr.Create); err != nil {
			return created, fmt.Errorf("create trigger %s: %w", tr.Name, err)
		}
		created = append(created, tr.Name)
	}
	r.checkKeys(ctx)
	return created, nil
}

var _ dal.Restorer = (*Restorer)(nil)

// keep the representative forms referenced (they are the lint test's).
var _ = []any{stRestoreTableRows, stRestoreCreate, stRestoreMakeTrigger, stRestoreAddKey, stBackupRows}
