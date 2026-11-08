package mssql

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The config store as provision writes it (dal.ConfigStore) on SQL Server,
// made like internal/dal/mysql/config.go: every statement is a method of
// configQ, which runs on the store's pool or on one transaction (configTx).
// The rules of what may be written stay in internal/provision.
//
// The config family's openers (Config, VendorQueueTables, ReferenceData,
// HLRNumbers, EngineConfig, TrafficConfig, HLRShardWriter) register here.

var _ = serves(func(d *dal.Driver) {
	d.Config = openConfig
	d.VendorQueueTables = openVendorQueueTables
	d.ReferenceData = openReferenceData
	d.HLRNumbers = openHLRNumbers
	d.EngineConfig = openEngineConfig
	d.TrafficConfig = openTrafficConfig
	d.HLRShardWriter = openHLRShardWriter
})

// ConfigStmts is every statement of the config family on SQL Server, by
// name: the same names as mysql.ConfigStmts (config_twins_test.go checks
// that none is missing). A statement built with a list or a column name is
// registered in one representative form.
var ConfigStmts = newRegistry()

// positional writes the ? marks of a statement as @p1, @p2 … in order:
// go-mssqldb's positional parameters, given in the order the MySQL DAL
// gives its own, so a call passes the same arguments on both. A mark inside
// a quoted literal is left alone. An @pN written out is kept as it is: a
// TOP (@pN) ahead of the marks names the argument MySQL gives its LIMIT
// (after the marks' ones), and a value used twice names its first mark.
func positional(q string) string {
	var b strings.Builder
	b.Grow(len(q) + 16)
	n, quoted := 0, false
	for i := 0; i < len(q); i++ {
		c := q[i]
		switch {
		case c == '\'':
			quoted = !quoted
			b.WriteByte(c)
		case c == '?' && !quoted:
			n++
			b.WriteString("@p")
			b.WriteString(strconv.Itoa(n))
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// addConfig registers a config statement written with ? marks.
func addConfig(name, q string) *stmt.Stmt {
	return ConfigStmts.Add(name, positional(q), false)
}

// configQ runs the config family's statements on q: the pool, or one
// transaction (tx set).
type configQ struct {
	q  Querier
	db *sql.DB // the pool; nil inside a transaction
	tx *sql.Tx // the transaction, when q is one
}

// inTx runs fn in the caller's transaction when there is one, else in a
// short one of its own (a read and its write that must not interleave).
func (c configQ) inTx(ctx context.Context, fn func(q Querier) error) error {
	if c.tx != nil {
		return fn(c.tx)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// ConfigStore is the config store on one handle.
type ConfigStore struct {
	configQ
	db  *sql.DB
	own bool // Close closes db (a handle of its own)
}

// NewConfigStore reads and writes the config rows on db (a test's own
// handle; Close leaves it open).
func NewConfigStore(db *sql.DB) *ConfigStore {
	if db == nil {
		return nil
	}
	return &ConfigStore{configQ: configQ{q: db, db: db}, db: db}
}

// OpenConfigStore opens a handle of its own on dsn (closed by Close), as
// the console always has. Like sql.Open it does not dial yet.
func OpenConfigStore(dsn string) (*ConfigStore, error) {
	db, err := open(dsn)
	if err != nil {
		return nil, err
	}
	s := NewConfigStore(db)
	s.own = true
	return s, nil
}

func openConfig(cfg config.Config) (dal.ConfigStore, error) {
	s, err := OpenConfigStore(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Begin starts one transaction (READ COMMITTED, which the database reads
// as a snapshot of each statement; a read that locks says so).
func (s *ConfigStore) Begin(ctx context.Context) (dal.ConfigTx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &configTx{configQ: configQ{q: tx, tx: tx}, tx: tx}, nil
}

// Ping fails when the store does not answer.
func (s *ConfigStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Close closes the handle when it is the store's own.
func (s *ConfigStore) Close() error {
	if !s.own {
		return nil
	}
	return s.db.Close()
}

// DB is the handle (a test's own setup and cleanup rows).
func (s *ConfigStore) DB() *sql.DB { return s.db }

// configTx is one transaction on the config store.
type configTx struct {
	configQ
	tx *sql.Tx
}

func (t *configTx) Commit() error   { return t.tx.Commit() }
func (t *configTx) Rollback() error { return t.tx.Rollback() }

var (
	_ dal.ConfigStore = (*ConfigStore)(nil)
	_ dal.ConfigTx    = (*configTx)(nil)
)

// MissingTable is SQL Server's "Invalid object name" (208).
func (configQ) MissingTable(err error) bool { return MissingTable(err) }

// count answers a "how many rows" statement.
func (c configQ) count(ctx context.Context, st *stmt.Stmt, args ...any) (int, error) {
	var n int
	start := time.Now()
	err := c.q.QueryRowContext(ctx, st.SQL, args...).Scan(&n)
	observe(st, start, 1)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// exists answers an "is there a row" statement with one query.
func (c configQ) exists(ctx context.Context, st *stmt.Stmt, args ...any) (bool, error) {
	n, err := c.count(ctx, st, args...)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// execRows runs a statement and answers the rows it changed (0 when the
// driver cannot say, as the callers have always read it).
func (c configQ) execRows(ctx context.Context, q string, args ...any) (int64, error) {
	res, err := c.q.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// insertID runs an INSERT … OUTPUT INSERTED.id and answers the new id.
func (c configQ) insertID(ctx context.Context, q string, args ...any) (int64, error) {
	var id int64
	err := c.q.QueryRowContext(ctx, q, args...).Scan(&id)
	return id, err
}

// rowsErr is a read's own error after its rows, as a dal.RowsError.
func rowsErr(rows *sql.Rows) error {
	if err := rows.Err(); err != nil {
		return dal.RowsError{Err: err}
	}
	return nil
}

// cfgID is NULL for 0, the "none" of every optional id column.
func cfgID(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}

// cfgStr is NULL for "".
func cfgStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// cfgVarchar is NULL for "", else s as a VARCHAR (a CHAR/VARCHAR column).
func cfgVarchar(s string) any {
	if s == "" {
		return nil
	}
	return Varchar(s)
}

// cfgNullStr is a dal.NullString as a parameter: NULL when not Valid.
func cfgNullStr(s dal.NullString) any {
	if !s.Valid {
		return nil
	}
	return s.String
}

// cfgNullTime is a dal.NullTime as a DATETIME2 parameter (UTC): NULL when
// not Valid.
func cfgNullTime(t dal.NullTime) any {
	if !t.Valid {
		return nil
	}
	return Time(t.Time)
}

// cfgTime is a time as a DATETIME2 parameter (UTC); the zero time is NULL.
func cfgTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return Time(t)
}

// cfgJSONValue is a value given for a JSON path as MySQL's JSON_SET stores
// it: the MySQL driver sends a bool as 1 or 0, so JSON_SET writes a number;
// JSON_MODIFY would write true or false. A bool goes as 1 or 0 here too.
func cfgJSONValue(v any) any {
	if b, ok := v.(bool); ok {
		if b {
			return 1
		}
		return 0
	}
	return v
}

// cfgBool is 1 or 0.
func cfgBool(b bool) int {
	if b {
		return 1
	}
	return 0
}

// wholeTable is the ceiling of a read that takes a whole config table (the
// cache's loads, a role's vendor list): MySQL reads such a table with no
// bound; here every read has one, far above any table's size.
const wholeTable = "TOP (10000000)"

// ---- the existence checks every area asks ---------------------------------

var (
	stAccountExists = addConfig("config.account_exists", "SELECT COUNT(*) FROM sms_service_config WHERE id = ?")
	stCompanyExists = addConfig("config.company_exists", "SELECT COUNT(*) FROM companies WHERE id = ?")
	stVendorExists  = addConfig("config.vendor_exists", "SELECT COUNT(*) FROM vendors WHERE id = ?")
)

func (c configQ) AccountExists(ctx context.Context, id int64) (bool, error) {
	return c.exists(ctx, stAccountExists, id)
}

func (c configQ) CompanyExists(ctx context.Context, id int64) (bool, error) {
	return c.exists(ctx, stCompanyExists, id)
}

func (c configQ) VendorExists(ctx context.Context, id int64) (bool, error) {
	return c.exists(ctx, stVendorExists, id)
}

// ---- the audit trail ------------------------------------------------------

// audit_log has its chain trigger (AFTER INSERT), so no insert into it may
// carry an OUTPUT clause; none needs one.
var stAudit = addConfig("config.audit", `
		INSERT INTO audit_log (actor, actor_name, account_id, company_id, company_name, action, summary,
		                       entity, entity_id, target_label, feature, detail, before_data, after_data)
		VALUES ('console','console',?,?,?,?,?,?,?,?,?,?,?,?)`)

// WriteAudit writes one console audit row.
func (c configQ) WriteAudit(ctx context.Context, r dal.AuditRow) error {
	_, err := c.q.ExecContext(ctx, stAudit.SQL,
		cfgID(r.AccountID), cfgID(r.CompanyID), cfgStr(r.CompanyName), r.Action, r.Summary, r.Entity,
		r.EntityID, cfgStr(r.Label), r.Feature, r.Detail, cfgStr(r.Before), cfgStr(r.After))
	return err
}

// ---- the queue store's vendor tables --------------------------------------

// VendorQueueTables is the queue store's table of each vendor (the console
// makes it with the vendor and drops it with the vendor), made from
// queue_template as CREATE TABLE … LIKE does on MySQL (clone.go).
type VendorQueueTables struct {
	db     *sql.DB
	own    bool
	cloner *Cloner
}

// OpenVendorQueueTables opens a handle of its own on dsn (closed by Close).
func OpenVendorQueueTables(dsn string) (*VendorQueueTables, error) {
	db, err := open(dsn)
	if err != nil {
		return nil, err
	}
	v := NewVendorQueueTables(db)
	v.own = true
	return v, nil
}

func openVendorQueueTables(cfg config.Config) (dal.VendorQueueTables, error) {
	v, err := OpenVendorQueueTables(cfg.QueueDSN)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// NewVendorQueueTables works on db (a test's own handle).
func NewVendorQueueTables(db *sql.DB) *VendorQueueTables {
	if db == nil {
		return nil
	}
	return &VendorQueueTables{db: db, cloner: NewCloner(db)}
}

var (
	// The table's name is the vendor's cid, which provision has checked.
	stQueueTableMake  = addConfig("config.queue_table_make", "CREATE TABLE [queue_<cid>] (… the columns, keys and indexes of queue_template …)")
	stQueueTableCount = addConfig("config.queue_table_count", "SELECT COUNT(*) FROM [queue_<cid>]")
	stQueueTableDrop  = addConfig("config.queue_table_drop", "DROP TABLE IF EXISTS [queue_<cid>]")
)

func (v *VendorQueueTables) Make(ctx context.Context, cid string) error {
	_ = stQueueTableMake
	return v.cloner.CreateLike(ctx, "queue_template", "queue_"+cid)
}

func (v *VendorQueueTables) Count(ctx context.Context, cid string) (int, error) {
	_ = stQueueTableCount
	var n int
	err := v.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteIdent("queue_"+cid)).Scan(&n)
	return n, err
}

func (v *VendorQueueTables) Drop(ctx context.Context, cid string) error {
	_ = stQueueTableDrop
	_, err := v.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteIdent("queue_"+cid))
	return err
}

func (v *VendorQueueTables) Ping(ctx context.Context) error { return v.db.PingContext(ctx) }

func (v *VendorQueueTables) Close() error {
	if !v.own {
		return nil
	}
	return v.db.Close()
}

// DB is the handle (a test's own setup and cleanup).
func (v *VendorQueueTables) DB() *sql.DB { return v.db }

var _ dal.VendorQueueTables = (*VendorQueueTables)(nil)
