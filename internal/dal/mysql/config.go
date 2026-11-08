package mysql

import (
	"context"
	"database/sql"
	"errors"

	mysqldrv "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The config store as provision writes it (dal.ConfigStore), moved here
// from internal/provision on 29 Oct 2026, unchanged: the same statements,
// the same transactions, the same locks. What may be written — every rule —
// stayed in internal/provision.
//
// Every statement is a method of configQ, which runs on the store's pool or
// on one transaction (configTx): a unit of work provision runs in one
// transaction begins one (Begin) and runs each of its statements there, as
// it did before.

// ConfigStmts is every statement of the config family on MySQL, by name:
// what the lint and plan tests read. A statement built with a list or a
// column name is registered in one representative form.
var ConfigStmts = stmt.New("mysql")

// querier is a *sql.DB or a *sql.Tx.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// configQ runs the config family's statements on q.
type configQ struct {
	q querier
}

// ConfigStore is the config store on one handle.
type ConfigStore struct {
	configQ
	db *sql.DB
}

// NewConfigStore reads and writes the config rows on db.
func NewConfigStore(db *sql.DB) *ConfigStore {
	if db == nil {
		return nil
	}
	return &ConfigStore{configQ: configQ{q: db}, db: db}
}

// The console's own handles on the config and queue stores.
var _ = serves(func(d *dal.Driver) {
	d.Config = func(cfg config.Config) (dal.ConfigStore, error) {
		s, err := OpenConfigStore(cfg.ConfigDSN)
		if err != nil {
			return nil, err
		}
		return s, nil
	}
	d.VendorQueueTables = func(cfg config.Config) (dal.VendorQueueTables, error) {
		v, err := OpenVendorQueueTables(cfg.QueueDSN)
		if err != nil {
			return nil, err
		}
		return v, nil
	}
})

// OpenConfigStore opens a handle of its own on dsn (closed by Close), as
// the console always has: sql.Open, which does not dial yet.
func OpenConfigStore(dsn string) (*ConfigStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return NewConfigStore(db), nil
}

// Begin starts one transaction.
func (s *ConfigStore) Begin(ctx context.Context) (dal.ConfigTx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &configTx{configQ: configQ{q: tx}, tx: tx}, nil
}

// Ping fails when the store does not answer.
func (s *ConfigStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Close closes the handle.
func (s *ConfigStore) Close() error { return s.db.Close() }

// DB is the handle (a test's own setup and cleanup rows).
func (s *ConfigStore) DB() *sql.DB { return s.db }

// HLRLookup is the HLR parts of the engine's config on this store's own
// handle (dal.HLRLookupOf).
func (s *ConfigStore) HLRLookup() dal.HLRLookupConfig { return NewEngineConfig(s.db).HLR }

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

// MissingTable is MySQL's ER_NO_SUCH_TABLE (1146).
func (configQ) MissingTable(err error) bool { return isMissingTable(err) }

func isMissingTable(err error) bool {
	var me *mysqldrv.MySQLError
	return errors.As(err, &me) && me.Number == 1146
}

// count answers a "how many rows" statement.
func (c configQ) count(ctx context.Context, st *stmt.Stmt, args ...any) (int, error) {
	var n int
	if err := c.q.QueryRowContext(ctx, st.SQL, args...).Scan(&n); err != nil {
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

// nullID is NULL for 0, the "none" of every optional id column.
func nullID(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}

// nullStr is NULL for "".
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ---- the existence checks every area asks ---------------------------------

var (
	stAccountExists = ConfigStmts.Add("config.account_exists", "SELECT COUNT(*) FROM sms_service_config WHERE id = ?", false)
	stCompanyExists = ConfigStmts.Add("config.company_exists", "SELECT COUNT(*) FROM companies WHERE id = ?", false)
	stVendorExists  = ConfigStmts.Add("config.vendor_exists", "SELECT COUNT(*) FROM vendors WHERE id = ?", false)
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

var stAudit = ConfigStmts.Add("config.audit", `
		INSERT INTO audit_log (actor, actor_name, account_id, company_id, company_name, action, summary,
		                       entity, entity_id, target_label, feature, detail, before_data, after_data)
		VALUES ('console','console',?,?,?,?,?,?,?,?,?,CAST(? AS JSON),CAST(? AS JSON),CAST(? AS JSON))`, false)

// WriteAudit writes one console audit row.
func (c configQ) WriteAudit(ctx context.Context, r dal.AuditRow) error {
	_, err := c.q.ExecContext(ctx, stAudit.SQL,
		nullID(r.AccountID), nullID(r.CompanyID), nullStr(r.CompanyName), r.Action, r.Summary, r.Entity,
		r.EntityID, nullStr(r.Label), r.Feature, r.Detail, nullStr(r.Before), nullStr(r.After))
	return err
}

// ---- the queue store's vendor tables --------------------------------------

// VendorQueueTables is the queue store's table of each vendor (the console
// makes it with the vendor and drops it with the vendor).
type VendorQueueTables struct {
	db *sql.DB
}

// OpenVendorQueueTables opens a handle of its own on dsn (closed by Close).
func OpenVendorQueueTables(dsn string) (*VendorQueueTables, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &VendorQueueTables{db: db}, nil
}

// NewVendorQueueTables works on db (a test's own handle).
func NewVendorQueueTables(db *sql.DB) *VendorQueueTables {
	if db == nil {
		return nil
	}
	return &VendorQueueTables{db: db}
}

var (
	// The table's name is the vendor's cid, which provision has checked.
	stQueueTableMake  = ConfigStmts.Add("config.queue_table_make", "CREATE TABLE IF NOT EXISTS `queue_<cid>` LIKE queue_template", false)
	stQueueTableCount = ConfigStmts.Add("config.queue_table_count", "SELECT COUNT(*) FROM `queue_<cid>`", false)
	stQueueTableDrop  = ConfigStmts.Add("config.queue_table_drop", "DROP TABLE IF EXISTS `queue_<cid>`", false)
)

func (v *VendorQueueTables) Make(ctx context.Context, cid string) error {
	_ = stQueueTableMake
	_, err := v.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS `queue_"+cid+"` LIKE queue_template")
	return err
}

func (v *VendorQueueTables) Count(ctx context.Context, cid string) (int, error) {
	_ = stQueueTableCount
	var n int
	err := v.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `queue_"+cid+"`").Scan(&n)
	return n, err
}

func (v *VendorQueueTables) Drop(ctx context.Context, cid string) error {
	_ = stQueueTableDrop
	_, err := v.db.ExecContext(ctx, "DROP TABLE IF EXISTS `queue_"+cid+"`")
	return err
}

func (v *VendorQueueTables) Ping(ctx context.Context) error { return v.db.PingContext(ctx) }
func (v *VendorQueueTables) Close() error                   { return v.db.Close() }

// DB is the handle (a test's own setup and cleanup).
func (v *VendorQueueTables) DB() *sql.DB { return v.db }

var _ dal.VendorQueueTables = (*VendorQueueTables)(nil)
