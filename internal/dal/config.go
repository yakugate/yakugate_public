package dal

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// The config family (plan of 29 Oct 2026): the rows the console and the
// Admin API write — companies, accounts, vendors, routing rules, rate cards,
// operators, spam and blocked numbers, gateway settings — and the audit
// trail beside them. The rules of what may be written stay in
// internal/provision; what they read and write is here, by what the data is:
//
//	Accounts        sms_service_config and what hangs off one account
//	Companies       companies, their balance of record (as the console moves it)
//	Vendors         vendors, their HTTP calls (http_endpoints)
//	Routing         routing rules and the plans they make
//	RateCards       rate cards, their rates and the notices of a change
//	Operators       networks and their number ranges
//	ContentRules    spam rules and blocked numbers
//	GatewaySettings gateway_settings, deployments, dialects and doors
//	AuditTrail      audit_log
//
// ConfigQueries is all of them; ConfigStore runs them on the store, ConfigTx
// inside one transaction (Begin). A unit of work provision runs in one
// transaction stays one: it begins one here and runs every statement of the
// unit on it. No *sql.Tx leaves the DAL.
//
// The config store is always the install's SQL database (dal.SQL); Redis
// does not serve it.

// ConfigQueries is every read and write of the config family, on the store
// or inside a transaction.
type ConfigQueries interface {
	Accounts
	Companies
	Vendors
	Routing
	RateCards
	Operators
	ContentRules
	GatewaySettings
	AuditTrail
	ConsoleViews

	// MissingTable says err is the database's "no such table": an optional
	// table a few additive features treat as empty.
	MissingTable(err error) bool
}

// ConfigStore is the config store as provision writes it.
type ConfigStore interface {
	ConfigQueries
	// Begin starts one transaction; every call on the ConfigTx runs in it.
	Begin(ctx context.Context) (ConfigTx, error)
	// Ping fails when the store does not answer.
	Ping(ctx context.Context) error
	// Close lets the handle go (a handle opened by OpenConfig).
	Close() error
}

// ConfigTx is one transaction on the config store.
type ConfigTx interface {
	ConfigQueries
	Commit() error
	Rollback() error
}

// WithTx runs fn in one transaction of s: committed when fn answers nil,
// rolled back otherwise.
func WithTx(ctx context.Context, s ConfigStore, fn func(tx ConfigTx) error) error {
	tx, err := s.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// VendorQueueTables is the queue store's own table of each vendor, as the
// console makes and removes it with the vendor (MySQL queues only).
type VendorQueueTables interface {
	// Make creates the vendor's queue table when it is not there.
	Make(ctx context.Context, cid string) error
	// Count is how many rows the vendor's queue table holds.
	Count(ctx context.Context, cid string) (int, error)
	// Drop removes the vendor's queue table when it is there.
	Drop(ctx context.Context, cid string) error
	Ping(ctx context.Context) error
	Close() error
}

// ErrNoRows is what a read of one row answers when there is none — the
// value database/sql has always given, so a caller that compares with it
// keeps working unchanged.
var ErrNoRows = sql.ErrNoRows

// The nullable values a config row carries. Each has the fields of its
// database/sql twin (String/Int64/Time/Bool/Float64 and Valid), so the rules
// that read them read the same fields; a DAL scans into them and passes them
// as arguments.

// NullString is a string that may be NULL.
type NullString struct {
	String string
	Valid  bool
}

// NullInt64 is an int64 that may be NULL.
type NullInt64 struct {
	Int64 int64
	Valid bool
}

// NullTime is a time that may be NULL.
type NullTime struct {
	Time  time.Time
	Valid bool
}

// NullBool is a bool that may be NULL.
type NullBool struct {
	Bool  bool
	Valid bool
}

// NullFloat64 is a float64 that may be NULL.
type NullFloat64 struct {
	Float64 float64
	Valid   bool
}

func (n *NullString) Scan(v any) error {
	var s sql.NullString
	err := s.Scan(v)
	n.String, n.Valid = s.String, s.Valid
	return err
}

func (n NullString) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.String, nil
}

func (n *NullInt64) Scan(v any) error {
	var s sql.NullInt64
	err := s.Scan(v)
	n.Int64, n.Valid = s.Int64, s.Valid
	return err
}

func (n NullInt64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int64, nil
}

func (n *NullTime) Scan(v any) error {
	var s sql.NullTime
	err := s.Scan(v)
	n.Time, n.Valid = s.Time, s.Valid
	return err
}

func (n NullTime) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Time, nil
}

func (n *NullBool) Scan(v any) error {
	var s sql.NullBool
	err := s.Scan(v)
	n.Bool, n.Valid = s.Bool, s.Valid
	return err
}

func (n NullBool) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Bool, nil
}

func (n *NullFloat64) Scan(v any) error {
	var s sql.NullFloat64
	err := s.Scan(v)
	n.Float64, n.Valid = s.Float64, s.Valid
	return err
}

func (n NullFloat64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Float64, nil
}

// OpenConfig opens the config store on the install's SQL database: a handle
// of its own, which the caller closes (the console's).
func OpenConfig(cfg config.Config) (ConfigStore, error) {
	on := SQL(cfg)
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.Config == nil {
		return nil, fmt.Errorf("dal: config on %s: %w", on, ErrNotServed)
	}
	return d.Config(cfg)
}

// OpenVendorQueueTables opens the queue store's vendor tables on the
// database YAKU_QUEUE_BACKEND names (the install's SQL database when it
// names none): a handle of its own, which the caller closes.
func OpenVendorQueueTables(cfg config.Config) (VendorQueueTables, error) {
	on := SQL(cfg)
	if b := norm(cfg.QueueBackend); b != "" && b != config.BackendRedis {
		on = b
	}
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.VendorQueueTables == nil {
		return nil, fmt.Errorf("dal: vendor queue tables on %s: %w", on, ErrNotServed)
	}
	return d.VendorQueueTables(cfg)
}
