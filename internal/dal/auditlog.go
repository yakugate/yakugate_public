package dal

import (
	"context"
	"fmt"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// OpenAuditLog opens audit_log on the install's SQL database (the config
// store's one pool a process).
func OpenAuditLog(cfg config.Config) (AuditLog, error) {
	e, err := OpenEngineConfig(cfg)
	if err != nil {
		return nil, err
	}
	if e.Audit == nil {
		return nil, fmt.Errorf("dal: audit log on %s: %w", SQL(cfg), ErrNotServed)
	}
	return e.Audit, nil
}

// The engine's own audit trail (internal/audit, plan of 29 Oct 2026): the
// rows its admin endpoints write into audit_log, and the reads that check the
// tamper-evident chain over it (config/0088). The digest is recomputed in
// internal/audit, never asked of the database; what is read is here.

// ScanError is a row a read could not scan, told apart from a read cut
// short (RowsError) where a caller words them apart.
type ScanError struct{ Err error }

func (e ScanError) Error() string { return e.Err.Error() }
func (e ScanError) Unwrap() error { return e.Err }

// AdminAuditRow is one audit_log row an engine admin endpoint writes. An
// empty text and a zero id or code are written NULL (Action and Outcome as
// they are); Detail is JSON text, "" = NULL.
type AdminAuditRow struct {
	Actor                    string
	AccountID, CompanyID     int64
	Action                   string
	Entity, EntityID, Label  string
	Method, Route            string
	ResponseCode             int
	IP                       string
	Summary, Outcome, Reason string
	Detail                   string
}

// AuditLog is audit_log as the engine writes and checks it.
type AuditLog interface {
	// AppendAudit writes one row, stamped with the database's NOW().
	AppendAudit(ctx context.Context, r AdminAuditRow) error
	// UnprotectedCount counts the rows written before the chain existed
	// (row_hash NULL).
	UnprotectedCount(ctx context.Context) (int64, error)
	// WalkChain reads the chained rows in id order and calls fn for each:
	// its id, prev_hash and row_hash, and the columns named, each read back
	// AS CHAR by the server. A row that would not scan ends the walk with a
	// ScanError, a read cut short with a RowsError (the statement's own
	// failure is returned as it is); an error from fn ends it with that
	// error.
	WalkChain(ctx context.Context, columns []string, fn func(id int64, prev, stored NullString, vals []NullString) error) error
	// ChainHead is the stored head hash. ErrNoRows: the head row is missing.
	ChainHead(ctx context.Context) (string, error)
}
