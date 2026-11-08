package dal

import "context"

// AuditTrail is audit_log, as the console writes it (actor "console").
type AuditTrail interface {
	// WriteAudit writes one row.
	WriteAudit(ctx context.Context, r AuditRow) error
}

// AuditRow is one audit_log row the console writes. A zero id and an empty
// text are written NULL (Summary, Action, Entity, EntityID and Feature
// always as they are); Detail is JSON text; Before and After are JSON text,
// "" = NULL.
type AuditRow struct {
	AccountID, CompanyID int64
	CompanyName          string
	Action, Summary      string
	Entity, EntityID     string
	Label, Feature       string
	Detail               string
	Before, After        string
}
