package mssql

import (
	"context"
	"database/sql"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// audit_log as the engine's admin endpoints write and check it
// (internal/audit) on SQL Server, made like internal/dal/mysql/engine_audit.go.
// The chain itself is the AFTER INSERT trigger trg_audit_log_chain (config
// base schema), which hashes each row's values as text the way MySQL's
// trigger does; the walk below reads them back as the same text.

var (
	stAuditAppend = addEngine("audit.append", `INSERT INTO audit_log
		   (actor, account_id, company_id, action, entity, entity_id, target_label,
		    method, route, response_code, ip, summary, outcome, reason, detail, created_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,SYSUTCDATETIME())`)
	stAuditUnprotected = addEngine("audit.unprotected",
		`SELECT COUNT(*) FROM audit_log WHERE row_hash IS NULL`)
	// The hashes are CHAR(64): read without the padding a shorter value
	// (the first row's empty predecessor) would carry.
	stAuditHead = addEngine("audit.head",
		`SELECT RTRIM(head_hash) FROM audit_chain_head WHERE id = 1`)

	// The chain walk, with its column list as verify.go builds it (two
	// columns here; the real list is every audited column).
	_ = addEngine("audit.chain", chainSQL([]string{"actor", "created_at"}))
)

// AuditLog is dal.AuditLog on the config store.
type AuditLog struct{ db *sql.DB }

// NewAuditLog is audit_log on db.
func NewAuditLog(db *sql.DB) *AuditLog { return &AuditLog{db: db} }

// AppendAudit writes one row. NULL rather than an empty string or a zero
// id, so a missing fact reads as missing (the same rule as on MySQL).
func (a *AuditLog) AppendAudit(ctx context.Context, e dal.AdminAuditRow) error {
	var code any
	if e.ResponseCode != 0 {
		code = e.ResponseCode
	}
	_, err := a.db.ExecContext(ctx, stAuditAppend.SQL,
		cfgStr(e.Actor), cfgID(e.AccountID), cfgID(e.CompanyID), e.Action, cfgStr(e.Entity), cfgStr(e.EntityID),
		cfgStr(e.Label), cfgStr(e.Method), cfgStr(e.Route), code, cfgStr(e.IP),
		cfgStr(e.Summary), Varchar(e.Outcome), cfgStr(e.Reason), cfgStr(e.Detail))
	return err
}

// UnprotectedCount counts the rows written before the chain existed.
func (a *AuditLog) UnprotectedCount(ctx context.Context) (int64, error) {
	var n int64
	err := a.db.QueryRowContext(ctx, stAuditUnprotected.SQL).Scan(&n)
	return n, err
}

// chainText is a column as the trigger turns it into text: a time as
// 'YYYY-MM-DD HH:MM:SS' (MySQL's CAST of a DATETIME), anything else as its
// plain text.
func chainText(col string) string {
	if col == "created_at" {
		return "CONVERT(NVARCHAR(19), created_at, 120)"
	}
	return "CONVERT(NVARCHAR(MAX), " + quoteIdent(col) + ")"
}

// chainSQL reads every column back as text, which is what makes an
// independent check of the hashes possible (verify.go says why).
func chainSQL(columns []string) string {
	parts := make([]string, len(columns))
	for i, c := range columns {
		parts[i] = chainText(c)
	}
	return `SELECT id, RTRIM(prev_hash), RTRIM(row_hash), ` + strings.Join(parts, ", ") + `
		   FROM audit_log WHERE row_hash IS NOT NULL ORDER BY id`
}

// WalkChain reads the chained rows in id order, one at a time.
func (a *AuditLog) WalkChain(ctx context.Context, columns []string, fn func(id int64, prev, stored dal.NullString, vals []dal.NullString) error) error {
	rows, err := a.db.QueryContext(ctx, chainSQL(columns))
	if err != nil {
		return err
	}
	defer rows.Close()
	nCols := len(columns)
	for rows.Next() {
		var (
			id      int64
			prev    dal.NullString
			stored  dal.NullString
			vals    = make([]dal.NullString, nCols)
			scanArg = make([]any, 0, nCols+3)
		)
		scanArg = append(scanArg, &id, &prev, &stored)
		for i := range vals {
			scanArg = append(scanArg, &vals[i])
		}
		if err := rows.Scan(scanArg...); err != nil {
			return dal.ScanError{Err: err}
		}
		if err := fn(id, prev, stored, vals); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return dal.RowsError{Err: err}
	}
	return nil
}

// ChainHead reads the stored head hash.
func (a *AuditLog) ChainHead(ctx context.Context) (string, error) {
	var head string
	err := a.db.QueryRowContext(ctx, stAuditHead.SQL).Scan(&head)
	return head, err
}

var _ dal.AuditLog = (*AuditLog)(nil)
