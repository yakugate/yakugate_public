package mysql

import (
	"context"
	"database/sql"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// audit_log as the engine's admin endpoints write and check it
// (internal/audit: audit.go's write, verify.go's reads), moved here
// unchanged on 29 Oct 2026.

var (
	stAuditAppend = EngineStmts.Add("audit.append", `INSERT INTO audit_log
		   (actor, account_id, company_id, action, entity, entity_id, target_label,
		    method, route, response_code, ip, summary, outcome, reason, detail, created_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,CAST(? AS JSON),NOW())`, false)
	stAuditUnprotected = EngineStmts.Add("audit.unprotected",
		`SELECT COUNT(*) FROM audit_log WHERE row_hash IS NULL`, false)
	stAuditHead = EngineStmts.Add("audit.head",
		`SELECT head_hash FROM audit_chain_head WHERE id = 1`, false)
)

func init() {
	// The chain walk, with its column list as verify.go builds it (two
	// columns here; the real list is every audited column).
	EngineStmts.Add("audit.chain", chainSQL([]string{"actor", "created_at"}), false)
}

// AuditLog is dal.AuditLog on the config store.
type AuditLog struct{ db *sql.DB }

// NewAuditLog is audit_log on db.
func NewAuditLog(db *sql.DB) *AuditLog { return &AuditLog{db: db} }

// AppendAudit writes one row.
func (a *AuditLog) AppendAudit(ctx context.Context, e dal.AdminAuditRow) error {
	var detail any
	if e.Detail != "" {
		detail = e.Detail
	}
	// NULL rather than an empty string or a zero id (nsStr, nz, nzInt: the
	// same rule as audit.go's nz/nzID/nzInt), so a missing fact reads as
	// missing instead of as "" or 0.
	_, err := a.db.ExecContext(ctx, stAuditAppend.SQL,
		nsStr(e.Actor), nz(e.AccountID), nz(e.CompanyID), e.Action, nsStr(e.Entity), nsStr(e.EntityID),
		nsStr(e.Label), nsStr(e.Method), nsStr(e.Route), nzInt(e.ResponseCode), nsStr(e.IP),
		nsStr(e.Summary), e.Outcome, nsStr(e.Reason), detail)
	return err
}

// UnprotectedCount counts the rows written before the chain existed.
func (a *AuditLog) UnprotectedCount(ctx context.Context) (int64, error) {
	var n int64
	err := a.db.QueryRowContext(ctx, stAuditUnprotected.SQL).Scan(&n)
	return n, err
}

// chainSQL reads every column back AS CHAR, which is the one detail that
// makes an independent implementation possible at all (verify.go says why).
func chainSQL(columns []string) string {
	parts := make([]string, len(columns))
	for i, c := range columns {
		parts[i] = "CAST(" + c + " AS CHAR)"
	}
	return `SELECT id, prev_hash, row_hash, ` + strings.Join(parts, ", ") + `
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
