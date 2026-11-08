package mysql

import (
	"context"
	"database/sql"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// The log settings, flow captures and capture records (internal/logcfg:
// logcfg.go, captures.go, shipper.go), moved here unchanged on 29 Oct 2026.

var (
	stLogAccounts = EngineStmts.Add("log_settings.accounts", `SELECT username, log_config FROM sms_service_config`, false)
	stLogVendors  = EngineStmts.Add("log_settings.vendors", `SELECT cid, log_config FROM vendors`, false)
	stLogMasters  = EngineStmts.Add("log_settings.masters", `
		SELECT a.username, c.master_number
		  FROM sms_service_config a JOIN companies c ON c.id = a.company_id
		 WHERE c.master_number IS NOT NULL AND c.master_number <> ''`, false)
	stLogSystem = EngineStmts.Add("log_settings.system", `SELECT log_config FROM system_log_config WHERE id = 1`, false)

	stCapturesHere = EngineStmts.Add("captures.running_here", captureSelect(runningHere), false)
	stCapturesAny  = EngineStmts.Add("captures.running_any", captureSelect(runningAny), false)
	stCapturePurge = EngineStmts.Add("captures.mark_purged",
		`UPDATE log_captures SET status = 'purged' WHERE id = ? AND status <> 'purged'`, false)

	stCaptureRecordsAdd = EngineStmts.Add("capture_records.add", captureInsertHead+"(?,?,?,?,?,?)", false)
	stCaptureCutoff     = EngineStmts.Add("capture_records.cutoff", `
			SELECT id FROM log_capture_records
			 WHERE capture_id = ? ORDER BY id DESC LIMIT 1 OFFSET ?`, false)
	stCaptureTrim = EngineStmts.Add("capture_records.trim",
		`DELETE FROM log_capture_records WHERE capture_id = ? AND id <= ?`, false)
)

const captureInsertHead = "INSERT INTO log_capture_records (capture_id, role, at, step, message_id, line) VALUES "

// LogSettings is dal.LogSettings on the config store.
type LogSettings struct{ db *sql.DB }

// NewLogSettings is the log settings on db.
func NewLogSettings(db *sql.DB) *LogSettings { return &LogSettings{db: db} }

func (s *LogSettings) AccountLogConfigs(ctx context.Context) ([]dal.NamedLogConfig, error) {
	return s.named(ctx, stLogAccounts.SQL)
}

func (s *LogSettings) VendorLogConfigs(ctx context.Context) ([]dal.NamedLogConfig, error) {
	return s.named(ctx, stLogVendors.SQL)
}

// named runs one name/config query.
func (s *LogSettings) named(ctx context.Context, q string) ([]dal.NamedLogConfig, error) {
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.NamedLogConfig
	for rows.Next() {
		var r dal.NamedLogConfig
		if err := rows.Scan(&r.Name, &r.Config); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AccountMasters is username -> the master number of the account's company.
func (s *LogSettings) AccountMasters(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, stLogMasters.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, master string
		if err := rows.Scan(&name, &master); err != nil {
			return nil, err
		}
		if name != "" {
			out[name] = master
		}
	}
	return out, rows.Err()
}

// SystemLogConfig reads the system row.
func (s *LogSettings) SystemLogConfig(ctx context.Context) (dal.NullString, error) {
	var raw dal.NullString
	err := s.db.QueryRowContext(ctx, stLogSystem.SQL).Scan(&raw)
	return raw, err
}

// captureSelect reads, in ONE statement, the captures that are running here
// and the ones whose files are due to go. The expiry comes back as SECONDS
// REMAINING, never as an absolute timestamp: log_captures.expires_at is a
// plain DATETIME in the database server's own clock, and a duration is the
// same number in every zone (logcfg/captures.go says the rest). A capture
// whose purge time passed in the last two days is read whatever its status.
// Flow captures only.
func captureSelect(running string) string {
	return `
		SELECT id, COALESCE(scope_type,''), COALESCE(scope_id,0), COALESCE(scope_name,''),
		       COALESCE(TIMESTAMPDIFF(SECOND, NOW(), expires_at), 0) AS secs_left,
		       COALESCE(` + running + `, 0) AS running,
		       COALESCE(purge_at <= NOW() AND purge_at > NOW() - INTERVAL 2 DAY, 0) AS due,
		       status = 'purged' AS marked
		  FROM log_captures
		 WHERE type = 'flow'
		   AND ((` + running + `)
		     OR (purge_at <= NOW() AND purge_at > NOW() - INTERVAL 2 DAY))`
}

const (
	runningHere = `status = 'running' AND expires_at > NOW() AND (deployment_id IS NULL OR deployment_id = ?)`
	runningAny  = `status = 'running' AND expires_at > NOW()`
)

// Captures reads the running and due captures. A store without the
// deployment column yet (migrations not run) reads every capture.
func (s *LogSettings) Captures(ctx context.Context, deployment string) ([]dal.CaptureRow, error) {
	rows, err := s.db.QueryContext(ctx, stCapturesHere.SQL, deployment, deployment)
	if err != nil && unknownColumnNamed(err, "deployment_id") {
		rows, err = s.db.QueryContext(ctx, stCapturesAny.SQL)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.CaptureRow
	for rows.Next() {
		var r dal.CaptureRow
		if err := rows.Scan(&r.ID, &r.ScopeType, &r.ScopeID, &r.Name, &r.SecsLeft, &r.Running, &r.Due, &r.Marked); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// unknownColumnNamed reports a MySQL "Unknown column" error naming col
// (quoted, as MySQL writes it, so "deployment_id" never matches
// "sent_deployment_id").
func unknownColumnNamed(err error, col string) bool {
	return err != nil && strings.Contains(err.Error(), "Unknown column") && strings.Contains(err.Error(), "'"+col+"'")
}

// MarkCapturePurged flips one capture's row.
func (s *LogSettings) MarkCapturePurged(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, stCapturePurge.SQL, id)
	return err
}

// AddCaptureRecords writes a batch in one INSERT. A capture that has already
// been purged takes its rows with it (ON DELETE CASCADE), so a late batch can
// fail its foreign key; the caller only logs.
func (s *LogSettings) AddCaptureRecords(ctx context.Context, batch []dal.CaptureRecord) error {
	var (
		sb   strings.Builder
		args = make([]any, 0, len(batch)*6)
	)
	sb.WriteString(captureInsertHead)
	for i, r := range batch {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?,?)")
		var msgID any
		if r.MessageID != "" {
			msgID = r.MessageID
		}
		args = append(args, r.CaptureID, r.Role, r.At.UTC(), r.Step, msgID, r.Line)
	}
	_, err := s.db.ExecContext(ctx, sb.String(), args...)
	return err
}

// TrimCaptureRecords keeps only the newest maxRecords rows of each capture.
// The cutoff is the id of the Nth newest row; everything at or below it goes.
// One indexed lookup plus one ranged delete, rather than counting rows.
func (s *LogSettings) TrimCaptureRecords(ctx context.Context, ids []int64, maxRecords int) error {
	if maxRecords <= 0 {
		return nil
	}
	for _, id := range ids {
		var cutoff sql.NullInt64
		err := s.db.QueryRowContext(ctx, stCaptureCutoff.SQL, id, maxRecords).Scan(&cutoff)
		if err == sql.ErrNoRows {
			continue // under the cap, nothing to trim
		}
		if err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, stCaptureTrim.SQL, id, cutoff.Int64); err != nil {
			return err
		}
	}
	return nil
}

var _ dal.LogSettings = (*LogSettings)(nil)
