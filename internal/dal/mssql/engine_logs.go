package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// The log settings, flow captures and capture records (internal/logcfg) on
// SQL Server, made like internal/dal/mysql/engine_logs.go.

var (
	stLogAccounts = addEngine("log_settings.accounts", `SELECT `+wholeTable+` username, log_config FROM sms_service_config`)
	stLogVendors  = addEngine("log_settings.vendors", `SELECT `+wholeTable+` cid, log_config FROM vendors`)
	stLogMasters  = addEngine("log_settings.masters", `
		SELECT a.username, c.master_number
		  FROM sms_service_config a JOIN companies c ON c.id = a.company_id
		 WHERE c.master_number IS NOT NULL AND c.master_number <> ''`)
	stLogSystem = addEngine("log_settings.system", `SELECT log_config FROM system_log_config WHERE id = 1`)

	stCapturesHere = addEngine("captures.running_here", captureSelect(runningHere))
	stCapturesAny  = addEngine("captures.running_any", captureSelect(runningAny))
	stCapturePurge = addEngine("captures.mark_purged",
		`UPDATE log_captures SET status = 'purged', updated_at = SYSUTCDATETIME() WHERE id = ? AND status <> 'purged'`)

	// A batch of records is ONE statement, as on MySQL: the rows go as one
	// JSON array (OPENJSON), so no batch meets the 2,100 parameters a
	// statement may carry.
	stCaptureRecordsAdd = addEngine("capture_records.add", `
		INSERT INTO log_capture_records (capture_id, role, at, step, message_id, line)
		SELECT capture_id, role, at, step, message_id, line
		  FROM OPENJSON(?) WITH (capture_id BIGINT '$.c', role NVARCHAR(32) '$.r', at DATETIME2(3) '$.a',
		                         step NVARCHAR(32) '$.s', message_id CHAR(26) '$.m', line NVARCHAR(MAX) '$.l')`)
	// The (n+1)th newest record of a capture (MySQL's ORDER BY id DESC LIMIT
	// 1 OFFSET n, which SQL Server's rules ban): no row while the capture
	// holds n or fewer.
	stCaptureCutoff = addEngine("capture_records.cutoff", `
			SELECT MIN(id) FROM (SELECT TOP (@p2 + 1) id FROM log_capture_records
			 WHERE capture_id = @p1 ORDER BY id DESC) k HAVING COUNT(*) > @p2`)
	stCaptureTrim = addEngine("capture_records.trim",
		`DELETE FROM log_capture_records WHERE capture_id = ? AND id <= ?`)
)

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
// REMAINING (whole seconds crossed, as MySQL's TIMESTAMPDIFF from its
// second-wide NOW()). Flow captures only.
func captureSelect(running string) string {
	return `
		SELECT id, COALESCE(scope_type,''), COALESCE(scope_id,0), COALESCE(scope_name,''),
		       COALESCE(DATEDIFF_BIG(SECOND, SYSUTCDATETIME(), expires_at), 0) AS secs_left,
		       CASE WHEN ` + running + ` THEN 1 ELSE 0 END AS running,
		       CASE WHEN purge_at <= SYSUTCDATETIME() AND purge_at > DATEADD(DAY, -2, SYSUTCDATETIME()) THEN 1 ELSE 0 END AS due,
		       CASE WHEN status = 'purged' THEN 1 ELSE 0 END AS marked
		  FROM log_captures
		 WHERE type = 'flow'
		   AND ((` + running + `)
		     OR (purge_at <= SYSUTCDATETIME() AND purge_at > DATEADD(DAY, -2, SYSUTCDATETIME())))`
}

const (
	runningHere = `status = 'running' AND expires_at > SYSUTCDATETIME() AND (deployment_id IS NULL OR deployment_id = ?)`
	runningAny  = `status = 'running' AND expires_at > SYSUTCDATETIME()`
)

// Captures reads the running and due captures. A store without the
// deployment column yet (migrations not run) reads every capture.
func (s *LogSettings) Captures(ctx context.Context, deployment string) ([]dal.CaptureRow, error) {
	rows, err := s.db.QueryContext(ctx, stCapturesHere.SQL, deployment, deployment)
	if err != nil && invalidColumnNamed(err, "deployment_id") {
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

// invalidColumnNamed reports SQL Server's "Invalid column name 'col'." (207).
func invalidColumnNamed(err error, col string) bool {
	return invalidColumn(err) && strings.Contains(err.Error(), "'"+col+"'")
}

// MarkCapturePurged flips one capture's row.
func (s *LogSettings) MarkCapturePurged(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, stCapturePurge.SQL, id)
	return err
}

// captureRecordJSON is one record as the add statement reads it.
type captureRecordJSON struct {
	C int64   `json:"c"`
	R string  `json:"r"`
	A string  `json:"a"`
	S string  `json:"s"`
	M *string `json:"m"`
	L string  `json:"l"`
}

// AddCaptureRecords writes a batch in one INSERT. A capture that has already
// been purged takes its rows with it (ON DELETE CASCADE), so a late batch can
// fail its foreign key; the caller only logs.
func (s *LogSettings) AddCaptureRecords(ctx context.Context, batch []dal.CaptureRecord) error {
	if len(batch) == 0 {
		return nil
	}
	rows := make([]captureRecordJSON, len(batch))
	for i, r := range batch {
		var msgID *string
		if r.MessageID != "" {
			m := r.MessageID
			msgID = &m
		}
		rows[i] = captureRecordJSON{C: r.CaptureID, R: r.Role, A: r.At.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000"),
			S: r.Step, M: msgID, L: r.Line}
	}
	text, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, stCaptureRecordsAdd.SQL, string(text))
	return err
}

// TrimCaptureRecords keeps only the newest maxRecords rows of each capture.
// The cutoff is the id of the (N+1)th newest row; everything at or below it
// goes. One indexed lookup plus one ranged delete, rather than counting rows.
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
