package mssql

import (
	"context"
	"database/sql"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The parts of the config store the shared engine packages read
// (internal/dal/engineconfig.go) on SQL Server, made like
// internal/dal/mysql/engine.go: vendors' send settings, receipt policies,
// change marks, HLR ranges and providers. The audit trail, the inbound
// doors and the log settings are in engine_audit.go, engine_doors.go and
// engine_logs.go. Their statements are in EngineStmts (stmts.go), under the
// MySQL DAL's names.

// addEngine registers an engine statement written with ? marks.
func addEngine(name, q string) *stmt.Stmt {
	return EngineStmts.Add(name, positional(q), false)
}

// openEngineConfig opens the parts on the config store's one pool a process.
func openEngineConfig(cfg config.Config) (*dal.EngineConfig, error) {
	db, err := Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return NewEngineConfig(db), nil
}

// NewEngineConfig is every part on db (the config store).
func NewEngineConfig(db *sql.DB) *dal.EngineConfig {
	return &dal.EngineConfig{
		VendorSends: &VendorSends{db: db},
		Policies:    &ReceiptPolicies{db: db},
		Marks:       &ChangeMarks{db: db},
		Doors:       &InboundDoors{db: db},
		HLR:         &HLRLookupConfig{db: db},
		Audit:       &AuditLog{db: db},
		Logs:        &LogSettings{db: db},
	}
}

// ---- vendors' send settings (attemptcap, queues) ----

var (
	stVendorMaxAttempts = addEngine("vendor_sends.max_attempts",
		`SELECT cid, max_attempts FROM vendors WHERE max_attempts > 0`)
	stVendorHasSplit = addEngine("vendor_sends.has_split",
		`SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID(N'vendors') AND name = 'queue_split'`)
	stVendorSplits = addEngine("vendor_sends.splits",
		`SELECT `+wholeTable+` cid, COALESCE(queue_split,'fair') FROM vendors`)
)

// VendorSends is dal.VendorSendSettings on the config store.
type VendorSends struct{ db *sql.DB }

// MaxAttempts reads each vendor's own cap. A row that would not scan is
// passed over.
func (v *VendorSends) MaxAttempts(ctx context.Context) (map[string]int, error) {
	perVendor := map[string]int{}
	rows, err := v.db.QueryContext(ctx, stVendorMaxAttempts.SQL)
	if err != nil {
		return perVendor, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid string
		var n int
		if rows.Scan(&cid, &n) == nil && n > 0 {
			perVendor[cid] = n
		}
	}
	return perVendor, nil
}

// SplitModes reads every vendor's split mode (vendors.queue_split), by cid.
// A config store without the column, or one that cannot be read, gives none.
func (v *VendorSends) SplitModes(ctx context.Context) map[string]string {
	out := map[string]string{}
	if v == nil || v.db == nil {
		return out
	}
	var n int
	if err := v.db.QueryRowContext(ctx, stVendorHasSplit.SQL).Scan(&n); err != nil || n == 0 {
		return out
	}
	rows, err := v.db.QueryContext(ctx, stVendorSplits.SQL)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var cid, mode string
		if rows.Scan(&cid, &mode) == nil {
			out[cid] = mode
		}
	}
	return out
}

// ---- receipt policies ----

var (
	stPolicyRows = addEngine("receipt_policies.rows",
		`SELECT id, receipt_policy, COALESCE(dlr_levels,''), COALESCE(part_receipts,'final') FROM sms_service_config
		  WHERE receipt_policy IS NOT NULL OR (dlr_levels IS NOT NULL AND dlr_levels <> 'registered') OR part_receipts = 'each'`)
	stPolicyAccounts = addEngine("receipt_policies.accounts", `SELECT `+wholeTable+` id FROM sms_service_config`)
)

// ReceiptPolicies is dal.ReceiptPolicies on the config store.
type ReceiptPolicies struct{ db *sql.DB }

// ReceiptPolicyRows reads the accounts that asked for something.
func (p *ReceiptPolicies) ReceiptPolicyRows(ctx context.Context) ([]dal.ReceiptPolicyRow, error) {
	rows, err := p.db.QueryContext(ctx, stPolicyRows.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.ReceiptPolicyRow
	for rows.Next() {
		var r dal.ReceiptPolicyRow
		if err := rows.Scan(&r.AccountID, &r.Policy, &r.Levels, &r.PartReceipts); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, dal.RowsError{Err: err}
	}
	return out, nil
}

// AccountIDs lists every account.
func (p *ReceiptPolicies) AccountIDs(ctx context.Context) ([]int64, error) {
	rows, err := p.db.QueryContext(ctx, stPolicyAccounts.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, dal.RowsError{Err: err}
	}
	return out, nil
}

// GatewaySettingText is one gateway setting's value as text (the console's
// statement, config.settings.setting_text).
func (p *ReceiptPolicies) GatewaySettingText(ctx context.Context, name string) (dal.NullString, error) {
	return configQ{q: p.db, db: p.db}.GatewaySettingText(ctx, name)
}

// MissingTable reports SQL Server's "Invalid object name" (208).
func (p *ReceiptPolicies) MissingTable(err error) bool { return MissingTable(err) }

// ---- change marks (reloadgate) ----

// The form Fingerprint builds, for one table (reloadgate's callers name
// two or three).
var _ = addEngine("change_marks.fingerprint", fingerprintSQL([]string{"http_endpoints WHERE type = 'dlr'"}))

// ChangeMarks is dal.ChangeMarks on the config store.
type ChangeMarks struct{ db *sql.DB }

// fingerprintSQL is the one statement over the tables: each one's count and
// newest updated_at, and whether any changed in the last second. CONCAT_WS
// takes three arguments at least, so one table's mark stands alone.
func fingerprintSQL(tables []string) string {
	var sigs, fresh []string
	for _, t := range tables {
		sigs = append(sigs, "(SELECT CONCAT(COUNT(*), ':', COALESCE(CONVERT(VARCHAR(27), MAX(updated_at), 121), '')) FROM "+t+")")
		fresh = append(fresh, "(SELECT CASE WHEN MAX(updated_at) >= DATEADD(SECOND, -1, SYSUTCDATETIME()) THEN 1 ELSE 0 END FROM "+t+")")
	}
	sig := sigs[0]
	if len(sigs) > 1 {
		sig = "CONCAT_WS('|', " + strings.Join(sigs, ", ") + ")"
	}
	return "SELECT TOP (1) " + sig + ", GREATEST(0, " + strings.Join(fresh, ", ") + ")"
}

// Fingerprint is a fingerprint over whole tables or parts of them.
func (m *ChangeMarks) Fingerprint(tables ...string) func(ctx context.Context) (string, bool, error) {
	q := fingerprintSQL(tables)
	db := m.db
	return func(ctx context.Context) (string, bool, error) {
		var sig string
		var f int
		if err := db.QueryRowContext(ctx, q).Scan(&sig, &f); err != nil {
			return "", false, err
		}
		return sig, f != 0, nil
	}
}

// NewChangeMarks is the change marks on db.
func NewChangeMarks(db *sql.DB) *ChangeMarks { return &ChangeMarks{db: db} }

// ---- HLR: number ranges and live providers ----

var (
	// The newest change as MySQL's DATE_FORMAT(…, '%Y%m%d%H%i%s').
	stRangesMark = addEngine("hlr_lookups.ranges_mark",
		`SELECT COUNT(*), COALESCE(CONVERT(VARCHAR(8), MAX(updated_at), 112) + REPLACE(CONVERT(VARCHAR(8), MAX(updated_at), 108), ':', ''), '')
		   FROM operators`)
	stRanges = addEngine("hlr_lookups.ranges", `
		SELECT `+wholeTable+` COALESCE(ccc,''), COALESCE(mcc,''), COALESCE(mnc,''),
		       COALESCE(from_series,''), COALESCE(to_series,''), COALESCE(operator_name,'')
		  FROM operators`)
	stHLRProviders = addEngine("hlr_lookups.providers", `
		SELECT id, COALESCE(name,''), COALESCE(url,''), COALESCE(method,'GET'),
		       COALESCE(headers,'{}'), COALESCE(auth,'{}'),
		       COALESCE(timeout_ms,0), COALESCE(cache_ttl_sec,0), COALESCE(config,'{}'),
		       COALESCE(body_template,'')
		  FROM http_endpoints
		 WHERE type='hlr' AND is_active=1 AND url IS NOT NULL AND url <> ''`)
)

// HLRLookupConfig is dal.HLRLookupConfig on the config store.
type HLRLookupConfig struct{ db *sql.DB }

// NumberRangesMark reads the operators table's count and newest change.
func (h *HLRLookupConfig) NumberRangesMark(ctx context.Context) (int64, string, error) {
	var n int64
	var newest sql.NullString
	if err := h.db.QueryRowContext(ctx, stRangesMark.SQL).Scan(&n, &newest); err != nil {
		return 0, "", err
	}
	return n, newest.String, nil
}

// NumberRanges reads every operators row.
func (h *HLRLookupConfig) NumberRanges(ctx context.Context, n int64) ([]dal.OperatorRangeRow, error) {
	rows, err := h.db.QueryContext(ctx, stRanges.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]dal.OperatorRangeRow, 0, n)
	for rows.Next() {
		var row dal.OperatorRangeRow
		if err := rows.Scan(&row.CCC, &row.MCC, &row.MNC, &row.From, &row.To, &row.OperatorName); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// HLRProviders reads the active hlr provider rows.
func (h *HLRLookupConfig) HLRProviders(ctx context.Context) ([]dal.HLRProviderRow, error) {
	rows, err := h.db.QueryContext(ctx, stHLRProviders.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.HLRProviderRow
	for rows.Next() {
		var row dal.HLRProviderRow
		if err := rows.Scan(&row.ID, &row.Name, &row.URL, &row.Method, &row.Headers, &row.Auth,
			&row.TimeoutMS, &row.CacheTTLSec, &row.Config, &row.BodyTemplate); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

var (
	_ dal.VendorSendSettings = (*VendorSends)(nil)
	_ dal.ReceiptPolicies    = (*ReceiptPolicies)(nil)
	_ dal.ChangeMarks        = (*ChangeMarks)(nil)
	_ dal.HLRLookupConfig    = (*HLRLookupConfig)(nil)
)
