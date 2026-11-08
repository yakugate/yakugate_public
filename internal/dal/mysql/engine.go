package mysql

import (
	"context"
	"database/sql"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// The parts of the config store the shared engine packages read
// (internal/dal/engineconfig.go), moved here unchanged on 29 Oct 2026 (the
// DAL plan) from internal/attemptcap, internal/queues (settings.go),
// internal/receiptpolicy (cache.go), internal/reloadgate, internal/inboundauth
// (cache.go, legacy.go), internal/hlr (ranges.go, live.go), internal/audit and
// internal/logcfg; and the named leases from internal/singleton (locks.go).

// EngineStmts is every statement of those parts, by name. A statement built
// at run time (a column list, an IN list, a table name) is registered in a
// representative form; the text a call sends is built the same way.
var EngineStmts = stmt.New("mysql")

// The shared engine packages' parts of the config store, on its one pool.
var _ = serves(func(d *dal.Driver) { d.EngineConfig = openEngineConfig })

// openEngineConfig opens the parts on the config store's one pool a process
// — the pool the roles' cfgDB is.
func openEngineConfig(cfg config.Config) (*dal.EngineConfig, error) {
	db, err := dbpool.Get(cfg.ConfigDSN)
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

// ---- the queue store's readiness (pipeline.QueueHealth) ----

// Ping asks the queue store's pool whether it answers, for readiness (the
// pipeline pinged DB() itself until 29 Oct 2026). A store with no pool
// answers dal.ErrNoQueueStore.
func (s *Store) Ping(ctx context.Context) error {
	db := s.DB()
	if db == nil {
		return dal.ErrNoQueueStore
	}
	return db.PingContext(ctx)
}

// ---- vendors' send settings (attemptcap, queues) ----

var (
	stVendorMaxAttempts = EngineStmts.Add("vendor_sends.max_attempts",
		`SELECT cid, max_attempts FROM vendors WHERE max_attempts > 0`, false)
	stVendorHasSplit = EngineStmts.Add("vendor_sends.has_split", `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'vendors' AND column_name = 'queue_split'`, false)
	stVendorSplits = EngineStmts.Add("vendor_sends.splits",
		`SELECT cid, COALESCE(queue_split,'fair') FROM vendors`, false)
)

// VendorSends is dal.VendorSendSettings on the config store.
type VendorSends struct{ db *sql.DB }

// MaxAttempts reads each vendor's own cap. A row that would not scan is
// passed over, as attemptcap.Apply did.
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

// SplitModes reads every vendor's split mode (vendors.queue_split,
// config/0161), by cid. A config store without the column, or one that
// cannot be read, gives none.
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
	stPolicyRows = EngineStmts.Add("receipt_policies.rows",
		`SELECT id, receipt_policy, COALESCE(dlr_levels,''), COALESCE(part_receipts,'final') FROM sms_service_config
		  WHERE receipt_policy IS NOT NULL OR (dlr_levels IS NOT NULL AND dlr_levels <> 'registered') OR part_receipts = 'each'`, false)
	stPolicyAccounts = EngineStmts.Add("receipt_policies.accounts", `SELECT id FROM sms_service_config`, false)
)

// ReceiptPolicies is dal.ReceiptPolicies on the config store.
type ReceiptPolicies struct{ db *sql.DB }

// ReceiptPolicyRows reads the accounts that asked for something: on a
// gateway where nobody uses either setting this reads no rows at all.
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

// GatewaySettingText is one gateway setting's value as text (the same
// statement as the console's, config.settings.setting_text).
func (p *ReceiptPolicies) GatewaySettingText(ctx context.Context, name string) (dal.NullString, error) {
	return configQ{p.db}.GatewaySettingText(ctx, name)
}

// MissingTable reports MySQL's ER_NO_SUCH_TABLE.
func (p *ReceiptPolicies) MissingTable(err error) bool { return isMissingTable(err) }

// ---- change marks (reloadgate) ----

func init() {
	// The form Fingerprint builds, for one table (reloadgate's callers name
	// two or three).
	EngineStmts.Add("change_marks.fingerprint", fingerprintSQL([]string{"http_endpoints WHERE type = 'dlr'"}), false)
}

// ChangeMarks is dal.ChangeMarks on the config store.
type ChangeMarks struct{ db *sql.DB }

// fingerprintSQL is the one statement over the tables.
func fingerprintSQL(tables []string) string {
	var sigs, fresh []string
	for _, t := range tables {
		sigs = append(sigs, "(SELECT CONCAT(COUNT(*), ':', COALESCE(MAX(updated_at), '')) FROM "+t+")")
		fresh = append(fresh, "(SELECT COALESCE(MAX(updated_at) >= NOW() - INTERVAL 1 SECOND, 0) FROM "+t+")")
	}
	return "SELECT CONCAT_WS('|', " + strings.Join(sigs, ", ") + "), GREATEST(0, " + strings.Join(fresh, ", ") + ")"
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
	stRangesMark = EngineStmts.Add("hlr_lookups.ranges_mark",
		`SELECT COUNT(*), COALESCE(DATE_FORMAT(MAX(updated_at), '%Y%m%d%H%i%s'), '') FROM operators`, false)
	stRanges = EngineStmts.Add("hlr_lookups.ranges", `
		SELECT COALESCE(ccc,''), COALESCE(mcc,''), COALESCE(mnc,''),
		       COALESCE(from_series,''), COALESCE(to_series,''), COALESCE(operator_name,'')
		  FROM operators`, false)
	stHLRProviders = EngineStmts.Add("hlr_lookups.providers", `
		SELECT id, COALESCE(name,''), COALESCE(url,''), COALESCE(method,'GET'),
		       COALESCE(headers,'{}'), COALESCE(auth,'{}'),
		       COALESCE(timeout_ms,0), COALESCE(cache_ttl_sec,0), COALESCE(config,'{}'),
		       COALESCE(body_template,'')
		  FROM http_endpoints
		 WHERE type='hlr' AND is_active=1 AND url IS NOT NULL AND url <> ''`, false)
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
