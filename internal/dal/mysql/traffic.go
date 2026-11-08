package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// Vendor quality (the hot store) and the traffic roles' parts of the config
// store.
var _ = serves(func(d *dal.Driver) {
	d.VendorQuality = openVendorQuality
	d.TrafficConfig = openTrafficConfig
})

// openVendorQuality is vendor quality's opener: vendor_quality_minute in the
// hot store, on its one pool a process (vendorquality.Open asks this only
// when the hot store is MySQL and named).
func openVendorQuality(cfg config.Config) (dal.VendorQuality, error) {
	db, err := dbpool.Get(cfg.HotDSN) // the process's one pool for the hot store
	if err != nil {
		return nil, err
	}
	return NewVendorQuality(db), nil
}

// openTrafficConfig opens the traffic roles' parts of the config store on
// its one pool a process — the pool the roles' cfgDB is.
func openTrafficConfig(cfg config.Config) (*dal.TrafficConfig, error) {
	db, err := dbpool.Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return NewTrafficConfig(db), nil
}

// The small parts of the config store the traffic roles keep beside their
// main work, and vendor quality's per-minute table in the hot store — moved
// here unchanged on 29 Oct 2026 (the DAL plan) from internal/vendorquality,
// vendorhealth, vendorstatus, vendorgroup, webhookalert and mo.

// TrafficStmts is every statement of those parts and of the multi-part
// buffer (concat.go), by name. A statement whose rows are added in a loop
// (several rows a statement) is registered in its one-row form; the text a
// call sends is that form with more rows.
var TrafficStmts = stmt.New("mysql")

var (
	stQualityAdd = TrafficStmts.Add("vendor_quality.add",
		qualityAddHead+"(?,?,?,?,?,?)"+qualityAddTail, false)
	stQualityPrune = TrafficStmts.Add("vendor_quality.prune",
		`DELETE FROM vendor_quality_minute WHERE minute < ?`, false)
	stQualityLoad = TrafficStmts.Add("vendor_quality.load", `
		SELECT vendor_id, mcc, mnc, SUM(delivered), SUM(judged)
		  FROM vendor_quality_minute WHERE minute >= ?
		 GROUP BY vendor_id, mcc, mnc`, false)

	stHealthRows = TrafficStmts.Add("vendor_health.rows", `
		SELECT vd.id, vd.cid, vd.service_status, COALESCE(vd.down_after_sec,30),
		       h.failing_since, h.reported_at, COALESCE(h.detail,''), COALESCE(vd.transport,''),
		       COALESCE(h.instance,'')
		  FROM vendors vd LEFT JOIN vendor_health h ON h.vendor_id = vd.id`, false)
	stHealthForget = TrafficStmts.Add("vendor_health.forget", `DELETE FROM vendor_health WHERE instance=?`, false)
	stHealthPrune  = TrafficStmts.Add("vendor_health.prune", `DELETE FROM vendor_health WHERE reported_at < ?`, false)
	stHealthReport = TrafficStmts.Add("vendor_health.report",
		healthReportHead+"(?,?,?,?,?,?)"+healthReportTail, false)
	stHealthDropOthers = TrafficStmts.Add("vendor_health.drop_others",
		healthDropHead+"?)", false)

	stStatusCodes = TrafficStmts.Add("vendor_status.codes",
		`SELECT vendor_id, scope, code, action, COALESCE(target_status,'') FROM vendor_status_codes`, false)
	stStatusSwitches = TrafficStmts.Add("vendor_status.switches",
		`SELECT id, dlr_code_overrides, COALESCE(cid,'') FROM vendors`, false)
	stVendorCompanies = TrafficStmts.Add("vendor_group.companies",
		"SELECT id, COALESCE(company_id,0) FROM vendors", false)

	stAlertClear = TrafficStmts.Add("webhook_alert.clear",
		"DELETE FROM webhook_delivery_alerts WHERE endpoint_id=? AND kind=?", false)
	stAlertFailed = TrafficStmts.Add("webhook_alert.failed", `
		INSERT INTO webhook_delivery_alerts
		  (endpoint_id, account_id, kind, url, first_failed_at, last_failed_at, given_up_count, failed_count, last_error)
		VALUES (?,?,?,?,NOW(),NOW(),?,?,?)
		ON DUPLICATE KEY UPDATE
		  last_failed_at=NOW(), given_up_count=given_up_count+VALUES(given_up_count),
		  failed_count=failed_count+VALUES(failed_count), last_error=VALUES(last_error),
		  account_id=VALUES(account_id), url=VALUES(url)`, false)

	// match_mccmncs replaced match_mcc/match_mnc in migration 0048 (combined
	// "mccmnc" strings, e.g. "41001"; NULL/empty = any network).
	stMORoutes = TrafficStmts.Add("mo.routes",
		`SELECT r.id, r.account_id, COALESCE(a.username,''), r.priority, r.receiver_regex, r.keyword_regex,
		        r.sender_regex, COALESCE(r.vendor_id,0), COALESCE(r.match_mccmncs,''),
		        COALESCE(a.optout_mode,'customer'), COALESCE(a.optout_keywords,''), COALESCE(a.optin_keywords,''),
		        COALESCE(a.company_id,0), CASE WHEN c.id IS NOT NULL THEN c.body_privacy ELSE 'plain_text' END,
		        COALESCE(CAST(a.deployments AS CHAR),'')
		 FROM routes r JOIN sms_service_config a ON a.id = r.account_id
		 LEFT JOIN companies c ON c.id = a.company_id
		 WHERE r.direction='MO' AND r.is_disabled=0 AND r.account_id IS NOT NULL
		 ORDER BY r.priority ASC`, false)
	stMOOptOut = TrafficStmts.Add("mo.opt_out", `
		INSERT INTO blocklist (msisdn, account_id, sender, source, reason, is_disabled)
		VALUES (?, ?, NULL, 'unsub', ?, 0)
		ON DUPLICATE KEY UPDATE
		  source      = 'unsub',
		  is_disabled = 0,
		  reason      = VALUES(reason),
		  expires_at  = NULL`, false)
	stMOOptIn = TrafficStmts.Add("mo.opt_in", `
		UPDATE blocklist SET is_disabled = 1
		 WHERE msisdn = ? AND account_id = ? AND source = 'unsub'`, false)
)

// TrafficStatements is the registry of these statements (the lint test).
func TrafficStatements() *stmt.Registry { return TrafficStmts }

// ---- vendor quality (hot store) ----

const (
	qualityAddHead = `INSERT INTO vendor_quality_minute (minute,vendor_id,mcc,mnc,delivered,judged) VALUES `
	qualityAddTail = ` ON DUPLICATE KEY UPDATE delivered=delivered+VALUES(delivered), judged=judged+VALUES(judged)`
)

// VendorQuality adds to vendor_quality_minute (hot/0036) when Redis is off. A
// small counter table, one row per minute, vendor and network — not a query
// over messages. Minutes and cutoffs are computed here, never with NOW().
type VendorQuality struct {
	db        *sql.DB
	mu        sync.Mutex
	lastPrune time.Time
}

var _ dal.VendorQuality = (*VendorQuality)(nil)

// NewVendorQuality wraps a hot-store connection. nil db = nil store.
func NewVendorQuality(db *sql.DB) *VendorQuality {
	if db == nil {
		return nil
	}
	return &VendorQuality{db: db}
}

// Add adds the buckets, 200 rows a statement, and once an hour drops what is
// older than a day.
func (s *VendorQuality) Add(ctx context.Context, bs []dal.QualityBucket) error {
	const per = 200
	for start := 0; start < len(bs); start += per {
		end := start + per
		if end > len(bs) {
			end = len(bs)
		}
		chunk := bs[start:end]
		var sb strings.Builder
		sb.WriteString(qualityAddHead)
		args := make([]any, 0, len(chunk)*6)
		for i, b := range chunk {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("(?,?,?,?,?,?)")
			args = append(args, b.Minute.UTC(), b.Key.VendorID, b.Key.MCC, b.Key.MNC, b.Counts.Delivered, b.Counts.Judged)
		}
		sb.WriteString(qualityAddTail)
		if _, err := s.db.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("vendor_quality_minute: %w", err)
		}
	}
	s.mu.Lock()
	prune := time.Since(s.lastPrune) >= time.Hour
	if prune {
		s.lastPrune = time.Now()
	}
	s.mu.Unlock()
	if prune {
		// A day is far more than the one-hour window any reader uses; the
		// rest is only kept so the table never grows past a day of rows.
		_, _ = s.db.ExecContext(ctx, stQualityPrune.SQL, time.Now().UTC().Add(-24*time.Hour))
	}
	return nil
}

// Load sums the window.
func (s *VendorQuality) Load(ctx context.Context, window time.Duration) (map[dal.QualityKey]dal.QualityCounts, error) {
	rows, err := s.db.QueryContext(ctx, stQualityLoad.SQL, time.Now().UTC().Add(-window).Truncate(time.Minute))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[dal.QualityKey]dal.QualityCounts{}
	for rows.Next() {
		var k dal.QualityKey
		var c dal.QualityCounts
		if err := rows.Scan(&k.VendorID, &k.MCC, &k.MNC, &c.Delivered, &c.Judged); err != nil {
			return nil, err
		}
		out[k] = c
	}
	return out, rows.Err()
}

// ---- the config store's small parts ----

// NewTrafficConfig opens every part on db (the config store's pool); a nil db
// opens none.
func NewTrafficConfig(db *sql.DB) *dal.TrafficConfig {
	if db == nil {
		return &dal.TrafficConfig{}
	}
	c := &configParts{db: db}
	r := &roleParts{db: db} // the roles' own parts (roleconfig.go)
	return &dal.TrafficConfig{Health: c, Codes: c, Companies: c, Alerts: c, MO: c,
		Vendors: r, Calls: r, Actions: r, Webhooks: r, Accounts: r, Captures: r}
}

type configParts struct{ db *sql.DB }

var (
	_ dal.VendorHealth      = (*configParts)(nil)
	_ dal.VendorStatusCodes = (*configParts)(nil)
	_ dal.VendorCompanies   = (*configParts)(nil)
	_ dal.WebhookAlerts     = (*configParts)(nil)
	_ dal.MORules           = (*configParts)(nil)
)

const (
	healthReportHead = `INSERT INTO vendor_health (vendor_id,instance,role,failing_since,reported_at,detail) VALUES `
	healthReportTail = ` ON DUPLICATE KEY UPDATE role=VALUES(role), failing_since=VALUES(failing_since),
		reported_at=VALUES(reported_at), detail=VALUES(detail)`
	healthDropHead = `DELETE FROM vendor_health WHERE instance=? AND vendor_id NOT IN (`
)

// Rows reads every vendor and its reports once.
func (c *configParts) Rows(ctx context.Context) ([]dal.VendorHealthRow, error) {
	rs, err := c.db.QueryContext(ctx, stHealthRows.SQL)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var rows []dal.VendorHealthRow
	for rs.Next() {
		var r dal.VendorHealthRow
		var since, at sql.NullTime
		if err := rs.Scan(&r.ID, &r.CID, &r.Status, &r.DownAfterSec, &since, &at, &r.Detail, &r.Transport, &r.Instance); err != nil {
			return nil, err
		}
		r.FailingSince = dal.NullTime{Time: since.Time, Valid: since.Valid}
		r.ReportedAt = dal.NullTime{Time: at.Time, Valid: at.Valid}
		rows = append(rows, r)
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

// Report writes an instance's reports and drops its other rows: one
// multi-row upsert, one DELETE.
func (c *configParts) Report(ctx context.Context, instance, role string, at time.Time, reps []dal.VendorHealthReport) error {
	if len(reps) == 0 {
		return c.Forget(ctx, instance)
	}
	var sb strings.Builder
	args := make([]any, 0, len(reps)*6)
	ids := make([]any, 0, len(reps)+1)
	ids = append(ids, instance)
	sb.WriteString(healthReportHead)
	for i, r := range reps {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?,?,?,?,?,?)")
		var since any
		if !r.FailingSince.IsZero() {
			since = r.FailingSince.UTC().Truncate(time.Millisecond)
		}
		var detail any
		if r.Detail != "" {
			detail = r.Detail
		}
		args = append(args, r.VendorID, instance, role, since, at, detail)
		ids = append(ids, r.VendorID)
	}
	sb.WriteString(healthReportTail)
	if _, err := c.db.ExecContext(ctx, sb.String(), args...); err != nil {
		return err
	}
	_, err := c.db.ExecContext(ctx, healthDropHead+
		strings.TrimRight(strings.Repeat("?,", len(ids)-1), ",")+`)`, ids...)
	return err
}

// Forget drops every row of an instance.
func (c *configParts) Forget(ctx context.Context, instance string) error {
	_, err := c.db.ExecContext(ctx, stHealthForget.SQL, instance)
	return err
}

// Prune drops rows reported before `before`.
func (c *configParts) Prune(ctx context.Context, before time.Time) error {
	_, err := c.db.ExecContext(ctx, stHealthPrune.SQL, before)
	return err
}

// StatusCodes reads every vendor_status_codes row.
func (c *configParts) StatusCodes(ctx context.Context) ([]dal.StatusCodeRow, error) {
	rows, err := c.db.QueryContext(ctx, stStatusCodes.SQL)
	if err != nil {
		return nil, fmt.Errorf("vendor_status_codes: %w", err)
	}
	var out []dal.StatusCodeRow
	for rows.Next() {
		var r dal.StatusCodeRow
		if err := rows.Scan(&r.VendorID, &r.Scope, &r.Code, &r.Action, &r.Target); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Switches reads every vendor's dlr_code_overrides switch and cid.
func (c *configParts) Switches(ctx context.Context) ([]dal.VendorReceiptSwitch, error) {
	orows, err := c.db.QueryContext(ctx, stStatusSwitches.SQL)
	if err != nil {
		return nil, fmt.Errorf("vendors.dlr_code_overrides: %w", err)
	}
	defer orows.Close()
	var out []dal.VendorReceiptSwitch
	for orows.Next() {
		var s dal.VendorReceiptSwitch
		var on int
		if err := orows.Scan(&s.VendorID, &on, &s.CID); err != nil {
			return nil, err
		}
		s.On = on != 0
		out = append(out, s)
	}
	if err := orows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// VendorCompanies reads every vendor's company (0 = none).
func (c *configParts) VendorCompanies(ctx context.Context) (map[int64]int64, error) {
	rows, err := c.db.QueryContext(ctx, stVendorCompanies.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var v, co int64
		if err := rows.Scan(&v, &co); err != nil {
			return nil, err
		}
		out[v] = co
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Failed adds counted failures to the endpoint's row.
func (c *configParts) Failed(ctx context.Context, a dal.WebhookAlert) error {
	_, err := c.db.ExecContext(ctx, stAlertFailed.SQL,
		a.EndpointID, a.AccountID, a.Kind, a.URL, a.GaveUp, a.Failed, a.LastError)
	return err
}

// Clear drops the endpoint's row of a kind.
func (c *configParts) Clear(ctx context.Context, endpointID int64, kind string) error {
	_, err := c.db.ExecContext(ctx, stAlertClear.SQL, endpointID, kind)
	return err
}

// MORoutes reads every enabled MO rule, by priority.
func (c *configParts) MORoutes(ctx context.Context) ([]dal.MORouteRow, error) {
	rows, err := c.db.QueryContext(ctx, stMORoutes.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.MORouteRow
	for rows.Next() {
		var r dal.MORouteRow
		var recv, kw, snd sql.NullString
		if err := rows.Scan(&r.ID, &r.AccountID, &r.Username, &r.Priority, &recv, &kw, &snd, &r.VendorID, &r.MCCMNCs,
			&r.OptOutMode, &r.OptOutWords, &r.OptInWords, &r.CompanyID, &r.Privacy, &r.Deployments); err != nil {
			return nil, err
		}
		r.Receiver, r.Keyword, r.Sender = nullable(recv), nullable(kw), nullable(snd)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		// What was read is still answered (never nil here): the service
		// installed it and said the error, as before the move.
		if out == nil {
			out = []dal.MORouteRow{}
		}
		return out, err
	}
	return out, nil
}

func nullable(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	s := v.String
	return &s
}

// OptOut blocks msisdn for the account as an unsubscribe (idempotent by the
// table's own unique key).
func (c *configParts) OptOut(ctx context.Context, accountID int64, msisdn, sender string) error {
	_, err := c.db.ExecContext(ctx, stMOOptOut.SQL, msisdn, accountID, "opted out by SMS to "+sender)
	return err
}

// OptIn disables the account's unsubscribe row for msisdn.
func (c *configParts) OptIn(ctx context.Context, accountID int64, msisdn string) error {
	_, err := c.db.ExecContext(ctx, stMOOptIn.SQL, msisdn, accountID)
	return err
}
