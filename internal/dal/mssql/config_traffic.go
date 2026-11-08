package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The small parts of the config store the traffic roles keep beside their
// main work (dal.TrafficConfig: vendor health, status codes, vendors'
// companies, webhook alerts, MO rules) on SQL Server, made like the config
// half of internal/dal/mysql/traffic.go. The roles' own parts are in
// config_roles.go (mysql/roleconfig.go).

// TrafficConfigStmts is every statement of the traffic config, under the
// names of mysql.TrafficStmts and mysql.RoleStmts.
var TrafficConfigStmts = newRegistry()

// addTraffic registers a traffic config statement written with ? marks.
func addTraffic(name, q string) *stmt.Stmt {
	return TrafficConfigStmts.Add(name, positional(q), false)
}

// openTrafficConfig opens the traffic roles' parts of the config store on
// its one pool a process.
func openTrafficConfig(cfg config.Config) (*dal.TrafficConfig, error) {
	db, err := Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return NewTrafficConfig(db), nil
}

// NewTrafficConfig opens every part on db (the config store's pool); a nil db
// opens none.
func NewTrafficConfig(db *sql.DB) *dal.TrafficConfig {
	if db == nil {
		return &dal.TrafficConfig{}
	}
	c := &trafficParts{db: db}
	r := &trafficRoleParts{db: db}
	return &dal.TrafficConfig{Health: c, Codes: c, Companies: c, Alerts: c, MO: c,
		Vendors: r, Calls: r, Actions: r, Webhooks: r, Accounts: r, Captures: r}
}

type trafficParts struct{ db *sql.DB }

var (
	_ dal.VendorHealth      = (*trafficParts)(nil)
	_ dal.VendorStatusCodes = (*trafficParts)(nil)
	_ dal.VendorCompanies   = (*trafficParts)(nil)
	_ dal.WebhookAlerts     = (*trafficParts)(nil)
	_ dal.MORules           = (*trafficParts)(nil)
)

var (
	stHealthRows = addTraffic("vendor_health.rows", `
		SELECT `+wholeTable+` vd.id, vd.cid, vd.service_status, COALESCE(vd.down_after_sec,30),
		       h.failing_since, h.reported_at, COALESCE(h.detail,''), COALESCE(vd.transport,''),
		       COALESCE(h.instance,'')
		  FROM vendors vd LEFT JOIN vendor_health h ON h.vendor_id = vd.id`)
	stHealthForget = addTraffic("vendor_health.forget", `DELETE FROM vendor_health WHERE instance=?`)
	stHealthPrune  = addTraffic("vendor_health.prune", `DELETE FROM vendor_health WHERE reported_at < ?`)
	// An instance's reports are ONE batch: the rows (one JSON array, read
	// with OPENJSON) upserted — UPDATE under UPDLOCK + SERIALIZABLE, then
	// INSERT — and the instance's other rows dropped. @p1 instance, @p2 role,
	// @p3 the time of the report, @p4 the rows.
	stHealthReport = addTraffic("vendor_health.report", `SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @yk_r TABLE (vendor_id BIGINT NOT NULL PRIMARY KEY, failing_since DATETIME2(3) NULL, detail NVARCHAR(190) NULL);
INSERT INTO @yk_r (vendor_id, failing_since, detail)
SELECT v, f, d FROM OPENJSON(@p4) WITH (v BIGINT '$.v', f DATETIME2(3) '$.f', d NVARCHAR(190) '$.d');
BEGIN TRAN;
UPDATE t SET role = @p2, failing_since = r.failing_since, reported_at = @p3, detail = r.detail
  FROM vendor_health AS t WITH (UPDLOCK, SERIALIZABLE) JOIN @yk_r AS r ON t.vendor_id = r.vendor_id AND t.instance = @p1;
INSERT INTO vendor_health (vendor_id, instance, role, failing_since, reported_at, detail)
SELECT r.vendor_id, @p1, @p2, r.failing_since, @p3, r.detail FROM @yk_r AS r
 WHERE NOT EXISTS (SELECT 1 FROM vendor_health AS t WITH (UPDLOCK, SERIALIZABLE) WHERE t.vendor_id = r.vendor_id AND t.instance = @p1);
COMMIT;
DELETE FROM vendor_health WHERE instance = @p1 AND vendor_id NOT IN (SELECT vendor_id FROM @yk_r);`)
	// The drop of an instance's other rows is the end of the report's batch.
	stHealthDropOthers = addTraffic("vendor_health.drop_others",
		`DELETE FROM vendor_health WHERE instance = @p1 AND vendor_id NOT IN (SELECT v FROM OPENJSON(@p2) WITH (v BIGINT '$.v'))`)

	stStatusCodes = addTraffic("vendor_status.codes",
		`SELECT `+wholeTable+` vendor_id, scope, code, action, COALESCE(target_status,'') FROM vendor_status_codes`)
	stStatusSwitches = addTraffic("vendor_status.switches",
		`SELECT `+wholeTable+` id, dlr_code_overrides, COALESCE(cid,'') FROM vendors`)
	stVendorCompanies = addTraffic("vendor_group.companies",
		"SELECT "+wholeTable+" id, COALESCE(company_id,0) FROM vendors")

	stAlertClear  = addTraffic("webhook_alert.clear", "DELETE FROM webhook_delivery_alerts WHERE endpoint_id=? AND kind=?")
	stAlertFailed = AddUpsert(TrafficConfigStmts, "webhook_alert.failed", UpsertSpec{
		Table: "webhook_delivery_alerts", Keys: []string{"endpoint_id"},
		Set: []string{"last_failed_at = SYSUTCDATETIME()",
			"given_up_count = t.given_up_count + $given_up_count",
			"failed_count = t.failed_count + $failed_count", "last_error = $last_error",
			"account_id = $account_id", "url = $url", "updated_at = SYSUTCDATETIME()"},
		Insert: []string{"endpoint_id", "account_id", "kind", "url", "first_failed_at", "last_failed_at",
			"given_up_count", "failed_count", "last_error"},
		InsertValues: []string{"", "", "", "", "SYSUTCDATETIME()", "SYSUTCDATETIME()"},
	}, false)

	// match_mccmncs replaced match_mcc/match_mnc in migration 0048.
	stMORoutes = addTraffic("mo.routes",
		`SELECT r.id, r.account_id, COALESCE(a.username,''), r.priority, r.receiver_regex, r.keyword_regex,
		        r.sender_regex, COALESCE(r.vendor_id,0), COALESCE(r.match_mccmncs,''),
		        COALESCE(a.optout_mode,'customer'), COALESCE(a.optout_keywords,''), COALESCE(a.optin_keywords,''),
		        COALESCE(a.company_id,0), CASE WHEN c.id IS NOT NULL THEN c.body_privacy ELSE 'plain_text' END,
		        COALESCE(a.deployments,'')
		 FROM routes r JOIN sms_service_config a ON a.id = r.account_id
		 LEFT JOIN companies c ON c.id = a.company_id
		 WHERE r.direction='MO' AND r.is_disabled=0 AND r.account_id IS NOT NULL
		 ORDER BY r.priority ASC`)
	// MySQL's INSERT … ON DUPLICATE KEY UPDATE here never meets its key:
	// uq_block is (msisdn, account_id, sender) and the sender is NULL, which
	// a unique key never matches (on MySQL, and in the filtered index here).
	// So every opt-out is a new row on both.
	stMOOptOut = addTraffic("mo.opt_out", `
		INSERT INTO blocklist (msisdn, account_id, sender, source, reason, is_disabled)
		VALUES (?, ?, NULL, 'unsub', ?, 0)`)
	stMOOptIn = addTraffic("mo.opt_in", `
		UPDATE blocklist SET is_disabled = 1, updated_at = SYSUTCDATETIME()
		 WHERE msisdn = ? AND account_id = ? AND source = 'unsub'`)
)

// Rows reads every vendor and its reports once.
func (c *trafficParts) Rows(ctx context.Context) ([]dal.VendorHealthRow, error) {
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

// healthJSON is one report as the report statement reads it.
type healthJSON struct {
	V int64   `json:"v"`
	F *string `json:"f"`
	D *string `json:"d"`
}

// Report writes an instance's reports and drops its other rows, in one
// round trip. A vendor reported twice keeps its last report, as the
// multi-row upsert does on MySQL.
func (c *trafficParts) Report(ctx context.Context, instance, role string, at time.Time, reps []dal.VendorHealthReport) error {
	if len(reps) == 0 {
		return c.Forget(ctx, instance)
	}
	_ = stHealthDropOthers
	byVendor := map[int64]int{}
	var rows []healthJSON
	for _, r := range reps {
		var since, detail *string
		if !r.FailingSince.IsZero() {
			s := r.FailingSince.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000")
			since = &s
		}
		if r.Detail != "" {
			d := r.Detail
			detail = &d
		}
		row := healthJSON{V: r.VendorID, F: since, D: detail}
		if i, ok := byVendor[r.VendorID]; ok {
			rows[i] = row
			continue
		}
		byVendor[r.VendorID] = len(rows)
		rows = append(rows, row)
	}
	text, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, stHealthReport.SQL, instance, role, Time(at), string(text))
	return err
}

// Forget drops every row of an instance.
func (c *trafficParts) Forget(ctx context.Context, instance string) error {
	_, err := c.db.ExecContext(ctx, stHealthForget.SQL, instance)
	return err
}

// Prune drops rows reported before `before`.
func (c *trafficParts) Prune(ctx context.Context, before time.Time) error {
	_, err := c.db.ExecContext(ctx, stHealthPrune.SQL, Time(before))
	return err
}

// StatusCodes reads every vendor_status_codes row.
func (c *trafficParts) StatusCodes(ctx context.Context) ([]dal.StatusCodeRow, error) {
	rows, err := c.db.QueryContext(ctx, stStatusCodes.SQL)
	if err != nil {
		return nil, fmt.Errorf("vendor_status_codes: %w", err)
	}
	defer rows.Close()
	var out []dal.StatusCodeRow
	for rows.Next() {
		var r dal.StatusCodeRow
		if err := rows.Scan(&r.VendorID, &r.Scope, &r.Code, &r.Action, &r.Target); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Switches reads every vendor's dlr_code_overrides switch and cid.
func (c *trafficParts) Switches(ctx context.Context) ([]dal.VendorReceiptSwitch, error) {
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
func (c *trafficParts) VendorCompanies(ctx context.Context) (map[int64]int64, error) {
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
func (c *trafficParts) Failed(ctx context.Context, a dal.WebhookAlert) error {
	_, err := stAlertFailed.Exec(ctx, c.db,
		sql.Named("endpoint_id", a.EndpointID), sql.Named("account_id", a.AccountID), sql.Named("kind", Varchar(a.Kind)),
		sql.Named("url", a.URL), sql.Named("given_up_count", a.GaveUp), sql.Named("failed_count", a.Failed),
		sql.Named("last_error", a.LastError))
	return err
}

// Clear drops the endpoint's row of a kind.
func (c *trafficParts) Clear(ctx context.Context, endpointID int64, kind string) error {
	_, err := c.db.ExecContext(ctx, stAlertClear.SQL, endpointID, Varchar(kind))
	return err
}

// MORoutes reads every enabled MO rule, by priority.
func (c *trafficParts) MORoutes(ctx context.Context) ([]dal.MORouteRow, error) {
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
		r.Receiver, r.Keyword, r.Sender = trafficNullable(recv), trafficNullable(kw), trafficNullable(snd)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		// What was read is still answered (never nil here), as on MySQL.
		if out == nil {
			out = []dal.MORouteRow{}
		}
		return out, err
	}
	return out, nil
}

func trafficNullable(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	s := v.String
	return &s
}

// OptOut blocks msisdn for the account as an unsubscribe.
func (c *trafficParts) OptOut(ctx context.Context, accountID int64, msisdn, sender string) error {
	_, err := c.db.ExecContext(ctx, stMOOptOut.SQL, msisdn, accountID, "opted out by SMS to "+sender)
	return err
}

// OptIn disables the account's unsubscribe row for msisdn.
func (c *trafficParts) OptIn(ctx context.Context, accountID int64, msisdn string) error {
	_, err := c.db.ExecContext(ctx, stMOOptIn.SQL, msisdn, accountID)
	return err
}
