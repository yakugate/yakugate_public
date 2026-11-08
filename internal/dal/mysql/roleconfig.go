package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"

	mysqldrv "github.com/go-sql-driver/mysql"
)

// The roles' own reads and writes of the config store (internal/dal
// roleconfig.go), moved here unchanged on 30 Oct 2026 from the roles that
// ran them on their config pool: blackhole, smpp-vendor, http-vendor (the
// vendors they load, a vendor's calls, the status-code actions), the dlr and
// mo roles (the customers' webhooks, an account's company, a vendor's id),
// the http-server (a webhook row, packet captures, a default card) and the
// retention role (its upkeep, the servers it compares).

// RoleStmts is every statement of those parts, by name. A vendor list is
// registered without the deployment filter its call adds at the end
// (deployment.SQLFilter); a ledger partition's ALTER with its name and date
// left as %s.
var RoleStmts = stmt.New("mysql")

// RoleStatements is the registry of these statements (the lint test).
func RoleStatements() *stmt.Registry { return RoleStmts }

var (
	stBlackholeVendors = RoleStmts.Add("vendors.blackhole",
		`SELECT id, cid, COALESCE(dlr_config,'{}'), COALESCE(deployments,'') FROM vendors
		 WHERE transport='blackhole' AND service_status='active'
		 -- Only the vendors placed on THIS deployment (config/0122), like
		 -- every vendor role: a blackhole is placed and chosen like any
		 -- other vendor (Saleem 2026-10-02, ending the 2026-09-30 rule that
		 -- it counted as placed everywhere — the doors let a rule name one
		 -- placed elsewhere, and its messages waited unclaimed). A vendor
		 -- placed nowhere is still claimed everywhere, as before.
		   AND `, false)
	stSMPPVendors = RoleStmts.Add("vendors.smpp", `
		SELECT id, cid, COALESCE(queue_config,'{}'), COALESCE(protocol_config,'{}'), COALESCE(credential_ref,''),
		       sessions, window_size, retry_interval_sec, enquire_link_sec, response_timeout_sec,
		       throttle_pause_sec, batch_size, COALESCE(msgid_transform,''), COALESCE(deployments,''),
		       COALESCE(down_after_sec,30), COALESCE(recheck_after_sec,60), COALESCE(encodings,'null'),
		       COALESCE(dlr_config,'{}'), COALESCE(own_intermediate_dlr,0)
		FROM vendors
		WHERE transport='smpp' AND service_status='active'
		  -- Only the vendors assigned to THIS deployment (config/0122). The
		  -- queue table per vendor is already the partition; pinning a vendor
		  -- means this deployment's roles hold its bind and open its queue,
		  -- which is what a carrier that allowlists one source IP requires.
		  -- A vendor assigned nowhere is bound everywhere, as before.
		  AND `, false)
	stHTTPVendors = RoleStmts.Add("vendors.http",
		`SELECT id, cid, COALESCE(adapter_config,'{}'), COALESCE(deployments,''),
		        COALESCE(down_after_sec,30), COALESCE(recheck_after_sec,60),
		        COALESCE(own_intermediate_dlr,0), COALESCE(dlr_config,'{}') FROM vendors
		 WHERE transport IN ('http','soap') AND service_status='active'
		 -- Only the vendors assigned to THIS deployment (config/0122). The
		 -- queue table per vendor is already the partition: pinning a vendor
		 -- means this deployment's roles open its queue and nobody else's,
		 -- which is what a carrier that allowlists one source IP requires.
		 -- A vendor assigned nowhere is claimed everywhere, as before.
		   AND `, false)
	stHTTPSettingsSig = RoleStmts.Add("vendors.http_signature", `
		SELECT CONCAT_WS('|',
		  (SELECT CONCAT(COUNT(*), ':', COALESCE(BIT_XOR(CRC32(CONCAT_WS('#', id, updated_at, service_status,
		                 COALESCE(CAST(adapter_config AS CHAR),''), COALESCE(CAST(deployments AS CHAR),''),
		                 COALESCE(down_after_sec,0), COALESCE(recheck_after_sec,0), COALESCE(own_intermediate_dlr,0)))),0))
		     FROM vendors WHERE transport IN ('http','soap')),
		  (SELECT CONCAT(COUNT(*), ':', COALESCE(BIT_XOR(CRC32(CONCAT_WS('#', id, updated_at, is_active,
		                 COALESCE(url,''), COALESCE(method,''), COALESCE(CAST(config AS CHAR),''),
		                 COALESCE(CAST(headers AS CHAR),''), COALESCE(CAST(auth AS CHAR),''), COALESCE(body_template,''),
		                 COALESCE(timeout_ms,0), COALESCE(rate_limit,0), COALESCE(submit_concurrency,0),
		                 COALESCE(CAST(tls AS CHAR),''), COALESCE(CAST(retry AS CHAR),''), COALESCE(CAST(circuit_breaker AS CHAR),'')))),0))
		     FROM http_endpoints WHERE type = 'vendor'))`, false)
	stVendorIDByCID = RoleStmts.Add("vendors.id_by_cid", "SELECT id FROM vendors WHERE cid=? LIMIT 1", false)
	stVendorNames   = RoleStmts.Add("vendors.names", "SELECT id, COALESCE(cid,''), COALESCE(name,'') FROM vendors", false)

	stSendKnobs = RoleStmts.Add("vendor_calls.send_knobs",
		`SELECT COALESCE(rate_limit,0), COALESCE(submit_concurrency,0) FROM http_endpoints WHERE id=?`, false)
	stVendorCall = RoleStmts.Add("vendor_calls.row", `
		SELECT id, COALESCE(name,''), COALESCE(url,''), COALESCE(method,'GET'),
		       COALESCE(headers,'{}'), COALESCE(auth,'{}'),
		       COALESCE(timeout_ms,0), COALESCE(cache_ttl_sec,0), COALESCE(config,'{}'),
		       COALESCE(body_template,''), COALESCE(tls,''), COALESCE(retry,'')
		FROM http_endpoints WHERE id=? AND is_active=1`, false)
	stVendorCallOn   = RoleStmts.Add("vendor_calls.active", `SELECT is_active FROM http_endpoints WHERE id=?`, false)
	stCircuitBreaker = RoleStmts.Add("vendor_calls.circuit_breaker",
		`SELECT COALESCE(circuit_breaker,'') FROM http_endpoints WHERE id=?`, false)

	stPauseVendor = RoleStmts.Add("code_actions.pause_vendor",
		`UPDATE vendors SET service_status='paused' WHERE id=? AND service_status='active'`, false)
	stAuditVendorPaused = RoleStmts.Add("code_actions.audit_paused",
		`INSERT INTO audit_log (actor, action, entity, entity_id, detail) VALUES ('smpp-vendor', 'vendor.paused_by_code', 'vendor', ?, CAST(? AS JSON))`, false)
	stBlockForAccount = RoleStmts.Add("code_actions.block", `
		INSERT INTO blocklist (msisdn, account_id, sender, source, reason, is_disabled)
		SELECT ?, ?, NULL, 'vendor', ?, 0 FROM DUAL
		 WHERE NOT EXISTS (SELECT 1 FROM blocklist
		                    WHERE msisdn = ? AND account_id = ? AND is_disabled = 0
		                      AND (expires_at IS NULL OR expires_at > NOW()))`, false)
	stAuditBlockAdded = RoleStmts.Add("code_actions.audit_block",
		`INSERT INTO audit_log (actor, account_id, action, entity, entity_id, detail) VALUES ('smpp-vendor', ?, 'blocklist.added_by_code', 'blocklist', ?, CAST(? AS JSON))`, false)

	// One row per ACCOUNT that names a webhook (config/0174, 6 Nov 2026):
	// the account names its DLR / MO webhook, and many accounts may name
	// one, so a shared webhook comes back once for each of them, each with
	// its own account's channel, attempts, gateways and dialect.
	stDLRWebhooks = RoleStmts.Add("webhooks.dlr",
		`SELECT e.id, a.id, e.url, COALESCE(e.retry,''),
		        COALESCE(a.dlr_channel,'both'), COALESCE(a.dlr_bind_attempts,3),
		        COALESCE(e.rate_limit,0), COALESCE(e.submit_concurrency,1), COALESCE(e.timeout_ms,0),
		        COALESCE(a.deployments,''),
		        COALESCE(e.name,''), COALESCE(e.method,'POST'), COALESCE(e.headers,''), COALESCE(e.auth,''),
		        COALESCE(e.body_template,''), COALESCE(e.tls,''), COALESCE(e.config,''),
		        COALESCE(d.response_map,''), COALESCE(a.company_id,0), COALESCE(a.dlr_max_attempts,0)
		 FROM sms_service_config a
		 JOIN http_endpoints e ON e.id = a.dlr_webhook_id
		 LEFT JOIN sms_service_dialects d ON d.id = a.dialect_id
		 WHERE e.type='dlr' AND e.is_active=1
		   AND e.url IS NOT NULL AND e.url<>''`, false)
	stMOWebhooks = RoleStmts.Add("webhooks.mo",
		`SELECT e.id, a.id, e.url, COALESCE(e.method,'POST'), COALESCE(e.retry,''),
		        COALESCE(e.rate_limit,0), COALESCE(e.submit_concurrency,1), COALESCE(e.timeout_ms,0),
		        COALESCE(a.deployments,''),
		        COALESCE(e.name,''), COALESCE(e.headers,''), COALESCE(e.auth,''),
		        COALESCE(e.body_template,''), COALESCE(e.tls,''), COALESCE(e.config,''),
		        COALESCE(d.response_map,''),
		        COALESCE(a.mo_channel,'both'), COALESCE(a.mo_bind_attempts,3), COALESCE(a.mo_max_attempts,0)
		   FROM sms_service_config a
		   JOIN http_endpoints e ON e.id = a.mo_webhook_id
		   LEFT JOIN sms_service_dialects d ON d.id = a.dialect_id
		 WHERE e.type='mo' AND e.is_active=1
		   AND e.url IS NOT NULL AND e.url<>''`, false)
	stWebhook = RoleStmts.Add("webhooks.row", `
		SELECT COALESCE(name,''), COALESCE(url,''), COALESCE(method,'POST'), COALESCE(headers,''), COALESCE(auth,''),
		       COALESCE(body_template,''), COALESCE(tls,''), COALESCE(config,'')
		  FROM http_endpoints WHERE id=? AND type IN ('dlr','mo')`, false)

	stAccountCompany = RoleStmts.Add("accounts.company",
		"SELECT COALESCE(company_id,0) FROM accounts WHERE id = ?", false)
	stDefaultSellCard = RoleStmts.Add("accounts.default_sell_card",
		`SELECT COALESCE(default_mt_rate_card_id,0) FROM sms_service_config WHERE id=?`, false)

	stCaptureRestarted = RoleStmts.Add("captures.restarted", `UPDATE log_captures
	                    SET status = 'failed', error = 'the gateway was restarted during the capture'
	                  WHERE type = 'traffic' AND status = 'running' AND host = ?`, false)
	stCaptureFailed = RoleStmts.Add("captures.failed",
		`UPDATE log_captures SET status = 'failed', error = ? WHERE id = ? AND type = 'traffic' AND status = 'running'`, false)
	stCaptureEnded = RoleStmts.Add("captures.ended", `UPDATE log_captures
	                    SET status = 'expired', size_bytes = ?, truncated = ?,
	                        expires_at = LEAST(expires_at, NOW())
	                  WHERE id = ? AND type = 'traffic' AND status = 'running'`, false)
	stCaptureOf = RoleStmts.Add("captures.of",
		`SELECT scope_name, COALESCE(reason, '') FROM log_captures WHERE id = ?`, false)
	stCapturePurged = RoleStmts.Add("captures.purged",
		`UPDATE log_captures SET status = 'purged' WHERE id = ? AND type = 'traffic'`, false)

	stCompanyHotDays = RoleStmts.Add("retention.company_hot_days",
		`SELECT id, COALESCE(CAST(data_retention->>'$.hot_days' AS UNSIGNED), 0),
		        COALESCE(CAST(data_retention->>'$.hot_months' AS UNSIGNED), 0) FROM companies`, false)
	stPruneUsageBatches = RoleStmts.Add("retention.prune_usage_batches",
		"DELETE FROM usage_batches WHERE created_at < DATE_SUB(NOW(), INTERVAL ? DAY) LIMIT 5000", false)
	stLedgerPartitions = RoleStmts.Add("retention.ledger_partitions", `
		SELECT COUNT(*) FROM information_schema.partitions
		WHERE table_schema = DATABASE() AND table_name='balance_history' AND partition_name=?`, false)
	stAddLedgerPartition = RoleStmts.Add("retention.add_ledger_partition", `ALTER TABLE balance_history REORGANIZE PARTITION pmax INTO (
		PARTITION %s VALUES LESS THAN ('%s'),
		PARTITION pmax VALUES LESS THAN (MAXVALUE))`, false)
	stPaidPeriods = RoleStmts.Add("retention.paid_periods", `
		SELECT i.company_id, DATE(i.period_from), DATE(i.period_to)
		FROM invoices i
		WHERE i.status='SENT' AND i.payment_status='PAID'
		  AND EXISTS (SELECT 1 FROM balance_history b
		              WHERE b.company_id = i.company_id AND b.type='commit'
		                AND b.created_at >= i.period_from
		                AND b.created_at < DATE_ADD(DATE(i.period_to), INTERVAL 1 DAY))`, false)
	stCommitDays = RoleStmts.Add("retention.commit_days", `
		SELECT DISTINCT DATE(created_at) FROM balance_history
		WHERE company_id=? AND type='commit'
		  AND created_at >= ? AND created_at < DATE_ADD(?, INTERVAL 1 DAY)
		ORDER BY 1`, false)
	stRollupDay = RoleStmts.Add("retention.rollup_day", `
		INSERT IGNORE INTO balance_history_rollup (day, company_id, account_id, commits, amount_total)
		SELECT DATE(created_at), company_id, account_id, COUNT(*), SUM(amount)
		FROM balance_history
		WHERE company_id=? AND type IN ('commit','invoice_item')
		  AND created_at >= ? AND created_at < DATE_ADD(?, INTERVAL 1 DAY)
		GROUP BY 1, 2, 3`, false)
	stRollupDrop = RoleStmts.Add("retention.rollup_drop", `
		DELETE FROM balance_history
		WHERE company_id=? AND type='commit'
		  AND created_at >= ? AND created_at < DATE_ADD(?, INTERVAL 1 DAY)`, false)
	stServerUUID = RoleStmts.Add("server.uuid", "SELECT @@server_uuid", false)
)

// roleParts is the roles' parts on the config store's pool.
type roleParts struct{ db *sql.DB }

var (
	_ dal.VendorRows        = (*roleParts)(nil)
	_ dal.VendorCalls       = (*roleParts)(nil)
	_ dal.VendorCodeActions = (*roleParts)(nil)
	_ dal.CustomerWebhooks  = (*roleParts)(nil)
	_ dal.AccountLookups    = (*roleParts)(nil)
	_ dal.PacketCaptures    = (*roleParts)(nil)
	_ dal.RetentionUpkeep   = (*roleParts)(nil)
)

// ---- the vendors a vendor role loads ----

func (c *roleParts) BlackholeVendors(ctx context.Context, dep string, each func(dal.BlackholeVendorRow, error)) error {
	depFilter, depArgs := deployment.SQLFilter("deployments", dep)
	rows, err := c.db.QueryContext(ctx, stBlackholeVendors.SQL+depFilter, depArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var v dal.BlackholeVendorRow
		err := rows.Scan(&v.ID, &v.CID, &v.DLRConfig, &v.Deployments)
		each(v, err)
	}
	return nil
}

func (c *roleParts) SMPPVendors(ctx context.Context, dep string, each func(dal.SMPPVendorRow, error)) error {
	depFilter, depArgs := deployment.SQLFilter("deployments", dep)
	rows, err := c.db.QueryContext(ctx, stSMPPVendors.SQL+depFilter, depArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var v dal.SMPPVendorRow
		err := rows.Scan(&v.ID, &v.CID, &v.QueueConfig, &v.ProtocolConfig, &v.CredentialRef,
			&v.Sessions, &v.WindowSize, &v.RetrySec, &v.EnquireSec, &v.RespSec, &v.ThrottleSec, &v.BatchSize, &v.MsgIDTransform, &v.Deployments,
			&v.DownAfterSec, &v.RecheckAfterSec, &v.Encodings, &v.DLRConfig, &v.OwnIntermediate)
		each(v, err)
	}
	// A connection lost half way through the rows is a list cut short, not
	// a smaller set of vendors.
	return rows.Err()
}

func (c *roleParts) HTTPVendors(ctx context.Context, dep string, each func(dal.HTTPVendorRow, error)) error {
	depFilter, depArgs := deployment.SQLFilter("deployments", dep)
	rows, err := c.db.QueryContext(ctx, stHTTPVendors.SQL+depFilter, depArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var v dal.HTTPVendorRow
		err := rows.Scan(&v.ID, &v.CID, &v.AdapterConfig, &v.Deployments, &v.DownAfterSec, &v.RecheckAfterSec, &v.OwnIntermediate, &v.DLRConfig)
		each(v, err)
	}
	return nil
}

func (c *roleParts) HTTPSettingsSignature(ctx context.Context) (string, error) {
	var sig string
	err := c.db.QueryRowContext(ctx, stHTTPSettingsSig.SQL).Scan(&sig)
	return sig, err
}

func (c *roleParts) VendorIDByCID(ctx context.Context, cid string) (int64, error) {
	var id int64
	err := c.db.QueryRowContext(ctx, stVendorIDByCID.SQL, cid).Scan(&id)
	return id, err
}

// VendorNames reads every vendor's cid and name (dal.VendorNamer).
func (c *roleParts) VendorNames(ctx context.Context) (map[int64]dal.VendorName, error) {
	rows, err := c.db.QueryContext(ctx, stVendorNames.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]dal.VendorName{}
	for rows.Next() {
		var id int64
		var v dal.VendorName
		if err := rows.Scan(&id, &v.CID, &v.Name); err != nil {
			return nil, err
		}
		out[id] = v
	}
	return out, rows.Err()
}

// ---- a vendor's calls ----

func (c *roleParts) SendKnobs(ctx context.Context, id int64) (batch, concurrency int, err error) {
	err = c.db.QueryRowContext(ctx, stSendKnobs.SQL, id).Scan(&batch, &concurrency)
	return
}

func (c *roleParts) Call(ctx context.Context, id int64) (dal.VendorCallRow, error) {
	var r dal.VendorCallRow
	err := c.db.QueryRowContext(ctx, stVendorCall.SQL, id).
		Scan(&r.ID, &r.Name, &r.URL, &r.Method, &r.Headers, &r.Auth,
			&r.TimeoutMS, &r.CacheTTLSec, &r.Config, &r.BodyTemplate, &r.TLS, &r.Retry)
	return r, err
}

func (c *roleParts) CallActive(ctx context.Context, id int64) (int, error) {
	var active int
	err := c.db.QueryRowContext(ctx, stVendorCallOn.SQL, id).Scan(&active)
	return active, err
}

func (c *roleParts) CircuitBreaker(ctx context.Context, id int64) (string, error) {
	var raw string
	err := c.db.QueryRowContext(ctx, stCircuitBreaker.SQL, id).Scan(&raw)
	return raw, err
}

// ---- what a vendor's status code asks for ----

func (c *roleParts) PauseVendor(ctx context.Context, vendorID int64) (int64, error) {
	res, err := c.db.ExecContext(ctx, stPauseVendor.SQL, vendorID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (c *roleParts) AuditVendorPaused(ctx context.Context, cid, detail string) error {
	_, err := c.db.ExecContext(ctx, stAuditVendorPaused.SQL, cid, detail)
	return err
}

func (c *roleParts) BlockForAccount(ctx context.Context, msisdn string, accountID int64, reason string) (int64, error) {
	res, err := c.db.ExecContext(ctx, stBlockForAccount.SQL,
		msisdn, accountID, reason, msisdn, accountID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (c *roleParts) AuditBlockAdded(ctx context.Context, accountID int64, msisdn, detail string) error {
	_, err := c.db.ExecContext(ctx, stAuditBlockAdded.SQL, accountID, msisdn, detail)
	return err
}

// ---- the customers' webhooks ----

func (c *roleParts) DLRWebhooks(ctx context.Context, each func(dal.WebhookRow, error)) error {
	rows, err := c.db.QueryContext(ctx, stDLRWebhooks.SQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var w dal.WebhookRow
		err := rows.Scan(&w.ID, &w.AccountID, &w.URL, &w.Retry, &w.Channel, &w.BindAttempts, &w.RateLimit, &w.Concurrency, &w.TimeoutMS, &w.Deployments,
			&w.Name, &w.Method, &w.Headers, &w.Auth, &w.BodyTemplate, &w.TLS, &w.Config, &w.ResponseMap, &w.CompanyID, &w.MaxAttempts)
		each(w, err)
	}
	return nil
}

func (c *roleParts) MOWebhooks(ctx context.Context, each func(dal.WebhookRow, error)) error {
	rows, err := c.db.QueryContext(ctx, stMOWebhooks.SQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var w dal.WebhookRow
		err := rows.Scan(&w.ID, &w.AccountID, &w.URL, &w.Method, &w.Retry, &w.RateLimit, &w.Concurrency, &w.TimeoutMS, &w.Deployments,
			&w.Name, &w.Headers, &w.Auth, &w.BodyTemplate, &w.TLS, &w.Config, &w.ResponseMap,
			&w.Channel, &w.BindAttempts, &w.MaxAttempts)
		each(w, err)
	}
	return nil
}

func (c *roleParts) Webhook(ctx context.Context, id int64) (dal.WebhookRow, error) {
	w := dal.WebhookRow{ID: id}
	err := c.db.QueryRowContext(ctx, stWebhook.SQL, id).
		Scan(&w.Name, &w.URL, &w.Method, &w.Headers, &w.Auth, &w.BodyTemplate, &w.TLS, &w.Config)
	return w, err
}

// ---- one-row lookups of an account ----

func (c *roleParts) AccountCompany(ctx context.Context, accountID int64) (int64, error) {
	var company int64
	err := c.db.QueryRowContext(ctx, stAccountCompany.SQL, accountID).Scan(&company)
	return company, err
}

func (c *roleParts) DefaultSellCard(ctx context.Context, accountID int64) (int64, error) {
	var id int64
	err := c.db.QueryRowContext(ctx, stDefaultSellCard.SQL, accountID).Scan(&id)
	return id, err
}

// ---- packet captures ----

func (c *roleParts) FailRunningOnHost(ctx context.Context, host string) error {
	_, err := c.db.ExecContext(ctx, stCaptureRestarted.SQL, host)
	return err
}

func (c *roleParts) Failed(ctx context.Context, id int64, errText string) error {
	_, err := c.db.ExecContext(ctx, stCaptureFailed.SQL, errText, id)
	return err
}

func (c *roleParts) Ended(ctx context.Context, id int64, fileBytes int64, truncated bool) error {
	_, err := c.db.ExecContext(ctx, stCaptureEnded.SQL, fileBytes, truncated, id)
	return err
}

func (c *roleParts) Of(ctx context.Context, id int64) (listener, reason string, err error) {
	err = c.db.QueryRowContext(ctx, stCaptureOf.SQL, id).Scan(&listener, &reason)
	return
}

func (c *roleParts) Purged(ctx context.Context, id int64) error {
	_, err := c.db.ExecContext(ctx, stCapturePurged.SQL, id)
	return err
}

// ---- retention's upkeep ----

func (c *roleParts) CompanyHotDays(ctx context.Context, each func(company int64, days, months int, err error)) error {
	rows, err := c.db.QueryContext(ctx, stCompanyHotDays.SQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var d, m int
		err := rows.Scan(&id, &d, &m)
		each(id, d, m, err)
	}
	return nil
}

func (c *roleParts) PruneUsageBatches(ctx context.Context, keepDays int) (int64, error) {
	res, err := c.db.ExecContext(ctx, stPruneUsageBatches.SQL, keepDays)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (c *roleParts) LedgerPartitions(ctx context.Context, name string) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx, stLedgerPartitions.SQL, name).Scan(&n)
	return n, err
}

func (c *roleParts) AddLedgerPartition(ctx context.Context, name, next string) error {
	_, err := c.db.ExecContext(ctx, fmt.Sprintf(stAddLedgerPartition.SQL, name, next))
	return err
}

func (c *roleParts) PaidPeriods(ctx context.Context) ([]dal.PaidPeriod, error) {
	rows, err := c.db.QueryContext(ctx, stPaidPeriods.SQL)
	if err != nil {
		return nil, err
	}
	var eligible []dal.PaidPeriod
	for rows.Next() {
		var pd dal.PaidPeriod
		if rows.Scan(&pd.Company, &pd.From, &pd.To) == nil {
			eligible = append(eligible, pd)
		}
	}
	rows.Close()
	return eligible, nil
}

func (c *roleParts) CommitDays(ctx context.Context, company int64, from, to time.Time) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, stCommitDays.SQL,
		company, from.Format("2006-01-02"), to.Format("2006-01-02"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d time.Time
		if rows.Scan(&d) == nil {
			out = append(out, d.Format("2006-01-02"))
		}
	}
	return out, rows.Err()
}

func (c *roleParts) RollupDay(ctx context.Context, company int64, day string) (int64, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// INSERT IGNORE: a re-run of an already-rolled day keeps the original
	// totals (the rules are the retention role's, ledger.go).
	if _, err := tx.ExecContext(ctx, stRollupDay.SQL, company, day, day); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, stRollupDrop.SQL, company, day, day)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ---- the servers retention compares ----

// serverID is one pool, asked which server it is on.
type serverID struct{ db *sql.DB }

func (s serverID) ServerUUID(ctx context.Context) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, stServerUUID.SQL).Scan(&id)
	return id, err
}

// NewServerIdentity asks db which server it is on (nil db = nil).
func NewServerIdentity(db *sql.DB) dal.ServerIdentity {
	if db == nil {
		return nil
	}
	return serverID{db: db}
}

// NewRetentionUpkeep is retention's upkeep on a config store pool (nil db =
// nil).
func NewRetentionUpkeep(db *sql.DB) dal.RetentionUpkeep {
	if db == nil {
		return nil
	}
	return &roleParts{db: db}
}

// ServerOnly is a MySQL DSN without its database: the server it names. ""
// for an empty or unreadable DSN.
func ServerOnly(dsn string) string {
	if dsn == "" {
		return ""
	}
	c, err := mysqldrv.ParseDSN(dsn)
	if err != nil {
		return ""
	}
	c.DBName = ""
	return c.FormatDSN()
}

// The retention role's upkeep and the servers it compares.
var _ = serves(func(d *dal.Driver) { d.Retention = openRetention })

// openRetention is retention's opener, on the pools the role opened itself
// before the move: the config store's (the upkeep), and, when the cold DSN
// names a server, that server's and the hot store's. A pool that cannot be
// opened leaves its part nil, as the role's fields were.
func openRetention(cfg config.Config) (*dal.Retention, error) {
	r := &dal.Retention{}
	if srv := ServerOnly(cfg.ColdDSN); srv != "" {
		cold, _ := dbpool.Get(srv)
		hot, _ := dbpool.Get(cfg.HotDSN)
		r.ColdServer, r.HotServer = NewServerIdentity(cold), NewServerIdentity(hot)
	}
	if c, err := dbpool.Get(cfg.ConfigDSN); err == nil {
		r.Upkeep = NewRetentionUpkeep(c)
	}
	return r, nil
}
