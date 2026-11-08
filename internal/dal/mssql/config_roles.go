package mssql

import (
	"context"
	"database/sql"

	"github.com/yakugate/yakugate/internal/dal"
)

// The roles' own reads and writes of the config store that are parts of
// dal.TrafficConfig (the vendors a vendor role loads, a vendor's calls, the
// status-code actions, the customers' webhooks, an account's lookups, packet
// captures) on SQL Server, made like internal/dal/mysql/roleconfig.go. The
// retention role's upkeep is not here.
//
// A vendor role loads only the vendors placed on its deployment
// (config/0122): the vendor lists end with deploymentFilter, as the MySQL
// statements end with deployment.SQLFilter. A vendor placed nowhere is
// loaded everywhere.

// trafficRoleParts is the roles' parts on the config store's pool.
type trafficRoleParts struct{ db *sql.DB }

var (
	_ dal.VendorRows        = (*trafficRoleParts)(nil)
	_ dal.VendorCalls       = (*trafficRoleParts)(nil)
	_ dal.VendorCodeActions = (*trafficRoleParts)(nil)
	_ dal.CustomerWebhooks  = (*trafficRoleParts)(nil)
	_ dal.AccountLookups    = (*trafficRoleParts)(nil)
	_ dal.PacketCaptures    = (*trafficRoleParts)(nil)
)

// deploymentAll is the deployment id that places a row everywhere
// (internal/deployment's legacyAll).
const deploymentAll = "*"

// deploymentFilter is deployment.SQLFilter for SQL Server: the rows of a
// deployments column (a JSON array of {"deployment_id": …} objects) placed
// nowhere, everywhere, or on dep. "" for dep is every row. The id is matched
// exactly, as MySQL's JSON_CONTAINS matches it.
func deploymentFilter(column, dep string) (string, []any) {
	if dep == "" {
		return "1=1", nil
	}
	q := "(" + column + " IS NULL OR " + jsonLength(column) + " = 0" +
		" OR EXISTS (SELECT 1 FROM OPENJSON(CASE WHEN ISJSON(" + column + ", ARRAY) = 1 THEN " + column + " END) AS d" +
		" WHERE d.[type] = 5 AND JSON_VALUE(d.[value], '$.deployment_id') COLLATE Latin1_General_100_BIN2 IN (N'" + deploymentAll + "', ?))" +
		" OR (ISJSON(" + column + ", OBJECT) = 1 AND JSON_VALUE(" + column + ", '$.deployment_id') COLLATE Latin1_General_100_BIN2 IN (N'" + deploymentAll + "', ?)))"
	return q, []any{dep, dep}
}

// The vendor lists up to their deployment filter.
const (
	blackholeVendorsHead = `SELECT ` + wholeTable + ` id, cid, COALESCE(dlr_config,'{}'), COALESCE(deployments,'') FROM vendors
		 WHERE transport='blackhole' AND service_status='active' AND `
	smppVendorsHead = `
		SELECT ` + wholeTable + ` id, cid, COALESCE(queue_config,'{}'), COALESCE(protocol_config,'{}'), COALESCE(credential_ref,''),
		       sessions, window_size, retry_interval_sec, enquire_link_sec, response_timeout_sec,
		       throttle_pause_sec, batch_size, COALESCE(msgid_transform,''), COALESCE(deployments,''),
		       COALESCE(down_after_sec,30), COALESCE(recheck_after_sec,60), COALESCE(encodings,'null'),
		       COALESCE(dlr_config,'{}'), COALESCE(own_intermediate_dlr,0)
		FROM vendors
		WHERE transport='smpp' AND service_status='active' AND `
	httpVendorsHead = `SELECT ` + wholeTable + ` id, cid, COALESCE(adapter_config,'{}'), COALESCE(deployments,''),
		        COALESCE(down_after_sec,30), COALESCE(recheck_after_sec,60),
		        COALESCE(own_intermediate_dlr,0), COALESCE(dlr_config,'{}') FROM vendors
		 WHERE transport IN ('http','soap') AND service_status='active' AND `
)

// deploymentFilterOne is the filter of one deployment, as registered.
var deploymentFilterOne, _ = deploymentFilter("deployments", "dep")

var (
	// Registered with the filter of one deployment.
	stBlackholeVendors = addTraffic("vendors.blackhole", blackholeVendorsHead+deploymentFilterOne)
	stSMPPVendors      = addTraffic("vendors.smpp", smppVendorsHead+deploymentFilterOne)
	stHTTPVendors      = addTraffic("vendors.http", httpVendorsHead+deploymentFilterOne)
	// MySQL's BIT_XOR(CRC32(CONCAT_WS('#', …))): an order-free sum of a hash
	// of each row (the first four bytes of its MD5, XORed by CHECKSUM_AGG).
	stHTTPSettingsSig = addTraffic("vendors.http_signature", `
		SELECT CONCAT_WS('|',
		  (SELECT CONCAT(COUNT(*), ':', COALESCE(CHECKSUM_AGG(CAST(CAST(HASHBYTES('MD5', CONCAT_WS(N'#', id, updated_at, service_status,
		                 COALESCE(adapter_config,''), COALESCE(deployments,''),
		                 COALESCE(down_after_sec,0), COALESCE(recheck_after_sec,0), COALESCE(own_intermediate_dlr,0))) AS BINARY(4)) AS INT)),0))
		     FROM vendors WHERE transport IN ('http','soap')),
		  (SELECT CONCAT(COUNT(*), ':', COALESCE(CHECKSUM_AGG(CAST(CAST(HASHBYTES('MD5', CONCAT_WS(N'#', id, updated_at, is_active,
		                 COALESCE(url,''), COALESCE(method,''), COALESCE(config,''),
		                 COALESCE(headers,''), COALESCE(auth,''), COALESCE(body_template,''),
		                 COALESCE(timeout_ms,0), COALESCE(rate_limit,0), COALESCE(submit_concurrency,0),
		                 COALESCE(tls,''), COALESCE(retry,''), COALESCE(circuit_breaker,''))) AS BINARY(4)) AS INT)),0))
		     FROM http_endpoints WHERE type = 'vendor'))`)
	stVendorIDByCID = addTraffic("vendors.id_by_cid", "SELECT TOP (1) id FROM vendors WHERE cid=?")
	stVendorNames   = addTraffic("vendors.names", "SELECT "+wholeTable+" id, COALESCE(cid,''), COALESCE(name,'') FROM vendors")

	stSendKnobs = addTraffic("vendor_calls.send_knobs",
		`SELECT COALESCE(rate_limit,0), COALESCE(submit_concurrency,0) FROM http_endpoints WHERE id=?`)
	stVendorCall = addTraffic("vendor_calls.row", `
		SELECT id, COALESCE(name,''), COALESCE(url,''), COALESCE(method,'GET'),
		       COALESCE(headers,'{}'), COALESCE(auth,'{}'),
		       COALESCE(timeout_ms,0), COALESCE(cache_ttl_sec,0), COALESCE(config,'{}'),
		       COALESCE(body_template,''), COALESCE(tls,''), COALESCE(retry,'')
		FROM http_endpoints WHERE id=? AND is_active=1`)
	stVendorCallOn   = addTraffic("vendor_calls.active", `SELECT is_active FROM http_endpoints WHERE id=?`)
	stCircuitBreaker = addTraffic("vendor_calls.circuit_breaker",
		`SELECT COALESCE(circuit_breaker,'') FROM http_endpoints WHERE id=?`)

	stPauseVendor = addTraffic("code_actions.pause_vendor",
		`UPDATE vendors SET service_status='paused', updated_at = SYSUTCDATETIME() WHERE id=? AND service_status='active'`)
	stAuditVendorPaused = addTraffic("code_actions.audit_paused",
		`INSERT INTO audit_log (actor, action, entity, entity_id, detail) VALUES ('smpp-vendor', 'vendor.paused_by_code', 'vendor', ?, ?)`)
	// The number blocked unless an open block of the account has it; the
	// range read is held (HOLDLOCK) so two roles do not both add one.
	stBlockForAccount = addTraffic("code_actions.block", `
		INSERT INTO blocklist (msisdn, account_id, sender, source, reason, is_disabled)
		SELECT ?, ?, NULL, 'vendor', ?, 0
		 WHERE NOT EXISTS (SELECT 1 FROM blocklist WITH (UPDLOCK, HOLDLOCK)
		                    WHERE msisdn = ? AND account_id = ? AND is_disabled = 0
		                      AND (expires_at IS NULL OR expires_at > SYSUTCDATETIME()))`)
	stAuditBlockAdded = addTraffic("code_actions.audit_block",
		`INSERT INTO audit_log (actor, account_id, action, entity, entity_id, detail) VALUES ('smpp-vendor', ?, 'blocklist.added_by_code', 'blocklist', ?, ?)`)

	// One row per ACCOUNT that names a webhook (config/0174, 6 Nov 2026):
	// the account names its DLR / MO webhook, and many accounts may name
	// one, so a shared webhook comes back once for each of them, each with
	// its own account's channel, attempts, gateways and dialect.
	stDLRWebhooks = addTraffic("webhooks.dlr",
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
		   AND e.url IS NOT NULL AND e.url<>''`)
	stMOWebhooks = addTraffic("webhooks.mo",
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
		   AND e.url IS NOT NULL AND e.url<>''`)
	stWebhook = addTraffic("webhooks.row", `
		SELECT COALESCE(name,''), COALESCE(url,''), COALESCE(method,'POST'), COALESCE(headers,''), COALESCE(auth,''),
		       COALESCE(body_template,''), COALESCE(tls,''), COALESCE(config,'')
		  FROM http_endpoints WHERE id=? AND type IN ('dlr','mo')`)

	// As on MySQL this names a table "accounts", which no schema has had
	// since config/0006: the lookup answers "Invalid object name" (MySQL
	// 1146) until it is pointed at sms_service_config.
	stAccountCompany = addTraffic("accounts.company",
		"SELECT COALESCE(company_id,0) FROM accounts WHERE id = ?")
	stDefaultSellCard = addTraffic("accounts.default_sell_card",
		`SELECT COALESCE(default_mt_rate_card_id,0) FROM sms_service_config WHERE id=?`)

	stCaptureRestarted = addTraffic("captures.restarted", `UPDATE log_captures
	                    SET status = 'failed', error = 'the gateway was restarted during the capture', updated_at = SYSUTCDATETIME()
	                  WHERE type = 'traffic' AND status = 'running' AND host = ?`)
	stCaptureFailed = addTraffic("captures.failed",
		`UPDATE log_captures SET status = 'failed', error = ?, updated_at = SYSUTCDATETIME() WHERE id = ? AND type = 'traffic' AND status = 'running'`)
	// LEAST(expires_at, NOW()), NULL when expires_at is.
	stCaptureEnded = addTraffic("captures.ended", `UPDATE log_captures
	                    SET status = 'expired', size_bytes = ?, truncated = ?,
	                        expires_at = CASE WHEN expires_at > SYSUTCDATETIME() THEN SYSUTCDATETIME() ELSE expires_at END,
	                        updated_at = SYSUTCDATETIME()
	                  WHERE id = ? AND type = 'traffic' AND status = 'running'`)
	stCaptureOf = addTraffic("captures.of",
		`SELECT scope_name, COALESCE(reason, '') FROM log_captures WHERE id = ?`)
	stCapturePurged = addTraffic("captures.purged",
		`UPDATE log_captures SET status = 'purged', updated_at = SYSUTCDATETIME() WHERE id = ? AND type = 'traffic'`)
)

// ---- the vendors a vendor role loads ----

func (c *trafficRoleParts) BlackholeVendors(ctx context.Context, dep string, each func(dal.BlackholeVendorRow, error)) error {
	depFilter, depArgs := deploymentFilter("deployments", dep)
	rows, err := c.db.QueryContext(ctx, positional(blackholeVendorsHead+depFilter), depArgs...)
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

func (c *trafficRoleParts) SMPPVendors(ctx context.Context, dep string, each func(dal.SMPPVendorRow, error)) error {
	depFilter, depArgs := deploymentFilter("deployments", dep)
	rows, err := c.db.QueryContext(ctx, positional(smppVendorsHead+depFilter), depArgs...)
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

func (c *trafficRoleParts) HTTPVendors(ctx context.Context, dep string, each func(dal.HTTPVendorRow, error)) error {
	depFilter, depArgs := deploymentFilter("deployments", dep)
	rows, err := c.db.QueryContext(ctx, positional(httpVendorsHead+depFilter), depArgs...)
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

func (c *trafficRoleParts) HTTPSettingsSignature(ctx context.Context) (string, error) {
	var sig string
	err := c.db.QueryRowContext(ctx, stHTTPSettingsSig.SQL).Scan(&sig)
	return sig, err
}

func (c *trafficRoleParts) VendorIDByCID(ctx context.Context, cid string) (int64, error) {
	var id int64
	err := c.db.QueryRowContext(ctx, stVendorIDByCID.SQL, cid).Scan(&id)
	return id, err
}

// VendorNames reads every vendor's cid and name (dal.VendorNamer).
func (c *trafficRoleParts) VendorNames(ctx context.Context) (map[int64]dal.VendorName, error) {
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

func (c *trafficRoleParts) SendKnobs(ctx context.Context, id int64) (batch, concurrency int, err error) {
	err = c.db.QueryRowContext(ctx, stSendKnobs.SQL, id).Scan(&batch, &concurrency)
	return
}

func (c *trafficRoleParts) Call(ctx context.Context, id int64) (dal.VendorCallRow, error) {
	var r dal.VendorCallRow
	err := c.db.QueryRowContext(ctx, stVendorCall.SQL, id).
		Scan(&r.ID, &r.Name, &r.URL, &r.Method, &r.Headers, &r.Auth,
			&r.TimeoutMS, &r.CacheTTLSec, &r.Config, &r.BodyTemplate, &r.TLS, &r.Retry)
	return r, err
}

func (c *trafficRoleParts) CallActive(ctx context.Context, id int64) (int, error) {
	var active int
	err := c.db.QueryRowContext(ctx, stVendorCallOn.SQL, id).Scan(&active)
	return active, err
}

func (c *trafficRoleParts) CircuitBreaker(ctx context.Context, id int64) (string, error) {
	var raw string
	err := c.db.QueryRowContext(ctx, stCircuitBreaker.SQL, id).Scan(&raw)
	return raw, err
}

// ---- what a vendor's status code asks for ----

func (c *trafficRoleParts) PauseVendor(ctx context.Context, vendorID int64) (int64, error) {
	res, err := c.db.ExecContext(ctx, stPauseVendor.SQL, vendorID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (c *trafficRoleParts) AuditVendorPaused(ctx context.Context, cid, detail string) error {
	_, err := c.db.ExecContext(ctx, stAuditVendorPaused.SQL, cid, detail)
	return err
}

func (c *trafficRoleParts) BlockForAccount(ctx context.Context, msisdn string, accountID int64, reason string) (int64, error) {
	res, err := c.db.ExecContext(ctx, stBlockForAccount.SQL,
		msisdn, accountID, reason, msisdn, accountID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (c *trafficRoleParts) AuditBlockAdded(ctx context.Context, accountID int64, msisdn, detail string) error {
	_, err := c.db.ExecContext(ctx, stAuditBlockAdded.SQL, accountID, msisdn, detail)
	return err
}

// ---- the customers' webhooks ----

func (c *trafficRoleParts) DLRWebhooks(ctx context.Context, each func(dal.WebhookRow, error)) error {
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

func (c *trafficRoleParts) MOWebhooks(ctx context.Context, each func(dal.WebhookRow, error)) error {
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

func (c *trafficRoleParts) Webhook(ctx context.Context, id int64) (dal.WebhookRow, error) {
	w := dal.WebhookRow{ID: id}
	err := c.db.QueryRowContext(ctx, stWebhook.SQL, id).
		Scan(&w.Name, &w.URL, &w.Method, &w.Headers, &w.Auth, &w.BodyTemplate, &w.TLS, &w.Config)
	return w, err
}

// ---- one-row lookups of an account ----

func (c *trafficRoleParts) AccountCompany(ctx context.Context, accountID int64) (int64, error) {
	var company int64
	err := c.db.QueryRowContext(ctx, stAccountCompany.SQL, accountID).Scan(&company)
	return company, err
}

func (c *trafficRoleParts) DefaultSellCard(ctx context.Context, accountID int64) (int64, error) {
	var id int64
	err := c.db.QueryRowContext(ctx, stDefaultSellCard.SQL, accountID).Scan(&id)
	return id, err
}

// ---- packet captures ----

func (c *trafficRoleParts) FailRunningOnHost(ctx context.Context, host string) error {
	_, err := c.db.ExecContext(ctx, stCaptureRestarted.SQL, host)
	return err
}

func (c *trafficRoleParts) Failed(ctx context.Context, id int64, errText string) error {
	_, err := c.db.ExecContext(ctx, stCaptureFailed.SQL, errText, id)
	return err
}

func (c *trafficRoleParts) Ended(ctx context.Context, id int64, fileBytes int64, truncated bool) error {
	_, err := c.db.ExecContext(ctx, stCaptureEnded.SQL, fileBytes, cfgBool(truncated), id)
	return err
}

func (c *trafficRoleParts) Of(ctx context.Context, id int64) (listener, reason string, err error) {
	err = c.db.QueryRowContext(ctx, stCaptureOf.SQL, id).Scan(&listener, &reason)
	return
}

func (c *trafficRoleParts) Purged(ctx context.Context, id int64) error {
	_, err := c.db.ExecContext(ctx, stCapturePurged.SQL, id)
	return err
}
