package mysql

import (
	"context"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// Vendors and their HTTP calls (dal.Vendors), moved from internal/provision
// (vendor.go, endpoint.go, vendorqueue.go) on 29 Oct 2026, unchanged. The
// rules — who may own a vendor, which call fits which slot, what a call may
// say — stayed in provision.

var (
	// Vendors.
	stVendorCompanyKind = ConfigStmts.Add("config.vendors.company_kind",
		"SELECT COALESCE(name,''), COALESCE(type,'') FROM companies WHERE id = ?", false)
	stVendorCIDTaken = ConfigStmts.Add("config.vendors.cid_taken", "SELECT COUNT(*) FROM vendors WHERE cid = ?", false)
	stVendorInsert   = ConfigStmts.Add("config.vendors.insert",
		`INSERT INTO vendors (cid, name, company_id, transport, service_status,
		                      queue_config, adapter_config, dlr_config, log_config,
		                      deployments)
		 VALUES (?,?,?,?,'active',?,?,'{}','{}',?)`, false)
	stVendorList = ConfigStmts.Add("config.vendors.list", `
		SELECT v.id, v.cid, COALESCE(v.name,''), COALESCE(c.name,'—'), v.transport,
		       v.service_status, COALESCE(v.adapter_config,'{}')
		  FROM vendors v
		  LEFT JOIN companies c ON c.id = v.company_id
		 ORDER BY v.id LIMIT ?`, false)
	stEndpointForProfile = ConfigStmts.Add("config.endpoints.for_profile", `
		SELECT id, COALESCE(name,''), COALESCE(url,''), COALESCE(method,'GET'),
		       COALESCE(headers,'{}'), COALESCE(auth,'{}'),
		       COALESCE(timeout_ms,0), COALESCE(cache_ttl_sec,0), COALESCE(config,'{}'),
		       COALESCE(body_template,'')
		FROM http_endpoints WHERE id=? AND is_active=1`, false)
	stVendorSetStatus = ConfigStmts.Add("config.vendors.set_status", "UPDATE vendors SET service_status = ? WHERE id = ?", false)
	stVendorDetail    = ConfigStmts.Add("config.vendors.detail", `
		SELECT v.id, v.cid, COALESCE(v.name,''), COALESCE(c.name,'—'), v.transport,
		       v.service_status, COALESCE(v.company_id,0),
		       COALESCE(v.queue_config,'{}'), COALESCE(v.adapter_config,'{}'),
		       IF(EXISTS(SELECT 1 FROM inbound_endpoints e WHERE e.vendor_id = v.id AND (
		              (e.token_hash IS NOT NULL AND e.token_hash <> '')
		              OR (e.username IS NOT NULL AND e.password IS NOT NULL)
		              OR (e.jwt IS NOT NULL AND JSON_LENGTH(e.jwt) > 0))), 'token', ''),
		       COALESCE(JSON_EXTRACT(v.protocol_config,'$.encodings'),'null'),
		       COALESCE(v.encodings,'null')
		  FROM vendors v
		  LEFT JOIN companies c ON c.id = v.company_id
		 WHERE v.id = ?`, false)
	stVendorRouteCount = ConfigStmts.Add("config.vendors.route_count", `SELECT COUNT(*) FROM routes WHERE vendor_id = ?
		OR (send_plan IS NOT NULL AND JSON_CONTAINS(JSON_EXTRACT(send_plan,'$**.vendor_id'), CAST(? AS JSON)))`, false)
	stVendorSetChargePoint = ConfigStmts.Add("config.vendors.set_charge_point", "UPDATE vendors SET charge_point = ? WHERE id = ?", false)
	stVendorSetDLRConfig   = ConfigStmts.Add("config.vendors.set_dlr_config",
		"UPDATE vendors SET dlr_config = JSON_SET(IF(JSON_TYPE(dlr_config) = 'OBJECT', dlr_config, JSON_OBJECT()), ?, ?) WHERE id = ?", false)
	stVendorSetName    = ConfigStmts.Add("config.vendors.set_name", "UPDATE vendors SET name = ? WHERE id = ?", false)
	stVendorSetCompany = ConfigStmts.Add("config.vendors.set_company", "UPDATE vendors SET company_id = ? WHERE id = ?", false)
	stVendorSetAdapter = ConfigStmts.Add("config.vendors.set_adapter", "UPDATE vendors SET adapter_config = CAST(? AS JSON) WHERE id = ?", false)
	stVendorRemoveProt = ConfigStmts.Add("config.vendors.remove_protocol_path",
		"UPDATE vendors SET protocol_config = JSON_REMOVE(protocol_config, ?) WHERE id = ? AND JSON_CONTAINS_PATH(protocol_config,'one',?)", false)
	stVendorMergeEnc = ConfigStmts.Add("config.vendors.merge_encodings", `
			UPDATE vendors SET protocol_config = JSON_SET(COALESCE(protocol_config,'{}'), '$.encodings',
			       JSON_MERGE_PATCH(COALESCE(JSON_EXTRACT(protocol_config,'$.encodings'), JSON_OBJECT()), CAST(? AS JSON)))
			 WHERE id = ?`, false)
	stVendorSetQueueConfig = ConfigStmts.Add("config.vendors.set_queue_config",
		"UPDATE vendors SET queue_config = JSON_SET(COALESCE(queue_config,'{}'),?,?) WHERE id = ?", false)
	stVendorDelete        = ConfigStmts.Add("config.vendors.delete", "DELETE FROM vendors WHERE id = ?", false)
	stVendorAdapterConfig = ConfigStmts.Add("config.vendors.adapter_config", "SELECT COALESCE(adapter_config,'{}') FROM vendors WHERE id = ?", false)

	// The vendor's queues (config/0161).
	stVendorQueueColumns = ConfigStmts.Add("config.vendors.queue_columns", `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'vendors' AND column_name IN ('queue_split','short_codes')`, false)
	stVendorQueueSettings = ConfigStmts.Add("config.vendors.queue_settings",
		"SELECT COALESCE(queue_split,'fair'), COALESCE(CAST(short_codes AS CHAR),'') FROM vendors WHERE id = ?", false)
	stVendorSetQueueSplit = ConfigStmts.Add("config.vendors.set_queue_split", "UPDATE vendors SET queue_split = ? WHERE id = ?", false)
	stVendorSetShortCodes = ConfigStmts.Add("config.vendors.set_short_codes", "UPDATE vendors SET short_codes = CAST(? AS JSON) WHERE id = ?", false)

	// HTTP calls.
	stVendorCIDTransport = ConfigStmts.Add("config.endpoints.vendor_cid_transport",
		"SELECT cid, COALESCE(transport,'') FROM vendors WHERE id = ?", false)
	stVendorTransport = ConfigStmts.Add("config.endpoints.vendor_transport", "SELECT COALESCE(transport,'') FROM vendors WHERE id = ?", false)
	stEndpointInsert  = ConfigStmts.Add("config.endpoints.insert", `
		INSERT INTO http_endpoints (type, name, vendor_id, account_id, url, method,
		                            headers, auth, timeout_ms, rate_limit, submit_concurrency, config, is_active)
		VALUES (?,?,?,?,?,?,'{}','{}',?,?,?,CAST(? AS JSON),1)`, false)
	// The list's WHERE is "" or "WHERE e.type = ?"; registered with it.
	stEndpointList = ConfigStmts.Add("config.endpoints.list", endpointListHead+"WHERE e.type = ?"+endpointListTail, false)
	stEndpointKind = ConfigStmts.Add("config.endpoints.kind",
		"SELECT type, COALESCE(name,''), COALESCE(CAST(config AS CHAR),'') FROM http_endpoints WHERE id = ?", false)
	// The accounts that name a webhook (config/0174), by username, then kind.
	stWebhookUsers = ConfigStmts.Add("config.endpoints.webhook_users", `
		SELECT dlr_webhook_id AS endpoint_id, id AS account_id, username, 'dlr' AS kind
		  FROM sms_service_config WHERE dlr_webhook_id = ?
		UNION ALL
		SELECT mo_webhook_id, id, username, 'mo' FROM sms_service_config WHERE mo_webhook_id = ?
		 ORDER BY username, kind`, false)
	stAllWebhookUsers = ConfigStmts.Add("config.endpoints.all_webhook_users", `
		SELECT dlr_webhook_id AS endpoint_id, id AS account_id, username, 'dlr' AS kind
		  FROM sms_service_config WHERE dlr_webhook_id IS NOT NULL
		UNION ALL
		SELECT mo_webhook_id, id, username, 'mo' FROM sms_service_config WHERE mo_webhook_id IS NOT NULL
		 ORDER BY username, kind`, false)
	stVendorAdapterConfigs = ConfigStmts.Add("config.endpoints.vendor_adapters",
		"SELECT cid, CAST(adapter_config AS CHAR) FROM vendors WHERE adapter_config IS NOT NULL", false)
	stOtherVendorAdapterConfigs = ConfigStmts.Add("config.endpoints.other_vendor_adapters",
		"SELECT cid, CAST(adapter_config AS CHAR) FROM vendors WHERE adapter_config IS NOT NULL AND id <> ?", false)
	stEndpointConfigsExcept = ConfigStmts.Add("config.endpoints.configs_except",
		"SELECT id, type, COALESCE(name,''), CAST(config AS CHAR) FROM http_endpoints WHERE id <> ? AND config IS NOT NULL", false)
	stAccountsUsingHLRProvider = ConfigStmts.Add("config.endpoints.hlr_users",
		`SELECT username FROM sms_service_config
			  WHERE hlr_provider_ids IS NOT NULL AND JSON_CONTAINS(hlr_provider_ids, CAST(? AS JSON))`, false)
	stLiveRatesUsingCRP = ConfigStmts.Add("config.endpoints.crp_rates", `SELECT COUNT(*) FROM rate_card_rates
			WHERE valid_to IS NULL AND superseded_by IS NULL AND TRIM(thirdparty_crp_id) IN (?, ?)`, false)
	stCompaniesUsingCRP = ConfigStmts.Add("config.endpoints.crp_companies",
		"SELECT name FROM companies WHERE TRIM(base_thirdparty_crp_id) IN (?, ?)", false)
	stKMSAssignments = ConfigStmts.Add("config.endpoints.kms_assignments",
		"SELECT COUNT(*) FROM tenant_kms_config WHERE kms_provider_id = ?", false)
	stDataKeysLike    = ConfigStmts.Add("config.endpoints.data_keys_like", "SELECT COUNT(*) FROM data_keys WHERE kek_ref LIKE ?", false)
	stEndpointDelete  = ConfigStmts.Add("config.endpoints.delete", "DELETE FROM http_endpoints WHERE id = ?", false)
	stEndpointForPick = ConfigStmts.Add("config.endpoints.for_pick", `
		SELECT type, COALESCE(name,''), vendor_id, is_active,
		       COALESCE(CAST(config AS CHAR),'{}'), COALESCE(CAST(headers AS CHAR),'{}')
		  FROM http_endpoints WHERE id = ?`, false)
	stVendorCID      = ConfigStmts.Add("config.endpoints.vendor_cid", "SELECT cid FROM vendors WHERE id = ?", false)
	stEndpointConfig = ConfigStmts.Add("config.endpoints.config",
		"SELECT COALESCE(CAST(config AS CHAR),'{}') FROM http_endpoints WHERE id = ?", false)
	stClaimCall = ConfigStmts.Add("config.endpoints.claim",
		"UPDATE http_endpoints SET vendor_id = ? WHERE type = 'vendor' AND id = ?", false)
	stClaimFreeCall = ConfigStmts.Add("config.endpoints.claim_free",
		"UPDATE http_endpoints SET vendor_id = ? WHERE type = 'vendor' AND vendor_id IS NULL AND id = ?", false)
	stVendorCompanyID = ConfigStmts.Add("config.endpoints.vendor_company", "SELECT company_id FROM vendors WHERE id = ?", false)
	// The keep list is appended as " AND id NOT IN (…)"; registered with one.
	stReleaseCalls           = ConfigStmts.Add("config.endpoints.release", releaseCallsHead+" AND id NOT IN (1)", false)
	stVendorAdapterTransport = ConfigStmts.Add("config.endpoints.vendor_adapter_transport",
		"SELECT COALESCE(CAST(adapter_config AS CHAR),''), LOWER(COALESCE(transport,'')) FROM vendors WHERE id = ?", false)
)

const (
	endpointListHead = `
		SELECT e.id, e.type, COALESCE(e.name,''), COALESCE(v.cid, a.username, 'shared'),
		       COALESCE(e.url,''), COALESCE(e.method,'GET'), COALESCE(e.is_active,0),
		       COALESCE(e.config,'{}')
		  FROM http_endpoints e
		  LEFT JOIN vendors v ON v.id = e.vendor_id
		  LEFT JOIN sms_service_config a ON a.id = e.account_id
		  `
	endpointListTail = `
		 ORDER BY e.id LIMIT ?`

	releaseCallsHead = "UPDATE http_endpoints SET vendor_id = NULL, company_id = COALESCE(company_id, ?) WHERE type = 'vendor' AND vendor_id = ?"
)

// ---- vendors ----

func (c configQ) VendorCompanyKind(ctx context.Context, companyID int64) (name, typ string, err error) {
	err = c.q.QueryRowContext(ctx, stVendorCompanyKind.SQL, companyID).Scan(&name, &typ)
	return name, typ, err
}

func (c configQ) VendorCIDTaken(ctx context.Context, cid string) (bool, error) {
	return c.exists(ctx, stVendorCIDTaken, cid)
}

func (c configQ) InsertVendor(ctx context.Context, v dal.VendorInsert) (int64, error) {
	res, err := c.q.ExecContext(ctx, stVendorInsert.SQL,
		v.CID, v.Name, v.CompanyID, v.Transport, v.QueueConfig, v.AdapterConfig, nullStr(v.Deployments))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (c configQ) VendorList(ctx context.Context, limit int) ([]dal.VendorListRow, error) {
	rows, err := c.q.QueryContext(ctx, stVendorList.SQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.VendorListRow
	for rows.Next() {
		var r dal.VendorListRow
		if err := rows.Scan(&r.ID, &r.CID, &r.Name, &r.Company, &r.Transport, &r.Status, &r.AdapterJSON); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, dal.RowsError{Err: err}
	}
	return out, nil
}

func (c configQ) EndpointForProfile(ctx context.Context, id int64) (dal.EndpointProfileRow, error) {
	var r dal.EndpointProfileRow
	err := c.q.QueryRowContext(ctx, stEndpointForProfile.SQL, id).
		Scan(&r.ID, &r.Name, &r.URL, &r.Method, &r.Headers, &r.Auth,
			&r.TimeoutMS, &r.CacheTTLSec, &r.Config, &r.BodyTemplate)
	return r, err
}

func (c configQ) SetVendorStatus(ctx context.Context, id int64, status string) (int64, error) {
	return c.vendorsExecRows(ctx, stVendorSetStatus.SQL, status, id)
}

func (c configQ) VendorDetail(ctx context.Context, id int64) (dal.VendorDetailRow, error) {
	var d dal.VendorDetailRow
	err := c.q.QueryRowContext(ctx, stVendorDetail.SQL, id).
		Scan(&d.ID, &d.CID, &d.Name, &d.Company, &d.Transport, &d.Status, &d.CompanyID,
			&d.QueueConfig, &d.AdapterConfig, &d.Inbound, &d.Encodings, &d.Accepted)
	return d, err
}

func (c configQ) VendorRouteCount(ctx context.Context, id int64) (int, error) {
	return c.count(ctx, stVendorRouteCount, id, id)
}

func (c configQ) SetVendorChargePoint(ctx context.Context, id int64, point string) error {
	_, err := c.q.ExecContext(ctx, stVendorSetChargePoint.SQL, point, id)
	return err
}

func (c configQ) SetVendorDLRConfig(ctx context.Context, id int64, path string, value any) error {
	_, err := c.q.ExecContext(ctx, stVendorSetDLRConfig.SQL, path, value, id)
	return err
}

func (c configQ) SetVendorName(ctx context.Context, id int64, name string) error {
	_, err := c.q.ExecContext(ctx, stVendorSetName.SQL, name, id)
	return err
}

func (c configQ) SetVendorCompany(ctx context.Context, id, companyID int64) error {
	_, err := c.q.ExecContext(ctx, stVendorSetCompany.SQL, companyID, id)
	return err
}

func (c configQ) SetVendorAdapterConfig(ctx context.Context, id int64, adapterJSON string) error {
	_, err := c.q.ExecContext(ctx, stVendorSetAdapter.SQL, adapterJSON, id)
	return err
}

func (c configQ) RemoveVendorProtocolPath(ctx context.Context, id int64, path string) error {
	_, err := c.q.ExecContext(ctx, stVendorRemoveProt.SQL, path, id, path)
	return err
}

func (c configQ) MergeVendorEncodings(ctx context.Context, id int64, encJSON string) error {
	_, err := c.q.ExecContext(ctx, stVendorMergeEnc.SQL, encJSON, id)
	return err
}

func (c configQ) SetVendorQueueConfig(ctx context.Context, id int64, path string, value any) error {
	_, err := c.q.ExecContext(ctx, stVendorSetQueueConfig.SQL, path, value, id)
	return err
}

func (c configQ) DeleteVendorRow(ctx context.Context, id int64) error {
	_, err := c.q.ExecContext(ctx, stVendorDelete.SQL, id)
	return err
}

func (c configQ) VendorAdapterConfig(ctx context.Context, id int64) (string, error) {
	var raw string
	err := c.q.QueryRowContext(ctx, stVendorAdapterConfig.SQL, id).Scan(&raw)
	return raw, err
}

// ---- the vendor's queues ----

func (c configQ) VendorQueueColumns(ctx context.Context) (int, error) {
	return c.count(ctx, stVendorQueueColumns)
}

func (c configQ) VendorQueueSettings(ctx context.Context, id int64) (split, shortCodes string, err error) {
	err = c.q.QueryRowContext(ctx, stVendorQueueSettings.SQL, id).Scan(&split, &shortCodes)
	return split, shortCodes, err
}

func (c configQ) SetVendorQueueSplit(ctx context.Context, id int64, split string) error {
	_, err := c.q.ExecContext(ctx, stVendorSetQueueSplit.SQL, split, id)
	return err
}

func (c configQ) SetVendorShortCodes(ctx context.Context, id int64, codes dal.NullString) error {
	var stored any // NULL for none
	if codes.Valid {
		stored = codes.String
	}
	_, err := c.q.ExecContext(ctx, stVendorSetShortCodes.SQL, stored, id)
	return err
}

// ---- HTTP calls ----

func (c configQ) VendorCIDTransport(ctx context.Context, id int64) (cid, transport string, err error) {
	err = c.q.QueryRowContext(ctx, stVendorCIDTransport.SQL, id).Scan(&cid, &transport)
	return cid, transport, err
}

func (c configQ) VendorTransport(ctx context.Context, id int64) (string, error) {
	var t string
	err := c.q.QueryRowContext(ctx, stVendorTransport.SQL, id).Scan(&t)
	return t, err
}

func (c configQ) InsertEndpoint(ctx context.Context, e dal.EndpointInsert) (int64, error) {
	res, err := c.q.ExecContext(ctx, stEndpointInsert.SQL,
		e.Type, e.Name, nullID(e.VendorID), nullID(e.AccountID), nullStr(e.URL), e.Method,
		e.TimeoutMS, e.RateLimit, e.Concurrency, e.Config)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (c configQ) EndpointList(ctx context.Context, kind string, limit int) ([]dal.EndpointListRow, error) {
	where, args := "", []any{}
	if kind != "" {
		where = "WHERE e.type = ?"
		args = append(args, kind)
	}
	args = append(args, limit)
	rows, err := c.q.QueryContext(ctx, endpointListHead+where+endpointListTail, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.EndpointListRow
	for rows.Next() {
		var r dal.EndpointListRow
		if err := rows.Scan(&r.ID, &r.Type, &r.Name, &r.Owner, &r.URL, &r.Method, &r.Active, &r.Config); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, r)
	}
	// A read cut short answers the rows read so far with its error, as the
	// list always has.
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

func (c configQ) EndpointKind(ctx context.Context, id int64) (typ, name, configJSON string, err error) {
	err = c.q.QueryRowContext(ctx, stEndpointKind.SQL, id).Scan(&typ, &name, &configJSON)
	return typ, name, configJSON, err
}

// vendorsAdapters reads cid + adapter_config rows; a row that cannot be
// read is left out, and the end of the read is not checked (as before).
func (c configQ) vendorsAdapters(ctx context.Context, q string, args ...any) ([]dal.VendorAdapterRow, error) {
	rows, err := c.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.VendorAdapterRow
	for rows.Next() {
		var r dal.VendorAdapterRow
		if rows.Scan(&r.CID, &r.AdapterJSON) != nil {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

func (c configQ) VendorAdapterConfigs(ctx context.Context) ([]dal.VendorAdapterRow, error) {
	return c.vendorsAdapters(ctx, stVendorAdapterConfigs.SQL)
}

func (c configQ) OtherVendorAdapterConfigs(ctx context.Context, vendorID int64) ([]dal.VendorAdapterRow, error) {
	return c.vendorsAdapters(ctx, stOtherVendorAdapterConfigs.SQL, vendorID)
}

func (c configQ) EndpointConfigsExcept(ctx context.Context, id int64) ([]dal.EndpointConfigRow, error) {
	rows, err := c.q.QueryContext(ctx, stEndpointConfigsExcept.SQL, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.EndpointConfigRow
	for rows.Next() {
		var r dal.EndpointConfigRow
		if rows.Scan(&r.ID, &r.Type, &r.Name, &r.Config) != nil {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

func (c configQ) WebhookUsers(ctx context.Context, endpointID int64) ([]dal.WebhookUser, error) {
	return c.webhookUsers(ctx, stWebhookUsers.SQL, endpointID, endpointID)
}

func (c configQ) AllWebhookUsers(ctx context.Context) ([]dal.WebhookUser, error) {
	return c.webhookUsers(ctx, stAllWebhookUsers.SQL)
}

// webhookUsers reads WebhookUser rows; a row that cannot be read is left out.
func (c configQ) webhookUsers(ctx context.Context, q string, args ...any) ([]dal.WebhookUser, error) {
	rows, err := c.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.WebhookUser
	for rows.Next() {
		var u dal.WebhookUser
		if rows.Scan(&u.EndpointID, &u.AccountID, &u.Username, &u.Kind) == nil {
			out = append(out, u)
		}
	}
	return out, rows.Err()
}

// vendorsNames reads one text column; a row that cannot be read is left out.
func (c configQ) vendorsNames(ctx context.Context, q string, args ...any) ([]string, error) {
	rows, err := c.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if rows.Scan(&s) == nil {
			out = append(out, s)
		}
	}
	return out, nil
}

func (c configQ) AccountsUsingHLRProvider(ctx context.Context, idJSON string) ([]string, error) {
	return c.vendorsNames(ctx, stAccountsUsingHLRProvider.SQL, idJSON)
}

func (c configQ) LiveRatesUsingCRP(ctx context.Context, ref1, ref2 string) (int, error) {
	return c.count(ctx, stLiveRatesUsingCRP, ref1, ref2)
}

func (c configQ) CompaniesUsingCRP(ctx context.Context, ref1, ref2 string) ([]string, error) {
	return c.vendorsNames(ctx, stCompaniesUsingCRP.SQL, ref1, ref2)
}

func (c configQ) KMSAssignmentsOf(ctx context.Context, providerID int64) (int, error) {
	return c.count(ctx, stKMSAssignments, providerID)
}

func (c configQ) DataKeysLike(ctx context.Context, pattern string) (int, error) {
	return c.count(ctx, stDataKeysLike, pattern)
}

func (c configQ) DeleteEndpointRow(ctx context.Context, id int64) (int64, error) {
	return c.vendorsExecRows(ctx, stEndpointDelete.SQL, id)
}

func (c configQ) EndpointForPick(ctx context.Context, id int64) (dal.EndpointPickRow, error) {
	var r dal.EndpointPickRow
	err := c.q.QueryRowContext(ctx, stEndpointForPick.SQL, id).
		Scan(&r.Type, &r.Name, &r.Owner, &r.Active, &r.Config, &r.Headers)
	return r, err
}

func (c configQ) VendorCID(ctx context.Context, id int64) (string, error) {
	var cid string
	err := c.q.QueryRowContext(ctx, stVendorCID.SQL, id).Scan(&cid)
	return cid, err
}

func (c configQ) EndpointConfig(ctx context.Context, id int64) (string, error) {
	var cfg string
	err := c.q.QueryRowContext(ctx, stEndpointConfig.SQL, id).Scan(&cfg)
	return cfg, err
}

func (c configQ) ClaimCall(ctx context.Context, id, vendorID int64) error {
	_, err := c.q.ExecContext(ctx, stClaimCall.SQL, vendorID, id)
	return err
}

func (c configQ) ClaimFreeCall(ctx context.Context, id, vendorID int64) error {
	_, err := c.q.ExecContext(ctx, stClaimFreeCall.SQL, vendorID, id)
	return err
}

func (c configQ) VendorCompanyID(ctx context.Context, vendorID int64) (dal.NullInt64, error) {
	var company dal.NullInt64
	err := c.q.QueryRowContext(ctx, stVendorCompanyID.SQL, vendorID).Scan(&company)
	return company, err
}

func (c configQ) ReleaseCalls(ctx context.Context, vendorID int64, company dal.NullInt64, keep []string) error {
	_ = stReleaseCalls
	q := releaseCallsHead
	if len(keep) > 0 {
		q += " AND id NOT IN (" + strings.Join(keep, ",") + ")"
	}
	var co any
	if company.Valid {
		co = company.Int64
	}
	_, err := c.q.ExecContext(ctx, q, co, vendorID)
	return err
}

func (c configQ) VendorAdapterTransport(ctx context.Context, id int64) (adapterJSON, transport string, err error) {
	err = c.q.QueryRowContext(ctx, stVendorAdapterTransport.SQL, id).Scan(&adapterJSON, &transport)
	return adapterJSON, transport, err
}

// vendorsExecRows runs a statement and answers the rows it changed (0 when the
// driver cannot say, as the callers have always read it).
func (c configQ) vendorsExecRows(ctx context.Context, q string, args ...any) (int64, error) {
	res, err := c.q.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
