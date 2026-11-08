package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// The reference cache's reads (dal.ReferenceData, dal.HLRNumbers) on SQL
// Server, made like internal/dal/mysql/reference.go: the same reads, the same
// choice of pool (the read instance for the periodic loads, the write
// instance for a while after a forced refresh).

// ---- the config store -----------------------------------------------------

// ReferenceData reads the config store's rows the cache is built from.
type ReferenceData struct {
	db         *sql.DB
	crd        *Read
	freshUntil atomic.Int64
}

// OpenReferenceData opens the reads on the process's one pool for dsn, and
// its read instance when one is set.
func OpenReferenceData(dsn string) (*ReferenceData, error) {
	db, err := Get(dsn)
	if err != nil {
		return nil, err
	}
	r := &ReferenceData{db: db}
	r.crd, _ = Reader(dsn)
	return r, nil
}

func openReferenceData(dsn string) (dal.ReferenceData, error) {
	r, err := OpenReferenceData(dsn)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// NewReferenceData reads on db alone (a test's own handle).
func NewReferenceData(db *sql.DB) *ReferenceData { return &ReferenceData{db: db} }

// DB is the write instance's pool (a test's own rows).
func (r *ReferenceData) DB() *sql.DB { return r.db }

// freshFor is how long the config store is read on the write instance after
// a forced refresh.
const freshFor = time.Minute

// cfgRead is the pool a reference-data read uses: the read instance when one
// is set, but the write instance for a context marked dbpool.Fresh and for
// freshFor after it (mysql/reference.go says why).
func (r *ReferenceData) cfgRead(ctx context.Context) *sql.DB {
	if r.crd == nil || !r.crd.Separate() {
		return r.db
	}
	now := time.Now().UnixNano()
	if dbpool.IsFresh(ctx) {
		r.freshUntil.Store(now + int64(freshFor))
		return r.db
	}
	if now < r.freshUntil.Load() {
		return r.db
	}
	return r.crd.DB()
}

// ReadsFromWrite says a read with ctx goes to the write instance now.
func (r *ReferenceData) ReadsFromWrite(ctx context.Context) bool { return r.cfgRead(ctx) == r.db }

// EndFreshHold ends the hold after a forced refresh now (a test's).
func (r *ReferenceData) EndFreshHold() { r.freshUntil.Store(0) }

// HasReadInstance says a read instance is set beside the write instance.
func (r *ReferenceData) HasReadInstance() bool { return r.crd != nil && r.crd.Separate() }

// MissingTable is SQL Server's "Invalid object name" (208).
func (r *ReferenceData) MissingTable(err error) bool { return MissingTable(err) }

var (
	stRefAccounts = addConfig("reference.accounts", `
		SELECT `+wholeTable+` a.id, a.user_id, a.username,
		       a.jwt, a.http, a.smpp, a.allowed_sender_ids, a.allowed_ip_list,
		       a.is_disabled, a.enforce_account_expiry,
		       a.enforce_spam_check, COALESCE(a.enforce_spam_strict,0), a.enforce_template_check, a.enforce_auto_replace,
		       a.enforce_duplicate_check, a.enforce_hlr_db_check, COALESCE(a.enforce_hlr_dip_url,''),
		       COALESCE(a.hlr_miss_policy,'proceed'),
		       CASE WHEN c.id IS NOT NULL THEN c.body_privacy ELSE a.body_privacy END,
		       COALESCE(a.dlr_channel,'both'), COALESCE(a.dlr_bind_attempts,3),
		       COALESCE(a.mo_channel,'both'), COALESCE(a.mo_bind_attempts,3),
		       COALESCE(a.dlr_max_attempts,0), COALESCE(a.mo_max_attempts,0),
		       CASE WHEN COALESCE(a.dlr_levels,'') = 'none' THEN 1 ELSE 0 END,
		       a.max_recipients, a.quiet_range, a.dedupe_ttl_sec, a.default_ttl_sec,
		       a.whitelist_numbers, a.smpp_config,
		       COALESCE(a.company_id,0), COALESCE(c.is_disabled,0),
		       `+prepaidOf("c.billing_config")+`,
		       COALESCE(c.balance,0), COALESCE(c.max_throughput,0),
		       c.quota, COALESCE(c.timezone,'UTC'),
		       COALESCE(RTRIM(c.currency),'USD'), COALESCE(c.base_conversion_rate,0), COALESCE(c.base_thirdparty_crp_id,''),
		       COALESCE(a.loss_detection_enabled,0),
		       COALESCE(a.default_mt_rate_card_id,0), COALESCE(a.charge_point,'acceptance'),
		       COALESCE(a.default_dlr_rate_card_id,0), COALESCE(a.default_hlr_rate_card_id,0),
		       CASE WHEN COALESCE(a.charge_failed_hlr,0) = 1 THEN 1 ELSE 0 END,
		       COALESCE(a.password,''), COALESCE(d.url, '/sms/v4/send'),
		       d.request_map, d.response_map, d.keep_unmapped,
		       d.auth_config, COALESCE(d.request_wrapper,''), COALESCE(d.request_format,''), COALESCE(RTRIM(a.token_hash),''),
		       COALESCE(a.store_auth_data,0),
		       COALESCE(a.access_scope,'both'),
		       a.dest_cap_hourly, a.dest_cap_daily,
		       COALESCE(a.octates_to_septets_unpacking,0),
		       COALESCE(a.abuse_throttle_tps,0),
		       a.hlr_order, a.hlr_cache_ttl_days, a.hlr_provider_ids,
		       COALESCE(a.deployments,''), COALESCE(a.default_sender,''),
		       c.country_quota
		FROM sms_service_config a
		LEFT JOIN companies c ON c.id = a.company_id
		LEFT JOIN sms_service_dialects d ON d.id = a.dialect_id`)
	stRefApprovedSenders = addConfig("reference.approved_senders",
		`SELECT account_id, sender FROM sender_ids
		 WHERE status = 'approved' AND account_id IS NOT NULL`)
	stRefHLRDefaults = addConfig("reference.hlr_defaults",
		`SELECT name, value FROM gateway_settings
		  WHERE name IN ('hlr_default_order','hlr_default_cache_ttl_days')`)
	stRefHasShortCodes = addConfig("reference.has_vendor_short_codes",
		`SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID(N'vendors') AND name = 'short_codes'`)
	stRefRouteVendors = addConfig("reference.route_vendors", fmt.Sprintf(refRouteVendorsSQL, refShortCodesCol, refEncodings))
	stRefRoutes       = addConfig("reference.routes", `
		SELECT `+wholeTable+` r.id, COALESCE(r.name,''), r.priority, r.route_type, COALESCE(r.weight,100), r.direction, COALESCE(r.account_id,0),
		       r.match_mccmncs, r.accept_undetected_network,
		       r.sender_regex, r.receiver_regex, r.keyword_regex, r.priority_regex,
		       COALESCE(r.vendor_id,0), COALESCE(r.buying_rate_card_id,0),
		       COALESCE(r.selling_rate_card_id,0), r.is_disabled,
		       r.send_plan, r.retry_rounds, r.retry_first_sec, r.retry_backoff, r.retry_max_wait_sec
		FROM routes r
		ORDER BY r.priority`)
	stRefRates = addConfig("reference.rates", `
		SELECT rr.rate_card_id, rc.kind, RTRIM(rc.currency), COALESCE(rr.match_mcc,''), COALESCE(rr.match_mnc,''),
		       COALESCE(rr.destination_country_code,''), rr.price_per_segment,
		       COALESCE(rr.conversion_rate,1), COALESCE(rr.thirdparty_crp_id,''),
		       rr.id, COALESCE(rr.tax_percentage,0), rr.effective_from,
		       COALESCE(RTRIM(co.currency),''), COALESCE(co.base_conversion_rate,0), COALESCE(co.base_thirdparty_crp_id,'')
		FROM rate_card_rates rr JOIN rate_cards rc ON rc.id = rr.rate_card_id
		LEFT JOIN companies co ON co.id = rc.company_id
		WHERE rc.is_disabled=0
		  AND rr.valid_to IS NULL`)
	stRefAutoReplace = addConfig("reference.auto_replace", `
		SELECT account_id, replace_regex, seq
		FROM auto_replace_text WHERE is_disabled = 0 ORDER BY account_id, seq`)
	stRefSpam = addConfig("reference.spam", `
		SELECT `+wholeTable+` company_id, list_type, COALESCE(category,''), COALESCE(action,'reject'),
		       COALESCE(destination_country_code,''), keywords,
		       COALESCE(keyword_regex,'')
		FROM spam_keywords
		ORDER BY id`)
	stRefTemplates = addConfig("reference.templates",
		`SELECT id, COALESCE(account_id,0), COALESCE(company_id,0), regex,
		        default_priority, shortcodes,
		        COALESCE(ap1,''), COALESCE(ap2,''), COALESCE(apg1,''), COALESCE(apg2,'')
		 FROM templates WHERE status = 'approved' AND is_disabled = 0
		 ORDER BY id`)
	stRefBlocks = addConfig("reference.blocks",
		`SELECT msisdn, COALESCE(account_id,0), COALESCE(company_id,0), COALESCE(sender,''), expires_at,
		        COALESCE(source,'manual')
		 FROM blocklist WHERE is_disabled = 0`)
	stRefCountryRules = addConfig("reference.country_rules",
		`SELECT COALESCE(account_id,0), destination_country_code, mode, quota,
		        COALESCE(rate_limit,0), COALESCE(priority,0)
		 FROM quota_by_countries WHERE is_disabled = 0`)
	// The store's clock and a table's newest change, both as MySQL writes a
	// DATETIME ('YYYY-MM-DD HH:MM:SS', UTC): refdata compares the two texts.
	// The clock is rounded to the second as a DATETIME2(0) column rounds the
	// SYSUTCDATETIME() it is given, so a row written this moment is never
	// read as ahead of the clock.
	stRefNow       = addConfig("reference.now", "SELECT TOP (1) CONVERT(VARCHAR(19), CAST(SYSUTCDATETIME() AS DATETIME2(0)), 120)")
	stRefTableMark = addConfig("reference.table_mark", "SELECT COUNT(*), CONVERT(VARCHAR(19), MAX(updated_at), 120), NULL FROM [<table>]")
)

const (
	refRouteVendorsSQL = `
		SELECT ` + wholeTable + ` v.id, v.cid, v.service_status, v.transport,
		       COALESCE(%[2]s,'null'),
		       COALESCE(JSON_VALUE(v.protocol_config,'$.multipart_mode'),''),
		       COALESCE(v.encodings,'null'), COALESCE(v.default_mt_buying_rate_card_id,0),
		       COALESCE(v.deployments,''), %[1]s
		FROM vendors v`
	// The short codes (config/0161): a store without the column has none.
	refShortCodesCol   = "COALESCE(v.short_codes,'')"
	refNoShortCodesCol = "''"
)

// refEncodings is JSON_EXTRACT(v.protocol_config,'$.encodings').
var refEncodings = jsonTextAt("v.protocol_config", "encodings")

// refSignatureExtra adds a checksum of the columns a same-second change can
// touch without moving MAX(updated_at) (mysql.SignatureExtra says why): the
// first four bytes of an MD5 of each row's columns, XORed over the table
// (CHECKSUM_AGG). CONCAT_WS skips a NULL, as on MySQL.
var refSignatureExtra = map[string]string{
	"sms_service_config": "CHECKSUM_AGG(CAST(CAST(HASHBYTES('MD5', CONCAT_WS(N'#', id, jwt, password, smpp_config, is_disabled, " +
		"COALESCE(abuse_throttle_tps,0))) AS BINARY(4)) AS INT))",
}

func (r *ReferenceData) Accounts(ctx context.Context) ([]dal.AccountRef, error) {
	rows, err := r.cfgRead(ctx).QueryContext(ctx, stRefAccounts.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.AccountRef
	for rows.Next() {
		var a dal.AccountRef
		if err := rows.Scan(&a.ID, &a.UserID, &a.Username,
			&a.JWT, &a.HTTP, &a.SMPP, &a.AllowedSenders, &a.AllowedIPs,
			&a.Disabled, &a.Expiry,
			&a.Spam, &a.SpamStrict, &a.Template, &a.AutoRepl,
			&a.Duplicate, &a.HLRDB, &a.HLRDipURL,
			&a.HLRMissPolicy,
			&a.BodyPrivacy,
			&a.DLRChannel, &a.DLRBindAttempts,
			&a.MOChannel, &a.MOBindAttempts,
			&a.DLRMaxAttempts, &a.MOMaxAttempts,
			&a.NoReceipts,
			&a.MaxRecipients, &a.Quiet, &a.DedupeTTL, &a.DefaultTTL,
			&a.Whitelist, &a.SMPPConfig,
			&a.CompanyID, &a.CompanyDisabled, &a.CompanyPrepaid,
			&a.CompanyBalance, &a.CompanyThroughput,
			&a.CompanyQuota, &a.CompanyTimezone,
			&a.CompanyCurrency, &a.CompanyBaseRate, &a.CompanyBaseCRP, &a.LossDetection, &a.DefaultMTSellCard, &a.ChargePoint,
			&a.DefaultDLRSellCard, &a.DefaultHLRSellCard,
			&a.ChargeFailedHLR,
			&a.APIPassword, &a.DialectURL, &a.RequestMap, &a.ResponseMap, &a.KeepUnmapped,
			&a.AuthConfig, &a.RequestWrapper, &a.RequestFormat, &a.TokenHash, &a.StoreAuth,
			&a.AccessScope,
			&a.CapHourly, &a.CapDaily,
			&a.PackedGSM7, &a.AbuseThrottleTPS,
			&a.HLROrder, &a.HLRCacheTTL, &a.HLRProviders, &a.Deployments, &a.DefaultSender,
			&a.CompanyCountryQuota); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

func (r *ReferenceData) ApprovedSenders(ctx context.Context) ([]dal.ApprovedSender, error) {
	rows, err := r.cfgRead(ctx).QueryContext(ctx, stRefApprovedSenders.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.ApprovedSender
	for rows.Next() {
		var s dal.ApprovedSender
		if err := rows.Scan(&s.AccountID, &s.Sender); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

func (r *ReferenceData) HLRDefaultSettings(ctx context.Context) ([]dal.Setting, error) {
	rows, err := r.db.QueryContext(ctx, stRefHLRDefaults.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.Setting
	for rows.Next() {
		var s dal.Setting
		if rows.Scan(&s.Name, &s.Value) != nil {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

func (r *ReferenceData) HasVendorShortCodes(ctx context.Context) bool {
	var n int
	err := r.cfgRead(ctx).QueryRowContext(ctx, stRefHasShortCodes.SQL).Scan(&n)
	return err == nil && n > 0
}

func (r *ReferenceData) RouteVendors(ctx context.Context, withShortCodes bool) ([]dal.RouteVendorRef, error) {
	q := stRefRouteVendors.SQL
	if !withShortCodes {
		q = fmt.Sprintf(refRouteVendorsSQL, refNoShortCodesCol, refEncodings)
	}
	rows, err := r.cfgRead(ctx).QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.RouteVendorRef
	for rows.Next() {
		var v dal.RouteVendorRef
		if err := rows.Scan(&v.ID, &v.CID, &v.Status, &v.Transport, &v.WireJSON, &v.MultipartMode, &v.Accepted, &v.DefaultBuy, &v.Deployments, &v.ShortCodes); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

func (r *ReferenceData) Routes(ctx context.Context) ([]dal.RouteRef, error) {
	rows, err := r.cfgRead(ctx).QueryContext(ctx, stRefRoutes.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.RouteRef
	for rows.Next() {
		var rt dal.RouteRef
		if err := rows.Scan(&rt.ID, &rt.Name, &rt.Priority, &rt.RouteType, &rt.Weight, &rt.Direction, &rt.AccountID,
			&rt.MCCMNCs, &rt.AcceptUndetected,
			&rt.Sender, &rt.Receiver, &rt.Keyword, &rt.PriRule,
			&rt.VendorID, &rt.BuyCard,
			&rt.SellCard, &rt.Disabled,
			&rt.Plan, &rt.Rounds, &rt.First, &rt.Backoff, &rt.MaxWait); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, rt)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

func (r *ReferenceData) Rates(ctx context.Context) ([]dal.RateRef, error) {
	rows, err := r.cfgRead(ctx).QueryContext(ctx, stRefRates.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.RateRef
	for rows.Next() {
		var x dal.RateRef
		if err := rows.Scan(&x.CardID, &x.Kind, &x.Currency, &x.MCC, &x.MNC, &x.Country, &x.Price,
			&x.ConvRate, &x.CRPRef, &x.ID, &x.TaxPct, &x.EffectiveFrom, &x.ConvTo, &x.OwnerBaseRate, &x.OwnerBaseCRP); err != nil {
			return out, dal.RowsError{Err: err}
		}
		out = append(out, x)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

func (r *ReferenceData) AutoReplace(ctx context.Context) ([]dal.AutoReplaceRef, error) {
	rows, err := r.cfgRead(ctx).QueryContext(ctx, stRefAutoReplace.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.AutoReplaceRef
	for rows.Next() {
		var x dal.AutoReplaceRef
		if err := rows.Scan(&x.AccountID, &x.Regex, &x.Seq); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, x)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

func (r *ReferenceData) Spam(ctx context.Context) ([]dal.SpamRef, error) {
	rows, err := r.cfgRead(ctx).QueryContext(ctx, stRefSpam.SQL) // the order rules are tried in
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.SpamRef
	for rows.Next() {
		var x dal.SpamRef
		if err := rows.Scan(&x.CompanyID, &x.ListType, &x.Category, &x.Action, &x.Country, &x.Keywords, &x.KeywordRegex); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, x)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

func (r *ReferenceData) Templates(ctx context.Context) ([]dal.TemplateRef, error) {
	rows, err := r.cfgRead(ctx).QueryContext(ctx, stRefTemplates.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.TemplateRef
	for rows.Next() {
		var x dal.TemplateRef
		if err := rows.Scan(&x.ID, &x.AccountID, &x.CompanyID, &x.Regex, &x.DefaultPriority, &x.Shortcodes, &x.AP1, &x.AP2, &x.APG1, &x.APG2); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, x)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

func (r *ReferenceData) Blocks(ctx context.Context) ([]dal.BlockRef, error) {
	rows, err := r.cfgRead(ctx).QueryContext(ctx, stRefBlocks.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.BlockRef
	for rows.Next() {
		var x dal.BlockRef
		if err := rows.Scan(&x.MSISDN, &x.AccountID, &x.CompanyID, &x.Sender, &x.ExpiresAt, &x.Source); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, x)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

func (r *ReferenceData) CountryRules(ctx context.Context) ([]dal.CountryRuleRef, error) {
	rows, err := r.cfgRead(ctx).QueryContext(ctx, stRefCountryRules.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.CountryRuleRef
	for rows.Next() {
		var x dal.CountryRuleRef
		if err := rows.Scan(&x.AccountID, &x.Country, &x.Mode, &x.Quota, &x.RateLimit, &x.Priority); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, x)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

func (r *ReferenceData) Now(ctx context.Context) (dal.NullString, error) {
	var now dal.NullString
	err := r.cfgRead(ctx).QueryRowContext(ctx, stRefNow.SQL).Scan(&now)
	return now, err
}

func (r *ReferenceData) TableMark(ctx context.Context, table string) (int64, dal.NullString, dal.NullString, error) {
	_ = stRefTableMark
	var n int64
	var max, extra dal.NullString
	q := "SELECT COUNT(*), CONVERT(VARCHAR(19), MAX(updated_at), 120), "
	if e, ok := refSignatureExtra[table]; ok {
		q += e
	} else {
		q += "NULL"
	}
	err := r.cfgRead(ctx).QueryRowContext(ctx, q+" FROM "+quoteIdent(table)).Scan(&n, &max, &extra)
	return n, max, extra, err
}

var _ dal.ReferenceData = (*ReferenceData)(nil)

// ---- the HLR store --------------------------------------------------------

// HLRNumbers is the HLR store as the reference cache reads it.
type HLRNumbers struct {
	qdb *sql.DB
	hrd *Read // the read instance, when one is set
}

// OpenHLRNumbers opens the HLR store on the process's one pool for dsn, and
// its read instance when one is set.
func OpenHLRNumbers(dsn string) (*HLRNumbers, error) {
	qdb, err := Get(dsn)
	if err != nil {
		return nil, err
	}
	h := &HLRNumbers{qdb: qdb}
	h.hrd, _ = Reader(dsn)
	return h, nil
}

func openHLRNumbers(dsn string) (dal.HLRNumbers, error) {
	h, err := OpenHLRNumbers(dsn)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// NewHLRNumbers reads on db alone (a test's own handle).
func NewHLRNumbers(db *sql.DB) *HLRNumbers { return &HLRNumbers{qdb: db} }

// hlrRead is the pool an HLR lookup, the HLR cache's loads and the JWT
// revocation list are read from: the read instance when one is set.
func (h *HLRNumbers) hlrRead() *sql.DB {
	if h.hrd != nil {
		if d := h.hrd.DB(); d != nil {
			return d
		}
	}
	return h.qdb
}

// A shard's ported and is_deleted are TINYINT in shards made from
// hlr_template and BIT in the memory-optimized ones (HLRShards.EnsureShard):
// both are read as INT. The number is given as VARCHAR, which an NVARCHAR
// and a VARCHAR key both seek on.
var (
	stHLRLookup = ConfigStmts.Add("reference.hlr_lookup",
		"SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), CAST(ported AS INT), last_check FROM [hlr_<first4>] WHERE msisdn = @p1 AND is_deleted = 0", true)
	stHLRShards = addConfig("reference.hlr_shards", `
		SELECT name FROM sys.tables
		WHERE name LIKE 'hlr[_]%' AND name <> 'hlr_template' AND schema_id = SCHEMA_ID()`)
	stHLRLastCheckCol = addConfig("reference.hlr_last_check_column", `
		SELECT t.name, c.is_nullable FROM sys.columns c JOIN sys.types t ON t.user_type_id = c.user_type_id
		 WHERE c.object_id = OBJECT_ID(?) AND c.name = 'last_check'`)
	stHLRHasDeleted = addConfig("reference.hlr_has_deleted", `
		SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID(?) AND name = 'is_deleted'`)
	// An index led by last_check, whatever its name (a shard made from the
	// template has idx_last_check; a memory-optimized one idx_hlr_last_check).
	stHLRHasIndex = addConfig("reference.hlr_has_index", `
		SELECT COUNT(*) FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		 WHERE ic.object_id = OBJECT_ID(?) AND ic.key_ordinal = 1 AND c.name = 'last_check'`)
	stHLRIsMemOpt = addConfig("reference.hlr_memory_optimized",
		`SELECT COUNT(*) FROM sys.tables WHERE object_id = OBJECT_ID(?) AND is_memory_optimized = 1`)
	stHLRCount     = addConfig("reference.hlr_count", "SELECT COUNT(*) FROM [hlr_<first4>]")
	stHLRLoadAll   = addConfig("reference.hlr_load_all", "SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), CAST(ported AS INT), CAST(is_deleted AS INT), last_check FROM [hlr_<first4>] WHERE is_deleted = 0")
	stHLRLoadSince = addConfig("reference.hlr_load_since", "SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), CAST(ported AS INT), CAST(is_deleted AS INT), last_check FROM [hlr_<first4>]"+hlrSinceWhere)
	stRevocations  = addConfig("reference.revocations",
		`SELECT jti FROM jwt_revocations WHERE expires_at > SYSUTCDATETIME()`)
	stQueueTables = addConfig("reference.queue_tables",
		`SELECT name FROM sys.tables WHERE name LIKE 'queue[_]%' AND schema_id = SCHEMA_ID()`)
	stQueueDepth = addConfig("reference.queue_depth", "SELECT COUNT(*) FROM [queue_<cid>] WHERE status='queued'")
	stQueueAge   = addConfig("reference.queue_age",
		"SELECT DATEDIFF_BIG(SECOND, MIN(created_at), SYSUTCDATETIME()) FROM (SELECT TOP (1000) created_at FROM [queue_<cid>]"+
			" WHERE status='queued' ORDER BY id) f")
)

// hlrSameSecondFor is how long, in seconds of the store's clock, the rows of
// the newest second seen are read again: longer than a row's write takes to
// commit (mysql/reference.go says why).
const hlrSameSecondFor = 10

// hlrSinceWhere is a delta load's filter: rows after the mark, and the
// mark's own second while it is recent. @p1 is the mark.
var hlrSinceWhere = " WHERE last_check > @p1 OR (last_check = @p1 AND @p1 >= DATEADD(SECOND, -" +
	strconv.Itoa(hlrSameSecondFor) + ", SYSUTCDATETIME()))"

// Lookup resolves one MSISDN from its first-4-digit shard (hlr_<first4>).
// Soft-deleted rows (is_deleted=1) are misses.
func (h *HLRNumbers) Lookup(shard, msisdn string) (dal.HLRRow, bool) {
	_ = stHLRLookup
	var e dal.HLRRow
	var ported int
	var lc sql.NullTime
	err := h.hlrRead().QueryRow(
		"SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), CAST(ported AS INT), last_check FROM "+quoteIdent("hlr_"+shard)+" WHERE msisdn = @p1 AND is_deleted = 0",
		Varchar(msisdn)).
		Scan(&e.MSISDN, &e.MCC, &e.MNC, &e.OperatorName, &e.CCC, &ported, &lc)
	if err != nil {
		return dal.HLRRow{}, false
	}
	e.Ported = ported != 0
	if lc.Valid {
		e.CheckedAt = lc.Time
	}
	return e, true
}

// shards lists the live hlr_<first4> tables (template excluded).
func (h *HLRNumbers) shards(ctx context.Context) ([]string, error) {
	rows, err := h.hlrRead().QueryContext(ctx, stHLRShards.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Prepare brings each live shard to the cache schema, as on MySQL: a
// last_check DATETIME2 that is NOT NULL with a default, an is_deleted flag,
// and an index led by last_check. Idempotent: it reads the catalog before
// altering, so a restart costs a few catalog reads a shard. SQL Server has
// no ON UPDATE: every writer of a shard sets last_check itself (PutShard
// does). A memory-optimized shard takes no index here (HLRShards.Prepare
// says why).
func (h *HLRNumbers) Prepare(ctx context.Context) error {
	shards, err := h.shards(ctx)
	if err != nil {
		return err
	}
	for _, t := range shards {
		qt := quoteIdent(t)
		var dtype string
		var nullable bool
		err := h.qdb.QueryRowContext(ctx, stHLRLastCheckCol.SQL, qt).Scan(&dtype, &nullable)
		switch {
		case err == sql.ErrNoRows:
			if _, err := h.qdb.ExecContext(ctx, "ALTER TABLE "+qt+" ADD last_check DATETIME2(0) NOT NULL"+
				" CONSTRAINT "+quoteIdent("DF_"+t+"_last_check")+" DEFAULT (SYSUTCDATETIME())"); err != nil {
				return fmt.Errorf("hlr prepare %s: %w", t, err)
			}
		case err != nil:
			return fmt.Errorf("hlr prepare %s: %w", t, err)
		case dtype != "datetime2" || nullable:
			// Convert in three steps so the rows with none keep a time.
			for _, q := range []string{
				"ALTER TABLE " + qt + " ALTER COLUMN last_check DATETIME2(0) NULL",
				"UPDATE " + qt + " SET last_check = SYSUTCDATETIME() WHERE last_check IS NULL",
				"ALTER TABLE " + qt + " ALTER COLUMN last_check DATETIME2(0) NOT NULL",
			} {
				if _, err := h.qdb.ExecContext(ctx, q); err != nil {
					return fmt.Errorf("hlr prepare %s: %w", t, err)
				}
			}
		}
		var hasDeleted int
		if err := h.qdb.QueryRowContext(ctx, stHLRHasDeleted.SQL, qt).Scan(&hasDeleted); err != nil {
			return fmt.Errorf("hlr prepare %s: %w", t, err)
		}
		if hasDeleted == 0 {
			if _, err := h.qdb.ExecContext(ctx, "ALTER TABLE "+qt+" ADD is_deleted TINYINT NOT NULL"+
				" CONSTRAINT "+quoteIdent("DF_"+t+"_is_deleted")+" DEFAULT (0)"); err != nil {
				return fmt.Errorf("hlr prepare %s: %w", t, err)
			}
		}
		var memOpt, hasIdx int
		if err := h.qdb.QueryRowContext(ctx, stHLRIsMemOpt.SQL, qt).Scan(&memOpt); err != nil {
			return fmt.Errorf("hlr prepare %s: %w", t, err)
		}
		if memOpt > 0 {
			continue
		}
		if err := h.qdb.QueryRowContext(ctx, stHLRHasIndex.SQL, qt).Scan(&hasIdx); err != nil {
			return fmt.Errorf("hlr prepare %s: %w", t, err)
		}
		if hasIdx == 0 {
			if _, err := h.qdb.ExecContext(ctx, "CREATE INDEX idx_last_check ON "+qt+" (last_check)"); err != nil {
				return fmt.Errorf("hlr prepare %s: %w", t, err)
			}
		}
	}
	return nil
}

// Count totals HLR rows across all shards (memory precheck input). One
// statement a shard table, as on MySQL.
func (h *HLRNumbers) Count(ctx context.Context) (int64, error) {
	_ = stHLRCount
	shards, err := h.shards(ctx)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, t := range shards {
		var n int64
		if err := h.hlrRead().QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteIdent(t)).Scan(&n); err != nil {
			return 0, fmt.Errorf("hlr count %s: %w", t, err)
		}
		total += n
	}
	return total, nil
}

// LoadSince returns entries changed at/after `since` across all shards plus
// the max last_check seen. Zero since = initial full load (soft-deleted rows
// are skipped; delta loads include them so the cache can evict). The rows
// AT `since` are read again only while that second is recent (hlrSinceWhere).
func (h *HLRNumbers) LoadSince(ctx context.Context, since time.Time) ([]dal.HLRRow, time.Time, error) {
	_, _ = stHLRLoadAll, stHLRLoadSince
	shards, err := h.shards(ctx)
	if err != nil {
		return nil, since, err
	}
	maxTS := since
	var out []dal.HLRRow
	for _, t := range shards {
		q := "SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), CAST(ported AS INT), CAST(is_deleted AS INT), last_check FROM " + quoteIdent(t)
		var rows *sql.Rows
		if since.IsZero() {
			rows, err = h.hlrRead().QueryContext(ctx, q+" WHERE is_deleted = 0")
		} else {
			rows, err = h.hlrRead().QueryContext(ctx, q+hlrSinceWhere, Time(since))
		}
		if err != nil {
			return nil, maxTS, fmt.Errorf("hlr load %s: %w", t, err)
		}
		for rows.Next() {
			var e dal.HLRRow
			var ported, deleted int
			var lc sql.NullTime
			if err := rows.Scan(&e.MSISDN, &e.MCC, &e.MNC, &e.OperatorName, &e.CCC, &ported, &deleted, &lc); err != nil {
				rows.Close()
				return nil, maxTS, fmt.Errorf("hlr load %s: %w", t, err)
			}
			e.Ported, e.Deleted = ported != 0, deleted != 0
			if lc.Valid {
				e.CheckedAt = lc.Time
			}
			if lc.Valid && lc.Time.After(maxTS) {
				maxTS = lc.Time
			}
			out = append(out, e)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, maxTS, fmt.Errorf("hlr load %s: %w", t, err)
		}
		rows.Close()
	}
	return out, maxTS, nil
}

// Revocations reads the jti revocation blocklist.
func (h *HLRNumbers) Revocations(ctx context.Context) ([]string, error) {
	rows, err := h.hlrRead().QueryContext(ctx, stRevocations.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var jti string
		if err := rows.Scan(&jti); err != nil {
			return nil, dal.RowsError{Err: err}
		}
		out = append(out, jti)
	}
	if err := rows.Err(); err != nil {
		return out, dal.RowsError{Err: err}
	}
	return out, nil
}

// QueueTables is every queue_<cid> table but the template, on the write
// instance.
func (h *HLRNumbers) QueueTables(ctx context.Context) ([]string, error) {
	rows, err := h.qdb.QueryContext(ctx, stQueueTables.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		if t != "queue_template" {
			tables = append(tables, t)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

// QueueDepth is one queue table's depth, and the oldest age from the first
// rows by id, on the store's own clock.
func (h *HLRNumbers) QueueDepth(ctx context.Context, t string) (dal.NullInt64, dal.NullInt64, error) {
	_, _ = stQueueDepth, stQueueAge
	var depth, age dal.NullInt64
	qt := quoteIdent(t)
	err := h.qdb.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+qt+" WHERE status='queued'").Scan(&depth)
	if err == nil && depth.Int64 > 0 {
		err = h.qdb.QueryRowContext(ctx,
			"SELECT DATEDIFF_BIG(SECOND, MIN(created_at), SYSUTCDATETIME()) FROM (SELECT TOP (1000) created_at FROM "+qt+
				" WHERE status='queued' ORDER BY id) f").Scan(&age)
	}
	return depth, age, err
}

var _ dal.HLRNumbers = (*HLRNumbers)(nil)
