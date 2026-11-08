package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// The reference cache's reads (dal.ReferenceData, dal.HLRNumbers), moved
// here from internal/refdata/loader_mysql.go on 29 Oct 2026 with the same
// statements and the same choice of pool. How a row becomes an account, a
// route or a rule stayed in internal/refdata.

// ---- the config store -----------------------------------------------------

// ReferenceData reads the config store's rows the cache is built from.
type ReferenceData struct {
	db *sql.DB
	// crd is where the config store is read from when a read instance is
	// set beside the write instance (dbpool.Reader, 2026-10-13). Reference
	// data may be a moment old, so the periodic loads and the change
	// fingerprint go there (cfgRead). freshUntil (unix nanoseconds) holds
	// the reads on the write instance for a while after a forced refresh.
	crd        *dbpool.Read
	freshUntil atomic.Int64
}

// The reference cache's reads, on the process's one pool a database.
var _ = serves(func(d *dal.Driver) {
	d.ReferenceData = func(dsn string) (dal.ReferenceData, error) {
		r, err := OpenReferenceData(dsn)
		if err != nil {
			return nil, err
		}
		return r, nil
	}
	d.HLRNumbers = func(dsn string) (dal.HLRNumbers, error) {
		h, err := OpenHLRNumbers(dsn)
		if err != nil {
			return nil, err
		}
		return h, nil
	}
})

// OpenReferenceData opens the reads on the process's one pool for dsn, and
// its read instance when one is set.
func OpenReferenceData(dsn string) (*ReferenceData, error) {
	db, err := dbpool.Get(dsn) // the process's one pool per database
	if err != nil {
		return nil, err
	}
	r := &ReferenceData{db: db}
	r.crd, _ = dbpool.Reader(dsn)
	return r, nil
}

// DB is the write instance's pool (a test's own rows).
func (r *ReferenceData) DB() *sql.DB { return r.db }

// freshFor is how long the config store is read on the write instance after
// a forced refresh.
const freshFor = time.Minute

// cfgRead is the pool a reference-data read uses.
//
// With a read instance set, the periodic loads and the change fingerprint
// are read there: a cache that is refreshed every 30 seconds does not mind
// rows a moment old. A FORCED refresh is different — the Admin API has just
// written a change and asks for it to be loaded now — so a context marked
// dbpool.Fresh reads the write instance, and so does everything for freshFor
// after it. Without that hold, the next periodic check would read the
// fingerprint from a read node that may not have the change yet, see it
// differ from the one just loaded, and load the OLD rows back over the new.
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

// ReadsFromWrite says a read with ctx goes to the write instance now (a
// test of the choice above).
func (r *ReferenceData) ReadsFromWrite(ctx context.Context) bool { return r.cfgRead(ctx) == r.db }

// EndFreshHold ends the hold after a forced refresh now (a test's).
func (r *ReferenceData) EndFreshHold() { r.freshUntil.Store(0) }

// HasReadInstance says a read instance is set beside the write instance.
func (r *ReferenceData) HasReadInstance() bool { return r.crd != nil && r.crd.Separate() }

// MissingTable is MySQL's ER_NO_SUCH_TABLE (1146).
func (r *ReferenceData) MissingTable(err error) bool { return isMissingTable(err) }

var (
	stRefAccounts = ConfigStmts.Add("reference.accounts", `
		SELECT a.id, a.user_id, a.username,
		       a.jwt, a.http, a.smpp, a.allowed_sender_ids, a.allowed_ip_list,
		       a.is_disabled, a.enforce_account_expiry,
		       a.enforce_spam_check, COALESCE(a.enforce_spam_strict,0), a.enforce_template_check, a.enforce_auto_replace,
		       a.enforce_duplicate_check, a.enforce_hlr_db_check, COALESCE(a.enforce_hlr_dip_url,''),
		       COALESCE(a.hlr_miss_policy,'proceed'),
		       CASE WHEN c.id IS NOT NULL THEN c.body_privacy ELSE a.body_privacy END,
		       COALESCE(a.dlr_channel,'both'), COALESCE(a.dlr_bind_attempts,3),
		       COALESCE(a.mo_channel,'both'), COALESCE(a.mo_bind_attempts,3),
		       COALESCE(a.dlr_max_attempts,0), COALESCE(a.mo_max_attempts,0),
		       COALESCE(a.dlr_levels,'') = 'none',
		       a.max_recipients, a.quiet_range, a.dedupe_ttl_sec, a.default_ttl_sec,
		       a.whitelist_numbers, a.smpp_config,
		       COALESCE(a.company_id,0), COALESCE(c.is_disabled,0),
		       COALESCE(JSON_EXTRACT(c.billing_config,'$.prepaid'),0) = 1,
		       COALESCE(c.balance,0), COALESCE(c.max_throughput,0),
		       c.quota, COALESCE(c.timezone,'UTC'),
		       COALESCE(c.currency,'USD'), COALESCE(c.base_conversion_rate,0), COALESCE(c.base_thirdparty_crp_id,''),
		       COALESCE(a.loss_detection_enabled,0),
		       COALESCE(a.default_mt_rate_card_id,0), COALESCE(a.charge_point,'acceptance'),
		       COALESCE(a.default_dlr_rate_card_id,0), COALESCE(a.default_hlr_rate_card_id,0),
		       COALESCE(a.charge_failed_hlr,0) = 1,
		       COALESCE(a.password,''), COALESCE(d.url, '/sms/v4/send'),
		       d.request_map, d.response_map, d.keep_unmapped,
		       d.auth_config, COALESCE(d.request_wrapper,''), COALESCE(d.request_format,''), COALESCE(a.token_hash,''),
		       COALESCE(a.store_auth_data,0),
		       COALESCE(a.access_scope,'both'),
		       a.dest_cap_hourly, a.dest_cap_daily,
		       COALESCE(a.octates_to_septets_unpacking,0),
		       -- mo_throughput/dlr_throughput are retired (config/0123):
		       -- throughput is per deployment now, and this is the abuse
		       -- monitor's penalty, which is enforcement rather than config.
		       COALESCE(a.abuse_throttle_tps,0),
		       a.hlr_order, a.hlr_cache_ttl_days, a.hlr_provider_ids,
		       COALESCE(a.deployments,''), COALESCE(a.default_sender,''),
		       c.country_quota
		FROM sms_service_config a
		LEFT JOIN companies c ON c.id = a.company_id
		LEFT JOIN sms_service_dialects d ON d.id = a.dialect_id`, false)
	stRefApprovedSenders = ConfigStmts.Add("reference.approved_senders",
		`SELECT account_id, sender FROM sender_ids
		 WHERE status = 'approved' AND account_id IS NOT NULL`, false)
	stRefHLRDefaults = ConfigStmts.Add("reference.hlr_defaults",
		`SELECT name, CAST(value AS CHAR) FROM gateway_settings
		  WHERE name IN ('hlr_default_order','hlr_default_cache_ttl_days')`, false)
	stRefHasShortCodes = ConfigStmts.Add("reference.has_vendor_short_codes", `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'vendors' AND column_name = 'short_codes'`, false)
	stRefRouteVendors = ConfigStmts.Add("reference.route_vendors", fmt.Sprintf(refRouteVendorsSQL, refShortCodesCol), false)
	stRefRoutes       = ConfigStmts.Add("reference.routes", `
		SELECT r.id, COALESCE(r.name,''), r.priority, r.route_type, COALESCE(r.weight,100), r.direction, COALESCE(r.account_id,0),
		       r.match_mccmncs, r.accept_undetected_network,
		       r.sender_regex, r.receiver_regex, r.keyword_regex, r.priority_regex,
		       COALESCE(r.vendor_id,0), COALESCE(r.buying_rate_card_id,0),
		       COALESCE(r.selling_rate_card_id,0), r.is_disabled,
		       r.send_plan, r.retry_rounds, r.retry_first_sec, r.retry_backoff, r.retry_max_wait_sec
		FROM routes r
		ORDER BY r.priority`, false)
	stRefRates = ConfigStmts.Add("reference.rates", `
		SELECT rr.rate_card_id, rc.kind, rc.currency, COALESCE(rr.match_mcc,''), COALESCE(rr.match_mnc,''),
		       COALESCE(rr.destination_country_code,''), rr.price_per_segment,
		       COALESCE(rr.conversion_rate,1), COALESCE(rr.thirdparty_crp_id,''),
		       rr.id, COALESCE(rr.tax_percentage,0), rr.effective_from,
		       COALESCE(co.currency,''), COALESCE(co.base_conversion_rate,0), COALESCE(co.base_thirdparty_crp_id,'')
		FROM rate_card_rates rr JOIN rate_cards rc ON rc.id = rr.rate_card_id
		LEFT JOIN companies co ON co.id = rc.company_id
		WHERE rc.is_disabled=0
		  AND rr.valid_to IS NULL`, false)
	stRefAutoReplace = ConfigStmts.Add("reference.auto_replace", `
		SELECT account_id, replace_regex, seq
		FROM auto_replace_text WHERE is_disabled = 0 ORDER BY account_id, seq`, false)
	stRefSpam = ConfigStmts.Add("reference.spam", `
		SELECT company_id, list_type, COALESCE(category,''), COALESCE(action,'reject'),
		       COALESCE(destination_country_code,''), keywords,
		       COALESCE(keyword_regex,'')
		FROM spam_keywords
		ORDER BY id`, false)
	stRefTemplates = ConfigStmts.Add("reference.templates",
		`SELECT id, COALESCE(account_id,0), COALESCE(company_id,0), regex,
		        default_priority, shortcodes,
		        COALESCE(ap1,''), COALESCE(ap2,''), COALESCE(apg1,''), COALESCE(apg2,'')
		 FROM templates WHERE status = 'approved' AND is_disabled = 0
		 ORDER BY id`, false)
	stRefBlocks = ConfigStmts.Add("reference.blocks",
		`SELECT msisdn, COALESCE(account_id,0), COALESCE(company_id,0), COALESCE(sender,''), expires_at,
		        COALESCE(source,'manual')
		 FROM blocklist WHERE is_disabled = 0`, false)
	stRefCountryRules = ConfigStmts.Add("reference.country_rules",
		`SELECT COALESCE(account_id,0), destination_country_code, mode, quota,
		        COALESCE(rate_limit,0), COALESCE(priority,0)
		 FROM quota_by_countries WHERE is_disabled = 0`, false)
	stRefNow       = ConfigStmts.Add("reference.now", "SELECT NOW()", false)
	stRefTableMark = ConfigStmts.Add("reference.table_mark", "SELECT COUNT(*), MAX(updated_at), NULL FROM <table>", false)
)

const (
	refRouteVendorsSQL = `
		SELECT v.id, v.cid, v.service_status, v.transport,
		       COALESCE(JSON_EXTRACT(v.protocol_config,'$.encodings'),'null'),
		       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v.protocol_config,'$.multipart_mode')),''),
		       COALESCE(v.encodings,'null'), COALESCE(v.default_mt_buying_rate_card_id,0),
		       COALESCE(CAST(v.deployments AS CHAR),''), %s
		FROM vendors v`
	// The short codes (config/0161): a store without the column has none.
	refShortCodesCol   = "COALESCE(CAST(v.short_codes AS CHAR),'')"
	refNoShortCodesCol = "''"
)

// SignatureExtra adds a checksum of the columns a same-second change can
// touch without moving MAX(updated_at), which counts whole seconds (QA
// BUG-YK-ACC-004): an account created and given its token in the same second
// kept a cached copy with no secret, and every send was refused until some
// other edit moved the fingerprint — six minutes in the test. Small tables
// only; a full read of each costs nothing next to the reload it saves.
//
// is_disabled and abuse_throttle_tps are what the abuse monitor writes, and
// it nudges nobody: its suspension or limit reaches the accept path only
// through this fingerprint (QA 2026-10-08, B-05). CONCAT_WS skips a NULL, so
// the limit is read through COALESCE — NULL and 0 are both "no limit".
var SignatureExtra = map[string]string{
	"sms_service_config": "BIT_XOR(CRC32(CONCAT_WS('#', id, jwt, password, smpp_config, is_disabled, COALESCE(abuse_throttle_tps,0))))",
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
	sc := refShortCodesCol
	if !withShortCodes {
		sc = refNoShortCodesCol
	}
	rows, err := r.cfgRead(ctx).QueryContext(ctx, fmt.Sprintf(refRouteVendorsSQL, sc))
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
	rows, err := r.cfgRead(ctx).QueryContext(ctx, stRefSpam.SQL) // the order rules are tried in, which the portal's tester follows
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
	q := "SELECT COUNT(*), MAX(updated_at), "
	if e, ok := SignatureExtra[table]; ok {
		q += e
	} else {
		q += "NULL"
	}
	err := r.cfgRead(ctx).QueryRowContext(ctx, q+" FROM "+table).Scan(&n, &max, &extra)
	return n, max, extra, err
}

var _ dal.ReferenceData = (*ReferenceData)(nil)

// ---- the HLR store --------------------------------------------------------

// HLRNumbers is the HLR store as the reference cache reads it.
type HLRNumbers struct {
	qdb *sql.DB
	// hrd is the read instance, when one is set (dbpool.Reader).
	hrd *dbpool.Read
}

// OpenHLRNumbers opens the HLR store on the process's one pool for dsn, and
// its read instance when one is set.
func OpenHLRNumbers(dsn string) (*HLRNumbers, error) {
	qdb, err := dbpool.Get(dsn)
	if err != nil {
		return nil, err
	}
	h := &HLRNumbers{qdb: qdb}
	h.hrd, _ = dbpool.Reader(dsn)
	return h, nil
}

// hlrRead is the pool an HLR lookup, the HLR cache's loads and the JWT
// revocation list are read from: the read instance when one is set. Number
// data is imported in bulk and a revocation is read every 30 seconds; none
// of them is hurt by a moment's delay.
func (h *HLRNumbers) hlrRead() *sql.DB {
	if h.hrd != nil {
		if d := h.hrd.DB(); d != nil {
			return d
		}
	}
	return h.qdb
}

var (
	stHLRLookup = ConfigStmts.Add("reference.hlr_lookup",
		"SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), ported, last_check FROM hlr_<first4> WHERE msisdn = ? AND is_deleted = 0", true)
	stHLRShards = ConfigStmts.Add("reference.hlr_shards", `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name LIKE 'hlr\_%'
		  AND table_name <> 'hlr_template'`, false)
	stHLRLastCheckCol = ConfigStmts.Add("reference.hlr_last_check_column", `
			SELECT data_type, extra FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'last_check'`, false)
	stHLRHasDeleted = ConfigStmts.Add("reference.hlr_has_deleted", `
			SELECT COUNT(*) > 0 FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'is_deleted'`, false)
	stHLRHasIndex = ConfigStmts.Add("reference.hlr_has_index", `
			SELECT COUNT(*) > 0 FROM information_schema.statistics
			WHERE table_schema = DATABASE() AND table_name = ? AND index_name = 'idx_last_check'`, false)
	stHLRCount     = ConfigStmts.Add("reference.hlr_count", "SELECT COUNT(*) FROM hlr_<first4>", false)
	stHLRLoadAll   = ConfigStmts.Add("reference.hlr_load_all", "SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), ported, is_deleted, last_check FROM hlr_<first4> WHERE is_deleted = 0", false)
	stHLRLoadSince = ConfigStmts.Add("reference.hlr_load_since", "SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), ported, is_deleted, last_check FROM hlr_<first4> WHERE last_check > ? OR (last_check = ? AND ? >= NOW() - INTERVAL 10 SECOND)", false)
	stRevocations  = ConfigStmts.Add("reference.revocations",
		`SELECT jti FROM jwt_revocations WHERE expires_at > NOW()`, false)
	stQueueTables = ConfigStmts.Add("reference.queue_tables", `SHOW TABLES LIKE 'queue\_%'`, false)
	stQueueDepth  = ConfigStmts.Add("reference.queue_depth", "SELECT COUNT(*) FROM `queue_<cid>` WHERE status='queued'", false)
	stQueueAge    = ConfigStmts.Add("reference.queue_age",
		"SELECT TIMESTAMPDIFF(SECOND, MIN(created_at), NOW()) FROM (SELECT created_at FROM `queue_<cid>`"+
			" WHERE status='queued' ORDER BY id LIMIT 1000) f", false)
)

// Lookup resolves one MSISDN from its first-4-digit shard (hlr_<first4>).
// Soft-deleted rows (is_deleted=1) are misses.
func (h *HLRNumbers) Lookup(shard, msisdn string) (dal.HLRRow, bool) {
	_ = stHLRLookup
	table := "hlr_" + shard
	var e dal.HLRRow
	var ported int
	var lc sql.NullTime
	err := h.hlrRead().QueryRow(
		"SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), ported, last_check FROM "+table+" WHERE msisdn = ? AND is_deleted = 0", msisdn).
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

// last_check is the change-tracking column (self-updating TIMESTAMP, indexed);
// is_deleted is the soft-delete flag. Migration queue/0005 shapes hlr_template;
// Prepare upgrades pre-existing dynamic shards to the same shape.

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

// Prepare upgrades each live shard to the cache schema. Idempotent: checks
// information_schema before altering, so restarts are no-ops.
func (h *HLRNumbers) Prepare(ctx context.Context) error {
	shards, err := h.shards(ctx)
	if err != nil {
		return err
	}
	for _, t := range shards {
		var dtype, extra string
		err := h.qdb.QueryRowContext(ctx, stHLRLastCheckCol.SQL, t).
			Scan(&dtype, &extra)
		if err != nil {
			return fmt.Errorf("hlr prepare %s: %w", t, err)
		}
		if dtype != "timestamp" || !strings.Contains(strings.ToLower(extra), "on update") {
			// Convert in three steps so pre-existing NULL/DATE values survive strict mode.
			if _, err := h.qdb.ExecContext(ctx,
				"ALTER TABLE "+t+" MODIFY COLUMN last_check TIMESTAMP NULL DEFAULT NULL"); err != nil {
				return fmt.Errorf("hlr prepare %s: %w", t, err)
			}
			if _, err := h.qdb.ExecContext(ctx,
				"UPDATE "+t+" SET last_check = CURRENT_TIMESTAMP WHERE last_check IS NULL"); err != nil {
				return fmt.Errorf("hlr prepare %s: %w", t, err)
			}
			if _, err := h.qdb.ExecContext(ctx,
				"ALTER TABLE "+t+" MODIFY COLUMN last_check TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"); err != nil {
				return fmt.Errorf("hlr prepare %s: %w", t, err)
			}
		}
		var hasDeleted bool
		if err := h.qdb.QueryRowContext(ctx, stHLRHasDeleted.SQL, t).
			Scan(&hasDeleted); err != nil {
			return fmt.Errorf("hlr prepare %s: %w", t, err)
		}
		if !hasDeleted {
			if _, err := h.qdb.ExecContext(ctx,
				"ALTER TABLE "+t+" ADD COLUMN is_deleted TINYINT(1) NOT NULL DEFAULT 0"); err != nil {
				return fmt.Errorf("hlr prepare %s: %w", t, err)
			}
		}
		var hasIdx bool
		if err := h.qdb.QueryRowContext(ctx, stHLRHasIndex.SQL, t).
			Scan(&hasIdx); err != nil {
			return fmt.Errorf("hlr prepare %s: %w", t, err)
		}
		if !hasIdx {
			if _, err := h.qdb.ExecContext(ctx,
				"ALTER TABLE "+t+" ADD INDEX idx_last_check (last_check)"); err != nil {
				return fmt.Errorf("hlr prepare %s: %w", t, err)
			}
		}
	}
	return nil
}

// Count totals HLR rows across all shards (memory precheck input).
func (h *HLRNumbers) Count(ctx context.Context) (int64, error) {
	_ = stHLRCount
	shards, err := h.shards(ctx)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, t := range shards {
		var n int64
		if err := h.hlrRead().QueryRowContext(ctx, "SELECT COUNT(*) FROM "+t).Scan(&n); err != nil {
			return 0, fmt.Errorf("hlr count %s: %w", t, err)
		}
		total += n
	}
	return total, nil
}

// hlrSameSecondFor is how long, in seconds of MySQL's clock, the rows of the
// newest second seen are read again: longer than a row's write takes to commit.
const hlrSameSecondFor = 10

// LoadSince returns entries changed at/after `since` across all shards plus the
// max last_check seen. Zero since = initial full load (soft-deleted rows are
// skipped; delta loads include them so the cache can evict).
//
// Rows AT `since` (the last pass's newest second) are read again only while
// that second is within hlrSameSecondFor of MySQL's own clock: a row
// committed late in that second is still caught, and the rows that share it
// are not loaded again on every pass for ever. Before (found by the test pass
// of 27-28 Oct 2026), an HLR import whose rows all had one last_check was
// written to Redis again every 30 s while no newer row came (the rig's 10,000
// rows: 335 SETs a second on an idle gateway).
func (h *HLRNumbers) LoadSince(ctx context.Context, since time.Time) ([]dal.HLRRow, time.Time, error) {
	_, _ = stHLRLoadAll, stHLRLoadSince
	shards, err := h.shards(ctx)
	if err != nil {
		return nil, since, err
	}
	maxTS := since
	var out []dal.HLRRow
	for _, t := range shards {
		q := "SELECT msisdn, mcc, mnc, COALESCE(operator_name, ''), COALESCE(ccc, ''), ported, is_deleted, last_check FROM " + t
		var rows *sql.Rows
		if since.IsZero() {
			rows, err = h.hlrRead().QueryContext(ctx, q+" WHERE is_deleted = 0")
		} else {
			rows, err = h.hlrRead().QueryContext(ctx, q+" WHERE last_check > ? OR (last_check = ? AND ? >= NOW() - INTERVAL "+
				strconv.Itoa(hlrSameSecondFor)+" SECOND)", since, since, since)
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
	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return nil, err
		}
		if t != "queue_template" {
			tables = append(tables, t)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	return tables, nil
}

// QueueDepth is one queue table's depth from the claim index alone (idx_claim
// covers status), and the oldest age from the first rows by id, worked out
// on MySQL's own clock (time.Since on created_at was off by the server's UTC
// offset).
func (h *HLRNumbers) QueueDepth(ctx context.Context, t string) (dal.NullInt64, dal.NullInt64, error) {
	_, _ = stQueueDepth, stQueueAge
	var depth, age dal.NullInt64
	err := h.qdb.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE status='queued'", t)).Scan(&depth)
	if err == nil && depth.Int64 > 0 {
		err = h.qdb.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT TIMESTAMPDIFF(SECOND, MIN(created_at), NOW()) FROM (SELECT created_at FROM `%s`"+
				" WHERE status='queued' ORDER BY id LIMIT 1000) f", t)).Scan(&age)
	}
	return depth, age, err
}

var _ dal.HLRNumbers = (*HLRNumbers)(nil)
