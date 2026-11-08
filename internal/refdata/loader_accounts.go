// Loading accounts: the account rows and their dialect, the workflow
// senders, and JWT revocations.

package refdata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/dialect"
)

// buildDialect reassembles a dialect.Config from the dialect-mapping columns
// (originally account_dialects/0021, merged into sms_service_config by 0031,
// split out into the shareable sms_service_dialects table by 0041 — see
// LoadAccounts) by round-tripping through the same JSON shape dialect.Parse
// already validates — internal/dialect stays free of any database/sql
// dependency, and its validation logic isn't duplicated here. A non-nil error
// means the stored blob is malformed or fails validation; the caller falls
// back to native (with the custom path salvaged — see LoadAccounts) rather
// than take the whole account offline.
//
// A dialectURL equal to the shared native path yields an empty custom_path in
// the assembled document, matching pre-0021 behavior exactly (every account
// now has a ROW, but a plain one is functionally indistinguishable from the
// old "no dialect at all" state).
func buildDialect(dialectURL, requestWrapper, requestFormat string, requestMapJSON, responseMapJSON, authConfigJSON dal.NullString, keepUnmapped dal.NullBool) (*dialect.Config, error) {
	doc := struct {
		CustomPath string `json:"custom_path,omitempty"`
		// The form field this client wraps its request in (config/0109), and
		// how to read its body when the Content-Type lies (config/0110).
		RequestWrapper string          `json:"request_wrapper,omitempty"`
		RequestFormat  string          `json:"request_format,omitempty"`
		Request        json.RawMessage `json:"request,omitempty"`
		Response       json.RawMessage `json:"response,omitempty"`
		KeepUnmapped   *bool           `json:"keep_unmapped,omitempty"`
		Auth           json.RawMessage `json:"auth,omitempty"`
	}{}
	if dialectURL != nativeSendPath {
		doc.CustomPath = dialectURL
	}
	doc.RequestWrapper, doc.RequestFormat = requestWrapper, requestFormat
	if requestMapJSON.Valid && requestMapJSON.String != "" {
		doc.Request = json.RawMessage(requestMapJSON.String)
	}
	if responseMapJSON.Valid && responseMapJSON.String != "" {
		doc.Response = json.RawMessage(responseMapJSON.String)
	}
	if keepUnmapped.Valid {
		v := keepUnmapped.Bool
		doc.KeepUnmapped = &v
	}
	if authConfigJSON.Valid && authConfigJSON.String != "" {
		doc.Auth = json.RawMessage(authConfigJSON.String)
	}
	// Nothing distinguishing at all -> pure native; skip the round-trip.
	if doc.CustomPath == "" && doc.RequestWrapper == "" && doc.RequestFormat == "" && len(doc.Request) == 0 && len(doc.Response) == 0 && doc.KeepUnmapped == nil && len(doc.Auth) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("assemble: %w", err)
	}
	d, err := dialect.Parse(string(b))
	if err != nil {
		return nil, err
	}
	return d, nil
}

// LoadAccounts reads sms_service_config — the Hadal-reconciled merge of the
// old accounts + account_dialects split (migration config/0031), trimmed
// 2026-07-28 (migrations 0044/0045/0046, PO: consolidate quota/balance/
// prepaid-billing/timezone/data-retention onto companies, drop several dead
// columns, fold dlr_url/mo_url/mo_method into JSON, move the cost/balance
// disclosure flags onto sms_service_dialects).
func (l *MySQL) LoadAccounts(ctx context.Context) ([]Account, error) {
	// LEFT JOIN companies: the tenant owns balance/prepaid/quota/timezone
	// outright now — there is no per-account fallback left to read (see
	// Prepaid()/EffectiveBalance() in cache.go). billing_config is a JSON
	// {"prepaid":N} (0044) — despite using the TRUE/FALSE literals, MySQL's
	// IF() returns an integer, so JSON_OBJECT stored a JSON number (0/1),
	// not a JSON boolean; comparing against the number 1 (not the string
	// 'true') is what actually matches, confirmed live. Yields a plain 0/1
	// the driver returns as an int, same scan pattern as every other
	// boolean flag below.
	//
	// api_password/token_hash (renamed api_password->password, 0045) stay
	// native columns on this row — genuinely per-account secrets. The dialect
	// SHAPE — url/request_map/response_map/auth_config/keep_unmapped — lives
	// on sms_service_dialects, referenced by dialect_id; a NULL dialect_id
	// (native contract) needs no shared row at all, so the LEFT JOIN's NULLs
	// COALESCE to the native path / defaults, exactly what buildDialect
	// already treats as "nothing distinguishing → nil Config."
	//
	// send_sms_cost/send_remaining_balance were on sms_service_dialects
	// briefly (0046) but dropped outright by 0047 (PO 2026-07-28) rather than
	// moved anywhere — the accept-time cost/balance disclosure (S-05) is now
	// unconditionally on for every account (ShowRequestCost/ShowRemainingBalance
	// hardcoded true below), matching these columns' original DEFAULT 1. There
	// is no longer any column, anywhere, that can turn it off.
	//
	// The three throughput columns are RETIRED (config/0123): a rate is set
	// per deployment and read from `deployments`, so none of them is loaded.
	// Historic note on what they were (0031's three-way split of the old
	// single `throughput`): mt gated the submit path, mo and dlr capped the
	// pushes to the account (mo and dlr roles, smppserver/throughput.go) since
	// 2026-09-14.
	refs, err := l.ref.Accounts(ctx)
	if err != nil {
		return nil, bare(err)
	}
	var out []Account
	for _, r := range refs {
		a := Account{
			ID: r.ID, Username: r.Username,
			HLRDipURL: r.HLRDipURL, HLRMissPolicy: r.HLRMissPolicy, BodyPrivacy: r.BodyPrivacy,
			DLRChannel: r.DLRChannel, DLRBindAttempts: r.DLRBindAttempts,
			MOChannel: r.MOChannel, MOBindAttempts: r.MOBindAttempts,
			DLRMaxAttempts: r.DLRMaxAttempts, MOMaxAttempts: r.MOMaxAttempts,
			NoReceipts: r.NoReceipts, MaxRecipients: r.MaxRecipients,
			CompanyID: r.CompanyID, CompanyBalance: r.CompanyBalance, CompanyThroughput: r.CompanyThroughput,
			CompanyTimezone: r.CompanyTimezone, CompanyCurrency: r.CompanyCurrency,
			CompanyBaseRate: r.CompanyBaseRate, CompanyBaseCRP: r.CompanyBaseCRP,
			DefaultMTSellCard: r.DefaultMTSellCard, ChargePoint: r.ChargePoint,
			DefaultDLRSellCard: r.DefaultDLRSellCard, DefaultHLRSellCard: r.DefaultHLRSellCard,
			ChargeFailedHLR: r.ChargeFailedHLR, APIPassword: r.APIPassword, TokenHash: r.TokenHash,
			AccessScope: r.AccessScope, AbuseThrottleTPS: r.AbuseThrottleTPS, DefaultSender: r.DefaultSender,
		}
		var (
			http, smpp, disabled, spam, spamStrict, tpl, ar, dup = r.HTTP, r.SMPP, r.Disabled, r.Spam, r.SpamStrict, r.Template, r.AutoRepl, r.Duplicate
			hlrdb, storeAuth                                     = r.HLRDB, r.StoreAuth
			coDisabled, coPrepaid, lossDet                       = r.CompanyDisabled, r.CompanyPrepaid, r.LossDetection
			dialectURL, requestWrapper, requestFormat            = r.DialectURL, r.RequestWrapper, r.RequestFormat
			requestMapJSON, responseMapJSON, authConfigJSON      = r.RequestMap, r.ResponseMap, r.AuthConfig
			keepUnmapped                                         = r.KeepUnmapped
			jwtJSON, sidJSON, ipJSON, wlJSON                     = r.JWT, r.AllowedSenders, r.AllowedIPs, r.Whitelist
			companyQuotaJSON                                     = r.CompanyQuota
			companyCountryQuotaJSON                              = r.CompanyCountryQuota
			smppJSON                                             = r.SMPPConfig
			expiry                                               = r.Expiry
			quiet                                                = r.Quiet
			dedupeTTL                                            = r.DedupeTTL
			defaultTTL                                           = r.DefaultTTL
			capHourly, capDaily                                  = r.CapHourly, r.CapDaily
			userID                                               = r.UserID
			packedGSM7                                           = r.PackedGSM7
			hlrOrderJSON, hlrProvidersJSON                       = r.HLROrder, r.HLRProviders
			deploymentsJSON                                      = r.Deployments
			hlrTTL                                               = r.HLRCacheTTL
		)
		a.StoreAuthData = b(storeAuth)
		a.SMPPPackedGSM7 = b(packedGSM7)
		// HLR policy: NULL = the gateway default, filled in below once the
		// defaults are read.
		a.HLROrder = nil
		if hlrOrderJSON.Valid {
			var steps []string
			if json.Unmarshal([]byte(hlrOrderJSON.String), &steps) == nil {
				a.HLROrder = CleanHLROrder(steps)
				if a.HLROrder == nil {
					a.HLROrder = []string{} // set, but nothing valid: no lookup
				}
			}
		}
		a.HLRCacheTTLDays = -1
		if hlrTTL.Valid {
			a.HLRCacheTTLDays = int(hlrTTL.Int64)
		}
		if hlrProvidersJSON.Valid {
			_ = json.Unmarshal([]byte(hlrProvidersJSON.String), &a.HLRProviderIDs)
		}
		// Which deployments this account may submit to (config/0122). A
		// broken value parses to nil, which means everywhere — one bad row
		// must never stop an account's traffic on every gateway at once.
		a.Deployments, _ = deployment.Parse(deploymentsJSON)
		if userID.Valid {
			a.UserID = userID.String
		}
		// No column drives this anymore (0047) — unconditionally on.
		a.ShowRequestCost, a.ShowRemainingBalance = true, true
		// message echo defaults on (preserves prior behaviour); segments is a
		// new opt-in field, default off (P1 scope finalize 2026-08-09).
		a.ShowMessage, a.ShowSegments = true, false
		a.CompanyDisabled, a.CompanyPrepaid = b(coDisabled), b(coPrepaid)
		a.LossDetection = b(lossDet)
		// A malformed dialect must never take the account offline: fall back to
		// the native contract rather than refuse its traffic. But the custom URL
		// is its own column, NOT part of the broken JSON — salvage it, or the
		// account's registered path silently vanishes and every submit 404s
		// (chunk-3 ruling 3.2, 2026-08-03: this fallback used to be log-free AND
		// dropped the path). Only the mapping degrades to native; routing, auth
		// and rendering-by-path keep working.
		if d, derr := buildDialect(dialectURL, requestWrapper, requestFormat, requestMapJSON, responseMapJSON, authConfigJSON, keepUnmapped); derr == nil {
			a.Dialect = d
			if d != nil {
				a.CustomPath = d.CustomPath
			}
		} else {
			log.Printf("[refdata] account %d has a MALFORMED dialect (%v) — serving the native shape on its configured URL", a.ID, derr)
			if dialectURL != nativeSendPath && dialectURL != "" {
				a.Dialect = &dialect.Config{CustomPath: dialectURL}
				a.CustomPath = dialectURL
			}
		}
		a.WhitelistNumbers = parseNumberSet(wlJSON)
		parseSMPPConfig(smppJSON, &a)
		a.HTTP, a.SMPP = b(http), b(smpp)
		a.HTTPOff = !a.HTTP
		a.Disabled = b(disabled)
		a.SpamCheck, a.SpamStrict, a.TemplateCheck, a.AutoReplace = b(spam), b(spamStrict), b(tpl), b(ar)
		a.DuplicateCheck, a.HLRDBCheck = b(dup), b(hlrdb)
		if expiry.Valid {
			t := expiry.Time
			a.AccountExpiry = &t
		}
		if jwtJSON.Valid && jwtJSON.String != "" {
			_ = json.Unmarshal([]byte(jwtJSON.String), &a.JWT)
		}
		if a.JWT.SubClaim == "" {
			a.JWT.SubClaim = "sub"
		}
		if a.JWT.LeewaySec == 0 {
			a.JWT.LeewaySec = 60
		}
		if companyQuotaJSON.Valid && companyQuotaJSON.String != "" {
			_ = json.Unmarshal([]byte(companyQuotaJSON.String), &a.CompanyQuota)
		}
		// The company's own per-country quota (QA API-020): accepted and shown
		// by the Admin API since config/0044 and read by nothing until now.
		if companyCountryQuotaJSON.Valid {
			a.CompanyCountryQuota = ParseCompanyCountryQuota(companyCountryQuotaJSON.String)
		}
		if a.CompanyTimezone == "" {
			a.CompanyTimezone = "UTC"
		}
		a.Shortcodes, a.AllowAllSC = parseSenderIDs(sidJSON)
		a.AllowedIPs = parseIPList(ipJSON)
		// A malformed allowlist entry matches nothing, and a non-empty list
		// IMPLIES enforcement — so one typo locks the account out of the API
		// with a 403 and no diagnostic anywhere. The Admin API refuses these at
		// write time (PO 2026-09-03); this catches rows that arrived by another
		// road: a SQL script, a bulk import, a future admin tool. Same
		// treatment as a malformed dialect or an uncompilable route pattern —
		// say so loudly rather than fail silently.
		if bad := ValidateAllowedIPs(a.AllowedIPs); len(bad) > 0 {
			log.Printf("[refdata] account %d (%s) has MALFORMED allowed_ip_list entries %v — each matches NOTHING, so this account is refused on every IP those entries were meant to permit", a.ID, a.Username, bad)
		}
		a.QuietStartMin, a.QuietEndMin = parseQuietRange(quiet)
		// Stored quiet hours that are not real times ("25:99-88:00") were
		// ignored without a trace (QA ACC-003). The Admin API refuses them
		// now; a row that arrived another way is said out loud, like the IP
		// list above.
		if quiet.Valid && strings.TrimSpace(quiet.String) != "" && a.QuietStartMin < 0 {
			log.Printf("[refdata] account %d (%s) has MALFORMED quiet_range %q — not HH:MM-HH:MM with real times, so NO quiet hours apply to it", a.ID, a.Username, quiet.String)
		}
		if dedupeTTL.Valid {
			a.DedupeTTLSec = int(dedupeTTL.Int64)
		}
		if defaultTTL.Valid {
			a.DefaultTTLSec = int(defaultTTL.Int64)
		}
		// NULL is the default and means no cap — left as 0, which the
		// destination-cap stage reads as "skip me entirely".
		if capHourly.Valid {
			a.DestCapHourly = int(capHourly.Int64)
		}
		if capDaily.Valid {
			a.DestCapDaily = int(capDaily.Int64)
		}
		out = append(out, a)
	}
	// Accounts without their own HLR order or cache time limit take the
	// gateway's (gateway_settings, config/0102).
	order, ttl := l.hlrDefaults(ctx)
	ApplyHLRDefaults(out, order, ttl)
	// Approved sender-ID workflow rows (P1 scope finalize 2026-08-09 §13,
	// migration config/0062) union into the legacy allowed_sender_ids set —
	// the sender stage then sees one merged allowlist. Only 'approved' rows
	// with an assignment count; pending/rejected are invisible to the engine.
	// A missing table (pre-0062 DB) is not an error — the workflow is additive.
	if err := l.mergeWorkflowSenders(ctx, out); err != nil {
		return nil, err
	}
	return out, nil
}

// mergeWorkflowSenders appends approved sender_ids rows to their account's
// Shortcodes. Accounts with the '*' wildcard are left alone (already allow-all).
func (l *MySQL) mergeWorkflowSenders(ctx context.Context, accts []Account) error {
	senders, err := l.ref.ApprovedSenders(ctx)
	if err != nil {
		if !rowsErr(err) && l.isMissingTable(err) {
			return nil // pre-0062 schema: workflow not provisioned yet
		}
		return bare(err)
	}
	byAcct := map[int64][]string{}
	for _, s := range senders {
		byAcct[s.AccountID] = append(byAcct[s.AccountID], s.Sender)
	}
	for i := range accts {
		add := byAcct[accts[i].ID]
		if len(add) == 0 || accts[i].AllowAllSC {
			continue
		}
		have := map[string]bool{}
		for _, sc := range accts[i].Shortcodes {
			have[sc] = true
		}
		for _, sc := range add {
			if !have[sc] {
				accts[i].Shortcodes = append(accts[i].Shortcodes, sc)
			}
		}
	}
	return nil
}

// LoadRevocations reads the jti revocation blocklist from the HLR store (D-124).
func (l *MySQL) LoadRevocations(ctx context.Context) (map[string]bool, error) {
	// No queue store we can read (see NewMySQL): report NO revocations rather
	// than an error, because an error here would empty the whole reference
	// cache and refuse all traffic. The consequence is stated loudly at
	// startup — a revoked token stays usable until it expires on its own.
	if l.hlr == nil {
		return map[string]bool{}, nil
	}
	m := map[string]bool{}
	jtis, err := l.hlr.Revocations(ctx)
	if err != nil && !rowsErr(err) {
		return m, nil // fail-open: no revocations rather than blocking auth entirely
	}
	if err != nil && jtis == nil {
		return nil, bare(err) // a row that would not scan
	}
	for _, jti := range jtis {
		m[jti] = true
	}
	return m, bare(err)
}
