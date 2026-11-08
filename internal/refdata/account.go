// Accounts: the Account record, its quota and JWT settings, and what an
// account's own fields say (balance, suspension, send path).

package refdata

import (
	"time"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/dialect"
)

// JWTConfig is the per-account JWT verification config (accounts.jwt JSON).
type JWTConfig struct {
	Alg    string `json:"alg"`    // HS256 | RS256 | ES256
	Secret string `json:"secret"` // dev: inline HS256 secret (prod uses SecretRef)
	// SecretRef is a secrets ref (env://, file://, …) resolved via internal/secrets.
	// When set it WINS over any inline Secret. SecretRefPrev is tried second on a
	// verification failure, so a signing key can be rotated without a flag day.
	// [SCOPE P1-SECRETS]
	SecretRef     string `json:"secret_ref"`
	SecretRefPrev string `json:"secret_ref_prev"`
	PublicKey     string `json:"public_key"` // PEM (RS256/ES256)
	JWKSURI       string `json:"jwks_uri"`
	Issuer        string `json:"issuer"`
	Audience      string `json:"audience"`
	SubClaim      string `json:"sub_claim"`  // claim that maps to the account (default "sub")
	LeewaySec     int    `json:"leeway_sec"` // clock-skew tolerance (default 60)
	// RequireExp / MaxLifetimeSec are the optional per-account token-lifetime
	// policy (chunk-2 ruling 2.6, 2026-08-03). Default (both unset) keeps the
	// deliberate legacy behaviour: a JWT without exp is accepted and never
	// expires. require_exp forces exp to be present; max_lifetime_sec
	// additionally caps how far in the future exp may lie (seconds from now).
	RequireExp     bool `json:"require_exp"`
	MaxLifetimeSec int  `json:"max_lifetime_sec"`
}

// Quota is the accounts.quota / quota_by_countries.quota JSON shape.
type Quota struct {
	DailyQuota   *int `json:"daily_quota"` // nil = unlimited
	DailyUsed    int  `json:"daily_used"`
	MonthlyQuota *int `json:"monthly_quota"`
	MonthlyUsed  int  `json:"monthly_used"`
}

// Exceeded reports whether either window is exhausted.
func (q Quota) Exceeded() bool {
	if q.DailyQuota != nil && q.DailyUsed >= *q.DailyQuota {
		return true
	}
	return q.MonthlyQuota != nil && q.MonthlyUsed >= *q.MonthlyQuota
}

// Account is the in-memory client config (accounts row, v2.0 schema).
type Account struct {
	ID       int64
	UserID   string // sms_service_config.user_id — FK to users.user_id; empty = no portal login linked [2026-07-28]
	Username string
	// APIPassword is the bcrypt hash for username/password API auth. Empty =
	// that scheme is disabled for this account (JWT only). Deliberately
	// one-way: unlike the reference gateway we cannot recover the plaintext.
	APIPassword string
	// TokenHash is the hex HMAC-SHA256 digest of this account's opaque static
	// token (sms_service_config.token_hash), present only when Dialect.Auth
	// declares the "token" scheme. Empty = that scheme is disabled for this
	// account. [2026-07-23]
	TokenHash string
	JWT       JWTConfig
	// Dialect is the parsed per-account API dialect (accounts.api_dialect).
	// nil = the native contract. Parsed once at cache load, never per request.
	Dialect *dialect.Config
	// CustomPath mirrors Dialect.CustomPath (empty when Dialect is nil), so
	// callers that only care about routing don't need a nil check on Dialect.
	CustomPath string

	HTTP bool
	// HTTPOff: the account's HTTP switch (sms_service_config.http) is OFF, so
	// it may not use the HTTP API (auth.HTTPState, 2026-10-12). The opposite
	// of HTTP, kept as its own field so that "not said" means allowed: an
	// account built by hand, and a copy in Redis written before this field,
	// both read as HTTP on.
	HTTPOff    bool
	SMPP       bool
	Shortcodes []string // allowed_sender_ids; empty when AllowAllSC
	// DefaultSender replaces a missing, malformed or unallotted sender
	// (sms_service_config.default_sender, config/0129). "" = none.
	DefaultSender string
	AllowAllSC    bool // sender_ids contains "*"
	// AllowedIPs also GATES enforcement (2026-07-28): a non-empty list means
	// "check IPs for this account" — enforce_ip_check was dropped as a
	// separate flag, same as WhitelistNumbers/whitelist_numbers below.
	AllowedIPs []string
	// AccessScope (P1 scope finalize 2026-08-09 §7/§9, migration config/0061):
	// "both" (default) | "api" | "portal". 'portal' refuses every API submit
	// after credential proof and un-scopes the IP allowlist from the engine
	// (the list then gates portal access only). Empty = "both" (pre-0061 rows).
	AccessScope   string
	Disabled      bool // is_disabled
	AccountExpiry *time.Time
	// StoreAuthData opts this account into capturing the presented credentials
	// on a credential-related auth failure, for debugging (P1 scope finalize
	// 2026-08-09). Default false; only genuine credential rejections are
	// captured, never a valid credential that failed for another reason.
	StoreAuthData  bool
	SpamCheck      bool // enforce_spam_check
	SpamStrict     bool // enforce_spam_strict — opt-in normalized/leet variant rejection at accept (D-035)
	TemplateCheck  bool // enforce_template_check
	AutoReplace    bool // enforce_auto_replace
	DuplicateCheck bool // enforce_duplicate_check
	HLRDBCheck     bool // enforce_hlr_db_check
	// HLRMissPolicy is what to do when HLR cannot name the operator:
	// "proceed" (default — send undetected, the pre-2026-09-03 behaviour) or
	// "reject" (refuse 403 HLR_UNRESOLVED). PO 2026-09-03 ruled the strict
	// reading: ANY unresolved number rejects, including one we failed to look
	// up because the provider timed out or the in-flight ceiling was full.
	HLRMissPolicy string // hlr_miss_policy
	HLRDipURL     string
	// DedupeTTLSec overrides the process-wide dedupe/idempotency window for
	// this account (accounts.dedupe_ttl_sec); 0 = use YAKU_DEDUPE_TTL_SEC.
	// [SCOPE P1-DEDUPE-TTL, PO 2026-07-27]
	DedupeTTLSec int
	// DestCapHourly / DestCapDaily cap how many messages this account may send
	// to the SAME destination inside a fixed UTC hour / UTC day (migration
	// config/0096). 0 = no cap, which is the default and costs nothing: the
	// stage returns before it touches a counter.
	//
	// Deliberately NOT the same thing as DuplicateCheck. Dedupe matches
	// sender+body+destination, so it only catches the identical message sent
	// twice; these catch many DIFFERENT messages aimed at one subscriber.
	DestCapHourly int
	DestCapDaily  int
	// DefaultTTLSec is the account-level message-lifetime default (D-026's
	// middle layer, TC-P7-09, migration config/0052): 0 = no override, fall
	// through to YAKU_DEFAULT_TTL_SEC. Only consulted when the request itself
	// didn't set a validity period. [SCOPE P7 / D-026]
	DefaultTTLSec int
	// BodyPrivacy is what is kept of a message's text: plain_text |
	// pii_masked | all_encrypted. It is the COMPANY's setting (companies.
	// body_privacy, config/0101, PO 2026-09-13); only an account with no
	// company still reads its own old sms_service_config.body_privacy.
	BodyPrivacy string
	// NoReceipts: this account's clients are sent no receipt at all
	// (sms_service_config.dlr_levels = 'none', config/0142). The enqueue
	// stage accepts every message of such an account as if the client had
	// asked for no receipt.
	NoReceipts bool

	// DLRChannel decides WHERE this account's receipts go: "bind" (the SMPP
	// bind only), "url" (the HTTP poster only) or "both" (bind first, url
	// takes over once the bind has failed DLRBindAttempts times). Before this
	// an account configured for both could be delivered the same receipt
	// twice, since the two legs drain one queue and were separated only by a
	// 5-second staleness gap (P5-B03, migration config/0077). Empty = "both".
	DLRChannel      string
	DLRBindAttempts int
	// MOChannel and MOBindAttempts are the same choice for this account's
	// incoming messages (config/0146, 2026-10-15): "bind", "url" or "both"
	// (bind first, the webhook once the bind has failed MOBindAttempts
	// times). Independent of DLRChannel, so MO may go to a webhook while the
	// receipts stay on the bind, or the other way round. Empty = "both".
	MOChannel      string
	MOBindAttempts int
	// DLRMaxAttempts and MOMaxAttempts are the most tries to give this
	// account's client a receipt or an MO, bind and webhook together
	// (config/0173, 5 Nov 2026); one that reaches it is given up. 0 = the
	// gateway's default (the webhook's own retry; a bind only the age limit).
	DLRMaxAttempts, MOMaxAttempts int

	// Tenant (companies row) this API client belongs to. Quota/balance/prepaid
	// billing/timezone/data-retention all moved to companies-only (2026-07-28,
	// PO: "must be in companies") — every account is expected to be linked
	// (existing unlinked rows were backfilled into their own dedicated
	// single-account company, migration config/0044). CompanyID 0 means the
	// company link is somehow still missing; Prepaid()/EffectiveBalance() then
	// fall back to zero-value (no credit) rather than a per-account field
	// that no longer exists.
	CompanyID       int64
	CompanyDisabled bool
	CompanyPrepaid  bool
	CompanyBalance  float64
	CompanyQuota    Quota // companies.quota — enforced by the quota stage (chunk-6 ruling 6.2)
	// CompanyCountryQuota is companies.country_quota: dialling-code prefix ->
	// quota, shared by every account of the company (QA API-020; the shape is
	// in company_country_quota.go). nil = none.
	CompanyCountryQuota map[string]Quota `json:",omitempty"`
	CompanyTimezone     string           // companies.timezone; defaults "UTC" if the company row is somehow missing
	// CompanyThroughput is companies.max_throughput — the SHARED segments/sec
	// cap every account under the company draws from (0 = unlimited).
	// [PO 2026-07-26]
	CompanyThroughput int
	// CompanyCurrency (companies.currency) and LossDetection back S-03:
	// refuse a route that would sell below its buy cost, once both prices
	// are converted into the company's currency. The switch is per ACCOUNT
	// (sms_service_config.loss_detection_enabled, config/0113, PO
	// 2026-09-17 — it was per company in config/0079, a column the engine
	// no longer reads). Off unless the account opted in.
	CompanyCurrency string
	LossDetection   bool
	// CompanyBaseRate (companies.base_conversion_rate) turns an amount in the
	// company's currency into the SYSTEM BASE currency: base = amount x rate,
	// so a PKR company under a USD base holds 0.0036. CompanyBaseCRP
	// (base_thirdparty_crp_id) names a currency provider whose rate for
	// company currency -> base currency is used instead while it has one.
	// Prices held in different currencies are compared in the base currency
	// (Saleem 2026-10-07: "loss is also detected in base currency as two
	// companies can be in different currency").
	CompanyBaseRate float64
	CompanyBaseCRP  string
	// DefaultMTSellCard (default_mt_rate_card_id) and ChargePoint
	// (charge_point, config/0114) let the accept path price a message
	// exactly and reserve that amount when the account is charged at
	// acceptance (PO 2026-09-17: reserve the exact amount, not an estimate).
	DefaultMTSellCard int64
	ChargePoint       string
	// DefaultDLRSellCard and DefaultHLRSellCard (default_dlr_rate_card_id,
	// default_hlr_rate_card_id) let the accept path fix the price of one
	// delivery receipt and one number lookup with the message's own
	// (billing.Quote, 2026-10-13): a card set later never prices a message
	// that was already accepted.
	DefaultDLRSellCard int64
	DefaultHLRSellCard int64
	// ChargeFailedHLR (sms_service_config.charge_failed_hlr): a live lookup
	// that found nothing is charged too. With the message's network source it
	// says at acceptance whether its lookup is one the billing role charges
	// (billing.LookupCharged), so the quote prices only such a lookup.
	ChargeFailedHLR bool
	// sms_service_config.mt_throughput is RETIRED (config/0123): an account's
	// rate is set per deployment and enforced there, so this cache does not
	// carry it at all. A field nothing reads is worse than no field — it
	// reads as live to the next person. The console shows the old value from
	// its own query, as history.
	MaxRecipients int
	QuietStartMin int // minutes-of-day, -1 = unset (parsed from quiet_range)
	QuietEndMin   int // minutes-of-day, -1 = unset

	// WhitelistNumbers also GATES enforcement (2026-07-28): a non-empty set
	// means "send ONLY to whitelisted numbers" — is_whitelist_number was
	// dropped as a separate flag, same pattern as AllowedIPs above.
	WhitelistNumbers map[string]struct{}

	// SMPP bind credentials/policy (accounts.smpp_config; used when SMPP=true). [SCOPE P2]
	SMPPSystemID     string
	SMPPPasswordHash string   // bcrypt
	SMPPAllowedModes []string // "TX","RX","TRX"; empty = all
	SMPPMaxBinds     int      // concurrent-bind cap, ALL modes; 0 = explicitly unlimited, absent in JSON = 10 (DEC-2/D-065)
	// SMPPEnquireLinkSec/SMPPIdleTimeoutSec configure the server-initiated
	// keepalive and inactivity expiry per account (accounts.smpp_config); 0 =
	// unset, the process applies its own default. [SCOPE P2-KEEPALIVE, PO 2026-07-27]
	SMPPEnquireLinkSec int
	// SMPPMultipartMode (smpp_config.multipart_mode, PO 2026-09-17): "udh"
	// parts (default) or "tlv" — a long inbound message goes to this client
	// as one deliver_sm with message_payload, on an SMPP 3.4+ bind only.
	SMPPMultipartMode  string
	SMPPIdleTimeoutSec int
	// SMPPWindow is how many submit_sm this account may have in flight at
	// once ON ONE BIND (P2 ingress window, PO 2026-09-04). Beyond it a submit
	// is answered ESME_RTHROTTLED so the client backs off. 0/absent = the
	// process default (YAKU_SMPP_WINDOW). 1 restores the old strictly-serial
	// behaviour for a client that cannot handle out-of-order responses.
	SMPPWindow int
	// SMPPDeliverWindow is how many deliver_sm — receipts and incoming
	// messages together — ONE bind of this account may have out without an
	// answer (smpp_config.deliver_window, Saleem 28 Oct 2026: "one setting for
	// both mo and dlr"). 0/absent = the process default
	// (YAKU_SMPP_DELIVER_WINDOW).
	SMPPDeliverWindow int
	// SMPPPackedGSM7 (sms_service_config.octates_to_septets_unpacking,
	// 2026-09-14): this client's SMPP text is GSM-7 PACKED into septets. The
	// server unpacks what it submits and packs what the gateway sends it
	// (incoming messages and receipts). Off = one character per byte.
	SMPPPackedGSM7 bool

	// The old MOThroughput / DLRThroughput are RETIRED (config/0123, PO
	// 2026-09-24: throughput is always per deployment) and no longer loaded
	// at all — the caps come from Deployments.Rate(deployment.ID(), …).
	//
	// AbuseThrottleTPS is not configuration — it is P10's penalty, a
	// gateway-wide segments/sec ceiling across every deployment, applied on
	// top of whatever the per-deployment entries allow. 0 = none. Only an
	// operator clears it.
	AbuseThrottleTPS int
	// Deployments is which gateways this account may submit to, with each
	// one's own throughput (sms_service_config.deployments, config/0122).
	// Empty = every deployment, which is what every account has until an
	// operator narrows it — so this is additive on a live gateway.
	Deployments deployment.List

	// HLR lookup policy (2026-09-14), resolved at load: the account's own
	// setting, else the gateway default (gateway_settings).
	//   HLROrder: the steps tried in order, first answer wins — "client",
	//     "data", "range", "live", "country" (see pipeline stage_hlr.go).
	//   HLRCacheTTLDays: how long an answer in our HLR data counts as known;
	//     0 = never expires.
	//   HLRProviderIDs: the live-lookup providers (http_endpoints ids), in
	//     order; empty = no live lookups.
	HLROrder        []string
	HLRCacheTTLDays int
	HLRProviderIDs  []int64

	// ShowRequestCost/ShowRemainingBalance gate the optional cost/balance
	// fields on the accept response. Briefly configurable via
	// sms_service_dialects (migration config/0046), but that toggle was
	// dropped outright by 0047 (PO 2026-07-28) rather than moved anywhere —
	// the loader now sets both unconditionally true for every account
	// (matching the deleted columns' original DEFAULT 1). There is currently
	// no way to turn this off; reintroducing that would need a new column
	// somewhere. Cost is priced synchronously via billing.Rater at accept
	// time — the same route/rate-card logic P9 uses, just called inline;
	// remaining balance is EffectiveBalance() minus that cost, a
	// cached-balance estimate since P9 still settles the real ledger
	// asynchronously. [SCOPE P1-COST, PO 2026-07-27]
	ShowRequestCost      bool
	ShowRemainingBalance bool
	// ShowMessage/ShowSegments gate the optional `message` (echo of the client's
	// submitted text) and per-recipient `segments` fields on the accept response
	// (P1 scope finalize 2026-08-09 — both became optional, "like cost").
	// ShowMessage defaults TRUE (preserves the pre-2026-08-09 behaviour where
	// message was always present); ShowSegments defaults FALSE (a new opt-in
	// field). A real per-account column can drive these later; the loader sets
	// the defaults today.
	ShowMessage  bool
	ShowSegments bool
}

// Prepaid reports whether this API client settles against a prepaid balance —
// always the tenant (company) setting now (2026-07-28: prepaid billing moved
// to companies-only). An account somehow still unlinked (CompanyID 0) has no
// billing home at all, so it reports postpaid/false rather than crediting it
// by default. [D-046]
func (a Account) Prepaid() bool {
	return a.CompanyID != 0 && a.CompanyPrepaid
}

// EffectiveBalance is the balance the credit stage and P9 settle against —
// always the company's now (2026-07-28). An unlinked account (CompanyID 0)
// has no balance to draw on: zero, not an unbounded/undefined credit.
func (a Account) EffectiveBalance() float64 {
	if a.CompanyID != 0 {
		return a.CompanyBalance
	}
	return 0
}

// Suspended reports whether traffic must be refused because either this API
// client or its owning tenant is disabled. Disabling a company suspends every
// account under it in one action.
func (a Account) Suspended() bool { return a.Disabled || a.CompanyDisabled }

// nativeSendPath is the default submit URL every account without a custom path
// uses. Kept here (not imported from httpserver) to avoid a package cycle —
// refdata is a lower layer than the HTTP edge. Moved v3→v4 2026-07-26
// (migration config/0023 rewrites the stored rows in the same step — the two
// must deploy together).
const nativeSendPath = "/sms/v4/send"

// ExpectedSendPath is the ONE valid submit path for this account: its own
// CustomPath when configured (replaces the native path — PO decision
// 2026-07-22), else the native /sms/v4/send.
func (a Account) ExpectedSendPath() string {
	if a.CustomPath != "" {
		return a.CustomPath
	}
	return nativeSendPath
}
