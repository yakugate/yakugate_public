// Package dialect translates between a client's own API vocabulary and
// Yakugate's canonical one, so a customer migrating from another gateway keeps
// their existing integration.
//
// It is deliberately pure — no I/O, no database, no HTTP types — so the whole
// mapping surface is unit-testable and can never fail a request by itself. A
// nil *Config means "native contract", which is what every account without a
// dialect configured uses.
//
// Position in the request path: this wraps the HTTP edge only. Inbound mapping
// runs BEFORE the native adapter and outbound mapping AFTER the pipeline
// returns, so the 18 pipeline stages, the queue and every downstream process
// only ever see canonical fields. [SCOPE P1-DIALECT / S-07]
package dialect

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Canonical response field names usable as `source` in a response mapping.
const (
	FieldStatusCode = "statusCode"
	// FieldStatus carries the uppercase reason vocabulary (ACCEPTED, or a
	// reject reason code) — this is what FieldMessage meant before the PO's
	// 2026-07-27 contract split: "message is actually status, and message is
	// the actual text the user sent us." Renamed rather than reusing
	// FieldMessage for both meanings, which would collide.
	FieldStatus       = "status"
	FieldMessage      = "message"
	FieldMessageID    = "messageId"
	FieldAcceptedTime = "acceptedTime"
	FieldError        = "error"
	FieldTo           = "to"
	// FieldRequestID is the server-generated batch/submission id (one ULID per
	// request, shared by every recipient of a fan-out) — returned in every
	// recipient-level response so a client can correlate a whole batch.
	// [PO 2026-07-26]
	FieldRequestID = "requestId"
	// FieldReference is the id the CLIENT supplied at submit, handed straight
	// back (PO 2026-09-06). It exists because `messageId` means two different
	// things in one exchange: the client sends theirs, we return ours. This is
	// the client's own value, under its own name, so the two are never
	// confused — and it is the same word on both edges, since an SMPP client's
	// user_message_reference is the same idea as an HTTP client's messageId.
	// Absent from the response when the client supplied nothing.
	FieldReference = "reference"
	// FieldSender/FieldBody expose the submitted sender and message text —
	// added 2026-07-23 for dialects whose response echoes them back (e.g. the
	// Jazz XML API's <originator>/<messagedata>). No prior dialect referenced
	// either, so this is purely additive: an account that doesn't map them
	// sees no change at all.
	FieldSender = "sender"
	FieldBody   = "body"
	// FieldResponseDateTime is a SEPARATE timestamp from FieldAcceptedTime —
	// same instant, formatted "2006-01-02 15:04:05" (the Jazz doc's literal
	// example format) rather than RFC3339. Kept distinct rather than
	// reformatting AcceptedTime itself, since other dialects/tests already
	// depend on AcceptedTime's existing ISO shape.
	FieldResponseDateTime = "responseDateTime"
	// FieldCost/FieldCurrency/FieldRemainingBalance are OPTIONAL — present only
	// on an accepted recipient whose account enables them
	// (sms_service_config.send_sms_cost / send_remaining_balance) AND whose
	// route resolved a sell rate. Unlike the fixed native field set above,
	// these genuinely appear/vanish per account and per outcome — there is no
	// "empty placeholder" for an unpriced or opted-out recipient. [PO 2026-07-27]
	FieldCost             = "cost"
	FieldCurrency         = "currency"
	FieldRemainingBalance = "remainingBalance"
	// The cost breakdown (PO 2026-09-03). `cost` has been the tax-INCLUSIVE
	// price since GW-13, and `total` is deliberately the same number under the
	// name a client reading net+tax expects to add up to; `cost` stays for
	// backwards compatibility. All four ride the same per-account flag as cost.
	FieldNet           = "net"
	FieldTax           = "tax"
	FieldTaxPercentage = "taxPercentage"
	FieldTotal         = "total"
	// FieldChargedAt says the quoted cost is NOT taken yet. An account charged
	// when a vendor takes the message ("submit") or when it is delivered
	// ("delivery") is charged later, at the price in force THEN — so the cost
	// in the reply is the price as it stands at acceptance, and may differ if
	// a rate changes in between (QA BUG-YK-RC-012). Absent for an account
	// charged at acceptance, whose cost is exactly what was taken. Rides with
	// cost, like the breakdown.
	FieldChargedAt = "chargedAt"
	// FieldCampaignID echoes the request's campaignId when it carried one (P1
	// scope finalize 2026-08-09) — present only when supplied, like cost.
	FieldCampaignID = "campaignId"
	// FieldSegments is the per-recipient segment count — OPTIONAL, gated per
	// account like cost (P1 scope finalize 2026-08-09).
	FieldSegments = "segments"
)

// canonicalRequestFields are the inbound names a mapping may target. Mapping to
// anything else is a configuration error: it would silently do nothing.
// ("type" was retired 2026-07-26 — priority is the only priority input.)
var canonicalRequestFields = map[string]bool{
	"to": true, "from": true, "text": true, "messageId": true,
	"channel": true, "operator": true, "gsm7": true, "encoding": true,
	"priority": true, "ap1": true, "ap2": true, "apg1": true, "apg2": true,
	"sendAt": true, "validityPeriod": true,
}

// canonicalResponseSources are the outbound values a response mapping's
// `source` may read — exactly the keys canonical() produces. Referencing
// anything else is a configuration error: project() would miss the lookup and
// the key would silently vanish from the customer's response (chunk-3 ruling
// 3.3, 2026-08-03 — the request side already erred on typos, the response
// side did not).
var canonicalResponseSources = map[string]bool{
	FieldStatusCode: true, FieldStatus: true, FieldMessage: true,
	FieldMessageID: true, FieldRequestID: true, FieldAcceptedTime: true,
	FieldError: true, FieldTo: true, FieldSender: true, FieldBody: true,
	FieldResponseDateTime: true, FieldCost: true, FieldCurrency: true,
	FieldRemainingBalance: true, FieldCampaignID: true, FieldSegments: true,
	FieldReference: true,
}

// Field is one alias mapping. Alias is our canonical name; Source is what the
// client calls it. Default fills a value the client omitted; Map translates
// specific values.
type Field struct {
	Alias   string            `json:"alias"`
	Source  string            `json:"source"`
	Default string            `json:"default,omitempty"`
	Map     map[string]string `json:"map,omitempty"`
	// SentIn says WHERE this field travels, which is as much a part of a
	// client's shape as what it is called (PO 2026-09-16). On a REQUEST:
	// "header", "query" or "body"; empty searches all three, body first, so
	// every dialect written before this keeps working unchanged. On a
	// RESPONSE only "body" is meaningful today.
	//
	// It exists because real specs split one request across places — an API
	// key in a header, the destination in the query string, the text in the
	// body — and a mapping that could only read the body could not describe
	// them without the customer changing their client.
	SentIn string `json:"in,omitempty"`

	// Else and When are for a reply field only (6 Nov 2026, Saleem: one
	// field list for every reply, a value list per field, "try all options,
	// else its value original, else default").
	//
	// Else is what a field with a value map sends when its value is NOT in
	// the map, or is empty: each item in order, the first that has a value —
	// another of our values (source) or a fixed text (value). None has one:
	// the original value, then Default. A field without Else keeps the order
	// it always had (Default for a missing value, then the map).
	Else []ElseItem `json:"else,omitempty"`
	// When sends the field on every reply (""), only on an accepted one
	// ("success") or only on a refusal ("error").
	When string `json:"when,omitempty"`
}

// ElseItem is one fallback of a reply field: one of our values, by its
// source name, or a fixed text. Exactly one of the two is set.
type ElseItem struct {
	Source string `json:"source,omitempty"`
	Value  string `json:"value,omitempty"`
}

// When values of a reply field.
const (
	WhenAlways  = ""
	WhenSuccess = "success"
	WhenError   = "error"
)

// maxElse bounds a field's fallbacks (the hot-path CPU bound, as maxMappings).
const maxElse = 5

// ErrorRule overrides the response for one reason code. StatusCode 0 = keep the
// native HTTP status; empty Fields = the body the refusal has with no rule at
// all (the native field set) — a status-only rule changes the status, nothing
// else (2026-09-29; it used to reuse the success mapping).
type ErrorRule struct {
	StatusCode int     `json:"status_code,omitempty"`
	Fields     []Field `json:"fields,omitempty"`
}

// Response describes the outbound mapping.
//
// Errors is keyed by our uppercase reason code. The special key "*" is an
// OPT-IN default rule (chunk-3 ruling 3.4, 2026-08-03): it applies to any
// rejection whose exact code has no rule of its own, for accounts whose
// parser must never see the native fallback shape — our reason vocabulary
// grows, so exact-code enumeration can never be future-proof. Without "*",
// unmapped rejections keep the honest native-shape fallback of 2026-07-23.
//
// Fields (6 Nov 2026) is ONE field list for every reply, accepted or refused:
// a refusal uses its error rule's own fields when the rule lists some, else
// Fields. A dialect that sets Fields needs error rules only to change the
// HTTP status. Without Fields, Success and the rules work as before.
type Response struct {
	Format       string               `json:"format,omitempty"` // json (default) | text | xml
	Fields       []Field              `json:"fields,omitempty"`
	Success      []Field              `json:"success,omitempty"`
	StatusCode   int                  `json:"status_code,omitempty"` // override the 200 accept status
	Errors       map[string]ErrorRule `json:"errors,omitempty"`
	TextTemplate string               `json:"text_template,omitempty"` // format=text: "OK|{messageId}"
	XMLRoot      string               `json:"xml_root,omitempty"`      // format=xml, default "response"
}

// Config is the whole per-account dialect (accounts.api_dialect).
type Config struct {
	Request      []Field  `json:"request,omitempty"`
	KeepUnmapped *bool    `json:"keep_unmapped,omitempty"` // default true
	Response     Response `json:"response,omitempty"`
	// token_headers was DROPPED 2026-08-03 (chunk-3 ruling 3.1, D-092): the
	// field was declared and documented but never read anywhere — the loader
	// never even populated it. The real per-account credential-name mechanism
	// is Auth below (scheme=token + token_key/token_source). Stray
	// token_headers keys in stored JSON are simply ignored by Unmarshal.
	// CustomPath is this account's own submit URL (e.g. "/api/v1/send"),
	// replacing the native /sms/v4/send for this account only — PO decision
	// 2026-07-22. Empty = the account uses the native path. Enforcement
	// (matching the authenticated account against the path actually hit) lives
	// in internal/process/httpserver; this package only validates the shape.
	CustomPath string `json:"custom_path,omitempty"`
	// RequestWrapper names the single form field that carries this client's
	// whole request as one string — the shape the older XML APIs use
	// (`xmldoc=<SMSRequest>…`). Empty keeps the built-in behaviour: a field
	// literally called `xmldoc`, or, failing that, a lone form field holding
	// what is unmistakably a document.
	//
	// Same path-only limitation as the credential names above: the body is
	// parsed before authentication, so this is read from the dialect the URL
	// resolves to, and an account on the shared native path cannot set it.
	RequestWrapper string `json:"request_wrapper,omitempty"`
	// RequestFormat says how to READ this client's body, for a client whose
	// Content-Type cannot be trusted (PO 2026-09-16). "json" | "form" | "xml";
	// empty reads the header, which is right for everyone who sets it
	// correctly — and that is almost everyone.
	//
	// It exists because a legacy client's header is not always its own: JSON
	// posted as text/plain, an XML document with no Content-Type at all, a
	// form declared as application/json. Refusing those taught the customer
	// nothing except that we would not take their traffic.
	RequestFormat string `json:"request_format,omitempty"`
	// Auth describes a per-account opaque STATIC TOKEN scheme — added
	// 2026-07-23 for a legacy client whose credential is neither a JWT nor a
	// username+password pair. nil = this account uses only the gateway-wide
	// schemes (JWT, username/password).
	Auth *AuthConfig `json:"auth,omitempty"`
}

// Auth token locations a client's opaque credential can live in.
const (
	TokenSourceHeader = "header"
	TokenSourceQuery  = "query"
	TokenSourceBody   = "body"
)

// AuthConfig declares where ONE client's opaque token lives on the wire, so
// internal/auth can find it before it knows which account the request
// belongs to (see internal/refdata.Mem.TokenCandidates — the set of these
// across every token-scheme account is what makes that possible without a
// per-request full-account scan).
type AuthConfig struct {
	// Scheme is always "token" today — kept as a field (not an implicit
	// presence check) so a future scheme can be added without a breaking
	// config-shape change.
	Scheme string `json:"scheme"`
	// TokenKey is the header/query name or body field name the client's own
	// spec uses (e.g. "X-Custom-Auth", or an XML tag "ApiKey" — decoded into
	// the same flat field map GET/JSON/xmldoc submissions already produce).
	// Matched case-insensitively, same as every other credential lookup here.
	TokenKey string `json:"token_key"`
	// TokenSource narrows WHERE to look: "header" | "query" | "body". Empty
	// tries all three, same fallback order credential() already uses for
	// username/password (header, then query, then body).
	TokenSource string `json:"token_source,omitempty"`
	// UsernameKey/PasswordKey rename the two credential fields for accounts
	// whose own spec calls them something else — `user`/`pass`, `ApiUser`,
	// `login` (PO 2026-09-16: "0% hardcoding"). Empty keeps `username` and
	// `password`, which is every account that has not asked otherwise.
	//
	// Matched case-insensitively wherever credentials are read (header, query,
	// then the parsed body), exactly like the default names.
	//
	// These can only apply to an account on its OWN submit path: the names are
	// needed BEFORE a username has been read, so the dialect has to be found
	// from the URL alone. On the shared native path no account is known that
	// early and the defaults stand.
	UsernameKey string `json:"username_key,omitempty"`
	PasswordKey string `json:"password_key,omitempty"`
	// And WHERE each arrives: "header" | "query" | "body". Empty keeps the
	// search order credentials have always used (header, then query, then the
	// parsed body). Naming the place is not only precision — it stops a
	// credential being picked up from somewhere the client never puts it.
	UsernameSource string `json:"username_source,omitempty"`
	PasswordSource string `json:"password_source,omitempty"`
}

// Request body formats a dialect may pin, when the client's own Content-Type
// cannot be relied on.
const (
	RequestFormatJSON = "json"
	RequestFormatForm = "form"
	RequestFormatXML  = "xml"
)

// BodyFormat is the format this client's body should be read as, "" to read
// the Content-Type header. Safe on a nil Config.
func (c *Config) BodyFormat() string {
	if c == nil {
		return ""
	}
	return c.RequestFormat
}

// Wrapper is the form field holding the whole request, "" when there is none
// configured. Safe on a nil Config, which is the native contract.
func (c *Config) Wrapper() string {
	if c == nil {
		return ""
	}
	return c.RequestWrapper
}

// Credential describes where one credential is found: what it is called, and
// which part of the request carries it ("" = search header, query, then body).
type Credential struct {
	Key    string
	Source string
}

// Credentials are the two credential lookups for this account. Defaults for
// everyone who has not said otherwise — including a nil Config.
func (c *Config) Credentials() (user, pass Credential) {
	user, pass = Credential{Key: "username"}, Credential{Key: "password"}
	if c == nil || c.Auth == nil {
		return
	}
	if c.Auth.UsernameKey != "" {
		user.Key = c.Auth.UsernameKey
	}
	if c.Auth.PasswordKey != "" {
		pass.Key = c.Auth.PasswordKey
	}
	user.Source, pass.Source = c.Auth.UsernameSource, c.Auth.PasswordSource
	return
}

// reservedPaths are HTTP-edge routes a custom_path must never shadow. Kept
// here (not in httpserver) so a bad config is rejected at Parse() time, before
// it ever reaches the router. Prefixes end in "/"; everything else is an exact
// match.
// /sms/v3/ is deliberately NOT reserved (2026-07-26): the native path moved to
// /sms/v4/send, and a migrating account that wants to keep its old v3 URL can
// now claim /sms/v3/send explicitly as a custom path.
var reservedPaths = []string{
	"/sms/v4/", "/messages/", "/admin/", "/healthz",
}

func isReservedPath(p string) bool {
	for _, r := range reservedPaths {
		if strings.HasSuffix(r, "/") {
			if strings.HasPrefix(p, r) {
				return true
			}
			continue
		}
		if p == r {
			return true
		}
	}
	return false
}

// Formats.
const (
	FormatJSON = "json"
	FormatText = "text"
	FormatXML  = "xml"
)

// Parse decodes and validates a dialect config. A nil/empty document yields a
// nil Config, meaning "native contract".
func Parse(raw string) (*Config, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return nil, nil
	}
	var c Config
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, fmt.Errorf("dialect: invalid JSON: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate rejects configurations that would silently misbehave — an unknown
// canonical alias, a duplicate alias, an unknown format, or a text format with
// no template. Catching these here means a typo surfaces as a clear error
// instead of a mapping that quietly does nothing.
func (c *Config) Validate() error {
	// Complexity cap (P1 scope finalize 2026-08-09 §8): ≤50 field mappings in
	// total and ≤5 nesting levels per dot-path. The cap doubles as the
	// hot-path CPU bound — a dialect is parsed once per cache refresh, but
	// mapping/rendering runs per request, so an unbounded config would let one
	// tenant's onboarding choice tax the shared accept path.
	total := len(c.Request) + len(c.Response.Fields) + len(c.Response.Success)
	for _, rule := range c.Response.Errors {
		total += len(rule.Fields)
	}
	if total > maxMappings {
		return fmt.Errorf("dialect: %d field mappings exceed the %d cap", total, maxMappings)
	}
	for _, f := range c.Request {
		if err := checkDepth("request source", f.Source); err != nil {
			return err
		}
		if len(f.Else) > 0 || f.When != "" {
			return fmt.Errorf("dialect: request alias %q: else and when belong to a reply field", f.Alias)
		}
	}
	if err := checkFieldDepths(c.Response.Fields); err != nil {
		return err
	}
	if err := checkFieldDepths(c.Response.Success); err != nil {
		return err
	}
	for _, rule := range c.Response.Errors {
		if err := checkFieldDepths(rule.Fields); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, f := range c.Request {
		if f.Alias == "" {
			return fmt.Errorf("dialect: request mapping has an entry with no alias")
		}
		if err := validateSource("request alias "+f.Alias+" in", f.SentIn); err != nil {
			return err
		}
		if !canonicalRequestFields[f.Alias] {
			return fmt.Errorf("dialect: unknown request alias %q", f.Alias)
		}
		if seen[f.Alias] {
			return fmt.Errorf("dialect: duplicate request alias %q", f.Alias)
		}
		seen[f.Alias] = true
	}
	switch c.Response.Format {
	case "", FormatJSON, FormatXML:
	case FormatText:
		if strings.TrimSpace(c.Response.TextTemplate) == "" {
			return fmt.Errorf("dialect: format=text requires text_template")
		}
	default:
		return fmt.Errorf("dialect: unknown response format %q", c.Response.Format)
	}
	if sc := c.Response.StatusCode; sc != 0 && (sc < 100 || sc > 599) {
		return fmt.Errorf("dialect: response status_code %d out of range", sc)
	}
	if err := validateResponseFields("response.fields", c.Response.Fields); err != nil {
		return err
	}
	if err := validateResponseFields("response.success", c.Response.Success); err != nil {
		return err
	}
	for code, rule := range c.Response.Errors {
		if sc := rule.StatusCode; sc != 0 && (sc < 100 || sc > 599) {
			return fmt.Errorf("dialect: error %q status_code %d out of range", code, sc)
		}
		if err := validateResponseFields("response.errors["+code+"]", rule.Fields); err != nil {
			return err
		}
	}
	if err := validateCustomPath(c.CustomPath); err != nil {
		return err
	}
	if err := validateWrapper(c.RequestWrapper); err != nil {
		return err
	}
	switch c.RequestFormat {
	case "", RequestFormatJSON, RequestFormatForm, RequestFormatXML:
	default:
		return fmt.Errorf("dialect: request_format %q must be json, form, xml, or empty (read the Content-Type)", c.RequestFormat)
	}
	if err := validateAuth(c.Auth); err != nil {
		return err
	}
	return nil
}

// validateResponseFields rejects response mapping entries that can never
// produce a value (chunk-3 ruling 3.3): an unknown source WITH NO default
// misses project()'s lookup and the key silently vanishes from the customer's
// response — the typo class this exists to catch. An entry carrying a Default
// is always legal regardless of its source: {"alias": "statuscode", "source":
// "jazzCode", "default": "300"} is a real live pattern (a constant field
// whose non-canonical source merely documents what the reference gateway
// called it), and it always yields the default.
// Complexity cap (P1 scope finalize 2026-08-09 §8).
const (
	maxMappings  = 50 // total field mappings across request + success + error rules
	maxPathDepth = 5  // dot-path segments ("a.b.c.d.e" is the deepest allowed)
)

// checkDepth bounds one dot-path's nesting.
func checkDepth(what, path string) error {
	if path == "" {
		return nil
	}
	if strings.Count(path, ".")+1 > maxPathDepth {
		return fmt.Errorf("dialect: %s %q exceeds %d nesting levels", what, path, maxPathDepth)
	}
	return nil
}

// checkFieldDepths bounds the output dot-paths of a response mapping (the
// alias side is where response nesting lives — sources are canonical names).
func checkFieldDepths(fields []Field) error {
	for _, f := range fields {
		if err := checkDepth("response alias", f.Alias); err != nil {
			return err
		}
	}
	return nil
}

func validateResponseFields(where string, fields []Field) error {
	for _, f := range fields {
		if f.Alias == "" {
			return fmt.Errorf("dialect: %s has an entry with no alias", where)
		}
		switch f.When {
		case WhenAlways, WhenSuccess, WhenError:
		default:
			return fmt.Errorf("dialect: %s alias %q: when %q must be success, error or empty", where, f.Alias, f.When)
		}
		if len(f.Else) > maxElse {
			return fmt.Errorf("dialect: %s alias %q has %d else items, at most %d", where, f.Alias, len(f.Else), maxElse)
		}
		for i, e := range f.Else {
			switch {
			case e.Source != "" && e.Value != "":
				return fmt.Errorf("dialect: %s alias %q else item %d sets both source and value", where, f.Alias, i+1)
			case e.Source == "" && e.Value == "":
				return fmt.Errorf("dialect: %s alias %q else item %d sets neither source nor value", where, f.Alias, i+1)
			case e.Source != "" && !canonicalResponseSources[e.Source]:
				return fmt.Errorf("dialect: %s alias %q else item %d reads unknown source %q", where, f.Alias, i+1, e.Source)
			}
		}
		if f.Default != "" {
			continue // always produces a value, whatever the source
		}
		src := f.Source
		if src == "" {
			src = f.Alias // self-map: {"alias":"status"} reads canonical "status"
		}
		if !canonicalResponseSources[src] {
			return fmt.Errorf("dialect: %s alias %q reads unknown source %q and has no default — it would silently render nothing", where, f.Alias, src)
		}
	}
	return nil
}

func validateAuth(a *AuthConfig) error {
	if a == nil {
		return nil
	}
	// The block now carries two independent things: an opaque-token scheme,
	// and the names this client's username and password arrive under
	// (2026-09-16). A block that only renames the two fields declares no
	// scheme and needs no token — requiring one would have made "0%
	// hardcoding" impossible for a client that still sends user + password.
	renamesOnly := a.Scheme == "" && strings.TrimSpace(a.TokenKey) == "" &&
		(a.UsernameKey != "" || a.PasswordKey != "")
	if !renamesOnly {
		if a.Scheme != "token" {
			return fmt.Errorf("dialect: auth.scheme %q unsupported (only \"token\", or omit it and set username_key/password_key)", a.Scheme)
		}
		if strings.TrimSpace(a.TokenKey) == "" {
			return fmt.Errorf("dialect: auth.token_key is required for the token scheme")
		}
	}
	// A renamed credential must be usable as a field name: it is looked up in
	// headers, the query string and the parsed body alike.
	for what, name := range map[string]string{"username_key": a.UsernameKey, "password_key": a.PasswordKey} {
		if name == "" {
			continue
		}
		if name != strings.TrimSpace(name) || strings.ContainsAny(name, " \t\r\n=&") || len(name) > 64 {
			return fmt.Errorf("dialect: auth.%s %q must be a plain field name (no spaces or separators, 64 characters at most)", what, name)
		}
	}
	for what, src := range map[string]string{
		"token_source": a.TokenSource, "username_source": a.UsernameSource, "password_source": a.PasswordSource,
	} {
		if err := validateSource(what, src); err != nil {
			return err
		}
	}
	return nil
}

// validateSource accepts the three places a value can travel in, or nothing,
// which means "search them all".
func validateSource(what, src string) error {
	switch src {
	case "", TokenSourceHeader, TokenSourceQuery, TokenSourceBody:
		return nil
	}
	return fmt.Errorf("dialect: %s %q must be header, query, body, or empty (search all)", what, src)
}

// validateWrapper checks the form field a client wraps its request in. Same
// rule as a renamed credential: it has to be a plain field name.
func validateWrapper(name string) error {
	if name == "" {
		return nil
	}
	if name != strings.TrimSpace(name) || strings.ContainsAny(name, " \t\r\n=&") || len(name) > 64 {
		return fmt.Errorf("dialect: request_wrapper %q must be a plain form field name (no spaces or separators, 64 characters at most)", name)
	}
	return nil
}

func validateCustomPath(p string) error {
	if p == "" {
		return nil
	}
	if !strings.HasPrefix(p, "/") || len(p) < 2 {
		return fmt.Errorf("dialect: custom_path %q must be an absolute path of at least 2 characters", p)
	}
	if strings.ContainsAny(p, " \t\n?#") {
		return fmt.Errorf("dialect: custom_path %q must not contain whitespace, '?' or '#'", p)
	}
	if strings.HasSuffix(p, "/") {
		return fmt.Errorf("dialect: custom_path %q must not end with '/'", p)
	}
	if isReservedPath(p) {
		return fmt.Errorf("dialect: custom_path %q collides with a reserved system route", p)
	}
	return nil
}

// keepUnmapped reports whether fields with no mapping entry pass through
// untouched. Defaults to true, matching the reference gateway.
func (c *Config) keepUnmapped() bool {
	if c == nil || c.KeepUnmapped == nil {
		return true
	}
	return *c.KeepUnmapped
}

// MapRequest renames a client's inbound fields to canonical ones, applies
// defaults and value translations. A nil Config returns the input unchanged.
//
// It works on a decoded field map, so a POST JSON body and a GET query string
// take exactly the same path. Source may be a dot-separated path (e.g.
// "customer.address.city") to reach into a nested JSON object at any depth —
// [SCOPE P1-DIALECT-NEST, PO 2026-07-27]. Query-string/form submissions are
// flat by construction, so dotted sources there simply never match (same as
// any other absent field) rather than erroring.
// Elsewhere finds a request value that does NOT travel in the body: a header
// or a query parameter, by name, case-insensitively. Supplied by the HTTP edge
// (the mapping package stays free of net/http); nil means body only, which is
// what every caller did before parts of a request could be mapped from
// elsewhere.
type Elsewhere func(where, name string) (string, bool)

// MapRequest maps a body-only request — the common case, kept as its own
// entry point so existing callers read unchanged.
func MapRequest(in map[string]any, c *Config) map[string]any {
	return MapRequestFrom(in, c, nil)
}

// MapRequestFrom maps a request whose fields may come from the body, a header
// or the query string, as each mapping says (PO 2026-09-16).
func MapRequestFrom(in map[string]any, c *Config, look Elsewhere) map[string]any {
	if c == nil || len(c.Request) == 0 {
		return in
	}
	out := map[string]any{}
	if c.keepUnmapped() {
		// Start from the client's own fields, then let mappings overwrite. Any
		// source field consumed by a mapping is removed below so a legacy name
		// does not linger alongside its canonical twin.
		for k, v := range in {
			out[k] = v
		}
	}
	for _, f := range c.Request {
		src := f.Source
		if src == "" {
			src = f.Alias
		}
		var v any
		var present bool
		switch f.SentIn {
		case TokenSourceHeader, TokenSourceQuery:
			// Named a place that is not the body: look only there, so a body
			// field of the same name cannot stand in for a header the client
			// did not send.
			if look != nil {
				var sv string
				if sv, present = look(f.SentIn, src); present {
					v = sv
				}
			}
		default:
			v, present = getPath(in, src)
			if !present && look != nil && f.SentIn == "" {
				// No place named: the body first, then the other two, the same
				// search order credentials have always used.
				for _, where := range []string{TokenSourceQuery, TokenSourceHeader} {
					if sv, ok := look(where, src); ok {
						v, present = sv, true
						break
					}
				}
			}
		}
		if present && f.SentIn == "" && src != f.Alias {
			deletePath(out, src)
		}
		if !present || isEmpty(v) {
			if f.Default == "" {
				continue
			}
			v = f.Default
		}
		if len(f.Map) > 0 {
			if mapped, ok := f.Map[toString(v)]; ok {
				v = mapped
			}
		}
		out[f.Alias] = v
	}
	return out
}

// getPath reads a (possibly dot-separated) value out of a decoded JSON map,
// walking into nested objects one segment at a time. A flat (dot-free) path
// is a plain map lookup — the overwhelmingly common case costs nothing extra.
func getPath(m map[string]any, path string) (any, bool) {
	if !strings.Contains(path, ".") {
		v, ok := m[path]
		return v, ok
	}
	var cur any = m
	for _, p := range strings.Split(path, ".") {
		cm, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = cm[p]; !ok {
			return nil, false
		}
	}
	return cur, true
}

// deletePath removes a (possibly nested) key in place — the dotted-path
// counterpart of getPath, used to drop a consumed legacy field name so it
// doesn't linger alongside its canonical twin when keep_unmapped is on.
func deletePath(m map[string]any, path string) {
	if !strings.Contains(path, ".") {
		delete(m, path)
		return
	}
	parts := strings.Split(path, ".")
	cur := m
	for _, p := range parts[:len(parts)-1] {
		next, ok := cur[p].(map[string]any)
		if !ok {
			return
		}
		cur = next
	}
	delete(cur, parts[len(parts)-1])
}

// setPath writes a (possibly nested) value, creating intermediate objects as
// needed — the response-side counterpart used when a client's desired output
// alias itself is dot-separated (e.g. "result.status" nests {"result":
// {"status": ...}} in the rendered JSON). [SCOPE P1-DIALECT-NEST]
func setPath(m map[string]any, path string, v any) {
	if !strings.Contains(path, ".") {
		m[path] = v
		return
	}
	parts := strings.Split(path, ".")
	cur := m
	for _, p := range parts[:len(parts)-1] {
		next, ok := cur[p].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[p] = next
		}
		cur = next
	}
	cur[parts[len(parts)-1]] = v
}

func isEmpty(v any) bool {
	if v == nil {
		return true
	}
	s, ok := v.(string)
	return ok && s == ""
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		return fmt.Sprint(t)
	}
}
