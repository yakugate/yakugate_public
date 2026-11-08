// Package httpadapter is the CONFIG-DRIVEN vendor HTTP egress framework: a new
// HTTP/REST/XML/SOAP SMS vendor is integrated by a JSON *Profile* only — no Go
// code. Templated requests ({placeholders}) + regex extraction handle GET/POST,
// query/form/json/xml bodies, header/query/basic/bearer/session auth, and
// delivery status by webhook or polling. Regex extraction is deliberate: it
// reads a vendor id or status out of XML, JSON, or plain text uniformly. [SCOPE P4]
package httpadapter

import (
	"fmt"
	"net/http"
	"regexp"
)

// Profile is one vendor's complete adapter definition (stored in vendors.adapter_config).
type Profile struct {
	Kind   string            `json:"kind"`   // rest | soap (informational; both use the same driver)
	Vars   map[string]string `json:"vars"`   // static creds/config referenced as {account},{password},{apikey}...
	Auth   *AuthStep         `json:"auth"`   // optional pre-send login; extracted values (e.g. session_id) feed later calls
	Send   Call              `json:"send"`   // the submit request + how to read the vendor message id
	Status *StatusSpec       `json:"status"` // delivery status strategy
	// Bulk (GW-15 slice 1, 2026-08-28): non-nil = the vendor takes CHUNKS of
	// messages per call (Hadal submissionMode BULK); see Adapter.SendBulk.
	Bulk *BulkSpec `json:"bulk,omitempty"`
}

// Call is one templated HTTP request plus how to read its response.
type Call struct {
	Method   string            `json:"method"`   // GET | POST (default GET)
	URL      string            `json:"url"`      // template, e.g. https://host/send.jsp
	Encoding string            `json:"encoding"` // query | form | json | xml (default query)
	Headers  map[string]string `json:"headers"`  // templated header name -> value
	Params   map[string]string `json:"params"`   // templated fields (query string, form body, or json object)
	Body     string            `json:"body"`     // raw body template (for xml/soap or custom json)
	OKMatch  string            `json:"ok_match"` // response must match this regex to count as success ("" = any 2xx)
	Extract  map[string]string `json:"extract"`  // name -> regex(one capture group): msgid on send, session vars on auth, status on poll
	// TextFormat is how the message text is written into the request
	// (2026-09-21, PO: per-endpoint text encoding for carrier APIs):
	//   "" / "plain" — as UTF-8 text (the default, and what it always was);
	//   "ucs2_hex"   — a UCS-2 message as the hex of its UTF-16 (big-endian)
	//                  bytes, e.g. "0633" for س; GSM-7/Latin-1 text stays plain.
	//                  The common "unicode=1&text=<hex>" carrier style.
	//   "hex"        — every message as UTF-16 hex.
	TextFormat string `json:"text_format,omitempty"`

	// ── GW-15 additions (Hadal http_endpoints semantics, 2026-08-27) ──
	// All zero-valued for legacy profiles, whose behavior is unchanged.

	// QueryParams always land on the query string, independent of Encoding —
	// Hadal's sentIn=QUERY on a JSON/XML POST.
	QueryParams map[string]string `json:"query_params,omitempty"`
	// ExtractJSON reads response values by dot-path into decoded JSON
	// ("result.messageId"). Takes precedence over Extract for the same name.
	ExtractJSON map[string]string `json:"extract_json,omitempty"`
	// Transform maps OUR field's value to the vendor's vocabulary before
	// rendering (Hadal valueMapping): field -> {ourValue: theirValue}.
	Transform map[string]map[string]string `json:"transform,omitempty"`
	// ParamMaps (6 Nov 2026): each parameter's OWN value mapping, by its
	// alias (form/query/JSON/XML parameters, query parameters, headers). It
	// changes only that parameter (parammap.go).
	ParamMaps map[string]ParamMap `json:"param_maps,omitempty"`
	// BodyVars: the body template's own variable mappings, adapter field ->
	// {ourValue: theirValue}, applied to the body only.
	BodyVars map[string]map[string]string `json:"body_vars,omitempty"`
	// ExtractMaps (6 Nov 2026, Saleem: "mapping translation for response as
	// well, as we do for request"): a response value's own value mapping, by
	// its name (msgid, status, error_code, a session variable, a rate, mcc…):
	// the vendor's word -> ours, applied as the value is read (extractVal,
	// extractAll). A value not listed is kept as read.
	ExtractMaps map[string]map[string]string `json:"extract_maps,omitempty"`
	// ExtractDefaults (6 Nov 2026, Saleem: "they provide mnc only and I have
	// to add mcc by default"): a response value's default, by its name. With
	// no place in the answer it is a fixed value (always sent back); with one,
	// it fills in when nothing is read there.
	ExtractDefaults map[string]string `json:"extract_defaults,omitempty"`
	// OKCodes: non-empty = the response status must be one of these
	// (successStatusCodes, plural since PO 2026-08-21). Empty = any 2xx.
	OKCodes []int `json:"ok_codes,omitempty"`
	// OKIdents: body values that must ALSO match for success
	// (successAdditionalIdentifiers).
	OKIdents []Ident `json:"ok_idents,omitempty"`
	// RetryCodes: response codes that are TRANSIENT (retryableStatusCodes).
	// When set, a non-success code outside this list is a PERMANENT rejection
	// (ErrPermanent) — the caller should fail the message, not retry it.
	RetryCodes []int `json:"retry_codes,omitempty"`
	// Hosts are failover base URLs tried in order after URL on a transport
	// error (Hadal hosts[]).
	Hosts []string `json:"hosts,omitempty"`

	// ── Endpoint-row settings the screen offers (2026-10-01, QA EP-012,
	// EP-018, SOAP-002). All zero = exactly the old behaviour.

	// ContentType is the Content-Type the request is sent with: the row's
	// own choice — text/xml for SOAP 1.1, application/soap+xml for SOAP 1.2,
	// a +json type — instead of the encoding's generic one. "" = the
	// encoding's (application/json, application/xml, form).
	ContentType string `json:"content_type,omitempty"`
	// TimeoutMS bounds one request to this call (the row's Timeout (ms)).
	// 0 = the caller's client timeout (20 s for a vendor call). On a vendor
	// call it is also capped at that 20 s, inside the queue's 30 s lease
	// (Adapter.do).
	TimeoutMS int `json:"timeout_ms,omitempty"`
	// Retry re-sends a send call at once when the vendor answered one of
	// RetryCodes, or the connection could not even be opened (the row's
	// Retry (JSON)). nil = no in-call retry, as before.
	Retry *RetrySpec `json:"retry,omitempty"`
	// Transport carries the row's TLS settings (TLS (JSON)); nil = the
	// caller's client unchanged. Built by the loader, never stored.
	Transport http.RoundTripper `json:"-"`
}

// Ident is one body assertion: the value at Source (dot-path for JSON, tag
// name for XML/regex fallback) must equal Value.
type Ident struct {
	Source string `json:"source"`
	Value  string `json:"value"`
}

// AuthStep is a pre-send login whose Extract values (e.g. session_id, token)
// are cached for TTLSec and merged into the render context of later calls.
// With a shared SessionStore wired (P4), the extracted vars are ALSO persisted
// per vendor in MySQL so every process/instance using the vendor reuses ONE
// session (PO 2026-08-10) instead of each logging in on its own.
type AuthStep struct {
	Call
	TTLSec int `json:"ttl_sec"` // cache lifetime for the extracted session vars (default 1800)

	// Steps is the multi-call login chain (Telenor-style: call 1 obtains a
	// session, call 2 exchanges it for a token). Each step's Extract values
	// merge into the render context of the NEXT step; the union of all
	// extractions becomes the cached session vars. When set, the embedded
	// single Call above is ignored (kept for existing one-call profiles).
	Steps []Call `json:"steps"`

	// ReauthOn marks a send/status response as "session invalid" by regex on
	// the body — the cached session is dropped (memory AND store), the login
	// re-runs, and the request is retried ONCE. HTTP 401 always triggers this
	// when an auth step is configured, with or without ReauthOn.
	ReauthOn string `json:"reauth_on"`

	// ReauthCodes/ReauthIdents (GW-15): status codes / body assertions that
	// mean "session expired" (sessionExpiredStatusCodes, plural since PO
	// 2026-08-21, + sessionExpiredAdditionalIdentifiers). Either signal drops
	// the shared session, re-logins, and retries once — same path as ReauthOn.
	ReauthCodes  []int   `json:"reauth_codes,omitempty"`
	ReauthIdents []Ident `json:"reauth_idents,omitempty"`

	// Revoke is the vendor's logout/invalidate call (templated with the
	// session vars, e.g. {token}). Fired when a vendor is deactivated/removed
	// (P4 lifecycle) or via Adapter.Revoke; the stored session row is deleted
	// either way so all instances drop it.
	Revoke *Call `json:"revoke"`
}

// StatusSpec is how delivery status is obtained.
type StatusSpec struct {
	Mode string `json:"mode"` // webhook | poll | none
	// Interval is the poll cadence in seconds. 0 = the http-vendor's own
	// back-off per message (first poll at once, then 10 s doubling to 10 min).
	Interval int               `json:"interval_sec"`
	Call     Call              `json:"call"`    // the status-query request (mode=poll); Extract["status"] reads the raw status
	Extract  map[string]string `json:"extract"` // mode=webhook: regexes to read "id" and "status" from the inbound callback
	Map      map[string]string `json:"map"`     // raw vendor status -> canonical (delivered|failed|expired|sent)
}

// Validate rejects a profile whose regexes cannot compile or whose status
// mode is unknown (P4 backlog TC-P4-40, built 2026-08-19). Before this, an
// uncompilable extract regex loaded silently and every extraction just
// returned "" — the vendor "worked" until someone noticed nothing ever got a
// message id. Called at vendor load; a failing profile is skipped LOUDLY.
func (p *Profile) Validate() error {
	// An accepted answer must be STATED, never assumed (PO 2026-09-17): a
	// vendor that always answers 200 and puts its error in the body used to
	// look accepted under the "any 2xx" default, so the message was marked
	// sent and never retried.
	if p.Send.OKMatch == "" && len(p.Send.OKCodes) == 0 && len(p.Send.OKIdents) == 0 {
		return fmt.Errorf("send: no success rule — set ok_match (a body pattern) or ok_codes (status codes) so an accepted answer is stated, not assumed")
	}
	switch p.Send.TextFormat {
	case "", TextPlain, TextUCS2Hex, TextHex:
	default:
		return fmt.Errorf("send: text_format %q: plain, ucs2_hex or hex", p.Send.TextFormat)
	}
	checkRe := func(where, pattern string) error {
		if pattern == "" {
			return nil
		}
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("%s: bad regex %q: %v", where, pattern, err)
		}
		return nil
	}
	// An extraction yields its value from capture group 1, so a pattern
	// without one can never read anything. For the two values every message
	// depends on — the send's message id and the poll's status — say so at
	// load instead of failing every message with "no message id extracted"
	// (QA SOAP-001). Any other extraction only has to compile, as before.
	needsGroup := func(where, name string) bool {
		return (where == "send" && name == "msgid") || (where == "status.call" && name == "status")
	}
	checkCall := func(where string, c *Call) error {
		if c == nil {
			return nil
		}
		if err := checkRe(where+".ok_match", c.OKMatch); err != nil {
			return err
		}
		for name, re := range c.Extract {
			if err := checkRe(where+".extract."+name, re); err != nil {
				return err
			}
			if re != "" && needsGroup(where, name) && regexp.MustCompile(re).NumSubexp() < 1 {
				return fmt.Errorf("%s.extract.%s: regex %q has no capture group — wrap the value in ( )", where, name, re)
			}
		}
		return nil
	}
	if err := checkCall("send", &p.Send); err != nil {
		return err
	}
	if a := p.Auth; a != nil {
		if err := checkCall("auth", &a.Call); err != nil {
			return err
		}
		for i := range a.Steps {
			if err := checkCall(fmt.Sprintf("auth.steps[%d]", i), &a.Steps[i]); err != nil {
				return err
			}
		}
		if err := checkRe("auth.reauth_on", a.ReauthOn); err != nil {
			return err
		}
		if err := checkCall("auth.revoke", a.Revoke); err != nil {
			return err
		}
	}
	if s := p.Status; s != nil {
		switch s.Mode {
		case "", "webhook", "poll", "none":
		default:
			return fmt.Errorf("status.mode %q must be webhook, poll, or none", s.Mode)
		}
		if err := checkCall("status.call", &s.Call); err != nil {
			return err
		}
		for name, re := range s.Extract {
			if err := checkRe("status.extract."+name, re); err != nil {
				return err
			}
		}
	}
	return nil
}
