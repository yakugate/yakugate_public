package httpadapter

// GW-15 convergence (PO 2026-08-20/21, built 2026-08-27): hydrate the adapter's
// Profile from http_endpoints rows instead of a hand-written vendors.
// adapter_config blob. The portal saves an HTTP vendor as
//
//	adapter_config = {"endpoints": {"send": <row id>, "poll": <row id>}}
//
// and each referenced row carries the Hadal request/response semantics in its
// config JSON (params[] with source/alias/sentIn/defaultValue/valueMapping/
// isSessionVariable, plural success/retryable/session-expired status codes,
// body identifiers, fixed response-alias vocabulary, hosts[] failover, bulk
// submission). The login call is its OWN row, referenced by the send/poll
// row's config.session_endpoint_id — one definition shared per owner, exactly
// the D-103 one-shared-session-per-vendor model.
//
// The same builder serves the provider-style callers (crp rate requests, HLR
// dips — Phase B) via CallFromEndpoint + FetchValues.

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/platform/outhttp"
)

// EndpointRow is one http_endpoints row, decoded by the caller (any store).
type EndpointRow struct {
	ID      int64
	Name    string
	URL     string
	Method  string
	Headers map[string]string // native headers column
	Auth    map[string]any    // native auth column: {"type":"basic"|"bearer","username","password","token","vars":{...}}
	// BodyTemplate is the body_template column: the request body with
	// {source} placeholders (the same source names the parameters use). Until
	// 2026-09-14 no loader read it, so a SOAP-wizard envelope or a KMS body was
	// saved and never sent.
	BodyTemplate string
	// TLS is the tls column: {"insecure_skip_verify","server_name","ca_ref",
	// "cert_ref","key_ref"} for the endpoint's HTTPS connection (see TLSConfig).
	TLS map[string]any
	// TLSConfig is TLS already built (secrets resolved) by a loader that has
	// a secrets store — the http-vendor role (QA EP-018). nil = system
	// defaults. The builder cannot resolve secrets itself, so it never reads
	// the TLS map; pushcall builds its own from it.
	TLSConfig *tls.Config
	// Retry is the retry column (ParseRetry), read for vendor calls.
	Retry       map[string]any
	TimeoutMS   int
	CacheTTLSec int
	Config      map[string]any // the Hadal config JSON
}

// hadal* mirror the portal-written config shape (round-trip verified 2026-08-20).
type hadalParam struct {
	Alias             string            `json:"alias"`
	SentIn            string            `json:"sentIn"` // QUERY | HEADERS | BODY
	Source            string            `json:"source"`
	DefaultValue      string            `json:"defaultValue"`
	IsSessionVariable bool              `json:"isSessionVariable"`
	ValueMapping      map[string]string `json:"valueMapping"`
	// VarMappings (6 Nov 2026): inside a value written with {variables},
	// a mapping for each chosen variable, keyed by its source name. With
	// ValueMapping (the whole value) they are this parameter's own mapping
	// and change nothing else in the call (ParamMap).
	VarMappings map[string]map[string]string `json:"varMappings"`
}

type hadalIdent struct {
	Source string `json:"source"`
	Value  string `json:"value"`
}

type hadalResponse struct {
	Type                                string       `json:"type"` // json | xml
	Params                              []hadalParam `json:"params"`
	SuccessStatusCodes                  []int        `json:"successStatusCodes"`
	SuccessStatusCode                   int          `json:"successStatusCode"` // legacy singular = one-element array
	RetryableStatusCodes                []int        `json:"retryableStatusCodes"`
	SessionExpiredStatusCodes           []int        `json:"sessionExpiredStatusCodes"`
	SessionExpiredStatusCode            int          `json:"sessionExpiredStatusCode"` // legacy singular
	SuccessAdditionalIdentifiers        []hadalIdent `json:"successAdditionalIdentifiers"`
	SessionExpiredAdditionalIdentifiers []hadalIdent `json:"sessionExpiredAdditionalIdentifiers"`
}

type hadalConfig struct {
	ContentType       string        `json:"contentType"`
	Params            []hadalParam  `json:"params"`
	Response          hadalResponse `json:"response"`
	SubmissionMode    string        `json:"submissionMode"` // SINGLE | BULK
	BulkSubmission    *hadalBulk    `json:"bulkSubmission"`
	RequiresSessionID bool          `json:"requiresSessionId"`
	SessionEndpointID int64         `json:"session_endpoint_id"`
	RequestEndpointID int64         `json:"request_endpoint_id"` // crp provider -> its rate request row
	Hosts             []string      `json:"hosts"`
	// StatusMap (GW-15 slice 3, 2026-08-28): raw vendor status -> canonical
	// (delivered|failed|expired|sent) on poll/webhook rows — the endpoint-model
	// twin of the legacy Profile's status.map.
	StatusMap map[string]string `json:"statusMap"`
	// TextFormat: how the text is written into this call — plain (default),
	// ucs2_hex or hex (Call.TextFormat).
	TextFormat string `json:"textFormat"`
	// IntervalSec on a poll row: how often one message may be polled. 0 =
	// the http-vendor's back-off (first poll at once, then 10 s doubling to
	// 10 min). Raw JSON only today; the Builder has no field for it.
	IntervalSec     int `json:"interval_sec"`
	PollIntervalSec int `json:"pollIntervalSec"`
	// BodyVarMappings (6 Nov 2026): the body template's own mapping for each
	// chosen {variable}, keyed by its source name; it changes the body only.
	// (A call-wide "translate" tried the same morning changed a value in
	// every parameter at once; it is gone.)
	BodyVarMappings map[string]map[string]string `json:"bodyVarMappings"`
}

// hadalBulk mirrors Hadal's bulkSubmission block (GW-15 slice 1).
type hadalBulk struct {
	ValueType           string `json:"valueType"`
	ToSeparator         string `json:"toSeparator"`
	OperatorIDSeparator string `json:"operatorIdSeparator"`
	ChunkQuantity       any    `json:"chunkQuantity"` // the portal writes strings ("50"); tolerate numbers too
	ProvidesSingleID    bool   `json:"providesSingleId"`
	TPSCountMode        string `json:"tpsCountMode"`
}

// BulkSpec is the adapter-side bulk plan hydrated from hadalBulk.
type BulkSpec struct {
	ToSeparator string // joins recipients into the `to` field (default ",")
	IDSeparator string // splits the response's operator_id list (default ",")
	ChunkSize   int    // max messages per vendor call (default 50)
	SingleID    bool   // vendor returns ONE id for the whole chunk
	// Array (Bulk Value Type "Array", "a native JSON array"): the recipients
	// go as a list — a JSON array in a JSON body, the parameter repeated in a
	// query or form, the element repeated in XML — and the ids may come back
	// as a list. Until 2026-10-01 the choice was saved and ignored.
	Array bool `json:"array,omitempty"`
	// CountMessages (TPS Count Mode "Total messages"): every message of a
	// chunk spends one unit of the vendor's rate, not the call as a whole.
	CountMessages bool `json:"count_messages,omitempty"`
}

func (h *hadalBulk) spec() *BulkSpec {
	b := &BulkSpec{ToSeparator: ",", IDSeparator: ",", ChunkSize: 50}
	if h == nil {
		return b
	}
	b.Array = strings.EqualFold(strings.TrimSpace(h.ValueType), "ARRAY")
	b.CountMessages = strings.EqualFold(strings.TrimSpace(h.TPSCountMode), "TOTAL_MESSAGES")
	if h.ToSeparator != "" {
		b.ToSeparator = h.ToSeparator
	}
	if h.OperatorIDSeparator != "" {
		b.IDSeparator = h.OperatorIDSeparator
	}
	switch v := h.ChunkQuantity.(type) {
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
			b.ChunkSize = n
		}
	case float64:
		if v > 0 {
			b.ChunkSize = int(v)
		}
	}
	b.SingleID = h.ProvidesSingleID
	return b
}

func decodeHadal(cfg map[string]any) (hadalConfig, error) {
	var h hadalConfig
	b, err := json.Marshal(cfg)
	if err != nil {
		return h, err
	}
	err = json.Unmarshal(b, &h)
	return h, err
}

// sourceTemplates maps the fixed per-role source vocabulary (the portal's
// SOURCE_STRUCTURES) onto the adapter's render-context keys.
var sourceTemplates = map[string]string{
	"to": "to", "text": "text", "from": "from", "message_id": "msgid",
	"priority": "priority", "encoding": "encoding", "segments": "segments",
	"hlr.mcc": "mcc", "hlr.mnc": "mnc",
	"ap1": "ap1", "ap2": "ap2", "apg1": "apg1", "apg2": "apg2",
	"session_id": "session_id",
	// poll: the id to query is the vendor's own message id
	"operator_id": "vendor_msg_id",
	// crp / hlr provider calls
	"from_currency": "from_currency", "to_currency": "to_currency",
	"msisdn": "msisdn",
	// kms encrypt / decrypt calls (2026-09-13): the master key's name, the
	// data key to lock, the locked data key to unlock
	"key": "key", "plaintext": "plaintext", "ciphertext": "ciphertext",
	// customer webhooks (2026-09-14): a receipt's status, raw receipt text,
	// error code and the client's own reference; an MO uses to/from/text and
	// message_id from the list above
	"reference": "reference", "status": "status", "receipt": "receipt", "error_code": "error_code",
	// and (2026-10-15) the receipt's SMPP word and its two times, RFC 3339 UTC:
	// when the message was accepted and when this state was reached
	"stat": "stat", "submitted_at": "submitted_at", "done_at": "done_at",
	// and (2026-11-04) when an incoming message was taken in, RFC 3339 UTC
	"accepted_at": "accepted_at",
	// and (2026-11-04) an incoming message's network and vendor: hlr.mcc and
	// hlr.mnc above, MCC+MNC together, whether the number is ported ("true",
	// "false", "" not known), the vendor's cid and name
	"hlr.operator": "operator", "hlr.ported": "ported",
	"vendor.cid": "vendor", "vendor.name": "vendor_name",
}

// tmplFor renders one request param's value template. A missing/unknown source
// with a defaultValue becomes the literal default; a known source renders live.
func tmplFor(p hadalParam, role string) string {
	src := p.Source
	if p.IsSessionVariable {
		src = "session_id"
	}
	if role == "poll" && src == "message_id" {
		src = "operator_id" // poll queries by the VENDOR id, not ours
	}
	if key, ok := sourceTemplates[src]; ok && src != "" {
		return "{" + key + "}"
	}
	// A default written with {variables} reads the same names a source or
	// the URL does ({message_id} inside Jazz's xmldoc was sent as written).
	return toAdapterKeys(p.DefaultValue, role)
}

// encodingFor derives the wire encoding from the row's contentType.
func encodingFor(contentType, method string) string {
	ct := strings.ToLower(contentType)
	switch {
	case strings.Contains(ct, "json"):
		return "json"
	case strings.Contains(ct, "xml"):
		return "xml"
	case strings.Contains(ct, "x-www-form-urlencoded"):
		return "form"
	case strings.EqualFold(method, "POST") && ct == "":
		return "form"
	}
	return "query"
}

// tagPathRe is a plain XML path ("messageId", "Body.SendSmsResponse.id",
// "ns:messageId") as opposed to a regular expression.
var tagPathRe = regexp.MustCompile(`^[A-Za-z_][\w.\-:]*$`)

// xmlSourceRegex is how one XML response source is read (QA SOAP-001).
//
// The screen tells the operator an XML source is "a regex with ONE capture
// group", and the SOAP wizard writes exactly that; the engine used to take
// every source as a tag name and quote it, so a wizard-made endpoint never
// read a message id. Now a source that is a plain path is read as before —
// its LAST segment as the tag, with any namespace prefix and attributes
// allowed — and anything else is used as the regex it is.
func xmlSourceRegex(src string) string {
	s := strings.TrimSpace(src)
	if s == "" {
		return ""
	}
	if !tagPathRe.MatchString(s) {
		if re, err := regexp.Compile(s); err == nil && re.NumSubexp() >= 1 {
			return s
		}
		// Neither a path nor a usable regex ("Status/@code"): read as the
		// old code did — a tag of that name, which matches nothing — so a
		// row with one odd extra value still loads.
		return "<" + regexp.QuoteMeta(s) + `>\s*(.+?)\s*</` + regexp.QuoteMeta(s) + ">"
	}
	seg := s
	if i := strings.LastIndexByte(seg, '.'); i >= 0 {
		seg = seg[i+1:]
	}
	if i := strings.LastIndexByte(seg, ':'); i >= 0 {
		seg = seg[i+1:] // "ns:messageId": any prefix (or none) matches
	}
	q := regexp.QuoteMeta(seg)
	return `<(?:[\w.\-]+:)?` + q + `(?:\s[^>]*)?>\s*(.+?)\s*</(?:[\w.\-]+:)?` + q + `>`
}

// xmlSourceReads reports whether an XML response source can read a value at
// all: a tag path, or a regex with a capture group. xmlSourceRegex turns
// anything else into a tag pattern that matches nothing, so the row loads.
func xmlSourceReads(src string) bool {
	s := strings.TrimSpace(src)
	if tagPathRe.MatchString(s) {
		return true
	}
	re, err := regexp.Compile(s)
	return err == nil && s != "" && re.NumSubexp() >= 1
}

// toAdapterKeys turns a row's {source} placeholders (the names the screen
// offers: {message_id}, {operator_id}, …) into the adapter's render keys, in
// the URL and failover hosts as well as the body template (QA EP-022: a poll
// URL with {msgid} or {operator_id} went out with the id empty). In a poll
// call {message_id} is the VENDOR's id, as it is for a parameter.
func toAdapterKeys(tmpl, role string) string {
	return phRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		src := m[1 : len(m)-1]
		if role == "poll" && src == "message_id" {
			src = "operator_id"
		}
		if key, ok := sourceTemplates[src]; ok {
			return "{" + key + "}"
		}
		return m
	})
}

// bodyless reports a method whose request carries no body: its parameters
// go on the query string (QA EP-009; Hadal did the same). Many servers and
// proxies drop a GET body.
func bodyless(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case "", http.MethodGet, http.MethodHead, http.MethodDelete:
		return true
	}
	return false
}

// tlsTransport is one endpoint's connection pool with its TLS settings.
// Idle connections are closed after a while so a transport left behind by a
// reload does not keep sockets open.
//
// It keeps as many idle connections a host as the shared pool does (outhttp:
// 512, it was 16): a TLS handshake a message is the dearest way to send one.
func tlsTransport(tc *tls.Config) *http.Transport {
	return outhttp.New(tc)
}

// wireContentType is the Content-Type a row's own choice puts on the wire
// (QA SOAP-002: the vendor was always sent application/xml). text/xml (SOAP
// 1.1) and application/soap+xml (SOAP 1.2) get "; charset=utf-8" when the
// row names no charset; for SOAP 1.2 the SOAPAction header becomes the
// action parameter of the content type, which is where 1.2 carries it.
func wireContentType(ct string, headers map[string]string) string {
	low := strings.ToLower(ct)
	soap12 := strings.Contains(low, "soap+xml")
	if (strings.HasPrefix(low, "text/xml") || soap12) && !strings.Contains(low, "charset") {
		ct += "; charset=utf-8"
	}
	if soap12 {
		for k, v := range headers {
			if !strings.EqualFold(k, "SOAPAction") {
				continue
			}
			delete(headers, k)
			if a := strings.Trim(strings.TrimSpace(v), `"`); a != "" && !strings.Contains(low, "action=") {
				ct += `; action="` + a + `"`
			}
		}
	}
	return ct
}

// callFromRow builds one Call from an endpoint row. role: send | poll | login |
// crp | hlr | kms. wantAliases maps a response alias to the adapter extraction name
// it should land under (e.g. operator_id -> msgid on send).
func callFromRow(row EndpointRow, role string, wantAliases map[string]string) (Call, hadalConfig, error) {
	h, err := decodeHadal(row.Config)
	if err != nil {
		return Call{}, h, fmt.Errorf("endpoint %d (%s): bad config: %w", row.ID, row.Name, err)
	}
	call := Call{
		Method: row.Method,
		// The URL's {source} placeholders read the same names as the
		// parameters do (QA EP-022).
		URL:      toAdapterKeys(row.URL, role),
		Encoding: encodingFor(h.ContentType, row.Method),
		Headers:  map[string]string{},
		Params:   map[string]string{},
		// Lower-cased here; an unknown value is refused by Profile.Validate.
		TextFormat: strings.ToLower(strings.TrimSpace(h.TextFormat)),
		// The row's Timeout (ms) bounds each request (QA EP-012); it was
		// read and never applied, so every vendor waited the fixed 20 s.
		TimeoutMS: row.TimeoutMS,
	}
	for _, host := range h.Hosts {
		call.Hosts = append(call.Hosts, toAdapterKeys(host, role))
	}
	for k, v := range row.Headers {
		call.Headers[k] = v
	}
	// The row's own body, with its {source} placeholders turned into the
	// adapter's field names. A body needs a body-carrying encoding: a row
	// that set a template but no content type gets JSON, or XML when the
	// template starts with "<".
	if tb := strings.TrimSpace(row.BodyTemplate); tb != "" {
		call.Body = toAdapterKeys(row.BodyTemplate, role)
		if call.Encoding == "query" || call.Encoding == "form" {
			if strings.HasPrefix(tb, "<") {
				call.Encoding = "xml"
			} else {
				call.Encoding = "json"
			}
		}
	} else if bodyless(row.Method) {
		// GET, HEAD and DELETE carry no body: their parameters go on the
		// query string whatever the content type (QA EP-009) — the JSON or
		// XML body they used to get is dropped by many servers and proxies.
		call.Encoding = "query"
	}
	// The row's own content type goes on the wire when it fits the body
	// (QA SOAP-002).
	if ct := strings.TrimSpace(h.ContentType); ct != "" && call.Encoding != "query" &&
		encodingFor(ct, http.MethodPost) == call.Encoding {
		call.ContentType = wireContentType(ct, call.Headers)
	}
	// TLS (JSON), built by the loader with secrets resolved (QA EP-018).
	if row.TLSConfig != nil {
		call.Transport = tlsTransport(row.TLSConfig)
	}
	call.Retry = ParseRetry(row.Retry)
	// native auth column: basic/bearer become an Authorization header
	if t, _ := row.Auth["type"].(string); t != "" {
		switch strings.ToLower(t) {
		case "basic":
			u, _ := row.Auth["username"].(string)
			p, _ := row.Auth["password"].(string)
			call.Headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(u+":"+p))
		case "bearer":
			tok, _ := row.Auth["token"].(string)
			call.Headers["Authorization"] = "Bearer " + tok
		}
	}
	// request params by sentIn
	for _, p := range h.Params {
		tmpl := tmplFor(p, role)
		switch strings.ToUpper(p.SentIn) {
		case "HEADERS":
			call.Headers[p.Alias] = tmpl
		case "QUERY":
			if call.Encoding == "query" {
				call.Params[p.Alias] = tmpl
			} else {
				if call.QueryParams == nil {
					call.QueryParams = map[string]string{}
				}
				call.QueryParams[p.Alias] = tmpl
			}
		default: // BODY (and unset)
			call.Params[p.Alias] = tmpl
		}
		// The parameter's OWN mapping (6 Nov 2026): it changes this
		// parameter only. It used to be laid over the whole call.
		pm := ParamMap{Vars: adapterVars(p.VarMappings, role)}
		if len(p.ValueMapping) > 0 {
			pm.Whole = p.ValueMapping
		}
		if len(pm.Whole) > 0 || len(pm.Vars) > 0 {
			if call.ParamMaps == nil {
				call.ParamMaps = map[string]ParamMap{}
			}
			call.ParamMaps[p.Alias] = pm
		}
	}
	call.BodyVars = adapterVars(h.BodyVarMappings, role)
	// response extraction: fixed alias vocabulary -> adapter names
	for _, rp := range h.Response.Params {
		name := rp.Alias
		if mapped, ok := wantAliases[rp.Alias]; ok {
			name = mapped
		}
		// The value's own mapping: the vendor's word -> ours (6 Nov 2026).
		if len(rp.ValueMapping) > 0 {
			if call.ExtractMaps == nil {
				call.ExtractMaps = map[string]map[string]string{}
			}
			call.ExtractMaps[name] = rp.ValueMapping
		}
		// Its default (6 Nov 2026): used when nothing is read; with no place
		// in the answer it is a fixed value. The send's message id and the
		// poll's status are read from every answer, never fixed: one id for
		// every message would match every receipt to the wrong message.
		if rp.DefaultValue != "" {
			if call.ExtractDefaults == nil {
				call.ExtractDefaults = map[string]string{}
			}
			call.ExtractDefaults[name] = rp.DefaultValue
		}
		if strings.TrimSpace(rp.Source) == "" {
			if (role == "send" && name == "msgid") || (role == "poll" && name == "status") {
				return Call{}, h, fmt.Errorf("endpoint %d (%s): response value %s needs a place in the answer, not only a default",
					row.ID, row.Name, rp.Alias)
			}
			continue // a fixed value: nothing to read
		}
		if strings.EqualFold(h.Response.Type, "xml") {
			// The send's message id and the poll's status are what every
			// message depends on. A source that can read neither (a regex
			// with no capture group, one that does not compile) would read
			// nothing on every answer: the vendor takes the message, we see
			// "no message id extracted" and hand it to the next vendor — the
			// handset gets it twice. Refused at load instead, with the reason.
			if ((role == "send" && name == "msgid") || (role == "poll" && name == "status")) && !xmlSourceReads(rp.Source) {
				return Call{}, h, fmt.Errorf("endpoint %d (%s): response value %q for %s is neither a tag name nor a regex with one capture group ( )",
					row.ID, row.Name, rp.Source, rp.Alias)
			}
			if call.Extract == nil {
				call.Extract = map[string]string{}
			}
			call.Extract[name] = xmlSourceRegex(rp.Source)
		} else {
			if call.ExtractJSON == nil {
				call.ExtractJSON = map[string]string{}
			}
			call.ExtractJSON[name] = rp.Source
		}
	}
	// success / retry semantics (plural since PO 2026-08-21; legacy singular = one-element)
	call.OKCodes = h.Response.SuccessStatusCodes
	if len(call.OKCodes) == 0 && h.Response.SuccessStatusCode != 0 {
		call.OKCodes = []int{h.Response.SuccessStatusCode}
	}
	call.RetryCodes = h.Response.RetryableStatusCodes
	for _, id := range h.Response.SuccessAdditionalIdentifiers {
		call.OKIdents = append(call.OKIdents, Ident(id))
	}
	return call, h, nil
}

// ProfileFromEndpoints assembles a full vendor Profile from the referenced
// rows. login/poll may be nil. statusMap translates the poll's raw status into
// canonical states (nil = lowercase passthrough).
func ProfileFromEndpoints(send EndpointRow, login, poll *EndpointRow, statusMap map[string]string) (Profile, error) {
	sendCall, sendCfg, err := callFromRow(send, "send", map[string]string{"operator_id": "msgid"})
	if err != nil {
		return Profile{}, err
	}
	// An accepted answer must be stated (PO 2026-09-17), the same rule
	// Profile.Validate applies to an adapter_config profile.
	if sendCall.OKMatch == "" && len(sendCall.OKCodes) == 0 && len(sendCall.OKIdents) == 0 {
		return Profile{}, fmt.Errorf("send call %q (#%d) states no success rule: on HTTP Endpoints set its success status codes or a success identifier", send.Name, send.ID)
	}
	p := Profile{Kind: "rest", Send: sendCall}

	if login != nil {
		loginCall, _, err := callFromRow(*login, "login", nil) // session vars keep their alias names
		if err != nil {
			return Profile{}, err
		}
		ttl := login.CacheTTLSec
		auth := &AuthStep{Call: loginCall, TTLSec: ttl}
		auth.ReauthCodes = sendCfg.Response.SessionExpiredStatusCodes
		if len(auth.ReauthCodes) == 0 && sendCfg.Response.SessionExpiredStatusCode != 0 {
			auth.ReauthCodes = []int{sendCfg.Response.SessionExpiredStatusCode}
		}
		for _, id := range sendCfg.Response.SessionExpiredAdditionalIdentifiers {
			auth.ReauthIdents = append(auth.ReauthIdents, Ident(id))
		}
		p.Auth = auth
	} else if sendCfg.RequiresSessionID || sendCfg.SessionEndpointID != 0 {
		return Profile{}, fmt.Errorf("send endpoint %d requires a session but its login row was not resolved", send.ID)
	}

	if poll != nil {
		pollCall, pollCfg, err := callFromRow(*poll, "poll", nil)
		if err != nil {
			return Profile{}, err
		}
		m := statusMap
		if len(pollCfg.StatusMap) > 0 {
			m = pollCfg.StatusMap // the row's own vocabulary wins (GW-15 slice 3)
		}
		iv := pollCfg.IntervalSec
		if iv <= 0 {
			iv = pollCfg.PollIntervalSec
		}
		p.Status = &StatusSpec{Mode: "poll", Call: pollCall, Map: m, Interval: max(iv, 0)}
	}
	// GW-15 slice 1 (2026-08-28): BULK submission is live — P4 batches
	// same-(sender,body) singles into chunks and calls SendBulk.
	if strings.EqualFold(sendCfg.SubmissionMode, "BULK") {
		p.Bulk = sendCfg.BulkSubmission.spec()
	}
	return p, nil
}

// CallFromEndpoint builds a one-shot provider call from a row. role selects
// the source vocabulary: "crp" (from_currency/to_currency) or "hlr" (msisdn).
func CallFromEndpoint(row EndpointRow, role string) (Call, error) {
	c, _, err := callFromRow(row, role, nil)
	return c, err
}

// FetchValues executes one provider-style call (crp rate request, HLR dip) and
// returns every extracted value. Success is judged by the call's own GW-15
// semantics (OKCodes/OKIdents, else 2xx).
func FetchValues(ctx context.Context, cl *http.Client, call Call, fields map[string]string) (map[string]string, error) {
	if cl == nil {
		cl = outhttp.Client(10 * time.Second)
	}
	body, code, err := doCall(ctx, cl, call, fields)
	if err != nil {
		return nil, err
	}
	if ok, cErr := classify(call, body, code); !ok {
		return nil, cErr
	} else if len(call.OKCodes) == 0 && code >= 400 {
		return nil, fmt.Errorf("http %d: %.160s", code, body)
	}
	out := map[string]string{}
	for _, name := range call.readNames() { // read values and fixed ones
		out[name] = extractVal(call, name, body)
	}
	return out, nil
}
