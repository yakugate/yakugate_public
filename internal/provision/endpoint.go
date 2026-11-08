package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// An HTTP endpoint is one configured call: a vendor's send, its status poll,
// its login, or a customer's receipt or incoming-message webhook. The portal
// stores these as `http_endpoints` rows with a builder-shaped `config` JSON,
// and the engine hydrates a vendor's profile from them (GW-15). The console
// writes exactly the same rows — one database, two doors.
//
// NOBODY SHOULD HAVE TO KNOW THE JSON. [SCOPE P14-ENDPOINT]
//
// The first version of this asked an operator to paste an adapter profile,
// which assumes they know a shape they could only have learned from the
// portal — the very thing a gateway-only install does not have. So the pieces
// are described here, field by field, with the vocabulary each one accepts,
// and the console asks for them one at a time.

// EndpointRole is what a call is for. The row's `name` carries it, because
// that is how the engine finds a vendor's send call.
type EndpointRole struct {
	Name string // send | poll | login (webhook: retired, issue 19)
	What string // what it is for, in a sentence
}

// VendorRoles are the calls a vendor can have. The "webhook" role — a call
// that only said how the carrier's receipt post is read — is retired (issue
// 19, 2026-10-27): a vendor's receipts are read by its Inbound DLR door, with
// a DLR dialect or the default reading. Calls made before keep their role and
// are read by nothing.
func VendorRoles() []EndpointRole {
	return []EndpointRole{
		{"send", "submits a message to the carrier. Every vendor needs one."},
		{"poll", "asks the carrier what happened to a message, when it does not call us back."},
		{"login", "signs in first and hands a session to the send call."},
	}
}

// WebhookRetired is what both doors say to a receipt "webhook" — the role
// of a new call, or the vendor slot that pointed at one (issue 19). The Admin
// API says the same (services/vendorCalls.js, services/endpointRules.js).
const WebhookRetired = "the DLR webhook is retired: a vendor's receipts are read by its Inbound DLR door " +
	"(portal: Vendors → Inbounds) — with a DLR dialect, or the usual field names when it has none"

// AccountWebhookTypes are the calls a CUSTOMER can have pointed at them.
func AccountWebhookTypes() []EndpointRole {
	return []EndpointRole{
		{"dlr", "pushes delivery receipts to the customer's own URL."},
		{"mo", "pushes incoming messages to the customer's own URL."},
	}
}

// Source is one value the gateway can put into a field, named the way the
// portal names it, with what it means in plain words.
type Source struct {
	Key  string
	What string
}

// SourcesFor is the vocabulary a call of this kind may draw on. Offered as a
// list so an operator picks rather than guesses — the whole point of the
// rewrite (PO 2026-09-18: "how can anyone know fields in an json").
func SourcesFor(kind string) []Source {
	switch kind {
	case "send":
		return []Source{
			{"to", "the destination number"},
			{"text", "the message text"},
			{"from", "the sender id"},
			{"message_id", "our own id for the message"},
			{"segments", "how many parts the message is"},
			{"encoding", "gsm7 or ucs2"},
			{"priority", "1 (most urgent) to 9"},
			{"hlr.mcc", "the network's country code, if known"},
			{"hlr.mnc", "the network code, if known"},
			// session_id is not offered: the console gives a call no
			// session (no login link), so it would always be sent empty.
			{"ap1", "pass-through field 1"},
			{"ap2", "pass-through field 2"},
		}
	case "poll":
		return []Source{{"operator_id", "the carrier's own message id"}}
	case "dlr":
		return []Source{
			{"message_id", "our id for the message"},
			{"status", "delivered, failed, expired"},
			{"error_code", "the carrier's error code, if any"},
			{"receipt", "the receipt text as the carrier wrote it"},
			{"reference", "the customer's own id for the message"},
			{"to", "the destination number"},
			{"from", "the sender id"},
		}
	case "mo":
		return []Source{
			{"message_id", "our id for the incoming message"},
			{"from", "the number that sent it"},
			{"to", "the short code it came to"},
			{"text", "the message text"},
		}
	case "login":
		return []Source{}
	}
	return nil
}

// Encodings is how a call carries its fields.
func Encodings() []string { return []string{"query", "form", "json", "xml"} }

// ResponseTypes is how the answer is read. "text" used to be offered too, but
// the engine reads every answer that is not xml as JSON paths, so a text
// answer read nothing; a plain-text answer is read as xml with a pattern
// that has ( ) around the value (e.g. OK:(\d+)).
func ResponseTypes() []string { return []string{"xml", "json"} }

// SentIns is where one field travels.
func SentIns() []string { return []string{"QUERY", "BODY"} }

// Param is one field of a request: the vendor's name for it, and what fills it.
type Param struct {
	Alias        string // the VENDOR's name for the field, e.g. "mobilenum"
	Source       string // which of our values fills it, e.g. "to"; empty = a fixed value
	SentIn       string // QUERY | BODY
	DefaultValue string // used when Source is empty: a fixed value, or {{secret:env://NAME}}
}

// ResponseParam reads one value out of the answer, e.g. the carrier's message
// id: alias "operator_id" (what WE call it) from source "messageId" (what the
// carrier calls it).
type ResponseParam struct {
	Alias  string
	Source string
}

// SuccessIdentifier is a value in the body that must be present for the call
// to count as accepted — the other half of "how do we know it worked".
type SuccessIdentifier struct {
	Source string
	Value  string
}

// Endpoint is a configured call, in the portal's own shape.
type Endpoint struct {
	Type      string // vendor | dlr | mo
	Name      string // send | poll | login | webhook (for a vendor)
	VendorID  int64  // vendor calls: whose it is (0 = shared)
	AccountID int64  // dlr/mo webhooks: whose customer it is
	URL       string
	Method    string
	Encoding  string // query | form | json | xml
	TimeoutMS int
	// TextFormat is how the message text goes into the call: plain (UTF-8,
	// the default), ucs2_hex (UCS-2 messages as UTF-16 hex) or hex.
	TextFormat string

	Params   []Param
	Response struct {
		Type       string
		Params     []ResponseParam
		OKStatus   []int
		OKContains []SuccessIdentifier
	}
}

// TextFormats are the ways the text can be written into a call.
func TextFormats() []string { return []string{"plain", "ucs2_hex", "hex"} }

// The Admin API's rules for an http_endpoints row (p13-backend/src/services/
// endpointRules.js, mirrored by the portal's endpointRules.js) — CHANGE
// TOGETHER. "One database, two doors": nothing here may be looser. The
// console is deliberately narrower in places: it sends GET, POST or PUT only,
// fields travel in the query or the body (no header fields), and it cannot
// write a body template, so it builds no SOAP call.
const (
	EndpointAliasMax       = 64
	EndpointMaxTimeoutMS   = 300000
	DefaultEndpointTimeout = 15000
	webhookPushRate        = 200 // a customer webhook's pushes per second (the Admin API's create default)
	webhookPushConcurrency = 4   // and pushes at once
)

var (
	// A query or body field name: what vendor APIs actually use, nothing that
	// breaks a query string, a JSON key or an XML element.
	endpointAliasRE = regexp.MustCompile(`^[A-Za-z0-9_.\-:\[\]@$~]+$`)
	// http(s), a host, no spaces anywhere; {placeholders} are fine.
	endpointURLRE = regexp.MustCompile(`(?i)^https?://[^\s/?#]+\S*$`)
	// A plain XML tag path, as the engine's tagPathRe.
	xmlTagPathRE = regexp.MustCompile(`^[A-Za-z_][\w.\-:]*$`)
)

// EndpointURLProblem says why u is not a web address the gateway can call,
// or "" when it is.
func EndpointURLProblem(u string) string {
	if endpointURLRE.MatchString(strings.TrimSpace(u)) {
		return ""
	}
	return fmt.Sprintf("%q is not a web address: it must begin http:// or https://, name a host, and have no spaces", strings.TrimSpace(u))
}

// XMLSourceReads reports whether a value can be read from an XML answer at
// src: a tag path, or a regex with a capture group — the engine's own test
// (httpadapter xmlSourceReads). For the send call's message id and the
// poll's status the engine will not load a call that fails it.
func XMLSourceReads(src string) bool {
	s := strings.TrimSpace(src)
	if xmlTagPathRE.MatchString(s) {
		return true
	}
	re, err := regexp.Compile(s)
	return err == nil && s != "" && re.NumSubexp() >= 1
}

// sentInOf is where a field really travels: its own choice, else what the
// console writes for it (endpointConfig).
func sentInOf(e Endpoint, p Param) string {
	if p.SentIn != "" {
		return strings.ToUpper(p.SentIn)
	}
	if e.Encoding != "query" && e.Method != "GET" {
		return "BODY"
	}
	return "QUERY"
}

// ValidateEndpoint checks a built call the way the engine will read it. Every
// refusal names the field an operator just answered.
func ValidateEndpoint(e Endpoint) error {
	if e.TextFormat != "" && !oneOf(e.TextFormat, TextFormats()) {
		return refusef("the text is written as plain, ucs2_hex or hex, not %q", e.TextFormat)
	}
	if !oneOf(e.Type, []string{"vendor", "dlr", "mo"}) {
		return refusef("an endpoint is for a vendor, or is a dlr or mo webhook; %q is none of those", e.Type)
	}
	name := strings.TrimSpace(e.Name)
	if name == "" {
		return refusef("the call needs a role: send, poll or login")
	}
	if e.Type == "vendor" && strings.EqualFold(name, "webhook") {
		return refusef("%s", WebhookRetired)
	}
	if e.Type == "vendor" && !oneOf(strings.ToLower(name), []string{"send", "poll", "login"}) {
		return refusef("a vendor call is send, poll or login, not %q", name)
	}
	// A receipt-callback reader (the vendor's "webhook") is never called: the
	// vendor posts its receipts to us. It needs no URL, method or fields —
	// only the names of the callback's fields.
	webhook := e.Type == "vendor" && strings.EqualFold(name, "webhook")
	if u := strings.TrimSpace(e.URL); u == "" {
		if !webhook {
			return refusef("the call needs a URL")
		}
	} else if why := EndpointURLProblem(u); why != "" {
		return refusef("%s", why)
	}
	if !(webhook && e.Method == "") && !oneOf(e.Method, []string{"GET", "POST", "PUT"}) {
		return refusef("method %q is not one we send: GET, POST or PUT", e.Method)
	}
	if e.Encoding != "" && !oneOf(e.Encoding, Encodings()) {
		return refusef("%q is not a way to carry the fields: %s", e.Encoding, strings.Join(Encodings(), ", "))
	}
	if e.TimeoutMS < 0 || e.TimeoutMS > EndpointMaxTimeoutMS {
		return refusef("give up after %d ms? Give a whole number of milliseconds from 1 to %d (5 minutes)", e.TimeoutMS, EndpointMaxTimeoutMS)
	}

	valid := map[string]bool{}
	for _, s := range SourcesFor(e.Name) {
		valid[s.Key] = true
	}
	for _, s := range SourcesFor(e.Type) { // dlr/mo webhooks use their own list
		valid[s.Key] = true
	}
	seen := map[string]bool{}
	bodyFields := 0
	for _, p := range e.Params {
		alias := strings.TrimSpace(p.Alias)
		if alias == "" {
			return refusef("a field with no name: give the name the vendor expects")
		}
		if r := []rune(alias); len(r) > EndpointAliasMax {
			return refusef("the field name %q is longer than %d characters", string(r[:24])+"…", EndpointAliasMax)
		}
		if !endpointAliasRE.MatchString(alias) {
			return refusef("the field name %q: letters, digits and _ - . : [ ] @ $ ~ only, no spaces or other symbols", alias)
		}
		if seen[alias] {
			return refusef("the field %q is given twice: the vendor would receive only the last one", alias)
		}
		seen[alias] = true
		src := strings.TrimSpace(p.Source)
		if src == "session_id" {
			return refusef("field %q reads the session id, but a call built here has no session, so it would be sent empty — link a login call on the portal's HTTP Webhooks", alias)
		}
		if src == "" && strings.TrimSpace(p.DefaultValue) == "" {
			return refusef("field %q has neither a value from the message nor a fixed value, so it would always be sent empty", alias)
		}
		if src != "" && len(valid) > 0 && !valid[src] {
			return refusef("field %q is filled from %q, which this kind of call does not have", alias, src)
		}
		if p.SentIn != "" && !oneOf(p.SentIn, SentIns()) {
			return refusef("field %q says it travels in %q; it is QUERY or BODY", alias, p.SentIn)
		}
		if sentInOf(e, p) == "BODY" {
			bodyFields++
		}
	}
	// An XML call with no body template (the console writes none) is sent as
	// a <request> document built from its Body fields; with none, the
	// carrier gets an empty one. A GET carries no body at all.
	if e.Type == "vendor" && !webhook && e.Encoding == "xml" && e.Method != "GET" && bodyFields == 0 {
		return refusef("an XML call is built from its Body fields, and this one has none — add the fields the carrier expects")
	}

	// A vendor's SEND call must state what an accepted answer looks like
	// (PO 2026-09-17). Without it, a carrier that answers 200 with an error
	// in the body looks accepted, and the message is never sent again. This
	// is the rule `testvendor` fails.
	if e.Type == "vendor" && e.Name == "send" {
		if len(e.Response.OKStatus) == 0 && len(e.Response.OKContains) == 0 {
			return refusef("a send call must say how we know the carrier accepted it: give the status codes that mean accepted, or a value that appears in the answer when it worked. Without one, an answer carrying an error reads as success")
		}
	}
	rtype := strings.ToLower(strings.TrimSpace(e.Response.Type))
	if rtype != "" && !oneOf(rtype, ResponseTypes()) {
		return refusef("the answer is read as %s; %q is not one of them", strings.Join(ResponseTypes(), " or "), e.Response.Type)
	}
	if rtype == "" && !webhook && (len(e.Response.Params) > 0 || len(e.Response.OKContains) > 0) {
		return refusef("say how the carrier's answer is read: xml or json")
	}
	rseen := map[string]bool{}
	for _, rp := range e.Response.Params {
		alias, src := strings.TrimSpace(rp.Alias), strings.TrimSpace(rp.Source)
		if alias == "" || src == "" {
			return refusef("a value read from the answer needs both our name for it and the carrier's")
		}
		if rseen[alias] {
			return refusef("%q is read from the answer twice — keep one", alias)
		}
		rseen[alias] = true
		vital := e.Type == "vendor" && rtype == "xml" &&
			((strings.EqualFold(name, "send") && alias == "operator_id") || (strings.EqualFold(name, "poll") && alias == "status"))
		if vital && !XMLSourceReads(src) {
			return refusef("%s is read from an XML answer by a tag name (e.g. messageId) or a pattern with one ( ) around the value — %q is neither, so the vendor would not load", alias, src)
		}
	}
	for _, id := range e.Response.OKContains {
		if strings.TrimSpace(id.Source) == "" || strings.TrimSpace(id.Value) == "" {
			return refusef("a value that must be in the answer needs both the carrier's name for it and the value")
		}
	}
	for _, code := range e.Response.OKStatus {
		if code < 100 || code > 599 {
			return refusef("%d is not an HTTP status code", code)
		}
	}
	// The receipt reader is skipped by the gateway without the field that
	// carries the carrier's message id (the dlr role's loadWebhookRow).
	if webhook && !rseen["operator_id"] {
		return refusef("a receipt callback needs the name of the field that carries the carrier's message id (operator_id) — without it no receipt can be matched")
	}
	return nil
}

// endpointConfig builds the portal's config JSON from the answered pieces.
// soapVendor marks a SOAP vendor's receipt reader as SOAP, or the portal's
// vendor form would not list it for that vendor (connectors/endpointOptions.js).
func endpointConfig(e Endpoint, soapVendor bool) (string, error) {
	type pj struct {
		Alias        string `json:"alias"`
		SentIn       string `json:"sentIn,omitempty"`
		Source       string `json:"source"`
		DefaultValue string `json:"defaultValue,omitempty"`
	}
	type rpj struct {
		Alias  string `json:"alias"`
		Source string `json:"source"`
	}
	type idj struct {
		Value  string `json:"value"`
		Source string `json:"source"`
	}
	cfg := map[string]any{}
	if e.Type == "vendor" && strings.EqualFold(e.Name, "webhook") {
		// A receipt callback's fields, as the portal writes them: no request,
		// no session; the answer type is never consulted.
		rps := make([]rpj, 0, len(e.Response.Params))
		for _, rp := range e.Response.Params {
			rps = append(rps, rpj{Alias: strings.TrimSpace(rp.Alias), Source: strings.TrimSpace(rp.Source)})
		}
		cfg["requiresSessionId"] = false
		cfg["response"] = map[string]any{"type": "json", "params": rps}
		if soapVendor {
			cfg["contentType"] = "text/xml"
		}
		b, err := json.Marshal(cfg)
		return string(b), err
	}
	ps := make([]pj, 0, len(e.Params))
	for _, p := range e.Params {
		sentIn := p.SentIn
		if sentIn == "" {
			sentIn = "QUERY"
			if e.Encoding != "query" && e.Method != "GET" {
				sentIn = "BODY"
			}
		}
		ps = append(ps, pj{Alias: strings.TrimSpace(p.Alias), SentIn: sentIn, Source: strings.TrimSpace(p.Source), DefaultValue: p.DefaultValue})
	}
	cfg["params"] = ps

	resp := map[string]any{}
	if e.Response.Type != "" {
		resp["type"] = strings.ToLower(strings.TrimSpace(e.Response.Type))
	}
	rps := make([]rpj, 0, len(e.Response.Params))
	for _, rp := range e.Response.Params {
		rps = append(rps, rpj{Alias: strings.TrimSpace(rp.Alias), Source: strings.TrimSpace(rp.Source)})
	}
	if len(rps) > 0 {
		resp["params"] = rps
	}
	if len(e.Response.OKStatus) > 0 {
		resp["successStatusCodes"] = e.Response.OKStatus
	}
	if len(e.Response.OKContains) > 0 {
		ids := make([]idj, 0, len(e.Response.OKContains))
		for _, id := range e.Response.OKContains {
			ids = append(ids, idj{Value: id.Value, Source: id.Source})
		}
		resp["successAdditionalIdentifiers"] = ids
	}
	if len(resp) > 0 {
		cfg["response"] = resp
	}
	switch e.Encoding {
	case "form":
		cfg["contentType"] = "application/x-www-form-urlencoded"
	case "json":
		cfg["contentType"] = "application/json"
	case "xml":
		// Plain XML. text/xml is SOAP 1.1 to the portal, whose vendor form
		// would then hide this call from an HTTP vendor.
		cfg["contentType"] = "application/xml"
	}
	if e.TextFormat != "" && e.TextFormat != "plain" {
		cfg["textFormat"] = e.TextFormat
	}
	cfg["submissionMode"] = "SINGLE"
	b, err := json.Marshal(cfg)
	return string(b), err
}

// EndpointVendorProblem says why a call of this role cannot belong to the
// vendor, or nil: the vendor must exist and make HTTP calls (the engine reads
// endpoint rows for http and soap vendors only), and a SOAP vendor's call is
// its envelope — a body template, which the console does not write. Its
// receipt reader (webhook) has no request, so it can be made here.
func (s Store) EndpointVendorProblem(ctx context.Context, vendorID int64, role string) error {
	if vendorID <= 0 {
		return nil
	}
	cid, transport, err := s.DB.VendorCIDTransport(ctx, vendorID)
	if err != nil {
		return refusef("there is no vendor #%d", vendorID)
	}
	switch strings.ToLower(transport) {
	case "http":
		return nil
	case "soap":
		if strings.EqualFold(role, "webhook") {
			return nil
		}
		return refusef("%s is a SOAP vendor: its %s call is a SOAP envelope, which is written on the portal's HTTP Webhooks (SOAP Wizard) — the console builds no SOAP call", cid, role)
	}
	return refusef("%s is a %s vendor and never makes HTTP calls — pick an HTTP or SOAP vendor", cid, strings.ToUpper(transport))
}

func (s Store) vendorTransport(ctx context.Context, vendorID int64) string {
	var t string
	if vendorID > 0 {
		t, _ = s.DB.VendorTransport(ctx, vendorID)
	}
	return strings.ToLower(t)
}

// CreateEndpoint writes the row the portal would write.
func (s Store) CreateEndpoint(ctx context.Context, e Endpoint) (int64, error) {
	e.Method = strings.ToUpper(strings.TrimSpace(e.Method))
	e.Name = strings.TrimSpace(e.Name)
	if e.Type == "vendor" {
		e.Name = strings.ToLower(e.Name)
	}
	webhook := e.Type == "vendor" && e.Name == "webhook"
	if webhook && e.Method == "" {
		e.Method = "POST" // the column's own default; a receipt reader is never called
	}
	if err := ValidateEndpoint(e); err != nil {
		return 0, err
	}
	if e.Type == "vendor" {
		if err := s.EndpointVendorProblem(ctx, e.VendorID, e.Name); err != nil {
			return 0, err
		}
	}
	cfg, err := endpointConfig(e, s.vendorTransport(ctx, e.VendorID) == "soap")
	if err != nil {
		return 0, err
	}
	// A customer webhook made for an account is the account's from the
	// start (config/0174): the account is pointed at the new row below.
	webhookFor := e.AccountID > 0 && (e.Type == "dlr" || e.Type == "mo")
	var acct AccountDetail
	var was AccountWebhooks
	if webhookFor {
		if acct, err = s.GetAccount(ctx, e.AccountID); err != nil {
			return 0, err
		}
		if was, err = s.GetAccountWebhooks(ctx, e.AccountID); err != nil {
			return 0, err
		}
	}
	// 0 and "" are written NULL.
	var vendorID, accountID int64
	if e.VendorID > 0 {
		vendorID = e.VendorID
	}
	if e.AccountID > 0 {
		accountID = e.AccountID
	}
	url := strings.TrimSpace(e.URL)
	timeout := e.TimeoutMS
	if timeout == 0 {
		timeout = DefaultEndpointTimeout // 0 = not answered: the column's default, never "no time at all"
	}
	// A customer webhook's rate_limit / submit_concurrency mean pushes per
	// second and pushes at once (PO 2026-09-11); the column defaults (50, 1)
	// are the vendor meaning, so the Admin API sets these on a create — and
	// so does this door.
	rate, conc := 50, 1
	if e.Type == "mo" || e.Type == "dlr" {
		rate, conc = webhookPushRate, webhookPushConcurrency
	}
	id, err := s.DB.InsertEndpoint(ctx, dal.EndpointInsert{
		Type: e.Type, Name: e.Name, VendorID: vendorID, AccountID: accountID, URL: url, Method: e.Method,
		TimeoutMS: timeout, RateLimit: rate, Concurrency: conc, Config: cfg,
	})
	if err != nil {
		return 0, fmt.Errorf("create the call: %w", err)
	}
	if webhookFor {
		// account_id stays as given (bookkeeping); the account's own
		// pointer is what delivers (contract rule 5, the Admin API alike).
		before := was.DLR
		if e.Type == "mo" {
			before = was.MO
		}
		if err := s.pointAccountAt(ctx, acct, e.Type, before, id); err != nil {
			return id, fmt.Errorf("created %s webhook #%d, but could not point account %s at it: %w", e.Type, id, acct.Username, err)
		}
	}
	return id, nil
}

// EndpointRow is one line of the endpoint list.
type EndpointRow struct {
	ID      int64
	Type    string
	Name    string
	Owner   string // the vendor's cid; for a dlr/mo webhook the accounts that name it ("acct1, acct2" or "nobody yet"); else "shared"
	URL     string
	Method  string
	Active  bool
	Fields  int
	Trouble string
}

// ListEndpoints shows every configured call with who owns it.
func (s Store) ListEndpoints(ctx context.Context, kind string, limit int) ([]EndpointRow, error) {
	if limit <= 0 {
		limit = 200
	}
	list, err := s.DB.EndpointList(ctx, kind, limit)
	var readErr error // a read cut short: the rows read so far, with it
	if err != nil {
		var re dal.RowsError
		if !errors.As(err, &re) {
			return nil, fmt.Errorf("list calls: %w", err)
		}
		if list == nil {
			return nil, re.Err
		}
		readErr = re.Err
	}
	// A dlr/mo webhook's owners are the accounts that name it (config/0174),
	// read once for the whole list.
	var named map[int64][]dal.WebhookUser
	for _, l := range list {
		if l.Type == "dlr" || l.Type == "mo" {
			users, err := s.DB.AllWebhookUsers(ctx)
			if err != nil {
				return nil, fmt.Errorf("list calls: read the accounts' webhooks: %w", err)
			}
			named = map[int64][]dal.WebhookUser{}
			for _, u := range users {
				named[u.EndpointID] = append(named[u.EndpointID], u)
			}
			break
		}
	}
	var out []EndpointRow
	for _, l := range list {
		r := EndpointRow{ID: l.ID, Type: l.Type, Name: l.Name, Owner: l.Owner, URL: l.URL, Method: l.Method}
		if r.Type == "dlr" || r.Type == "mo" {
			r.Owner = webhookOwners(named[r.ID])
		}
		act, cfg := l.Active, l.Config
		r.Active = act != 0
		var c struct {
			Params   []json.RawMessage `json:"params"`
			Response struct {
				OKStatus []int             `json:"successStatusCodes"`
				OKIdents []json.RawMessage `json:"successAdditionalIdentifiers"`
				Params   []ResponseParam   `json:"params"`
			} `json:"response"`
		}
		_ = json.Unmarshal([]byte(cfg), &c)
		r.Fields = len(c.Params)
		if r.Type == "vendor" && r.Name == "send" &&
			len(c.Response.OKStatus) == 0 && len(c.Response.OKIdents) == 0 {
			r.Trouble = "no success rule — the vendor will not load"
		}
		if r.Type == "vendor" && r.Name == "webhook" {
			// Retired (issue 19): kept, read by nothing.
			r.Trouble = "retired — no longer read: the vendor's receipts are read by its Inbound DLR door"
		}
		out = append(out, r)
	}
	return out, readErr
}

// refID reads an id that JSON may hold as a number or as digits in text.
func refID(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return n
	}
	return 0
}

// EndpointUsers names everything that still points at call id — the Admin
// API's assertEndpointUnused (services/endpointRules.js), change together:
// a vendor's send/poll/webhook (any) slot, another call's login or a
// currency provider's request, a KMS provider's encrypt/decrypt call, an
// account's HLR order, a live rate's or a company's currency provider, and
// the companies and data keys a KMS provider locks, and the accounts that
// name it as their DLR or MO webhook (config/0174), last, by username. Deleted, each of them
// would call nothing — and a vendor with a dangling send call looks exactly
// like one that was never set up (QA VEN-010).
func (s Store) EndpointUsers(ctx context.Context, id int64) ([]string, error) {
	typ, name, cfgJSON, err := s.DB.EndpointKind(ctx, id)
	if err != nil {
		return nil, refusef("there is no call #%d", id)
	}
	var users []string
	vendors, err := s.DB.VendorAdapterConfigs(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the vendors: %w", err)
	}
	for _, v := range vendors {
		cid, ac := v.CID, v.AdapterJSON
		var a struct {
			Endpoints map[string]any `json:"endpoints"`
		}
		if json.Unmarshal([]byte(ac), &a) != nil {
			continue
		}
		for slot, ref := range a.Endpoints {
			if refID(ref) == id {
				users = append(users, fmt.Sprintf("vendor %s (its %s call)", cid, slot))
			}
		}
	}

	calls, err := s.DB.EndpointConfigsExcept(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("read the calls: %w", err)
	}
	for _, o := range calls {
		oid, otype, oname, oc := o.ID, o.Type, o.Name, o.Config
		var c map[string]any
		if json.Unmarshal([]byte(oc), &c) != nil {
			continue
		}
		if refID(c["session_endpoint_id"]) == id {
			users = append(users, fmt.Sprintf("%s call %q #%d (its login call)", otype, oname, oid))
		}
		if refID(c["request_endpoint_id"]) == id {
			users = append(users, fmt.Sprintf("%s provider %q #%d (its request)", otype, oname, oid))
		}
		if otype == "kms" && (refID(c["encrypt_endpoint_id"]) == id || refID(c["decrypt_endpoint_id"]) == id) {
			users = append(users, fmt.Sprintf("KMS provider %q #%d (its encrypt/decrypt call)", oname, oid))
		}
	}

	// A count that cannot be read counts nothing, as before.
	count := func(n int, _ error) int { return n }
	switch typ {
	case "hlr":
		names, err := s.DB.AccountsUsingHLRProvider(ctx, strconv.FormatInt(id, 10))
		if err == nil {
			for _, u := range names {
				users = append(users, fmt.Sprintf("account %s (its HLR order)", u))
			}
		}
	case "crp":
		ref1, ref2 := strconv.FormatInt(id, 10), strings.TrimSpace(name)
		if n := count(s.DB.LiveRatesUsingCRP(ctx, ref1, ref2)); n > 0 {
			users = append(users, fmt.Sprintf("%d live rate(s) on rate cards (their currency provider)", n))
		}
		names, err := s.DB.CompaniesUsingCRP(ctx, ref1, ref2)
		if err == nil {
			for _, n := range names {
				users = append(users, fmt.Sprintf("company %s (its currency provider)", n))
			}
		}
	case "kms":
		// A KMS PROVIDER row (no kms_role) is what companies' keys are locked under.
		var c map[string]any
		_ = json.Unmarshal([]byte(cfgJSON), &c)
		if role, _ := c["kms_role"].(string); strings.TrimSpace(role) == "" {
			cos := count(s.DB.KMSAssignmentsOf(ctx, id))
			keys := count(s.DB.DataKeysLike(ctx, fmt.Sprintf("http-kms://%d/%%", id)))
			if cos > 0 || keys > 0 {
				users = append(users, fmt.Sprintf("%d company KMS assignment(s) and %d stored data key(s) — disable the provider instead", cos, keys))
			}
		}
	}
	// Read for any type: a pointer is never left naming a row of another
	// type, but if one were, deleting the row would still strand it.
	named, err := s.DB.WebhookUsers(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("read the accounts' webhooks: %w", err)
	}
	for _, u := range named {
		users = append(users, webhookUserWords(u))
	}
	return users, nil
}

// DeleteEndpoint removes a call, and refuses while anything still points at
// it (EndpointUsers), naming them.
func (s Store) DeleteEndpoint(ctx context.Context, id int64) error {
	users, err := s.EndpointUsers(ctx, id)
	if err != nil {
		return err
	}
	if len(users) > 0 {
		return refusef("this call is still in use by %s. Point them at another call first, then delete it",
			strings.Join(users, ", "))
	}
	n, err := s.DB.DeleteEndpointRow(ctx, id)
	if err != nil {
		return fmt.Errorf("delete call %d: %w", id, err)
	}
	if n == 0 {
		return refusef("there is no call #%d", id)
	}
	return nil
}

// callSlots are the slots of a vendor that name one of its calls. The
// "webhook" slot is retired (issue 19, 2026-10-27; config/0153 moved what it
// said onto the vendor's DLR door): refused, and dropped from a vendor that
// still has one at its next save.
var callSlots = map[string]string{"send": "send call", "poll": "poll call"}

// soapContentTypes are what a SOAP vendor's calls are sent as.
var soapContentTypes = map[string]bool{"text/xml": true, "application/soap+xml": true}

// callFitsVendor says why a vendor cannot be pointed at call id in this slot,
// or nil. A call is made on its own and the VENDOR assigns it (2026-10-12):
// the Admin API's rule, services/vendorCalls.js — CHANGE TOGETHER. The call
// must be a vendor call of the slot's own role, switched on, used and owned
// by no other vendor, and SOAP for a SOAP vendor's send and poll.
func (s Store) callFitsVendor(ctx context.Context, vendorID int64, transport, slot string, id int64) error {
	what := callSlots[slot]
	call, err := s.DB.EndpointForPick(ctx, id)
	if err != nil {
		return refusef("there is no call #%d to pick", id)
	}
	typ, name, owner, active, cfgJSON, headersJSON := call.Type, call.Name, call.Owner, call.Active, call.Config, call.Headers
	if typ != "vendor" {
		return refusef("#%d is a %s call, not a vendor call", id, strings.ToUpper(typ))
	}
	if role := strings.ToLower(strings.TrimSpace(name)); role != slot {
		return refusef("#%d is a %s call, so it cannot be this vendor's %s — pick a call whose role is %s", id, role, what, slot)
	}
	if active == 0 {
		return refusef("call #%d is switched off — switch it on, or pick another", id)
	}
	others, err := s.DB.OtherVendorAdapterConfigs(ctx, vendorID)
	if err != nil {
		return fmt.Errorf("read the vendors: %w", err)
	}
	var usedBy, usedAs string
	for _, o := range others {
		cid, ac := o.CID, o.AdapterJSON
		var a struct {
			Endpoints map[string]any `json:"endpoints"`
		}
		if json.Unmarshal([]byte(ac), &a) != nil {
			continue
		}
		for other, ref := range a.Endpoints {
			if refID(ref) == id && usedBy == "" {
				usedBy, usedAs = cid, other
			}
		}
	}
	if usedBy != "" {
		return refusef("call #%d is already the %s call of vendor %s. A call belongs to one vendor — pick another, or make a new one", id, usedAs, usedBy)
	}
	if owner.Valid && owner.Int64 != vendorID {
		// A vendor that is gone owns nothing: its call is free again.
		if cid, err := s.DB.VendorCID(ctx, owner.Int64); err == nil {
			return refusef("call #%d is vendor %s's. A call belongs to one vendor — pick another, or make a new one", id, cid)
		}
	}
	if transport == "soap" {
		var cfg struct {
			ContentType string `json:"contentType"`
		}
		_ = json.Unmarshal([]byte(cfgJSON), &cfg)
		ct := cfg.ContentType
		if ct == "" {
			var headers map[string]any
			_ = json.Unmarshal([]byte(headersJSON), &headers)
			for k, v := range headers {
				if strings.EqualFold(k, "content-type") {
					ct, _ = v.(string)
				}
			}
		}
		ct = strings.ToLower(strings.TrimSpace(strings.SplitN(ct, ";", 2)[0]))
		if !soapContentTypes[ct] {
			return refusef("this is a SOAP vendor, so its %s is text/xml (SOAP 1.1) or application/soap+xml (SOAP 1.2) — call #%d is not", what, id)
		}
	}
	return nil
}

// syncCallOwners makes http_endpoints.vendor_id say what the vendor now
// uses, as the Admin API does after a vendor save (services/vendorCalls.js,
// CHANGE TOGETHER): a call just picked becomes the vendor's; one it already
// pointed at, and the login call its calls log in through, become its own
// when they are nobody's; one that was its own and is no longer used is let
// go — nobody's again, filed under the vendor's company when it had none.
func (s Store) syncCallOwners(ctx context.Context, vendorID int64, refs map[string]int64, changed []int64) error {
	used := map[int64]bool{}
	for _, id := range refs {
		if id > 0 {
			used[id] = true
		}
	}
	for id := range used {
		cfgJSON, err := s.DB.EndpointConfig(ctx, id)
		if err != nil {
			continue
		}
		var c map[string]any
		if json.Unmarshal([]byte(cfgJSON), &c) == nil {
			if login := refID(c["session_endpoint_id"]); login > 0 {
				used[login] = true
			}
		}
	}
	for _, id := range changed {
		if err := s.DB.ClaimCall(ctx, id, vendorID); err != nil {
			return fmt.Errorf("mark call %d as vendor %d's: %w", id, vendorID, err)
		}
	}
	keep := make([]string, 0, len(used))
	for id := range used {
		if err := s.DB.ClaimFreeCall(ctx, id, vendorID); err != nil {
			return fmt.Errorf("mark call %d as vendor %d's: %w", id, vendorID, err)
		}
		keep = append(keep, strconv.FormatInt(id, 10))
	}
	return s.releaseCalls(ctx, vendorID, keep)
}

// releaseCalls lets go of a vendor's calls: all of them, or all but keep
// (ids, already digits). company is read before a delete removes the vendor.
func (s Store) releaseCalls(ctx context.Context, vendorID int64, keep []string) error {
	company, _ := s.DB.VendorCompanyID(ctx, vendorID)
	if err := s.DB.ReleaseCalls(ctx, vendorID, company, keep); err != nil {
		return fmt.Errorf("let go of vendor %d's calls: %w", vendorID, err)
	}
	return nil
}

// AttachEndpoints points a vendor at its calls, the way the portal does —
// {"endpoints":{"send":74,"poll":75}} — rather than an inline profile. The
// engine hydrates the vendor from those rows (GW-15). The slots named are set
// (an id of 0 clears one); the others are KEPT — attaching a new send call
// used to drop the vendor's poll call. The receipt "webhook" slot is retired
// (issue 19): asked for, it is refused; still stored, it is dropped.
//
// A call need not have been made for this vendor (2026-10-12): one that is
// nobody's is assigned here, and a slot that changes is judged by
// callFitsVendor. http_endpoints.vendor_id follows what the vendor uses.
func (s Store) AttachEndpoints(ctx context.Context, vendorID int64, refs map[string]int64) error {
	clean := map[string]int64{}
	cur, transport, err := s.DB.VendorAdapterTransport(ctx, vendorID)
	if err != nil {
		return refusef("there is no vendor #%d", vendorID)
	}
	var have struct {
		Endpoints map[string]any `json:"endpoints"`
	}
	if json.Unmarshal([]byte(cur), &have) == nil {
		for k, v := range have.Endpoints {
			if n := refID(v); n > 0 {
				clean[k] = n
			}
		}
	}
	delete(clean, "webhook")
	var changed []int64
	for k, v := range refs {
		if k == "webhook" && v > 0 {
			return refusef("%s", WebhookRetired)
		}
		if v > 0 {
			if _, slot := callSlots[k]; slot && clean[k] != v {
				if err := s.callFitsVendor(ctx, vendorID, transport, k, v); err != nil {
					return err
				}
				changed = append(changed, v)
			}
			clean[k] = v
		} else {
			delete(clean, k)
		}
	}
	if clean["send"] == 0 {
		return refusef("a vendor needs a send call before it can be used")
	}
	b, err := json.Marshal(map[string]any{"endpoints": clean})
	if err != nil {
		return err
	}
	if err := s.DB.SetVendorAdapterConfig(ctx, vendorID, string(b)); err != nil {
		return fmt.Errorf("attach the calls to vendor %d: %w", vendorID, err)
	}
	if err := s.syncCallOwners(ctx, vendorID, clean, changed); err != nil {
		return err
	}
	// Ask the engine's own loader whether that vendor would now work, and say
	// so immediately rather than leaving it to be discovered by a queue that
	// never drains.
	if t := s.vendorTrouble(ctx, "http", string(b)); t != "" {
		return refusef("saved, but the vendor still will not load: %s", t)
	}
	return nil
}

// TestPattern runs a capture pattern against a sample the operator pastes, so
// "does this read the carrier's id out?" is answered before any traffic flows.
// It uses Go's own regexp — the same engine that will run it for real.
func TestPattern(pattern, sample string) (string, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", refusef("that pattern cannot be read: %v", err)
	}
	m := re.FindStringSubmatch(sample)
	if m == nil {
		return "", refusef("the pattern found nothing in that sample")
	}
	if len(m) > 1 {
		return m[1], nil
	}
	return m[0], nil
}
