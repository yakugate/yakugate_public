package httpserver

// POST /admin/webhook/test — the webhook tester (Tools, PO 2026-09-12).
//
// "We never get your receipts" is the other half of support's day. The usual
// cause is on the customer's side — a URL that 404s, a firewall, an endpoint
// that answers 200 only to GET, a TLS certificate that expired — and until now
// the only way to show them was to send a real message and read the dlr role's
// log.
//
// This sends ONE sample receipt (kind "dlr") or incoming message (kind "mo")
// to a URL and reports exactly what came back. The request is built by the
// same code the dlr and mo roles use (dlr.NewWebhookRequest,
// mo.NewWebhookRequest), so a customer testing against it sees the real body,
// and the verdict applies the roles' own rule: any 2xx is delivered, anything
// else is retried.
//
// Which URLs a caller may test is the Admin API's decision: an operator may
// test any, a customer only the URLs set on their own accounts. For a
// customer the Admin API also sets publicOnly, because this tool hands the
// reply BODY back — unlike the real push — and a customer who runs their own
// receiving server could otherwise answer with a redirect to 127.0.0.1 or a
// cloud metadata address and read what comes back. publicOnly refuses any
// connection to a loopback, private, link-local or otherwise internal
// address, checked on the address actually dialled, so a redirect or a DNS
// name that later resolves inward is caught too.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/oklog/ulid/v2"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	mosvc "github.com/yakugate/yakugate/internal/mo"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/pushcall"
	"github.com/yakugate/yakugate/internal/pushpool"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// webhookBodyLimit is how much of the customer's reply is shown. Enough for
// any error page's first screen; a tester has no reason to hold more.
const webhookBodyLimit = 4 << 10

// maxWebhookTimeout caps a test. The real push is bounded by the endpoint's
// own timeout_ms; a test that hangs longer than this helps nobody.
const maxWebhookTimeout = 30 * time.Second

type webhookTestRequest struct {
	URL    string `json:"url"`
	Kind   string `json:"kind"`   // "dlr" (default) | "mo"
	Method string `json:"method"` // MO only: GET | POST (default POST); receipts are always POST
	// Receipt fields. Status is a canonical status: delivered (default),
	// failed, expired, rejected or sent.
	Status    string `json:"status"`
	ErrCode   string `json:"errCode"`
	Reference string `json:"reference"`
	// Sample message addresses and text. Defaults are harmless placeholders.
	From      string `json:"from"`
	To        string `json:"to"`
	Text      string `json:"text"`
	TimeoutMS int    `json:"timeoutMs"`
	// PublicOnly refuses internal addresses (see the file comment).
	PublicOnly bool `json:"publicOnly"`
	// EndpointID, when set, tests that saved webhook row exactly as the
	// gateway would push to it (2026-09-14): its headers, auth, TLS, its own
	// request if it shapes one, and its success rules. URL is then the row's.
	EndpointID int64 `json:"endpointId"`
	// Values sets the sample's value of any field a push can carry
	// (2026-11-06, Arsalan: the tester asks for every parameter of the
	// webhook). Keys are the webhook builder's source names (message_id,
	// status, stat, receipt, error_code, submitted_at, hlr.mcc, vendor.cid, …)
	// or the push's own names (msgid, mcc, vendor, …). status and error_code
	// work as Status and ErrCode do, so the receipt text follows them unless
	// receipt is set too. A key no push carries is refused.
	Values map[string]string `json:"values"`
	// Extra fields are added to the request once it is built, after the
	// webhook's own: a field of the same name in the same place is replaced.
	Extra []webhookExtra `json:"extra"`
}

// webhookExtra is one field the tester adds to the request.
type webhookExtra struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	// In is query, header or body. Empty = body, or query for a request
	// that carries no body (GET).
	In string `json:"in"`
}

// Limits on what a test may add: a tester has no reason for more.
const (
	maxWebhookValues = 50
	maxWebhookExtra  = 20
)

type webhookHop struct {
	Status int    `json:"status"`
	URL    string `json:"url"`
}

type webhookTestResponse struct {
	Kind    string `json:"kind"`
	Method  string `json:"method"`
	URL     string `json:"url"` // the URL actually requested (a GET MO carries its query)
	Request struct {
		Headers map[string]string `json:"headers"`
		Body    string            `json:"body,omitempty"`
	} `json:"request"`
	// Delivered is the dlr/mo roles' verdict: a 2xx and nothing else.
	Delivered bool   `json:"delivered"`
	Verdict   string `json:"verdict"`
	Status    int    `json:"status,omitempty"`
	// Error is set when no HTTP answer came back at all (DNS, refused, TLS,
	// timeout).
	Error     string            `json:"error,omitempty"`
	TookMS    int64             `json:"tookMs"`
	Redirects []webhookHop      `json:"redirects,omitempty"`
	Headers   map[string]string `json:"responseHeaders,omitempty"`
	Body      string            `json:"responseBody,omitempty"`
	Truncated bool              `json:"responseTruncated,omitempty"`
	SentAt    string            `json:"sentAt"`
}

func (p *Process) handleWebhookTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req webhookTestRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	target := strings.TrimSpace(req.URL)
	if u, err := url.Parse(target); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		writeJSON(w, 400, map[string]string{"error": "url must be a full http:// or https:// address"})
		return
	}
	if len(req.Values) > maxWebhookValues || len(req.Extra) > maxWebhookExtra {
		writeJSON(w, 400, map[string]string{"error": fmt.Sprintf("at most %d values and %d extra fields", maxWebhookValues, maxWebhookExtra)})
		return
	}
	hreq, out, fields, err := buildWebhookTest(r.Context(), target, req)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	var ep *pushcall.Endpoint
	if req.EndpointID > 0 {
		// The saved row, built the way the dlr and mo roles build it.
		if ep, err = p.webhookEndpoint(r.Context(), req.EndpointID); err != nil {
			writeJSON(w, 400, map[string]string{"error": "webhook settings: " + err.Error()})
			return
		}
		built := hreq
		if hreq, err = ep.Request(r.Context(), fields, func(context.Context) (*http.Request, error) { return built, nil }); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
	}
	for _, x := range req.Extra {
		if err := setRequestField(hreq, x); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
	}
	showRequest(out, hreq)
	timeout := pushpool.DefaultTimeout
	if req.TimeoutMS > 0 {
		timeout = min(time.Duration(req.TimeoutMS)*time.Millisecond, maxWebhookTimeout)
	}
	runWebhookTest(hreq, timeout, req.PublicOnly, out, ep)
	writeJSON(w, 200, out)
}

// buildWebhookTest makes the sample request, with the roles' own builders,
// and returns the sample's values for a row that shapes its own request.
func buildWebhookTest(ctx context.Context, target string, req webhookTestRequest) (*http.Request, *webhookTestResponse, map[string]string, error) {
	out := &webhookTestResponse{SentAt: time.Now().UTC().Format(time.RFC3339)}
	kind := strings.ToLower(strings.TrimSpace(req.Kind))
	if kind == "" {
		kind = "dlr"
	}
	vals, err := webhookValues(kind, req.Values)
	if err != nil {
		return nil, nil, nil, err
	}
	// pick is the value the tester was given for a field, else def.
	pick := func(key, def string) string {
		if v, ok := vals[key]; ok {
			return v
		}
		return def
	}
	id := pick("msgid", ulid.Make().String())
	var (
		hreq   *http.Request
		fields map[string]string
	)
	switch kind {
	case "dlr":
		out.Kind, out.Method = "dlr", http.MethodPost
		st := model.Status(strings.ToLower(firstNonBlank(pick("status", req.Status), string(model.StatusDelivered))))
		switch st {
		case model.StatusDelivered, model.StatusFailed, model.StatusExpired, model.StatusRejected, model.StatusSent:
		default:
			return nil, nil, nil, fmt.Errorf("status must be delivered, failed, expired, rejected or sent")
		}
		done := time.Now()
		receipt := pick("receipt", dlrsvc.ReceiptText(id, st, strings.TrimSpace(pick("error_code", req.ErrCode)), done))
		// A receipt goes back TO the handset's message FROM the sender.
		body := dlrsvc.WebhookBody{
			MessageID:   id,
			Reference:   pick("reference", strings.TrimSpace(req.Reference)),
			Status:      string(st),
			Stat:        pick("stat", string(dlrsvc.StatFor(st))),
			Receipt:     receipt,
			ErrorCode:   pick("error_code", dlrsvc.ErrorCodeOf(receipt)),
			To:          pick("to", firstNonBlank(req.From, "923001234567")),
			From:        pick("from", firstNonBlank(req.To, "YAKUTEST")),
			SubmittedAt: pick("submitted_at", dlrsvc.WebhookTime(dlrsvc.SubmittedAt(id))),
			DoneAt:      pick("done_at", dlrsvc.WebhookTime(done)),
		}
		fields = map[string]string{"msgid": id, "reference": body.Reference, "status": body.Status, "stat": body.Stat,
			"receipt": body.Receipt, "error_code": body.ErrorCode, "to": body.To, "from": body.From,
			"submitted_at": body.SubmittedAt, "done_at": body.DoneAt}
		hreq, err = dlrsvc.NewWebhookRequest(ctx, target, body, nil) // the tester shows the native shape; a push follows the account's dialect
	case "mo":
		out.Kind = "mo"
		out.Method = strings.ToUpper(firstNonBlank(req.Method, http.MethodPost))
		if out.Method != http.MethodGet && out.Method != http.MethodPost {
			return nil, nil, nil, fmt.Errorf("method must be GET or POST")
		}
		// An MO comes FROM the handset TO the customer's short code.
		msg := model.Message{ID: id,
			Sender:      pick("from", firstNonBlank(req.From, "923001234567")),
			Destination: pick("to", firstNonBlank(req.To, "YAKUTEST")),
			Body:        pick("text", firstNonBlank(req.Text, "Test message from the Yakugate webhook tester")),
		}
		// A sample network and vendor (4 Nov 2026), so the tester shows the
		// whole shape a push can have.
		facts := mosvc.PushFacts{MCC: pick("mcc", "410"), MNC: pick("mnc", "01"), Ported: pick("ported", "false"),
			VendorCID: pick("vendor", "sample-vendor"), VendorName: pick("vendor_name", "Sample vendor")}
		fields = map[string]string{"msgid": id, "from": msg.Sender, "to": msg.Destination, "text": msg.Body,
			"encoding": pick("encoding", "gsm7"), "accepted_at": pick("accepted_at", mosvc.AcceptedText(msg))}
		for k, v := range facts.Sources() {
			fields[k] = v
		}
		fields["operator"] = pick("operator", fields["operator"])
		hreq, err = mosvc.NewWebhookRequestWith(ctx, target, out.Method, msg, nil, facts)
		// The gateway's own MO body works these two out itself; a value the
		// tester was given is written over what it worked out.
		for key, native := range map[string]string{"accepted_at": "acceptedAt", "operator": "operator"} {
			if v, ok := vals[key]; ok && err == nil {
				err = setRequestField(hreq, webhookExtra{Name: native, Value: v})
			}
		}
	default:
		return nil, nil, nil, fmt.Errorf(`kind must be "dlr" or "mo"`)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	showRequest(out, hreq)
	return hreq, out, fields, nil
}

// webhookSourceKeys maps the webhook builder's source names (the portal's
// SOURCE_STRUCTURES) onto the push's own field names, where they differ.
var webhookSourceKeys = map[string]string{
	"message_id": "msgid", "hlr.mcc": "mcc", "hlr.mnc": "mnc", "hlr.operator": "operator",
	"hlr.ported": "ported", "vendor.cid": "vendor", "vendor.name": "vendor_name",
}

// webhookFields is every field a push of each kind can carry, by its own name.
var webhookFields = map[string][]string{
	"dlr": {"msgid", "reference", "status", "stat", "receipt", "error_code", "to", "from", "submitted_at", "done_at"},
	"mo":  {"msgid", "from", "to", "text", "encoding", "accepted_at", "mcc", "mnc", "operator", "ported", "vendor", "vendor_name"},
}

// webhookValues reads the tester's values under the push's own names and
// refuses one the kind cannot carry, so a typo is said, not ignored.
func webhookValues(kind string, in map[string]string) (map[string]string, error) {
	known := webhookFields[kind]
	out := make(map[string]string, len(in))
	for k, v := range in {
		key := strings.TrimSpace(k)
		if alias, ok := webhookSourceKeys[key]; ok {
			key = alias
		}
		ok := false
		for _, f := range known {
			ok = ok || f == key
		}
		if !ok {
			return nil, fmt.Errorf("values: %q is not a field of a %s push", k, strings.ToUpper(kind))
		}
		out[key] = v
	}
	return out, nil
}

// showRequest copies the request about to be sent into the answer.
func showRequest(out *webhookTestResponse, req *http.Request) {
	out.URL, out.Method = req.URL.String(), req.Method
	out.Request.Headers = flatHeaders(req.Header)
	out.Request.Body = ""
	if req.GetBody != nil {
		if rc, err := req.GetBody(); err == nil {
			b, _ := io.ReadAll(rc)
			rc.Close()
			out.Request.Body = string(b)
		}
	}
}

// headerName is what an HTTP header name may be (RFC 9110 token).
var headerName = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")

// setRequestField puts one field on a built request: in the query, a header,
// or the body. A body field goes into a JSON object or a form body; any other
// body (XML, text) is the webhook's own text, which has no field to set.
func setRequestField(req *http.Request, x webhookExtra) error {
	name := strings.TrimSpace(x.Name)
	if name == "" || len(name) > 128 {
		return fmt.Errorf("an extra field needs a name of 1 to 128 characters")
	}
	in := strings.ToLower(strings.TrimSpace(x.In))
	body := ""
	if req.GetBody != nil {
		if rc, err := req.GetBody(); err == nil {
			b, _ := io.ReadAll(rc)
			rc.Close()
			body = string(b)
		}
	}
	if in == "" {
		in = "body"
		if req.Method == http.MethodGet || req.Method == http.MethodHead {
			in = "query"
		}
	}
	switch in {
	case "query":
		q := req.URL.Query()
		q.Set(name, x.Value)
		req.URL.RawQuery = q.Encode()
		return nil
	case "header":
		if !headerName.MatchString(name) {
			return fmt.Errorf("%q cannot be a header name", name)
		}
		req.Header.Set(name, x.Value)
		return nil
	case "body":
	default:
		return fmt.Errorf("extra field %q: in must be query, header or body", name)
	}
	if req.Method == http.MethodGet || req.Method == http.MethodHead {
		return fmt.Errorf("extra field %q: a %s request has no body; put it in the query", name, req.Method)
	}
	ct := strings.ToLower(req.Header.Get("Content-Type"))
	var next string
	switch {
	case strings.Contains(ct, "x-www-form-urlencoded"):
		v, err := url.ParseQuery(body)
		if err != nil {
			return fmt.Errorf("extra field %q: the body is not a form: %v", name, err)
		}
		v.Set(name, x.Value)
		next = v.Encode()
	case strings.Contains(ct, "json") || (ct == "" && strings.TrimSpace(body) == ""):
		obj := map[string]any{}
		if strings.TrimSpace(body) != "" {
			if err := json.Unmarshal([]byte(body), &obj); err != nil {
				return fmt.Errorf("extra field %q: the body is not a JSON object, so a field cannot be added to it; put it in the query or a header", name)
			}
		}
		obj[name] = x.Value
		b, _ := json.Marshal(obj)
		next = string(b)
		if ct == "" {
			req.Header.Set("Content-Type", "application/json")
		}
	default:
		return fmt.Errorf("extra field %q: this webhook sends a %s body, which has no fields to add to; put it in the query or a header", name, ct)
	}
	req.Body = io.NopCloser(strings.NewReader(next))
	req.ContentLength = int64(len(next))
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(next)), nil }
	return nil
}

// runWebhookTest sends it and fills in what came back. ep (a saved row, or
// nil) adds its TLS settings and judges the answer by its own rules.
func runWebhookTest(hreq *http.Request, timeout time.Duration, publicOnly bool, out *webhookTestResponse, ep *pushcall.Endpoint) {
	var hops []webhookHop
	tr := webhookTransport(publicOnly)
	if tc := ep.TLS(); tc != nil {
		tr.TLSClientConfig = tc.Clone()
	}
	client := &http.Client{
		Timeout:   timeout,
		Transport: tr,
		// Follow redirects as the roles' plain client does, but note each
		// one: a URL that redirects is a common cause of a lost POST body.
		CheckRedirect: func(next *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			status := 0
			if next.Response != nil {
				status = next.Response.StatusCode
			}
			hops = append(hops, webhookHop{Status: status, URL: next.URL.String()})
			return nil
		},
	}
	start := time.Now()
	resp, err := client.Do(hreq)
	out.TookMS = time.Since(start).Milliseconds()
	out.Redirects = hops
	if err != nil {
		out.Error = err.Error()
		out.Verdict = "No answer, so the gateway would retry later and, once the endpoint's retry policy runs out, give up."
		return
	}
	defer resp.Body.Close()
	out.Status = resp.StatusCode
	out.Headers = flatHeaders(resp.Header)
	b, _ := io.ReadAll(io.LimitReader(resp.Body, webhookBodyLimit+1))
	if len(b) > webhookBodyLimit {
		b, out.Truncated = b[:webhookBodyLimit], true
	}
	out.Body = string(b)
	// The roles' rule, exactly: a row that shapes its own request is judged
	// by its own success and retry codes; otherwise 2xx is delivered.
	jerr := ep.Judge(resp.StatusCode, out.Body)
	switch {
	case jerr == nil && ep.Custom():
		out.Delivered = true
		out.Verdict = "Delivered by this endpoint's own success rules. The gateway would not send it again."
	case jerr == nil:
		out.Delivered = true
		out.Verdict = "Delivered. The gateway counts any 2xx answer as delivered and does not send it again."
	case errors.Is(jerr, httpadapter.ErrPermanent):
		out.Verdict = fmt.Sprintf("Not delivered (HTTP %d), and this endpoint's settings call that answer final, so the gateway would give up on the message at once.", resp.StatusCode)
	case ep.Custom():
		out.Verdict = fmt.Sprintf("Not delivered (HTTP %d) by this endpoint's own success rules. The gateway would retry later and, once the retry policy runs out, give up.", resp.StatusCode)
	default:
		out.Verdict = fmt.Sprintf("Not delivered (HTTP %d). The gateway would retry later and, once the endpoint's retry policy runs out, give up. Only a 2xx answer counts.", resp.StatusCode)
	}
}

// webhookEndpoint loads one saved receipt / MO webhook row and builds it the
// way the dlr and mo roles do.
func (p *Process) webhookEndpoint(ctx context.Context, id int64) (*pushcall.Endpoint, error) {
	if p.traffic == nil {
		return nil, fmt.Errorf("the config store is not available")
	}
	w, err := p.traffic.CustomerWebhooks().Webhook(ctx, id)
	raw := pushcall.Raw{ID: id, Name: w.Name, URL: w.URL, Method: w.Method, Headers: w.Headers, Auth: w.Auth,
		BodyTemplate: w.BodyTemplate, TLS: w.TLS, Config: w.Config}
	if err != nil {
		return nil, fmt.Errorf("webhook endpoint %d: %w", id, err)
	}
	row, err := raw.Row()
	if err != nil {
		return nil, err
	}
	return pushcall.Build(ctx, row, config.Secrets)
}

// errInternalAddress is what a public-only test says when it is refused.
var errInternalAddress = fmt.Errorf("the address is internal (loopback, private or link-local), which this test may not reach")

// webhookTransport is a plain transport; with publicOnly its dialer refuses
// internal addresses. The check runs in Control, on the IP actually being
// dialled — after DNS — so neither a redirect nor a name that resolves
// inward gets past it.
func webhookTransport(publicOnly bool) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	if !publicOnly {
		return t
	}
	d := &net.Dialer{Timeout: 10 * time.Second, Control: func(_, address string, _ syscall.RawConn) error {
		ap, err := netip.ParseAddrPort(address)
		if err != nil || internalAddr(ap.Addr()) {
			return errInternalAddress
		}
		return nil
	}}
	t.DialContext = d.DialContext
	t.Proxy = nil // a proxy would dial on our behalf, past the check
	return t
}

// cgnat is 100.64.0.0/10, shared address space that is internal in practice.
var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// internalAddr reports whether an address is one a customer's test must not
// reach.
func internalAddr(a netip.Addr) bool {
	a = a.Unmap()
	return a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() ||
		a.IsInterfaceLocalMulticast() || a.IsMulticast() || a.IsUnspecified() || cgnat.Contains(a)
}

// flatHeaders is a header map with one line per name, sorted for display.
func flatHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out[k] = strings.Join(h[k], ", ")
	}
	return out
}

func firstNonBlank(v, def string) string {
	if s := strings.TrimSpace(v); s != "" {
		return s
	}
	return def
}
