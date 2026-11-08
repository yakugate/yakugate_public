// Package pushcall turns a customer's webhook row (http_endpoints type 'dlr'
// or 'mo') into the push the gateway makes to it (PO 2026-09-14: "these should
// also be http endpoint … configurable and can have anything").
//
// Until now the receipt and MO roles read only a webhook row's URL, method,
// retry, rate and timeout. The headers, auth, TLS and body the portal lets a
// customer set were saved and ignored, so a webhook that needed an API key
// could not work. Now a webhook row is an ordinary HTTP endpoint, built with
// the same shared builder as vendor, HLR, currency and KMS calls:
//
//   - A row with request settings of its own (parameters, or a body template)
//     is sent exactly as configured: any method, content type, parameters in
//     the query, headers or body, any body shape, success and retry codes.
//     Its placeholders come from the push's fields — for a receipt
//     {message_id} {reference} {status} {stat} {receipt} {error_code} {to}
//     {from} {submitted_at} {done_at}; for an incoming message {message_id}
//     {from} {to} {text} {encoding} {accepted_at}.
//   - A row without them keeps the gateway's own body (dlr.WebhookBody /
//     mo.WebhookBody), so nothing changes for a customer who set only a URL —
//     plus the row's headers, auth and TLS, which it now always gets.
//
// Header and auth values may name a secret, {{secret:REF}}, resolved when the
// row is (re)loaded, as KMS rows do. Pushes are not signed (not now, same
// ruling).
package pushcall

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/yakugate/yakugate/internal/platform/outhttp"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// secretRe matches {{secret:REF}}.
var secretRe = regexp.MustCompile(`\{\{\s*secret:([^}]+?)\s*\}\}`)

// Endpoint is one webhook row, ready to push to.
type Endpoint struct {
	custom    bool
	call      httpadapter.Call
	tls       *tls.Config
	transport *http.Transport // one per endpoint with TLS settings, so pushes reuse connections
}

// Build resolves the row's secrets and builds its call.
func Build(ctx context.Context, row httpadapter.EndpointRow, res Resolver) (*Endpoint, error) {
	r := row
	if len(row.Headers) > 0 {
		r.Headers = make(map[string]string, len(row.Headers))
		for k, v := range row.Headers {
			out, err := resolve(ctx, v, res)
			if err != nil {
				return nil, fmt.Errorf("header %s: %w", k, err)
			}
			r.Headers[k] = out
		}
	}
	if len(row.Auth) > 0 {
		r.Auth = make(map[string]any, len(row.Auth))
		for k, v := range row.Auth {
			if sv, ok := v.(string); ok {
				out, err := resolve(ctx, sv, res)
				if err != nil {
					return nil, fmt.Errorf("auth %s: %w", k, err)
				}
				r.Auth[k] = out
				continue
			}
			r.Auth[k] = v
		}
	}
	call, err := httpadapter.CallFromEndpoint(r, "webhook")
	if err != nil {
		return nil, err
	}
	tc, err := httpadapter.TLSConfig(ctx, row.TLS, res)
	if err != nil {
		return nil, err
	}
	custom := strings.TrimSpace(row.BodyTemplate) != "" || len(call.Params) > 0 || len(call.QueryParams) > 0
	e := &Endpoint{custom: custom, call: call, tls: tc}
	if tc != nil {
		e.transport = outhttp.New(tc)
	}
	return e, nil
}

// resolve replaces every {{secret:REF}} in v. The ref may be named in an
// error (it is config, not a secret); the value never is.
func resolve(ctx context.Context, v string, res Resolver) (string, error) {
	var rerr error
	out := secretRe.ReplaceAllStringFunc(v, func(s string) string {
		ref := secretRe.FindStringSubmatch(s)[1]
		if res == nil {
			rerr = fmt.Errorf("uses a secret but no secrets store is available")
			return ""
		}
		val, err := res.ResolveString(ctx, ref)
		if err != nil {
			rerr = fmt.Errorf("cannot resolve secret %s: %v", ref, err)
			return ""
		}
		return strings.TrimSpace(val)
	})
	return out, rerr
}

// Custom reports whether the row shapes its own request.
func (e *Endpoint) Custom() bool { return e != nil && e.custom }

// Request builds one push. fallback builds the gateway's own request for a
// row that does not shape its own; it gets the row's headers and auth added.
// A nil Endpoint (a target built without one) is the fallback alone.
func (e *Endpoint) Request(ctx context.Context, fields map[string]string, fallback func(context.Context) (*http.Request, error)) (*http.Request, error) {
	if e.Custom() {
		return httpadapter.NewRequest(ctx, e.call, fields)
	}
	req, err := fallback(ctx)
	if err != nil || e == nil {
		return req, err
	}
	if req, err = withMethod(req, e.call.Method); err != nil {
		return nil, err
	}
	for k, v := range e.call.Headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

// withMethod sends the gateway's own request with the row's method (QA
// EP-023: a receipt webhook set to GET with only a URL was POSTed a JSON
// body). GET, HEAD and DELETE carry no body, so the body's fields move to the
// query string — the shape an incoming-message GET push already has. PUT and
// PATCH keep the body. A body that is not a flat JSON object is left as it
// is, with only the method changed.
func withMethod(req *http.Request, method string) (*http.Request, error) {
	m := strings.ToUpper(strings.TrimSpace(method))
	if m == "" || m == req.Method {
		return req, nil
	}
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodDelete:
	default:
		req.Method = m
		return req, nil
	}
	var raw []byte
	if req.Body != nil && req.Body != http.NoBody {
		b, err := io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			return nil, err
		}
		raw = b
	}
	var fields map[string]any
	if len(bytes.TrimSpace(raw)) > 0 && json.Unmarshal(raw, &fields) != nil {
		req.Method = m
		req.Body, req.GetBody, req.ContentLength = io.NopCloser(bytes.NewReader(raw)), nil, int64(len(raw))
		return req, nil
	}
	q := req.URL.Query()
	for k, v := range fields {
		switch t := v.(type) {
		case string:
			q.Set(k, t)
		case nil:
		default:
			b, _ := json.Marshal(t)
			q.Set(k, string(b))
		}
	}
	out := req.Clone(req.Context())
	out.Method = m
	out.URL.RawQuery = q.Encode()
	out.Body, out.GetBody, out.ContentLength = http.NoBody, nil, 0
	out.Header.Del("Content-Type")
	return out, nil
}

// Client is base (its timeout and redirect rules) with this endpoint's TLS
// settings, or base itself when it has none.
func (e *Endpoint) Client(base *http.Client) *http.Client {
	if e == nil || e.transport == nil || base == nil {
		return base
	}
	c := *base
	c.Transport = e.transport
	return &c
}

// TLS is the endpoint's TLS settings (nil = system defaults).
func (e *Endpoint) TLS() *tls.Config {
	if e == nil {
		return nil
	}
	return e.tls
}

// Judge says whether an answer counts as delivered: nil is success. A custom
// row is judged by its own success and retry codes; the gateway's own
// request needs a 2xx.
func (e *Endpoint) Judge(code int, body string) error {
	if e.Custom() {
		return httpadapter.Judge(e.call, body, code)
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("HTTP %d", code)
	}
	return nil
}

// Send pushes and judges the answer: nil is success. A custom row is judged by
// its own success and retry codes (a failure that will never improve wraps
// httpadapter.ErrPermanent); the gateway's own request succeeds on a 2xx.
func (e *Endpoint) Send(cl *http.Client, req *http.Request) (int, error) {
	resp, err := cl.Do(req)
	if err != nil {
		return 0, err
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	return resp.StatusCode, e.Judge(resp.StatusCode, string(b))
}

// Raw is a webhook row as read from http_endpoints (JSON columns as text).
type Raw struct {
	ID                                       int64
	Name, URL, Method                        string
	Headers, Auth, BodyTemplate, TLS, Config string
}

// Row parses a Raw into the shared builder's row.
func (r Raw) Row() (httpadapter.EndpointRow, error) {
	row := httpadapter.EndpointRow{ID: r.ID, Name: r.Name, URL: r.URL, Method: r.Method, BodyTemplate: r.BodyTemplate}
	for _, f := range []struct {
		name, raw string
		dst       any
	}{{"headers", r.Headers, &row.Headers}, {"auth", r.Auth, &row.Auth}, {"tls", r.TLS, &row.TLS}, {"config", r.Config, &row.Config}} {
		if t := strings.TrimSpace(f.raw); t != "" && t != "null" {
			if err := json.Unmarshal([]byte(t), f.dst); err != nil {
				return row, fmt.Errorf("endpoint %d: %s is not valid JSON: %v", r.ID, f.name, err)
			}
		}
	}
	return row, nil
}

// Cache keeps each webhook row built until the row changes: the roles reload
// their targets every second, and resolving secrets and TLS files that often
// would be waste.
type Cache struct {
	res Resolver
	mu  sync.Mutex
	m   map[int64]cached
}

type cached struct {
	fp  string
	ep  *Endpoint
	err error
}

// Resolver is the secrets resolver (config.Secrets).
type Resolver = httpadapter.Resolver

// NewCache returns an empty cache.
func NewCache(res Resolver) *Cache { return &Cache{res: res, m: map[int64]cached{}} }

// Get returns the built endpoint for r, building it again only when r changed.
// An error is kept too, so a row that cannot be built fails its pushes with
// the reason — never sends without the auth it asked for.
func (c *Cache) Get(ctx context.Context, r Raw) (*Endpoint, error) {
	fp := strings.Join([]string{r.Name, r.URL, r.Method, r.Headers, r.Auth, r.BodyTemplate, r.TLS, r.Config}, "\x00")
	c.mu.Lock()
	hit, ok := c.m[r.ID]
	c.mu.Unlock()
	if ok && hit.fp == fp {
		return hit.ep, hit.err
	}
	var ep *Endpoint
	row, err := r.Row()
	if err == nil {
		ep, err = Build(ctx, row, c.res)
	}
	c.mu.Lock()
	c.m[r.ID] = cached{fp: fp, ep: ep, err: err}
	c.mu.Unlock()
	return ep, err
}
