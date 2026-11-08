// Package httpx is Yakugate's shared outbound HTTP client (D-087): one pooled,
// timeout-bounded, TLS-aware client class used by every process that calls an
// external HTTP service — the HLR live dip (P1), JWKS key fetching (P1 auth),
// and later the config-driven vendor adapter, DLR/MO posters, and CRP/KMS
// callers. Keeping one client means connection pooling, timeouts, and TLS are
// configured once and correctly, instead of each caller hand-rolling an
// *http.Client with subtly different (often unbounded) settings.
//
// This is deliberately a THIN, dependency-free core: pooling + a per-call
// timeout + context propagation + a hard response-size cap. Higher-level
// concerns that only some callers need (circuit breaker, idempotency-aware
// retries, per-endpoint rate limiting, latency counters) are layered on by the
// caller or added here behind options as they are actually needed — not
// speculatively.
package httpx

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"time"
)

// Client is a pooled outbound HTTP client. Safe for concurrent use; create one
// per logical upstream (or one shared) and reuse it — never per request, or the
// connection pool is defeated.
type Client struct {
	hc      *http.Client
	timeout time.Duration
	maxBody int64
}

// Option configures a Client.
type Option func(*Client)

// WithTimeout sets the per-call timeout (the ceiling on a single Do, applied
// via the request context). Default 10s. On the HLR hot path this is set far
// lower (see YAKU_HLR_DIP_TIMEOUT_MS) so a slow provider can never exceed the
// submit latency budget.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithMaxBodyBytes caps how much of a response body Do will read (defence
// against a hostile or misconfigured upstream streaming an unbounded body into
// memory). Default 1 MiB.
func WithMaxBodyBytes(n int64) Option {
	return func(c *Client) {
		if n > 0 {
			c.maxBody = n
		}
	}
}

// WithInsecureSkipVerify disables TLS certificate verification. ONLY for a
// controlled test/dev upstream — never in production. Off by default.
func WithInsecureSkipVerify(skip bool) Option {
	return func(c *Client) {
		if tr, ok := c.hc.Transport.(*http.Transport); ok && tr.TLSClientConfig != nil {
			tr.TLSClientConfig.InsecureSkipVerify = skip
		}
	}
}

// New builds a pooled Client. The transport bounds idle connections and dial/
// TLS handshake time so a dead upstream fails fast rather than hanging a pool
// slot indefinitely.
func New(opts ...Option) *Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   50,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	c := &Client{
		// No http.Client.Timeout: the per-call timeout is applied via the
		// request context in Do, which is context-cancellation-correct and lets
		// each call carry its own deadline off one shared client.
		hc:      &http.Client{Transport: tr},
		timeout: 10 * time.Second,
		maxBody: 1 << 20, // 1 MiB
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Response is a completed request's outcome: the status and the (size-capped)
// body already read into memory, so the caller never has to remember to close
// a body or bound its own read.
type Response struct {
	Status int
	Body   []byte
	Header http.Header
}

// Do executes one request, bounded by the client's timeout (layered under any
// shorter deadline already on ctx) and its response-size cap. The body is fully
// read and closed before returning, so there is nothing for the caller to leak.
func (c *Client) Do(ctx context.Context, method, url string, header http.Header, body io.Reader) (*Response, error) {
	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(cctx, method, url, body)
	if err != nil {
		return nil, err
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(io.LimitReader(resp.Body, c.maxBody))
	if err != nil {
		return nil, err
	}
	return &Response{Status: resp.StatusCode, Body: b, Header: resp.Header}, nil
}
