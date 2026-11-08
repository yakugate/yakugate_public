// Package outhttp is the connection pool the gateway's outbound HTTP calls
// share: a vendor's send and status calls, a customer's receipt and
// incoming-message webhooks, provider lookups.
//
// Why it exists (load test of 2026-10-13, Saleem: "out throughput is not
// satisfactory"): every one of those callers made a plain http.Client, which
// uses Go's default transport, and that keeps TWO idle connections a host.
// A vendor call set to run 64 — or 1,000 — at a time therefore opened a new
// TCP connection for nearly every message and closed it again: a connect, a
// close and (over TLS) a handshake a message, on our side and on the
// vendor's, and a local port held for a minute after each. Here a host keeps
// as many idle connections as the calls in flight need, so a connection is
// made once and used again.
package outhttp

import (
	"crypto/tls"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultIdlePerHost is how many idle connections are kept per host when
// YAKU_HTTP_IDLE_PER_HOST does not say. An idle connection is closed after
// IdleFor, so a burst leaves nothing open for long.
const (
	DefaultIdlePerHost = 512
	IdleFor            = 90 * time.Second
)

// IdlePerHost is YAKU_HTTP_IDLE_PER_HOST (1 to 10,000), else the default.
func IdlePerHost() int {
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_HTTP_IDLE_PER_HOST"))); err == nil && n >= 1 && n <= 10000 {
		return n
	}
	return DefaultIdlePerHost
}

// New makes a transport like Go's default one — proxy from the environment,
// the same dial and handshake timeouts, HTTP/2 when the host offers it — that
// keeps IdlePerHost idle connections a host and no limit across hosts. tc is
// the TLS settings of one endpoint, nil for the system's.
func New(tc *tls.Config) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = tc
	t.MaxIdleConns = 0 // across all hosts: no limit of its own
	t.MaxIdleConnsPerHost = IdlePerHost()
	t.IdleConnTimeout = IdleFor
	return t
}

var (
	once   sync.Once
	shared *http.Transport
)

// Shared is the one transport of this process for calls with no TLS settings
// of their own. Calls to the same host share its connections whoever makes
// them.
func Shared() *http.Transport {
	once.Do(func() { shared = New(nil) })
	return shared
}

// Client is an http.Client on the shared transport with this timeout.
func Client(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: Shared()}
}
