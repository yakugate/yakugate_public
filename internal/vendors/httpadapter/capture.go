package httpadapter

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// A call as it went, kept raw (Saleem 4-5 Nov 2026: customer webhooks first,
// then "check if we save vendor responses and requests in raw for mt, if not
// then save them too"). Secrets are masked before anything is kept: an
// Authorization or cookie header, any header, query value, JSON, form or XML
// field named like a key, token, secret, password or signature, and a URL's
// password. Bodies are cut at their caps, on a character boundary, and say so.
//
// pushcall keeps a customer webhook's call with these; a vendor call is kept
// by asking doCall to (WithCallRecorder).

// How much of a call is kept.
const (
	ReqBodyCap   = 16 << 10
	RespBodyCap  = 8 << 10
	HeadersCap   = 4 << 10
	URLCap       = 2000
	ErrorCap     = 500
	MaskedSecret = "***"
)

// CapturedCall is one vendor call as it went.
type CapturedCall struct {
	Method, URL string
	ReqHeaders  string // "Name: value" lines, secrets masked
	ReqBody     string
	ReqCut      bool
	Status      int // 0 = no answer
	RespHeaders string
	RespBody    string
	RespCut     bool
	Err         string // the call could not be made, or had no answer
	SentAt      time.Time
	Took        time.Duration
}

// CallRecorder keeps the calls doCall makes for one context.
type CallRecorder struct {
	mu    sync.Mutex
	calls []CapturedCall
}

type recorderKey struct{}

// WithCallRecorder is ctx with a recorder: every call made with it (a send, a
// retry after a new login, the login itself, each failover host) is kept, in
// order.
func WithCallRecorder(ctx context.Context) (context.Context, *CallRecorder) {
	r := &CallRecorder{}
	return context.WithValue(ctx, recorderKey{}, r), r
}

func recorderFrom(ctx context.Context) *CallRecorder {
	r, _ := ctx.Value(recorderKey{}).(*CallRecorder)
	return r
}

func (r *CallRecorder) add(c CapturedCall) {
	r.mu.Lock()
	r.calls = append(r.calls, c)
	r.mu.Unlock()
}

// Last is the newest call kept: the one whose answer counted.
func (r *CallRecorder) Last() (CapturedCall, bool) {
	if r == nil {
		return CapturedCall{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return CapturedCall{}, false
	}
	return r.calls[len(r.calls)-1], true
}

// Calls is every call kept, in order.
func (r *CallRecorder) Calls() []CapturedCall {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]CapturedCall(nil), r.calls...)
}

// captureRequest starts a call's record from its request.
func captureRequest(req *http.Request) CapturedCall {
	c := CapturedCall{Method: req.Method, URL: MaskURL(req.URL), ReqHeaders: MaskHeaders(req.Header, req.Host), SentAt: time.Now()}
	c.ReqBody, c.ReqCut = RequestBody(req)
	return c
}

// RequestBody reads a request's body without spending it, secrets masked and
// cut at ReqBodyCap: from GetBody (what http.NewRequest sets for a string,
// bytes or reader of them), else by reading it and putting a copy back.
func RequestBody(req *http.Request) (string, bool) {
	if req.Body == nil || req.Body == http.NoBody {
		return "", false
	}
	var b []byte
	if req.GetBody != nil {
		rc, err := req.GetBody()
		if err != nil {
			return "", false
		}
		b, _ = io.ReadAll(rc)
		rc.Close()
	} else {
		b, _ = io.ReadAll(req.Body)
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(b))
	}
	return Keep(MaskBody(b), ReqBodyCap)
}

// Keep is text cut at max bytes, on a character boundary; cut says it was.
func Keep(b []byte, max int) (string, bool) {
	if len(b) <= max {
		return strings.ToValidUTF8(string(b), "\uFFFD"), false
	}
	b = b[:max]
	for len(b) > 0 && !utf8.Valid(b) {
		b = b[:len(b)-1]
	}
	return string(b), true
}

// Cut is s cut at max bytes, on a character boundary.
func Cut(s string, max int) string {
	out, _ := Keep([]byte(s), max)
	return out
}

// SecretName is a header, query, JSON or form name whose value is a secret.
var SecretName = regexp.MustCompile(`(?i)(authori[sz]ation|cookie|token|secret|passw(or)?d|pwd|api[-_]?key|apikey|access[-_]?key|signature|^auth$|^key$|credential)`)

// MaskHeaders is the headers as "Name: value" lines, by name, a secret's
// value masked. host, when set, comes first (Go sends it apart from Header).
func MaskHeaders(h http.Header, host string) string {
	var lines []string
	if host != "" {
		lines = append(lines, "Host: "+host)
	}
	names := make([]string, 0, len(h))
	for k := range h {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		for _, v := range h[k] {
			if SecretName.MatchString(k) {
				v = maskedValue(v)
			}
			lines = append(lines, k+": "+v)
		}
	}
	return Cut(strings.Join(lines, "\n"), HeadersCap)
}

// maskedValue keeps an Authorization scheme ("Bearer ***").
func maskedValue(v string) string {
	if scheme, _, ok := strings.Cut(v, " "); ok && len(scheme) <= 10 && !strings.ContainsAny(scheme, "=:") {
		return scheme + " " + MaskedSecret
	}
	return MaskedSecret
}

// MaskURL is the URL with a password and secret query values masked.
func MaskURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	c := *u
	if c.User != nil {
		if _, has := c.User.Password(); has {
			c.User = url.UserPassword(c.User.Username(), MaskedSecret)
		}
	}
	if c.RawQuery != "" {
		q := c.Query()
		changed := false
		for k := range q {
			if SecretName.MatchString(k) {
				for i := range q[k] {
					q[k][i] = MaskedSecret
				}
				changed = true
			}
		}
		if changed {
			c.RawQuery = q.Encode()
		}
	}
	return Cut(c.String(), URLCap)
}

// Secret values inside a body, found by their names: a JSON string or number
// ("password": "x"), a form value (password=x) and an XML element
// (<password>x</password>). The body is otherwise kept exactly as sent.
var (
	jsonSecret = regexp.MustCompile(`("([^"\\]*)"\s*:\s*)("(?:[^"\\]|\\.)*"|-?[0-9][0-9.eE+-]*)`)
	formSecret = regexp.MustCompile(`(^|&)([^=&]+)=([^&]*)`)
	xmlSecret  = regexp.MustCompile(`(<([A-Za-z_][\w.:-]*)(?:\s[^>]*)?>)([^<]*)(</([A-Za-z_][\w.:-]*)>)`)
)

// MaskBody is a body with its secret values masked.
func MaskBody(b []byte) []byte {
	t := bytes.TrimSpace(b)
	if len(t) == 0 {
		return b
	}
	switch t[0] {
	case '{', '[':
		return jsonSecret.ReplaceAllFunc(b, func(m []byte) []byte {
			s := jsonSecret.FindSubmatch(m)
			if !SecretName.Match(s[2]) {
				return m
			}
			return append(append([]byte{}, s[1]...), []byte(`"`+MaskedSecret+`"`)...)
		})
	case '<':
		return xmlSecret.ReplaceAllFunc(b, func(m []byte) []byte {
			s := xmlSecret.FindSubmatch(m)
			if !SecretName.Match(s[2]) || !bytes.Equal(s[2], s[5]) {
				return m
			}
			return append(append(append([]byte{}, s[1]...), MaskedSecret...), s[4]...)
		})
	}
	if bytes.ContainsRune(t, '=') && !bytes.ContainsAny(t, " \n") {
		return formSecret.ReplaceAllFunc(b, func(m []byte) []byte {
			s := formSecret.FindSubmatch(m)
			name, _ := url.QueryUnescape(string(s[2]))
			if !SecretName.MatchString(name) {
				return m
			}
			return append(append(append([]byte{}, s[1]...), s[2]...), []byte("="+MaskedSecret)...)
		})
	}
	return b
}

// TestAddCall keeps c on r as if a call had been made: for other packages'
// tests, which have no vendor to call.
func TestAddCall(r *CallRecorder, c CapturedCall) { r.add(c) }
