package kms

// A key service reached over HTTP, configured rather than coded
// (PO 2026-09-13).
//
// The two calls a key service needs — ENCRYPT (lock a data key under a named
// master key) and DECRYPT (unlock it) — are ordinary rows of the central
// http_endpoints collection, built with the same request builder and run by
// the same code (internal/vendors/httpadapter) as every other outbound call:
// vendor sends, currency rates, HLR lookups. A KMS provider is one more row of
// type 'kms' that points at the pair:
//
//	provider row  config {"kind":"http", "encrypt_endpoint_id": 41,
//	                      "decrypt_endpoint_id": 42, "encoding": "base64"}
//	request rows  config.kms_role "encrypt" | "decrypt", params and response
//	              in the builder's usual shape
//
// Request values come from a fixed vocabulary: {key} (the master key's name at
// the provider), {plaintext} (the data key, encoded) on encrypt, {ciphertext}
// (the locked data key exactly as encrypt returned it) on decrypt. The answer
// is the response alias "ciphertext" on encrypt and "plaintext" on decrypt.
// A header may hold {{secret:REF}}, resolved here through internal/secrets, so
// a key service's token need not sit in the config database.
//
// What travels to the provider is a random 32-byte DATA key, never a message:
// envelope encryption (internal/crypto) locks one data key per company per
// window and encrypts message bodies locally with it.
//
// Key refs look like http-kms://<provider id>/<key name>. The provider row's
// id, not its URLs, is what a stored data key remembers, so an operator can
// point a provider at new request rows without touching a stored key.

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// HTTPScheme is the key-ref scheme of an HTTP key service.
const HTTPScheme = "http-kms"

// HTTPProvider is a KMS provider row: which request rows lock and unlock.
type HTTPProvider struct {
	Kind              string `json:"kind"` // "http" (the default)
	EncryptEndpointID int64  `json:"encrypt_endpoint_id"`
	DecryptEndpointID int64  `json:"decrypt_endpoint_id"`
	// Encoding of the data key in {plaintext} and in decrypt's answer:
	// "base64" (the default, what Vault uses) or "hex".
	Encoding string `json:"encoding,omitempty"`
	// Active mirrors http_endpoints.is_active: a disabled provider refuses
	// every call rather than quietly working.
	Active bool `json:"-"`
}

// Validate reports what is missing from a provider row.
func (p HTTPProvider) Validate() error {
	if p.Kind != "" && p.Kind != "http" {
		return fmt.Errorf("kms: provider kind %q is not an HTTP key service", p.Kind)
	}
	if p.EncryptEndpointID <= 0 || p.DecryptEndpointID <= 0 {
		return fmt.Errorf("kms: the provider must name an encrypt endpoint and a decrypt endpoint")
	}
	switch p.Encoding {
	case "", "base64", "hex":
	default:
		return fmt.Errorf("kms: encoding must be base64 or hex")
	}
	return nil
}

// Endpoint is one request row, as the store reads it.
type Endpoint struct {
	Row    httpadapter.EndpointRow
	Role   string // config.kms_role: "encrypt" | "decrypt"
	Active bool
}

// ProviderSource loads provider rows and their request rows. The config store
// implements it.
type ProviderSource interface {
	KMSProvider(ctx context.Context, id int64) (HTTPProvider, bool, error)
	KMSEndpoint(ctx context.Context, id int64) (Endpoint, bool, error)
}

// SecretResolver resolves {{secret:REF}} in headers.
type SecretResolver interface {
	ResolveString(ctx context.Context, ref string) (string, error)
}

// HTTPKeyManager performs the two calls. One instance serves every provider;
// the key ref says which.
type HTTPKeyManager struct {
	src     ProviderSource
	secrets SecretResolver
	ttl     time.Duration

	mu    sync.Mutex
	cache map[int64]cachedProvider
}

// cachedProvider is a provider with its two calls built, ready to run.
type cachedProvider struct {
	p                HTTPProvider
	encrypt, decrypt builtCall
	at               time.Time
}

type builtCall struct {
	call    httpadapter.Call
	timeout time.Duration
	name    string
}

// providerTTL: an edited provider or request row is picked up within this
// long. A data key is locked once per window and unlocked once per process,
// so a short cache costs nothing.
const providerTTL = 30 * time.Second

// NewHTTPKeyManager builds the manager. secrets may be nil (no secret refs).
func NewHTTPKeyManager(src ProviderSource, secrets SecretResolver) *HTTPKeyManager {
	return &HTTPKeyManager{src: src, secrets: secrets, ttl: providerTTL, cache: map[int64]cachedProvider{}}
}

// HTTPKeyRef builds the key ref for a master key at a provider.
func HTTPKeyRef(providerID int64, key string) string {
	return HTTPScheme + "://" + strconv.FormatInt(providerID, 10) + "/" + key
}

// ParseHTTPKeyRef splits http-kms://<id>/<key>.
func ParseHTTPKeyRef(ref string) (id int64, key string, err error) {
	scheme, rest := schemeOf(ref)
	if scheme != HTTPScheme {
		return 0, "", fmt.Errorf("kms: %q is not an %s ref", ref, HTTPScheme)
	}
	i := strings.IndexByte(rest, '/')
	if i <= 0 || i == len(rest)-1 {
		return 0, "", fmt.Errorf("kms: malformed %s ref %q (want %s://<provider id>/<key>)", HTTPScheme, ref, HTTPScheme)
	}
	id, err = strconv.ParseInt(rest[:i], 10, 64)
	if err != nil || id <= 0 {
		return 0, "", fmt.Errorf("kms: malformed provider id in %q", ref)
	}
	return id, rest[i+1:], nil
}

// Forget drops a cached provider, so the next call reads it (and its request
// rows) afresh — the connection test runs right after an operator edits one.
func (m *HTTPKeyManager) Forget(id int64) {
	m.mu.Lock()
	delete(m.cache, id)
	m.mu.Unlock()
}

func (m *HTTPKeyManager) provider(ctx context.Context, id int64) (cachedProvider, error) {
	m.mu.Lock()
	c, ok := m.cache[id]
	m.mu.Unlock()
	if ok && time.Since(c.at) < m.ttl {
		return c, nil
	}
	fresh, err := m.load(ctx, id)
	if err != nil {
		var unavailable bool
		if ok {
			// Keep working on the last good copy through a config-store blip —
			// but never past a definite answer (a deleted or broken row).
			_, unavailable = err.(storeError)
		}
		if unavailable {
			return c, nil
		}
		return cachedProvider{}, err
	}
	m.mu.Lock()
	m.cache[id] = fresh
	m.mu.Unlock()
	return fresh, nil
}

// storeError marks a failure to READ config (as opposed to config that is
// wrong), so a cached provider may outlive it.
type storeError struct{ error }

func (m *HTTPKeyManager) load(ctx context.Context, id int64) (cachedProvider, error) {
	p, found, err := m.src.KMSProvider(ctx, id)
	if err != nil {
		return cachedProvider{}, storeError{fmt.Errorf("%w: reading KMS provider %d: %v", ErrUnavailable, id, err)}
	}
	if !found {
		return cachedProvider{}, fmt.Errorf("kms: KMS provider %d does not exist", id)
	}
	if err := p.Validate(); err != nil {
		return cachedProvider{}, fmt.Errorf("KMS provider %d: %w", id, err)
	}
	enc, err := m.buildCall(ctx, p.EncryptEndpointID, "encrypt")
	if err != nil {
		return cachedProvider{}, fmt.Errorf("KMS provider %d: %w", id, err)
	}
	dec, err := m.buildCall(ctx, p.DecryptEndpointID, "decrypt")
	if err != nil {
		return cachedProvider{}, fmt.Errorf("KMS provider %d: %w", id, err)
	}
	return cachedProvider{p: p, encrypt: enc, decrypt: dec, at: time.Now()}, nil
}

// needs is what each role must send and read back.
var needs = map[string]struct{ send, answer string }{
	"encrypt": {"plaintext", "ciphertext"},
	"decrypt": {"ciphertext", "plaintext"},
}

// buildCall loads one request row and builds it with the shared builder.
func (m *HTTPKeyManager) buildCall(ctx context.Context, id int64, role string) (builtCall, error) {
	ep, found, err := m.src.KMSEndpoint(ctx, id)
	if err != nil {
		return builtCall{}, storeError{fmt.Errorf("%w: reading %s endpoint %d: %v", ErrUnavailable, role, id, err)}
	}
	if !found {
		return builtCall{}, fmt.Errorf("kms: the %s endpoint %d does not exist (or is not of type kms)", role, id)
	}
	if ep.Role != "" && ep.Role != role {
		return builtCall{}, fmt.Errorf("kms: endpoint %d is a %s call, not a %s call", id, ep.Role, role)
	}
	if !ep.Active {
		return builtCall{}, fmt.Errorf("%w: the %s endpoint %d is disabled", ErrUnavailable, role, id)
	}
	call, err := httpadapter.CallFromEndpoint(ep.Row, "kms")
	if err != nil {
		return builtCall{}, err
	}
	n := needs[role]
	if !callSends(call, n.send) {
		return builtCall{}, fmt.Errorf("kms: the %s endpoint %d never sends %q — map a request field to it", role, id, n.send)
	}
	if _, ok := call.ExtractJSON[n.answer]; !ok {
		if _, ok := call.Extract[n.answer]; !ok {
			return builtCall{}, fmt.Errorf("kms: the %s endpoint %d does not read %q from the answer — add a response field with that alias", role, id, n.answer)
		}
	}
	timeout := time.Duration(ep.Row.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return builtCall{call: call, timeout: timeout, name: ep.Row.Name}, nil
}

// callSends reports whether a built call places {field} anywhere it sends.
func callSends(c httpadapter.Call, field string) bool {
	ph := "{" + field + "}"
	if strings.Contains(c.URL, ph) || strings.Contains(c.Body, ph) {
		return true
	}
	for _, set := range []map[string]string{c.Params, c.QueryParams, c.Headers} {
		for _, v := range set {
			if strings.Contains(v, ph) {
				return true
			}
		}
	}
	return false
}

// Encrypt locks plaintext (a data key) under the ref's master key.
func (m *HTTPKeyManager) Encrypt(ctx context.Context, keyRef string, plaintext []byte) ([]byte, error) {
	id, key, err := ParseHTTPKeyRef(keyRef)
	if err != nil {
		return nil, err
	}
	p, err := m.provider(ctx, id)
	if err != nil {
		return nil, err
	}
	encoded := base64.StdEncoding.EncodeToString(plaintext)
	if p.p.Encoding == "hex" {
		encoded = hex.EncodeToString(plaintext)
	}
	out, err := m.run(ctx, p, p.encrypt, map[string]string{"key": key, "plaintext": encoded}, "ciphertext")
	if err != nil {
		return nil, fmt.Errorf("encrypt at KMS provider %d: %w", id, err)
	}
	return []byte(out), nil
}

// Decrypt unlocks what Encrypt returned.
func (m *HTTPKeyManager) Decrypt(ctx context.Context, keyRef string, ciphertext []byte) ([]byte, error) {
	id, key, err := ParseHTTPKeyRef(keyRef)
	if err != nil {
		return nil, err
	}
	p, err := m.provider(ctx, id)
	if err != nil {
		return nil, err
	}
	out, err := m.run(ctx, p, p.decrypt, map[string]string{"key": key, "ciphertext": string(ciphertext)}, "plaintext")
	if err != nil {
		return nil, fmt.Errorf("decrypt at KMS provider %d: %w", id, err)
	}
	var plain []byte
	if p.p.Encoding == "hex" {
		plain, err = hex.DecodeString(out)
	} else {
		plain, err = base64.StdEncoding.DecodeString(out)
	}
	if err != nil {
		enc := p.p.Encoding
		if enc == "" {
			enc = "base64"
		}
		return nil, fmt.Errorf("decrypt at KMS provider %d: the answer is not %s", id, enc)
	}
	return plain, nil
}

// GenerateDataKey: the data key is made locally and locked with Encrypt.
func (m *HTTPKeyManager) GenerateDataKey(context.Context, string) ([]byte, []byte, error) {
	return nil, nil, ErrNotSupported
}

// secretRe matches {{secret:REF}}.
var secretRe = regexp.MustCompile(`\{\{\s*secret:([^}]+?)\s*\}\}`)

// run resolves the header secrets, sends the call through the shared runner
// and returns the answer field. Errors name the endpoint and the status,
// never a header value.
func (m *HTTPKeyManager) run(ctx context.Context, p cachedProvider, b builtCall, fields map[string]string, answer string) (string, error) {
	if !p.p.Active {
		return "", fmt.Errorf("%w: the provider is disabled", ErrUnavailable)
	}
	call := b.call
	if len(call.Headers) > 0 {
		hs := make(map[string]string, len(call.Headers))
		for k, v := range call.Headers {
			var rerr error
			v = secretRe.ReplaceAllStringFunc(v, func(s string) string {
				ref := secretRe.FindStringSubmatch(s)[1]
				if m.secrets == nil {
					rerr = fmt.Errorf("kms: header %s uses a secret but no secrets store is available", k)
					return ""
				}
				val, err := m.secrets.ResolveString(ctx, ref)
				if err != nil {
					// The ref is config, not a secret, so it may be named.
					rerr = fmt.Errorf("kms: cannot resolve secret %s: %v", ref, err)
					return ""
				}
				return strings.TrimSpace(val)
			})
			if rerr != nil {
				return "", rerr
			}
			hs[k] = v
		}
		call.Headers = hs
	}
	vals, err := httpadapter.FetchValues(ctx, &http.Client{Timeout: b.timeout}, call, fields)
	if err != nil {
		return "", fmt.Errorf("%w: endpoint %q: %v", ErrUnavailable, b.name, err)
	}
	out := strings.TrimSpace(vals[answer])
	if out == "" {
		return "", fmt.Errorf("%w: endpoint %q answered without %q", ErrUnavailable, b.name, answer)
	}
	return out, nil
}
