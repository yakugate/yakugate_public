// Package secrets resolves secret REFERENCES to bytes, so credentials and keys
// live outside config rows and environment values. A ref is URI-style:
//
//	env://VAR_NAME             read from the process environment
//	file://relative/or/abs     read from a file (trailing whitespace trimmed)
//	vault://mount/path#field   HashiCorp Vault (Phase 2 — not implemented yet)
//
// Anything that is NOT a ref is returned to the caller unchanged by IsRef/Maybe,
// so plain inline values keep working everywhere during migration.
//
// Two properties matter operationally:
//   - resolution is CACHED (default 5 min) so it never happens per message;
//   - resolution is FAIL-STATIC: if a provider breaks, the last known value is
//     served (loudly) rather than taking the gateway down. A secret store outage
//     must not stop traffic.
//
// Secret VALUES are never logged and never placed in error strings. [SCOPE P1-SECRETS]
package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	// ErrUnknownScheme means the ref has no registered provider.
	ErrUnknownScheme = errors.New("secrets: unknown ref scheme")
	// ErrProviderUnavailable means the backend is not implemented/reachable.
	ErrProviderUnavailable = errors.New("secrets: provider unavailable")
	// ErrNotFound means the provider ran but the secret does not exist.
	ErrNotFound = errors.New("secrets: not found")
	// ErrEmpty means the secret resolved to nothing, which is never valid.
	ErrEmpty = errors.New("secrets: resolved to an empty value")
)

// Provider fetches the bytes for one ref. Implementations MUST NOT include the
// secret value in returned errors.
type Provider interface {
	Resolve(ctx context.Context, ref string) ([]byte, error)
}

// AuditFunc is called on every resolution outcome. It receives the ref (safe to
// log — it names a location, not a value) and the error, never the value.
type AuditFunc func(ref string, err error)

// refSchemes are the schemes that actually name a secret. A value whose scheme
// is NOT in here is a literal, whatever it looks like.
//
// Adding a Provider for a new scheme means adding it here too, or values using
// it will be passed through as literals instead of resolved.
var refSchemes = map[string]bool{
	"env":   true,
	"file":  true,
	"vault": true,
}

// IsRef reports whether s is a secret reference rather than a literal.
//
// It matches against a KNOWN list of schemes rather than treating any
// "<letters>://..." as a reference, and that distinction is load-bearing: a
// database DSN is exactly that shape. "mongodb://host:27017/db" and
// "sqlserver://user:pass@host:1433?database=x" are ordinary connection strings,
// and the old rule classified both as secret references with an unknown scheme
// — so the config layer REFUSED them and the store was never built. It went
// unnoticed because a MySQL DSN is "user:pass@tcp(host:port)/db", which has no
// "://" in it at all, and MySQL is the only backend anyone had configured.
// Found 2026-09-09 by pointing the queue store at a real MongoDB.
//
// The trade: a mistyped scheme ("filee://etc/secret") is now treated as a
// literal instead of erroring. That is the better failure — a wrong literal
// fails immediately and visibly at the thing that uses it, whereas the old
// behaviour broke valid configuration that had nothing to do with secrets.
func IsRef(s string) bool {
	scheme, _ := schemeOf(s)
	return scheme != "" && refSchemes[scheme]
}

func schemeOf(ref string) (scheme, rest string) {
	i := strings.Index(ref, "://")
	if i < 0 {
		return "", ref
	}
	return strings.ToLower(ref[:i]), ref[i+3:]
}

type entry struct {
	val []byte
	at  time.Time
}

// Registry routes refs to providers and owns the cache + audit hook.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
	cache     map[string]entry
	ttl       time.Duration
	audit     AuditFunc
}

// Option configures a Registry.
type Option func(*Registry)

// WithTTL overrides the cache lifetime (default 5m).
func WithTTL(d time.Duration) Option {
	return func(r *Registry) {
		if d > 0 {
			r.ttl = d
		}
	}
}

// WithAudit installs the resolution audit hook.
func WithAudit(f AuditFunc) Option { return func(r *Registry) { r.audit = f } }

// WithProvider registers (or replaces) a provider for a scheme.
func WithProvider(scheme string, p Provider) Option {
	return func(r *Registry) { r.providers[strings.ToLower(scheme)] = p }
}

// New builds a Registry with the on-prem providers registered: env and file are
// real, vault is a stub until Phase 2. Cloud KMS schemes are deliberately absent
// — adding them is a WithProvider call, no change here.
func New(opts ...Option) *Registry {
	r := &Registry{
		providers: map[string]Provider{
			"env":   EnvProvider{},
			"file":  FileProvider{},
			"vault": UnavailableProvider{Name: "vault"},
		},
		cache: map[string]entry{},
		ttl:   5 * time.Minute,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Resolve returns the bytes for a ref, using the cache when fresh. On provider
// failure it serves the last known value if there is one (fail-static) and still
// reports the failure to the audit hook.
func (r *Registry) Resolve(ctx context.Context, ref string) ([]byte, error) {
	if ref == "" {
		return nil, ErrEmpty
	}
	r.mu.RLock()
	e, cached := r.cache[ref]
	r.mu.RUnlock()
	if cached && time.Since(e.at) < r.ttl {
		return e.val, nil
	}

	scheme, _ := schemeOf(ref)
	r.mu.RLock()
	p, ok := r.providers[scheme]
	r.mu.RUnlock()
	if !ok {
		err := fmt.Errorf("%w: %q", ErrUnknownScheme, scheme)
		r.report(ref, err)
		return nil, err
	}

	val, err := p.Resolve(ctx, ref)
	if err == nil && len(val) == 0 {
		err = ErrEmpty
	}
	if err != nil {
		r.report(ref, err)
		if cached {
			// Fail-static: a secret-store outage must not stop traffic. The stale
			// value keeps the gateway running; the audit hook carries the alarm.
			return e.val, nil
		}
		return nil, err
	}
	r.mu.Lock()
	r.cache[ref] = entry{val: val, at: time.Now()}
	r.mu.Unlock()
	r.report(ref, nil)
	return val, nil
}

// ResolveString is Resolve with a string result.
func (r *Registry) ResolveString(ctx context.Context, ref string) (string, error) {
	b, err := r.Resolve(ctx, ref)
	return string(b), err
}

// Maybe resolves s when it is a ref, and returns it unchanged when it is a plain
// literal — the migration path for values that may be either.
func (r *Registry) Maybe(ctx context.Context, s string) (string, error) {
	if !IsRef(s) {
		return s, nil
	}
	return r.ResolveString(ctx, s)
}

// Invalidate drops a cached ref (use after a known rotation).
func (r *Registry) Invalidate(ref string) {
	r.mu.Lock()
	delete(r.cache, ref)
	r.mu.Unlock()
}

func (r *Registry) report(ref string, err error) {
	if r.audit != nil {
		r.audit(ref, err)
	}
}

// --- providers ---

// EnvProvider resolves env://VAR_NAME.
type EnvProvider struct{}

func (EnvProvider) Resolve(_ context.Context, ref string) ([]byte, error) {
	_, name := schemeOf(ref)
	v, ok := os.LookupEnv(name)
	if !ok {
		return nil, fmt.Errorf("%w: env %s", ErrNotFound, name)
	}
	return []byte(v), nil
}

// FileProvider resolves file://path. Trailing newlines are trimmed so a key file
// written by `echo` behaves the same as one written without one.
type FileProvider struct{}

func (FileProvider) Resolve(_ context.Context, ref string) ([]byte, error) {
	_, path := schemeOf(ref)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: file %s", ErrNotFound, path)
		}
		return nil, fmt.Errorf("secrets: read %s: %w", path, err)
	}
	return []byte(strings.TrimRight(string(b), "\r\n")), nil
}

// UnavailableProvider is a registered-but-not-implemented scheme. It exists so a
// ref using it fails with a clear reason instead of "unknown scheme".
type UnavailableProvider struct{ Name string }

func (u UnavailableProvider) Resolve(_ context.Context, _ string) ([]byte, error) {
	return nil, fmt.Errorf("%w: %s is not implemented yet", ErrProviderUnavailable, u.Name)
}
