// Package kms provides remote KEY operations for envelope encryption — the
// counterpart to internal/secrets (which resolves plain secret VALUES).
// Where internal/secrets.Provider fetches bytes a caller then uses directly,
// KeyManager NEVER releases the key itself: it performs Encrypt/Decrypt
// remotely, so a KEK backed by a real KMS (GCP/AWS/Azure/HSM) never exists
// as plaintext bytes inside this process. internal/crypto is the one caller —
// it uses a KeyManager to wrap/unwrap DEKs instead of the local-AES-GCM path
// it uses today for the dev file://env:// KEK case. [SCOPE P1-KMS]
//
// Multi-tenant: each tenant (companies row) is configured with its own KEK
// via the tenant_kms_config table (migration config/0043) — kms_provider_id
// selects which catalog entry (kms_providers), key_ref names the specific
// KEK within it. A Registry resolves a keyRef's scheme to the right
// KeyManager, mirroring internal/secrets.Registry's scheme-routing exactly.
package kms

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrUnknownScheme means the ref has no registered KeyManager.
	ErrUnknownScheme = errors.New("kms: unknown key ref scheme")
	// ErrNotSupported means the backend doesn't offer this operation — callers
	// fall back to the locally-generate-then-wrap path (every backend except
	// AWS KMS needs this fallback for GenerateDataKey; internal/crypto already
	// generates DEKs locally today, so this is simply "keep doing that").
	ErrNotSupported = errors.New("kms: operation not supported by this backend")
	// ErrUnavailable means the backend is reachable in principle but the call
	// failed (network, auth, throttling) — distinct from ErrNotSupported.
	ErrUnavailable = errors.New("kms: backend unavailable")
)

// KeyManager performs key operations WITHOUT ever returning the key itself.
// Implementations must never log plaintext or the KEK; only ciphertext/refs
// are safe to log. [SCOPE P1-KMS]
type KeyManager interface {
	// Encrypt wraps plaintext under the named key. For a KEK, plaintext is a
	// locally-generated DEK; for a data value directly, plaintext is the
	// value itself — callers decide which, KeyManager doesn't care.
	Encrypt(ctx context.Context, keyRef string, plaintext []byte) ([]byte, error)
	// Decrypt reverses Encrypt.
	Decrypt(ctx context.Context, keyRef string, ciphertext []byte) ([]byte, error)
	// GenerateDataKey asks the backend to generate a data key directly,
	// returning both the plaintext (used immediately, never persisted) and
	// the wrapped form (persisted). Backends without a native call return
	// ErrNotSupported — the caller (internal/crypto) already knows how to
	// generate a DEK locally and wrap it via Encrypt instead.
	GenerateDataKey(ctx context.Context, keyRef string) (plaintextDEK, wrappedDEK []byte, err error)
}

func schemeOf(ref string) (scheme, rest string) {
	i := strings.Index(ref, "://")
	if i < 0 {
		return "", ref
	}
	return strings.ToLower(ref[:i]), ref[i+3:]
}

// Registry routes a key ref to the KeyManager registered for its scheme.
// Mirrors internal/secrets.Registry's shape deliberately — same mental model,
// different job (key ops, never a raw value).
type Registry struct {
	mu       sync.RWMutex
	managers map[string]KeyManager
}

// NewRegistry builds an empty Registry — callers register backends with
// WithManager; there is no default (unlike internal/secrets.New, which
// always has env/file). A KeyManager backend always needs real configuration
// (a project, a vault addr, an HSM library path) — there is no meaningful
// zero-config default the way env://file:// are for plain secrets.
func NewRegistry() *Registry {
	return &Registry{managers: map[string]KeyManager{}}
}

// WithManager registers (or replaces) the KeyManager for a scheme.
func (r *Registry) WithManager(scheme string, m KeyManager) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.managers[strings.ToLower(scheme)] = m
}

// Encrypt resolves keyRef's scheme and delegates.
func (r *Registry) Encrypt(ctx context.Context, keyRef string, plaintext []byte) ([]byte, error) {
	m, err := r.managerFor(keyRef)
	if err != nil {
		return nil, err
	}
	return m.Encrypt(ctx, keyRef, plaintext)
}

// Decrypt resolves keyRef's scheme and delegates.
func (r *Registry) Decrypt(ctx context.Context, keyRef string, ciphertext []byte) ([]byte, error) {
	m, err := r.managerFor(keyRef)
	if err != nil {
		return nil, err
	}
	return m.Decrypt(ctx, keyRef, ciphertext)
}

// GenerateDataKey resolves keyRef's scheme and delegates.
func (r *Registry) GenerateDataKey(ctx context.Context, keyRef string) ([]byte, []byte, error) {
	m, err := r.managerFor(keyRef)
	if err != nil {
		return nil, nil, err
	}
	return m.GenerateDataKey(ctx, keyRef)
}

// HasManager reports whether keyRef's scheme has a registered KeyManager —
// internal/crypto uses this to decide remote-KEK vs. today's local-KEK path
// without having to attempt (and fail) an operation first.
func (r *Registry) HasManager(keyRef string) bool {
	scheme, _ := schemeOf(keyRef)
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.managers[scheme]
	return ok
}

func (r *Registry) managerFor(keyRef string) (KeyManager, error) {
	scheme, _ := schemeOf(keyRef)
	r.mu.RLock()
	m, ok := r.managers[scheme]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownScheme, scheme)
	}
	return m, nil
}
