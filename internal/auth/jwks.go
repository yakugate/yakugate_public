package auth

// JWKS (JSON Web Key Set) resolution (P1 scope finalize 2026-08-09 — moved into
// this phase from Phase 2, now that internal/httpx exists). When an account
// configures jwt.jwks_uri instead of an inline public_key, the verifier fetches
// the issuer's key set over the shared httpx client, matches the token's `kid`
// header to a key in the set, and verifies against it. Fetched key sets are
// cached per URI with a TTL so verification is not a network call per request;
// a cache miss on an unknown kid refetches once (handling key rotation) before
// failing.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/httpx"
)

// jwksTTL is how long a fetched key set is trusted before a refetch. Short
// enough to pick up a rotated set within minutes, long enough that steady
// traffic never fetches per request.
const jwksTTL = 5 * time.Minute

// jwksResolver fetches and caches key sets. Safe for concurrent use.
type jwksResolver struct {
	client *httpx.Client
	mu     sync.Mutex
	cache  map[string]*jwksEntry // uri -> keys
}

type jwksEntry struct {
	keys    map[string]any // kid -> *rsa.PublicKey | *ecdsa.PublicKey
	fetched time.Time
}

// jwks lazily builds and returns the shared JWKS resolver (one httpx client +
// key-set cache for the whole authenticator).
func (a *Authenticator) jwks() *jwksResolver {
	a.jwksOnce.Do(func() { a.jwksRes = newJWKSResolver() })
	return a.jwksRes
}

func newJWKSResolver() *jwksResolver {
	return &jwksResolver{
		client: httpx.New(httpx.WithTimeout(5 * time.Second)),
		cache:  map[string]*jwksEntry{},
	}
}

// Key returns the public key for (uri, kid), fetching/refetching as needed. An
// unknown kid against a cached set triggers exactly one refetch (a rotation may
// have added the key), then fails if still absent.
func (r *jwksResolver) Key(uri, kid string) (any, error) {
	if k, ok := r.lookup(uri, kid, false); ok {
		return k, nil
	}
	// miss (stale, or unknown kid) -> refetch once
	if err := r.fetch(uri); err != nil {
		return nil, err
	}
	if k, ok := r.lookup(uri, kid, true); ok {
		return k, nil
	}
	return nil, fmt.Errorf("kid %q not in key set", kid)
}

// lookup returns a cached key. forceFresh requires the entry to be post-fetch
// (ignores the TTL) — used right after a refetch so we don't loop.
func (r *jwksResolver) lookup(uri, kid string, forceFresh bool) (any, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.cache[uri]
	if e == nil {
		return nil, false
	}
	if !forceFresh && time.Since(e.fetched) > jwksTTL {
		return nil, false // stale -> caller refetches
	}
	k, ok := e.keys[kid]
	return k, ok
}

// fetch pulls the key set and replaces the cache entry.
func (r *jwksResolver) fetch(uri string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := r.client.Do(ctx, http.MethodGet, uri, nil, nil)
	if err != nil {
		return err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return fmt.Errorf("jwks status %d", resp.Status)
	}
	keys, err := parseJWKS(resp.Body)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.cache[uri] = &jwksEntry{keys: keys, fetched: time.Now()}
	r.mu.Unlock()
	return nil
}

// jwk is one key in a JWKS document (only the fields we verify with).
type jwk struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"` // RSA | EC
	N   string `json:"n"`   // RSA modulus (base64url)
	E   string `json:"e"`   // RSA exponent (base64url)
	Crv string `json:"crv"` // EC curve, e.g. P-256
	X   string `json:"x"`   // EC x (base64url)
	Y   string `json:"y"`   // EC y (base64url)
}

// parseJWKS decodes a key set into kid -> crypto public key. Keys we can't use
// (unsupported kty/crv) are skipped, not fatal — a set may legitimately carry
// keys for algorithms this account doesn't use.
func parseJWKS(body []byte) (map[string]any, error) {
	var doc struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("jwks bad json: %w", err)
	}
	out := map[string]any{}
	for _, k := range doc.Keys {
		if k.Kid == "" {
			continue
		}
		switch k.Kty {
		case "RSA":
			if pub, err := k.rsa(); err == nil {
				out[k.Kid] = pub
			}
		case "EC":
			if pub, err := k.ecdsa(); err == nil {
				out[k.Kid] = pub
			}
		}
	}
	if len(out) == 0 {
		return nil, errors.New("jwks has no usable keys")
	}
	return out, nil
}

func (k jwk) rsa() (*rsa.PublicKey, error) {
	nb, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, err
	}
	eb, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, err
	}
	e := 0
	for _, b := range eb {
		e = e<<8 | int(b)
	}
	if e == 0 {
		return nil, errors.New("rsa exponent zero")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: e}, nil
}

func (k jwk) ecdsa() (*ecdsa.PublicKey, error) {
	if k.Crv != "P-256" { // ES256 only, matching the verifier
		return nil, fmt.Errorf("unsupported EC curve %q", k.Crv)
	}
	xb, err := base64.RawURLEncoding.DecodeString(k.X)
	if err != nil {
		return nil, err
	}
	yb, err := base64.RawURLEncoding.DecodeString(k.Y)
	if err != nil {
		return nil, err
	}
	return &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     new(big.Int).SetBytes(xb),
		Y:     new(big.Int).SetBytes(yb),
	}, nil
}
