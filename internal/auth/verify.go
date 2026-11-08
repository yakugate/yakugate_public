package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/refdata"
)

// The JWT checks, on their own — no account, no reference cache, no lockout.
//
// They were extracted from Authenticator.verify (2026-09-23) because a second
// edge now needs them: an INBOUND vendor callback authenticates with the same
// schemes a client's submit does (PO 2026-09-23, "same auth management system
// as for accounts"). Two implementations of "is this JWT good" would drift,
// and the one that drifts is the one nobody is watching.
//
// What stayed behind in verify(): finding WHICH account a token belongs to,
// the lockout ladders, and the opaque-vs-specific refusal wording. Those are
// the submit edge's own concerns — the inbound edge already knows which
// endpoint it is talking to, because the URL says so.

// Claims are the registered JWT claims the gateway reads.
type Claims struct {
	Iss string `json:"iss"`
	Aud string `json:"aud"`
	Sub string `json:"sub"`
	Exp int64  `json:"exp"`
	Nbf int64  `json:"nbf"`
	Iat int64  `json:"iat"`
	Jti string `json:"jti"`
}

// Keys is the verification key material for one JWT config: HS256 candidate
// secrets (best first — a resolved secret_ref, then secret_ref_prev, so a key
// rotates without a flag day) and the parsed RS256/ES256 public key.
type Keys struct {
	HS256  [][]byte
	Public any
}

// SplitToken splits a compact JWT and decodes its header and claims. reason
// is "" on success; a non-empty reason means the token is not a JWT we can
// even read, which every caller treats as a plain failure.
func SplitToken(tok string) (hdrAlg, kid, signing string, sig []byte, claims Claims, claimRaw []byte, reason string) {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return "", "", "", nil, Claims{}, nil, "malformed token"
	}
	headerRaw, err := b64(parts[0])
	if err != nil {
		return "", "", "", nil, Claims{}, nil, "bad header"
	}
	claimRaw, err = b64(parts[1])
	if err != nil {
		return "", "", "", nil, Claims{}, nil, "bad payload"
	}
	var hdr struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
		Kid string `json:"kid"`
	}
	if json.Unmarshal(headerRaw, &hdr) != nil {
		return "", "", "", nil, Claims{}, nil, "bad header json"
	}
	if json.Unmarshal(claimRaw, &claims) != nil {
		return "", "", "", nil, Claims{}, nil, "bad claims json"
	}
	sig, err = b64(parts[2])
	if err != nil {
		return "", "", "", nil, Claims{}, nil, "bad signature encoding"
	}
	return hdr.Alg, hdr.Kid, parts[0] + "." + parts[1], sig, claims, claimRaw, ""
}

// VerifySignature checks the algorithm and the signature against one JWT
// config. "" means good; anything else is the reason, for the server log.
//
// `alg: none` is refused outright, and the header's algorithm must be the one
// the config names — accepting whatever the token asks for is the oldest JWT
// hole there is.
func VerifySignature(cfg refdata.JWTConfig, hdrAlg, signing string, sig []byte, keys Keys) string {
	cfgAlg := cfg.Alg
	if cfgAlg == "" {
		cfgAlg = "HS256"
	}
	if strings.EqualFold(hdrAlg, "none") || !strings.EqualFold(hdrAlg, cfgAlg) {
		return "alg mismatch"
	}
	switch strings.ToUpper(cfgAlg) {
	case "HS256":
		if len(keys.HS256) == 0 {
			return "no verification secret"
		}
		for _, k := range keys.HS256 {
			mac := hmac.New(sha256.New, k)
			mac.Write([]byte(signing))
			if subtle.ConstantTimeCompare(mac.Sum(nil), sig) == 1 {
				return ""
			}
		}
		return "bad signature"
	case "RS256", "ES256":
		if keys.Public == nil {
			return "no public key or jwks_uri configured"
		}
		if !verifyAsym(strings.ToUpper(cfgAlg), keys.Public, signing, sig) {
			return "bad signature"
		}
		return ""
	}
	return cfgAlg + " not supported"
}

// CheckClaims applies the temporal and issuer/audience policy. "" means good.
//
// The default is deliberate legacy parity: a JWT with no exp is accepted and
// never expires. `require_exp` forces exp to be present and
// `max_lifetime_sec` caps how far ahead it may lie — the two knobs an
// operator turns on for a credential that must not live for ever.
func CheckClaims(c Claims, cfg refdata.JWTConfig, now time.Time) string {
	leeway := int64(cfg.LeewaySec)
	sec := now.Unix()
	if cfg.RequireExp && c.Exp == 0 {
		return "exp claim required"
	}
	if max := int64(cfg.MaxLifetimeSec); max > 0 {
		if c.Exp == 0 {
			return "exp claim required"
		}
		if c.Exp > sec+max+leeway {
			return "exp exceeds max lifetime"
		}
	}
	if c.Exp != 0 && sec > c.Exp+leeway {
		return "expired"
	}
	if c.Nbf != 0 && sec < c.Nbf-leeway {
		return "not yet valid"
	}
	if cfg.Issuer != "" && c.Iss != cfg.Issuer {
		return "bad issuer"
	}
	if cfg.Audience != "" && c.Aud != cfg.Audience {
		return "bad audience"
	}
	return ""
}

// ParsePublicKey parses a PEM RS256/ES256 verification key. The
// Authenticator has its own caching version for accounts; this is the plain
// one for a caller that holds a single key, such as an inbound endpoint.
func ParsePublicKey(pemText string) (any, error) {
	block, _ := pem.Decode([]byte(pemText))
	if block == nil {
		return nil, errors.New("not PEM")
	}
	switch block.Type {
	case "RSA PUBLIC KEY": // PKCS#1
		return x509.ParsePKCS1PublicKey(block.Bytes)
	default: // "PUBLIC KEY" (PKIX)
		return x509.ParsePKIXPublicKey(block.Bytes)
	}
}

// HashToken is the deterministic digest a stored opaque token is compared
// against: HMAC-SHA256 under the server pepper (YAKU_TOKEN_PEPPER).
//
// Exported for the inbound edge, which stores a vendor's callback token the
// same way an account's API token is stored. Deliberately not bcrypt — see
// hashToken's own note: an opaque high-entropy token needs to be indexable,
// and bcrypt can only verify against a row already found.
func HashToken(pepper, raw string) string { return hashToken(pepper, raw) }
