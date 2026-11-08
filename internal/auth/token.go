package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/yakugate/yakugate/internal/dialect"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/refdata"
)

// hashToken computes the hex HMAC-SHA256 digest of a raw opaque token using
// the server-side pepper. Deliberately NOT bcrypt: bcrypt is salted and
// non-deterministic, so it can only ever VERIFY a value against one already-
// known row, never FIND the row a value belongs to. An opaque token has no
// separate "username half" the way username/password auth does (see
// credential()'s doc comment), so it needs a deterministic digest to be
// directly indexable — refdata.Mem.byTokenHash is a plain map keyed on
// exactly this. High-entropy tokens don't need bcrypt's slow/salted
// properties the way human-chosen passwords do; this is the same pattern
// Stripe/GitHub/AWS use for their own API keys. [2026-07-23]
func hashToken(pepper, raw string) string {
	mac := hmac.New(sha256.New, []byte(pepper))
	mac.Write([]byte(raw))
	return hex.EncodeToString(mac.Sum(nil))
}

// tokenCandidateValue extracts one (source, key) candidate's value from the
// request, matched case-insensitively — the header/query lookups mirror
// credential()'s existing per-source scans, and the body lookup reads from
// the SAME pre-parsed fields map (httpserver.parseFields) everything else
// already consults. Returns "" when this particular candidate isn't present,
// which is the normal case for every candidate except the one (if any) a
// given request actually uses.
func tokenCandidateValue(r *http.Request, fields map[string]any, cand refdata.TokenCandidate) string {
	switch cand.Source {
	case dialect.TokenSourceHeader:
		return strings.TrimSpace(r.Header.Get(cand.Key)) // http.Header.Get canonicalizes, so this is already case-insensitive
	case dialect.TokenSourceQuery:
		for k, vs := range r.URL.Query() {
			if strings.EqualFold(k, cand.Key) && len(vs) > 0 {
				if v := strings.TrimSpace(vs[0]); v != "" {
					return v
				}
			}
		}
	case dialect.TokenSourceBody:
		for k, v := range fields {
			if !strings.EqualFold(k, cand.Key) {
				continue
			}
			if s, ok := v.(string); ok {
				if s = strings.TrimSpace(s); s != "" {
					return s
				}
			}
		}
	}
	return ""
}

// fromToken tries every configured token candidate against the request.
//
// Returns (account, nil) on success, (nil, reject) when at least one
// candidate WAS present but matched no account (a real auth failure — the
// caller must not silently fall through to another scheme and mask it behind
// a generic "missing credentials"), and (nil, nil) when NO candidate produced
// any value at all, meaning this request isn't attempting the token scheme —
// only then should the caller try JWT/username-password.
//
// Every non-empty candidate is tried (not just the first) before giving up,
// since two distinct legacy integrations could coincidentally configure the
// same header/query/field NAME for genuinely different accounts.
func (a *Authenticator) fromToken(r *http.Request, fields map[string]any, ip string) (*refdata.Account, *pipeline.Reject) {
	var firstAttemptID string
	for _, cand := range a.ref.TokenCandidates() {
		raw := tokenCandidateValue(r, fields, cand)
		if raw == "" {
			continue
		}
		hash := hashToken(a.tokenPepper, raw)
		id := "tok:" + hash
		if firstAttemptID == "" {
			firstAttemptID = id
		}
		if locked, retry := a.lock.Locked(id); locked {
			return nil, retryReject(423, "AUTH_LOCKED: too many failures", retry)
		}
		acct, ok := a.ref.AccountByTokenHash(hash)
		if !ok {
			continue // try the remaining candidates before failing
		}
		// Same post-authentication gates as the JWT and username/password paths.
		return a.postAuth(acct, id)
	}
	if firstAttemptID == "" {
		return nil, nil // nothing attempted -- not this scheme
	}
	// No account was ever resolved, so no allowlist is knowable — both ladders
	// count (see fail's table).
	return nil, a.fail(firstAttemptID, ip, nil, "invalid token")
}
