package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"github.com/yakugate/yakugate/internal/dialect"
	"net/http"
	"strings"

	"golang.org/x/crypto/bcrypt"

	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/refdata"
)

// NativeTokenKey is the ONE native token header/query name (PO 2026-07-26):
// `yaku-sms-key`. The earlier grab-bag of gateway-wide aliases (`x-api-key`,
// `token`, `yaku-api-key`, and a legacy-brand name) was removed the same day —
// "yaku" appears in exactly one client-visible place, the auth key, and a
// migrating client's own header name is per-account dialect config
// (token_headers / auth_config), never a gateway-wide default.
const NativeTokenKey = "yaku-sms-key"

// extractToken finds the JWT: the native `yaku-sms-key` header, then
// `Authorization: Bearer`, then `yaku-sms-key` in the query string (query
// credentials are accepted gateway-wide for legacy parity with
// username/password — same mitigations apply, see internal/pipeline/sanitize).
//
// Per-account aliases (dialect.token_headers) cannot be consulted here: they
// live on the account, and we have no account until the token identifies one.
// They are honoured by the static-token scheme's candidate scan instead.
func extractToken(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get(NativeTokenKey)); v != "" {
		return v
	}
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		if v := strings.TrimSpace(strings.TrimPrefix(h, "Bearer ")); v != "" {
			return v
		}
	}
	if v := strings.TrimSpace(r.URL.Query().Get(NativeTokenKey)); v != "" {
		return v
	}
	return ""
}

// credential reads a named credential from headers, then the query string,
// then the request's already-parsed body/query field map (fields — see
// httpserver.parseFields). All three are matched CASE-INSENSITIVELY: headers
// already are via net/http (Header.Get canonicalizes), but url.Values and a
// plain map are exact-key, so both fallbacks need an explicit case-insensitive
// scan — a real vendor's published contract is under no obligation to use our
// casing (the Mobilink/Jazz Direct API documents `Username=`/`Password=`,
// capitalized).
//
// The fields fallback exists because some dialects carry credentials INSIDE
// the body rather than a header or the query string — the Jazz XML API wraps
// Username/Password as tags inside its xmldoc form field, which parseFields
// decodes into this same map before auth ever runs (2026-07-23). fields may
// be nil (callers with nothing pre-parsed yet); the fallback is then simply
// skipped, changing nothing for the header/query-only cases.
//
// Query-string and body credentials are accepted by PO decision for legacy
// compatibility. The cost is that they can land in access logs, proxy logs,
// browser history, or (for the body case) nowhere at all since it's never
// logged — the reject log stores only the URL path and never the raw query
// or body (internal/pipeline/sanitize.go), and no error body echoes either.
func credential(r *http.Request, fields map[string]any, name string) string {
	return credentialFrom(r, fields, dialect.Credential{Key: name})
}

// credentialFrom is credential() narrowed to one place when the dialect names
// one: a client that sends its key in a header must not have a body field of
// the same name accepted instead, which is the difference between "we found
// something called that" and "we found what they sent".
func credentialFrom(r *http.Request, fields map[string]any, c dialect.Credential) string {
	switch c.Source {
	case dialect.TokenSourceHeader:
		return strings.TrimSpace(r.Header.Get(c.Key))
	case dialect.TokenSourceQuery:
		return queryValue(r, c.Key)
	case dialect.TokenSourceBody:
		return fieldValue(fields, c.Key)
	}
	if v := strings.TrimSpace(r.Header.Get(c.Key)); v != "" {
		return v
	}
	if v := queryValue(r, c.Key); v != "" {
		return v
	}
	return fieldValue(fields, c.Key)
}

// queryValue reads one query parameter case-insensitively.
func queryValue(r *http.Request, name string) string {
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 && strings.EqualFold(k, name) {
			if v := strings.TrimSpace(vs[0]); v != "" {
				return v
			}
		}
	}
	return ""
}

// fieldValue reads one parsed body field case-insensitively.
func fieldValue(fields map[string]any, name string) string {
	for k, v := range fields {
		if !strings.EqualFold(k, name) {
			continue
		}
		if s, ok := v.(string); ok {
			if s = strings.TrimSpace(s); s != "" {
				return s
			}
		}
	}
	return ""
}

// credEntry is one verified-credential cache slot (chunk-2 ruling 2.1,
// 2026-08-03): once a presented password has passed bcrypt, later requests
// carrying the SAME password skip bcrypt behind a constant-time SHA-256
// compare. bcrypt at its default cost is tens of milliseconds of pure CPU —
// paid per request it caps a password-auth client far below the 4k msg/s
// target and blows the p95 < 50 ms budget on its own (P2 is unaffected: one
// verify per bind, not per message). The entry binds to the exact bcrypt hash
// it was verified against, so a password change (a new hash arriving via the
// 30s refdata refresh) self-invalidates it; a wrong password never touches
// the cache and always pays full bcrypt, so brute-force cost is unchanged.
type credEntry struct {
	bcryptHash string
	presented  [sha256.Size]byte
}

// fromCredentials authenticates a username/password submission.
//
// Failures are deliberately indistinguishable to the caller — unknown user,
// disabled scheme and wrong password all return the same 401 — so the endpoint
// cannot be used to enumerate valid usernames. Lockout is keyed on the
// username, so repeated guesses against one account trip the same 5-per-minute
// limit as token auth.
func (a *Authenticator) fromCredentials(r *http.Request, fields map[string]any, ip string) (*refdata.Account, *pipeline.Reject) {
	// What this client's own spec calls its two credentials, and where each
	// travels (PO 2026-09-16). Read from the dialect the URL resolves to,
	// because both are needed before any username has been read — so they can
	// only come from the path, never from the account, and the defaults stand
	// on the native path.
	userCred, passCred := a.ref.DialectForPath(r.URL.Path).Credentials()
	user := credentialFrom(r, fields, userCred)
	pass := credentialFrom(r, fields, passCred)
	if user == "" || pass == "" {
		return nil, pipeline.NewReject(401, "AUTH_FAILED: missing credentials")
	}

	id := "u:" + user
	if locked, retry := a.lock.Locked(id); locked {
		a.report(SecurityEvent{Type: EventAuthLocked, Account: user, Identity: id, IP: ip,
			Reason: "identity is locked out", RetryAfter: retry})
		return nil, retryReject(423, "AUTH_LOCKED: too many failures", retry)
	}

	acct, ok := a.ref.AccountByUsername(user)
	if !ok || acct.APIPassword == "" {
		// Unknown user (or scheme disabled): no allowlist knowable — both count.
		return nil, a.fail(id, ip, nil, "invalid username or password")
	}
	sum := sha256.Sum256([]byte(pass))
	if v, ok := a.credCache.Load(user); ok {
		if e := v.(credEntry); e.bcryptHash == acct.APIPassword &&
			subtle.ConstantTimeCompare(e.presented[:], sum[:]) == 1 {
			return a.postAuth(acct, id)
		}
	}
	if bcrypt.CompareHashAndPassword([]byte(acct.APIPassword), []byte(pass)) != nil {
		// Known account, wrong password: the allowlist skip-table applies —
		// a guesser off the account's allowlisted IPs must not be able to
		// lock the real owner out.
		return nil, a.fail(id, ip, acct, "invalid username or password")
	}
	a.credCache.Store(user, credEntry{bcryptHash: acct.APIPassword, presented: sum})
	return a.postAuth(acct, id)
}
