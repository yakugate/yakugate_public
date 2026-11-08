// Package auth resolves the client account from a JWT — the ONLY auth scheme
// (database-structure.docx v2.0). The key a client presents (yaku-sms-key or
// Authorization: Bearer) is always a JWT, verified against the account's jwt
// config (accounts.jwt JSON). HS256 is verified inline with the standard
// library; RS256/ES256 verify against the account's PEM public_key, or
// against the keys fetched from jwt.jwks_uri (jwks.go — built after the
// chunk-2 ruling 2.3 of 2026-08-03 had parked it; the Admin API does not
// expose the field yet). Repeated failures lock the identity. [SCOPE P1-AUTH]
package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/secrets"
)

// Locker is the brute-force ladder policy — consumer-side interface so auth
// takes either the in-memory single-node lockout.Ladder or the
// cluster-accurate lockout.Shared under the YAKU_REDIS_ADDR gate (2026-07-24).
// Escalation semantics (rungs, decay, keep-level-on-success) live entirely in
// the implementation; auth only consumes the outcomes. [PO 2026-07-26]
type Locker interface {
	// Locked reports whether id is locked and for how much longer (Retry-After).
	Locked(id string) (bool, time.Duration)
	// Fail records a failure; on a trip it reports the lock duration and the
	// escalation level reached (level >= 3 should raise an alert).
	Fail(id string) (locked bool, retryAfter time.Duration, level int)
	// Reset clears the failure counter on success (keeps the escalation level).
	Reset(id string)
}

// SprayLocker is the credential-spray guard's consumer-side interface —
// satisfied by lockout.SprayGuard and lockout.SharedSprayGuard. Detects one
// source IP trying many DISTINCT identities in a short window and trips an
// escalating ladder on the FIRST such crossing (PO 2026-07-27), rather than
// waiting for a per-identity failure-count grace. [SCOPE P1-AUTH]
type SprayLocker interface {
	Locked(ip string) (bool, time.Duration)
	Observe(ip, identity string) (tripped bool, retryAfter time.Duration, level int)
}

// Authenticator authenticates JWTs against the reference cache, with lockout.
type Authenticator struct {
	ref     *refdata.Mem
	lock    Locker              // identity ladder (username / JWT subject / token digest): 5 fails/min
	ipLock  Locker              // optional per-source-IP throttle ladder: 20 fails/min; nil = disabled
	spray   SprayLocker         // optional credential-spray guard (distinct identities/IP); nil = disabled
	seclog  func(SecurityEvent) // every security event (P13 Type 2); nil = off
	secrets *secrets.Registry   // optional: resolves jwt.secret_ref (nil = inline only)
	warned  sync.Map            // account ids already warned about inline secrets
	ipres   *IPResolver         // trusted-proxy client-IP resolution; nil = TCP peer only (chunk-2 ruling 2.5)
	// credCache holds verified-credential entries (username -> credEntry) so a
	// repeat submission of the SAME correct password skips bcrypt — see
	// credentials.go's credEntry doc (chunk-2 ruling 2.1).
	credCache sync.Map
	// pubKeys caches parsed RS256/ES256 public keys by their PEM text — a key
	// rotation simply misses and re-parses, no invalidation hook needed.
	pubKeys sync.Map
	// jwks resolves keys from an account's jwt.jwks_uri (P1 scope finalize
	// 2026-08-09); built lazily on first JWKS-configured account.
	jwksOnce sync.Once
	jwksRes  *jwksResolver
	// tokenPepper is the server-side HMAC key for the opaque static-token
	// scheme (internal/auth/token.go's hashToken). Never stored alongside the
	// digest it protects — set via WithTokenPepper, env-configured only.
	tokenPepper string
}

// New builds an Authenticator over the reference cache and a lockout policy.
// Secret refs are unresolved unless WithSecrets is used.
func New(ref *refdata.Mem, lock Locker, opts ...Option) *Authenticator {
	a := &Authenticator{ref: ref, lock: lock}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Option configures an Authenticator.
type Option func(*Authenticator)

// WithSecrets enables resolution of accounts.jwt.secret_ref / secret_ref_prev.
// [SCOPE P1-SECRETS]
func WithSecrets(r *secrets.Registry) Option {
	return func(a *Authenticator) { a.secrets = r }
}

// WithIPThrottle arms the per-source-IP ladder (threshold 20 fails/min). A
// trip THROTTLES (pre-auth 429 + Retry-After), never hard-blocks — enterprise
// clients share NAT egress IPs, so a hard block would punish every legitimate
// account behind the address. [PO 2026-07-26]
func WithIPThrottle(l Locker) Option {
	return func(a *Authenticator) { a.ipLock = l }
}

// WithSprayGuard arms the credential-spray detector (PO 2026-07-27): an IP
// trying many distinct identities in a short window is BLOCKED immediately
// (escalating rungs from the first crossing), a stricter response than the
// volume-only IP throttle above since a rotating-identity pattern is itself
// the attack signature.
func WithSprayGuard(g SprayLocker) Option {
	return func(a *Authenticator) { a.spray = g }
}

// WithTrustedProxies arms X-Forwarded-For resolution behind the configured
// trusted proxies (chunk-2 ruling 2.5, closing open item B-02). nil keeps the
// pre-existing peer-only behaviour — see IPResolver's doc for the safety rule.
func WithTrustedProxies(res *IPResolver) Option {
	return func(a *Authenticator) { a.ipres = res }
}

// WithTokenPepper sets the HMAC key the opaque static-token scheme signs
// candidate values with before looking them up (internal/auth/token.go). Not
// setting this leaves the scheme unable to match anything usefully (an empty
// pepper still HMACs, just weakly) — callers should always supply a real one,
// env-configured the same way accounts.jwt's dev secret is. [2026-07-23]
func WithTokenPepper(pepper string) Option {
	return func(a *Authenticator) { a.tokenPepper = pepper }
}

// hs256Keys returns the HS256 verification keys to try, best first. A resolvable
// secret_ref WINS over any inline secret; secret_ref_prev is tried second to
// support rotation. Inline secrets keep working (dev seeds) but warn once per
// account. Resolution is cached inside the secrets registry, so this is not a
// per-message KMS call. [SCOPE P1-SECRETS]
func (a *Authenticator) hs256Keys(acct *refdata.Account) [][]byte {
	var out [][]byte
	if a.secrets != nil {
		ctx := context.Background()
		for _, ref := range []string{acct.JWT.SecretRef, acct.JWT.SecretRefPrev} {
			if ref == "" {
				continue
			}
			v, err := a.secrets.Resolve(ctx, ref)
			if err != nil {
				// Never log the ref's value; the registry's audit hook carries the
				// alarm. Fall through to any remaining candidate.
				continue
			}
			out = append(out, v)
		}
	}
	if acct.JWT.Secret != "" {
		if _, seen := a.warned.LoadOrStore(acct.ID, true); !seen && acct.JWT.SecretRef == "" {
			log.Printf("[auth] account %d uses an INLINE jwt secret; move it to secret_ref", acct.ID)
		}
		out = append(out, []byte(acct.JWT.Secret))
	}
	return out
}

// jwtClaims is the subset of registered claims we validate.
// jwtClaims is auth.Claims (verify.go), kept as a name this file's older
// callers still read naturally.
type jwtClaims = Claims

// orAlg is the configured algorithm, defaulting to HS256 as it always has.
func orAlg(alg string) string {
	if alg == "" {
		return "HS256"
	}
	return alg
}

// FromRequest extracts and verifies the JWT, returning the account. fields is
// the request's already-parsed body/query field map (httpserver.parseFields) —
// pass nil if the caller has nothing pre-parsed; only the username/password
// fallback (fromCredentials) ever consults it, for dialects whose credentials
// live inside the body (2026-07-23).
func (a *Authenticator) FromRequest(r *http.Request, fields map[string]any) (*refdata.Account, *pipeline.Reject) {
	// Per-source-IP throttle FIRST (pre-auth, PO 2026-07-26): a throttled
	// address is answered before any credential work — that is the point (an
	// auth-flood from one IP must not buy CPU), and it is a throttle with a
	// Retry-After, never a hard block. The address is trusted-proxy-resolved
	// (chunk-2 ruling 2.5): nil resolver = the TCP peer, exactly as before.
	ip := a.ipres.FromRequest(r)
	if a.ipLock != nil {
		if locked, retry := a.ipLock.Locked("ip:" + ip); locked {
			// Reported here as well as at the trip: this is the address being
			// turned away AGAIN, which is what shows the shape of an ongoing
			// attack. The trip itself fires once, in fail().
			a.report(SecurityEvent{Type: EventIPThrottled, IP: ip,
				Reason: "address is throttled for repeated auth failures", RetryAfter: retry})
			return nil, retryReject(429, "IP_THROTTLED: too many auth failures from this address", retry)
		}
	}
	// Credential-spray guard (PO 2026-07-27): checked ahead of any credential
	// work, same as the volume throttle above — a locked-out spraying IP must
	// not buy CPU either.
	if a.spray != nil {
		if locked, retry := a.spray.Locked(ip); locked {
			a.report(SecurityEvent{Type: EventSprayBlocked, IP: ip,
				Reason: "address is blocked for a credential-spray pattern", RetryAfter: retry})
			return nil, retryReject(423, "IP_LOCKED: credential-spray pattern detected", retry)
		}
	}

	// Opaque static-token scheme first (2026-07-23): a small, bounded,
	// data-driven set of per-account (source,key) candidates — a legacy
	// client's own custom header/query/body key name, distinct from the
	// canonical yaku-sms-key/Authorization vocabulary below. Checked first so it
	// never has to coexist ambiguously with a JWT/username+password attempt.
	// fromToken returns (nil, nil) when the request touches NONE of the
	// configured candidates at all — only then do we fall through; if it
	// touched one and failed, that specific failure is returned as-is rather
	// than being masked behind a later, unrelated "missing credentials".
	if acct, rej := a.fromToken(r, fields, ip); acct != nil || rej != nil {
		return acct, rej
	}

	tok := extractToken(r)
	if tok == "" {
		// No token: fall back to username/password, which both GET and POST
		// accept, in headers, the query string, or the body (PO 2026-07-22/23).
		return a.fromCredentials(r, fields, ip)
	}
	id := "t:" + tok
	if locked, retry := a.lock.Locked(id); locked {
		a.report(SecurityEvent{Type: EventAuthLocked, Identity: id, IP: ip,
			Reason: "identity is locked out", RetryAfter: retry})
		return nil, retryReject(423, "AUTH_LOCKED: too many failures", retry)
	}

	acct, rej := a.verify(tok, id, ip)
	if rej != nil {
		return nil, rej
	}
	return a.postAuth(acct, id)
}

// postAuth applies the account-state gates every scheme shares once its
// credential proof succeeded, and clears the identity ladder's failure
// counter on the way out. Suspended covers the owning company too: disabling
// a tenant suspends every API client under it in one action. [0017_companies]
func (a *Authenticator) postAuth(acct *refdata.Account, id string) (*refdata.Account, *pipeline.Reject) {
	if rej := AccountState(acct); rej != nil {
		// The credential was right, so we know whose request this is: the
		// edge records the refusal against the account (Reject.Proven).
		rej.Proven = acct.Username
		return nil, rej
	}
	if rej := HTTPState(acct); rej != nil {
		rej.Proven = acct.Username
		return nil, rej
	}
	a.lock.Reset(id)
	return acct, nil
}

// HTTPState is the account's HTTP switch (sms_service_config.http): an
// account with HTTP switched off may not use the HTTP API, whatever
// credential it shows.
//
// Until 2026-10-12 nothing on the accept path read the switch: an account
// set to SMPP only was still ACCEPTED over HTTP with a valid token (go-live
// test P1 D9). The SMPP side has always refused a bind when ITS switch is off
// (ESME_RBINDFAIL); this is the same rule for the other door. It is not part
// of AccountState, which also judges an account that sends over SMPP.
//
// The column is NOT NULL with a default of 1, so only an account somebody
// switched off is refused.
func HTTPState(acct *refdata.Account) *pipeline.Reject {
	if acct.HTTPOff {
		return pipeline.NewReject(403, "ACCOUNT_DISABLED: HTTP is switched off for this account")
	}
	return nil
}

// AccountState is the account-state half of authentication: may this account
// use the API at all, whatever credential it shows? Exported for the "Will
// this send?" check, which must refuse a disabled account exactly as a real
// submit would.
func AccountState(acct *refdata.Account) *pipeline.Reject {
	if acct.Suspended() {
		return pipeline.NewReject(403, "ACCOUNT_DISABLED")
	}
	if acct.AccountExpiry != nil && acct.AccountExpiry.Before(time.Now()) {
		return pipeline.NewReject(403, "ACCOUNT_DISABLED: expired")
	}
	// 3-way access scope (P1 scope finalize 2026-08-09 §7, config/0061): a
	// portal-only account's credentials prove fine but the API surface is
	// closed to it — same reason family as the other account-state gates.
	// Checked AFTER credential proof so a wrong password on a portal-only
	// account still climbs the ladders like any other failure.
	if acct.AccessScope == "portal" {
		return pipeline.NewReject(403, "ACCOUNT_DISABLED: portal-only account")
	}
	return nil
}

// remoteIP extracts the peer address (no X-Forwarded-For — resolving the real
// client behind a trusted proxy is open scope item B-02, and honouring XFF
// blindly here would let anyone spoof their way around the IP ladder).
func remoteIP(r *http.Request) string {
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return strings.Trim(host, "[]")
}

// retryReject builds a Reject carrying a Retry-After duration.
func retryReject(code int, reason string, retry time.Duration) *pipeline.Reject {
	rej := pipeline.NewReject(code, reason)
	if secs := int(retry / time.Second); secs > 0 {
		rej.RetryAfter = secs
	} else if retry > 0 {
		rej.RetryAfter = 1
	}
	return rej
}

// fail is the ONE place a failed attempt is counted, implementing the
// PO-decided DoS-mitigation table (2026-07-26):
//
//	account has an IP allowlist, request IP allowlisted  → identity + IP
//	account has an IP allowlist, request IP NOT on it    → IP ONLY
//	no allowlist configured (or account unknown)         → identity + IP
//
// The skip exists so an attacker who knows a username but not an allowlisted
// address cannot lock the real owner out of their own account (a DoS via the
// lockout itself); their attempts still climb the IP ladder. acct is nil when
// the failure happened before an account was resolved (unknown user/subject,
// malformed token) — no allowlist is knowable, so both ladders count.
func (a *Authenticator) fail(id, ip string, acct *refdata.Account, reason string) *pipeline.Reject {
	acctName := ""
	if acct != nil {
		acctName = acct.Username
	}
	if a.ipLock != nil && ip != "" {
		if _, retry, level := a.ipLock.Fail("ip:" + ip); level >= 3 {
			a.report(SecurityEvent{Type: EventLadderRung, Account: acctName, Identity: id, IP: ip,
				Reason: "per-IP throttle ladder reached rung " + strconv.Itoa(level), Rung: level, RetryAfter: retry})
		}
		// An IP trip does NOT change THIS response — the identity outcome
		// below stands; the throttle answers the address's NEXT request,
		// pre-auth, in FromRequest.
	}
	// Credential-spray guard (PO 2026-07-27): id is the specific identity this
	// attempt tried ("t:<token>" / "u:<username>") — observing it against ip
	// trips immediately once enough DISTINCT identities have failed from this
	// address, same "answers the NEXT request" shape as the IP throttle above.
	if a.spray != nil && ip != "" && id != "" {
		if _, retry, level := a.spray.Observe(ip, id); level >= 3 {
			a.report(SecurityEvent{Type: EventSprayBlocked, Account: acctName, Identity: id, IP: ip,
				Reason: "credential-spray guard reached rung " + strconv.Itoa(level), Rung: level, RetryAfter: retry})
		}
	}
	countIdentity := acct == nil || len(acct.AllowedIPs) == 0 || acct.IPAllowed(ip)
	if !countIdentity {
		a.report(SecurityEvent{Type: EventAuthFail, Account: acctName, Identity: id, IP: ip, Reason: reason})
		return pipeline.NewReject(401, "AUTH_FAILED: "+reason)
	}
	locked, retry, level := a.lock.Fail(id)
	if level >= 3 {
		a.report(SecurityEvent{Type: EventLadderRung, Account: acctName, Identity: id, IP: ip,
			Reason: "identity lockout ladder reached rung " + strconv.Itoa(level), Rung: level, RetryAfter: retry})
	}
	if locked {
		a.report(SecurityEvent{Type: EventAuthLocked, Account: acctName, Identity: id, IP: ip,
			Reason: reason, Rung: level, RetryAfter: retry})
		return retryReject(423, "AUTH_LOCKED: too many failures", retry)
	}
	a.report(SecurityEvent{Type: EventAuthFail, Account: acctName, Identity: id, IP: ip, Reason: reason})
	return pipeline.NewReject(401, "AUTH_FAILED: "+reason)
}

// failOpaque counts a PRE-signature verification failure exactly like fail(),
// but the client-visible reason is always the same generic string: before the
// signature is proven, distinct reasons ("unknown subject" vs "bad signature")
// would tell a guesser which usernames exist — the same enumeration hole
// fromCredentials already closes with its uniform reason (chunk-2 ruling 2.2,
// 2026-08-03). The specific cause goes to the server log only, and never the
// token value itself.
func (a *Authenticator) failOpaque(id, ip string, acct *refdata.Account, detail string) *pipeline.Reject {
	log.Printf("[auth] jwt rejected ip=%s: %s", ip, detail)
	return a.fail(id, ip, acct, "invalid token")
}

// resolveSubject maps the token's subject claim to an account. Most
// deployments only ever use the registered "sub" (the loader's default), which
// stays a single struct-field read; when any account configures a custom
// sub_claim (chunk-2 ruling 2.3 — the field existed but was never consulted)
// the claim set is decoded generically and each configured name is tried. A
// match only counts when the resolved account's OWN sub_claim is the name that
// found it, so one tenant's custom claim can never resolve another tenant.
func (a *Authenticator) resolveSubject(claimRaw []byte, claims *jwtClaims) (*refdata.Account, string) {
	names := a.ref.SubClaimNames()
	if len(names) <= 1 {
		if claims.Sub == "" {
			return nil, "no subject"
		}
		if acct, ok := a.ref.AccountByUsername(claims.Sub); ok {
			return acct, ""
		}
		return nil, "unknown subject"
	}
	var all map[string]any
	if json.Unmarshal(claimRaw, &all) != nil {
		return nil, "bad claims json"
	}
	for _, name := range names {
		v, _ := all[name].(string)
		if v == "" {
			continue
		}
		acct, ok := a.ref.AccountByUsername(v)
		if !ok {
			continue
		}
		eff := acct.JWT.SubClaim
		if eff == "" {
			eff = "sub"
		}
		if eff == name {
			return acct, ""
		}
	}
	return nil, "unknown subject"
}

func (a *Authenticator) verify(tok, id, ip string) (*refdata.Account, *pipeline.Reject) {
	// Reading the token apart is shared with the INBOUND edge (verify.go), so
	// a vendor callback and a client submit can never disagree about what a
	// JWT even is.
	hdrAlg, kid, signing, sig, claims, claimRaw, reason := SplitToken(tok)
	if reason != "" {
		return nil, a.failOpaque(id, ip, nil, reason)
	}

	acct, why := a.resolveSubject(claimRaw, &claims)
	if acct == nil {
		return nil, a.failOpaque(id, ip, nil, why)
	}

	// The key material for this account: HS256 candidates best-first (the
	// resolved secret_ref over any inline secret, secret_ref_prev second so a
	// key rotates without a flag day), or the asymmetric public key — an
	// inline PEM, else the account's jwks_uri with the token's `kid`.
	keys := Keys{}
	if strings.EqualFold(orAlg(acct.JWT.Alg), "HS256") {
		keys.HS256 = a.hs256Keys(acct)
	} else {
		switch {
		case acct.JWT.PublicKey != "":
			p, err := a.publicKey(acct.JWT.PublicKey)
			if err != nil {
				return nil, a.failOpaque(id, ip, acct, "bad public key: "+err.Error())
			}
			keys.Public = p
		case acct.JWT.JWKSURI != "":
			p, err := a.jwks().Key(acct.JWT.JWKSURI, kid)
			if err != nil {
				return nil, a.failOpaque(id, ip, acct, "jwks: "+err.Error())
			}
			keys.Public = p
		}
	}
	if reason := VerifySignature(acct.JWT, hdrAlg, signing, sig, keys); reason != "" {
		return nil, a.failOpaque(id, ip, acct, reason)
	}

	// Temporal + issuer/audience claims, shared with the inbound edge.
	// Post-signature failures KEEP their specific reasons (unlike failOpaque
	// above): producing any of them requires holding the signing key, so the
	// detail helps the genuine client debug and tells a guesser nothing.
	if reason := CheckClaims(claims, acct.JWT, time.Now()); reason != "" {
		return nil, a.fail(id, ip, acct, reason)
	}
	if claims.Jti != "" && a.ref.Revoked(claims.Jti) {
		return nil, a.fail(id, ip, acct, "revoked")
	}
	return acct, nil
}

func b64(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}
