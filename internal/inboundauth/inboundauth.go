// Package inboundauth authenticates a VENDOR calling the gateway — delivery
// receipts on /dlr/<cid> and inbound messages on /mo/<cid> — with the same
// machinery a client's submit is authenticated by (PO 2026-09-23: "same auth
// management system as for accounts ... all settings as accounts, i.e.
// throughput, IP validation, all required ones").
//
// What this replaces: three flat secrets shared by both callback kinds
// (internal/webhookauth), compared in constant time and nothing else. No rate
// limit, no lockout, no rotation, no audit, no way to close one door and
// leave the other open. Meanwhile a CLIENT gets JWT with key rotation, bcrypt
// passwords, an IP allow-list, per-account throughput, an escalating lockout
// ladder, a credential-spray guard and a security event for every refusal.
//
// The asymmetry was backwards. A forged receipt rewrites a real message's
// outcome; a forged MO is BILLED, lands in the customer's log as genuine and
// can trigger their auto-reply into sending real traffic.
//
// # One door per kind
//
// Each vendor has a row per kind (config/0121): its own credentials, its own
// IP list, its own throughput, its own on/off switch. Receipts and inbound
// messages are different doors because they are different risks and often
// different systems at the vendor's end.
//
// # How a request is judged, in order
//
//  1. per-IP throttle      pre-auth, so a flood buys no CPU — throttle with
//     Retry-After, never a hard block, because vendors
//     share NAT egress and a block would silence every
//     other vendor behind it
//  2. spray guard          one address probing many distinct doors
//  3. endpoint disabled    closed door, nothing else consulted
//  4. IP allow-list        a condition: when set, it must pass
//  5. credentials          alternatives, as for an account: JWT, token, or
//     username+password. One configured scheme must be
//     satisfied; a scheme that was ATTEMPTED and failed
//     is a refusal, never a fall-through
//  6. throughput           calls a second for this door
//
// The signed-body (HMAC) scheme was retired on 2026-09-28 (PO: "in inbound we
// don't need signature"). inbound_endpoints.hmac_config is no longer read.
//
// # Where each credential is
//
// Vendors put their credentials anywhere: a header of their own naming, the
// query string, a field of the body. The door's dialect says where, per
// credential (sms_service_dialects.auth_config for a dlr/mo dialect,
// config/0128; see places.go). A door with no dialect, or a dialect that names
// no place, keeps the reading every door had before.
//
// A failure climbs the door's own ladder (`in:<vendor>:<kind>`) and raises a
// security event. The caller is told "forbidden" and nothing else: naming the
// failed check tells a guesser which one to work on.
//
// # Nothing configured means refused
//
// The 2026-08-31 ruling stands: a door with no credential, no IP list and no
// legacy config is closed, so the protection cannot be defeated by never
// setting it up. A row that carries nothing yet falls back to the vendor's
// old inbound_config, which is what makes config/0121 safe to apply live.
package inboundauth

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/callbackmap"

	"golang.org/x/crypto/bcrypt"

	"github.com/yakugate/yakugate/internal/auth"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/seclog"
	"github.com/yakugate/yakugate/internal/webhookauth"
)

// Kinds of inbound callback. They are separate doors, never one.
const (
	KindDLR = "dlr"
	KindMO  = "mo"
)

// Kinds lists them, for the Admin API, the portal and the console.
func Kinds() []string { return []string{KindDLR, KindMO} }

// Endpoint is one door: how one vendor proves itself for one kind of
// callback. It is the inbound twin of refdata.Account, field for field where
// the two mean the same thing.
type Endpoint struct {
	ID        int64
	VendorID  int64
	VendorCID string
	Kind      string
	Disabled  bool

	// Credentials — alternatives, exactly as an account's are.
	JWT         refdata.JWTConfig
	TokenHash   string // HMAC-SHA256 under the server pepper, never the token
	TokenSource string // any | header | query | path
	TokenHeader string // header/query name; empty = the gateway default
	Username    string
	Password    string // bcrypt
	// Places says where in the request each credential is (config/0127).
	// Empty = the reading every door had before.
	Places Places

	// Conditions — enforced whenever they are set.
	AllowedIPs []string

	// MaxThroughput caps callbacks a second for this door. 0 = no limit.
	MaxThroughput int

	// Legacy is the vendor's pre-0121 inbound_config. It answers only while
	// this row carries no credential of its own, so a gateway upgrading in
	// place keeps working until an operator sets the new ones.
	Legacy webhookauth.Config

	// Request and Reply are how this door reads the vendor's callback and
	// what it answers (config/0125, internal/callbackmap). nil = the old
	// behaviour: the webhook row / guessed names, and an empty 200.
	Request *callbackmap.Request
	Reply   *callbackmap.Reply
	// Dialect names the dlr/mo dialect this door reads with (config/0128),
	// "" = none. Its request_map (with request_format and request_wrapper),
	// response_map (with keep_unmapped) and auth_config are Request, Reply
	// and Places above.
	Dialect string
	// MappingErr says why a stored mapping was not loaded, for the log. The
	// door then keeps the old behaviour rather than half a mapping.
	MappingErr string

	// CustomPath is the door's own path beside its default /<kind>/<cid>
	// (issue 20, config/0152), cleaned; "" = none (paths.go).
	CustomPath string
	// DialectURL is the own URL of the dialect this door reads with (27 Oct:
	// sms_service_dialects.url of a dlr / mo dialect), cleaned; "" = none. A
	// callback to it is for whichever of that dialect's doors its credentials
	// name (Guard.Identify).
	DialectURL string
	// PathNote says, for the log, why a stored path of this door is not used
	// (its own path or its dialect's URL does not read), or that another door
	// on its dialect's URL has the same token or username. The door's
	// reading is not touched by it (MappingErr is that).
	PathNote string
}

// Exists reports whether this is a door someone set up: a row of its own, or
// a vendor's old shared setting not yet moved onto one (MoveLegacy). A vendor
// with neither has no door, and its callbacks are refused whatever they say
// (issue 19: "creating inbound is necessary").
func (e Endpoint) Exists() bool { return e.ID != 0 || e.Legacy.Configured() }

// DefaultTokenHeader is where a callback token is looked for when the door
// names no header of its own. Deliberately the same key a client uses, so an
// operator has one name to remember.
const DefaultTokenHeader = "yaku-sms-key"

// HasCredential reports whether this row carries a credential of its own.
func (e Endpoint) HasCredential() bool {
	return e.TokenHash != "" || (e.Username != "" && e.Password != "") ||
		e.JWT.Secret != "" || e.JWT.SecretRef != "" || e.JWT.PublicKey != "" || e.JWT.JWKSURI != ""
}

// Configured reports whether anything at all guards this door.
func (e Endpoint) Configured() bool {
	return e.HasCredential() || len(e.AllowedIPs) > 0 || e.Legacy.Configured()
}

// Result is one judgement. OK means let the callback through; otherwise
// Status is what to answer, Reason is for OUR log only, and RetryAfter is the
// seconds to advertise on a 429.
type Result struct {
	OK         bool
	Status     int
	Reason     string
	RetryAfter int
	// Identity is what was tried, safe to write down (seclog.SafeIdentity
	// applied), for the security event and the log line.
	Identity string
}

func ok() Result { return Result{OK: true} }

func refuse(status int, reason string) Result {
	return Result{Status: status, Reason: reason}
}

// Locker is the brute-force ladder — satisfied by lockout.Ladder and
// lockout.Shared, the same two the submit edge takes.
type Locker interface {
	Locked(id string) (bool, time.Duration)
	Fail(id string) (locked bool, retryAfter time.Duration, level int)
	Reset(id string)
}

// SprayLocker is the credential-spray guard (lockout.SprayGuard / …Redis).
type SprayLocker interface {
	Locked(ip string) (bool, time.Duration)
	Observe(ip, identity string) (tripped bool, retryAfter time.Duration, level int)
}

// Granter is the per-second rate limiter (ratelimit.RateLimiter / …Redis).
type Granter interface {
	Grant(key string, limit, want int) int
	Return(key string, n int)
}

// SecretResolver resolves env:// / file:// refs (internal/secrets.Registry).
type SecretResolver interface {
	Resolve(ctx context.Context, ref string) ([]byte, error)
}

// Guard judges inbound callbacks. Zero value works: no ladders, no limiter,
// no secrets — only the checks that need none.
type Guard struct {
	pepper  string
	lock    Locker
	ipLock  Locker
	spray   SprayLocker
	grant   Granter
	secrets SecretResolver
	report  func(seclog.Event)
	ip      func(*http.Request) string
	creds   *credCache
}

// Option configures a Guard.
type Option func(*Guard)

// WithPepper sets YAKU_TOKEN_PEPPER — the same pepper the submit edge hashes
// account tokens with, so one gateway has one token digest.
func WithPepper(p string) Option { return func(g *Guard) { g.pepper = p } }

// WithLadder arms the per-door lockout ladder.
func WithLadder(l Locker) Option { return func(g *Guard) { g.lock = l } }

// WithIPThrottle arms the pre-auth per-address throttle. A trip THROTTLES
// with a Retry-After and never hard-blocks: vendors share NAT egress, and a
// block would silence every other vendor behind that address.
func WithIPThrottle(l Locker) Option { return func(g *Guard) { g.ipLock = l } }

// WithSprayGuard arms the spray detector: one address probing many distinct
// doors is the attack signature itself.
func WithSprayGuard(s SprayLocker) Option { return func(g *Guard) { g.spray = s } }

// WithThroughput arms the per-door rate limit.
func WithThroughput(gr Granter) Option { return func(g *Guard) { g.grant = gr } }

// WithSecrets enables env:// / file:// refs in jwt.secret_ref.
func WithSecrets(s SecretResolver) Option { return func(g *Guard) { g.secrets = s } }

// WithReporter installs the security-event sink.
func WithReporter(f func(seclog.Event)) Option { return func(g *Guard) { g.report = f } }

// WithIPResolver installs the trusted-proxy address resolver, so an inbound
// callback's address is read under exactly the rule the submit edge uses.
func WithIPResolver(f func(*http.Request) string) Option {
	return func(g *Guard) { g.ip = f }
}

// New builds a Guard.
func New(opts ...Option) *Guard {
	g := &Guard{creds: newCredCache()}
	for _, o := range opts {
		o(g)
	}
	return g
}

// Check judges one callback. body is the already-read request body (a
// credential may be a field of it); pass nil when there is none.
// urlToken is the path segment after the vendor cid, "" when absent.
func (g *Guard) Check(r *http.Request, body []byte, e Endpoint, urlToken string) Result {
	ip := g.clientIP(r)
	door := "in:" + e.VendorCID + ":" + e.Kind

	// 1. Pre-auth throttle: a flood from one address is answered before any
	//    credential work, which is the point of doing it first.
	if g.ipLock != nil {
		if locked, retry := g.ipLock.Locked("ip:" + ip); locked {
			g.event(seclog.EventIPThrottled, e, ip, door, "address is throttled for repeated inbound failures", retry, 0)
			return Result{Status: http.StatusTooManyRequests, Reason: "address throttled",
				RetryAfter: secs(retry), Identity: door}
		}
	}
	// 2. Spray: one address trying many different doors.
	if g.spray != nil {
		if locked, retry := g.spray.Locked(ip); locked {
			g.event(seclog.EventSprayBlocked, e, ip, door, "address is blocked for probing many vendors", retry, 0)
			return Result{Status: http.StatusTooManyRequests, Reason: "address blocked",
				RetryAfter: secs(retry), Identity: door}
		}
	}
	// 3. The door's own ladder.
	if g.lock != nil {
		if locked, retry := g.lock.Locked(door); locked {
			g.event(seclog.EventAuthLocked, e, ip, door, "this inbound endpoint is locked after repeated failures", retry, 0)
			return Result{Status: http.StatusTooManyRequests, Reason: "endpoint locked",
				RetryAfter: secs(retry), Identity: door}
		}
	}
	if e.Disabled {
		// Not a credential failure: nothing to lock, nothing to learn from.
		return refuse(http.StatusForbidden, "this inbound endpoint is switched off")
	}
	if !e.Configured() {
		return refuse(http.StatusForbidden,
			"vendor has no inbound security configured — set a token, password, JWT or IP allow-list on this endpoint")
	}
	// 4. IP allow-list: a condition, not an alternative.
	if len(e.AllowedIPs) > 0 && !webhookauth.IPAllowed(ip, e.AllowedIPs) {
		g.event(seclog.EventIPNotAllowed, e, ip, door, "source address is not in this endpoint's allow-list", 0, 0)
		return g.fail(e, ip, door, "source address "+ip+" is not in the allow-list")
	}
	// 5. Credentials — alternatives, as for an account.
	if e.HasCredential() {
		if reason := g.checkCredentials(r, body, e, urlToken); reason != "" {
			return g.fail(e, ip, door, reason)
		}
	} else if e.Legacy.Configured() {
		// The pre-0121 config still answers while this door has none of its
		// own, so an upgrade never locks a working vendor out.
		if okLegacy, why := e.Legacy.Check(r, urlToken, nil); !okLegacy {
			return g.fail(e, ip, door, "legacy inbound_config: "+why)
		}
	}
	// 6. Throughput, once we know who is calling: a limit is about capacity,
	//    and refusing an unauthenticated caller costs less than counting it.
	if g.grant != nil && e.MaxThroughput > 0 {
		if g.grant.Grant("inrate:"+e.VendorCID+":"+e.Kind, e.MaxThroughput, 1) < 1 {
			return Result{Status: http.StatusTooManyRequests,
				Reason: "over this endpoint's callbacks-per-second limit", RetryAfter: 1, Identity: door}
		}
	}
	if g.lock != nil {
		g.lock.Reset(door)
	}
	return ok()
}

// fail records one authentication failure: the door's ladder, the spray
// guard's view of this address, and a security event.
func (g *Guard) fail(e Endpoint, ip, door, reason string) Result {
	rung := 0
	var retry time.Duration
	if g.lock != nil {
		_, retry, rung = g.lock.Fail(door)
	}
	if g.spray != nil {
		g.spray.Observe(ip, door)
	}
	g.event(seclog.EventAuthFail, e, ip, door, reason, retry, rung)
	if rung >= 3 {
		g.event(seclog.EventLadderRung, e, ip, door, "this inbound endpoint has failed repeatedly — check the vendor's configuration", retry, rung)
	}
	// Always the same answer, whatever failed: naming the check that refused
	// tells a guesser which one to work on next.
	return Result{Status: http.StatusForbidden, Reason: reason, Identity: door}
}

func (g *Guard) event(typ string, e Endpoint, ip, door, reason string, retry time.Duration, rung int) {
	if g.report == nil {
		return
	}
	g.report(seclog.Event{Type: typ, Identity: door, IP: ip, Reason: reason,
		Rung: rung, RetryAfter: retry})
}

func (g *Guard) clientIP(r *http.Request) string {
	if g.ip != nil {
		return g.ip(r)
	}
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 && !strings.HasSuffix(host, "]") {
		return strings.Trim(host[:i], "[]")
	}
	return host
}

func secs(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(d.Seconds() + 0.999)
}

// checkCredentials tries the schemes this door carries. "" = satisfied.
//
// The schemes are alternatives, like an account's: a door may carry a token
// AND a password so the vendor can move from one to the other without a flag
// day. Presenting one and getting it wrong is a refusal — never a quiet
// fall-through to the next scheme, which would hide the real failure.
func (g *Guard) checkCredentials(r *http.Request, body []byte, e Endpoint, urlToken string) string {
	attempted := false
	req := newRequestValues(r, body)

	if e.TokenHash != "" {
		if raw := presentedToken(req, e, urlToken); raw != "" {
			attempted = true
			if subtle.ConstantTimeCompare([]byte(auth.HashToken(g.pepper, raw)), []byte(e.TokenHash)) == 1 {
				return ""
			}
			return "token is wrong"
		}
	}
	if e.Username != "" && e.Password != "" {
		user, pass, okBasic := userAndPassword(req, e)
		if okBasic && user != "" {
			attempted = true
			if user != e.Username {
				return "username is wrong"
			}
			if g.creds.verify(e.Password, pass) {
				return ""
			}
			return "password is wrong"
		}
	}
	if e.JWT.Secret != "" || e.JWT.SecretRef != "" || e.JWT.PublicKey != "" {
		if tok := presentedJWT(req, e); tok != "" {
			attempted = true
			return g.checkJWT(tok, e)
		}
	}
	if attempted {
		return "credentials did not match"
	}
	return "no credentials presented"
}

// checkJWT verifies a token with the SAME code the submit edge uses
// (internal/auth): one implementation, so the two edges cannot drift.
func (g *Guard) checkJWT(tok string, e Endpoint) string {
	hdrAlg, _, signing, sig, claims, _, reason := auth.SplitToken(tok)
	if reason != "" {
		return "jwt: " + reason
	}
	keys := auth.Keys{}
	if strings.EqualFold(orHS(e.JWT.Alg), "HS256") {
		for _, ref := range []string{e.JWT.SecretRef, e.JWT.SecretRefPrev} {
			if ref == "" || g.secrets == nil {
				continue
			}
			if v, err := g.secrets.Resolve(context.Background(), ref); err == nil {
				keys.HS256 = append(keys.HS256, v)
			}
		}
		if e.JWT.Secret != "" {
			keys.HS256 = append(keys.HS256, []byte(e.JWT.Secret))
		}
	} else if e.JWT.PublicKey != "" {
		pub, err := auth.ParsePublicKey(e.JWT.PublicKey)
		if err != nil {
			return "jwt: bad public key"
		}
		keys.Public = pub
	}
	if reason := auth.VerifySignature(e.JWT, hdrAlg, signing, sig, keys); reason != "" {
		return "jwt: " + reason
	}
	if reason := auth.CheckClaims(claims, e.JWT, time.Now()); reason != "" {
		return "jwt: " + reason
	}
	// The subject must name this vendor when the door asks for it — the same
	// idea as an account's sub_claim, so one vendor's token cannot open
	// another's door.
	if want := e.JWT.SubClaim; want != "" && want != "sub" {
		// A non-default sub_claim names a claim we do not decode here; the
		// signature already proves the holder, so this is left to the
		// operator's own issuer policy rather than half-checked.
		return ""
	}
	if claims.Sub != "" && !strings.EqualFold(claims.Sub, e.VendorCID) &&
		!strings.EqualFold(claims.Sub, e.VendorCID+":"+e.Kind) {
		return "jwt: subject is not this vendor"
	}
	return ""
}

func orHS(alg string) string {
	if alg == "" {
		return "HS256"
	}
	return alg
}

// credCache is the verified-credential cache, the same idea as the submit
// edge's: once a password has passed bcrypt, the same password is accepted
// again behind a constant-time compare. bcrypt is tens of milliseconds of
// pure CPU, and a busy vendor's receipts would pay it per callback.
type credCache struct {
	*verifiedCache
}

func newCredCache() *credCache { return &credCache{newVerifiedCache()} }

func (c *credCache) verify(bcryptHash, presented string) bool {
	if bcryptHash == "" || presented == "" {
		return false
	}
	if c.hit(bcryptHash, presented) {
		return true
	}
	if bcrypt.CompareHashAndPassword([]byte(bcryptHash), []byte(presented)) != nil {
		return false
	}
	c.store(bcryptHash, presented)
	return true
}
