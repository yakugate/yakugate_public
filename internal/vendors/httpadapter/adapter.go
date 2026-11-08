package httpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"github.com/yakugate/yakugate/internal/platform/outhttp"
)

var phRe = regexp.MustCompile(`\{(\w+)\}`)

// render substitutes {key} placeholders from ctx (unknown -> empty).
func render(tmpl string, ctx map[string]string) string {
	return phRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		return ctx[m[1:len(m)-1]]
	})
}

// renderEscaped is render for a raw JSON or XML body: each value is escaped
// for where it lands (2026-09-14). A message text holding a quote used to
// break a JSON body template, and one holding "&" or "<" an XML one.
func renderEscaped(tmpl string, ctx map[string]string, esc func(string) string) string {
	return phRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		return esc(ctx[m[1:len(m)-1]])
	})
}

// jsonEscape escapes s for use INSIDE a JSON string (no surrounding quotes).
func jsonEscape(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
}

// xmlEscape escapes s for XML text and attribute values.
func xmlEscape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return strings.ReplaceAll(b.String(), `"`, "&#34;")
}

// extract returns the first capture group of pattern applied to s ("" if none).
func extract(pattern, s string) string {
	if pattern == "" {
		return ""
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return ""
	}
	if m := re.FindStringSubmatch(s); len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

// ErrPermanent marks a vendor response that will never succeed on retry (the
// status code is neither a success nor in the call's RetryCodes). Callers
// should finalize the message as failed instead of re-queueing it.
var ErrPermanent = errors.New("permanent vendor rejection")

// Rejection is a permanent refusal that carries the vendor's exact HTTP
// status, so the record and the client's receipt can say which one it was
// (PO 2026-09-17: "carry exact status codes with receipt and record") —
// until then both held a placeholder and the status lived in the log only.
// It unwraps to ErrPermanent, so every errors.Is(err, ErrPermanent) caller
// keeps working unchanged.
type Rejection struct {
	Code int
	Body string
}

func (r *Rejection) Error() string {
	return fmt.Sprintf("vendor rejected (http %d): %.160s", r.Code, r.Body)
}
func (r *Rejection) Unwrap() error { return ErrPermanent }

// RejectionCode is the HTTP status behind a permanent refusal, when the error
// carries one; ok=false for a permanent error of any other origin.
func RejectionCode(err error) (int, bool) {
	var r *Rejection
	if errors.As(err, &r) {
		return r.Code, true
	}
	return 0, false
}

// extractPath walks decoded JSON by dot-path ("result.messageId") and returns
// the value as a string ("" when absent). Numbers/bools stringify.
func extractPath(body, path string) string {
	if path == "" {
		return ""
	}
	var v any
	if json.Unmarshal([]byte(body), &v) != nil {
		return ""
	}
	for _, seg := range strings.Split(path, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return ""
		}
		if v, ok = m[seg]; !ok {
			return ""
		}
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		return strings.TrimSuffix(strconv.FormatFloat(t, 'f', -1, 64), ".0")
	case bool:
		return strconv.FormatBool(t)
	default:
		return ""
	}
}

// extractVal reads one named response value: JSON dot-path first (GW-15), then
// the legacy regex.
func extractVal(call Call, name, body string) string {
	if p := call.ExtractJSON[name]; p != "" {
		if v := extractPath(body, p); v != "" {
			return call.mapRead(name, v)
		}
	}
	if v := call.mapRead(name, extract(call.Extract[name], body)); v != "" {
		return v
	}
	return call.ExtractDefaults[name] // nothing read: the value's default, if any
}

// readNames are the response values a call yields: those read from the
// answer and those with only a default (a fixed value).
func (call Call) readNames() []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range []map[string]string{call.Extract, call.ExtractJSON, call.ExtractDefaults} {
		for name := range m {
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
	}
	return out
}

// mapRead applies a response value's own mapping (ExtractMaps): the
// vendor's word -> ours. A value not listed, or empty, is kept as read.
func (call Call) mapRead(name, v string) string {
	if m := call.ExtractMaps[name]; len(m) > 0 && v != "" {
		if mapped, ok := m[v]; ok {
			return mapped
		}
	}
	return v
}

// NotAccepted is an answer the vendor GAVE that the send call does not read
// as an acceptance: a 2xx or 3xx whose body fails the call's success rule.
//
// The message is not sent, exactly as before ("not now": the next vendor of
// its round, then later rounds). What this type adds is the other half of the
// fact: the vendor is REACHABLE AND ANSWERING. The role used to count such an
// answer with the calls that got none, so a vendor answering 200 to every
// call was shown as down, and then sent one trial message at a time, because
// OUR success rule was written wrong (Saleem, 2026-10-12: "how can an http
// endpoint be down"). The fault is in the call's settings, and the vendor's
// health must not say otherwise.
type NotAccepted struct {
	Code int
	Text string
}

func (e *NotAccepted) Error() string { return e.Text }

// notOK is "vendor response not OK", typed by what kind of answer it was: a
// status under 400 is the vendor answering (NotAccepted); 5xx, 429 and 408
// reach here only when no retry codes are listed, and are the vendor's own
// trouble — a plain error, as before.
func notOK(code int, body string) error {
	text := fmt.Sprintf("vendor response not OK (http %d): %.160s", code, body)
	if code < 400 {
		return &NotAccepted{Code: code, Text: text}
	}
	return errors.New(text)
}

// Answered reports whether a failed call was nevertheless ANSWERED by the
// vendor: refused outright (ErrPermanent), or answered with something the
// call does not accept (NotAccepted). Either way the vendor is up.
func Answered(err error) bool {
	var na *NotAccepted
	return errors.Is(err, ErrPermanent) || errors.As(err, &na)
}

// AuthError is a call that could not be made because no session could be
// got: the login failed, or its answer held no session. The vendor was never
// asked to send anything.
type AuthError struct{ Err error }

func (e *AuthError) Error() string { return "auth: " + e.Err.Error() }
func (e *AuthError) Unwrap() error { return e.Err }

// NoSession reports whether err is a call that failed for want of a session.
func NoSession(err error) bool {
	var ae *AuthError
	return errors.As(err, &ae)
}

// idSource is where a call reads the vendor's message id from: the JSON path
// or the pattern the operator gave. "" means the call names none — the vendor
// gives no id of its own.
func idSource(call Call) string {
	if p := call.ExtractJSON["msgid"]; p != "" {
		return p
	}
	return call.Extract["msgid"]
}

// IDSource is where the send call reads the vendor's message id from, or ""
// when it reads none. The role uses it to say so when an accepted answer had
// nothing there.
func (a *Adapter) IDSource() string { return idSource(a.p.Send) }

// identMatches checks one body assertion: dot-path equality on JSON, else a
// <tag>value</tag> / regex fallback on non-JSON bodies.
func identMatches(id Ident, body string) bool {
	if v := extractPath(body, id.Source); v != "" {
		return v == id.Value
	}
	// The same reading as an XML response value: a tag path (any namespace
	// prefix) or, when the operator wrote one, a regex (QA SOAP-001).
	return extract(xmlSourceRegex(id.Source), body) == id.Value
}

// classify evaluates a response against the call's GW-15 success semantics.
// ok=true means success. When not ok, err is ErrPermanent-wrapped unless the
// code is retryable. Legacy calls (no OKCodes/OKIdents/RetryCodes) return
// ok=true and let the caller's existing OKMatch/2xx logic decide.
func classify(call Call, body string, code int) (ok bool, err error) {
	if len(call.OKCodes) == 0 && len(call.OKIdents) == 0 && len(call.RetryCodes) == 0 {
		return true, nil
	}
	codeOK := code < 400
	if len(call.OKCodes) > 0 {
		codeOK = false
		for _, c := range call.OKCodes {
			if code == c {
				codeOK = true
				break
			}
		}
	}
	identsOK := true
	for _, id := range call.OKIdents {
		if !identMatches(id, body) {
			identsOK = false
			break
		}
	}
	if codeOK && identsOK {
		return true, nil
	}
	for _, c := range call.RetryCodes {
		if code == c {
			return false, fmt.Errorf("vendor transient http %d: %.160s", code, body)
		}
	}
	if len(call.RetryCodes) > 0 {
		return false, &Rejection{Code: code, Body: body}
	}
	// No Retryable Status Codes listed (QA EP-019, 2026-10-01): the vendor's
	// own trouble — 5xx, 429 Too Many Requests, 408 Request Timeout — is
	// worth another vendor or a later round, so it stays "not now". Any other
	// 4xx is the vendor refusing THIS request (bad number, bad credentials,
	// bad body); sending it again cannot help, so the message fails at once
	// with the vendor's code, as when codes are listed. Anything else that is
	// not a success (a 2xx/3xx whose body is not the success answer) keeps
	// the old reading.
	if code >= 400 && code < 500 && !transientClientCode(code) {
		return false, &Rejection{Code: code, Body: body}
	}
	return false, notOK(code, body)
}

// transientClientCode is a 4xx that says "later", not "never".
func transientClientCode(code int) bool {
	return code == 408 || code == 429
}

// SessionStore shares auth-extracted session vars across every process and
// instance using one vendor (PO 2026-08-10: persisted in MySQL, keyed by
// vendor id). Implemented by internal/store/mysql on the queue store.
type SessionStore interface {
	LoadVendorSession(ctx context.Context, vendorID int64) (vars map[string]string, expiresAt time.Time, err error)
	SaveVendorSession(ctx context.Context, vendorID int64, cid string, vars map[string]string, expiresAt time.Time) error
	DeleteVendorSession(ctx context.Context, vendorID int64) error
	// LockVendorLogin serializes the login so N instances with an expired
	// session produce ONE new login, not N (some vendors invalidate the
	// previous session on every login). Returns the release func.
	LockVendorLogin(ctx context.Context, vendorID int64) (release func(), err error)
}

// memRecheck caps how long the in-memory copy of a SHARED session is trusted
// before re-reading the store, so an operator-deleted row (forced re-login)
// propagates to every instance quickly instead of after the full auth TTL.
const memRecheck = 30 * time.Second

// Adapter drives one vendor Profile. Safe for concurrent Send/Status.
type Adapter struct {
	p        Profile
	cl       *http.Client
	store    SessionStore // nil = in-memory session only (tests / storeless)
	vendorID int64
	cid      string
	mu       sync.Mutex
	sess     map[string]string // cached auth-extracted vars (e.g. session_id)
	sessExp  time.Time         // vendor-side session validity
	memExp   time.Time         // when to re-check the shared store (<= sessExp)

	// observe, when set, hears the HTTP status of every send request — 0 when
	// no response came back at all. Monitoring only (P4's response-code
	// breakdown); never consulted for a decision.
	observe func(code int)

	// Session telemetry for monitoring, under mu. Counts and times only: the
	// session variables are credentials and never leave this struct.
	logins        int64
	loginFailures int64
	adopted       int64 // sessions taken over from another instance via the shared store
	lastLoginAt   time.Time
	lastLoginErr  string
}

// ObserveResponses registers f to hear the HTTP status of every send request
// (single and bulk, including a re-auth retry), or 0 when the request got no
// response. Call before the adapter is shared; f must be cheap and safe for
// concurrent use.
func (a *Adapter) ObserveResponses(f func(code int)) { a.observe = f }

// SessionInfo is a session-auth vendor's login state, for monitoring.
type SessionInfo struct {
	Active         bool   `json:"active"`
	ExpiresAt      string `json:"expires_at,omitempty"`
	ExpiresInSec   int64  `json:"expires_in_sec"`
	Logins         int64  `json:"logins"`
	LoginFailures  int64  `json:"login_failures"`
	Adopted        int64  `json:"sessions_adopted"`
	LastLoginAt    string `json:"last_login_at,omitempty"`
	LastLoginError string `json:"last_login_error,omitempty"`
}

// SessionInfo reports the login state. ok=false for a vendor that does not
// log in at all, so a caller can omit the block rather than show zeros that
// look like a broken login.
func (a *Adapter) SessionInfo() (SessionInfo, bool) {
	if a.p.Auth == nil {
		return SessionInfo{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	si := SessionInfo{Logins: a.logins, LoginFailures: a.loginFailures, Adopted: a.adopted,
		LastLoginError: a.lastLoginErr}
	if !a.sessExp.IsZero() {
		si.ExpiresAt = a.sessExp.UTC().Format(time.RFC3339)
		if now.Before(a.sessExp) {
			si.Active = true
			si.ExpiresInSec = int64(a.sessExp.Sub(now).Seconds())
		}
	}
	if !a.lastLoginAt.IsZero() {
		si.LastLoginAt = a.lastLoginAt.UTC().Format(time.RFC3339)
	}
	return si, true
}

func (a *Adapter) noteResponse(code int) {
	if a.observe != nil {
		a.observe(code)
	}
}

// New builds an adapter with an in-memory-only session (no sharing).
func New(p Profile) *Adapter {
	// On the shared connection pool (outhttp): a send call that runs many at
	// a time uses a connection again instead of opening one a message.
	return &Adapter{p: p, cl: outhttp.Client(20 * time.Second), sess: map[string]string{}}
}

// NewShared builds an adapter whose session vars are shared through store,
// keyed by vendorID. A nil store degrades to New's in-memory behavior.
func NewShared(p Profile, vendorID int64, cid string, store SessionStore) *Adapter {
	a := New(p)
	a.store, a.vendorID, a.cid = store, vendorID, cid
	return a
}

// Send delivers one message and returns the vendor-assigned message id.
// fields carries the canonical message (to, text, sender, from, msgid, ...).
// SubmitURL is the vendor's configured send endpoint, for monitoring display.
//
// Returned WHOLE — the caller trims it. A vendor URL routinely carries an API
// key in its query string, and this value reaches a dashboard and the stored
// monitoring history, so whoever renders it is responsible for cutting it back
// to scheme+host (see httpvendor.hostOf).
func (a *Adapter) SubmitURL() string { return a.p.Send.URL }

func (a *Adapter) Send(ctx context.Context, fields map[string]string) (string, error) {
	var body string
	var code int
	err := a.withAuthRetry(ctx, fields, func(c map[string]string) (string, int, error) {
		var e error
		body, code, e = a.doSend(ctx, c)
		return body, code, e
	})
	if err != nil {
		return "", err
	}
	if ok, cErr := classify(a.p.Send, body, code); !ok {
		return "", cErr
	} else if a.p.Send.OKMatch != "" {
		if ok, _ := regexp.MatchString(a.p.Send.OKMatch, body); !ok {
			return "", notOK(code, body)
		}
	} else if len(a.p.Send.OKCodes) == 0 && code >= 400 {
		return "", fmt.Errorf("vendor http %d: %.160s", code, body)
	}
	// The success rule above is the WHOLE of "accepted" (Saleem, 2026-10-12:
	// "if there is nothing so, it should be undefined, not failed"). The
	// vendor's id is read when it is there; an answer without one — the call
	// maps none, or nothing is at the path it maps — is still an accepted
	// message with no vendor id. "" tells the caller so: it keeps its own id
	// as the reference and says in the vendor's log when a mapped path read
	// nothing.
	//
	// Until now an empty id failed the message here, AFTER the vendor had
	// taken it, and it was handed to the next vendor: sent twice. The id is
	// not a success check; a vendor that answers 200 with an error in the
	// body is told apart by the success rule's body identifier.
	return extractVal(a.p.Send, "msgid", body), nil
}

// Status polls the vendor for a message's delivery status (mode=poll). ok=false
// when the profile has no poll status or nothing conclusive was returned.
func (a *Adapter) Status(ctx context.Context, vendorMsgID string) (canonical string, ok bool, err error) {
	canonical, _, ok, err = a.StatusDetail(ctx, vendorMsgID)
	return canonical, ok, err
}

// StatusDetail is Status plus the vendor's error code for the outcome, read
// from the poll answer under the response name "error_code" ("" when the
// vendor's poll settings do not name one). The code is what a vendor's
// receipt status codes are looked up by (PO 2026-09-15: receipt codes for
// HTTP vendors too), and it is kept on the receipt record.
func (a *Adapter) StatusDetail(ctx context.Context, vendorMsgID string) (canonical, errorCode string, ok bool, err error) {
	if a.p.Status == nil || a.p.Status.Mode != "poll" {
		return "", "", false, nil
	}
	var body string
	// The id to ask about is the vendor's own, under every name a poll call
	// may use for it: {vendor_msg_id}, {operator_id} and {msgid} — the
	// seeded sample poll rows say {msgid}, and it went out empty (QA EP-022).
	ids := map[string]string{"vendor_msg_id": vendorMsgID, "operator_id": vendorMsgID, "msgid": vendorMsgID}
	err = a.withAuthRetry(ctx, ids, func(c map[string]string) (string, int, error) {
		var e error
		var code int
		body, code, e = a.do(ctx, a.p.Status.Call, c)
		return body, code, e
	})
	if err != nil {
		return "", "", false, err
	}
	raw := extractVal(a.p.Status.Call, "status", body)
	if raw == "" {
		return "", "", false, nil
	}
	errorCode = strings.TrimSpace(extractVal(a.p.Status.Call, "error_code", body))
	if m, ok := a.p.Status.Map[raw]; ok {
		return m, errorCode, true, nil
	}
	return strings.ToLower(raw), errorCode, true, nil
}

// withAuthRetry runs one authenticated request. When the response says the
// session died early (HTTP 401, or the profile's reauth_on regex matches), the
// cached session is dropped everywhere and the login re-runs — ONE retry, so a
// vendor that 401s everything cannot loop.
func (a *Adapter) withAuthRetry(ctx context.Context, fields map[string]string, call func(map[string]string) (string, int, error)) error {
	for attempt := 0; ; attempt++ {
		c := a.newCtx(fields)
		if a.p.Auth != nil {
			if err := a.ensureAuth(ctx, c); err != nil {
				return &AuthError{Err: err}
			}
		}
		body, code, err := call(c)
		if err != nil {
			return err
		}
		if attempt == 0 && a.p.Auth != nil && a.sessionRejected(body, code) {
			a.invalidate(ctx)
			continue
		}
		return nil
	}
}

// sessionRejected reports whether a response means "your session/token is no
// longer valid" per the profile.
func (a *Adapter) sessionRejected(body string, code int) bool {
	if code == http.StatusUnauthorized {
		return true
	}
	if a.p.Auth == nil {
		return false
	}
	if a.p.Auth.ReauthOn != "" {
		if ok, _ := regexp.MatchString(a.p.Auth.ReauthOn, body); ok {
			return true
		}
	}
	for _, c := range a.p.Auth.ReauthCodes { // GW-15: sessionExpiredStatusCodes
		if code == c {
			return true
		}
	}
	for _, id := range a.p.Auth.ReauthIdents {
		if identMatches(id, body) {
			return true
		}
	}
	return false
}

// invalidate drops the cached session from memory AND the shared store so the
// next ensureAuth (here and in every other instance) performs a fresh login.
func (a *Adapter) invalidate(ctx context.Context) {
	a.mu.Lock()
	a.sess, a.sessExp, a.memExp = map[string]string{}, time.Time{}, time.Time{}
	a.mu.Unlock()
	if a.store != nil {
		a.store.DeleteVendorSession(ctx, a.vendorID)
	}
}

// Revoke ends the vendor-side session (the profile's auth.revoke call, when
// configured) and deletes the shared session row so every instance drops it.
// Called by P4 when a vendor is deactivated/removed; safe with no live session.
func (a *Adapter) Revoke(ctx context.Context) error {
	// Adopt whatever session exists (memory, else store) so {token}/{session_id}
	// render in the revoke call.
	c := a.newCtx(nil)
	a.mu.Lock()
	hasSess := len(a.sess) > 0
	a.mu.Unlock()
	if !hasSess && a.store != nil {
		if vars, exp, err := a.store.LoadVendorSession(ctx, a.vendorID); err == nil && len(vars) > 0 && time.Now().Before(exp) {
			hasSess = true
			for k, v := range vars {
				c[k] = v
			}
		}
	}
	var callErr error
	if a.p.Auth != nil && a.p.Auth.Revoke != nil && hasSess {
		_, _, callErr = a.do(ctx, *a.p.Auth.Revoke, c)
	}
	a.invalidate(ctx)
	return callErr
}

// PollMode reports whether this vendor's status is obtained by polling.
func (a *Adapter) PollMode() bool { return a.p.Status != nil && a.p.Status.Mode == "poll" }

// PollInterval is the vendor's CONFIGURED status-poll cadence; 0 = not
// configured, meaning the caller's own back-off per message applies. (It
// used to mean "every Step cycle", which polled each message twice a second
// for ever — QA EP-021, SOAP-006.)
func (a *Adapter) PollInterval() time.Duration {
	if a.p.Status != nil && a.p.Status.Interval > 0 {
		return time.Duration(a.p.Status.Interval) * time.Second
	}
	return 0
}

// newCtx seeds the render context with the profile vars then the message fields.
func (a *Adapter) newCtx(fields map[string]string) map[string]string {
	c := make(map[string]string, len(a.p.Vars)+len(fields)+len(a.sess))
	for k, v := range a.p.Vars {
		c[k] = v
	}
	a.mu.Lock()
	for k, v := range a.sess {
		c[k] = v
	}
	a.mu.Unlock()
	for k, v := range fields {
		c[k] = v
	}
	return c
}

// NeedsSession reports whether this vendor's calls go out under a session:
// its send call logs in first.
func (a *Adapter) NeedsSession() bool { return a.p.Auth != nil }

// EnsureSession makes sure the vendor has a session NOW: the one in memory,
// the shared one in the store, or a fresh login — the same steps a send takes
// before its call. The role calls it when it loads the vendor (Saleem,
// 2026-10-12: "if http endpoint has session, then session must be got on
// startup of vendor connector"), so a login that cannot work is known before
// the first message, and the first message does not wait on a login.
// nil for a vendor that needs no session.
func (a *Adapter) EnsureSession(ctx context.Context) error {
	if a.p.Auth == nil {
		return nil
	}
	if err := a.ensureAuth(ctx, a.newCtx(nil)); err != nil {
		return &AuthError{Err: err}
	}
	return nil
}

// ensureAuth reuses a valid session (memory first, then the shared store) or
// performs the login chain, and merges the session vars into c. With a store,
// the login runs under a per-vendor lock so N instances hitting an expired
// session produce ONE fresh login, and the result is persisted for all.
func (a *Adapter) ensureAuth(ctx context.Context, c map[string]string) error {
	now := time.Now()
	a.mu.Lock()
	memOK := now.Before(a.sessExp) && len(a.sess) > 0 && (a.store == nil || now.Before(a.memExp))
	if memOK {
		for k, v := range a.sess {
			c[k] = v
		}
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()

	if a.store != nil {
		if a.adoptStored(ctx, c) {
			return nil
		}
		if release, err := a.store.LockVendorLogin(ctx, a.vendorID); err == nil {
			defer release()
			// Someone else may have logged in while we waited on the lock.
			if a.adoptStored(ctx, c) {
				return nil
			}
		} // lock failure: degrade to an unserialized login rather than not sending
	}

	sess, err := a.login(ctx, c)
	if err != nil {
		a.mu.Lock()
		a.loginFailures++
		a.lastLoginErr = err.Error()
		if len(a.lastLoginErr) > 160 {
			a.lastLoginErr = a.lastLoginErr[:160] + "…"
		}
		a.mu.Unlock()
		return err
	}
	exp := time.Now().Add(a.authTTL())
	a.mu.Lock()
	a.logins++
	a.lastLoginAt = time.Now()
	a.lastLoginErr = ""
	a.sess, a.sessExp = sess, exp
	a.memExp = memCap(exp)
	a.mu.Unlock()
	if a.store != nil {
		if err := a.store.SaveVendorSession(ctx, a.vendorID, a.cid, sess, exp); err != nil {
			return fmt.Errorf("persist session: %w", err)
		}
	}
	for k, v := range sess {
		c[k] = v
	}
	return nil
}

// adoptStored loads the shared session row; when valid it becomes this
// instance's cached session and is merged into c.
func (a *Adapter) adoptStored(ctx context.Context, c map[string]string) bool {
	vars, exp, err := a.store.LoadVendorSession(ctx, a.vendorID)
	if err != nil || len(vars) == 0 || !time.Now().Before(exp) {
		return false
	}
	a.mu.Lock()
	a.sess, a.sessExp = vars, exp
	a.memExp = memCap(exp)
	a.adopted++
	a.mu.Unlock()
	for k, v := range vars {
		c[k] = v
	}
	return true
}

// login runs the auth chain (auth.steps; else the single embedded call). Each
// step's extractions feed the next step's templates; the union is the session.
func (a *Adapter) login(ctx context.Context, c map[string]string) (map[string]string, error) {
	steps := a.p.Auth.Steps
	if len(steps) == 0 {
		steps = []Call{a.p.Auth.Call}
	}
	// Work on a copy so a failed chain never leaks half-extracted vars into c.
	lc := make(map[string]string, len(c))
	for k, v := range c {
		lc[k] = v
	}
	sess := map[string]string{}
	for i, step := range steps {
		body, code, err := a.do(ctx, step, lc)
		if err != nil {
			return nil, fmt.Errorf("auth step %d: %w", i+1, err)
		}
		if step.OKMatch != "" {
			if ok, _ := regexp.MatchString(step.OKMatch, body); !ok {
				return nil, fmt.Errorf("auth step %d response not OK (http %d): %.160s", i+1, code, body)
			}
		} else if code >= 400 {
			return nil, fmt.Errorf("auth step %d http %d: %.160s", i+1, code, body)
		}
		gotAny := len(step.Extract) == 0 && len(step.ExtractJSON) == 0
		for _, name := range step.readNames() {
			v := extractVal(step, name, body)
			// Only a value READ from the answer shows the login worked; a
			// fixed value (a default with no place) is there whatever came.
			if v != "" && (step.Extract[name] != "" || step.ExtractJSON[name] != "") && v != step.ExtractDefaults[name] {
				gotAny = true
			}
			sess[name] = v
			lc[name] = v
		}
		// A step whose every extraction came back empty is a failed login even
		// on HTTP 200 — caching it would poison EVERY instance via the store.
		if !gotAny {
			return nil, fmt.Errorf("auth step %d extracted nothing (http %d): %.160s", i+1, code, body)
		}
	}
	return sess, nil
}

func (a *Adapter) authTTL() time.Duration {
	ttl := a.p.Auth.TTLSec
	if ttl <= 0 {
		ttl = 1800
	}
	return time.Duration(ttl) * time.Second
}

// memCap bounds in-memory trust of a shared session (see memRecheck).
func memCap(exp time.Time) time.Time {
	if m := time.Now().Add(memRecheck); m.Before(exp) {
		return m
	}
	return exp
}

// do renders and executes one Call, returning the response body and status code.
//
// A row's Timeout (ms) applies, but never past the adapter's own client
// timeout (20 s), the wait every vendor call had before the row was read: a
// claimed message is leased for 30 s, and a call outliving its lease lets
// another http-vendor process claim and send the same message again.
func (a *Adapter) do(ctx context.Context, call Call, c map[string]string) (string, int, error) {
	if lim := a.cl.Timeout; lim > 0 && time.Duration(call.TimeoutMS)*time.Millisecond > lim {
		call.TimeoutMS = int(lim / time.Millisecond)
	}
	return doCall(ctx, a.cl, call, c)
}

// doSend runs the send call, re-sending it at once by the row's Retry (JSON)
// when the vendor said "not now" or could not be reached (RetrySpec). Every
// try is counted for monitoring. A re-send is started only when it can finish
// inside retryWindow of the first try (its wait plus the call's own timeout),
// so the retries never carry a message past its queue lease.
func (a *Adapter) doSend(ctx context.Context, c map[string]string) (string, int, error) {
	call := a.p.Send
	tries := call.Retry.tries()
	perTry := a.cl.Timeout
	if t := time.Duration(call.TimeoutMS) * time.Millisecond; t > 0 && (perTry <= 0 || t < perTry) {
		perTry = t
	}
	start := time.Now()
	for i := 0; ; i++ {
		body, code, err := a.do(ctx, call, c)
		a.noteResponse(code) // 0 when the request never got an answer
		if i+1 >= tries || !retryNow(call, code, err) {
			return body, code, err
		}
		wait := call.Retry.delay(i)
		if time.Since(start)+wait+perTry > retryWindow || !sleepCtx(ctx, wait) {
			return body, code, err
		}
	}
}

// CloseIdleConnections drops the idle connections of the calls that carry
// their own TLS transport. Called on an adapter a reload replaced.
func (a *Adapter) CloseIdleConnections() {
	calls := []*Call{&a.p.Send}
	if a.p.Status != nil {
		calls = append(calls, &a.p.Status.Call)
	}
	if a.p.Auth != nil {
		calls = append(calls, &a.p.Auth.Call, a.p.Auth.Revoke)
		for i := range a.p.Auth.Steps {
			calls = append(calls, &a.p.Auth.Steps[i])
		}
	}
	for _, c := range calls {
		if c == nil {
			continue
		}
		if t, ok := c.Transport.(interface{ CloseIdleConnections() }); ok {
			t.CloseIdleConnections()
		}
	}
}

// clientFor is cl with the call's own timeout and TLS transport, or cl itself
// when the call has neither.
func clientFor(cl *http.Client, call Call) *http.Client {
	if call.TimeoutMS <= 0 && call.Transport == nil {
		return cl
	}
	c := *cl
	if call.TimeoutMS > 0 {
		c.Timeout = time.Duration(call.TimeoutMS) * time.Millisecond
	}
	if call.Transport != nil {
		c.Transport = call.Transport
	}
	return &c
}

// doCall renders and executes one Call — package-level so provider-style
// one-shot callers (FX, HLR dip; GW-15 Phase B) reuse the exact request
// machinery P4 uses. On a transport error the call fails over to each of
// call.Hosts in order (Hadal hosts[]).
func doCall(ctx context.Context, cl *http.Client, call Call, c map[string]string) (string, int, error) {
	cl = clientFor(cl, call)
	c = transformed(call, c)
	urls := append([]string{call.URL}, call.Hosts...)
	var lastErr, reached error
	for _, base := range urls {
		req, err := newRequest(ctx, call, base, c)
		if err != nil {
			return "", 0, err
		}
		// The call as it went, when the caller asked for it (a vendor's
		// send, kept with the message's sub_ row; capture.go).
		rec := recorderFrom(ctx)
		var cc CapturedCall
		if rec != nil {
			cc = captureRequest(req)
		}
		resp, err := cl.Do(req)
		if err != nil {
			err = bareURLError(err)
			if rec != nil {
				cc.Took, cc.Err = time.Since(cc.SentAt), Cut(err.Error(), ErrorCap)
				rec.add(cc)
			}
			lastErr = err // transport error: try the next failover host
			if reached == nil && !notConnected(err) {
				reached = err // this host may have the request (a timeout, a dropped answer)
			}
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if rec != nil {
			cc.Took, cc.Status = time.Since(cc.SentAt), resp.StatusCode
			cc.RespHeaders = MaskHeaders(resp.Header, "")
			cc.RespBody, cc.RespCut = Keep(MaskBody(b), RespBodyCap)
			rec.add(cc)
		}
		return string(b), resp.StatusCode, nil
	}
	if reached != nil && notConnected(lastErr) {
		// The last host could not be reached, but an earlier one may hold
		// the request: say so, so the send's Retry (JSON), which re-sends
		// only what never left, does not send it a second time.
		return "", 0, fmt.Errorf("%v (and an earlier host: %w)", lastErr, reached)
	}
	return "", 0, lastErr
}

// bareURLError takes the query string and any login out of the URL a
// transport error names. Go's client writes the whole URL into the error
// ("Get \"https://…/send?message=…&mobilenum=…&password=…\": …"), and that
// error went to the vendor's log, to Monitoring's last error and to the
// Routing Rules "why": the vendor's password, the message and the number,
// on screens and in files (go-live test of 18 Oct 2026, P4 A4, old bug
// P4-4). The address and path stay, to say which host failed; "?…" says a
// query was there.
func bareURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		ue.URL = bareURL(ue.URL)
	}
	return err
}

// bareURL is raw with no login and no query string.
func bareURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		if i := strings.IndexAny(raw, "?#"); i >= 0 {
			return raw[:i] + "?…"
		}
		return raw
	}
	u.User = nil
	u.Fragment, u.RawFragment = "", ""
	if u.RawQuery != "" || u.ForceQuery {
		u.RawQuery, u.ForceQuery = "", false
		return u.String() + "?…"
	}
	return u.String()
}

// transformed applies the call's valueMapping (GW-15) — OUR field values into
// the other side's vocabulary — on a copy, so the caller's map is never
// mutated.
func transformed(call Call, c map[string]string) map[string]string {
	c = withTextFormat(call, c)
	if len(call.Transform) == 0 {
		return c
	}
	tc := make(map[string]string, len(c))
	for k, v := range c {
		tc[k] = v
	}
	for field, m := range call.Transform {
		if mapped, ok := m[tc[field]]; ok {
			tc[field] = mapped
		}
	}
	return tc
}

// NewRequest builds the call's request to its primary URL with fields filled
// in, without sending it — for callers that send it themselves (customer
// webhooks, and the webhook tester that shows exactly what would go out).
func NewRequest(ctx context.Context, call Call, fields map[string]string) (*http.Request, error) {
	return newRequest(ctx, call, call.URL, transformed(call, fields))
}

// Judge says whether a response to call counts as success, with the call's
// own GW-15 rules (success codes, identifiers, retry codes); a call without
// any accepts only a 2xx. A failure that will never improve wraps
// ErrPermanent.
func Judge(call Call, body string, code int) error {
	ok, err := classify(call, body, code)
	if !ok {
		return err
	}
	if len(call.OKCodes) == 0 && (code < 200 || code >= 300) {
		return fmt.Errorf("http %d: %.160s", code, body)
	}
	return nil
}

// newRequest renders one request for base (the URL or a failover host).
func newRequest(ctx context.Context, call Call, base string, c map[string]string) (*http.Request, error) {
	method := strings.ToUpper(call.Method)
	if method == "" {
		method = "GET"
	}
	enc := call.Encoding
	if enc == "" {
		enc = "query"
	}
	var body io.Reader
	ctype := ""
	switch enc {
	case "form":
		q := url.Values{}
		setParams(q, call.Params, call.ParamMaps, c)
		body, ctype = strings.NewReader(q.Encode()), "application/x-www-form-urlencoded"
	case "json":
		if call.Body != "" {
			bc := withVars(c, call.BodyVars) // the body's own variable mappings
			body, ctype = strings.NewReader(renderEscaped(withLists(call.Body, bc), bc, jsonEscape)), "application/json"
		} else {
			m := map[string]any{}
			for k, v := range call.Params {
				pm := call.ParamMaps[k]
				if l, ok := listFor(v, c); ok {
					m[k] = pm.list(l)
				} else {
					m[k] = pm.fill(v, c, render)
				}
			}
			b, _ := json.Marshal(m)
			body, ctype = bytes.NewReader(b), "application/json"
		}
	case "xml":
		if call.Body != "" {
			body = strings.NewReader(renderEscaped(call.Body, withVars(c, call.BodyVars), xmlEscape))
		} else {
			// No body template: the Request Parameters become the XML
			// document (QA EP-006 — they were dropped and the vendor got an
			// empty body).
			body = strings.NewReader(xmlFromParams(call.Params, call.ParamMaps, c))
		}
		ctype = "application/xml"
	}
	if ctype != "" && call.ContentType != "" {
		ctype = call.ContentType
	}
	u := render(base, c)
	q := url.Values{}
	if enc == "query" {
		setParams(q, call.Params, call.ParamMaps, c)
	}
	for k, v := range call.QueryParams { // sentIn=QUERY regardless of encoding
		// an XML document in the query is escaped too; its own mapping only
		q.Set(k, call.ParamMaps[k].fill(v, c, renderParam))
	}
	if len(q) > 0 {
		sep := "?"
		if strings.Contains(u, "?") {
			sep = "&"
		}
		u += sep + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	for k, v := range call.Headers {
		req.Header.Set(k, call.ParamMaps[k].fill(v, c, render))
	}
	return req, nil
}

// BulkMode reports whether this vendor takes chunked submissions.
func (a *Adapter) BulkMode() bool { return a.p.Bulk != nil }

// BulkCountsMessages reports TPS Count Mode "Total messages": each message of
// a chunk spends one unit of the vendor's rate, not the call as a whole.
func (a *Adapter) BulkCountsMessages() bool { return a.p.Bulk != nil && a.p.Bulk.CountMessages }

// BulkSingleID reports that the send call says the vendor gives ONE id for a
// whole bulk call ("provides single id").
func (a *Adapter) BulkSingleID() bool { return a.p.Bulk != nil && a.p.Bulk.SingleID }

// BulkChunkSize returns the vendor's max messages per call (0 when not bulk).
func (a *Adapter) BulkChunkSize() int {
	if a.p.Bulk == nil {
		return 0
	}
	return a.p.Bulk.ChunkSize
}

// SendBulk delivers one CHUNK of messages in a single vendor call (GW-15
// slice 1, Hadal submissionMode BULK). All fields render from the FIRST
// message except `to`, which is every recipient joined by the vendor's
// separator — callers must therefore only chunk messages sharing the same
// sender and body. Returns one vendor id per message, position-matched:
// providesSingleId vendors repeat their single id across the chunk; list
// vendors' ids are split on the id separator. SendBulkCall says how many ids
// the answer held.
func (a *Adapter) SendBulk(ctx context.Context, batch []map[string]string) ([]string, error) {
	ids, _, err := a.SendBulkCall(ctx, batch)
	return ids, err
}

// SendBulkCall is SendBulk, and `read` is how many ids the accepted answer
// held: 0 (none), 1 (one for the whole call), len(batch) (one a message), or
// another count.
//
// The success rule is the whole of "accepted" (Saleem, 2026-10-12): an id is
// read when it is there and is never a success check. So an answer the rule
// accepted is never an error for its ids (full go-live retest, 19 Oct 2026,
// P4 B8: a list vendor that answered ONE id for five numbers had its call
// failed with "bulk id count mismatch", and the five messages the vendor had
// taken were handed on and sent again):
//   - one id for the call is the call's id: every message gets it, as for a
//     vendor set to "provides single id". The role files them as one call's
//     group, and a receipt finds its message by the number it names.
//   - a list of another length cannot be matched to the messages: each gets
//     "" (filed under our own id; the role says so in the vendor's log).
func (a *Adapter) SendBulkCall(ctx context.Context, batch []map[string]string) (ids []string, read int, err error) {
	if a.p.Bulk == nil {
		return nil, 0, fmt.Errorf("vendor has no bulk mode")
	}
	if len(batch) == 0 {
		return nil, 0, nil
	}
	// One message goes as an ordinary send — except for an Array vendor,
	// which expects a list even of one.
	if len(batch) == 1 && !a.p.Bulk.Array {
		id, err := a.Send(ctx, batch[0])
		if err != nil {
			return nil, 0, err
		}
		if id != "" {
			read = 1
		}
		return []string{id}, read, nil
	}
	tos := make([]string, len(batch))
	for i, f := range batch {
		tos[i] = f["to"]
	}
	fields := make(map[string]string, len(batch[0]))
	for k, v := range batch[0] {
		fields[k] = v
	}
	fields["to"] = strings.Join(tos, a.p.Bulk.ToSeparator)
	if a.p.Bulk.Array {
		// Bulk Value Type "Array": the recipients as a list where the call
		// can carry one; a body template's unquoted {to} keeps the joined form.
		l, _ := json.Marshal(tos)
		fields[listPrefix+"to"] = string(l)
	}

	var body string
	var code int
	err = a.withAuthRetry(ctx, fields, func(c map[string]string) (string, int, error) {
		var e error
		body, code, e = a.doSend(ctx, c)
		return body, code, e
	})
	if err != nil {
		return nil, 0, err
	}
	if ok, cErr := classify(a.p.Send, body, code); !ok {
		return nil, 0, cErr
	} else if a.p.Send.OKMatch != "" {
		if ok, _ := regexp.MatchString(a.p.Send.OKMatch, body); !ok {
			return nil, 0, notOK(code, body)
		}
	} else if len(a.p.Send.OKCodes) == 0 && code >= 400 {
		return nil, 0, fmt.Errorf("vendor http %d: %.160s", code, body)
	}
	// Every id the answer holds: a JSON list (or a list of objects), every
	// repeated XML element, else the one value split on the separator.
	all := extractAll(a.p.Send, "msgid", body)
	if len(all) == 0 {
		if v := extractVal(a.p.Send, "msgid", body); v != "" {
			all = []string{v}
		}
	}
	ids = make([]string, len(batch))
	if len(all) == 0 {
		// No id in an accepted answer (see Send): one "" a message.
		return ids, 0, nil
	}
	raw := all[0]
	if a.p.Bulk.SingleID {
		for i := range ids {
			ids[i] = raw
		}
		return ids, 1, nil
	}
	parts := all
	if len(all) == 1 && a.p.Bulk.IDSeparator != "" {
		parts = strings.Split(raw, a.p.Bulk.IDSeparator)
	}
	var got []string
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			got = append(got, p)
		}
	}
	switch {
	case len(parts) == len(batch): // one a message, in order ("" where the list has a gap)
		for i, p := range parts {
			ids[i] = strings.TrimSpace(p)
		}
		return ids, len(got), nil
	case len(got) == 1: // one id for the whole call
		for i := range ids {
			ids[i] = got[0]
		}
		return ids, 1, nil
	default: // a list that cannot be matched: one "" a message
		return ids, len(got), nil
	}
}

// The text formats a call may ask for (Call.TextFormat).
const (
	TextPlain   = "plain"
	TextUCS2Hex = "ucs2_hex"
	TextHex     = "hex"
)

// withTextFormat writes the text the way the call asks, on a copy. It runs
// before valueMapping, so a mapping still sees the message's own encoding.
func withTextFormat(call Call, c map[string]string) map[string]string {
	text, has := c["text"]
	if !has {
		return c
	}
	switch call.TextFormat {
	case TextHex:
	case TextUCS2Hex:
		if c["encoding"] != "ucs2" {
			return c
		}
	default:
		return c
	}
	tc := make(map[string]string, len(c))
	for k, v := range c {
		tc[k] = v
	}
	tc["text"] = UTF16Hex(text)
	return tc
}

// UTF16Hex is text as the upper-case hex of its UTF-16 big-endian bytes —
// "سلام" is "0633064406270645". An emoji is its surrogate pair.
func UTF16Hex(text string) string {
	u := utf16.Encode([]rune(text))
	b := make([]byte, 0, 4*len(u))
	const digits = "0123456789ABCDEF"
	for _, r := range u {
		b = append(b, digits[r>>12&0xF], digits[r>>8&0xF], digits[r>>4&0xF], digits[r&0xF])
	}
	return string(b)
}
