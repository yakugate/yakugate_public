package httpserver

// Admin endpoints have their OWN credential, separate from the read-only
// status key (PO 2026-09-08: "separate admin credential").
//
// Why separate: /status and /status/cluster only READ. The /admin/* endpoints
// change things — they reload live config for every account, re-seed a
// customer's balance, lift a lockout, and decrypt a customer's message body.
// Handing the same secret to a monitoring scraper and to whoever may decrypt
// message content is one boundary doing two jobs.
//
// Four of these endpoints were not gated AT ALL until now, on the same port
// customers submit to: /admin/cache/refresh (force a full cache reload plus a
// MySQL->Redis sync, as often as the caller likes), /admin/balance/refresh
// (re-seed a live balance from an account id in the query string), and the two
// read paths. That is what this closes.

import (
	"crypto/subtle"
	"net/http"
)

// adminAuth holds the accepted admin credentials, read once at start-up.
type adminAuth struct {
	token string // static token, header "yaku-admin-key"
	user  string // optional HTTP Basic pair
	pass  string
}

// configured reports whether ANY admin credential exists. When none does, the
// admin endpoints are disabled rather than open — the same default-closed
// posture the status page already takes.
func (a adminAuth) configured() bool { return a.token != "" || (a.user != "" && a.pass != "") }

// ok checks one request's credentials.
//
// Deliberately NOT accepted in the query string, unlike the status key. Every
// admin call is now written to audit_log with its route, and query strings end
// up in logs, proxies and browser history — a secret there would leak into the
// very trail built to watch these endpoints.
//
// Constant-time comparison: these are static secrets compared on every call,
// so a byte-by-byte early exit is a timing oracle worth closing cheaply.
func (a adminAuth) ok(r *http.Request) bool {
	if a.token != "" {
		if k := r.Header.Get("yaku-admin-key"); k != "" &&
			subtle.ConstantTimeCompare([]byte(k), []byte(a.token)) == 1 {
			return true
		}
	}
	if a.user != "" && a.pass != "" {
		u, p, ok := r.BasicAuth()
		if ok &&
			subtle.ConstantTimeCompare([]byte(u), []byte(a.user)) == 1 &&
			subtle.ConstantTimeCompare([]byte(p), []byte(a.pass)) == 1 {
			return true
		}
	}
	return false
}

// adminGate wraps an admin handler. Order matters at the call site: the audit
// wrapper goes OUTSIDE this, so a refused attempt is still recorded — someone
// probing the admin surface with a wrong key is exactly the row an auditor
// wants.
func adminGate(a adminAuth, next func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.configured() {
			// Default-closed: an unconfigured deployment exposes nothing
			// rather than everything.
			http.NotFound(w, r)
			return
		}
		if !a.ok(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="yakugate-admin"`)
			writeJSON(w, 401, map[string]string{"error": "AUTH_FAILED"})
			return
		}
		next(w, r)
	}
}
