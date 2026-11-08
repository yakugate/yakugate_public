package httpserver

// Operator actions on the engine's own admin endpoints are recorded to
// audit_log (PO 2026-09-05: "record all four" — every admin endpoint, not
// only the ones that change something).
//
// Before this, the whole trail was one printf line in handleUnblock. The
// endpoint that most needed a trail had none at all: /admin/decrypt returns a
// customer's decrypted message body to whoever holds the status key, and left
// no record that it had been read.
//
// A wrapper rather than a line inside each handler: it cannot be forgotten
// when the next admin endpoint is added, and it records the outcome the
// operator actually saw, since it observes the response status.
//
// It wraps OUTSIDE the status-key gate on purpose. Wrapped inside, a call with
// a missing or wrong key is refused by the gate and never reaches the
// recorder — and someone probing the admin surface with the wrong key is
// exactly the row an auditor most wants to find. Those land as outcome
// "failure" with response_code 401.

import (
	"net"
	"net/http"

	"github.com/yakugate/yakugate/internal/audit"
)

// auditedWriter watches the response so the recorded outcome is the one the
// caller got, not the one the handler intended.
type auditedWriter struct {
	http.ResponseWriter
	status int
}

func (w *auditedWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *auditedWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK // an implicit 200, same as net/http
	}
	return w.ResponseWriter.Write(b)
}

// audited wraps an admin handler so the call is recorded. action is the stable
// dotted verb an auditor filters on; entity names what it acts on.
//
// The row is written after the handler returns, so it carries the real status
// code, and the write itself is fire-and-forget inside audit.Record — an
// unreachable config store must never make an admin endpoint hang or fail.
func (p *Process) audited(action, entity string, next func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		aw := &auditedWriter{ResponseWriter: w}
		next(aw, r)
		if p.aud == nil {
			return
		}
		status := aw.status
		if status == 0 {
			status = http.StatusOK
		}
		outcome := "success"
		if status >= 400 {
			outcome = "failure"
		}
		p.aud.Record(audit.Entry{
			Actor:        "engine-admin", // the status key, not a person — the IP narrows it
			Action:       action,
			Entity:       entity,
			Method:       r.Method,
			Route:        r.URL.Path,
			ResponseCode: status,
			IP:           p.clientIP(r),
			Outcome:      outcome,
			Summary:      adminSummary(action, status),
		})
	}
}

// adminSummary is the one-sentence column, written here rather than at each
// call site so the wording stays consistent across endpoints.
func adminSummary(action string, status int) string {
	if status >= 400 {
		return "admin action refused: " + action
	}
	return "admin action: " + action
}

// clientIP resolves the caller's address the same way every other security
// decision in P1 does — honouring X-Forwarded-For only from trusted proxies,
// so an audit row cannot be given a forged address by the caller.
//
// The resolver is nil when YAKU_TRUSTED_PROXIES is unset (the common case), so
// the fallback has to strip the port itself: RemoteAddr is "host:port", and an
// audit trail wants the address, not the ephemeral port that changes on every
// request and makes rows from one operator look like rows from many.
func (p *Process) clientIP(r *http.Request) string {
	if p.ipres != nil {
		return p.ipres.FromRequest(r)
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}
