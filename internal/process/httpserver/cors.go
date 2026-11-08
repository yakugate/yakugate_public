package httpserver

// CORS support (P1 scope finalize 2026-08-09): browser clients are expected, so
// P1 answers cross-origin preflight (OPTIONS) and emits Access-Control-* headers
// on allowed origins. Off by default (no YAKU_CORS_ORIGINS) — a bare
// server-to-server deployment stays exactly as it was, with no CORS headers and
// OPTIONS falling through to the normal 405. Origins are an explicit allow-list
// (or the single value "*" to allow any); nothing is reflected blindly.

import (
	"net/http"
	"strings"
)

// corsPolicy holds the parsed allow-list. allowAny short-circuits to "*".
type corsPolicy struct {
	allowAny bool
	origins  map[string]bool
}

// newCORSPolicy parses a comma-separated origin list (YAKU_CORS_ORIGINS). An
// empty list yields a nil policy — CORS entirely disabled.
func newCORSPolicy(list string) *corsPolicy {
	list = strings.TrimSpace(list)
	if list == "" {
		return nil
	}
	p := &corsPolicy{origins: map[string]bool{}}
	for _, o := range strings.Split(list, ",") {
		o = strings.TrimSpace(o)
		if o == "*" {
			p.allowAny = true
		} else if o != "" {
			p.origins[o] = true
		}
	}
	return p
}

func (p *corsPolicy) allowed(origin string) bool {
	return p != nil && (p.allowAny || p.origins[origin])
}

// wrap adds CORS handling around next. A nil policy returns next unchanged.
func (p *corsPolicy) wrap(next http.Handler) http.Handler {
	if p == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && p.allowed(origin) {
			h := w.Header()
			// Echo the specific origin (not "*") so credentialed requests work
			// and the response is unambiguous. Vary:Origin keeps caches correct.
			h.Set("Access-Control-Allow-Origin", origin)
			h.Add("Vary", "Origin")
			h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization, yaku-sms-key")
			h.Set("Access-Control-Max-Age", "600")
		}
		// Preflight: answer and stop (never runs auth/pipeline for an OPTIONS).
		if r.Method == http.MethodOptions {
			status := http.StatusNoContent
			if origin != "" && !p.allowed(origin) {
				status = http.StatusForbidden // disallowed origin's preflight is refused
			}
			w.WriteHeader(status)
			return
		}
		next.ServeHTTP(w, r)
	})
}
