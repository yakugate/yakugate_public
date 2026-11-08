package inboundauth

import (
	"crypto/subtle"
	"net/http"
	"sort"
	"strings"

	"github.com/yakugate/yakugate/internal/auth"
	"github.com/yakugate/yakugate/internal/seclog"
)

// Identify picks, of the doors that read with a dialect whose own URL a
// callback came to, the one the callback's credentials name (27 Oct, Saleem:
// "dialects for mo and dlr will also have custom urls, similar to MT one").
// As on a client dialect's path, the path picks the dialect and the
// credentials pick who is calling:
//
//	a token      the door whose token it is (by its digest, in constant time)
//	a username   the door with that username; its password is the door's check
//	a JWT        the door its subject names, <cid>:<kind> or <cid> — read
//	             without its signature here; the door's check verifies it
//
// Each door's own places are used (they are its dialect's, so the same for
// all of them) and its own token source, so a token at the end of the path
// is read as on the door's own path.
//
// One door named: it is returned with an OK result, and the caller judges the
// callback at it with Check — its switch, IP list, credentials, throughput
// and ladder — exactly as on its own path. None named: a bare 403 that climbs
// the address's throttle and the spray guard ONCE, however many doors read
// with the dialect (no door's ladder: the caller named none). More than one
// (two doors there share a token or a username; the load says so in the log):
// a 403 and nothing climbed — the fault is the setup's, not the caller's.
//
// The pre-auth throttle and the spray guard are asked first, as in Check, so
// a flood costs no credential work.
func (g *Guard) Identify(r *http.Request, body []byte, doors []Endpoint, urlToken string) (Endpoint, Result) {
	ip := g.clientIP(r)
	where := "in:url"
	if len(doors) > 0 {
		where = "in:url:" + doors[0].Kind + ":" + PathKey(doors[0].DialectURL)
	}
	if g.ipLock != nil {
		if locked, retry := g.ipLock.Locked("ip:" + ip); locked {
			g.event(seclog.EventIPThrottled, Endpoint{}, ip, where, "address is throttled for repeated inbound failures", retry, 0)
			return Endpoint{}, Result{Status: http.StatusTooManyRequests, Reason: "address throttled",
				RetryAfter: secs(retry), Identity: where}
		}
	}
	if g.spray != nil {
		if locked, retry := g.spray.Locked(ip); locked {
			g.event(seclog.EventSprayBlocked, Endpoint{}, ip, where, "address is blocked for probing many vendors", retry, 0)
			return Endpoint{}, Result{Status: http.StatusTooManyRequests, Reason: "address blocked",
				RetryAfter: secs(retry), Identity: where}
		}
	}
	req := newRequestValues(r, body)
	digests := map[string]string{}
	var named []Endpoint
	for _, e := range doors {
		if g.names(req, e, urlToken, digests) {
			named = append(named, e)
		}
	}
	switch len(named) {
	case 1:
		return named[0], ok()
	case 0:
		reason := "the credentials name none of the doors that read with this dialect"
		if g.ipLock != nil {
			g.ipLock.Fail("ip:" + ip)
		}
		if g.spray != nil {
			g.spray.Observe(ip, where)
		}
		g.event(seclog.EventAuthFail, Endpoint{}, ip, where, reason, 0, 0)
		return Endpoint{}, Result{Status: http.StatusForbidden, Reason: reason, Identity: where}
	}
	cids := make([]string, 0, len(named))
	for _, e := range named {
		cids = append(cids, e.VendorCID)
	}
	sort.Strings(cids)
	return Endpoint{}, Result{Status: http.StatusForbidden, Identity: where,
		Reason: "the credentials name more than one door (vendors " + strings.Join(cids, ", ") +
			") — give each door its own token and username"}
}

// names reports whether a callback's credentials name this door. Nothing is
// verified here but a token's digest; Check verifies the rest.
func (g *Guard) names(req *requestValues, e Endpoint, urlToken string, digests map[string]string) bool {
	if e.TokenHash != "" {
		if raw := presentedToken(req, e, urlToken); raw != "" {
			d, seen := digests[raw]
			if !seen {
				d = auth.HashToken(g.pepper, raw)
				digests[raw] = d
			}
			if subtle.ConstantTimeCompare([]byte(d), []byte(e.TokenHash)) == 1 {
				return true
			}
		}
	}
	if e.Username != "" {
		if user, _, ok := userAndPassword(req, e); ok && user != "" && user == e.Username {
			return true
		}
	}
	if e.JWT.Secret != "" || e.JWT.SecretRef != "" || e.JWT.PublicKey != "" {
		if tok := presentedJWT(req, e); tok != "" {
			_, _, _, _, claims, _, reason := auth.SplitToken(tok)
			if reason == "" && claims.Sub != "" && (strings.EqualFold(claims.Sub, e.VendorCID) ||
				strings.EqualFold(claims.Sub, e.VendorCID+":"+e.Kind)) {
				return true
			}
		}
	}
	return false
}
