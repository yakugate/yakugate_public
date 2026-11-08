package auth

import (
	"net/http"
	"net/netip"
	"strings"
)

// IPResolver resolves the real client address behind trusted reverse proxies
// (chunk-2 ruling 2.5, 2026-08-03 — closing open item B-02). The gateway
// deliberately ignored X-Forwarded-For until now, because honouring it
// blindly lets anyone spoof past the IP ladders; but that made every proxied
// deployment share the proxy's address across all three IP-keyed defences
// (volume throttle, spray guard, per-account allowlists) — one attacker's
// failures throttled every tenant. The rule here is the standard safe one:
// X-Forwarded-For is consulted ONLY when the TCP peer itself is a configured
// trusted proxy, and the client is the RIGHTMOST hop not in the trusted set
// (rightmost is the one value the trusted chain appended itself; everything
// left of it is client-supplied and forgeable). A nil resolver — no
// YAKU_TRUSTED_PROXIES — preserves the old behaviour exactly: TCP peer only.
type IPResolver struct {
	trusted []netip.Prefix
}

// NewIPResolver parses a comma-separated list of CIDRs or bare IPs
// (YAKU_TRUSTED_PROXIES). Invalid entries are returned for the caller to log;
// the resolver is nil when no valid entry remains, leaving peer-only
// behaviour in force.
func NewIPResolver(list string) (*IPResolver, []string) {
	var trusted []netip.Prefix
	var invalid []string
	for _, part := range strings.Split(list, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if p, err := netip.ParsePrefix(part); err == nil {
			trusted = append(trusted, p.Masked())
			continue
		}
		if a, err := netip.ParseAddr(part); err == nil {
			trusted = append(trusted, netip.PrefixFrom(a.Unmap(), a.Unmap().BitLen()))
			continue
		}
		invalid = append(invalid, part)
	}
	if len(trusted) == 0 {
		return nil, invalid
	}
	return &IPResolver{trusted: trusted}, invalid
}

func (res *IPResolver) isTrusted(ip string) bool {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}
	a = a.Unmap()
	for _, p := range res.trusted {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// FromRequest returns the client IP used for ladder keys, the spray guard,
// per-account allowlists, and reject logging. Nil-safe: a nil resolver
// returns the TCP peer, which is the pre-2.5 behaviour verbatim.
func (res *IPResolver) FromRequest(r *http.Request) string {
	peer := remoteIP(r)
	if res == nil || !res.isTrusted(peer) {
		return peer
	}
	var hops []string
	for _, h := range r.Header.Values("X-Forwarded-For") {
		for _, part := range strings.Split(h, ",") {
			if p := strings.TrimSpace(part); p != "" {
				hops = append(hops, p)
			}
		}
	}
	for i := len(hops) - 1; i >= 0; i-- {
		hop := normalizeHop(hops[i])
		if !res.isTrusted(hop) {
			return hop
		}
	}
	if len(hops) > 0 {
		// Every hop is a trusted proxy: the leftmost is the chain's origin.
		return normalizeHop(hops[0])
	}
	return peer // trusted peer, no X-Forwarded-For header at all
}

// normalizeHop tolerates the "ip:port" / "[v6]:port" / "[v6]" forms some
// proxies emit in X-Forwarded-For instead of a bare address. A value that
// parses as no address form at all is returned as-is — it will simply never
// match a trusted prefix or an allowlist entry.
func normalizeHop(s string) string {
	if a, err := netip.ParseAddr(s); err == nil {
		return a.Unmap().String()
	}
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return ap.Addr().Unmap().String()
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		if a, err := netip.ParseAddr(s[1 : len(s)-1]); err == nil {
			return a.Unmap().String()
		}
	}
	return s
}
