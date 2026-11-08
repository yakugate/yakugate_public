// Package webhookauth authenticates INBOUND vendor callbacks — the delivery
// receipts P5 serves on /dlr/<cid> and the mobile-originated messages P6 serves
// on /mo/<cid>.
//
// Until 2026-08-31 both endpoints accepted anyone who knew or guessed an active
// vendor's cid. A correctly-shaped POST from any caller could flip a real
// message to delivered/failed, or inject a fake inbound message — which is
// worse, because an MO is billed, lands in the client's logs as genuine, and
// can trigger their auto-reply into sending real traffic. (P5-B01.)
//
// THREE CHECKS, ALL OPTIONAL, ALL ENFORCED WHEN SET
//
//	token   — a secret in the URL: /dlr/<cid>/<token>. Universal: every vendor
//	          can store a URL, even ones that cannot add headers. Weakest, since
//	          the secret appears in proxy/access logs.
//	header  — a secret in a named header. Better, when the vendor supports it.
//	ip      — an allow-list of addresses/CIDRs the callback may come from.
//
// A vendor must satisfy EVERY check configured for it. Different vendors can
// prove themselves differently, which is the point: their capabilities differ
// and a single mandated scheme would lock real ones out.
//
// NO CONFIG MEANS REFUSED (PO ruling 2026-08-31). A vendor with nothing set is
// rejected rather than waved through, so the protection cannot be defeated by
// simply never configuring it. The refusal names the vendor and says exactly
// what was missing — an operator must never be left guessing why receipts
// stopped.
package webhookauth

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"strings"

	"github.com/yakugate/yakugate/internal/auth"
)

// Config is one vendor's inbound rules, stored as vendors.inbound_config JSON.
//
//	{"token":"…","header":{"name":"X-Yaku-Token","value":"…"},
//	 "ip_allow":["203.0.113.7","198.51.100.0/24"]}
type Config struct {
	Token  string `json:"token,omitempty"`
	Header struct {
		Name  string `json:"name,omitempty"`
		Value string `json:"value,omitempty"`
	} `json:"header,omitempty"`
	IPAllow []string `json:"ip_allow,omitempty"`
}

// Parse reads a stored inbound_config column. Unreadable JSON yields an EMPTY
// config, which Check then refuses — a config we cannot understand must never
// read as "no checks required".
func Parse(raw string) Config {
	var c Config
	if strings.TrimSpace(raw) == "" {
		return c
	}
	_ = json.Unmarshal([]byte(raw), &c)
	return c
}

// Configured reports whether this vendor has any inbound protection at all.
func (c Config) Configured() bool {
	return c.Token != "" || (c.Header.Name != "" && c.Header.Value != "") || len(c.IPAllow) > 0
}

// Check authenticates one request. urlToken is the path segment after the
// vendor cid ("" when the caller sent none). res may be nil (peer address
// only). The returned reason is for the SERVER LOG, never the response body:
// telling a caller which check failed helps them find the one that passes.
func (c Config) Check(r *http.Request, urlToken string, res *auth.IPResolver) (bool, string) {
	if !c.Configured() {
		return false, "vendor has no inbound security configured — set a URL token, a header secret or an IP allow-list on the vendor"
	}
	if c.Token != "" {
		// Constant-time: a byte-by-byte comparison leaks the secret's prefix to
		// anyone willing to time the responses.
		if subtle.ConstantTimeCompare([]byte(urlToken), []byte(c.Token)) != 1 {
			return false, "URL token missing or wrong"
		}
	}
	if c.Header.Name != "" && c.Header.Value != "" {
		got := r.Header.Get(c.Header.Name)
		if subtle.ConstantTimeCompare([]byte(got), []byte(c.Header.Value)) != 1 {
			return false, fmt.Sprintf("header %s missing or wrong", c.Header.Name)
		}
	}
	if len(c.IPAllow) > 0 {
		ip := clientIP(r, res)
		if !ipAllowed(ip, c.IPAllow) {
			return false, fmt.Sprintf("source address %s is not in the allow-list", ip)
		}
	}
	return true, ""
}

// clientIP reuses P1's resolver so X-Forwarded-For is honoured under exactly
// the same rule everywhere (D-091): only from listed trusted proxies, never on
// a bare header a caller can set for itself.
func clientIP(r *http.Request, res *auth.IPResolver) string {
	if res != nil {
		return res.FromRequest(r)
	}
	host := r.RemoteAddr
	if h, _, err := splitHostPort(host); err == nil {
		return h
	}
	return host
}

func splitHostPort(s string) (string, string, error) {
	if i := strings.LastIndex(s, ":"); i > 0 && !strings.HasSuffix(s, "]") {
		return strings.Trim(s[:i], "[]"), s[i+1:], nil
	}
	return s, "", fmt.Errorf("no port")
}

// IPAllowed reports whether an address is in an allow-list of addresses and
// CIDRs. Exported for internal/inboundauth, which applies the same list on
// the per-endpoint rows that replaced this package's flat config — one
// implementation, so the two cannot disagree about what a CIDR means.
func IPAllowed(ip string, list []string) bool { return ipAllowed(ip, list) }

func ipAllowed(ip string, list []string) bool {
	addr, err := netip.ParseAddr(strings.Trim(ip, "[]"))
	if err != nil {
		return false
	}
	for _, entry := range list {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if p, err := netip.ParsePrefix(entry); err == nil {
			if p.Masked().Contains(addr) {
				return true
			}
			continue
		}
		if a, err := netip.ParseAddr(entry); err == nil && a == addr {
			return true
		}
	}
	return false
}

// UnconfiguredWarner logs a vendor with no inbound protection ONCE, not on
// every config refresh. The message matters — it is the only way an operator
// learns why a vendor's callbacks are being refused — but repeating it every 30
// seconds turns it into scenery, and a log nobody reads protects nobody. A
// vendor that is fixed and later broken again warns again.
type UnconfiguredWarner struct{ warned map[string]bool }

// Warn reports the cids that are newly unconfigured this round. `unconfigured`
// is the current set; anything absent from it is forgotten, so re-breaking a
// vendor speaks up again.
func (w *UnconfiguredWarner) Warn(unconfigured []string) []string {
	if w.warned == nil {
		w.warned = map[string]bool{}
	}
	now := make(map[string]bool, len(unconfigured))
	var fresh []string
	for _, cid := range unconfigured {
		now[cid] = true
		if !w.warned[cid] {
			fresh = append(fresh, cid)
		}
	}
	w.warned = now
	return fresh
}

// SplitPath pulls the vendor cid and an optional URL token out of a webhook
// path. Both /dlr/<cid> and /dlr/<cid>/<token> are accepted so a vendor that
// cannot carry a token still reaches its own vendor row — and is then refused
// by Check, with a reason, rather than 404ing as an unknown vendor.
func SplitPath(rest string) (cid, token string) {
	rest = strings.Trim(rest, "/")
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[:i], rest[i+1:]
	}
	return rest, ""
}
