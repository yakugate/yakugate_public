// An account's IP allow-list: how a caller's address is matched against it,
// and the check of a list before it is saved.

package refdata

import (
	"net"
	"strconv"
	"strings"
)

// IPAllowed reports whether ip matches the account's allowlist. Only
// meaningful when IPCheck is set — callers gate on that themselves. Shared by
// the pipeline's ip-allow stage and by internal/auth's lockout skip-table (a
// failed attempt from a non-allowlisted IP must not count toward the identity
// lock — PO 2026-07-26).
//
// Supported entry forms (PO 2026-07-26):
//
//	10.20.30.40                exact IPv4
//	2001:db8::1                exact IPv6 (parsed comparison, not string —
//	                           "2001:0db8::1" and "2001:db8::1" both match)
//	10.20.30.0/24              CIDR, either family (2001:db8::/32)
//	203.0.113.10-203.0.113.20  inclusive range, either family
//	192.168.1.*  /  10.0.*.*   per-octet wildcard (IPv4); *.*.*.* = match-all
//	!<any of the above>        negation — evaluated FIRST: a deny beats every
//	                           allow, or '!' would be meaningless
//
// Hostnames, ASNs, geo tags, regexes and ports are deliberately NOT supported
// (DNS on the auth path, external databases, unauditable patterns). A
// malformed entry matches nothing — use ValidateAllowedIPs at write time so a
// typo is caught instead of silently locking the client out.
func (a Account) IPAllowed(ip string) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	// Pass 1: negations. A deny wins over any allow.
	for _, entry := range a.AllowedIPs {
		if strings.HasPrefix(entry, "!") && ipEntryMatches(strings.TrimPrefix(entry, "!"), parsed, ip) {
			return false
		}
	}
	// Pass 2: allows. `0.0.0.0` is the explicit "allow all" wildcard (P1 scope
	// finalize 2026-08-09) — an off-switch for IP restriction that reads more
	// obviously than the equivalent `*.*.*.*`; still subject to the negations
	// above, so `0.0.0.0` + `!<bad>` = allow all except the denied set.
	for _, entry := range a.AllowedIPs {
		if e := strings.TrimSpace(entry); e == "0.0.0.0" {
			return true
		}
		if !strings.HasPrefix(entry, "!") && ipEntryMatches(entry, parsed, ip) {
			return true
		}
	}
	return false
}

// ipEntryMatches evaluates ONE (un-negated) allowlist entry against an IP.
func ipEntryMatches(entry string, parsed net.IP, raw string) bool {
	entry = strings.TrimSpace(entry)
	switch {
	case entry == "":
		return false
	case strings.Contains(entry, "/"): // CIDR, either family
		_, cidr, err := net.ParseCIDR(entry)
		return err == nil && cidr.Contains(parsed)
	case strings.Contains(entry, "-"): // inclusive range "lo-hi"
		lo, hi, ok := parseIPRange(entry)
		return ok && ipWithin(parsed, lo, hi)
	case strings.Contains(entry, "*"): // per-octet wildcard (IPv4)
		return octetWildcardMatch(entry, raw)
	default: // exact, either family (parsed comparison, not string)
		if e := net.ParseIP(entry); e != nil {
			return e.Equal(parsed)
		}
		return false
	}
}

func parseIPRange(entry string) (lo, hi net.IP, ok bool) {
	parts := strings.SplitN(entry, "-", 2)
	if len(parts) != 2 {
		return nil, nil, false
	}
	lo = net.ParseIP(strings.TrimSpace(parts[0]))
	hi = net.ParseIP(strings.TrimSpace(parts[1]))
	return lo, hi, lo != nil && hi != nil
}

// ipWithin reports lo <= ip <= hi on 16-byte canonical forms.
func ipWithin(ip, lo, hi net.IP) bool {
	v, l, h := ip.To16(), lo.To16(), hi.To16()
	if v == nil || l == nil || h == nil {
		return false
	}
	return bytesCompare(v, l) >= 0 && bytesCompare(v, h) <= 0
}

func bytesCompare(a, b net.IP) int {
	for i := range a {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	return 0
}

// octetWildcardMatch matches IPv4 per-octet wildcards: "192.168.1.*",
// "10.0.*.*", and the long-standing match-all "*.*.*.*" (which is just the
// fully-wildcarded case of the same grammar).
func octetWildcardMatch(pattern, ip string) bool {
	p, v := strings.Split(pattern, "."), strings.Split(ip, ".")
	if len(p) != 4 || len(v) != 4 {
		return false
	}
	for i := range p {
		if p[i] != "*" && p[i] != v[i] {
			return false
		}
	}
	return true
}

// ValidateAllowedIPs returns the entries that match NO valid form — the
// write-time guard (Admin API / P11) the allowlist grammar requires: a typo'd
// entry silently matches nothing at runtime, which presents as "client locked
// out" with no diagnostic. Empty result = every entry is well-formed.
func ValidateAllowedIPs(entries []string) (invalid []string) {
	for _, e := range entries {
		probe := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(e), "!"))
		ok := false
		switch {
		case probe == "":
		case strings.Contains(probe, "/"):
			_, _, err := net.ParseCIDR(probe)
			ok = err == nil
		case strings.Contains(probe, "-"):
			_, _, ok = parseIPRange(probe)
		case strings.Contains(probe, "*"):
			parts := strings.Split(probe, ".")
			ok = len(parts) == 4
			for _, p := range parts {
				if p == "*" {
					continue
				}
				n, err := strconv.Atoi(p)
				if err != nil || n < 0 || n > 255 {
					ok = false
				}
			}
		default:
			ok = net.ParseIP(probe) != nil
		}
		if !ok {
			invalid = append(invalid, e)
		}
	}
	return invalid
}
