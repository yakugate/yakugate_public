// Country rules: per-account allow or deny, quotas and rate limits by
// destination country.

package refdata

import (
	"strings"
)

// CountryRule is a per-account, per-country rule: allow/deny plus optional
// quotas and a per-country segments/sec rate limit (0 = unlimited).
type CountryRule struct {
	Mode      string // "allow" | "deny"
	Quota     Quota
	RateLimit int // segments/sec cap for the whole country (PO 2026-07-26)
	// Priority orders evaluation within one scope (account's own rules, then
	// global): LOWER is evaluated first; among equal priorities the longest
	// matching prefix wins (P1 scope finalize 2026-08-09). The default rule
	// (map key "" — a NULL destination_country_code) matches every
	// destination, so it acts as the account's catch-all disposition.
	Priority int
}

// countryRuleFor resolves the governing rule with ACCOUNT OWNERSHIP ABSOLUTE
// (D-094 addendum, PO 2026-08-04: "only account specific country rules will
// work"): if ANY of the account's own rules matches the destination, the
// account's longest match governs and the global set is not consulted at all
// — a more specific global rule can no longer override an account's own
// intent in either direction. Global rules (account_id NULL -> 0) apply only
// to destinations the account's own rule set says nothing about.
func (m *Mem) countryRuleFor(accountID int64, msisdn string) (CountryRule, bool) {
	_, r, _, ok := m.countryRuleLookup(accountID, msisdn)
	return r, ok
}

// countryRuleLookup is the ONE country-rule resolver (chunk-6 ruling 6.3 —
// CountryRateLimit used to carry its own duplicated pick logic that missed
// the D-094 ownership-absolute addendum): account rules govern outright when
// any matches; global rules are the fallback. Returns the matched prefix and
// whether the governing rule is the account's OWN (owned=true) — the
// rate-limit/quota stages scope their buckets by that (chunk-6 ruling 6.4).
func (m *Mem) countryRuleLookup(accountID int64, msisdn string) (cc string, r CountryRule, owned, ok bool) {
	if cc, r, ok := longestPrefixRule(m.countryRules[accountID], msisdn); ok {
		return cc, r, true, true
	}
	cc, r, ok = longestPrefixRule(m.countryRules[0], msisdn)
	return cc, r, false, ok
}

// longestPrefixRule picks the longest matching prefix within ONE rule set.
// best starts at -1 so a zero-length (catch-all) prefix still counts.
// longestPrefixRule picks the governing rule within ONE scope by the P1 scope
// finalize (2026-08-09) ordering: lowest Priority first, then longest matching
// prefix as the tiebreak. An empty-prefix rule (map key "", a NULL
// destination_country_code = the scope's default/catch-all) matches every
// destination — so an account with a default rule always has a match, which is
// what makes the account's disposition absolute over the global set.
func longestPrefixRule(rules map[string]CountryRule, msisdn string) (string, CountryRule, bool) {
	var bestCC string
	var best CountryRule
	found := false
	for pfx, r := range rules {
		if !strings.HasPrefix(msisdn, pfx) {
			continue
		}
		if !found ||
			r.Priority < best.Priority ||
			(r.Priority == best.Priority && len(pfx) > len(bestCC)) {
			bestCC, best, found = pfx, r, true
		}
	}
	return bestCC, best, found
}

// CountryRateLimit resolves the destination's per-country segments/sec cap
// via the ONE country-rule resolver (ownership-absolute, D-094 addendum) and
// the matched prefix. owned reports whether the governing rule is the
// account's OWN — the stage scopes the bucket by it (chunk-6 ruling 6.4: an
// account-specific cap must not share its bucket with other accounts'
// traffic). ok=false / limit 0 = no cap configured. [PO 2026-07-26]
func (m *Mem) CountryRateLimit(accountID int64, msisdn string) (cc string, limit int, owned, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cc, r, owned, ok := m.countryRuleLookup(accountID, msisdn)
	return cc, r.RateLimit, owned, ok
}

// CountryQuotaRule resolves the governing country rule's matched prefix, its
// Quota (the DB-stored BASE counters), and ownership — the quota stage's
// lookup (chunk-6 rulings 6.1/6.4). ok=false = no rule matches.
func (m *Mem) CountryQuotaRule(accountID int64, msisdn string) (cc string, q Quota, owned, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cc, r, owned, ok := m.countryRuleLookup(accountID, msisdn)
	return cc, r.Quota, owned, ok
}

// CountryDenied reports whether a destination matches a deny rule.
func (m *Mem) CountryDenied(accountID int64, msisdn string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.countryRuleFor(accountID, msisdn)
	return ok && r.Mode == "deny"
}

// CountryQuotaExceeded reports whether the destination's country quota is exhausted.
func (m *Mem) CountryQuotaExceeded(accountID int64, msisdn string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.countryRuleFor(accountID, msisdn)
	if !ok {
		return false
	}
	return r.Quota.Exceeded()
}
