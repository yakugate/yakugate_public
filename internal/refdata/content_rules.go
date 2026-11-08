// Content rules: auto-replace, spam keyword lists and the blocklist, and the
// cache's lookups of them.

package refdata

import (
	"log"
	"regexp"
	"time"
)

// Block is one blocklist / DND entry. Scope: AccountID > 0 = that account;
// else CompanyID > 0 = every account of that company (GW-18, 2026-08-27);
// else global. Sender "" = all senders; a non-empty Sender matches EXACTLY
// (PO ruling 2026-08-27 — the column was renamed from sender_mask because it
// never was a mask).
type Block struct {
	Msisdn    string
	AccountID int64
	CompanyID int64
	Sender    string
	// ExpiresAt auto-unblocks the entry once past (P1 scope finalize
	// 2026-08-09); the zero value = permanent (NULL in the DB).
	ExpiresAt time.Time
	// Source is why the row exists: manual / unsub / abuse are ordinary
	// blocks, "honeypot" is a TRAP number (config/0097). Both refuse the same
	// way — the difference is that a trap hit also raises an operator alarm,
	// because nobody is on the other end of a trap and one hit says the
	// sender's whole recipient list was scraped or bought.
	Source string
}

// AutoReplaceRule is one ordered find->replace (auto_replace_text.replace_regex).
type AutoReplaceRule struct {
	Match   *regexp.Regexp // nil for a literal find
	Literal string         // set when Match is nil
	Replace string
	Seq     int
}

// SpamLists is the full set of keyword lists LoadSpam returns: the
// gateway-wide universal blocked list, plus per-company blocked/allowed
// overrides keyed by company_id (migration config/0051: spam_keywords scopes
// to company_id, not account_id, so one company-level exception covers every
// account under it). CompanyAllowed takes priority over BOTH Universal and
// that same company's own CompanyBlocked — a company can explicitly
// un-block a globally-blocked word for itself. [SCOPE P1-10 company scope]
type SpamLists struct {
	Universal      []SpamRule
	CompanyBlocked map[int64][]SpamRule
	CompanyAllowed map[int64][]SpamRule
}

// SpamRule is one spam_keywords row under the GW-17 remodel (PO 2026-08-27:
// "keywords array is now keyword_regex — regex will do that"): the pattern IS
// the list. Country ("" = everywhere) scopes the rule to destinations with
// that dialing prefix; Category groups rules for operators (identity only —
// every applicable rule is enforced regardless of category).
type SpamRule struct {
	Category string
	// Action is what a match DOES: "reject" (default), "flag" (send anyway,
	// mark the record) or "require_template" (allow only in approved
	// wording). config/0098.
	Action  string
	Country string // destination dialing-prefix scope; "" = all destinations
	Pattern string // regex source (compiled case-insensitive at cache load)
}

// SpamMatcher is a SpamRule compiled once per refresh (the chunk-5 5.3
// hot-path rule: per-message work must not include compilation).
// Spam rule actions (config/0098).
const (
	SpamActionReject   = "reject"           // refuse — the only behaviour before 2026-09-09
	SpamActionFlag     = "flag"             // send anyway, mark the record (observe mode)
	SpamActionTemplate = "require_template" // allow only if the body matches an approved template
)

type SpamMatcher struct {
	Category string
	Action   string
	Country  string
	Re       *regexp.Regexp
}

// CompileSpamRules compiles a list of rules the one way they are read: case
// does not matter, a rule that does not compile is dropped and said so, and
// an empty action is "reject". The cache uses it at every reload; the abuse
// monitor uses it too, so the two never read a rule differently (QA
// 2026-10-08, B-01: the monitor had its own reader, which still expected the
// keyword list of before GW-17 and so saw no rule saved since).
func CompileSpamRules(list []SpamRule) []SpamMatcher {
	out := make([]SpamMatcher, 0, len(list))
	for _, r := range list {
		if r.Pattern == "" {
			continue
		}
		re, err := regexp.Compile("(?i)" + r.Pattern)
		if err != nil {
			// Say-something rule (5.6): a silently dropped spam rule is a
			// silently open gate.
			log.Printf("[refdata] spam rule %q (country %q) does not compile — DROPPED: %v", r.Pattern, r.Country, err)
			continue
		}
		// An empty action is "reject" — the column defaults to it, and a row
		// arriving from a backend that has not got the column yet must behave
		// exactly as it did before this existed.
		act := r.Action
		if act == "" {
			act = SpamActionReject
		}
		out = append(out, SpamMatcher{Category: r.Category, Action: act, Country: r.Country, Re: re})
	}
	return out
}

func (m *Mem) AutoReplace(accountID int64) []AutoReplaceRule {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.autoReplace[accountID]
}

// SpamUniversalRules returns the gateway-wide blocked rules, compiled at
// cache load (GW-17 regex remodel).
func (m *Mem) SpamUniversalRules() []SpamMatcher {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.spamUniPrep
}

// SpamAllowedRules returns companyID's own allow rules — a matching allow rule
// exempts the MESSAGE from the universal (and that company's own) blocked
// rules. nil for company_id=0: unlinked accounts never get company-level
// scoping, the same rule EffectiveBalance()/Prepaid() follow elsewhere.
func (m *Mem) SpamAllowedRules(companyID int64) []SpamMatcher {
	if companyID == 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.spamCoAllow[companyID]
}

// SpamCompanyBlockedRules returns companyID's own additional blocked rules,
// on top of the universal set.
func (m *Mem) SpamCompanyBlockedRules(companyID int64) []SpamMatcher {
	if companyID == 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.spamCoPrep[companyID]
}

// Blocked reports whether a destination (optionally for a sender) is blocked.
func (m *Mem) Blocked(msisdn string, accountID, companyID int64, sender string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now()
	// live reports whether a key is present AND not past its expiry (zero =
	// permanent). The two-value map form is required — a permanent block stores
	// the zero time, indistinguishable from "absent" by value (P1 scope 2026-08-09).
	// Tiers: global, then company (GW-18), then account — any hit blocks.
	live := func(key string) bool {
		if exp, ok := m.blockGlobal[key]; ok && (exp.IsZero() || exp.After(now)) {
			return true
		}
		if companyID != 0 {
			if exp, ok := m.blockCo[key+"|c:"+itoa(companyID)]; ok && (exp.IsZero() || exp.After(now)) {
				return true
			}
		}
		if exp, ok := m.blockAcct[key+"|"+itoa(accountID)]; ok && (exp.IsZero() || exp.After(now)) {
			return true
		}
		return false
	}
	if live(msisdn) {
		return true
	}
	if sender != "" && live(msisdn+"|s:"+sender) {
		return true
	}
	return false
}

// IsTrap reports whether a destination is a TRAP number — a blocklist row with
// source='honeypot' (config/0097).
//
// Meant to be asked only after Blocked has already returned true, to decide
// whether this refusal deserves an operator alarm. A trap has no subscriber
// behind it, so a send to one is evidence that the sender's recipient list was
// scraped, bought or stolen — and that the operator or regulator watching that
// number saw it happen.
//
// The CLIENT is never told the difference (PO 2026-09-09): a distinct refusal
// would let anyone with an account probe the gateway to find our traps, strip
// them from their list, and keep sending to the rest of it.
func (m *Mem) IsTrap(dest string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	exp, ok := m.blockTrap[dest]
	return ok && (exp.IsZero() || exp.After(time.Now()))
}
