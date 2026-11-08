package pipeline

import (
	"context"
	"strings"

	"github.com/yakugate/yakugate/internal/abuse"
	"github.com/yakugate/yakugate/internal/refdata"
)

// --- stage 11/18: spam (universal + per-company rule check) --- [SCOPE P1-10]
//
// GW-17 regex remodel (PO 2026-08-27: "keywords array is now keyword_regex —
// regex will do that"): a rule is a compiled case-insensitive regex, optionally
// scoped to a destination dialing prefix (destination_country_code), grouped
// by category (identity only). A company allow rule that matches the message
// exempts it from the universal and that company's own blocked rules.
type spamStage struct{ ref *refdata.Mem }

func (spamStage) Name() string { return "spam" }
func (s spamStage) Run(_ context.Context, e *Envelope) *Reject {
	if !e.Account.SpamCheck {
		return nil
	}
	body := e.Msg.Body
	// Strict accounts (D-035, opt-in) also run every rule against the
	// normalized/leet-folded body — "l.o.a.n" / "l0an" fold back to "loan".
	// Variant matching inherently false-positives; enforce_spam_strict is the
	// operator accepting that trade.
	var norm string
	if e.Account.SpamStrict {
		norm = abuse.Normalize(e.Msg.Body)
	}
	dest := e.Msg.Destination
	inScope := func(r refdata.SpamMatcher) bool {
		return r.Country == "" || strings.HasPrefix(dest, r.Country)
	}
	hits := func(r refdata.SpamMatcher) bool {
		if !inScope(r) {
			return false
		}
		if r.Re.MatchString(body) {
			return true
		}
		return e.Account.SpamStrict && norm != "" && r.Re.MatchString(norm)
	}
	// Allow rules override BOTH the universal list and the company's own
	// blocked list — evaluated lazily, once, only when something blocks.
	allowedChecked, allowedHit := false, false
	allowed := func() bool {
		if !allowedChecked {
			allowedChecked = true
			for _, r := range s.ref.SpamAllowedRules(e.Account.CompanyID) {
				if hits(r) {
					allowedHit = true
					break
				}
			}
		}
		return allowedHit
	}
	for _, r := range s.ref.SpamUniversalRules() {
		if hits(r) && !allowed() {
			if rej := s.act(e, r, "universal"); rej != nil {
				return rej
			}
		}
	}
	for _, r := range s.ref.SpamCompanyBlockedRules(e.Account.CompanyID) {
		if hits(r) && !allowed() {
			if rej := s.act(e, r, "company"); rej != nil {
				return rej
			}
		}
	}
	return nil
}

// act applies one matched rule's configured action (config/0098). Returning
// nil means "this rule is satisfied, keep scanning" — deliberately NOT "the
// message is fine". A flag rule must never stop the scan, or a later reject
// rule would be skipped and a flagged message would sail past a rule that
// should have refused it.
func (s spamStage) act(e *Envelope, r refdata.SpamMatcher, scope string) *Reject {
	switch r.Action {
	case refdata.SpamActionFlag:
		// Observe mode: SEND it, and write down that this rule matched. The
		// customer is told nothing — the whole point is to learn what a rule
		// would do without anyone's traffic being refused while you find out.
		e.SpamFlags = append(e.SpamFlags, spamLabel(r, scope))
		return nil
	case refdata.SpamActionTemplate:
		// This vocabulary is allowed, but only in wording the operator
		// approved. Matched a template, carry on; matched none, refuse — and
		// refuse as TEMPLATE_MISMATCH, not SPAM_KEYWORD, because that is
		// honestly what went wrong and it is the same 422 the account's own
		// template gate gives.
		if e.TemplateMatched {
			return nil
		}
		rej := reject(422, "TEMPLATE_MISMATCH")
		rej.Audit = spamLabel(r, scope) + " requires an approved template"
		return rej
	default:
		// 403, not 400: policy refusal, uniform with the other policy blocks
		// (P1 scope finalize 2026-08-09).
		return spamReject(r, scope)
	}
}

// spamLabel names a rule the way an operator files it: which list, which
// category.
func spamLabel(r refdata.SpamMatcher, scope string) string {
	cat := r.Category
	if cat == "" {
		cat = "uncategorised"
	}
	return "spam rule: " + scope + "/" + cat
}

// spamReject is the one refusal this stage makes. The CLIENT always sees the
// bare "SPAM_KEYWORD" it has always seen — unchanged for every existing
// integration, and it keeps our rule taxonomy to ourselves, so a sender cannot
// map out which category they tripped by probing one message at a time
// (PO 2026-09-09).
//
// The category and which list it came from ride along in Audit, which reaches
// the rejected-submit record and the server log only. That is what makes the
// category column worth filling in: without it every refusal reads
// "SPAM_KEYWORD" and nobody can answer "how much gambling traffic did we
// refuse this month" from the records.
func spamReject(r refdata.SpamMatcher, scope string) *Reject {
	rej := reject(403, "SPAM_KEYWORD")
	rej.Audit = spamLabel(r, scope)
	return rej
}
