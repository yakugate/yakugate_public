// Templates: the approved message templates, how they compile, and when a
// message needs a template verdict.

package refdata

import (
	"log"
	"regexp"
	"regexp/syntax"
)

// NeedsTemplateVerdict reports whether any blocked rule in scope for this
// company uses action='require_template'.
//
// It exists purely to keep the cost of that action off everyone else's hot
// path. The template stage returns immediately for an account with
// enforce_template_check off, and most accounts have it off. require_template
// needs a verdict from that stage anyway, so without this gate every message
// on the gateway would start running every template regex the moment ONE
// operator set ONE rule to require_template. Computed once per cache refresh,
// so the check here is a map read.
func (m *Mem) NeedsTemplateVerdict(companyID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.spamNeedsTplUni {
		return true
	}
	return companyID != 0 && m.spamNeedsTplCo[companyID]
}

// anyRequiresTemplate reports whether a prepared rule list contains one.
func anyRequiresTemplate(list []SpamMatcher) bool {
	for _, r := range list {
		if r.Action == SpamActionTemplate {
			return true
		}
	}
	return false
}

// Templates returns the templates that apply to an account: its own rows plus
// its company's (GW-16 — company rows live under the negated company id, see
// LoadTemplates). companyID 0 = unlinked account, account rows only.
func (m *Mem) Templates(accountID, companyID int64) []Template {
	m.mu.RLock()
	defer m.mu.RUnlock()
	own := m.templates[accountID]
	if companyID == 0 {
		return own
	}
	co := m.templates[-companyID]
	if len(co) == 0 {
		return own
	}
	out := make([]Template, 0, len(own)+len(co))
	out = append(out, own...)
	return append(out, co...)
}

// Passthrough is a template's own ap1/ap2/apg1/apg2 (config/0130): each one
// fills the same field of a matched message when the client left it empty.
type Passthrough struct{ AP1, AP2, APG1, APG2 string }

// TemplateRegex is one approved template's id + match pattern (loader output).
type TemplateRegex struct {
	ID    int64
	Match string
	// DefaultPriority forces the pipeline priority when a message matches this
	// template AND the client didn't set one explicitly (0 = no override, the
	// generic default applies). [SCOPE P1-TEMPLATE-PRIORITY, S-11]
	DefaultPriority int
	// Shortcodes narrows which senders may use this template (S-11, PO
	// 2026-09-04). Empty = any sender the account is already allowed, which
	// is every template that predates this. (A template's own sender,
	// default_sender, is retired since 2026-09-29.)
	Shortcodes []string
	// Pass fills the client pass-through fields it left empty (config/0130).
	Pass Passthrough
}

// Template is a compiled approved template held in the cache. Prefix is the
// regex's guaranteed literal prefix of the MATCH (may be empty); Anchored
// reports whether the match must start at the beginning of the text. The
// template stage uses these to skip regexes that provably cannot match:
// anchored → body must start with Prefix; unanchored → body must contain it.
type Template struct {
	ID              int64
	Re              *regexp.Regexp
	Prefix          string
	Anchored        bool
	DefaultPriority int
	// Shortcodes restricts this template to specific senders (S-11, PO
	// 2026-09-04). Empty = no restriction.
	Shortcodes []string
	// Pass: the template's own ap1/ap2/apg1/apg2 (config/0130).
	Pass Passthrough
}

// AllowsSender reports whether this template may be used for a message sent
// from `sender`. A template with no shortcode scope allows every sender — the
// account's own allowlist (stage 12) is still the outer gate either way.
func (t Template) AllowsSender(sender string) bool {
	if len(t.Shortcodes) == 0 {
		return true
	}
	for _, sc := range t.Shortcodes {
		if sc == sender {
			return true
		}
	}
	return false
}

// CompileTemplates compiles what LoadTemplates returns, key for key: an
// account's own templates under its id, a company's under its id negated
// (GW-16). The cache uses it at every reload and the abuse monitor for
// template drift, so both match a text against the same thing (QA
// 2026-10-08, B-02: the monitor joined on account_id and saw no company
// template).
//
// Each key's list comes back in the order the template stage tries it — the
// more specific pattern first, then the lower id (template_order.go) — and a
// forced priority outside 1-9 is dropped (usablePriority).
func CompileTemplates(raw map[int64][]TemplateRegex) map[int64][]Template {
	tpl := make(map[int64][]Template, len(raw))
	weight := map[int64]int{}
	for acct, pats := range raw {
		for _, p := range pats {
			re, e := regexp.Compile(p.Match)
			if e != nil {
				// A dropped template is NOT harmless: with enforce_template_check
				// on it silently shrinks the approved set — down to rejecting
				// 100% of the account's traffic TEMPLATE_MISMATCH if it was the
				// only one. Same say-something rule as the malformed-dialect
				// salvage (chunk-5 ruling 5.6, 2026-08-04).
				log.Printf("[refdata] template %d (account %d) regex does not compile (%v) — DROPPED", p.ID, acct, e)
				continue
			}
			prefix, _ := re.LiteralPrefix()
			weight[p.ID] = literalWeight(p.Match)
			tpl[acct] = append(tpl[acct], Template{
				ID: p.ID, Re: re,
				Prefix: prefix, Anchored: startAnchored(p.Match),
				DefaultPriority: usablePriority(p.ID, p.DefaultPriority),
				Shortcodes:      p.Shortcodes,
				Pass:            p.Pass,
			})
		}
	}
	for _, list := range tpl {
		sortTemplates(list, weight)
	}
	return tpl
}

// startAnchored reports whether every match of pattern must begin at the
// start of the text ("^" in default mode or `\A`; "(?m)^" does NOT qualify).
func startAnchored(pattern string) bool {
	p, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return false
	}
	prog, err := syntax.Compile(p.Simplify())
	if err != nil {
		return false
	}
	return prog.StartCond()&syntax.EmptyBeginText != 0
}
