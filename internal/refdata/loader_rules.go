// Loading the message rules: auto-replace, spam keywords, templates, the
// blocklist and country rules.

package refdata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/yakugate/yakugate/internal/msisdn"
)

// LoadAutoReplace reads auto_replace_text — from the CONFIG store since GW-04
// (PO 2026-08-27; migration config/0064). It moved out of the queue store so
// the Admin API can manage it without breaking the "never touch the queue
// store" invariant. replace_regex is {match,replace}.
func (l *MySQL) LoadAutoReplace(ctx context.Context) (map[int64][]AutoReplaceRule, error) {
	m := map[int64][]AutoReplaceRule{}
	refs, err := l.ref.AutoReplace(ctx)
	if err != nil && !rowsErr(err) {
		// Only a genuinely ABSENT table is the tolerated "optional" case
		// (chunk-5 ruling 5.8, 2026-08-04). Any other error used to be
		// swallowed as an empty rule set — silently sending UNREPLACED text
		// for a whole refresh cycle on a transient DB error. Returning the
		// error instead aborts this Refresh, which keeps the PREVIOUS cycle's
		// rules live until the next successful reload.
		if l.isMissingTable(err) { // ER_NO_SUCH_TABLE
			return m, nil
		}
		return nil, fmt.Errorf("auto_replace_text: %w", err)
	}
	if err != nil && refs == nil {
		return nil, bare(err) // a row that would not scan
	}
	for _, x := range refs {
		acct, rr, seq := x.AccountID, x.Regex, x.Seq
		var raw RawRegexJSON
		if json.Unmarshal([]byte(rr), &raw) != nil {
			continue
		}
		rule := AutoReplaceRule{Seq: seq, Replace: raw.Replace}
		// A "match" that is a plain literal is applied as a literal; if it looks
		// like a pattern and compiles, use regex.
		if re, err := regexp.Compile(raw.Match); err == nil && isPattern(raw.Match) {
			rule.Match = re
		} else {
			rule.Literal = raw.Match
		}
		m[acct] = append(m[acct], rule)
	}
	return m, bare(err)
}

func isPattern(s string) bool {
	return strings.ContainsAny(s, `.*+?()[]{}^$|\`)
}

// LoadSpam returns the universal (gateway-wide) blocked keyword list plus
// every company's own blocked/allowed override lists (migration config/0051:
// spam_keywords now scopes to company_id, not account_id — company_id IS
// NULL is the universal/global row, unchanged in meaning).
//
// keywords is [{"word":"...","enabled":true}, ...] (PO 2026-07-27, Hadal
// reconciliation migration config/0036 — per-keyword enable/disable, adopted
// while keeping the batched-JSON-read shape rather than one row per keyword).
// A pre-migration flat string array is also accepted so an unmigrated row
// degrades to "every keyword enabled" instead of silently vanishing.
func (l *MySQL) LoadSpam(ctx context.Context) (SpamLists, error) {
	// GW-17 remodel (PO 2026-08-27): keyword_regex IS the list — the keywords
	// array is retired as a storage format. A row still carrying only the
	// legacy array is converted here (escaped alternation of its enabled
	// words) so nothing breaks, but the Admin API writes regex-only now.
	// category and destination_country_code finally reach the matcher.
	refs, err := l.ref.Spam(ctx) // by id: the order rules are tried in, which the portal's tester follows
	if err != nil && refs == nil {
		return SpamLists{}, bare(err)
	}
	out := SpamLists{CompanyBlocked: map[int64][]SpamRule{}, CompanyAllowed: map[int64][]SpamRule{}}
	for _, x := range refs {
		companyID := x.CompanyID
		listType, category, action, country, kwJSON, rxJSON := x.ListType, x.Category, x.Action, x.Country, x.Keywords, x.KeywordRegex
		pattern := parseSpamRegex(rxJSON)
		if pattern == "" {
			pattern = legacyKeywordsToRegex(kwJSON)
		}
		if pattern == "" {
			continue
		}
		rule := SpamRule{Category: category, Action: action, Country: msisdn.Clean(country), Pattern: pattern}
		switch {
		case !companyID.Valid:
			// A global 'allowed' row has nothing to override at the universal
			// level — there is no broader scope above it — so it's ignored rather
			// than silently applied somewhere unexpected.
			if listType == "blocked" {
				out.Universal = append(out.Universal, rule)
			}
		case listType == "blocked":
			out.CompanyBlocked[companyID.Int64] = append(out.CompanyBlocked[companyID.Int64], rule)
		case listType == "allowed":
			out.CompanyAllowed[companyID.Int64] = append(out.CompanyAllowed[companyID.Int64], rule)
		}
	}
	return out, bare(err)
}

// parseSpamRegex reads keyword_regex: {"match":"..."} (the templates shape) or
// a bare JSON string; anything else is "".
func parseSpamRegex(rxJSON string) string {
	if rxJSON == "" || rxJSON == "null" {
		return ""
	}
	var obj struct {
		Match string `json:"match"`
	}
	if json.Unmarshal([]byte(rxJSON), &obj) == nil && obj.Match != "" {
		return obj.Match
	}
	var bare string
	if json.Unmarshal([]byte(rxJSON), &bare) == nil {
		return bare
	}
	return ""
}

// LegacyKeywordsToRegex is legacyKeywordsToRegex for other readers of the
// table (the console), so a rule is read one way everywhere.
func LegacyKeywordsToRegex(kwJSON string) string { return legacyKeywordsToRegex(kwJSON) }

// legacyKeywordsToRegex converts a pre-GW-17 keywords array into the
// equivalent regex: an escaped alternation of its ENABLED words.
func legacyKeywordsToRegex(kwJSON string) string {
	words := parseSpamKeywords(kwJSON)
	if len(words) == 0 {
		return ""
	}
	esc := make([]string, 0, len(words))
	for _, w := range words {
		if w != "" {
			esc = append(esc, regexp.QuoteMeta(w))
		}
	}
	if len(esc) == 0 {
		return ""
	}
	return strings.Join(esc, "|")
}

// parseSpamKeywords accepts either shape keywords has held: the current
// [{"word","enabled"}] objects (only enabled ones returned) or the
// pre-migration flat []string (every entry implicitly enabled).
func parseSpamKeywords(raw string) []string {
	var objs []struct {
		Word    string `json:"word"`
		Enabled bool   `json:"enabled"`
	}
	if json.Unmarshal([]byte(raw), &objs) == nil && len(objs) > 0 {
		out := make([]string, 0, len(objs))
		for _, o := range objs {
			if o.Enabled && o.Word != "" {
				out = append(out, o.Word)
			}
		}
		return out
	}
	var flat []string
	if json.Unmarshal([]byte(raw), &flat) == nil {
		return flat
	}
	return nil
}

// LoadTemplates returns approved, enabled templates (id + match-regex) per account.
func (l *MySQL) LoadTemplates(ctx context.Context) (map[int64][]TemplateRegex, error) {
	// GW-16 (PO ruling 2026-08-24, built 2026-08-27): templates are authored
	// per COMPANY and apply to every account under it. A row with account_id
	// keys normally; a company-level row (account_id NULL, company_id set)
	// keys under the NEGATED company id — a private key-space convention that
	// keeps the Loader interface (and the Redis snapshot serialization)
	// unchanged. Mem.Templates unions both sides.
	// templates.default_sender is RETIRED (Saleem 2026-09-29: "no default
	// sender ids for templates"): kept in the table, read by nothing. A
	// template only says which senders may use it (shortcodes); the sender
	// itself is the client's, else the account's default sender.
	//
	// Only ENABLED templates (QA templates BUG-006, 2026-10-14): a template
	// switched off in the portal (is_disabled = 1, config/0036) was still
	// matched, stamped, and still forced its priority and ap fields. The
	// abuse monitor reads templates through this same call, so its drift
	// check follows.
	//
	// ORDER BY id: the order the stage tries a key's templates in is settled
	// by CompileTemplates (more literal text first, then the lower id); a
	// fixed read order keeps that sort's ties stable from one reload to the
	// next.
	refs, err := l.ref.Templates(ctx)
	if err != nil && refs == nil {
		return nil, bare(err)
	}
	m := map[int64][]TemplateRegex{}
	for _, x := range refs {
		id, acct, company := x.ID, x.AccountID, x.CompanyID
		rxJSON, defPrio, scJSON := x.Regex, x.DefaultPriority, x.Shortcodes
		ap := Passthrough{AP1: x.AP1, AP2: x.AP2, APG1: x.APG1, APG2: x.APG2}
		key := acct
		if acct == 0 && company > 0 {
			key = -company // company-level template (GW-16)
		}
		var raw RawRegexJSON
		if json.Unmarshal([]byte(rxJSON), &raw) == nil && raw.Match != "" {
			m[key] = append(m[key], TemplateRegex{
				ID: id, Match: raw.Match,
				DefaultPriority: defPrio,
				Shortcodes:      parseStringList(scJSON),
				Pass:            ap,
			})
		} else {
			// Say-something rule (chunk-5 ruling 5.6, 2026-08-04): a silently
			// dropped template can silently reject ALL of an account's traffic
			// when enforce_template_check is on. The compile-failure twin of
			// this log lives in cache.Refresh.
			log.Printf("[refdata] template %d (account %d) has malformed regex JSON — DROPPED", id, acct)
		}
	}
	return m, bare(err)
}

// LoadBlocklist returns DND / blocklist entries. Scope resolution per GW-18:
// account_id > 0 = account, else company_id > 0 = whole company, else global.
// `sender` (renamed from sender_mask by config/0063) matches exactly.
func (l *MySQL) LoadBlocklist(ctx context.Context) ([]Block, error) {
	refs, err := l.ref.Blocks(ctx)
	if err != nil && refs == nil {
		return nil, bare(err)
	}
	var out []Block
	for _, x := range refs {
		bk := Block{Msisdn: x.MSISDN, AccountID: x.AccountID, CompanyID: x.CompanyID, Sender: x.Sender, Source: x.Source}
		exp := x.ExpiresAt // NULL = permanent (P1 scope 2026-08-09)
		if exp.Valid {
			bk.ExpiresAt = exp.Time
		}
		out = append(out, bk)
	}
	return out, bare(err)
}

// LoadCountryRules returns per-account + global country rules from quota_by_countries.
func (l *MySQL) LoadCountryRules(ctx context.Context) (map[int64]map[string]CountryRule, error) {
	// destination_country_code is nullable (migration config/0054): a NULL is
	// the account's DEFAULT rule, keyed "" in the map. priority orders
	// evaluation (P1 scope finalize 2026-08-09).
	refs, err := l.ref.CountryRules(ctx)
	if err != nil && refs == nil {
		return nil, bare(err)
	}
	m := map[int64]map[string]CountryRule{}
	for _, x := range refs {
		acct, cc, mode, quotaJSON := x.AccountID, x.Country, x.Mode, x.Quota
		rateLimit, priority := x.RateLimit, x.Priority
		r := CountryRule{Mode: mode, RateLimit: rateLimit, Priority: priority}
		if quotaJSON.Valid && quotaJSON.String != "" {
			_ = json.Unmarshal([]byte(quotaJSON.String), &r.Quota)
		}
		if m[acct] == nil {
			m[acct] = map[string]CountryRule{}
		}
		m[acct][cc.String] = r // cc.String is "" when NULL -> the default rule
	}
	return m, bare(err)
}
