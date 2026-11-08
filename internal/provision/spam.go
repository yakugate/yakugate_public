package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/msisdn"
	"github.com/yakugate/yakugate/internal/refdata"
)

// Spam rules (table spam_keywords). [SCOPE P14-SPAM]
//
// One row is one rule: a scope (every company when company_id is NULL, else
// that company), a kind (blocked | allowed), a name, an optional destination
// prefix, and ONE pattern in keyword_regex {"match": …} (GW-17). The engine
// compiles "(?i)"+pattern with Go's RE2 and runs it on the raw body — and on
// the leet-folded body too for an account with strict checking on — but only
// for accounts whose spam check is on (sms_service_config.enforce_spam_check).
// A company ALLOWED rule that matches exempts the message from every blocked
// rule, universal and the company's own.
//
// The rules here are the Admin API's (p13-backend resources.js, `spam`), so
// the two doors cannot disagree:
//   - a universal ALLOWED rule is refused: nothing above it to override, and
//     the engine skips it;
//   - one rule per (scope, prefix, kind, name), checked NULL-safe — the
//     table's unique key cannot, as company and prefix are usually NULL;
//   - an allowed rule's action is always "reject" (it exempts, it does not
//     act);
//   - the pattern must compile as the engine compiles it; the retired
//     keywords array is written [].
//
// Stricter than the portal, never looser: the name is capped at the column's
// 32 characters (the API allows 64 and MySQL then refuses the row), and the
// prefix must be digits (the engine cleans it to digits anyway).

// SpamRule is one rule as the console shows it.
type SpamRule struct {
	ID        int64
	CompanyID int64  // 0 = every company (universal)
	Company   string // "every company" for a universal rule
	Kind      string // blocked | allowed
	Action    string // reject | flag | require_template (blocked rules)
	Name      string // the category column
	Prefix    string // destination dialling prefix; "" = every destination
	Pattern   string // what the gateway enforces — for a Legacy rule, its old words as a pattern
	Legacy    bool   // only the retired keywords array: Pattern is converted from it
}

// SpamKinds are the two kinds of rule.
func SpamKinds() []string { return []string{"blocked", "allowed"} }

// SpamActions are what a matching blocked rule does.
func SpamActions() []string { return []string{"reject", "flag", "require_template"} }

// SpamActionWhat says what an action does, for a menu.
func SpamActionWhat(a string) string {
	switch a {
	case "reject":
		return "refuse the message (403 SPAM_KEYWORD)"
	case "flag":
		return "send it, and mark the record — see what a rule would catch before it refuses anyone"
	case "require_template":
		return "refuse it unless the text matches one of the account's approved templates"
	}
	return ""
}

const spamNameMax = 32 // spam_keywords.category is VARCHAR(32)

// WordsToPattern turns "loan, free credit, win" into a pattern matching any
// of the words — the same conversion the Admin API makes of a keyword list,
// so an operator who does not write patterns is still served.
func WordsToPattern(words string) (string, error) {
	var parts []string
	seen := map[string]bool{}
	for _, w := range strings.Split(words, ",") {
		w = strings.TrimSpace(w)
		if w == "" || seen[strings.ToLower(w)] {
			continue
		}
		seen[strings.ToLower(w)] = true
		parts = append(parts, regexp.QuoteMeta(w))
	}
	if len(parts) == 0 {
		return "", refusef("no words given")
	}
	return strings.Join(parts, "|"), nil
}

// CheckSpamPattern compiles a pattern the way the engine does. A pattern the
// engine cannot compile would be dropped at load, and the rule would look
// saved while blocking nothing.
func CheckSpamPattern(p string) error {
	if strings.TrimSpace(p) == "" {
		return refusef("the pattern is empty")
	}
	if _, err := regexp.Compile("(?i)" + p); err != nil {
		return refusef("the gateway cannot use this pattern: %s", strings.TrimPrefix(err.Error(), "error parsing regexp: "))
	}
	return nil
}

// SpamMatches reports whether a rule's pattern matches a text, as the engine
// tests it (case-insensitive, anywhere in the text).
func SpamMatches(pattern, text string) bool {
	re, err := regexp.Compile("(?i)" + pattern)
	return err == nil && re.MatchString(text)
}

// ValidateSpamRule checks a rule before it is written.
func ValidateSpamRule(r SpamRule) error {
	if r.Kind != "blocked" && r.Kind != "allowed" {
		return refusef("a rule is blocked or allowed, not %q", r.Kind)
	}
	if r.Kind == "allowed" && r.CompanyID == 0 {
		return refusef("an allowed rule only means something for one company: it lifts the universal " +
			"blocked rules for that company. A universal allowed rule has nothing above it and the gateway ignores it")
	}
	if r.Kind == "blocked" {
		ok := false
		for _, a := range SpamActions() {
			ok = ok || r.Action == a
		}
		if !ok {
			return refusef("the action is reject, flag or require_template, not %q", r.Action)
		}
	}
	if n := strings.TrimSpace(r.Name); n == "" {
		return refusef("the rule needs a name")
	} else if len([]rune(n)) > spamNameMax {
		return refusef("the name is %d characters; at most %d", len([]rune(n)), spamNameMax)
	}
	if r.Prefix != "" {
		if c := msisdn.Clean(r.Prefix); c != r.Prefix || !isDigits(c) || len(c) > 6 {
			return refusef("the destination prefix is up to 6 digits, e.g. 92 — %q is not", r.Prefix)
		}
	}
	return CheckSpamPattern(r.Pattern)
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// ListSpamRules lists every rule: universal first, then by company.
func (s Store) ListSpamRules(ctx context.Context) ([]SpamRule, error) {
	rows, err := s.DB.SpamRuleRows(ctx)
	if err != nil {
		if re, ok := err.(dal.RowsError); ok {
			return nil, re.Err
		}
		return nil, fmt.Errorf("list spam rules: %w", err)
	}
	var out []SpamRule
	for _, row := range rows {
		r := SpamRule{ID: row.ID, CompanyID: row.CompanyID, Company: row.Company, Kind: row.Kind, Action: row.Action, Name: row.Name, Prefix: row.Prefix}
		rx, words := row.PatternJSON, row.WordsJSON
		r.Pattern = spamPatternOf(rx)
		if r.Pattern == "" {
			// A rule from before GW-17 holds only a word list, which the
			// engine still enforces by converting it the same way. Showing it
			// as "no pattern" — and leaving it out of the console's message
			// test — hid a rule that refuses traffic (found 2026-09-21).
			r.Pattern = refdata.LegacyKeywordsToRegex(words)
			r.Legacy = r.Pattern != ""
		}
		if r.CompanyID == 0 {
			r.Company = "every company"
		}
		out = append(out, r)
	}
	return out, nil
}

// spamPatternOf reads keyword_regex the way the engine's spam loader does ({"match":…}, or a
// bare JSON string).
func spamPatternOf(rx string) string {
	var obj struct {
		Match string `json:"match"`
	}
	if json.Unmarshal([]byte(rx), &obj) == nil && obj.Match != "" {
		return obj.Match
	}
	var bare string
	if json.Unmarshal([]byte(rx), &bare) == nil {
		return bare
	}
	return ""
}

// GetSpamRule reads one rule.
func (s Store) GetSpamRule(ctx context.Context, id int64) (SpamRule, error) {
	all, err := s.ListSpamRules(ctx)
	if err != nil {
		return SpamRule{}, err
	}
	for _, r := range all {
		if r.ID == id {
			return r, nil
		}
	}
	return SpamRule{}, refusef("there is no spam rule #%d", id)
}

// spamClash is the id of another rule with the same scope, prefix, kind and
// name (case-insensitive), or 0.
func (s Store) spamClash(ctx context.Context, r SpamRule, self int64) (int64, error) {
	id, err := s.DB.SpamRuleClash(ctx, r.CompanyID, r.Prefix, r.Kind, strings.TrimSpace(r.Name), self)
	if err == dal.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// CreateSpamRule adds a rule and audits it as spam.create.
func (s Store) CreateSpamRule(ctx context.Context, r SpamRule) (int64, error) {
	r.Name = strings.TrimSpace(r.Name)
	if r.Kind == "allowed" {
		r.Action = "reject"
	}
	if err := ValidateSpamRule(r); err != nil {
		return 0, err
	}
	if r.CompanyID != 0 {
		ok, err := s.DB.CompanyExists(ctx, r.CompanyID)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, refusef("there is no company #%d", r.CompanyID)
		}
	}
	if id, err := s.spamClash(ctx, r, 0); err != nil {
		return 0, err
	} else if id != 0 {
		return 0, refusef("there is already a %s rule named %q here (#%d) — change its pattern, or name this one differently", r.Kind, r.Name, id)
	}
	rx, _ := json.Marshal(map[string]string{"match": r.Pattern})
	id, err := s.DB.InsertSpamRule(ctx, dal.SpamRuleInsert{
		CompanyID: r.CompanyID, Prefix: r.Prefix, Kind: r.Kind, Action: r.Action, Name: r.Name, PatternJSON: string(rx),
	})
	if err != nil {
		return 0, fmt.Errorf("add spam rule: %w", err)
	}
	return id, s.audit(ctx, auditEntry{
		Action: "spam.create", Entity: "spam list", Feature: "spam", EntityID: id,
		CompanyID: r.CompanyID, Label: r.Name, Summary: "Created spam list from the console",
		Detail: map[string]any{"after": spamDetail(r)},
	})
}

// SpamRuleFields are what may be changed on a rule. Scope, kind and prefix are
// fixed: changing them makes a different rule, so delete and add instead.
func SpamRuleFields(kind string) []Field {
	f := []Field{
		{Key: "pattern", Label: "Pattern", Kind: "text", Hint: "what the rule looks for, e.g. loan|free credit (case does not matter)"},
		{Key: "name", Label: "Name", Kind: "text"},
	}
	if kind == "blocked" {
		f = append(f, Field{Key: "action", Label: "What it does", Kind: "choice", Choices: SpamActions(),
			Hint: "reject = refuse; flag = send and mark; require_template = only in approved wording"})
	}
	return f
}

// UpdateSpamRule changes one field and audits it as spam.update.
func (s Store) UpdateSpamRule(ctx context.Context, id int64, key, value string) error {
	cur, err := s.GetSpamRule(ctx, id)
	if err != nil {
		return err
	}
	want := cur
	switch key {
	case "pattern":
		want.Pattern = value
	case "name":
		want.Name = strings.TrimSpace(value)
	case "action":
		if cur.Kind != "blocked" {
			return refusef("an allowed rule exempts a message; it has no action")
		}
		want.Action = strings.ToLower(strings.TrimSpace(value))
	default:
		return refusef("%q is not a setting on a spam rule", key)
	}
	if err := ValidateSpamRule(want); err != nil {
		return err
	}
	if key == "name" {
		if other, err := s.spamClash(ctx, want, id); err != nil {
			return err
		} else if other != 0 {
			return refusef("there is already a %s rule named %q here (#%d)", want.Kind, want.Name, other)
		}
	}
	switch key {
	case "pattern":
		rx, _ := json.Marshal(map[string]string{"match": want.Pattern})
		// The array goes to [] with it: a rule cleared of words must not keep
		// matching them through the legacy path.
		err = s.DB.SetSpamPattern(ctx, id, string(rx))
	case "name":
		err = s.DB.SetSpamName(ctx, id, want.Name)
	case "action":
		err = s.DB.SetSpamAction(ctx, id, want.Action)
	}
	if err != nil {
		return fmt.Errorf("update spam rule %d: %w", id, err)
	}
	return s.audit(ctx, auditEntry{
		Action: "spam.update", Entity: "spam list", Feature: "spam", EntityID: id,
		CompanyID: cur.CompanyID, Label: want.Name, Summary: "Updated spam list from the console",
		Detail: map[string]any{"before": spamDetail(cur), "after": spamDetail(want)},
	})
}

// DeleteSpamRule removes a rule (the portal deletes too; there is no off
// switch on a rule — GW-17) and audits it as spam.delete.
func (s Store) DeleteSpamRule(ctx context.Context, id int64) error {
	cur, err := s.GetSpamRule(ctx, id)
	if err != nil {
		return err
	}
	if err := s.DB.DeleteSpamRow(ctx, id); err != nil {
		return fmt.Errorf("delete spam rule %d: %w", id, err)
	}
	return s.audit(ctx, auditEntry{
		Action: "spam.delete", Entity: "spam list", Feature: "spam", EntityID: id,
		CompanyID: cur.CompanyID, Label: cur.Name, Summary: "Deleted spam list from the console",
		Detail: map[string]any{"before": spamDetail(cur)},
	})
}

// SpamCheckCounts says how many accounts the rules apply to at all.
func (s Store) SpamCheckCounts(ctx context.Context) (on, total int, err error) {
	return s.DB.SpamCheckAccounts(ctx)
}

func spamDetail(r SpamRule) map[string]any {
	return map[string]any{
		"company_id": nullID(r.CompanyID), "list_type": r.Kind, "action": r.Action,
		"category": r.Name, "destination_country_code": nullStr(r.Prefix),
		"keyword_regex": map[string]string{"match": r.Pattern},
	}
}

// itoa is a short int64 formatter for messages.
func itoa(n int64) string { return strconv.FormatInt(n, 10) }
