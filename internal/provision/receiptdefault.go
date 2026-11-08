package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/receiptpolicy"
)

// The gateway-wide DEFAULT receipt policy (issue 23, 2026-10-27): what a
// client is told where its account's own receipt policy has no rule for the
// case. One row of gateway_settings (receipt_policy_default), the same shape
// and the same parser as an account's policy (internal/receiptpolicy). The
// Admin API's door is services/receiptPolicyDefault.js (PUT
// /api/accounts/receipt-policy-default): the same row, the same stored form
// (the parser's), the same audit action — one database, two doors.
//
// The console edits it rule by rule (Gateway → Default receipt policy), never
// as JSON: one word, an error code and a hold-back for each rule. A split, or
// a rule that replaces a receipt, is set in the portal; the console shows it,
// keeps it, and can remove it.

// PolicyRule names one rule of a receipt policy the console sets on its own.
type PolicyRule struct {
	Key   string // on_accepted | progress | on_refused | submit_code | no_receipt
	Label string
	Hint  string
	// FinalOnly: the rule takes the six final words only.
	FinalOnly bool
}

// PolicyRules are the rules the console offers, in the order it lists them.
// The two codes of the gateway's own are submit_code rows with a fixed code.
func PolicyRules() []PolicyRule {
	return []PolicyRule{
		{Key: "on_accepted", Label: "When the vendor accepts a message",
			Hint: "For vendors that never send receipts. A progress word (ACCEPTD, ENROUTE) is the progress receipt instead."},
		{Key: "progress", Label: "Progress receipt when the vendor accepts it",
			Hint: "ACCEPTD or ENROUTE in place of the gateway's own ACCEPTD; a final word closes the message."},
		{Key: "on_refused", Label: "When the vendor refuses a message (every code)",
			Hint: "A vendor's refusal at submit. With no rule anywhere the client gets REJECTD with the vendor's code."},
		{Key: "submit_code:" + receiptpolicy.CodeValidityExpired, Label: "Validity over before it was sent (VALIDITY_EXPIRED)",
			Hint: "The gateway's own outcome. With no rule anywhere the client gets EXPIRED.", FinalOnly: true},
		{Key: "submit_code:" + receiptpolicy.CodeMaxAttempts, Label: "Its tries ran out (MAX_ATTEMPTS_REACHED)",
			Hint: "The gateway's own outcome. With no rule anywhere the client gets UNDELIV.", FinalOnly: true},
		{Key: "submit_code", Label: "One vendor refusal code (SMPP_58, HTTP_400, or * for the rest)",
			Hint: "The exact code wins over *, and both over the refusal rule."},
		{Key: "no_receipt", Label: "When no receipt arrives in time",
			Hint: "After the minutes you give (1 to 10080), the client is told this once.", FinalOnly: true},
	}
}

// PolicyAnswer is one rule's answer as the console asks it.
type PolicyAnswer struct {
	Word     string // one of the eight receipt words (receiptpolicy.Word reads either spelling)
	Err      string // the code sent; "" = none (000)
	DelayMin int    // hold-back in seconds, 0 = at once
	DelayMax int
	AfterMin int // no_receipt only
}

// SetPolicyRule is p with one rule set (key from PolicyRules; code for a
// submit_code row with no fixed code). The result is checked by the
// gateway's own parser, so a rule the gateway would refuse is refused here,
// with its words. p is not changed.
func SetPolicyRule(p *receiptpolicy.Policy, key, code string, a PolicyAnswer) (*receiptpolicy.Policy, error) {
	next := clonePolicy(p)
	act := receiptpolicy.Action{Status: a.Word, Err: strings.TrimSpace(a.Err), DelayMinSec: a.DelayMin, DelayMaxSec: a.DelayMax}
	switch {
	case key == "on_accepted":
		next.OnAccepted = &act
	case key == "on_refused":
		next.OnRefused = &act
	case key == "progress":
		next.Progress = &receiptpolicy.Progress{Stat: a.Word, DelayMinSec: a.DelayMin, DelayMaxSec: a.DelayMax}
	case key == "no_receipt":
		next.NoReceipt = &receiptpolicy.Timeout{AfterMin: a.AfterMin, Action: receiptpolicy.Action{Status: a.Word, Err: act.Err}}
	case strings.HasPrefix(key, "submit_code"):
		if fixed := strings.TrimPrefix(key, "submit_code:"); fixed != key {
			code = fixed
		}
		code = strings.TrimSpace(code)
		if code == "" {
			return nil, refusef("a refusal code is needed (SMPP_58, HTTP_400, %s, %s, or * for the rest)",
				receiptpolicy.CodeValidityExpired, receiptpolicy.CodeMaxAttempts)
		}
		next.SubmitCodes = withoutCode(next.SubmitCodes, code)
		next.SubmitCodes = append(next.SubmitCodes, receiptpolicy.CodeRule{Code: code, Action: act})
	default:
		return nil, refusef("%q is not a rule of a receipt policy", key)
	}
	return checked(next)
}

// RemovePolicyRule is p without one rule: a key of PolicyRules (with code for
// a submit_code row), or "receipt" with index for a rule that replaces a
// receipt. nil = nothing is left. p is not changed.
func RemovePolicyRule(p *receiptpolicy.Policy, key, code string, index int) (*receiptpolicy.Policy, error) {
	next := clonePolicy(p)
	switch {
	case key == "on_accepted":
		next.OnAccepted = nil
	case key == "on_refused":
		next.OnRefused = nil
	case key == "progress":
		next.Progress = nil
	case key == "no_receipt":
		next.NoReceipt = nil
	case key == "receipt":
		if index < 0 || index >= len(next.Receipts) {
			return nil, refusef("there is no receipt rule %d", index+1)
		}
		next.Receipts = append(next.Receipts[:index:index], next.Receipts[index+1:]...)
	case strings.HasPrefix(key, "submit_code"):
		if fixed := strings.TrimPrefix(key, "submit_code:"); fixed != key {
			code = fixed
		}
		next.SubmitCodes = withoutCode(next.SubmitCodes, code)
	default:
		return nil, refusef("%q is not a rule of a receipt policy", key)
	}
	if next.OnAccepted == nil && next.OnRefused == nil && next.Progress == nil && next.NoReceipt == nil &&
		len(next.SubmitCodes) == 0 && len(next.Receipts) == 0 {
		return nil, nil
	}
	return checked(next)
}

// PolicyLines says a policy's rules in words, one a line, for the console.
func PolicyLines(p *receiptpolicy.Policy) []string {
	if p == nil {
		return nil
	}
	var out []string
	add := func(what string, a receiptpolicy.Action) { out = append(out, what+": "+answerWords(a)) }
	if p.OnAccepted != nil {
		add("the vendor accepts it", *p.OnAccepted)
	}
	if p.Progress != nil {
		out = append(out, "progress receipt when it is accepted: "+receiptpolicy.Word(p.Progress.Stat)+delayWords(p.Progress.DelayMinSec, p.Progress.DelayMaxSec))
	}
	if p.OnRefused != nil {
		add("the vendor refuses it", *p.OnRefused)
	}
	for _, r := range p.SubmitCodes {
		what := "refusal code " + r.Code
		switch receiptpolicy.OwnCode(r.Code) {
		case receiptpolicy.CodeValidityExpired:
			what = "validity over (" + r.Code + ")"
		case receiptpolicy.CodeMaxAttempts:
			what = "tries ran out (" + r.Code + ")"
		}
		add(what, r.Action)
	}
	for i, r := range p.Receipts {
		match := strings.TrimSpace(r.Stat + " " + r.Err)
		out = append(out, fmt.Sprintf("receipt rule %d (%s): %s — change it in the portal", i+1, match, answerWords(r.Action)))
	}
	if p.NoReceipt != nil {
		add(fmt.Sprintf("no receipt in %d min", p.NoReceipt.AfterMin), p.NoReceipt.Action)
	}
	return out
}

func answerWords(a receiptpolicy.Action) string {
	if len(a.Split) > 0 {
		parts := make([]string, 0, len(a.Split))
		for _, s := range a.Split {
			parts = append(parts, fmt.Sprintf("%s %d%%", receiptpolicy.Word(s.Status), s.Pct))
		}
		return "split " + strings.Join(parts, ", ") + " (change it in the portal)" + delayWords(a.DelayMinSec, a.DelayMaxSec)
	}
	s := receiptpolicy.Word(a.Status)
	if a.Err != "" {
		s += " err " + a.Err
	}
	return s + delayWords(a.DelayMinSec, a.DelayMaxSec)
}

func delayWords(min, max int) string {
	switch {
	case max == 0:
		return ""
	case min == max:
		return fmt.Sprintf(", held %d s", min)
	}
	return fmt.Sprintf(", held %d–%d s", min, max)
}

// ReceiptPolicyDefault reads the gateway-wide default (nil = none).
//
// It IS receiptpolicy.ReadDefault, the roles' own reading (one copy since
// 29 Oct 2026): no row, or a config store from before gateway_settings
// existed, is none; a stored value is checked by the gateway's parser.
func (s Store) ReceiptPolicyDefault(ctx context.Context) (*receiptpolicy.Policy, error) {
	if s.DB == nil {
		return nil, nil
	}
	return receiptpolicy.ReadDefault(ctx, s.DB)
}

// SaveReceiptPolicyDefault stores the default (nil removes it), as the Admin
// API does: checked by the gateway's parser, stored in the parser's form,
// with a receipt_policy.default.update (or .delete) audit row holding the
// policy before and after. changed=false: it was that already.
func (s Store) SaveReceiptPolicyDefault(ctx context.Context, p *receiptpolicy.Policy) (changed bool, err error) {
	before, err := s.ReceiptPolicyDefault(ctx)
	if err != nil {
		return false, err
	}
	var after *receiptpolicy.Policy
	if p != nil {
		if after, err = checked(clonePolicy(p)); err != nil {
			return false, err
		}
	}
	if samePolicy(before, after) {
		return false, nil
	}
	if after == nil {
		err = s.DB.DeleteGatewaySetting(ctx, receiptpolicy.DefaultSetting)
	} else {
		raw, _ := json.Marshal(after)
		err = s.DB.SaveGatewaySetting(ctx, receiptpolicy.DefaultSetting, string(raw))
	}
	if err != nil {
		return false, fmt.Errorf("save the default receipt policy: %w", err)
	}
	action, summary := "receipt_policy.default.update", "Updated the gateway default receipt policy from the console"
	if after == nil {
		action, summary = "receipt_policy.default.delete", "Removed the gateway default receipt policy from the console"
	}
	return true, s.audit(ctx, auditEntry{
		Action: action, Entity: "gateway_settings", Feature: "accounts", EntityKey: receiptpolicy.DefaultSetting,
		Label: "Gateway default receipt policy", Summary: summary,
		Before: map[string]any{"policy": policyJSON(before)}, After: map[string]any{"policy": policyJSON(after)},
	})
}

// checked runs a policy through the gateway's own parser: refused in its
// words, else the parser's form (the Admin API stores the same).
func checked(p *receiptpolicy.Policy) (*receiptpolicy.Policy, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	out, err := receiptpolicy.Parse(raw)
	if err != nil {
		return nil, refusef("%s", strings.TrimPrefix(err.Error(), "receipt policy: "))
	}
	return out, nil
}

// clonePolicy is a deep copy through JSON (the default under it, if any, is
// not part of a policy as stored). nil = an empty policy.
func clonePolicy(p *receiptpolicy.Policy) *receiptpolicy.Policy {
	out := &receiptpolicy.Policy{}
	if p == nil {
		return out
	}
	raw, _ := json.Marshal(p)
	_ = json.Unmarshal(raw, out)
	return out
}

// withoutCode is rows without the one for code (in either name).
func withoutCode(rows []receiptpolicy.CodeRule, code string) []receiptpolicy.CodeRule {
	want := strings.ToUpper(strings.TrimSpace(code))
	if own := receiptpolicy.OwnCode(code); own != "" {
		want = own
	}
	out := rows[:0:0]
	for _, r := range rows {
		have := strings.ToUpper(strings.TrimSpace(r.Code))
		if own := receiptpolicy.OwnCode(r.Code); own != "" {
			have = own
		}
		if have != want {
			out = append(out, r)
		}
	}
	return out
}

func policyJSON(p *receiptpolicy.Policy) any {
	if p == nil {
		return nil
	}
	var v any
	raw, _ := json.Marshal(p)
	_ = json.Unmarshal(raw, &v)
	return v
}

func samePolicy(a, b *receiptpolicy.Policy) bool {
	x, _ := json.Marshal(policyJSON(a))
	y, _ := json.Marshal(policyJSON(b))
	return string(x) == string(y)
}
