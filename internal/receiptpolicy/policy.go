// Package receiptpolicy decides what a CLIENT is told about a message, when
// that is not simply what the vendor said (PO 2026-09-22).
//
// Two real needs sit behind it. Most HTTP vendors answer the submit call and
// then say nothing ever again — there is no receipt to pass on, and the
// customer's system waits for a final status that never comes. And a vendor
// that does send receipts may use a word or an error code the customer's
// system cannot read, or one an operator has agreed to report differently.
//
// So an account may carry a policy saying:
//
//	on_accepted   the vendor took the message  -> tell the client DELIVRD
//	on_refused    the vendor refused it        -> tell the client UNDELIV
//	submit_codes  per refusal code             -> a different answer per code
//	receipts      a real receipt arrived       -> tell the client another status
//	no_receipt    nothing arrived in N minutes -> tell the client something
//
// Three rules hold everywhere and are what keep this honest (config/0119):
//
//  1. The RECORD keeps what really happened. An invented answer is written
//     beside it (record_index.told_status), never over it, so vendor quality,
//     delivery ratios, retry and monitoring all still see the truth.
//  2. A client is told a FINAL status once. Whoever sets told_status first
//     wins; the vendor's own receipt is still recorded afterwards, but not
//     pushed. A customer never gets two contradicting final receipts.
//  3. Billing follows what the client was told (PO 2026-09-22).
//
// The policy is one JSON object on sms_service_config.receipt_policy. This
// package is the only parser in the engine; the Admin API validates against
// the same rules (p13-backend/src/services/receiptPolicy.js), and the two
// must be changed together.
//
//	{
//	  "on_accepted": {"status": "delivered", "delay_min_sec": 5, "delay_max_sec": 40},
//	  "on_refused":  {"status": "failed"},
//	  "submit_codes": [{"code": "SMPP_58", "status": "failed", "err": "058"}],
//	  "receipts":    [{"stat": "failed", "err": "0AB", "status": "expired", "send_err": "010"}],
//	  "no_receipt":  {"after_min": 60, "status": "delivered"},
//	  "progress":    {"stat": "ENROUTE", "delay_min_sec": 2}
//	}
//
// progress (2026-09-29, Saleem: "must have more options ACCEPTED and ENROUTE")
// is the receipt sent when the vendor takes the message, in place of the
// gateway's own ACCEPTD.
//
// What a rule may say is one of the EIGHT receipt words (2026-10-12, Saleem:
// "introduce all 8 type of dlr receipts in all 3 accept, progress and reject"),
// and the WORD decides what the receipt does, not the rule it is written in:
//
//	DELIVRD UNDELIV EXPIRED REJECTD DELETED UNKNOWN   final — the message is
//	    closed for the client: told once (record_index.told_*), a later real
//	    receipt is recorded and not sent, billing follows it
//	ACCEPTD ENROUTE                                   progress — a receipt and
//	    nothing more: never "told", never billed as an outcome, never blocks
//	    a final
//
// So on_accepted with ENROUTE is the progress receipt, and progress with
// DELIVRD closes the message exactly as on_accepted does. A refusal answered
// with a progress word sends the client that receipt in place of the failure,
// and the record, the status pull and billing keep the truth. The rules that
// only exist to close a message — receipts, no_receipt, and every share of a
// split — take the six final words only.
//
// Any action may carry a split instead of one status, which is how a share of
// traffic is reported one way and the rest another:
//
//	"on_accepted": {"split": [{"status":"delivered","pct":95},
//	                          {"status":"failed","pct":5,"err":"0AB"}]}
//
// The gateway's own outcomes (issue 23, 2026-10-27): a message that never
// reached a vendor has a reason code of ours, not a vendor's. submit_codes[]
// rows may name the two — VALIDITY_EXPIRED (its validity ended before it was
// sent) and MAX_ATTEMPTS_REACHED (its tries ran out) — and only an exact row
// answers them: "*" and on_refused are about a vendor's refusal and leave
// them alone. Such a row takes the six final words only (the message is
// closed for good; nothing else will ever come for it).
//
// A gateway-wide default (gateway_settings.receipt_policy_default, the same
// shape, the same parser) sits under every account's policy: the account's
// own rule wins where it has one for the case, the default's applies where it
// has none, and where neither has one the client gets the standard word
// (internal/dlr, own.go). Over joins the two; the decisions below ask the
// account first and the default second.
package receiptpolicy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// The eight receipt words a client may be told (SMPP 3.4 `stat:`). They are
// the same eight as dlr.Stat, spelled out here because package dlr reads this
// one and the import runs one way only; a test in internal/dlr holds the two
// lists, and what each word means, together.
const (
	WordDelivrd = "DELIVRD"
	WordUndeliv = "UNDELIV"
	WordExpired = "EXPIRED"
	WordRejectd = "REJECTD"
	WordDeleted = "DELETED"
	WordUnknown = "UNKNOWN"
	WordAccptd  = "ACCEPTD"
	WordEnroute = "ENROUTE"
)

// Words lists the eight, the six final ones first — the order the portal
// offers them in.
var Words = []string{WordDelivrd, WordUndeliv, WordExpired, WordRejectd, WordDeleted, WordUnknown,
	WordAccptd, WordEnroute}

// ProgressStats are the two words that do not close a message.
var ProgressStats = []string{WordAccptd, WordEnroute}

// stored is how a rule's `status` is kept for the three words a policy could
// say before 2026-10-12. They keep that spelling, so a policy saved then reads
// the same to a gateway built before this day and after it; the five newer
// ones are stored as the word itself.
var stored = map[string]string{WordDelivrd: "delivered", WordUndeliv: "failed", WordExpired: "expired"}

// Word reads what a rule says, in either spelling and any case, as one of the
// eight. "" = it is not one.
func Word(v string) string {
	k := strings.ToUpper(strings.TrimSpace(v))
	switch k {
	case "DELIVERED":
		return WordDelivrd
	case "FAILED":
		return WordUndeliv
	case "ACCEPTED":
		return WordAccptd
	}
	if oneOf(k, Words) {
		return k
	}
	return ""
}

// Final reports whether a word closes the message for the client. ACCEPTD and
// ENROUTE do not.
func Final(word string) bool {
	return oneOf(word, Words) && !oneOf(word, ProgressStats)
}

// StatusOf is the message status a word implies — what record_index.told_status
// holds and what billing reads. A progress word implies `sent`, which is what
// the message already is.
func StatusOf(word string) model.Status {
	switch word {
	case WordDelivrd:
		return model.StatusDelivered
	case WordExpired:
		return model.StatusExpired
	case WordAccptd, WordEnroute:
		return model.StatusSent
	case WordUndeliv, WordRejectd, WordDeleted, WordUnknown:
		return model.StatusFailed
	}
	return ""
}

// The gateway's own reason codes (issue 22, Saleem 23 Oct: "both are similar,
// use the meaningful one"): the codes on a message that never reached a
// vendor. They were EXPIRED and MAX_ATTEMPTS until 2026-10-27; old rows keep
// those, and every reader takes both (OwnCode).
const (
	// CodeValidityExpired: its validity ended while it waited to be sent.
	CodeValidityExpired = "VALIDITY_EXPIRED"
	// CodeMaxAttempts: its send tries, or its rule's retry rounds, ran out.
	CodeMaxAttempts = "MAX_ATTEMPTS_REACHED"
	// The names written before 2026-10-27.
	OldCodeExpired     = "EXPIRED"
	OldCodeMaxAttempts = "MAX_ATTEMPTS"
)

// OwnCodes are the gateway's own reason codes a submit_codes[] row may name.
var OwnCodes = []string{CodeValidityExpired, CodeMaxAttempts}

// OwnCode reads a reason code, in either name and any case, as one of the
// gateway's own. "" = it is not one (a vendor's code, or "*").
func OwnCode(code string) string {
	switch strings.ToUpper(strings.TrimSpace(code)) {
	case CodeValidityExpired, OldCodeExpired:
		return CodeValidityExpired
	case CodeMaxAttempts, OldCodeMaxAttempts:
		return CodeMaxAttempts
	}
	return ""
}

// cleanCode is a submit_codes[] row's code as it is stored: upper case, and
// an old name of the gateway's own codes as the new one.
func cleanCode(code string) string {
	if own := OwnCode(code); own != "" {
		return own
	}
	return strings.ToUpper(strings.TrimSpace(code))
}

// spelling is the stored form of a word in a rule's `status`.
func spelling(word string) string {
	if v, ok := stored[word]; ok {
		return v
	}
	return word
}

// choices lists what a rule may say, for an error a person has to act on.
func choices(withProgress bool) string {
	list := make([]string, 0, len(Words))
	for _, w := range Words {
		if !withProgress && !Final(w) {
			continue
		}
		if v, ok := stored[w]; ok {
			w += " (" + v + ")"
		}
		list = append(list, w)
	}
	return strings.Join(list, ", ")
}

// Pick is one possible answer and how often it is used. Pct values in a split
// must add up to 100, so every message gets an answer.
type Pick struct {
	Status string `json:"status"`
	Err    string `json:"err,omitempty"`
	Pct    int    `json:"pct"`
}

// Action is what to tell the client when a rule matches: one status, or a
// split of several, optionally held back for a while. Status is one of the
// eight words (Word reads it); the shares of a split are final words.
//
// The delay exists because an instant DELIVRD on a submit answer is visibly
// not a network receipt: a customer watching their own traffic sees every
// message resolve in the same millisecond it was accepted. A delay range
// spreads them; both ends in seconds, equal ends = a fixed wait.
type Action struct {
	Status string `json:"status,omitempty"`
	Err    string `json:"err,omitempty"`
	Split  []Pick `json:"split,omitempty"`

	DelayMinSec int `json:"delay_min_sec,omitempty"`
	DelayMaxSec int `json:"delay_max_sec,omitempty"`
}

// CodeRule answers one submit refusal code — "SMPP_58", "HTTP_400", or "*"
// for every code the rows above did not name. Codes are the exact strings the
// vendor roles put on the record (rejectionCode / SMPP_<hex>).
type CodeRule struct {
	Code   string `json:"code"`
	Action        // inline: a rule IS an action plus what it matches
}

// ReceiptRule answers a real receipt. Stat is the status it maps to for us
// (delivered|failed|expired) or the vendor's own word (UNDELIV, REJECTD…);
// either spelling matches. Err is the receipt's error code, exact, and empty
// means "whatever the code". "*" in either place matches anything.
//
// Rules are tried in the order the operator wrote them and the first match
// wins — the same "your order is the priority" rule the spam list uses.
//
// SendErr, not `err`, is the code the CLIENT is given: on this one rule type
// `err` is already the thing being matched, and one key cannot mean both. It
// also shadows the embedded Action's own `err` in Go's JSON decoding, which
// is why the field exists at all. Empty = pass the vendor's own code on.
type ReceiptRule struct {
	Stat    string `json:"stat,omitempty"`
	Err     string `json:"err,omitempty"`
	SendErr string `json:"send_err,omitempty"`
	Action
}

// Timeout is the no-receipt rule: the message was sent, the vendor said
// nothing for AfterMin minutes, so the client is told this and the message
// stops waiting.
type Timeout struct {
	AfterMin int `json:"after_min"`
	Action
}

// Progress is the receipt sent when the vendor takes a message, in place of
// the gateway's own ACCEPTD. With a final word it closes the message, exactly
// as on_accepted does.
type Progress struct {
	Stat        string `json:"stat"` // one of the eight words
	DelayMinSec int    `json:"delay_min_sec,omitempty"`
	DelayMaxSec int    `json:"delay_max_sec,omitempty"`
}

// Policy is one account's whole rule set, or the gateway-wide default.
type Policy struct {
	OnAccepted  *Action       `json:"on_accepted,omitempty"`
	OnRefused   *Action       `json:"on_refused,omitempty"`
	SubmitCodes []CodeRule    `json:"submit_codes,omitempty"`
	Receipts    []ReceiptRule `json:"receipts,omitempty"`
	NoReceipt   *Timeout      `json:"no_receipt,omitempty"`
	Progress    *Progress     `json:"progress,omitempty"`

	// under is the gateway-wide default, asked for a case this policy has no
	// rule for (Over). Never stored: unexported, so JSON leaves it out.
	under *Policy
}

// Rule names, as they are written to record_index.told_by.
const (
	RuleAccepted   = "on_accepted"
	RuleRefused    = "on_refused"
	RuleSubmitCode = "submit_code"
	RuleReceipt    = "receipt"
	RuleNoReceipt  = "no_receipt"
	// RuleProgress is the progress rule closing a message, when its word is
	// a final one.
	RuleProgress = "progress"
	// RuleGatewayCode is a submit_codes[] row on one of the gateway's own
	// codes (VALIDITY_EXPIRED, MAX_ATTEMPTS_REACHED).
	RuleGatewayCode = "gateway_code"
)

// DefaultPrefix marks a rule of the gateway-wide default in told_by and in
// the receipt row's raw text ("default:on_accepted", "policy:default:…"), so
// support can tell the account's own rule from the default's. The longest,
// "default:gateway_code", fits told_by's 24 characters.
const DefaultPrefix = "default:"

// Over is this policy with def under it: a case this policy has no rule for
// is answered by def. Either may be nil; a nil policy over a default is an
// empty one, so every answer is the default's and named as such. The policy
// itself is not changed (a copy is returned), and def's own default is never
// followed: there is one level only.
func (p *Policy) Over(def *Policy) *Policy {
	if def == nil {
		if p == nil || p.under == nil {
			return p
		}
		c := *p // a default taken away
		c.under = nil
		return &c
	}
	d := *def
	d.under = nil
	if p == nil {
		return &Policy{under: &d}
	}
	c := *p
	c.under = &d
	return &c
}

// Default is the gateway-wide default this policy was joined with, or nil.
func (p *Policy) Default() *Policy {
	if p == nil {
		return nil
	}
	return p.under
}

// ofDefault names a rule of the default.
func ofDefault(rule string) string {
	if rule == "" {
		return ""
	}
	return DefaultPrefix + rule
}

// ownRules reports whether this policy (not its default) has any rule other
// than rows on the gateway's own codes.
func (p *Policy) ownRules() bool {
	if p == nil {
		return false
	}
	if p.OnAccepted != nil || p.OnRefused != nil || p.NoReceipt != nil || p.Progress != nil || len(p.Receipts) > 0 {
		return true
	}
	for _, r := range p.SubmitCodes {
		if OwnCode(r.Code) == "" {
			return true
		}
	}
	return false
}

// OnReceiptPath reports whether this policy, or the default under it, can
// change what a vendor's receipt does: replace it (receipts), or have told
// the client a final answer before it came (on_accepted, progress,
// on_refused, a vendor's refusal code, no_receipt) — so the receipt path must
// read the record. Rows on the gateway's own codes alone do not: those
// messages never reached a vendor and no receipt comes for them, so a gateway
// whose default holds only those keeps every receipt on the fast path.
func (p *Policy) OnReceiptPath() bool {
	if p == nil {
		return false
	}
	return p.ownRules() || p.under.ownRules()
}

// Outcome is one decided answer for one message.
type Outcome struct {
	// Status is the message status the word implies (StatusOf): REJECTD,
	// DELETED and UNKNOWN are all `failed`, a progress word is `sent`.
	Status model.Status
	// Stat is the receipt word the client is sent, one of the eight.
	Stat  string
	Err   string
	Delay time.Duration
	// Rule is which part of the policy decided, for told_by and the logs.
	Rule string
}

// Final reports whether this answer closes the message for the client. A
// progress word does not: it is sent, and nothing is recorded as told.
func (o Outcome) Final() bool {
	if o.Stat != "" {
		return Final(o.Stat)
	}
	return o.Status != "" && o.Status != model.StatusSent
}

// Parse reads and CHECKS a stored policy. An unreadable or invalid policy is
// an error, never a half-applied one: a policy that decides what customers
// are told must not run in a shape nobody meant.
//
// Unknown fields are refused, so a misspelled key ("on_accept") is a loud
// error at save time instead of a rule that silently never fires.
func Parse(raw []byte) (*Policy, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" || string(raw) == "{}" {
		return nil, nil // no policy: the client hears only what the vendor said
	}
	var p Policy
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("receipt policy: %w", err)
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

// Validate checks a policy the way the Admin API must check it before saving.
func (p *Policy) Validate() error {
	if p == nil {
		return nil
	}
	if p.OnAccepted == nil && p.OnRefused == nil && p.NoReceipt == nil && p.Progress == nil &&
		len(p.SubmitCodes) == 0 && len(p.Receipts) == 0 {
		return fmt.Errorf("receipt policy: it says nothing; remove it instead of saving an empty one")
	}
	if err := p.OnAccepted.validate("on_accepted", true); err != nil {
		return err
	}
	if err := p.OnRefused.validate("on_refused", true); err != nil {
		return err
	}
	seen := map[string]bool{}
	for i := range p.SubmitCodes {
		r := &p.SubmitCodes[i]
		r.Code = cleanCode(r.Code)
		if r.Code == "" {
			return fmt.Errorf("submit_codes[%d]: needs a code (the vendor's refusal code, %s or %s, or * for the rest)",
				i, CodeValidityExpired, CodeMaxAttempts)
		}
		if seen[r.Code] {
			return fmt.Errorf("submit_codes: %s is listed twice", r.Code)
		}
		seen[r.Code] = true
		// One of the gateway's own outcomes is final for good: no vendor
		// took the message and nothing else will come, so a progress word in
		// its place would leave the client waiting for ever.
		if err := r.Action.validate(fmt.Sprintf("submit_codes[%d]", i), OwnCode(r.Code) == ""); err != nil {
			return err
		}
	}
	for i := range p.Receipts {
		r := &p.Receipts[i]
		r.Stat = strings.TrimSpace(r.Stat)
		r.Err = strings.TrimSpace(r.Err)
		if r.Stat == "" && r.Err == "" {
			return fmt.Errorf("receipts[%d]: needs a stat or an err to match (use \"*\" to match every receipt)", i)
		}
		// A real receipt is replaced by a final answer only: a progress word
		// in its place would leave the client waiting for ever.
		if err := r.Action.validate(fmt.Sprintf("receipts[%d]", i), false); err != nil {
			return err
		}
		// The embedded Action's own `err` can never be set from JSON here
		// (this rule's `err` shadows it), so the sent code comes from
		// send_err — checked the same way and then carried as the action's.
		r.SendErr = strings.TrimSpace(r.SendErr)
		if err := checkErr(fmt.Sprintf("receipts[%d].send_err", i), r.SendErr); err != nil {
			return err
		}
		r.Action.Err = r.SendErr
	}
	if p.Progress != nil {
		g := p.Progress
		w := Word(g.Stat)
		if w == "" {
			return fmt.Errorf("progress.stat: %q — it must be one of %s", strings.TrimSpace(g.Stat), choices(true))
		}
		g.Stat = w
		// The same delay rules as an action: one end is a fixed wait, at
		// most a day.
		d := Action{Status: "delivered", DelayMinSec: g.DelayMinSec, DelayMaxSec: g.DelayMaxSec}
		if err := d.validate("progress", true); err != nil {
			return err
		}
		g.DelayMinSec, g.DelayMaxSec = d.DelayMinSec, d.DelayMaxSec
	}
	// The two rules for "the vendor took it" must not say the same kind of
	// thing twice: two final answers would be two final receipts, and two
	// progress words two notices where the operator meant one.
	if p.OnAccepted != nil && p.Progress != nil {
		switch accProgress := p.OnAccepted.progress(); {
		case Final(p.Progress.Stat) && !accProgress:
			return fmt.Errorf("progress.stat: %s is a final answer, and on_accepted already gives one when the vendor takes the message — a client is told a final status once, so keep one of the two", p.Progress.Stat)
		case !Final(p.Progress.Stat) && accProgress:
			return fmt.Errorf("on_accepted.status: %s is a progress receipt, and the progress rule already sends one (%s) — keep one of the two", Word(p.OnAccepted.Status), p.Progress.Stat)
		}
	}
	if p.NoReceipt != nil {
		// A week: past that the message has usually left the hot store, and a
		// receipt that late is of no use to the customer's system anyway.
		if p.NoReceipt.AfterMin < 1 || p.NoReceipt.AfterMin > 7*24*60 {
			return fmt.Errorf("no_receipt.after_min: %d — it must be between 1 minute and 7 days (10080)", p.NoReceipt.AfterMin)
		}
		// Final only: this rule is what takes a message out of the sweep, and
		// a progress word would not.
		if err := p.NoReceipt.Action.validate("no_receipt", false); err != nil {
			return err
		}
	}
	return nil
}

// progress reports whether the action's answer is a progress word (ACCEPTD,
// ENROUTE) — a receipt that does not close the message. A split never is: its
// shares are final answers.
func (a *Action) progress() bool {
	if a == nil || len(a.Split) > 0 {
		return false
	}
	w := Word(a.Status)
	return w != "" && !Final(w)
}

// validate checks one action. A nil action is a rule that is simply not set.
// withProgress says whether the rule may answer with a progress word: the
// rules that only exist to close a message may not.
func (a *Action) validate(where string, withProgress bool) error {
	if a == nil {
		return nil
	}
	a.Status = strings.TrimSpace(a.Status)
	a.Err = strings.TrimSpace(a.Err)
	switch {
	case a.Status != "" && len(a.Split) > 0:
		return fmt.Errorf("%s: give a status or a split, not both", where)
	case a.Status == "" && len(a.Split) == 0:
		return fmt.Errorf("%s: needs a status to tell the client (%s)", where, choices(withProgress))
	}
	if a.Status != "" {
		w := Word(a.Status)
		switch {
		case w == "":
			return fmt.Errorf("%s.status: %q — it must be one of %s", where, a.Status, choices(withProgress))
		case !Final(w) && !withProgress:
			return fmt.Errorf("%s.status: %s is a progress receipt and does not close a message — this rule needs a final answer (%s)", where, w, choices(false))
		case !Final(w) && a.Err != "":
			return fmt.Errorf("%s.err: a progress receipt (%s) carries no error code — leave it empty", where, w)
		}
		a.Status = spelling(w)
		if err := checkErr(where, a.Err); err != nil {
			return err
		}
	}
	if len(a.Split) > 0 {
		if len(a.Split) > 10 {
			return fmt.Errorf("%s.split: %d parts is more than the 10 allowed", where, len(a.Split))
		}
		total := 0
		for i := range a.Split {
			s := &a.Split[i]
			s.Status = strings.TrimSpace(s.Status)
			s.Err = strings.TrimSpace(s.Err)
			w := Word(s.Status)
			switch {
			case w == "":
				return fmt.Errorf("%s.split[%d].status: %q — it must be one of %s", where, i, s.Status, choices(false))
			case !Final(w):
				// A split shares out how a message is CLOSED; a share that
				// leaves it open is a different rule (progress).
				return fmt.Errorf("%s.split[%d].status: %s is a progress receipt — a share must be a final answer (%s)", where, i, w, choices(false))
			}
			s.Status = spelling(w)
			if s.Pct < 1 || s.Pct > 100 {
				return fmt.Errorf("%s.split[%d].pct: %d — a share is between 1 and 100", where, i, s.Pct)
			}
			if err := checkErr(fmt.Sprintf("%s.split[%d]", where, i), s.Err); err != nil {
				return err
			}
			total += s.Pct
		}
		if total != 100 {
			return fmt.Errorf("%s.split: the shares add up to %d, not 100 — every message needs an answer", where, total)
		}
	}
	// One end given is a fixed wait, not an open range.
	if a.DelayMaxSec == 0 && a.DelayMinSec > 0 {
		a.DelayMaxSec = a.DelayMinSec
	}
	if a.DelayMinSec < 0 || a.DelayMaxSec < 0 {
		return fmt.Errorf("%s: a delay cannot be negative", where)
	}
	if a.DelayMinSec > a.DelayMaxSec {
		return fmt.Errorf("%s: delay_min_sec (%d) is after delay_max_sec (%d)", where, a.DelayMinSec, a.DelayMaxSec)
	}
	// A day. A receipt held longer than the client-push give-up window would
	// be swept away before it was ever delivered.
	if a.DelayMaxSec > 24*60*60 {
		return fmt.Errorf("%s: a receipt cannot be held back for more than a day (%d seconds asked)", where, a.DelayMaxSec)
	}
	return nil
}

// checkErr keeps an error code renderable in a receipt's `err:` field, which
// ends at the first space (dlr.errField).
func checkErr(where, code string) error {
	if code == "" {
		return nil
	}
	if strings.ContainsAny(code, " \t\r\n") {
		return fmt.Errorf("%s.err: %q — an error code cannot contain spaces, the receipt field ends at one", where, code)
	}
	if len(code) > 13 {
		return fmt.Errorf("%s.err: %q is longer than the 13 characters a receipt carries", where, code)
	}
	return nil
}

// ProgressOnAccept is the progress receipt to send when the vendor takes a
// message: its word (ACCEPTD or ENROUTE) and how long to hold it back. It
// comes from the progress rule, or from on_accepted when THAT answers with a
// progress word — the word decides what a receipt does, not the rule's name.
// ok=false = neither; the gateway's own ACCEPTD applies as before.
func (p *Policy) ProgressOnAccept() (stat string, delay time.Duration, ok bool) {
	if p == nil {
		return "", 0, false
	}
	var min, max int
	switch {
	case p.Progress != nil && !Final(Word(p.Progress.Stat)):
		stat, min, max = Word(p.Progress.Stat), p.Progress.DelayMinSec, p.Progress.DelayMaxSec
	case p.OnAccepted.progress():
		stat, min, max = Word(p.OnAccepted.Status), p.OnAccepted.DelayMinSec, p.OnAccepted.DelayMaxSec
	}
	if stat == "" {
		// No progress receipt of the account's own: the default's.
		return p.under.ProgressOnAccept()
	}
	secs := min
	if span := max - min; span > 0 {
		secs += intn(nil, span+1)
	}
	return stat, time.Duration(secs) * time.Second, true
}

// Submitted is the rule that CLOSES a message the vendor has just taken, and
// its name for told_by: on_accepted when it gives a final answer (or a split),
// else the progress rule when its word is a final one. nil = the client is
// not told the message is done at accept.
func (p *Policy) Submitted() (*Action, string) {
	if p == nil {
		return nil, ""
	}
	if a := p.OnAccepted; a != nil && !a.progress() {
		return a, RuleAccepted
	}
	if g := p.Progress; g != nil && Final(Word(g.Stat)) {
		return &Action{Status: spelling(Word(g.Stat)), DelayMinSec: g.DelayMinSec, DelayMaxSec: g.DelayMaxSec}, RuleProgress
	}
	if a, rule := p.under.Submitted(); a != nil {
		return a, ofDefault(rule)
	}
	return nil, ""
}

// Refused is the rule for a submit the vendor refused, given the code the
// record carries (SMPP_45, HTTP_400, VENDOR_REJECTED). The exact code wins,
// then a "*" row, then on_refused. nil = tell the client what really
// happened, exactly as before this feature existed. The answer may be a
// progress word: the client is then sent that receipt in place of the
// failure, and nothing is recorded as told (Outcome.Final).
//
// It is also the rule for one of the gateway's own outcomes (OwnCode, either
// name): only an exact row answers it, never "*" or on_refused, and its rule
// name is RuleGatewayCode. Where the account has no rule for the case, the
// default's applies (its rule name starts with DefaultPrefix).
func (p *Policy) Refused(code string) (*Action, string) {
	if p == nil {
		return nil, ""
	}
	own := OwnCode(code)
	code = cleanCode(code)
	var star *CodeRule
	for i := range p.SubmitCodes {
		r := &p.SubmitCodes[i]
		if r.Code == code {
			if own != "" {
				return &r.Action, RuleGatewayCode
			}
			return &r.Action, RuleSubmitCode
		}
		if r.Code == "*" && star == nil {
			star = r
		}
	}
	switch {
	case own != "":
		// "*" and on_refused are about a vendor's refusal.
	case star != nil:
		return &star.Action, RuleSubmitCode
	case p.OnRefused != nil:
		return p.OnRefused, RuleRefused
	}
	if a, rule := p.under.Refused(code); a != nil {
		return a, ofDefault(rule)
	}
	return nil, ""
}

// ForReceipt is the rule for a real receipt: st is what it maps to for us,
// rawStat the vendor's own word, errCode its error code. First match wins.
func (p *Policy) ForReceipt(st model.Status, rawStat, errCode string) *Action {
	a, _ := p.ForReceiptRule(st, rawStat, errCode)
	return a
}

// ForReceiptRule is ForReceipt with the rule's name: the account's rows are
// tried first, then the default's (DefaultPrefix + RuleReceipt).
func (p *Policy) ForReceiptRule(st model.Status, rawStat, errCode string) (*Action, string) {
	if p == nil {
		return nil, ""
	}
	for i := range p.Receipts {
		r := &p.Receipts[i]
		if !matches(r.Stat, string(st), rawStat) {
			continue
		}
		if !matchesErr(r.Err, errCode) {
			continue
		}
		return &r.Action, RuleReceipt
	}
	if a, rule := p.under.ForReceiptRule(st, rawStat, errCode); a != nil {
		return a, ofDefault(rule)
	}
	return nil, ""
}

// Timeout is the no-receipt rule and how long to wait, or nil.
func (p *Policy) Timeout() (*Action, time.Duration) {
	a, after, _ := p.TimeoutRule()
	return a, after
}

// TimeoutRule is Timeout with the rule's name: the account's own no-receipt
// rule, else the default's.
func (p *Policy) TimeoutRule() (*Action, time.Duration, string) {
	if p == nil {
		return nil, 0, ""
	}
	if p.NoReceipt == nil {
		if a, after, rule := p.under.TimeoutRule(); a != nil {
			return a, after, ofDefault(rule)
		}
		return nil, 0, ""
	}
	return &p.NoReceipt.Action, time.Duration(p.NoReceipt.AfterMin) * time.Minute, RuleNoReceipt
}

// matches compares a rule's stat against both spellings of what arrived.
func matches(want, canonical, raw string) bool {
	if want == "" || want == "*" {
		return true
	}
	return strings.EqualFold(want, canonical) || strings.EqualFold(want, raw)
}

// matchesErr compares a rule's error code. Empty matches every code — a rule
// on the status alone — and "*" says the same thing out loud.
func matchesErr(want, got string) bool {
	if want == "" || want == "*" {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(want), strings.TrimSpace(got))
}

// Decide turns a matched action into the one answer this message gets.
//
// rnd may be nil, which uses the shared source; tests pass their own so a
// split and a delay are repeatable.
func Decide(a *Action, rule string, rnd *rand.Rand) Outcome {
	if a == nil {
		return Outcome{}
	}
	said, errCode := a.Status, a.Err
	if len(a.Split) > 0 {
		roll := intn(rnd, 100) // 0..99
		acc := 0
		for _, s := range a.Split {
			acc += s.Pct
			if roll < acc {
				said, errCode = s.Status, s.Err
				break
			}
		}
		if said == "" { // shares that do not reach 100 cannot pass Validate
			last := a.Split[len(a.Split)-1]
			said, errCode = last.Status, last.Err
		}
	}
	var delay time.Duration
	if a.DelayMaxSec > 0 {
		span := a.DelayMaxSec - a.DelayMinSec
		secs := a.DelayMinSec
		if span > 0 {
			secs += intn(rnd, span+1)
		}
		delay = time.Duration(secs) * time.Second
	}
	word := Word(said)
	return Outcome{Status: StatusOf(word), Stat: word, Err: errCode, Delay: delay, Rule: rule}
}

func intn(rnd *rand.Rand, n int) int {
	if n <= 0 {
		return 0
	}
	if rnd != nil {
		return rnd.Intn(n)
	}
	return rand.Intn(n)
}

func oneOf(v string, list []string) bool {
	for _, x := range list {
		if v == x {
			return true
		}
	}
	return false
}
