package logging

// Process 13 / P1 register 1.9.2 — no OTP or PII in plaintext logs.
//
// Everything that reaches a log file goes through Logger.write, including the
// old Printf call sites, so this is the one place a rule can be applied
// without hunting call sites forever. The alternative — auditing every
// Printf in the engine and trusting nobody adds another — is how the leak
// happened in the first place.
//
// FLOW CAPTURES ARE DELIBERATELY EXEMPT. A capture is a time-boxed diagnostic
// an operator turns on for one account, and its whole value is seeing the
// real message text (migration config/0084 carves that out explicitly, and
// says what keeps it acceptable). Captures reach their files through
// CaptureRecord/CaptureTo, which never call write, so they are unaffected by
// anything here. The one exception is a SYSTEM capture, which is fed from
// write and therefore sees the redacted line — correct, because a system
// capture is not scoped to the account whose data it would be showing.
//
// Two thresholds, because one number cannot serve both jobs:
//
//   - Message content (a field named text/body/content) is masked at FOUR
//     digits, the same rule the PO chose for stored bodies. A one-time code
//     is four to six digits; anything higher lets the thing we are most
//     trying to keep out of logs straight through.
//   - Everything else, including the plain msg sentence, is masked at SEVEN.
//     Ordinary log lines are full of counters, ports, sizes, row counts and
//     durations, and masking those makes a log unreadable while protecting
//     nothing. Seven is below every E.164 subscriber number and above the
//     numbers an engine talks about itself with.
//
// Named secrets are removed outright rather than masked: a password or token
// is not made safe by hiding its digits.

import (
	"strings"

	"github.com/yakugate/yakugate/internal/mask"
)

// contentMinDigits is the threshold for a field carrying message text — low
// enough to catch a one-time code.
const contentMinDigits = mask.DefaultMinDigits

// generalMinDigits is the threshold everywhere else: long enough not to eat
// the counters and ids an engine logs about itself, short enough that no
// subscriber number survives.
const generalMinDigits = 7

// redactedValue replaces a secret entirely. Not asterisks of the same length:
// the length of a password is itself worth nothing to support and something
// to an attacker.
const redactedValue = "[redacted]"

// contentFields carry a message the customer wrote. Masked at the lower
// threshold.
var contentFields = map[string]bool{
	"text": true, "body": true, "content": true, "message": true, "sms": true,
}

// secretFields carry credential material and are dropped, never masked.
var secretFields = map[string]bool{
	"password": true, "passwd": true, "pass": true, "secret": true,
	"token": true, "jwt": true, "apikey": true, "api_key": true, "apiKey": true,
	"authorization": true, "auth": true, "credential": true, "credentials": true,
	"privatekey": true, "private_key": true, "sessionkey": true, "session_key": true,
}

// redactEnabled is set once at configure time. Redaction is ON by default: a
// deployment has to opt OUT deliberately, because the failure mode of
// forgetting to opt in is subscriber numbers sitting in a log file for the
// retention period.
var redactEnabled = true

// SetRedaction turns log redaction on or off process-wide. Off is for a
// debugging session on a deployment with no real traffic; it is never the
// right setting where customer messages flow.
func SetRedaction(on bool) { redactEnabled = on }

// RedactionEnabled reports the current setting, so a start-up line can say
// plainly which way this process is running.
func RedactionEnabled() bool { return redactEnabled }

// redactMessage cleans the human sentence of a record — the destination in a
// Printf line, most often.
func redactMessage(s string) string {
	if !redactEnabled || s == "" {
		return s
	}
	return maskNumbers(s, generalMinDigits)
}

// maskNumbers masks every run of at least min ASCII digits that stands as a
// number of its own, and leaves alone a run that is part of a word: one with
// a letter right before or after it, or after a letter and a '-' or '_'
// ("01M5770428NBVJ…", "ZV1234567890-716", "MAIN-1234567"), and the digits
// after a decimal point ("0.1234567").
//
// It was mask.Digits over the whole sentence, so seven digits inside a
// message id or a vendor's id were masked like a subscriber's number, and an
// operator could not search a log for a message (go-live test of 18 Oct 2026,
// P3, P4 D6, P7, P10 A6: "dead-lettered msg=01M576AVT*******EJASQXA9CM", a
// ratio written "0.****************"). A number — "923001234567",
// "+923001234567", "to=923001234567", "923001234567->8005", "0300-1234567" —
// is still masked. A vendor id made of digits alone cannot be told from a
// number, and stays masked.
func maskNumbers(s string, min int) string {
	var b strings.Builder
	last := 0 // what is written of s so far
	for i := 0; i < len(s); {
		if !isASCIIDigit(s[i]) {
			i++
			continue
		}
		j := i
		for j < len(s) && isASCIIDigit(s[j]) {
			j++
		}
		if j-i >= min && !inWord(s, i, j) {
			if b.Len() == 0 {
				b.Grow(len(s))
			}
			b.WriteString(s[last:i])
			b.WriteString(strings.Repeat("*", j-i))
			last = j
		}
		i = j
	}
	if last == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

// inWord reports whether the digits s[i:j] belong to an identifier or a
// decimal fraction rather than standing as a number (maskNumbers).
func inWord(s string, i, j int) bool {
	if i > 0 && isASCIILetter(s[i-1]) || j < len(s) && isASCIILetter(s[j]) {
		return true
	}
	if i > 1 && (s[i-1] == '-' || s[i-1] == '_') && isASCIILetter(s[i-2]) {
		return true
	}
	return i > 1 && s[i-1] == '.' && isASCIIDigit(s[i-2])
}

func isASCIIDigit(c byte) bool  { return c >= '0' && c <= '9' }
func isASCIILetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }

// redactFields returns a cleaned copy of f. A COPY: the caller's map may be
// reused, or belong to a struct that is written elsewhere, and quietly
// rewriting someone else's data is a worse bug than the one being fixed.
//
// Returns f itself when nothing needed changing, so the common case of a
// record with no sensitive field allocates nothing.
func redactFields(f Fields) Fields {
	if !redactEnabled || len(f) == 0 {
		return f
	}
	var out Fields
	for k, v := range f {
		nv, changed := redactValue(k, v)
		if !changed {
			continue
		}
		if out == nil {
			out = make(Fields, len(f))
			for ck, cv := range f {
				out[ck] = cv
			}
		}
		out[k] = nv
	}
	if out == nil {
		return f
	}
	return out
}

// redactValue applies the rule for one field, reporting whether it changed
// anything so the caller can avoid copying the map for nothing.
func redactValue(key string, v any) (any, bool) {
	lk := strings.ToLower(key)
	if secretFields[lk] {
		return redactedValue, true
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return v, false
	}
	// Message text keeps the whole rule: a one-time code inside a word is
	// still a code. Every other field is read as the sentence is.
	m := maskNumbers(s, generalMinDigits)
	if contentFields[lk] {
		m = mask.Digits(s, contentMinDigits)
	}
	if m == s {
		return v, false
	}
	return m, true
}
