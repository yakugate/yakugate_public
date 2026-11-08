// Package senderid is the rule for what a sender (the "from" of an SMS) may
// look like. One rule for every Go door: the accept path's validate stage
// (internal/pipeline, both HTTP and SMPP) and the console
// (internal/provision). The Admin API mirrors it — services/defaultSender.js
// wellFormedSender and routes/senderIdRoutes.js validSender — so the three
// must change together.
//
// A sender is either
//   - a NUMBER: digits, optionally after one leading '+', up to 15 digits
//     (the E.164 maximum); or
//   - a NAME (alphanumeric): up to 11 characters (the GSM 03.40 alphanumeric
//     originator limit), made only of letters A-Z / a-z, digits 0-9, space
//     and the three marks . - _
//
// The name set is what operators accept for an alphanumeric sender, and the
// set the Admin API has always let a sender ID be registered with. It used to
// be "any GSM-7 character", which let @ # $ % & ! ? and the like through to
// vendors that refuse them (BUG-YK-API-003, 2026-10-01).
package senderid

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

// Default limits; the pipeline may raise or lower them with
// YAKU_SENDER_MAX_ALPHA / YAKU_SENDER_MAX_NUMERIC.
const (
	DefaultMaxAlpha   = 11
	DefaultMaxNumeric = 15
)

// NameMarks are the only non-alphanumeric characters a name sender may use,
// besides space.
const NameMarks = ".-_"

// Numeric reports a sender that is a number: one or more digits, optionally
// after a single leading '+'.
func Numeric(s string) bool {
	d := strings.TrimPrefix(s, "+")
	if d == "" {
		return false
	}
	for i := 0; i < len(d); i++ {
		if d[i] < '0' || d[i] > '9' {
			return false
		}
	}
	return true
}

// NameChar reports a character a name (alphanumeric) sender may use.
func NameChar(r rune) bool {
	switch {
	case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == ' ':
		return true
	}
	return strings.ContainsRune(NameMarks, r)
}

// Problem says, in plain words, why s cannot be a sender — "" when it can.
// The pipeline puts it after "INVALID_SENDER: ".
func Problem(s string, maxAlpha, maxNumeric int) string {
	if strings.TrimSpace(s) == "" {
		return "sender is empty"
	}
	if Numeric(s) {
		if n := len(strings.TrimPrefix(s, "+")); n > maxNumeric {
			return "numeric sender exceeds " + strconv.Itoa(maxNumeric) + " digits"
		}
		return ""
	}
	if utf8.RuneCountInString(s) > maxAlpha {
		return "alphanumeric sender exceeds " + strconv.Itoa(maxAlpha) + " chars"
	}
	for _, r := range s {
		if !NameChar(r) {
			return "alphanumeric sender may use only letters A-Z, digits, space and . - _ (not " + strconv.QuoteRune(r) + ")"
		}
	}
	return ""
}

// WellFormed reports a sender the gateway accepts under the default limits.
func WellFormed(s string) bool { return Problem(s, DefaultMaxAlpha, DefaultMaxNumeric) == "" }
