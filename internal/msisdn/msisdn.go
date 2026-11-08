// Package msisdn holds destination-number normalization — a BUSINESS rule
// shared by both ingress edges (D-064 direction, PO 2026-07-26): a number's
// validity must not depend on whether the client used HTTP or SMPP. Pipeline
// stage 1 (validate) calls Clean for every message; the HTTP adapter also
// calls it during recipient splitting because cross-format fan-out de-dupe
// must compare normalized values before the pipeline runs.
package msisdn

import (
	"strings"
	"unicode"
)

// skimmable reports formatting junk clients commonly paste around a phone
// number: '+', dots, dashes, parentheses, any whitespace (incl. non-breaking),
// and the invisible direction/zero-width marks that ride along when copying
// from RTL chats or spreadsheets. Letters and other symbols are NOT skimmed —
// genuine garbage must still fail validation instead of being silently
// mutated.
func skimmable(r rune) bool {
	switch r {
	case '+', '.', '-', '(', ')':
		return true
	case '\u200b', '\u200c', '\u200d', '\u200e', '\u200f', // zero-width, ZWNJ/ZWJ, LRM/RLM
		'\u202a', '\u202b', '\u202c', '\u202d', '\u202e', // directional embeddings/overrides
		'\u2060', '\ufeff': // word-joiner, BOM
		return true
	}
	return unicode.IsSpace(r)
}

// Clean normalizes one number, in this order: 1) skim junk (any count, any
// position), 2) convert Eastern Arabic (٠-٩) and Urdu (۰-۹) digits to ASCII,
// 3) trim leading zeros (the international "00…" prefix — E.164 country codes
// never start with 0; zeros INSIDE the number are kept). A token that is all
// zeros is returned as-is so it fails validation instead of silently
// disappearing.
//
// Clean is the lenient form, for matching numbers kept in reference data
// (blocklists, prefixes, number ranges). A number a message is sent TO goes
// through Destination, which does not read a national number as an
// international one.
func Clean(tok string) string {
	digits := normalize(tok)
	if trimmed := strings.TrimLeft(digits, "0"); trimmed != "" {
		return trimmed
	}
	return digits
}

// Destination cleans a number a message is sent TO. It is Clean with one
// difference: a number written in NATIONAL form — one leading 0 (the trunk
// prefix) and then a non-zero digit, as in 03001234567 — keeps its 0, so the
// validate stage refuses it (see National). Clean would drop the 0 and read
// the rest as a full international number with a different country code
// (03001234567 → 3001234567, a Greek number): the gateway adds no country
// code, so it must not pretend the number has one (BUG-YK-API-001/SND-001,
// 2026-10-01). Two or more leading zeros are an international prefix (00,
// 000) and are dropped as before. Destination(Destination(x)) ==
// Destination(x), so the HTTP adapter and the validate stage may both call it.
func Destination(tok string) string {
	digits := normalize(tok)
	if national(digits) {
		return digits
	}
	if trimmed := strings.TrimLeft(digits, "0"); trimmed != "" {
		return trimmed
	}
	return digits
}

// National reports a number written in national form: after skimming junk,
// exactly one leading 0 followed by a digit 1-9 (03001234567, +0300…,
// "0300 1234567"). Such a number has no country code and cannot be sent.
func National(tok string) bool { return national(normalize(tok)) }

func national(digits string) bool {
	return len(digits) >= 2 && digits[0] == '0' && digits[1] >= '1' && digits[1] <= '9'
}

// normalize skims junk and converts Arabic/Urdu digits, keeping every other
// character (so validation can refuse it) and every zero.
func normalize(tok string) string {
	var b strings.Builder
	for _, r := range tok {
		switch {
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r >= '٠' && r <= '٩': // Eastern Arabic-Indic digits
			b.WriteRune('0' + (r - '٠'))
		case r >= '۰' && r <= '۹': // Extended Arabic-Indic (Urdu)
			b.WriteRune('0' + (r - '۰'))
		case skimmable(r):
			// dropped
		default:
			b.WriteRune(r) // unknown char kept → validation rejects it
		}
	}
	return b.String()
}
