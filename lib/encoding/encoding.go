// Package encoding decides SMS encoding (GSM-7 / UCS-2 / Latin-1) and segment
// count, honouring an explicit preference string: "true"/"gsm7" force GSM-7,
// "false"/"ucs2" ask for UCS-2 (given only when the text needs it — GSM-7
// text is sent and billed as GSM-7, PO 2026-09-21), "latin1" forces Latin-1
// (ISO-8859-1), "" = auto.
package encoding

import (
	"errors"
	"strings"
	"unicode/utf8"
)

const (
	GSM7   = "gsm7"
	UCS2   = "ucs2"
	Latin1 = "latin1"
)

// ErrNonGSM7 is returned when gsm7=true but the text contains non-GSM-7 chars.
var ErrNonGSM7 = errors.New("encoding: text contains non-GSM-7 characters")

// ErrNonLatin1 is returned when encoding=latin1 is requested but the text
// contains a code point outside ISO-8859-1 (U+0000-U+00FF).
var ErrNonLatin1 = errors.New("encoding: text contains non-Latin-1 characters")

const gsm7Basic = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ ÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?" +
	"¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"

// gsm7Ext is the GSM 03.38 extension table: each of these is the escape 0x1B
// and one more septet on the wire, two septets in all. Form feed (ESC 0x0A)
// is one of them; it was left out until 19 Oct 2026, so a text with one went
// as UCS-2 (Saleem: "these characters carry one escape character with them").
const gsm7Ext = "^{}\\[~]|€\f"

func isGSM7(text string) bool {
	for _, r := range text {
		if !strings.ContainsRune(gsm7Basic, r) && !strings.ContainsRune(gsm7Ext, r) {
			return false
		}
	}
	return true
}

// IsGSM7 reports whether every rune fits the GSM 03.38 default alphabet
// (basic + extension tables). Exported for the validate stage's alphanumeric
// sender-ID charset check (chunk-4 ruling 4.6, 2026-08-03).
func IsGSM7(text string) bool { return isGSM7(text) }

func segments(units, single, multi int) int {
	if units <= single {
		return 1
	}
	return (units + multi - 1) / multi
}

// segmentsByCost counts parts the exact way the egress splitter slices them
// (greedy, rune-atomic — chunk-4 ruling 4.1): a 2-unit rune (astral pair,
// GSM-7 extension escape) never straddles a part boundary, so with an odd
// capacity like 67 the true part count can exceed the naive ceil — e.g. 100
// emoji are 200 units, ceil says 3 parts, but the wire fits only 33 emoji
// (66 units) per part → 4. Billing/rate-limit must count what actually
// leaves the gateway.
func segmentsByCost(text, enc string, single, multi int) int {
	if UnitLen(text, enc) <= single {
		return 1
	}
	segs, used := 1, 0
	for _, r := range text {
		c := UnitCost(r, enc)
		if used+c > multi {
			segs++
			used = 0
		}
		used += c
	}
	return segs
}

// isLatin1 reports whether every rune fits ISO-8859-1 (U+0000-U+00FF) — the
// 8-bit alphabet SMPP data_coding 0x03 already decodes on egress.
func isLatin1(text string) bool {
	for _, r := range text {
		if r > 0xFF {
			return false
		}
	}
	return true
}

// UnitCost is one rune's wire cost in enc's capacity unit: septets for GSM-7
// (extension chars cost 2), UTF-16 code units for UCS-2 (astral runes cost
// 2), bytes for Latin-1. Exported for the SMPP egress splitter, so part
// boundaries obey the same arithmetic segment planning uses. [chunk-4 ruling 4.1]
func UnitCost(r rune, enc string) int {
	switch enc {
	case GSM7:
		if strings.ContainsRune(gsm7Ext, r) {
			return 2
		}
		return 1
	case UCS2:
		if r > 0xFFFF {
			return 2
		}
		return 1
	default: // latin1: one byte per rune by construction
		return 1
	}
}

// UnitLen is UnitCost summed over a whole string.
func UnitLen(text, enc string) int {
	n := 0
	for _, r := range text {
		n += UnitCost(r, enc)
	}
	return n
}

// SplitByCost breaks text into the fewest chunks whose UnitCost each stays
// within limit, never splitting a rune's own cost across two chunks — the
// same wire-cost arithmetic UnitLen uses, so a part boundary never falls
// where the wire capacity actually would (an emoji or extension character
// mid-chunk). Shared by every multi-PDU splitter in the codebase: MT egress
// toward vendors (internal/process/smppvendor's own splitByCost, kept as a
// thin wrapper so its long-standing behaviour and tests are unchanged) and MO
// delivery toward a bound client (internal/process/smppserver, P6-B6,
// 2026-09-03) — one arithmetic implementation, not two that could drift.
func SplitByCost(text, enc string, limit int) []string {
	var chunks []string
	var cur []rune
	used := 0
	for _, r := range text {
		c := UnitCost(r, enc)
		if used+c > limit && len(cur) > 0 {
			chunks = append(chunks, string(cur))
			cur, used = nil, 0
		}
		cur = append(cur, r)
		used += c
	}
	if len(cur) > 0 {
		chunks = append(chunks, string(cur))
	}
	return chunks
}

// Plan returns the chosen encoding and segment count, honouring an explicit
// preference: "true"/"gsm7" forces GSM-7, "false"/"ucs2" forces UCS-2,
// "latin1" forces Latin-1 — each errors if the text can't fit that alphabet.
// "" auto-detects between GSM-7 and UCS-2 (Latin-1 is never chosen by
// auto-detect: it is strictly narrower than UCS-2 and only useful to a client
// that explicitly wants the smaller 8-bit encoding for its European-only
// text). [SCOPE P1-09]
func Plan(text, pref string) (enc string, segs int, err error) {
	switch strings.ToLower(strings.TrimSpace(pref)) {
	case "true", "1", "yes", GSM7:
		if !isGSM7(text) {
			return "", 0, ErrNonGSM7
		}
		return GSM7, segmentsByCost(text, GSM7, 160, 153), nil
	case "false", "0", "no", UCS2:
		// A client asking for UCS-2 on text GSM-7 can carry gets GSM-7 (PO
		// 2026-09-21): it reads the same on the handset, takes a third of the
		// parts, and is billed for those — 100 plain characters are 1 part,
		// not 2. Text that needs UCS-2 still gets it.
		if isGSM7(text) {
			return GSM7, segmentsByCost(text, GSM7, 160, 153), nil
		}
		return UCS2, segmentsByCost(text, UCS2, 70, 67), nil
	case Latin1:
		if !isLatin1(text) {
			return "", 0, ErrNonLatin1
		}
		// 8-bit alphabet: 140 bytes/1 char each single-segment, 134 once the
		// 6-byte UDH is subtracted for a concatenated part — same shape as
		// GSM-7's 160/153 and UCS-2's 70/67 pairs.
		return Latin1, segments(utf8.RuneCountInString(text), 140, 134), nil
	default:
		if isGSM7(text) {
			return GSM7, segmentsByCost(text, GSM7, 160, 153), nil
		}
		return UCS2, segmentsByCost(text, UCS2, 70, 67), nil
	}
}
