package dlr

import (
	"math/big"
	"strings"
)

// msgIDCandidates returns the set of forms a vendor's message id might take when
// the DLR's `id:` field and the original submit_sm_resp id differ only in
// REPRESENTATION, not value. This is the classic SMPP interop trap: a vendor
// returns the id as hex in submit_sm_resp and as decimal in the receipt (or the
// reverse), or with different zero-padding — and a byte-for-byte correlation
// then silently fails, stranding the message at `sent` forever.
//
// The approach is deliberately NON-LOSSY: we do not canonicalise the stored id
// (which could collide two distinct ids). Instead, at receipt time we generate
// the plausible equivalent forms of the INCOMING id and try each against what is
// stored. The original value is always first, so an exact match short-circuits
// and unusual ids are never mangled.
//
// Order matters — most-likely first, exact always first:
//  1. the raw value (exact match, the common case)
//  2. trimmed of surrounding whitespace
//  3. the same id in the other letter case     ("cb5bff728e" for "CB5BFF728E")
//  4. hex→decimal  (id was stored decimal, receipt is hex)   e.g. "1A2B" → "6699"
//  5. decimal→hex  (id was stored hex, receipt is decimal)   e.g. "6699" → "1a2b", "1A2B"
//  6. leading-zeros stripped, of the id and of each form above
//
// 2026-10-12 (go-live test P3 B3, Saleem: "i will get submit_sm or deliver_sm
// either of them in decimal and other in hexadecimal, they should also
// match"). Both directions were tried already, but three things made a pair
// miss: the hex form was written in lower case only, and his SMSC answers in
// UPPER case ("CB5BFF728E") — the receipts kept in Redis are looked up byte
// for byte, so that never matched there; an id longer than 16 hex digits (or
// 20 decimal ones) did not fit in 64 bits and was not converted at all; and a
// converted form was not tried without its leading zeros.
//
// De-duplicated, order-preserving. Empty input yields no candidates.
func msgIDCandidates(id string) []string {
	if id == "" {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	add := func(s string) {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	// Each form, then the same without its leading zeros (one digit is kept).
	addBoth := func(s string) {
		add(s)
		if trimmed := strings.TrimLeft(s, "0"); trimmed != "" && trimmed != s {
			add(trimmed)
		}
	}

	add(id)
	t := strings.TrimSpace(id)
	add(t)

	// strip an optional 0x/0X prefix before numeric interpretation
	hexBody := t
	if len(hexBody) > 2 && (hexBody[:2] == "0x" || hexBody[:2] == "0X") {
		hexBody = hexBody[2:]
		add(hexBody)
	}
	// The same HEX id written in the other case. Only a hex id: every form is
	// one more look for a receipt that matches nothing, and an id of another
	// kind ("V-1", a UUID with dashes) does not change case between a
	// vendor's two answers.
	if isHex(hexBody) && hasLetters(hexBody) {
		add(strings.ToUpper(hexBody))
		add(strings.ToLower(hexBody))
	}

	// hex → decimal: only when the body is valid hex.
	if isHex(hexBody) {
		if n, ok := new(big.Int).SetString(hexBody, 16); ok {
			add(n.Text(10))
		}
	}
	// decimal → hex, in both letter cases: nothing says which one the vendor
	// used at submit.
	if isDecimal(t) {
		if n, ok := new(big.Int).SetString(t, 10); ok {
			h := n.Text(16)
			add(h)
			add(strings.ToUpper(h))
		}
	}
	// leading zeros stripped (e.g. "000123" → "123"); keep at least one digit
	for _, form := range append([]string(nil), out...) {
		addBoth(form)
	}
	return out
}

func hasLetters(s string) bool {
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			return true
		}
	}
	return false
}

func isHex(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}

func isDecimal(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
