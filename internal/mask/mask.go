// Package mask hides sensitive values in a message body before it is stored
// or shown back (PO 2026-09-08, closing the P1 "Masking" row).
//
// The rule is deliberately blunt: any run of digits at or over a threshold is
// replaced, wherever it appears. The PO chose this over masking only template
// variables, because most traffic is not templated and the sensitive parts —
// one-time codes, card fragments, account and reference numbers — are digits
// in every case. The accepted cost is that harmless numbers get masked too: an
// amount, a date, an order number.
//
// What is NOT touched: the message that goes to the handset. Masking applies
// where the body is KEPT or SHOWN — the stored record, the submit response,
// and the text a vendor echoes back in a receipt — never to what we send.
package mask

import (
	"strings"
	"unicode"
)

// DefaultMinDigits is the shortest run that gets masked. Four covers the
// common one-time code and the last-four of a card; going lower would mask
// almost every number in ordinary text, including years and small amounts.
const DefaultMinDigits = 4

// Digits replaces every run of minLen or more digits with the same number of
// asterisks. Same length rather than a fixed marker: support staff can still
// tell "a 6-digit code" from "a 16-digit number", which is often the whole
// question, and the body's length and segment count stay honest.
//
// Non-digits are untouched, so "Your code is 482913, valid 5 min" becomes
// "Your code is ******, valid 5 min" — the sentence still reads, and the
// secret is gone.
func Digits(s string, minLen int) string {
	if s == "" {
		return s
	}
	if minLen <= 0 {
		minLen = DefaultMinDigits
	}
	var b strings.Builder
	b.Grow(len(s))
	// Single pass, buffering only the digit run currently being read.
	var run []byte
	for _, r := range s {
		// ASCII digits only. Arabic-Indic and other numerals are left alone
		// deliberately: masking them would need a Unicode-aware rule, and
		// getting that half-right is worse than not doing it.
		if r < 128 && unicode.IsDigit(r) {
			run = append(run, byte(r))
			continue
		}
		run = writeRun(&b, run, minLen)
		b.WriteRune(r)
	}
	writeRun(&b, run, minLen)
	return b.String()
}

// writeRun flushes a buffered digit run, masked or verbatim, and returns an
// empty buffer.
func writeRun(b *strings.Builder, buf []byte, minLen int) []byte {
	if len(buf) == 0 {
		return buf
	}
	if len(buf) >= minLen {
		b.WriteString(strings.Repeat("*", len(buf)))
	} else {
		b.Write(buf)
	}
	return buf[:0]
}

// ReceiptText masks only the part of a delivery receipt AFTER `text:`.
//
// A receipt reads `id:12345 sub:001 dlvrd:001 submit date:2609081200 done
// date:2609081201 stat:DELIVRD err:000 text:Your code is 482913`. Masking
// digit runs across the whole string would destroy the vendor's message id,
// the counters and the timestamps — the very fields the receipt exists to
// carry, and the ones correlation depends on. Only the trailing text, which is
// where a vendor may echo the customer's message, is masked.
//
// A receipt with no `text:` field, or nothing after it, comes back unchanged.
func ReceiptText(receipt string, minLen int) string {
	i := strings.Index(receipt, "text:")
	if i < 0 {
		return receipt
	}
	head, tail := receipt[:i+len("text:")], receipt[i+len("text:"):]
	if tail == "" {
		return receipt
	}
	return head + Digits(tail, minLen)
}
