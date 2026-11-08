// Package abuse holds the async abuse-scoring logic used by the abuse-monitor
// process (P10/D-034): content normalization + leet folding for spam
// anti-evasion, and the per-signal severity evaluators. Pure functions — the
// process package owns the SQL and the action ladder. [SCOPE P10]
package abuse

import (
	"strings"
	"unicode"
)

// leetDigit maps the digits a sender uses in place of letters: l0an, ca5h.
// Always applied — a digit is already alphanumeric, so substituting one can
// never turn punctuation into a letter.
var leetDigit = map[rune]rune{
	'0': 'o', '1': 'i', '3': 'e', '4': 'a', '5': 's', '7': 't', '8': 'b',
}

// leetSymbol maps the punctuation a sender uses in place of letters: l@an,
// ca$h. Applied ONLY between two alphanumerics.
//
// The edge rule matters. Applied everywhere, "OFFER!!" became "offerii" —
// two exclamation marks read as letters — which then failed to match a rule on
// "loanoffer" because the word no longer ended where the keyword did. A
// trailing '!' is punctuation; an '!' wedged between two letters is somebody
// hiding a word.
var leetSymbol = map[rune]rune{
	'@': 'a', '$': 's', '€': 'e', '!': 'i', '+': 't',
}

// lookalike maps letters from other alphabets that are visually identical to
// Latin ones (PO 2026-09-09).
//
// This is the disguise a serious sender actually uses, because unlike l0an it
// is INVISIBLE — "lоan" with a Cyrillic о reads as "loan" to a human and is a
// completely different string to a computer. Until now the map above was
// called "leetspeak/homoglyph" and contained no homoglyphs at all, so a
// keyword rule missed every one of them.
//
// Deliberately the COMMON ones — Cyrillic and Greek letters that share a
// Latin glyph — rather than Unicode's full confusables table. The full table
// runs to thousands of entries and folds unrelated scripts together in ways
// that surprise people; these twenty-odd cover what is actually used to evade
// a keyword filter.
var lookalike = map[rune]rune{
	// Cyrillic
	'а': 'a', 'в': 'b', 'с': 'c', 'е': 'e', 'н': 'h', 'к': 'k', 'м': 'm',
	'о': 'o', 'р': 'p', 'т': 't', 'у': 'y', 'х': 'x', 'і': 'i', 'ѕ': 's',
	'ј': 'j', 'ԁ': 'd', 'ɡ': 'g',
	// Greek
	'α': 'a', 'β': 'b', 'ε': 'e', 'ι': 'i', 'κ': 'k', 'ν': 'v', 'ο': 'o',
	'ρ': 'p', 'τ': 't', 'υ': 'u', 'χ': 'x', 'ϲ': 'c',
}

// Normalize folds a body to its evasion-resistant form: lowercase, leet and
// lookalike substitution, then the disguises inside a word removed — dots,
// dashes, zero-width characters and the like all vanish.
//
// WHAT IT DELIBERATELY DOES NOT DO, since 2026-09-09: glue two real words
// together. The previous version dropped every separator including spaces, so
// "Hello an update" became "helloanupdate" — which contains "loan", and an
// innocent message was refused by a rule on that word. That false positive is
// why enforce_spam_strict was opt-in, and why it was on for zero of twenty-six
// accounts: the protection existed and nobody could afford to use it.
//
// The rule now: a separator is removed when it is NOT a plain space, or when
// it sits beside a single letter. Both are the signature of deliberate
// spacing — "l.o.a.n", "l o a n" — while an ordinary space between two
// multi-letter words is a real boundary and is kept.
//
//	"l0an offer"       -> "loan offer"
//	"l.o.a.n offer"    -> "loan offer"
//	"l o a n offer"    -> "loan offer"
//	"lоan offer"       -> "loan offer"     (Cyrillic о)
//	"Hello an update"  -> "hello an update"   (unchanged: a real boundary)
//
// A keyword's own spaces are matched flexibly by MatchesKeyword, so a rule on
// "loan offer" still catches "loanoffer".
func Normalize(s string) string {
	// Pass 1: classify every rune, deciding substitutions with the context
	// that makes them safe. Separators are kept for now, because whether to
	// drop a space depends on its neighbours.
	runes := []rune(strings.ToLower(s))
	hasLetter := wordHasLetter(runes)
	var toks []tok
	for i, r := range runes {
		if isSpace(r) {
			toks = append(toks, tok{r: ' ', sep: true, space: true})
			continue
		}
		f := foldRune(runes, i, hasLetter[i])
		if unicode.IsLetter(f) || unicode.IsDigit(f) {
			toks = append(toks, tok{r: f})
			continue
		}
		toks = append(toks, tok{r: f, sep: true}) // punctuation, zero-width
	}

	// Pass 2: emit letters, and decide each space. runLen tells us how many
	// letters the current unbroken word has, which is what distinguishes
	// "l o a n" from "hello an".
	var b strings.Builder
	b.Grow(len(s))
	pendingSpace := false
	runLen := 0
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if !t.sep {
			if pendingSpace {
				b.WriteByte(' ')
				pendingSpace = false
				runLen = 0
			}
			b.WriteRune(t.r)
			runLen++
			continue
		}
		if !t.space {
			continue // punctuation and zero-width always vanish
		}
		// Keep a space only when it separates two REAL words: more than one
		// letter before it and more than one after. Anything else is
		// deliberate spacing and is closed up.
		if runLen == 1 || nextRunLen(toks, i+1) == 1 {
			continue
		}
		if b.Len() > 0 {
			pendingSpace = true
		}
	}
	return b.String()
}

func isSpace(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' }

// wordHasLetter reports, for each rune position, whether the whitespace-
// delimited word it belongs to contains at least one letter.
//
// This is what stops a genuine number being folded into letters. Digits stand
// in for letters only inside a word that has letters — "l0an" is somebody
// hiding "loan"; "100" is one hundred, and turning it into "ioo" invents a
// word that was never written. An amount, an OTP or a reference number should
// pass through untouched.
func wordHasLetter(rs []rune) []bool {
	out := make([]bool, len(rs))
	i := 0
	for i < len(rs) {
		if isSpace(rs[i]) {
			i++
			continue
		}
		j, letter := i, false
		for j < len(rs) && !isSpace(rs[j]) {
			// Lookalikes count: a Cyrillic о IS a letter for this purpose.
			if _, ok := lookalike[rs[j]]; ok || unicode.IsLetter(rs[j]) {
				letter = true
			}
			j++
		}
		for k := i; k < j; k++ {
			out[k] = letter
		}
		i = j
	}
	return out
}

// foldRune applies the substitution tables to one rune, given its position.
func foldRune(rs []rune, i int, wordHasLetter bool) rune {
	r := rs[i]
	if m, ok := lookalike[r]; ok {
		return m
	}
	if m, ok := leetDigit[r]; ok && wordHasLetter {
		return m
	}
	if m, ok := leetSymbol[r]; ok && betweenAlnum(rs, i) {
		return m
	}
	return r
}

// betweenAlnum reports whether the rune at i has a letter or digit on both
// sides — the shape of a symbol standing in for a letter, as against one being
// ordinary punctuation at the edge of a word.
func betweenAlnum(rs []rune, i int) bool {
	if i == 0 || i+1 >= len(rs) {
		return false
	}
	prev, next := rs[i-1], rs[i+1]
	return (unicode.IsLetter(prev) || unicode.IsDigit(prev)) &&
		(unicode.IsLetter(next) || unicode.IsDigit(next))
}

// tok is one classified rune of the input: a letter/digit, a space, or other
// punctuation. Kept as a type rather than three parallel slices because the
// second pass needs to look both ways from a separator.
type tok struct {
	r     rune
	sep   bool
	space bool
}

// nextRunLen counts the unbroken letters starting at i, ignoring the
// punctuation that is about to be stripped anyway.
func nextRunLen(toks []tok, i int) int {
	n := 0
	for ; i < len(toks); i++ {
		if toks[i].space {
			break
		}
		if !toks[i].sep {
			n++
		}
	}
	return n
}

// A digit-preserving variant (NormalizeKeepNumbers) existed here but was never
// wired into any scoring path; plain Normalize was judged sufficient across
// the live suite and the variant was REMOVED (PO ruling 2026-07-26,
// B-P10-02). If digit-bearing keywords ever matter, resurrect from git
// history rather than re-deriving the neighbour-run logic.

// HitsKeyword reports whether a normalized body contains a normalized
// keyword. Both sides must be pre-normalized with Normalize.
//
// Two ways to hit, and the second is where the care is.
//
//  1. The keyword appears in the body as written. Covers everything Normalize
//     has already folded together — l0an, l.o.a.n, lоan, l o a n.
//
//  2. The keyword appears once the body's word gaps are closed up — so a rule
//     written "loanoffer" still catches "loan offer", and a rule written
//     "loan offer" still catches "loanoffer". An operator should not have to
//     guess how a sender spaced it.
//
// The second is exactly what used to make this unusable: closing "Hello an
// update" into "helloanupdate" made it match a rule on "loan". So a closed-up
// match counts ONLY when it consumes whole words at both ends. "loanoffer"
// covers all of "loan" and all of "offer" — a real hit. "loan" inside
// "helloanupdate" starts in the middle of "hello" — not a hit.
func HitsKeyword(bodyNorm, keywordNorm string) bool {
	if keywordNorm == "" {
		return false
	}
	if strings.Contains(bodyNorm, keywordNorm) {
		return true
	}
	return closedUpHit(bodyNorm, keywordNorm)
}

// closedUpHit looks for the keyword in the body with all word gaps removed,
// accepting only a match that starts where a word starts and ends where a word
// ends.
func closedUpHit(bodyNorm, keywordNorm string) bool {
	body, starts, ends := closeUp(bodyNorm)
	kw := despace(keywordNorm)
	if kw == "" || len(kw) > len(body) {
		return false
	}
	for i := 0; i+len(kw) <= len(body); i++ {
		if body[i:i+len(kw)] != kw {
			continue
		}
		if starts[i] && ends[i+len(kw)-1] {
			return true
		}
	}
	return false
}

// closeUp removes the spaces from a normalized body and reports, for each
// remaining byte, whether it began a word and whether it ended one. Those two
// marks are what let a closed-up match tell "loan offer" from "hello an".
func closeUp(s string) (closed string, isStart, isEnd []bool) {
	var b strings.Builder
	b.Grow(len(s))
	atStart := true
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' {
			atStart = true
			if n := len(isEnd); n > 0 {
				isEnd[n-1] = true
			}
			continue
		}
		b.WriteByte(s[i])
		isStart = append(isStart, atStart)
		isEnd = append(isEnd, false)
		atStart = false
	}
	if n := len(isEnd); n > 0 {
		isEnd[n-1] = true // the last word ends at the end of the string
	}
	return b.String(), isStart, isEnd
}

func despace(s string) string { return strings.ReplaceAll(s, " ", "") }
