package abuse

import (
	"strings"
	"unicode"
)

// Formatting anomaly — P10 register row "Formatting anomaly".
//
// Spam often LOOKS different before you read a word of it: shouting in
// capitals, runs of exclamation marks, emoji walls, and padding with invisible
// characters to break up a word a filter would otherwise catch.
//
// Two rulings shape this (PO 2026-09-09).
//
// BASELINE-RELATIVE, not absolute thresholds. The question is not "is this
// message loud" but "is this account suddenly louder than it has ever been".
// A customer whose house style is SHOUTY ALL-CAPS ALERTS is not abusive, and a
// fixed 70%-capitals rule would flag them every single day while teaching us
// nothing. A customer who has sent calm sentences for a year and starts
// shouting today has either changed campaign or lost control of their account,
// and that is worth a look.
//
// SCRIPT-AWARE. Urdu and Arabic have no upper case at all, so a capitals ratio
// is not merely less useful there — it is structurally zero. Scoring it anyway
// would quietly turn this into "English marketing looks suspicious, Urdu spam
// does not", which is worse than having no signal, because it looks like
// coverage. So the capitals component is only measured on text that HAS cased
// letters, and its absence is not evidence of anything.
//
// Padding is the strongest and cheapest part: zero-width joiners, repeated
// invisible characters and unusual whitespace have no honest use in an SMS.
// Unlike caps and punctuation it is judged absolutely, because there is no
// legitimate baseline for it to deviate from.

// Shape is the measurable formatting of one body, all values 0..1.
type Shape struct {
	// Caps is capital letters as a share of cased letters. Meaningless when
	// Cased is false — see the script rule above.
	Caps  float64
	Cased bool
	// Punct is runs of two or more of ! ? . as a share of the text.
	Punct float64
	// Emoji is emoji and symbol runes as a share of runes.
	Emoji float64
	// Padding is invisible and zero-width characters as a share of runes.
	// Judged absolutely, not against a baseline.
	Padding float64
}

// Measure describes one message's formatting.
func Measure(body string) Shape {
	var s Shape
	if body == "" {
		return s
	}
	var runes, cased, upper, emoji, invisible, punctRun, punctChars int
	run := 0
	for _, r := range body {
		runes++
		switch {
		case isInvisible(r):
			invisible++
		case unicode.IsUpper(r):
			cased++
			upper++
		case unicode.IsLower(r):
			cased++
		case unicode.IsSymbol(r) || (r >= 0x1F000 && r <= 0x1FAFF):
			emoji++
		}
		if r == '!' || r == '?' || r == '.' {
			run++
			if run >= 2 {
				punctRun++
			}
		} else {
			if run >= 2 {
				punctChars += run
			}
			run = 0
		}
	}
	if run >= 2 {
		punctChars += run
	}
	_ = punctRun
	if runes == 0 {
		return s
	}
	// Cased only when there is enough cased text to mean anything. A single
	// stray Latin letter in an Urdu message must not make it "100% capitals".
	if cased >= 8 {
		s.Cased = true
		s.Caps = float64(upper) / float64(cased)
	}
	s.Punct = float64(punctChars) / float64(runes)
	s.Emoji = float64(emoji) / float64(runes)
	s.Padding = float64(invisible) / float64(runes)
	return s
}

// isInvisible covers the characters that break a word up without showing
// anything: zero-width space/joiner/non-joiner, the BOM, directional marks and
// the non-breaking space. abuse.Normalize already strips this class for
// matching; here we COUNT it, because its presence is itself the signal.
func isInvisible(r rune) bool {
	switch r {
	case '\u200b', // zero-width space
		'\u200c', // zero-width non-joiner
		'\u200d', // zero-width joiner
		'\ufeff', // BOM / zero-width no-break space
		'\u2060', // word joiner
		'\u200e', // left-to-right mark
		'\u200f', // right-to-left mark
		'\u00a0', // non-breaking space
		'\u180e': // Mongolian vowel separator
		return true
	}
	return false
}

// FormattingParams tune the signal.
type FormattingParams struct {
	Common
	// MinBaselineMsgs is how much history an account needs before its baseline
	// means anything. Below it the signal returns nothing rather than guessing
	// — the same lesson as the spend signal's min_baseline: a new customer's
	// first campaign must not be judged against a baseline built from three
	// messages.
	MinBaselineMsgs int `json:"min_baseline_msgs"`
	// MinWindowMsgs is the same guard on the other side: a couple of loud
	// messages are not a trend.
	MinWindowMsgs int `json:"min_window_msgs"`
	// DeltaWarn is how far above baseline a component must move to score at
	// all, as an absolute difference in ratio (0.30 = thirty points of ratio).
	DeltaWarn float64 `json:"delta_warn"`
	// DeltaFull is the deviation that scores full strength.
	DeltaFull float64 `json:"delta_full"`
	// PaddingFull is the share of invisible characters that scores full
	// strength on its own. Absolute, because no honest baseline exists.
	PaddingFull float64 `json:"padding_full"`
	// BaselineHours is how far back the account's own norm is taken from
	// (the hours before the window).
	BaselineHours int `json:"baseline_hours"`
	// SampleMsgs caps the bodies read per account for each of the window and
	// the baseline, so one busy account cannot make a sweep read its whole
	// day; ScanLimit caps the sweep as a whole.
	SampleMsgs int `json:"sample_msgs"`
	ScanLimit  int `json:"scan_limit"`
}

// Validate names the first key this rule cannot run without.
func (p FormattingParams) Validate() error {
	if err := needWindow(p.Common); err != nil {
		return err
	}
	return needBaseline(p.BaselineHours)
}

// EvalFormatting returns 0..1 strength for the score engine — never a
// Severity, because this signal does not act alone.
//
// window and baseline are the AVERAGED shapes over their periods.
func EvalFormatting(window, baseline Shape, windowMsgs, baselineMsgs int, p FormattingParams) float64 {
	// Padding first: it is judged absolutely and needs no history, so it can
	// fire on a brand-new account, which is exactly when someone testing an
	// evasion technique would show up.
	pad := 0.0
	if p.PaddingFull > 0 && window.Padding > 0 {
		pad = window.Padding / p.PaddingFull
		if pad > 1 {
			pad = 1
		}
	}
	if windowMsgs < p.MinWindowMsgs || baselineMsgs < p.MinBaselineMsgs {
		return pad // not enough history to compare; padding still counts
	}
	worst := pad
	// Caps ONLY when both sides actually had cased text. If either is
	// uncased the comparison is meaningless, not zero — see the script rule.
	if window.Cased && baseline.Cased {
		if d := deviation(window.Caps, baseline.Caps, p); d > worst {
			worst = d
		}
	}
	if d := deviation(window.Punct, baseline.Punct, p); d > worst {
		worst = d
	}
	if d := deviation(window.Emoji, baseline.Emoji, p); d > worst {
		worst = d
	}
	return worst
}

// deviation scales an upward move from baseline into 0..1. Only upward: an
// account that got QUIETER is not a concern.
func deviation(now, base float64, p FormattingParams) float64 {
	d := now - base
	if d <= 0 || p.DeltaWarn <= 0 || p.DeltaFull <= p.DeltaWarn {
		return 0
	}
	if d < p.DeltaWarn {
		return 0
	}
	s := (d - p.DeltaWarn) / (p.DeltaFull - p.DeltaWarn)
	if s > 1 {
		s = 1
	}
	return s
}

// HasCased reports whether text carries enough cased letters for a capitals
// ratio to mean anything — exported so a caller can explain in a report why a
// message was not judged on capitals.
func HasCased(body string) bool { return Measure(body).Cased }

// AverageShape averages the shapes of many bodies, carrying Cased forward only
// when enough of them actually had cased text. Without that an account sending
// mostly Urdu with a few English messages would be compared on a capitals
// ratio derived from almost nothing.
func AverageShape(bodies []string) (Shape, int) {
	var sum Shape
	var n, cased int
	for _, b := range bodies {
		if strings.TrimSpace(b) == "" {
			continue
		}
		s := Measure(b)
		n++
		sum.Punct += s.Punct
		sum.Emoji += s.Emoji
		sum.Padding += s.Padding
		if s.Cased {
			cased++
			sum.Caps += s.Caps
		}
	}
	if n == 0 {
		return Shape{}, 0
	}
	out := Shape{
		Punct:   sum.Punct / float64(n),
		Emoji:   sum.Emoji / float64(n),
		Padding: sum.Padding / float64(n),
	}
	// Half the sample must be cased before a capitals ratio is quoted at all.
	if cased*2 >= n && cased > 0 {
		out.Cased = true
		out.Caps = sum.Caps / float64(cased)
	}
	return out, n
}
