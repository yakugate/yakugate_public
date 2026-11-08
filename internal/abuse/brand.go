package abuse

import "strings"

// Brand impersonation — P10 register row "Brand-impersonation detection".
//
// A message claiming to be a bank, a telco or a government service when it is
// not. This is the highest-harm SMS abuse there is: it is how people lose
// money, and it is what regulators fine aggregators over.
//
// The permission model already mostly exists. Sender-ID approval controls who
// may SEND AS a brand. The gap this closes is the message BODY: an account with
// a perfectly legitimate sender id writing someone else's brand into the text.
//
// PO 2026-09-09 ruled the protected list REUSES sender_ids rather than adding a
// table. That carries one consequence the caller must respect, and it is worth
// stating plainly because getting it wrong breaks ordinary traffic: sender_ids
// means "may send as", which is NOT the same permission as "may mention". A
// customer registering a generic sender like INFO or ALERT must not thereby
// make those words protected for everyone else. Only rows explicitly marked as
// body-protected belong in the list handed to this evaluator.
//
// Matching runs on the DISGUISE-FOLDED body, so HB1, H-B-L and a Cyrillic-Н
// spelling all collapse onto the same protected name — Normalize already does
// that work, and evasion by respelling is the norm rather than the exception.

// Brand is one protected name and the accounts entitled to use it.
type Brand struct {
	// Name as registered. Folded once at load, not per message.
	Name string
	// Folded is Name run through Normalize. Precomputed because this runs per
	// message and compilation on the hot path is the chunk-5 5.3 rule.
	Folded string
	// AllowedAccounts may use this brand in a body. Empty means NOBODY may,
	// which is the right default for a name nobody has claimed.
	AllowedAccounts map[int64]bool
}

// PrepBrands folds each name once. Names that fold to nothing are dropped: a
// protected name of "!!!" would otherwise match every message.
func PrepBrands(in []Brand) []Brand {
	out := make([]Brand, 0, len(in))
	for _, b := range in {
		f := Normalize(b.Name)
		if strings.TrimSpace(f) == "" {
			continue
		}
		b.Folded = f
		out = append(out, b)
	}
	return out
}

// BrandHit is one impersonation finding.
type BrandHit struct {
	Brand string
	// Disguised is true when the raw body did NOT contain the name but the
	// folded body did — i.e. it was spelled to evade. Worth separating: an
	// honest mention is careless, a disguised one is deliberate.
	Disguised bool
}

// FindBrandImpersonation returns the protected names this account used without
// being entitled to. nil when there is nothing to report.
func FindBrandImpersonation(body string, accountID int64, brands []Brand) []BrandHit {
	if body == "" || len(brands) == 0 {
		return nil
	}
	rawLower := strings.ToLower(body)
	folded := Normalize(body)
	var hits []BrandHit
	for _, b := range brands {
		if b.AllowedAccounts[accountID] {
			continue // this account owns the brand; saying its own name is fine
		}
		rawHit := hitsBrandName(rawLower, strings.ToLower(b.Name))
		foldHit := hitsBrandName(folded, b.Folded)
		if !rawHit && !foldHit {
			continue
		}
		hits = append(hits, BrandHit{Brand: b.Name, Disguised: !rawHit && foldHit})
	}
	return hits
}

// BrandParams tune the signal.
type BrandParams struct {
	Common
	// DisguisedFull is the strength a single DISGUISED hit scores. Spelling a
	// brand as HB1 is not carelessness, so one is enough to matter.
	DisguisedFull float64 `json:"disguised_full"`
	// PlainFull is the strength a single plain-spelling hit scores. Lower,
	// because a legitimate message can name another company for honest reasons
	// — "pay your HBL bill here" from a payments partner, say.
	PlainFull float64 `json:"plain_full"`
	// MinMessages is how many messages in the window must carry a hit before
	// this scores at all, so one odd message does not move an account's score.
	MinMessages int `json:"min_messages"`
	// SampleMsgs caps the bodies read for one account and ScanLimit the
	// sweep as a whole (see SpamParams). 0 = 200.
	SampleMsgs int `json:"sample_msgs"`
	ScanLimit  int `json:"scan_limit"`
}

// Validate names the first key this rule cannot run without.
func (p BrandParams) Validate() error { return needWindow(p.Common) }

// EvalBrand converts hit counts over a window into 0..1 strength for the score
// engine. Like formatting, it never returns a Severity: it does not act alone.
func EvalBrand(plainHits, disguisedHits, windowMsgs int, p BrandParams) float64 {
	total := plainHits + disguisedHits
	if total == 0 || total < p.MinMessages {
		return 0
	}
	s := 0.0
	if disguisedHits > 0 && p.DisguisedFull > 0 {
		s = p.DisguisedFull
	}
	if plainHits > 0 && p.PlainFull > s {
		s = p.PlainFull
	}
	// Scaled by how much of the window carried a hit, so a campaign built on
	// impersonation scores higher than a single stray message.
	if windowMsgs > 0 {
		share := float64(total) / float64(windowMsgs)
		if share > 1 {
			share = 1
		}
		s *= 0.5 + 0.5*share
	}
	if s > 1 {
		s = 1
	}
	return s
}

// hitsBrandName is deliberately NOT HitsKeyword.
//
// HitsKeyword takes a plain substring match first, which is right for a spam
// keyword — "loan" inside "loans" should hit. It is wrong for a brand: "HBL"
// appears inside "SHBLY", and short brand names are common enough that
// substring matching would flag ordinary traffic constantly. So the raw check
// requires WORD BOUNDARIES, and the folded check reuses closedUpHit, which
// already insists a match start where a word starts and end where one ends.
//
// Known limitation, recorded rather than papered over: the shared leet map
// folds '1' to 'i', so "HB1" becomes "hbi" and does NOT match HBL. Changing
// that map to '1'->'l' would fix this brand and break others ("l0an" style
// matching relies on the current mapping), and P1's strict spam gate shares
// it. A brand that is commonly respelled with a digit needs that spelling
// registered as its own protected name.
func hitsBrandName(text, name string) bool {
	if name == "" {
		return false
	}
	return containsWholeWord(text, name) || closedUpHit(text, name)
}

// containsWholeWord reports whether name appears in text bounded by
// non-alphanumerics on both sides.
func containsWholeWord(text, name string) bool {
	for i := 0; ; {
		j := strings.Index(text[i:], name)
		if j < 0 {
			return false
		}
		at := i + j
		end := at + len(name)
		if !alnumAt(text, at-1) && !alnumAt(text, end) {
			return true
		}
		i = at + 1
		if i >= len(text) {
			return false
		}
	}
}

// alnumAt reports whether the byte at i is a letter or digit. Out of range
// counts as a boundary, so a name at the very start or end of the text
// matches.
func alnumAt(s string, i int) bool {
	if i < 0 || i >= len(s) {
		return false
	}
	c := s[i]
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}
