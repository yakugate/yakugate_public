package abuse

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"unicode"
)

// Campaign signature — P10 register row "Content-hash campaign signature".
//
// Answers "what else went out under this campaign" when an operator is looking
// at a flagged message. It is for INVESTIGATION, not scoring: the near-duplicate
// row was ruled not-building (PO 2026-09-09), so nothing gates on this.
//
// What goes into the hash decides what counts as the same campaign, and the
// ruling was: fold disguises, strip digits, drop very short words.
//
//	"Your OTP is 4429 valid 10 minutes"  \
//	"Your OTP is 8871 valid 10 minutes"   >-- one campaign
//
// WHAT THIS DOES NOT DO, and it is the important caveat. The ruling asked for
// "digits and names stripped". Digits are easy — they are recognisable. NAMES
// ARE NOT: "Ali" and "Sara" are ordinary words, and no exact hash can remove
// them without a name list. So:
//
//	"Hi Ali, your loan is approved"    -- these are DIFFERENT
//	"Hi Sara, your loan is approved"      campaigns to this function
//
// Raising the word-length floor does not fix it (names are 3-6 letters, like
// most meaningful words) and neither does keeping only the longest words.
// Grouping personalised variants needs SIMILARITY rather than equality — the
// shingle/MinHash approach that was deliberately ruled out on the
// near-duplicate row. This function groups messages that are identical after
// folding, plus disguised respellings and reordering, which covers bulk
// identical campaigns but not per-recipient personalisation.
//
// The SENDER is deliberately not part of the hash. Including it would keep
// identical wording under two different sender ids apart, and one operation
// running under several sender ids is exactly the pattern worth being able to
// see.
//
// This is complementary to the client-supplied campaignId, not a replacement:
// the id says what the sender CALLED it (and can be omitted or faked), the hash
// says what it LOOKS like (ours, and it works across accounts that never
// coordinated).

// minCampaignWord is the shortest word that survives into the signature.
// Two-letter words are mostly grammar and carry no campaign identity; keeping
// them makes two unrelated messages collide on "to", "is", "at".
const minCampaignWord = 3

// CampaignHash returns the signature for a message body, or "" when the body
// has nothing distinctive left after folding — an empty signature is honest
// about grouping nothing, whereas hashing an empty string would group every
// such message together.
func CampaignHash(body string) string {
	sig := campaignSignature(body)
	if sig == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(sig))
	return hex.EncodeToString(sum[:])
}

// campaignSignature is the text the hash is taken over. Separate so a test can
// assert on something readable rather than on a hex digest.
func campaignSignature(body string) string {
	folded := Normalize(body)
	if folded == "" {
		return ""
	}
	words := strings.Fields(folded)
	kept := make([]string, 0, len(words))
	seen := map[string]bool{}
	for _, w := range words {
		w = strings.TrimFunc(w, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
		if len(w) < minCampaignWord {
			continue
		}
		// Drop anything that is all digits: an amount, a code, an order
		// number. These are exactly the per-recipient variation the signature
		// exists to see through.
		if allDigits(w) {
			continue
		}
		if seen[w] {
			continue
		}
		seen[w] = true
		kept = append(kept, w)
	}
	if len(kept) == 0 {
		return ""
	}
	// Sorted, so word order does not split a campaign. A sender who shuffles
	// a sentence is doing it precisely to look different.
	sort.Strings(kept)
	return strings.Join(kept, " ")
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(s) > 0
}
