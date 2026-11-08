package httpserver

// POST /admin/regex/check — GW-34 (PO 2026-08-28).
//
// Every rule the portal stores as a pattern (route sender/receiver/keyword/
// priority filters, spam rules, templates, auto-replace) is compiled by THIS
// binary with Go's RE2. The Admin API used to check those patterns with
// JavaScript's `new RegExp`, which is a different language:
//
//   (?<=USD )\d+   (?!free)win   (loan)\1     -> JavaScript accepts, RE2 rejects
//   (?i)promo                                 -> JavaScript rejects, RE2 accepts
//
// So an operator could save a rule the engine would never enforce, and could
// be blocked from saving one it would. Rather than keep a hand-written list
// of the differences in Node and hope it stays in step, the Admin API asks
// the engine — the actual authority — and only falls back to its own local
// check when this endpoint is unreachable.
//
// Status-key gated like the other /admin tools. Read-only: it compiles a
// pattern and throws the result away, touching no state.
//
// The regex tester (Tools, PO 2026-09-12) adds sample texts. Each is matched
// the way the rule's own stage matches it, chosen by `use`:
//
//	rule (default)  routes' sender/receiver/keyword/priority filters and
//	                templates: the pattern as written, found anywhere in the
//	                text, case-sensitive — MatchString, like the stages
//	spam            spam rules: case-insensitive ("(?i)" is added, as
//	                refdata.CompileSpamRules does), and also tried against the
//	                folded text an enforce_spam_strict account is checked on
//
// With `replace`, each sample also shows the text after ReplaceAllString — the
// call auto-replace rules and route rewrites make.

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"

	"github.com/yakugate/yakugate/internal/abuse"
)

// regexCheckLimit caps one request. A route carries four patterns; a generous
// ceiling still refuses an accidental bulk-compile of someone's whole table.
const regexCheckLimit = 64

// Sample limits: a tester needs a handful of messages, not a mailbox. RE2's
// linear time means no pattern can make these slow; the caps only bound the
// reply's size.
const (
	regexSampleLimit    = 20
	regexSampleMaxRunes = 2000
	regexMatchLimit     = 50
)

// regexPart is one run of a sample: matched or not. The portal highlights
// these in order, so it never has to turn Go's byte offsets into JavaScript
// string positions (they differ for any non-ASCII text).
type regexPart struct {
	Text  string `json:"t"`
	Match bool   `json:"m,omitempty"`
}

type regexSample struct {
	Text    string      `json:"text"`
	Matched bool        `json:"matched"`
	Parts   []regexPart `json:"parts"`
	// Groups holds each match's capture groups (group 0 is the whole match).
	Groups [][]string `json:"groups,omitempty"`
	// Replaced is the text after ReplaceAllString, when a replacement was
	// given.
	Replaced *string `json:"replaced,omitempty"`
	// Folded is the spam-strict text (spam use only) and whether the rule
	// matches it.
	Folded        string `json:"folded,omitempty"`
	FoldedMatched bool   `json:"foldedMatched,omitempty"`
}

func (p *Process) handleRegexCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		// One pattern, or many with a field label each — a route sends all
		// four at once so the operator sees every problem in one go rather
		// than fixing them one save at a time.
		Pattern  string `json:"pattern,omitempty"`
		Patterns []struct {
			Field   string `json:"field"`
			Pattern string `json:"pattern"`
		} `json:"patterns,omitempty"`
		// The tester's extras; see the file comment.
		Samples []string `json:"samples,omitempty"`
		Use     string   `json:"use,omitempty"`
		Replace *string  `json:"replace,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if req.Pattern != "" {
		req.Patterns = append(req.Patterns, struct {
			Field   string `json:"field"`
			Pattern string `json:"pattern"`
		}{Field: "pattern", Pattern: req.Pattern})
	}
	if len(req.Patterns) == 0 {
		writeJSON(w, 400, map[string]string{"error": "pattern or patterns is required"})
		return
	}
	if len(req.Patterns) > regexCheckLimit {
		writeJSON(w, 400, map[string]string{"error": "too many patterns in one request"})
		return
	}
	if len(req.Samples) > regexSampleLimit {
		writeJSON(w, 400, map[string]string{"error": "too many samples in one request"})
		return
	}
	for _, t := range req.Samples {
		if len([]rune(t)) > regexSampleMaxRunes {
			writeJSON(w, 400, map[string]string{"error": "a sample is longer than 2000 characters"})
			return
		}
	}
	use := strings.ToLower(strings.TrimSpace(req.Use))
	if use != "" && use != "rule" && use != "spam" {
		writeJSON(w, 400, map[string]string{"error": `use must be "rule" or "spam"`})
		return
	}

	allValid := true
	results := make([]map[string]any, 0, len(req.Patterns))
	for _, in := range req.Patterns {
		res := map[string]any{"field": in.Field, "pattern": in.Pattern, "valid": true}
		// An empty pattern is "no filter", not an error — the same meaning
		// the loaders give it. Saying otherwise would block clearing a field.
		if in.Pattern != "" {
			if re, err := regexp.Compile(in.Pattern); err != nil {
				res["valid"] = false
				res["error"] = err.Error()
				allValid = false
			} else if len(req.Samples) > 0 {
				// Spam compiles with (?i) added, as refdata.CompileSpamRules
				// does. Compiled again rather than trusted: "(?i)" + a pattern
				// that compiles on its own always compiles too, but checking
				// costs nothing and a wrong assumption would lie to the user.
				if use == "spam" {
					if sre, err := regexp.Compile("(?i)" + in.Pattern); err == nil {
						re = sre
					}
				}
				res["samples"] = testSamples(re, req.Samples, use == "spam", req.Replace)
			}
		}
		results = append(results, res)
	}
	writeJSON(w, 200, map[string]any{"valid": allValid, "results": results})
}

// testSamples runs one compiled rule over the samples.
func testSamples(re *regexp.Regexp, samples []string, spam bool, replace *string) []regexSample {
	out := make([]regexSample, 0, len(samples))
	for _, text := range samples {
		s := regexSample{Text: text, Matched: re.MatchString(text)}
		locs := re.FindAllStringSubmatchIndex(text, regexMatchLimit)
		at := 0
		for _, loc := range locs {
			if loc[0] > at {
				s.Parts = append(s.Parts, regexPart{Text: text[at:loc[0]]})
			}
			if loc[1] > loc[0] {
				s.Parts = append(s.Parts, regexPart{Text: text[loc[0]:loc[1]], Match: true})
			}
			var groups []string
			for g := 0; g+1 < len(loc); g += 2 {
				if loc[g] < 0 {
					groups = append(groups, "")
					continue
				}
				groups = append(groups, text[loc[g]:loc[g+1]])
			}
			s.Groups = append(s.Groups, groups)
			at = loc[1]
		}
		if at < len(text) {
			s.Parts = append(s.Parts, regexPart{Text: text[at:]})
		}
		if replace != nil {
			r := re.ReplaceAllString(text, *replace)
			s.Replaced = &r
		}
		if spam {
			s.Folded = abuse.Normalize(text)
			s.FoldedMatched = s.Folded != "" && re.MatchString(s.Folded)
		}
		out = append(out, s)
	}
	return out
}
