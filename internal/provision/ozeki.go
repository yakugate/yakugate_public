package provision

import "strings"

// ParseCondition reads a routing-rule condition written the way the portal
// accepts it (PO 2026-09-24; GW-35, 2026-08-28: "match and modify in single
// input for to, from and text"), Ozeki style:
//
//	^9230              a pattern as typed: matches, rewrites nothing
//	/^9230/            the same, delimited
//	/^0(\d+)/92$1/     matches ^0(\d+) AND rewrites what it matched to 92$1
//
// It is the Go twin of p13/src/modules/rules/ozeki.js and the two MUST read
// the same text the same way — "one database, two doors": a door that parsed
// this differently would store a different rule for the same words.
//
// The danger it removes is specific. Without parsing, the console compiled the
// WHOLE string as a pattern. "/^ACME$/ACMEPK/" compiles fine as a regex — and
// then matches something else entirely, so the rule quietly takes the wrong
// traffic. Parsing first means the pattern that is checked is the pattern
// that is stored.
//
// A `/` that belongs inside a pattern is written `\/`; every other escape is
// regex and kept verbatim.
func ParseCondition(raw string) (match, replace string, err error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", "", nil
	}
	if !strings.HasPrefix(text, "/") {
		return text, "", nil // a pattern exactly as typed
	}
	var segs []string
	var cur strings.Builder
	endedOnDelimiter := false
	for i := 1; i < len(text); i++ {
		ch := text[i]
		if ch == '\\' && i+1 < len(text) {
			if text[i+1] == '/' {
				cur.WriteByte('/')
			} else {
				cur.WriteByte(ch)
				cur.WriteByte(text[i+1])
			}
			i++
			endedOnDelimiter = false
			continue
		}
		if ch == '/' {
			segs = append(segs, cur.String())
			cur.Reset()
			endedOnDelimiter = i == len(text)-1
			continue
		}
		cur.WriteByte(ch)
		endedOnDelimiter = false
	}
	if !endedOnDelimiter || cur.Len() > 0 {
		return "", "", refusef(`a delimited condition ends with / — write /pattern/ or /pattern/replacement/. A / inside the pattern is written \/`)
	}
	switch len(segs) {
	case 1:
		return segs[0], "", nil
	case 2:
		if segs[0] == "" {
			return "", "", refusef("there is nothing to rewrite: give a pattern before the replacement, /pattern/replacement/")
		}
		return segs[0], segs[1], nil
	}
	return "", "", refusef(`too many / — use /pattern/ to match, or /pattern/replacement/ to match and rewrite. A / inside a pattern is written \/`)
}
