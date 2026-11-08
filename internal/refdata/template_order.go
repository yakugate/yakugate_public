package refdata

import (
	"log"
	"regexp/syntax"
	"sort"
	"sync"
)

// Which template a message matches, when several would (QA templates
// API-005, 2026-10-14).
//
// The template stage stops at the first template that matches, so the order
// the cache hands them over in decides which one is stamped on the message
// and whose forced priority and ap fields it gets. That order used to be
// whatever MySQL returned — in practice the oldest template won, however
// general it was. It is now fixed and documented (user guide chapter 15):
//
//  1. the account's own templates before its company's (Mem.Templates — a
//     template written for one account is the more particular one);
//  2. within each, the more specific pattern first: the one with more
//     literal characters every match must contain ("^Your OTP is [0-9]{6}$"
//     before "^Your .+$", which before ".*");
//  3. then the lower template id (the older one).
//
// It is not the priority the template forces: that is what a matched message
// is sent with, not a rank among templates.

// literalWeight is how many literal characters every match of pattern must
// contain: what rule 2 above sorts by. A pattern that does not parse weighs
// nothing (it is dropped at compile anyway).
func literalWeight(pattern string) int {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return 0
	}
	return literalRunes(re)
}

// literalRunes counts the literal runes a match of re cannot do without: an
// optional part (?, *, {0,n}) counts nothing, an alternation its poorest
// branch, a repeat its minimum. Capped so a huge repeat cannot overflow.
func literalRunes(re *syntax.Regexp) int {
	const most = 1 << 20
	n := 0
	switch re.Op {
	case syntax.OpLiteral:
		n = len(re.Rune)
	case syntax.OpConcat:
		for _, s := range re.Sub {
			n += literalRunes(s)
		}
	case syntax.OpCapture, syntax.OpPlus:
		n = literalRunes(re.Sub[0])
	case syntax.OpRepeat:
		if re.Min > 0 {
			n = re.Min * literalRunes(re.Sub[0])
		}
	case syntax.OpAlternate:
		for i, s := range re.Sub {
			if w := literalRunes(s); i == 0 || w < n {
				n = w
			}
		}
	}
	if n > most {
		return most
	}
	return n
}

// sortTemplates puts one key's templates in the order the stage tries them:
// the more literal text first, then the lower id.
func sortTemplates(list []Template, weight map[int64]int) {
	sort.SliceStable(list, func(i, j int) bool {
		wi, wj := weight[list[i].ID], weight[list[j].ID]
		if wi != wj {
			return wi > wj
		}
		return list[i].ID < list[j].ID
	})
}

// badPriorityLogged remembers which template forced which out-of-range
// priority, so the line is written once and not at every reload.
var badPriorityLogged sync.Map // template id -> the value logged

// usablePriority is the priority a template may force: 1 to 9, the gateway's
// scale, or 0 for none (QA templates BUG-007). The Admin API refuses anything
// else since 2026-10-14; a value saved before then (50, 999) is ignored here
// rather than sent as is — the message keeps the client's priority or the
// default — and said once in the log.
func usablePriority(id int64, p int) int {
	if p == 0 || (p >= 1 && p <= 9) {
		return p
	}
	if prev, ok := badPriorityLogged.Load(id); !ok || prev.(int) != p {
		badPriorityLogged.Store(id, p)
		log.Printf("[refdata] template %d forces priority %d, outside 1-9 — IGNORED (the message keeps its own or the default)", id, p)
	}
	return 0
}
