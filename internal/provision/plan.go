package provision

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/netcode"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// An account's ROUTING PLAN (Saleem 2026-09-30): its outgoing rules in the
// order the gateway reads them; the first rule that catches a message sends
// it. The order is kept for the operator — nobody types a position:
//
//  1. special cases (a sender, text or priority condition), in the
//     operator's order, except that a rule never sits below one that
//     catches everything it catches;
//  2. each country, by name: its network rules (by network name) above its
//     whole-country rule;
//  3. any destination, last.
//
// This is the prototype's canon() + fixSpecials()
// (scratchpad route-plans/route-plan-simulator.html, spec
// rule-creation-ux.md) — the same in the console and, in time, the Admin
// API and the portal. Rules made before 2026-09-30 in shapes no longer made
// (several destinations, network tabs, a receiver condition) are placed by
// what they catch, so nothing is dropped from the plan. [SCOPE P14-ROUTE]

// PlanRule is one rule as the plan reads it.
type PlanRule struct {
	ID        int64
	Name      string
	Priority  int
	On        bool
	Direction string // MT (the plan) | MO

	Dest          Destination // Kind "" when the rule names several destinations (made before 2026-09-30)
	Networks      []string    // routes.match_mccmncs as stored
	AcceptUnknown bool        // accept_undetected_network

	Conds   []Cond   // its conditions (sender, text, priority; receiver on old rules)
	Changes []Change // what it changes before sending
	// Stored is each field's {match, replace} as stored (sender, receiver,
	// text, priority): the engine's own test and rewrite in TestNumber.
	Stored map[string][2]string

	Plan      *sendplan.Plan // nil on an old rule (route_type + vendor_id)
	Legacy    bool           // an old rule: send_plan NULL
	RouteType string
	VendorID  int64 // routes.vendor_id (MO: the vendor they arrive from)
	Weight    int
	PlanError string // the stored plan cannot be read: the engine skips the rule

	Retry                                      *sendplan.Retry // nil = the gateway's default
	SellCard, BuyCard, DLRSellCard, DLRBuyCard int64

	// For ordering an old rule of several networks: their country's name and
	// the first network's name (filled by the store; a one-destination rule
	// uses Dest).
	CountryName string
	NetworkName string
}

// Special reports a special case: a rule with a condition.
func (r PlanRule) Special() bool { return len(r.Conds) > 0 }

// Levels is where the rule sends: Main, then the backups. An old rule is its
// one vendor, read the way the engine reads it (sendplan.FromLegacy).
func (r PlanRule) Levels() []sendplan.Level {
	if r.Plan != nil {
		return r.Plan.Levels
	}
	if r.Legacy && r.VendorID > 0 {
		lv := sendplan.Level{Vendors: []sendplan.Vendor{{VendorID: r.VendorID}}}
		return []sendplan.Level{lv}
	}
	return nil
}

// Method is the rule's way of choosing.
func (r PlanRule) Method() string {
	if r.Plan != nil {
		if r.Plan.Method != "" {
			return r.Plan.Method
		}
		if len(r.Plan.Levels) > 0 {
			return r.Plan.MethodFor(r.Plan.Levels[0])
		}
		u, _ := r.Plan.Unified()
		return u.Method
	}
	return sendplan.MethodFromRouteType(r.RouteType)
}

// OldShape lists what about the rule is no longer made (2026-09-30): it
// still sends as before.
func (r PlanRule) OldShape() []string {
	var out []string
	if r.Dest.Kind == "" {
		out = append(out, "several destinations")
	}
	if r.Plan != nil && len(r.Plan.Exceptions) > 0 {
		out = append(out, "network tabs")
	}
	for _, c := range r.Conds {
		if c.Field == FieldReceiver {
			out = append(out, "a receiver condition")
		}
	}
	if r.Plan != nil && r.Plan.PerLevel() {
		out = append(out, "a way of choosing per level")
	}
	if r.Legacy {
		out = append(out, "one vendor, chosen by its rule type")
	}
	return out
}

// reach is what numbers a rule takes, by network alone.
type reach struct {
	all     bool     // every number, known or not
	codes   []string // "410" (a country) or "410004" (a network)
	unknown bool     // also numbers whose country is not found
}

func cleanCodes(n []string) []string {
	var out []string
	for _, v := range n {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func (r PlanRule) reach() reach {
	nets := cleanCodes(r.Networks)
	if r.Plan != nil && r.Plan.NetworksOnly() {
		// A network-only rule takes only the networks it has vendors for.
		var codes []string
		for code := range r.Plan.Exceptions {
			if len(nets) == 0 || networksCover(nets, code) {
				codes = append(codes, code)
			}
		}
		sort.Strings(codes)
		return reach{codes: codes}
	}
	if len(nets) == 0 {
		return reach{all: true}
	}
	return reach{codes: nets, unknown: r.AcceptUnknown}
}

func entryCovers(a, b string) bool {
	am, an := netcode.Split(a)
	bm, bn := netcode.Split(b)
	if am == "" || am != bm {
		return false
	}
	if an == "" {
		return true
	}
	return bn != "" && netcode.SameMNC(an, bn)
}

func entriesOverlap(a, b string) bool {
	am, an := netcode.Split(a)
	bm, bn := netcode.Split(b)
	if am == "" || am != bm {
		return false
	}
	return an == "" || bn == "" || netcode.SameMNC(an, bn)
}

func reachCovers(a, b reach) bool {
	if a.all {
		return true
	}
	if b.all {
		return false
	}
	for _, x := range b.codes {
		ok := false
		for _, y := range a.codes {
			ok = ok || entryCovers(y, x)
		}
		if !ok {
			return false
		}
	}
	return !b.unknown || a.unknown
}

func reachOverlap(a, b reach) bool {
	if a.all || b.all || (a.unknown && b.unknown) {
		return true
	}
	for _, x := range a.codes {
		for _, y := range b.codes {
			if entriesOverlap(x, y) {
				return true
			}
		}
	}
	return false
}

func (r PlanRule) cond(field string) *Cond {
	for i := range r.Conds {
		if r.Conds[i].Field == field {
			return &r.Conds[i]
		}
	}
	return nil
}

// Covers reports whether rule a catches every message rule b catches
// (the prototype's covers): its destination takes all of b's, and each of
// its conditions takes every value b's condition on that field takes.
func Covers(a, b PlanRule) bool {
	if !reachCovers(a.reach(), b.reach()) {
		return false
	}
	for _, ca := range a.Conds {
		cb := b.cond(ca.Field)
		if cb == nil || !condCovers(ca, *cb) {
			return false
		}
	}
	return true
}

// SameCatch reports two rules that catch exactly the same messages.
func SameCatch(a, b PlanRule) bool { return Covers(a, b) && Covers(b, a) }

// CanOverlap reports whether one message could be caught by both rules.
func CanOverlap(a, b PlanRule) bool {
	if !reachOverlap(a.reach(), b.reach()) {
		return false
	}
	for _, ca := range a.Conds {
		if cb := b.cond(ca.Field); cb != nil && !condsMeet(ca, *cb) {
			return false
		}
	}
	return true
}

// ── the order ─────────────────────────────────────────────────────────────

const (
	grpSpecial = iota
	grpNetwork
	grpCountry
	grpSeveral
	grpAny
)

// group is where a rule sits in the order, and the country it sits under.
func (r PlanRule) group() (int, string) {
	if r.Special() {
		return grpSpecial, ""
	}
	rc := r.reach()
	if rc.all {
		return grpAny, ""
	}
	mcc := ""
	country := false
	for _, c := range rc.codes {
		m, n := netcode.Split(c)
		if mcc != "" && m != mcc {
			return grpSeveral, ""
		}
		mcc = m
		country = country || n == ""
	}
	if mcc == "" {
		return grpSeveral, ""
	}
	if country {
		return grpCountry, mcc
	}
	return grpNetwork, mcc
}

func (r PlanRule) countryLabel(mcc string) string {
	if r.Dest.Country != "" && r.Dest.MCC == mcc {
		return r.Dest.Country
	}
	if r.CountryName != "" {
		return r.CountryName
	}
	return "MCC " + mcc
}

func (r PlanRule) networkLabel() string {
	if r.Dest.Kind == DestNetwork && r.Dest.Network != "" {
		return r.Dest.Network
	}
	if r.NetworkName != "" {
		return r.NetworkName
	}
	return strings.Join(cleanCodes(r.Networks), ",")
}

// tier is one rule, or several OLD rules at one priority that can catch
// the same message, which the engine reads as one shared level
// (pipeline.ruleGroups): they move together and keep one priority. Old
// rules at one priority that never meet are independent.
//
// "Can catch the same message" is followed through the whole priority, not
// only from one rule to the next: old rules A (Pakistan), B (UAE), C
// (Pakistan) at one priority are the tiers {A, C} and {B} — A and C share
// every Pakistan message, and splitting them onto two priorities would hand
// all of those to A.
type tier []int

func tiersOf(rules []PlanRule) []tier {
	var out []tier
	for i, r := range rules {
		if !r.Legacy {
			out = append(out, tier{i})
			continue
		}
		join := -1
		for k := 0; k < len(out); k++ {
			f := rules[out[k][0]]
			if !f.Legacy || f.Priority != r.Priority {
				continue
			}
			meets := false
			for _, j := range out[k] {
				if CanOverlap(rules[j], r) {
					meets = true
					break
				}
			}
			if !meets {
				continue
			}
			if join < 0 {
				join = k
				out[k] = append(out[k], i)
				continue
			}
			// r meets two tiers: through it they share, so they are one.
			out[join] = append(out[join], out[k]...)
			out = append(out[:k], out[k+1:]...)
			k--
		}
		if join < 0 {
			out = append(out, tier{i})
		}
	}
	return out
}

// fixCover moves a rule up to just above the first rule that catches every
// message it catches and not the other way round (the prototype's
// fixSpecials), until nothing moves.
func fixCover(rules []PlanRule, ts []tier) []tier {
	a := append([]tier(nil), ts...)
	for guard := 0; guard < 300; guard++ {
		moved := false
	outer:
		for i := 0; i < len(a); i++ {
			for j := i + 1; j < len(a); j++ {
				x, y := rules[a[i][0]], rules[a[j][0]]
				if Covers(x, y) && !Covers(y, x) {
					t := a[j]
					a = append(a[:j], a[j+1:]...)
					a = append(a[:i], append([]tier{t}, a[i:]...)...)
					moved = true
					break outer
				}
			}
		}
		if !moved {
			break
		}
	}
	return a
}

// CanonicalOrder is the plan in the order the screens keep. Rules are given
// in the order the gateway reads them now (priority, then id); special cases
// keep their relative order.
func CanonicalOrder(rules []PlanRule) []PlanRule {
	ts := tiersOf(rules)
	var special, several, anyT []tier
	nets := map[string][]tier{}
	ctry := map[string][]tier{}
	var mccs []string
	seen := map[string]bool{}
	for _, t := range ts {
		g, mcc := rules[t[0]].group()
		switch g {
		case grpSpecial:
			special = append(special, t)
		case grpSeveral:
			several = append(several, t)
		case grpAny:
			anyT = append(anyT, t)
		case grpNetwork, grpCountry:
			if !seen[mcc] {
				seen[mcc] = true
				mccs = append(mccs, mcc)
			}
			if g == grpNetwork {
				nets[mcc] = append(nets[mcc], t)
			} else {
				ctry[mcc] = append(ctry[mcc], t)
			}
		}
	}
	label := map[string]string{}
	for _, t := range ts {
		if _, mcc := rules[t[0]].group(); mcc != "" && label[mcc] == "" {
			label[mcc] = strings.ToLower(rules[t[0]].countryLabel(mcc))
		}
	}
	sort.SliceStable(mccs, func(i, j int) bool {
		if label[mccs[i]] != label[mccs[j]] {
			return label[mccs[i]] < label[mccs[j]]
		}
		return mccs[i] < mccs[j]
	})
	out := fixCover(rules, special)
	for _, m := range mccs {
		n := nets[m]
		sort.SliceStable(n, func(i, j int) bool {
			a, b := rules[n[i][0]], rules[n[j][0]]
			if la, lb := strings.ToLower(a.networkLabel()), strings.ToLower(b.networkLabel()); la != lb {
				return la < lb
			}
			return strings.Join(a.Networks, ",") < strings.Join(b.Networks, ",")
		})
		out = append(out, fixCover(rules, n)...)
		out = append(out, fixCover(rules, ctry[m])...)
	}
	out = append(out, fixCover(rules, several)...)
	out = append(out, fixCover(rules, anyT)...)
	res := make([]PlanRule, 0, len(rules))
	for _, t := range out {
		for _, i := range t {
			res = append(res, rules[i])
		}
	}
	return res
}

// InOrder reports whether the rules already stand in the order the screens keep.
func InOrder(rules []PlanRule) bool {
	c := CanonicalOrder(rules)
	for i := range c {
		if c[i].ID != rules[i].ID {
			return false
		}
	}
	return true
}

// Move is one rule changing place: positions are 1-based.
type Move struct {
	ID       int64
	Name     string
	From, To int
}

// Moves lists the rules whose place differs between two orders of one plan.
func Moves(before, after []PlanRule) []Move {
	pos := map[int64]int{}
	for i, r := range before {
		pos[r.ID] = i + 1
	}
	var out []Move
	for i, r := range after {
		if from := pos[r.ID]; from != i+1 {
			out = append(out, Move{ID: r.ID, Name: r.Name, From: from, To: i + 1})
		}
	}
	return out
}

// MoveWords is "'Zong': 2 → 1".
func (m Move) Words() string {
	if m.From == 0 {
		return fmt.Sprintf("'%s': new, rule %d", m.Name, m.To)
	}
	return fmt.Sprintf("'%s': %d → %d", m.Name, m.From, m.To)
}

func sameTier(a, b PlanRule) bool {
	return a.Legacy && b.Legacy && a.Priority == b.Priority && CanOverlap(a, b)
}

// Hidden maps each rule that is on and hidden to the index of the first rule
// above it (on) that catches every message it catches. A hidden rule still
// gets messages when every vendor above is down — "hidden", not "never used".
func Hidden(rules []PlanRule) map[int64]int {
	out := map[int64]int{}
	for i, r := range rules {
		if !r.On || r.PlanError != "" {
			continue
		}
		for j := 0; j < i; j++ {
			a := rules[j]
			if !a.On || a.PlanError != "" || sameTier(a, r) {
				continue
			}
			if Covers(a, r) {
				out[r.ID] = j
				break
			}
		}
	}
	return out
}

// ── words ───────────────────────────────────────────────────────────────

// VendorNames names vendors for the plan's words; a missing one reads "#id".
type VendorNames map[int64]string

func (n VendorNames) name(id int64) string {
	if s := n[id]; s != "" {
		return s
	}
	return fmt.Sprintf("#%d", id)
}

func shortWay(m string) string {
	switch m {
	case sendplan.Cheapest:
		return "Cheapest"
	case sendplan.Best:
		return "Best delivery"
	case sendplan.Value:
		return "Best value"
	case sendplan.Least:
		return "Least busy"
	}
	return "Direct"
}

func wayAdj(m string) string {
	switch m {
	case sendplan.Cheapest:
		return "cheapest"
	case sendplan.Best:
		return "best-delivering"
	case sendplan.Value:
		return "best value"
	case sendplan.Least:
		return "least busy"
	}
	return ""
}

func wayTake(m string) string {
	switch m {
	case sendplan.Cheapest:
		return "the cheapest vendor that can send"
	case sendplan.Best:
		return "the best-delivering vendor that can send"
	case sendplan.Value:
		return "the best-value vendor that can send"
	case sendplan.Least:
		return "the least busy vendor that can send"
	}
	return "the next vendor in your order"
}

func (n VendorNames) names(lv sendplan.Level) []string {
	out := make([]string, len(lv.Vendors))
	for i, v := range lv.Vendors {
		out[i] = n.name(v.VendorID)
	}
	return out
}

// LevelPlain: "Jazz-A", "Jazz-A, then Zong-B", "the cheapest of C and D",
// "Jazz-A 70%, Zong-B 30%".
func (n VendorNames) LevelPlain(lv sendplan.Level, method string) string {
	names := n.names(lv)
	switch {
	case len(names) == 0:
		return "no vendor"
	case len(names) == 1:
		return names[0]
	case lv.Share:
		var s []string
		for _, v := range lv.Vendors {
			s = append(s, fmt.Sprintf("%s %d%%", n.name(v.VendorID), v.Pct))
		}
		return strings.Join(s, ", ")
	case method == sendplan.Direct || method == "":
		return strings.Join(names, ", then ")
	}
	return "the " + wayAdj(method) + " of " + andWords(names)
}

// LevelShort: "Jazz-A", "Direct: Jazz-A, Zong-B", "Cheapest: C, D",
// "Shared: A 70%, B 30%".
func (n VendorNames) LevelShort(lv sendplan.Level, method string) string {
	names := n.names(lv)
	switch {
	case len(names) == 0:
		return "No vendor"
	case len(names) == 1:
		return names[0]
	case lv.Share:
		var s []string
		for _, v := range lv.Vendors {
			s = append(s, fmt.Sprintf("%s %d%%", n.name(v.VendorID), v.Pct))
		}
		return "Shared: " + strings.Join(s, ", ")
	}
	return shortWay(method) + ": " + strings.Join(names, ", ")
}

// levelSentence is a level inside "In words": shares say who takes a failed share.
func (n VendorNames) levelSentence(lv sendplan.Level, method string) string {
	if lv.Share && len(lv.Vendors) > 1 {
		var s []string
		for _, v := range lv.Vendors {
			s = append(s, fmt.Sprintf("%s (%d%%)", n.name(v.VendorID), v.Pct))
		}
		out := andWords(s)
		if method != sendplan.Direct {
			out += ", a failed share going to " + wayTake(method)
		}
		return out
	}
	return n.LevelPlain(lv, method)
}

// Sentence is a rule "In words" (the prototype's sentence()): "Every message
// to CMPAK (Zong) (Pakistan 410-004) goes to Zong-B."
func (r PlanRule) Sentence(n VendorNames, cards map[int64]string) string {
	which := "Every message"
	verb := "goes to"
	var conds []string
	for _, c := range r.Conds {
		if c.Field == FieldReceiver {
			continue
		}
		conds = append(conds, c.Phrase())
	}
	to := r.Dest.Label()
	if r.Dest.Kind == DestAny {
		to = "any destination"
	} else if r.Dest.Kind == "" {
		to = "networks " + strings.Join(cleanCodes(r.Networks), ", ")
	}
	if rc := r.cond(FieldReceiver); rc != nil {
		to += " (" + rc.Phrase() + ")"
	}
	var s string
	levels := r.Levels()
	main := "no vendor yet"
	if len(levels) > 0 && len(levels[0].Vendors) > 0 {
		main = n.levelSentence(levels[0], r.Method())
	} else if r.Plan != nil && r.Plan.NetworksOnly() {
		main = "the vendors of each of its networks"
	}
	if len(conds) > 0 {
		which = "Messages " + strings.Join(conds, " ")
		verb = "go to"
		s = which + ", to " + to + ", " + verb + " " + main + "."
	} else {
		s = which + " to " + to + " " + verb + " " + main + "."
	}
	for i := 1; i < len(levels); i++ {
		prev := "Backup " + strconv.Itoa(i-1) + " either"
		if i == 1 {
			prev = "none of them"
			if len(levels[0].Vendors) == 1 {
				prev = n.name(levels[0].Vendors[0].VendorID)
			}
		}
		s += fmt.Sprintf(" If %s can't send, Backup %d sends to %s.", prev, i, n.levelSentence(levels[i], r.Method()))
	}
	if r.Plan != nil {
		for _, code := range sortedExceptionCodes(r.Plan) {
			s += fmt.Sprintf(" Network %s has its own vendors: %s.", code, n.LevelPlain(r.Plan.Exceptions[code][0], r.Method()))
		}
	}
	for _, ch := range r.Changes {
		s += " " + ch.Words()
	}
	if r.SellCard != 0 {
		s += " Priced on " + cardName(cards, r.SellCard) + "."
	}
	if !r.On {
		s += " The rule is off, so it catches nothing."
	}
	return s
}

func cardName(cards map[int64]string, id int64) string {
	if s := cards[id]; s != "" {
		return s
	}
	return fmt.Sprintf("card #%d", id)
}

func sortedExceptionCodes(p *sendplan.Plan) []string {
	var out []string
	for c := range p.Exceptions {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// ── retries in words ────────────────────────────────────────────────────

func dur(s int) string {
	fmtNum := func(v float64) string {
		if v == float64(int(v)) {
			return strconv.Itoa(int(v))
		}
		return strconv.FormatFloat(v, 'f', 1, 64)
	}
	switch {
	case s < 60:
		return strconv.Itoa(s) + " s"
	case s < 3600:
		return fmtNum(float64(s)/60) + " min"
	}
	return fmtNum(float64(s)/3600) + " h"
}

// RetryWords says a rule's retries as real waits: "5 more rounds, after
// 30 s, 1 min, 2 min, 4 min, 8 min. Gives up about 15.5 min after the first
// try; the client gets UNDELIV."
func RetryWords(r *sendplan.Retry) string {
	rt := sendplan.DefaultRetry
	if r != nil {
		rt = *r
	}
	if rt.Rounds == 0 {
		return "No retries: it fails after the first round."
	}
	var waits []int
	total := 0
	c := rt.FirstSec
	for i := 0; i < rt.Rounds; i++ {
		w := c
		if rt.Backoff == "double" {
			if w > rt.MaxWaitSec {
				w = rt.MaxWaitSec
			}
			c *= 2
		}
		waits = append(waits, w)
		total += w
	}
	when := dur(rt.FirstSec) + " apart"
	if rt.Backoff != "same" {
		var ws []string
		for _, w := range waits {
			ws = append(ws, dur(w))
		}
		when = "after " + strings.Join(ws, ", ")
	}
	return fmt.Sprintf("%s, %s. Gives up about %s after the first try; the client gets UNDELIV.",
		plural(rt.Rounds, "1 more round", strconv.Itoa(rt.Rounds)+" more rounds"), when, dur(total))
}

// RetryShort: "5 · 30 s · double · 30 min" / "3 · 1 min · same".
func RetryShort(r *sendplan.Retry) string {
	rt := sendplan.DefaultRetry
	if r != nil {
		rt = *r
	}
	if rt.Backoff == "double" {
		return fmt.Sprintf("%d · %s · double · %s", rt.Rounds, dur(rt.FirstSec), dur(rt.MaxWaitSec))
	}
	return fmt.Sprintf("%d · %s · same", rt.Rounds, dur(rt.FirstSec))
}

// ── one rule's place in the plan ───────────────────────────────────────

func posOf(rules []PlanRule, id int64) int {
	for i, r := range rules {
		if r.ID == id {
			return i + 1
		}
	}
	return 0
}

// destCovers: the rule's destination takes every number of dest.
func destTakes(r PlanRule, d Destination) bool {
	var target reach
	switch d.Kind {
	case DestAny:
		target = reach{all: true}
	case DestCountry, DestNetwork:
		target = reach{codes: d.Networks()}
	default:
		return false
	}
	return reachCovers(r.reach(), target)
}

// nextFor is the first rule below r (on, not a special case) whose
// destination takes all of r's.
func nextFor(rules []PlanRule, i int) *PlanRule {
	r := rules[i]
	for j := i + 1; j < len(rules); j++ {
		x := rules[j]
		if x.On && !x.Special() && x.PlanError == "" && reachCovers(x.reach(), r.reach()) {
			return &rules[j]
		}
	}
	return nil
}

// FallbackText is what the rule falls back to: "→ Backup 1: Telenor-G →
// rule 3 'Pakistan' → accepted and retried".
func fallbackText(rules []PlanRule, i int, n VendorNames) string {
	r := rules[i]
	var parts []string
	levels := r.Levels()
	for k := 1; k < len(levels); k++ {
		parts = append(parts, fmt.Sprintf("Backup %d: %s", k, strings.Join(n.names(levels[k]), ", ")))
	}
	if r.Special() {
		if r.Dest.Kind == DestAny {
			parts = append(parts, "the rule for the number")
		} else {
			parts = append(parts, "the "+r.Dest.Short()+" rules")
		}
	} else if nx := nextFor(rules, i); nx != nil {
		parts = append(parts, fmt.Sprintf("rule %d '%s'", posOf(rules, nx.ID), nx.Name))
	}
	parts = append(parts, "accepted and retried")
	return "→ " + strings.Join(parts, " → ")
}

// inheritFor is the rule that takes a destination's numbers when it has no
// rule of its own: the network's, else its country's, else any destination.
func inheritFor(rules []PlanRule, d Destination) *PlanRule {
	find := func(ok func(PlanRule) bool) *PlanRule {
		for i := range rules {
			x := rules[i]
			if x.On && !x.Special() && x.PlanError == "" && ok(x) {
				return &rules[i]
			}
		}
		return nil
	}
	isNet := func(x PlanRule) bool { return x.Dest.Kind == DestNetwork && x.Dest.Same(d) }
	isCountry := func(mcc string) func(PlanRule) bool {
		return func(x PlanRule) bool { return x.Dest.Kind == DestCountry && x.Dest.MCC == mcc }
	}
	isAny := func(x PlanRule) bool { return x.Dest.Kind == DestAny }
	switch d.Kind {
	case DestNetwork:
		if r := find(isNet); r != nil {
			return r
		}
		if r := find(isCountry(d.MCC)); r != nil {
			return r
		}
	case DestCountry:
		if r := find(isCountry(d.MCC)); r != nil {
			return r
		}
	}
	return find(isAny)
}

// deleteEffect says where a rule's messages go when it is deleted (the
// prototype's deleteEffect): "CMPAK (Zong) numbers will go to rule 3
// 'Pakistan' (the cheapest of Vendor-C and Vendor-D)."
func deleteEffect(account string, rules []PlanRule, id int64, n VendorNames) string {
	var r *PlanRule
	var rest []PlanRule
	for i := range rules {
		if rules[i].ID == id {
			r = &rules[i]
			continue
		}
		if rules[i].On {
			rest = append(rest, rules[i])
		}
	}
	if r == nil {
		return ""
	}
	if r.Direction == "MO" {
		return "Incoming messages only this rule takes will no longer be delivered to " + account + "."
	}
	if len(rest) == 0 {
		return account + " will have no rule and cannot send: every message is refused (503 NO_ROUTE)."
	}
	if r.Special() {
		return "Messages it catches will go to the rule for their number, like other messages."
	}
	var next *PlanRule
	for i := range rest {
		x := rest[i]
		if !x.Special() && x.PlanError == "" && reachCovers(x.reach(), r.reach()) {
			next = &rest[i]
			break
		}
	}
	what := "Messages that no other rule catches"
	switch r.Dest.Kind {
	case DestNetwork:
		what = r.Dest.Short() + " numbers"
	case DestCountry:
		what = r.Dest.Short() + " numbers that no network rule takes"
	case "":
		what = "Numbers on its networks"
	}
	if next == nil {
		return what + " will be refused (503 NO_ROUTE): no other rule catches them."
	}
	lv := next.Levels()
	who := "no vendor"
	if len(lv) > 0 {
		who = n.LevelPlain(lv[0], next.Method())
	}
	return fmt.Sprintf("%s will go to rule %d '%s' (%s).", what, posOf(rules, next.ID), next.Name, who)
}

// switchEffect says what switching a rule off or on does, and whether to
// ask first: turning off the last rule that is on stops the account sending.
func switchEffect(account string, rules []PlanRule, id int64, on bool) (string, bool) {
	var r *PlanRule
	others := 0
	for i := range rules {
		if rules[i].ID == id {
			r = &rules[i]
		} else if rules[i].On {
			others++
		}
	}
	if r == nil {
		return "", false
	}
	if on {
		return fmt.Sprintf("Turned on '%s'.", r.Name), false
	}
	if r.Direction != "MO" && others == 0 {
		return fmt.Sprintf("Turn off '%s'? %s will stop sending: no other rule is on, so every new message is refused (503 NO_ROUTE).", r.Name, account), true
	}
	s := fmt.Sprintf("Turned off '%s'.", r.Name)
	if r.Direction != "MO" && !r.Special() && r.Dest.Kind != DestAny && r.Dest.Kind != "" {
		var rest []PlanRule
		for _, x := range rules {
			if x.ID != id {
				rest = append(rest, x)
			}
		}
		if nx := inheritFor(rest, r.Dest); nx != nil {
			s += fmt.Sprintf(" %s numbers use rule %d '%s'.", r.Dest.Short(), posOf(rules, nx.ID), nx.Name)
		} else {
			s += " " + r.Dest.Short() + " numbers are refused (503 NO_ROUTE)."
		}
	}
	return s, false
}

// AutoName is the name a rule gets from its choices ("Zong", "Pakistan",
// "Everything else", "MyBank OTP"), as the prototype's autoName.
func AutoName(d Destination, conds []Cond) string {
	if len(conds) == 0 {
		switch d.Kind {
		case "":
			return "New rule"
		case DestAny:
			return "Everything else"
		}
		return d.Short()
	}
	var parts []string
	for _, c := range conds {
		switch {
		case c.Field == FieldPriority:
			parts = append(parts, "Priority "+strings.Join(c.digits(), ","))
		case c.Value == "":
		case c.Op == OpPattern:
			parts = append(parts, strings.ToUpper(fieldWord(c.Field)[:1])+fieldWord(c.Field)[1:]+" pattern")
		case c.Op == OpOneOf:
			parts = append(parts, strings.Join(splitValues(c.Value), "/"))
		case c.Op == OpStarts:
			parts = append(parts, c.Value+"…")
		case c.Op == OpContains && c.Field == FieldSender:
			parts = append(parts, "…"+c.Value+"…")
		default:
			parts = append(parts, c.Value)
		}
	}
	s := strings.Join(parts, " ")
	if s == "" {
		s = "Special case"
	}
	if d.Kind != DestAny && d.Kind != "" {
		s += " · " + d.Short()
	}
	if r := []rune(s); len(r) > 128 {
		s = string(r[:128])
	}
	return s
}
