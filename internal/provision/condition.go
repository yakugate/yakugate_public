package provision

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Conditions and changes of a routing rule, over the stored pattern shape
// (routing-plans 2026-09-30). [SCOPE P14-ROUTE]
//
// The engine stores ONE {match, replace} per field (sender_regex,
// receiver_regex, keyword_regex, priority_regex). A match alone only
// filters; a match with a replacement filters AND rewrites what it matched
// (Ozeki style, ParseCondition). So a field holds at most one condition and
// one change, and the two must fit in one pattern:
//
//	condition only           → {match: <condition>}
//	change only              → {match: <the change's pattern>, replace}
//	condition + "change to"  → {match: <condition, widened to the whole value>, replace}
//
// The shapes are the portal's (p13/src/modules/rules/conditions.js:
// conditionPattern, changePattern, parseCondition, parseChange,
// rowsFromRule, ruleFromRows) — CHANGE TOGETHER, so a rule written by one
// door opens in the other as the same rows. Two additions here, both stored
// in shapes the portal already reads (as a pattern or a raw change):
// "ignore capitals" is a leading (?i), and "replace what matched" beside a
// "contains"/"starts with" condition is the condition's own pattern with the
// replacement (the prototype's "Replace what matched with").

// The fields a rule can test or change.
const (
	FieldSender   = "sender"
	FieldReceiver = "receiver" // a destination is chosen instead; kept on old rules
	FieldText     = "text"
	FieldPriority = "priority"
)

// Condition tests. Priority takes is / oneof over the digits 1..9.
const (
	OpIs       = "is"
	OpOneOf    = "oneof"
	OpStarts   = "starts"
	OpContains = "contains"
	OpPattern  = "pattern"
)

// Change actions (the portal's words).
const (
	ChangeSet          = "set"          // change to
	ChangeReplace      = "replace"      // replace (what matched, or a word) with
	ChangeAddPrefix    = "addprefix"    // add at start
	ChangeAddSuffix    = "addsuffix"    // add at end (text)
	ChangeRemovePrefix = "removeprefix" // remove from start (then add)
	ChangeRaw          = "raw"          // pattern → replacement
)

// Cond is one "Only when" row: a special case's sender, text or priority
// test (receiver only on rules made before 2026-09-30).
type Cond struct {
	Field      string // sender | text | priority | receiver
	Op         string // is | oneof | starts | contains | pattern
	Value      string // oneof: comma separated; priority: digits "1, 2"; pattern: the regex
	IgnoreCase bool   // stored as a leading (?i)
}

// Change is one "Change before sending" row: what the vendor gets instead of
// what the client sent. Priority cannot be changed.
type Change struct {
	Field  string // sender | receiver | text
	Action string // set | replace | addprefix | addsuffix | removeprefix | raw
	Value  string // set: the new value; replace: what to look for ("" = what the condition matched); add*: what to add; removeprefix: what to remove; raw: the pattern
	With   string // replace: the replacement ("" = remove it); removeprefix: what to add instead (optional); raw: the replacement
}

// CondOps lists the tests a field offers, in the order a screen offers them.
func CondOps(field string) []string {
	switch field {
	case FieldSender:
		return []string{OpIs, OpStarts, OpContains, OpOneOf, OpPattern}
	case FieldText:
		return []string{OpContains, OpStarts, OpIs, OpPattern}
	case FieldPriority:
		return []string{OpIs, OpOneOf, OpPattern}
	case FieldReceiver:
		return []string{OpStarts, OpIs, OpOneOf, OpPattern}
	}
	return nil
}

// ChangeActions lists the changes a field offers when it has NO condition.
// Beside a condition only "change to" fits, and "replace what matched" beside
// "contains" or "starts with" (ChangeActionsWith).
func ChangeActions(field string) []string {
	switch field {
	case FieldSender:
		return []string{ChangeSet, ChangeReplace, ChangeAddPrefix, ChangeRemovePrefix, ChangeRaw}
	case FieldReceiver:
		return []string{ChangeRemovePrefix, ChangeAddPrefix, ChangeReplace, ChangeSet, ChangeRaw}
	case FieldText:
		return []string{ChangeReplace, ChangeAddPrefix, ChangeAddSuffix, ChangeSet, ChangeRaw}
	}
	return nil
}

// ChangeActionsWith is what a field with this condition may carry as its change.
func ChangeActionsWith(c *Cond) []string {
	if c == nil {
		return nil
	}
	if c.Field == FieldPriority {
		return nil
	}
	if c.Op == OpContains || c.Op == OpStarts {
		return []string{ChangeSet, ChangeReplace}
	}
	return []string{ChangeSet}
}

// OpWords and ActionWords are the menu words.
func OpWords(op string) string {
	return map[string]string{OpIs: "is", OpOneOf: "is one of", OpStarts: "starts with",
		OpContains: "contains", OpPattern: "matches pattern"}[op]
}

func ActionWords(a string) string {
	return map[string]string{ChangeSet: "change to", ChangeReplace: "replace", ChangeAddPrefix: "add at start",
		ChangeAddSuffix: "add at end", ChangeRemovePrefix: "remove from start", ChangeRaw: "pattern → replacement"}[a]
}

func fieldWord(f string) string {
	switch f {
	case FieldText:
		return "text"
	case FieldReceiver:
		return "receiver"
	case FieldPriority:
		return "priority"
	}
	return "sender"
}

// group1 is Go's "the first group" in a replacement.
const group1 = "${1}"

var groupRefRe = regexp.MustCompile(`\$\{?\w`)

func groupRef(r string) bool { return groupRefRe.MatchString(r) }

// splitValues reads a comma-separated list, dropping blanks.
func splitValues(v string) []string {
	var out []string
	for _, x := range strings.Split(v, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}

// literal is the text a pattern spells, or ok=false when it uses any regex
// syntax (conditions.js literal).
func literal(p string) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		c := p[i]
		if c == '\\' {
			if i+1 >= len(p) || !strings.ContainsRune(`.*+?^${}()|[]\/-`, rune(p[i+1])) {
				return "", false
			}
			b.WriteByte(p[i+1])
			i++
			continue
		}
		if strings.IndexByte(".*+?^${}()|[]", c) >= 0 {
			return "", false
		}
		b.WriteByte(c)
	}
	return b.String(), true
}

// basePattern is the condition without its (?i).
func (c Cond) basePattern() string {
	v := c.Value
	switch c.Op {
	case OpIs:
		return "^" + regexp.QuoteMeta(v) + "$"
	case OpStarts:
		return "^" + regexp.QuoteMeta(v)
	case OpContains:
		return regexp.QuoteMeta(v)
	case OpOneOf:
		vals := splitValues(v)
		if c.Field == FieldPriority {
			sort.Strings(vals)
		}
		q := make([]string, len(vals))
		for i, x := range vals {
			q[i] = regexp.QuoteMeta(x)
		}
		if c.Field == FieldPriority && len(q) == 1 {
			return "^" + q[0] + "$"
		}
		return "^(" + strings.Join(q, "|") + ")$"
	}
	return v
}

// Pattern is the condition as the engine stores it.
func (c Cond) Pattern() string {
	p := c.basePattern()
	if c.IgnoreCase && c.Field != FieldPriority && c.Op != OpPattern {
		return "(?i)" + p
	}
	return p
}

// whole widens a condition to the whole value, so "change to" replaces all
// of it (conditions.js whole).
func (c Cond) whole() string {
	p := c.basePattern()
	switch c.Op {
	case OpIs, OpOneOf:
	case OpStarts:
		p += ".*$"
	default:
		p = "(?s)^.*(?:" + p + ").*$"
	}
	if c.IgnoreCase && c.Op != OpPattern {
		p = "(?i)" + p
	}
	return p
}

// parseCond reads a stored match as a condition row (conditions.js
// parseCondition, plus a leading (?i) read as "ignore capitals").
func parseCond(field, match string) Cond {
	if strings.HasPrefix(match, "(?i)") && field != FieldPriority {
		if c := parseCondPlain(field, strings.TrimPrefix(match, "(?i)")); c.Op != OpPattern {
			c.IgnoreCase = true
			return c
		}
	}
	return parseCondPlain(field, match)
}

var oneOfRe = regexp.MustCompile(`^\^\((.*)\)\$$`)

func parseCondPlain(field, m string) Cond {
	ops := map[string]bool{}
	for _, o := range CondOps(field) {
		ops[o] = true
	}
	if g := oneOfRe.FindStringSubmatch(m); g != nil && ops[OpOneOf] {
		var parts []string
		ok := true
		for _, x := range strings.Split(g[1], "|") {
			l, lok := literal(x)
			if !lok || l == "" {
				ok = false
				break
			}
			parts = append(parts, l)
		}
		if ok {
			return Cond{Field: field, Op: OpOneOf, Value: strings.Join(parts, ", ")}
		}
	}
	if strings.HasPrefix(m, "^") && strings.HasSuffix(m, "$") && !strings.HasSuffix(m, `\$`) && len(m) >= 2 && ops[OpIs] {
		if l, ok := literal(m[1 : len(m)-1]); ok {
			return Cond{Field: field, Op: OpIs, Value: l}
		}
	}
	if strings.HasPrefix(m, "^") && ops[OpStarts] {
		if l, ok := literal(m[1:]); ok {
			return Cond{Field: field, Op: OpStarts, Value: l}
		}
	}
	if ops[OpContains] {
		if l, ok := literal(m); ok && l != "" {
			return Cond{Field: field, Op: OpContains, Value: l}
		}
	}
	return Cond{Field: field, Op: OpPattern, Value: m}
}

// unwhole reads a condition widened to the whole value (whole) back as the
// condition: "^My.*$" is "starts with My", "(?s)^.*(?:OTP).*$" is
// "contains OTP". The portal reads the two as a raw change; both store the
// same thing.
func unwhole(field, match string) (Cond, bool) {
	allowed := func(op string) bool {
		for _, o := range CondOps(field) {
			if o == op {
				return true
			}
		}
		return false
	}
	m, ci := match, false
	if strings.HasPrefix(m, "(?i)") {
		m, ci = m[4:], true
	}
	var c Cond
	switch {
	case strings.HasPrefix(m, "(?s)^.*(?:") && strings.HasSuffix(m, ").*$"):
		inner := m[len("(?s)^.*(?:") : len(m)-len(").*$")]
		if l, ok := literal(inner); ok && l != "" && allowed(OpContains) {
			c = Cond{Field: field, Op: OpContains, Value: l, IgnoreCase: ci}
		} else if !ci && allowed(OpPattern) {
			c = Cond{Field: field, Op: OpPattern, Value: inner}
		}
	case strings.HasPrefix(m, "^") && strings.HasSuffix(m, ".*$") && !strings.HasSuffix(m, `\.*$`):
		if l, ok := literal(m[1 : len(m)-3]); ok && l != "" && allowed(OpStarts) {
			c = Cond{Field: field, Op: OpStarts, Value: l, IgnoreCase: ci}
		}
	default:
		if pc := parseCond(field, match); pc.Op == OpIs || pc.Op == OpOneOf {
			c = pc
		}
	}
	if c.Op == "" || c.whole() != match {
		return Cond{}, false
	}
	return c, true
}

// matchesAll reports a stored match that every value passes: a change-only
// pattern (^.*$, ^, $, "remove from start") or none at all. It is not a
// condition, and a rule carrying only such patterns is not a special case.
func matchesAll(m string) bool {
	switch m {
	case "", "^", "$", "^.*$", "(?s)^.*$", ".*", "(?s).*", "(?s)^.*()$", "^.*", "(?s)^.*":
		return true
	}
	return removePrefixRe.MatchString(m)
}

var removePrefixRe = regexp.MustCompile(`^\(\?s\)\^\(\?:(.*)\)\?\(\.\*\)\$$`)

// parseChange reads a stored {match, replace} as a change row (conditions.js
// parseChange). "remove from start, then add" is read too: the portal opens
// it as a raw change, which stores the same thing.
func parseChange(field, m, r string) Change {
	if (m == "^.*$" || m == "(?s)^.*$") && !groupRef(r) {
		return Change{Field: field, Action: ChangeSet, Value: r}
	}
	if m == "^" && r != "" {
		return Change{Field: field, Action: ChangeAddPrefix, Value: r}
	}
	if m == "$" && r != "" && field == FieldText {
		return Change{Field: field, Action: ChangeAddSuffix, Value: r}
	}
	if r == group1 && m == "(?s)^.*()$" {
		return Change{Field: field, Action: ChangeSet}
	}
	if r == group1 && strings.HasSuffix(m, "()") {
		if l, ok := literal(strings.TrimSuffix(m, "()")); ok && l != "" {
			return Change{Field: field, Action: ChangeReplace, Value: l}
		}
	}
	if g := removePrefixRe.FindStringSubmatch(m); g != nil && strings.HasSuffix(r, group1) {
		then := strings.TrimSuffix(r, group1)
		if l, ok := literal(g[1]); ok && !strings.Contains(then, "$") {
			return Change{Field: field, Action: ChangeRemovePrefix, Value: l, With: then}
		}
	}
	if l, ok := literal(m); ok && l != "" && !groupRef(r) {
		return Change{Field: field, Action: ChangeReplace, Value: l, With: r}
	}
	return Change{Field: field, Action: ChangeRaw, Value: m, With: r}
}

// ReadField reads one stored field as its condition and its change (either
// may be nil). A change that only some values pass (a word replaced, a raw
// pattern) comes back WITH the condition it implies, because the engine
// skips the rule for a value its pattern does not match — so it is one.
func ReadField(field, match, replace string) (*Cond, *Change) {
	if match == "" && replace == "" {
		return nil, nil
	}
	if replace == "" || field == FieldPriority {
		if matchesAll(match) {
			return nil, nil
		}
		c := parseCond(field, match)
		return &c, nil
	}
	ch := parseChange(field, match, replace)
	switch ch.Action {
	case ChangeRaw:
		if c, ok := unwhole(field, match); ok && !groupRef(replace) {
			return &c, &Change{Field: field, Action: ChangeSet, Value: replace}
		}
		c := parseCond(field, match)
		if (c.Op == OpContains || c.Op == OpStarts) && c.Pattern() == match && !groupRef(replace) {
			return &c, &Change{Field: field, Action: ChangeReplace, With: replace}
		}
		if matchesAll(match) {
			return nil, &ch
		}
		return &Cond{Field: field, Op: OpPattern, Value: match}, &ch
	case ChangeReplace:
		return &Cond{Field: field, Op: OpContains, Value: ch.Value}, &ch
	}
	return nil, &ch
}

// FieldParts is one field's condition and change as the stored {match,
// replace} (conditions.js ruleFromRows + changePattern). Either may be nil.
func FieldParts(field string, c *Cond, ch *Change) (match, replace string, err error) {
	if c != nil && c.Field == "" {
		c.Field = field
	}
	if ch != nil && ch.Field == "" {
		ch.Field = field
	}
	if c != nil {
		if err := c.Check(); err != nil {
			return "", "", err
		}
	}
	if ch == nil {
		if c == nil {
			return "", "", nil
		}
		return c.Pattern(), "", nil
	}
	if field == FieldPriority {
		return "", "", refusef("the priority cannot be changed: it only decides which messages a rule catches")
	}
	ok := false
	for _, a := range ChangeActions(field) {
		ok = ok || a == ch.Action
	}
	if !ok {
		return "", "", refusef("%q is not a change the %s offers (%s)", ch.Action, fieldWord(field), strings.Join(ChangeActions(field), ", "))
	}
	if c != nil {
		switch {
		case ch.Action == ChangeSet:
			if ch.Value == "" {
				return "", "", refusef(`"change to" beside a %s condition needs a value`, fieldWord(field))
			}
			return c.whole(), ch.Value, nil
		case ch.Action == ChangeReplace && (c.Op == OpContains || c.Op == OpStarts) && (ch.Value == "" || ch.Value == c.Value):
			if ch.With == "" {
				if c.Op == OpContains && !c.IgnoreCase {
					return regexp.QuoteMeta(c.Value) + "()", group1, nil
				}
				return c.Pattern() + "()", group1, nil
			}
			return c.Pattern(), ch.With, nil
		case ch.Action == ChangeReplace && c.Op == OpContains && strings.EqualFold(ch.Value, c.Value):
			return c.Pattern(), ch.With, nil
		case ch.Action == ChangeRaw && (c.Op == OpPattern && c.Value == ch.Value):
			return ch.Value, ch.With, nil
		}
		return "", "", refusef(`the %s has a condition and a %q change. One field holds one pattern, so only "change to" can sit beside a condition (and "replace what matched" beside "contains" or "starts with"). Remove one of them`,
			fieldWord(field), ActionWords(ch.Action))
	}
	switch ch.Action {
	case ChangeSet:
		if ch.Value == "" {
			return "(?s)^.*()$", group1, nil
		}
		if field == FieldText {
			return "(?s)^.*$", ch.Value, nil
		}
		return "^.*$", ch.Value, nil
	case ChangeReplace:
		if ch.Value == "" {
			return "", "", refusef("the %s change has nothing to look for: type the words to replace", fieldWord(field))
		}
		if ch.With == "" {
			return regexp.QuoteMeta(ch.Value) + "()", group1, nil
		}
		return regexp.QuoteMeta(ch.Value), ch.With, nil
	case ChangeAddPrefix, ChangeAddSuffix:
		if ch.Value == "" {
			return "", "", refusef("type what to add to the %s", fieldWord(field))
		}
		if ch.Action == ChangeAddPrefix {
			return "^", ch.Value, nil
		}
		return "$", ch.Value, nil
	case ChangeRemovePrefix:
		if ch.Value == "" {
			return "", "", refusef("type what to remove from the start of the %s", fieldWord(field))
		}
		if strings.Contains(ch.With, "$") {
			return "", "", refusef("what is added after removing cannot contain $")
		}
		return "(?s)^(?:" + regexp.QuoteMeta(ch.Value) + ")?(.*)$", ch.With + group1, nil
	case ChangeRaw:
		if ch.Value == "" {
			return "", "", refusef("the %s change has no pattern", fieldWord(field))
		}
		if _, err := regexp.Compile(ch.Value); err != nil {
			return "", "", refusef("this pattern is not valid: %v", err)
		}
		return ch.Value, ch.With, nil
	}
	return "", "", refusef("%q is not a change", ch.Action)
}

// FieldToText is one field as the text a Route carries (FromPattern, Keyword,
// ToPattern, PriorityPattern): Ozeki style, "/match/replace/" when it
// rewrites. CreateRoute and UpdateRoute store it as {match, replace}.
func FieldToText(field string, c *Cond, ch *Change) (string, error) {
	m, r, err := FieldParts(field, c, ch)
	if err != nil {
		return "", err
	}
	return OzekiText(m, r), nil
}

// OzekiText writes a stored {match, replace} back as the text
// ParseCondition reads.
func OzekiText(match, replace string) string {
	if replace == "" && !strings.HasPrefix(match, "/") {
		return match
	}
	esc := func(s string) string { return strings.ReplaceAll(s, "/", `\/`) }
	if replace == "" {
		return "/" + esc(match) + "/"
	}
	return "/" + esc(match) + "/" + esc(replace) + "/"
}

// TextToField reads a Route's field text back as rows.
func TextToField(field, text string) (*Cond, *Change, error) {
	m, r, err := ParseCondition(text)
	if err != nil {
		return nil, nil, err
	}
	c, ch := ReadField(field, m, r)
	return c, ch, nil
}

// Check says what is wrong with a condition row, in the screen's words.
func (c Cond) Check() error {
	if c.Field == FieldPriority {
		ds := c.digits()
		if c.Op == OpPattern {
			if _, err := regexp.Compile(c.Value); err != nil {
				return refusef("this pattern is not valid: %v", err)
			}
		} else {
			for _, v := range splitValues(c.Value) {
				if n, err := strconv.Atoi(v); err != nil || n < 1 || n > 9 || len(v) != 1 {
					return refusef("priority %q is not one of 1 to 9 (1 is the most urgent)", v)
				}
			}
		}
		switch len(ds) {
		case 0:
			return refusef("pick at least one priority (1 to 9; a message sent without one counts as 9)")
		case 9:
			return refusef("this condition fits every message. Remove it")
		}
		return nil
	}
	ok := false
	for _, o := range CondOps(c.Field) {
		ok = ok || o == c.Op
	}
	if !ok {
		return refusef("%q is not a test the %s offers (%s)", c.Op, fieldWord(c.Field), strings.Join(CondOps(c.Field), ", "))
	}
	if strings.TrimSpace(c.Value) == "" {
		if c.Field == FieldText {
			return refusef("type the text to look for")
		}
		return refusef("type what the %s must be", fieldWord(c.Field))
	}
	if c.Op == OpOneOf && len(splitValues(c.Value)) == 0 {
		return refusef("type at least one value")
	}
	re, err := regexp.Compile(c.Pattern())
	if err != nil {
		return refusef("this pattern is not valid: %v. The gateway uses Go's regular expressions, which have no look-behind or back-references", err)
	}
	all := true
	for _, s := range []string{"", "a", "MyBank", "Your OTP 1", "zz9", "923001234567"} {
		all = all && re.MatchString(s)
	}
	if all {
		return refusef("this condition fits every message. Remove it")
	}
	return nil
}

// digits is the priorities 1..9 a priority condition takes, by the engine's
// own test: the pattern against the number.
func (c Cond) digits() []string {
	re, err := regexp.Compile(c.Pattern())
	if err != nil {
		return nil
	}
	var out []string
	for d := 1; d <= 9; d++ {
		if s := strconv.Itoa(d); re.MatchString(s) {
			out = append(out, s)
		}
	}
	return out
}

// Matches is the engine's test of this condition against one value
// (priority: the message's priority, 0 counting as 9).
func (c Cond) Matches(value string) bool {
	re, err := regexp.Compile(c.Pattern())
	return err == nil && re.MatchString(value)
}

func orWords(a []string) string {
	switch len(a) {
	case 0:
		return ""
	case 1:
		return a[0]
	}
	return strings.Join(a[:len(a)-1], ", ") + " or " + a[len(a)-1]
}

func andWords(a []string) string {
	switch len(a) {
	case 0:
		return ""
	case 1:
		return a[0]
	}
	return strings.Join(a[:len(a)-1], ", ") + " and " + a[len(a)-1]
}

func (c Cond) valueWords() string {
	switch c.Op {
	case OpOneOf:
		return orWords(splitValues(c.Value))
	case OpPattern:
		return "/" + c.Value + "/"
	}
	return c.Value
}

// Words is the condition in plain words: "sender is MyBank", "text contains
// OTP (any capitals)", "priority 1 or 2".
func (c Cond) Words() string {
	if c.Field == FieldPriority {
		return "priority " + orWords(c.digits())
	}
	s := fieldWord(c.Field) + " " + OpWords(c.Op) + " " + c.valueWords()
	if c.IgnoreCase {
		s += " (any capitals)"
	}
	return s
}

// Negation says why a value fails the condition: "text does not contain OTP".
func (c Cond) Negation() string {
	if c.Field == FieldPriority {
		return "priority is not " + orWords(c.digits())
	}
	f := fieldWord(c.Field)
	switch c.Op {
	case OpIs, OpOneOf:
		return f + " is not " + c.valueWords()
	case OpStarts:
		return f + " does not start with " + c.Value
	case OpContains:
		return f + " does not contain " + c.Value
	}
	return f + " does not match /" + c.Value + "/"
}

// Phrase is the condition inside a sentence: "from sender MyBank", "whose
// text contains OTP (any capitals)", "with priority 1".
func (c Cond) Phrase() string {
	caps := ""
	if c.IgnoreCase {
		caps = " (any capitals)"
	}
	v := c.valueWords()
	switch c.Field {
	case FieldPriority:
		return "with priority " + orWords(c.digits())
	case FieldSender:
		switch c.Op {
		case OpIs, OpOneOf:
			return "from sender " + v + caps
		case OpStarts:
			return "from a sender starting with " + v + caps
		case OpContains:
			return "from a sender containing " + v + caps
		}
		return "from a sender matching " + v
	case FieldReceiver:
		switch c.Op {
		case OpIs, OpOneOf:
			return "to number " + v
		case OpStarts:
			return "to a number starting with " + v
		}
		return "to a number matching " + v
	}
	switch c.Op {
	case OpContains:
		return "whose text contains " + v + caps
	case OpStarts:
		return "whose text starts with " + v + caps
	case OpIs:
		return "whose text is " + v + caps
	}
	return "whose text matches " + v
}

// Words is the change in a sentence: "The sender is changed to 8558."
func (ch Change) Words() string {
	f := fieldWord(ch.Field)
	switch ch.Action {
	case ChangeSet:
		if ch.Value == "" {
			return "The " + f + " is emptied."
		}
		return "The " + f + " is changed to " + ch.Value + "."
	case ChangeAddPrefix:
		return "The " + f + " gets " + ch.Value + " added at the start."
	case ChangeAddSuffix:
		return "The " + f + " gets " + ch.Value + " added at the end."
	case ChangeReplace:
		what := "what matched"
		if ch.Value != "" {
			what = ch.Value
		}
		with := "nothing"
		if ch.With != "" {
			with = ch.With
		}
		return "In the " + f + ", " + what + " is replaced with " + with + "."
	case ChangeRemovePrefix:
		s := "The " + f + " loses " + ch.Value + " at the start"
		if ch.With != "" {
			s += " and gets " + ch.With + " instead"
		}
		return s + "."
	}
	return fmt.Sprintf("The %s is rewritten: /%s/ → %s.", f, ch.Value, ch.With)
}

// ── what covers what (the prototype's condCovers / condsMeet) ──────────────

type strSet struct {
	kind string // set | pre | sub | pat
	vals []string
	val  string
}

func (c Cond) strSet(ci bool) strSet {
	n := func(s string) string {
		if ci {
			return strings.ToLower(s)
		}
		return s
	}
	switch c.Op {
	case OpIs:
		return strSet{kind: "set", vals: []string{n(c.Value)}}
	case OpOneOf:
		var vs []string
		for _, v := range splitValues(c.Value) {
			vs = append(vs, n(v))
		}
		return strSet{kind: "set", vals: vs}
	case OpStarts:
		return strSet{kind: "pre", val: n(c.Value)}
	case OpContains:
		return strSet{kind: "sub", val: n(c.Value)}
	}
	return strSet{kind: "pat", val: c.Value}
}

// condCovers: a catches every value b catches.
func condCovers(a, b Cond) bool {
	if a.Field == FieldPriority {
		have := map[string]bool{}
		for _, d := range a.digits() {
			have[d] = true
		}
		for _, d := range b.digits() {
			if !have[d] {
				return false
			}
		}
		return true
	}
	if !a.IgnoreCase && b.IgnoreCase {
		return false
	}
	A := a.strSet(a.IgnoreCase)
	B := b.strSet(a.IgnoreCase)
	if A.kind == "pat" || B.kind == "pat" {
		return A.kind == "pat" && B.kind == "pat" && a.Value == b.Value && a.IgnoreCase == b.IgnoreCase
	}
	acc := func(s string) bool {
		switch A.kind {
		case "set":
			for _, v := range A.vals {
				if v == s {
					return true
				}
			}
			return false
		case "pre":
			return strings.HasPrefix(s, A.val)
		}
		return strings.Contains(s, A.val)
	}
	switch B.kind {
	case "set":
		for _, v := range B.vals {
			if !acc(v) {
				return false
			}
		}
		return true
	case "pre":
		switch A.kind {
		case "pre":
			return strings.HasPrefix(B.val, A.val)
		case "sub":
			return strings.Contains(B.val, A.val)
		}
		return false
	case "sub":
		return A.kind == "sub" && strings.Contains(B.val, A.val)
	}
	return false
}

// condsMeet: one value could pass both.
func condsMeet(a, b Cond) bool {
	if a.Field == FieldPriority {
		have := map[string]bool{}
		for _, d := range a.digits() {
			have[d] = true
		}
		for _, d := range b.digits() {
			if have[d] {
				return true
			}
		}
		return false
	}
	ci := a.IgnoreCase || b.IgnoreCase
	A, B := a.strSet(ci), b.strSet(ci)
	if A.kind == "pat" || B.kind == "pat" {
		return true
	}
	if A.kind == "set" && B.kind == "set" {
		for _, x := range A.vals {
			for _, y := range B.vals {
				if x == y {
					return true
				}
			}
		}
		return false
	}
	setMeets := func(X, O strSet) bool {
		for _, x := range X.vals {
			if (O.kind == "pre" && strings.HasPrefix(x, O.val)) || (O.kind != "pre" && strings.Contains(x, O.val)) {
				return true
			}
		}
		return false
	}
	if A.kind == "set" {
		return setMeets(A, B)
	}
	if B.kind == "set" {
		return setMeets(B, A)
	}
	if A.kind == "pre" && B.kind == "pre" {
		return strings.HasPrefix(A.val, B.val) || strings.HasPrefix(B.val, A.val)
	}
	return true
}
