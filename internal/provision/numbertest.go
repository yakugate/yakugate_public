package provision

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/msisdn"
	"github.com/yakugate/yakugate/internal/netcode"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// Test a number (rule-creation-ux.md section 11): which rule of a plan
// catches a number, why each rule above passed it, which vendor sends it,
// and what happens when they cannot. The number is placed from the
// operators' number series, and each rule is judged by its STORED patterns
// with Go's regexp — the engine's own test (pipeline routeMismatch), not a
// reading of them. Nothing is sent. [SCOPE P14-ROUTE]

// TestInput is one test.
type TestInput struct {
	Number   string
	Sender   string  // "" = the account's default sender
	Text     string  // "" = not typed: a text condition is "not judged"
	Priority int     // 0 = not sent, counts as 9
	Down     []int64 // vendors to treat as unable to send ("What if a vendor is down?")
	// Draft tests the plan with this rule in it — added (EditingID 0) or in
	// place of rule EditingID ("Test with: your changes").
	Draft     *Route
	EditingID int64
}

// TestRow is one rule's answer to the number.
type TestRow struct {
	Position int
	ID       int64
	Name     string
	Catches  bool
	Unjudged bool
	Why      string // why it passed ("not Pakistan", "text does not contain OTP"), or "not judged: …"
}

// TestVendor is one vendor in the order the message is offered.
type TestVendor struct {
	ID    int64
	Name  string
	State string // sends now | sends 70% | one of these, by Cheapest at send time | if needed | down, skipped | switched off, skipped | already tried
}

// TestStep is one level of one catching rule, in the order tried.
type TestStep struct {
	Position int    // the rule's place
	Rule     string // its name
	Level    string // Main, Backup 1…
	Next     bool   // a rule after the one that catches it
	Way      string // "direct", "shared", "cheapest"…
	Vendors  []TestVendor
}

// NumberTest is the whole answer.
type NumberTest struct {
	Place      NumberPlace
	Judged     string   // what the rules were judged on: "Pakistan · CMPAK (Zong) · 410-004", "country not found"
	Notes      []string // HLR check off, the default sender used
	Rows       []TestRow
	Caught     int // the position of the rule that catches it; 0 = none
	Steps      []TestStep
	Headline   string
	Level      string // ok | warn | bad
	IfNone     string // "If none can send: accepted and retried. 5 more rounds, …"
	AddFor     *Destination
	AddNote    string   // "CMPAK (Zong) numbers have no rule of their own."
	VendorGets []string // "Sender: MyBank → 8558"
	Verdict    string   // with a Draft: what this rule does with the number
	Lines      []string // the answer as lines of text, ready to print
}

type testMsg struct {
	number               string
	mcc, mnc             string
	netName, country     string
	sender               string
	senderTyped          bool
	text                 string
	textTyped            bool
	priority             int
	unknownCountryReason string
}

// levelsFor is where a rule sends this message: its network's own vendors
// when it has a tab for it, else Main and backups (refdata LevelsFor).
func levelsFor(r PlanRule, mcc, mnc string) []sendplan.Level {
	if r.Plan != nil && mcc != "" && mnc != "" && len(r.Plan.Exceptions) > 0 {
		for _, code := range sortedExceptionCodes(r.Plan) {
			if netcode.Matches(code, mcc, mnc) {
				return r.Plan.Exceptions[code]
			}
		}
	}
	return r.Levels()
}

func containsNet(list []string, mcc, mnc string) bool {
	for _, v := range list {
		if netcode.Matches(v, mcc, mnc) {
			return true
		}
	}
	return false
}

// catchWhy judges one rule the way pipeline.routeMismatch does.
func catchWhy(r PlanRule, m testMsg) (ok, unjudged bool, why string) {
	if !r.On {
		return false, false, "turned off"
	}
	if r.PlanError != "" {
		return false, false, "its send plan cannot be read, so the gateway skips it"
	}
	nets := cleanCodes(r.Networks)
	if len(nets) > 0 {
		switch {
		case m.mcc == "" && m.mnc == "":
			if !r.AcceptUnknown {
				return false, false, "the country is not found"
			}
		case !containsNet(nets, m.mcc, m.mnc):
			switch r.Dest.Kind {
			case DestCountry:
				return false, false, "not " + r.Dest.Short()
			case DestNetwork:
				switch {
				case m.mnc != "":
					return false, false, orNet(m.netName, m.mcc+"-"+m.mnc) + " is not " + r.Dest.Short()
				case m.mcc == r.Dest.MCC:
					return false, false, "the network is not found, so network rules do not apply"
				}
				return false, false, "not " + r.Dest.Short()
			}
			return false, false, "this number's network is not one of the rule's networks (" + strings.Join(nets, ", ") + ")"
		}
	}
	if r.Plan != nil && r.Plan.NetworksOnly() && len(levelsFor(r, m.mcc, m.mnc)) == 0 {
		return false, false, "it has vendors only for networks " + strings.Join(sortedExceptionCodes(r.Plan), ", ")
	}
	for _, f := range []string{FieldSender, FieldReceiver, FieldText, FieldPriority} {
		st, has := r.Stored[f]
		if !has || st[0] == "" {
			continue
		}
		value := ""
		switch f {
		case FieldSender:
			if m.sender == "" {
				if c, _ := ReadField(f, st[0], st[1]); c != nil {
					return false, true, "not judged: type a sender to test it"
				}
			}
			value = m.sender
		case FieldReceiver:
			value = m.number
		case FieldText:
			if !m.textTyped {
				if c, _ := ReadField(f, st[0], st[1]); c != nil {
					return false, true, "not judged: type a text to test it"
				}
			}
			value = m.text
		case FieldPriority:
			p := m.priority
			if p == 0 {
				p = 9
			}
			value = strconv.Itoa(p)
		}
		re, err := regexp.Compile(st[0])
		if err != nil {
			return false, false, "its " + fieldWord(f) + " pattern cannot be read, so the gateway skips it"
		}
		if !re.MatchString(value) {
			if c, _ := ReadField(f, st[0], st[1]); c != nil {
				return false, false, c.Negation()
			}
			return false, false, fieldWord(f) + " does not match /" + st[0] + "/"
		}
	}
	return true, false, ""
}

func orNet(name, code string) string {
	if name != "" {
		return name
	}
	return "network " + code
}

// evaluate walks the plan for one message (the prototype's evaluate()).
func evaluate(account string, rules []PlanRule, m testMsg, n VendorNames, info map[int64]vendorInfo, down map[int64]bool) NumberTest {
	var t NumberTest
	type catcher struct {
		pos int
		r   PlanRule
	}
	var catchers []catcher
	for i, r := range rules {
		ok, unj, why := catchWhy(r, m)
		t.Rows = append(t.Rows, TestRow{Position: i + 1, ID: r.ID, Name: r.Name, Catches: ok, Unjudged: unj, Why: why})
		if ok {
			catchers = append(catchers, catcher{i + 1, r})
		}
	}
	if len(catchers) == 0 {
		t.Level, t.Headline = "bad", "No rule catches it: refused (503 NO_ROUTE)."
	} else {
		t.Caught = catchers[0].pos
	}
	tried := map[int64]bool{}
	sendStep, sendCatcher, sendLevel := -1, -1, -1
	for ci, c := range catchers {
		method := c.r.Method()
		for li, lv := range levelsFor(c.r, m.mcc, m.mnc) {
			st := TestStep{Position: c.pos, Rule: c.r.Name, Level: sendplan.LevelName(li), Next: ci > 0}
			switch {
			case len(lv.Vendors) < 2:
			case lv.Share:
				st.Way = "shared"
			default:
				st.Way = strings.ToLower(shortWay(method))
			}
			var ups []int
			for _, v := range lv.Vendors {
				tv := TestVendor{ID: v.VendorID, Name: n.name(v.VendorID), State: "if needed"}
				vi, known := info[v.VendorID]
				switch {
				case tried[v.VendorID]:
					tv.State = "already tried"
				case down[v.VendorID]:
					tv.State = "down, skipped"
				case known && vi.status != "" && vi.status != "active":
					tv.State = "switched off (" + vi.status + "), skipped"
				default:
					ups = append(ups, len(st.Vendors))
				}
				tried[v.VendorID] = true
				st.Vendors = append(st.Vendors, tv)
			}
			if sendStep < 0 && len(ups) > 0 {
				sendStep, sendCatcher, sendLevel = len(t.Steps), ci, li
				switch {
				case lv.Share:
					for _, u := range ups {
						st.Vendors[u].State = fmt.Sprintf("sends %d%%", lv.Vendors[u].Pct)
					}
				case method != sendplan.Direct && len(ups) > 1:
					for _, u := range ups {
						st.Vendors[u].State = "one of these, by " + shortWay(method) + " at send time"
					}
				default:
					st.Vendors[ups[0]].State = "sends now"
				}
			}
			t.Steps = append(t.Steps, st)
		}
	}
	if len(catchers) > 0 {
		win := catchers[0]
		t.IfNone = "If none can send: accepted and retried. " + RetryWords(win.r.Retry)
		if sendStep < 0 {
			t.Level = "warn"
			t.Headline = fmt.Sprintf("Caught by rule %d, but no vendor that could send it is up now: accepted and waits.", win.pos)
		} else {
			st := t.Steps[sendStep]
			c := catchers[sendCatcher]
			lv := levelsFor(c.r, m.mcc, m.mnc)[sendLevel]
			var ups, out []string
			for i, v := range st.Vendors {
				if strings.HasPrefix(v.State, "sends") || strings.HasPrefix(v.State, "one of") {
					ups = append(ups, st.Vendors[i].Name)
				} else {
					out = append(out, st.Vendors[i].Name)
				}
			}
			method := c.r.Method()
			who := ups[0]
			switch {
			case lv.Share && len(ups) > 1:
				var s []string
				for _, v := range st.Vendors {
					if strings.HasPrefix(v.State, "sends") {
						s = append(s, v.Name+" "+strings.TrimPrefix(v.State, "sends "))
					}
				}
				who = "Shared: " + strings.Join(s, ", ")
			case len(ups) > 1:
				who = "the " + wayAdj(method) + " of " + andWords(ups) + " (chosen at send time)"
			}
			takes := ""
			if lv.Share && len(out) > 0 {
				verb := " It takes"
				if len(ups) > 1 {
					verb = " They take"
				}
				takes = verb + " the share of " + andWords(out) + ", which can't send."
			}
			t.Level = "ok"
			switch {
			case sendCatcher == 0 && sendLevel == 0:
				if lv.Share && len(ups) > 1 {
					t.Headline = fmt.Sprintf("Rule %d '%s' catches it. %s.%s", c.pos, c.r.Name, who, takes)
				} else {
					t.Headline = fmt.Sprintf("Rule %d '%s' catches it. %s sends it.%s", c.pos, c.r.Name, upperFirst(who), takes)
				}
			case sendCatcher == 0:
				t.Level = "warn"
				failed := "Main can't send."
				switch {
				case sendLevel == 2:
					failed = "Main and Backup 1 can't send."
				case sendLevel > 2:
					failed = fmt.Sprintf("Main and Backup 1–%d can't send.", sendLevel-1)
				}
				t.Headline = fmt.Sprintf("%s %s sends it: %s.", failed, sendplan.LevelName(sendLevel), who)
			default:
				t.Level = "warn"
				t.Headline = fmt.Sprintf("No vendor of rule %d can send. Rule %d '%s' sends it: %s.", win.pos, c.pos, c.r.Name, who)
			}
		}
		// What the vendor gets.
		apply := func(f, v string) string {
			st, ok := win.r.Stored[f]
			if !ok || st[0] == "" || st[1] == "" {
				return v
			}
			re, err := regexp.Compile(st[0])
			if err != nil {
				return v
			}
			out := re.ReplaceAllString(v, st[1])
			if f == FieldReceiver {
				out = msisdn.Clean(out)
			}
			return out
		}
		for _, x := range []struct{ f, label, v string }{
			{FieldSender, "Sender", m.sender}, {FieldReceiver, "Receiver", m.number}, {FieldText, "Text", m.text},
		} {
			if got := apply(x.f, x.v); got != x.v {
				t.VendorGets = append(t.VendorGets, fmt.Sprintf("%s: %s → %s", x.label, orDash(x.v), orDash(got)))
			}
		}
	}
	// A rule of its own for this number's network, country, or everything else.
	var plain []PlanRule
	for _, r := range rules {
		if !r.Special() {
			plain = append(plain, r)
		}
	}
	has := func(ok func(PlanRule) bool) bool {
		for _, r := range plain {
			if ok(r) {
				return true
			}
		}
		return false
	}
	switch {
	case m.mnc != "":
		d := Destination{Kind: DestNetwork, MCC: m.mcc, MNC: m.mnc, Network: m.netName, Country: m.country}
		if !has(func(r PlanRule) bool { return r.Dest.Kind == DestNetwork && r.Dest.Same(d) }) {
			t.AddFor, t.AddNote = &d, d.Short()+" numbers have no rule of their own."
		}
	case m.mcc != "":
		d := Destination{Kind: DestCountry, MCC: m.mcc, Country: m.country}
		if !has(func(r PlanRule) bool { return r.Dest.Kind == DestCountry && r.Dest.MCC == m.mcc }) {
			t.AddFor, t.AddNote = &d, d.Short()+" numbers have no rule of their own."
		}
	default:
		if !has(func(r PlanRule) bool { return r.Dest.Kind == DestAny }) {
			d := AnyDestination()
			t.AddFor, t.AddNote = &d, "No rule sends to any destination."
		}
	}
	return t
}

// upperFirst starts a sentence: "the cheapest of…" → "The cheapest of…".
// A vendor's cid is left as it is written.
func upperFirst(s string) string {
	if strings.HasPrefix(s, "the ") {
		return "The " + s[4:]
	}
	return s
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// TestNumber tests a number against an account's plan: which rule catches
// it, why each rule above passed it, the Main and backup order, the next
// rule if every vendor is down, or "no rule: refused (503 NO_ROUTE)".
// priority 0 = not sent (counts as 9).
func (s Store) TestNumber(ctx context.Context, accountID int64, number, sender, text string, priority int) (NumberTest, error) {
	return s.TestNumberWith(ctx, accountID, TestInput{Number: number, Sender: sender, Text: text, Priority: priority})
}

// TestNumberWith is TestNumber with the editor's extras: vendors to treat
// as down, and a draft rule to test the plan with.
func (s Store) TestNumberWith(ctx context.Context, accountID int64, in TestInput) (NumberTest, error) {
	var t NumberTest
	acct, err := s.planAccount(ctx, accountID)
	if err != nil {
		return t, err
	}
	if t.Place, err = s.PlaceNumber(ctx, in.Number); err != nil {
		return t, err
	}
	if t.Place.Problem != "" {
		t.Level, t.Headline = "bad", t.Place.Problem
		t.Lines = []string{t.Place.Problem}
		return t, nil
	}
	if in.Priority < 0 || in.Priority > 9 {
		t.Level, t.Headline = "bad", "A priority is 1 to 9 (1 is the most urgent); 0 means not sent, which counts as 9."
		t.Lines = []string{t.Headline}
		return t, nil
	}
	rules, err := loadRules(ctx, s.DB, accountID, "MT", false)
	if err != nil {
		return t, err
	}
	var draft PlanRule
	if in.Draft != nil {
		d := *in.Draft
		d.AccountID = accountID
		if d.Direction == "" {
			d.Direction = "MT"
		}
		if draft, err = d.planRule(); err != nil {
			return t, err
		}
		rules = append(rules, draft)
	}
	if err := s.nameRules(ctx, rules); err != nil {
		return t, err
	}
	if in.Draft != nil {
		draft = rules[len(rules)-1]
		rules = CanonicalOrder(mergeDraft(rules[:len(rules)-1], draft, in.EditingID))
	}
	names, info, err := s.vendorInfos(ctx)
	if err != nil {
		return t, err
	}
	m := testMsg{number: t.Place.Digits, mcc: t.Place.MCC, mnc: t.Place.MNC, netName: t.Place.Network, country: t.Place.Country,
		sender: strings.TrimSpace(in.Sender), text: in.Text, textTyped: in.Text != "", priority: in.Priority}
	m.senderTyped = m.sender != ""
	var notes []string
	if !m.senderTyped && acct.DefaultSender != "" {
		m.sender = acct.DefaultSender
		for _, r := range rules {
			if r.cond(FieldSender) != nil {
				notes = append(notes, fmt.Sprintf("No sender typed: the gateway would use %s's default sender '%s'.", acct.Username, acct.DefaultSender))
				break
			}
		}
	}
	t.Judged = strings.TrimSuffix(t.Place.Found, " From the number series it falls in.")
	if !acct.HLRCheck {
		m.mcc, m.mnc, m.netName = "", "", ""
		notes = append(notes, acct.Username+"'s HLR check is off: the gateway does not look the number up, so it counts as 'country not found' (unless the client sends the network with the message). Judged that way here.")
		t.Judged = "country not found (HLR check off)"
	}
	down := map[int64]bool{}
	for _, id := range in.Down {
		down[id] = true
	}
	res := evaluate(acct.Username, rules, m, names, info, down)
	res.Place, res.Judged, res.Notes = t.Place, t.Judged, notes
	if in.Draft != nil {
		id := in.EditingID
		if id <= 0 {
			id = -1
		}
		res.Verdict = draftVerdict(rules, res, id)
	}
	res.Lines = testLines(res)
	return res, nil
}

// draftVerdict says what the rule being edited does with the number.
func draftVerdict(rules []PlanRule, t NumberTest, id int64) string {
	var mine *TestRow
	for i := range t.Rows {
		if t.Rows[i].ID == id {
			mine = &t.Rows[i]
		}
	}
	r := rules[posOf(rules, id)-1]
	switch {
	case !r.On:
		return "The rule is off. It catches nothing."
	case mine == nil:
		return "This rule is not in the plan yet."
	case mine.Unjudged:
		return "This rule is " + mine.Why + "."
	case !mine.Catches:
		s := "Not this rule: " + mine.Why + "."
		if t.Caught > 0 {
			s += fmt.Sprintf(" Rule %d '%s' catches it.", t.Caught, rules[t.Caught-1].Name)
		} else {
			s += " No rule catches it: refused (503 NO_ROUTE)."
		}
		return s
	case t.Caught != mine.Position:
		return fmt.Sprintf("Rule %d '%s' catches it first. This rule gets it only if every vendor of that rule is down.", t.Caught, rules[t.Caught-1].Name)
	}
	if len(r.Levels()) == 0 || len(r.Levels()[0].Vendors) == 0 {
		return "This rule catches it. Main has no vendor yet."
	}
	return "This rule catches it."
}

// testLines is the answer as text.
func testLines(t NumberTest) []string {
	out := []string{t.Place.Found}
	out = append(out, t.Notes...)
	if t.Verdict != "" {
		out = append(out, t.Verdict)
	}
	out = append(out, t.Headline)
	for _, r := range t.Rows {
		if t.Caught > 0 && r.Position > t.Caught && !r.Catches {
			continue
		}
		var state string
		switch {
		case r.Position == t.Caught:
			state = "catches it"
		case r.Catches:
			state = "next, if needed"
		case r.Unjudged:
			state = r.Why
		default:
			state = "passed: " + r.Why
		}
		out = append(out, fmt.Sprintf("  %d. %s — %s", r.Position, r.Name, state))
	}
	if len(t.Steps) > 0 {
		out = append(out, "Order:")
		for _, st := range t.Steps {
			label := st.Level
			if st.Next {
				label = fmt.Sprintf("Next: rule %d '%s' · %s", st.Position, st.Rule, st.Level)
			}
			if st.Way != "" {
				label += " · " + st.Way
			}
			var vs []string
			for _, v := range st.Vendors {
				vs = append(vs, v.Name+" ("+v.State+")")
			}
			out = append(out, "  "+label+": "+strings.Join(vs, ", "))
		}
	}
	if t.IfNone != "" {
		out = append(out, "  "+t.IfNone)
	}
	if len(t.VendorGets) > 0 {
		out = append(out, "What the vendor gets:")
		for _, v := range t.VendorGets {
			out = append(out, "  "+v)
		}
	}
	if t.AddFor != nil {
		out = append(out, t.AddNote+" You can add a rule for "+t.AddFor.Short()+".")
	}
	return out
}
