package abusemonitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yakugate/yakugate/internal/billingstore"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/abuse"
	"github.com/yakugate/yakugate/internal/abusecount"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/refdata"
)

// finding is the worst signal verdict for one account in this sweep.
type finding struct {
	sev          abuse.Severity
	signal       string
	detail       map[string]any
	windowMin    int // the signal's window: the cooldown of a rule that gives none
	cooldownMin  int
	throttlePct  float64
	unlimitedTPS int // cap for accounts with unlimited throughput (B-P10-01)
}

// ladder is one signal's verdict for an account, with the rule's own action
// settings (c is the rule as it applies to that account, override and all).
func ladder(signal string, sev abuse.Severity, c abuse.Common, detail map[string]any) finding {
	return finding{sev: sev, signal: signal, detail: detail, windowMin: c.WindowMin,
		cooldownMin: c.CooldownMin, throttlePct: c.ThrottlePct, unlimitedTPS: c.ThrottleUnlimitedTPS}
}

// Step runs one input -> logic -> output cycle for the "abuse-monitor" process.
//
//	INPUT:  recent hot records (sliding windows; rejected rows excluded — P1
//	        already acted on them, and P1-REJECTLOG persists them into these
//	        same tables, PO ruling 2026-07-26 B-P10-04) + abuse_rules signal
//	        params + spam keywords / templates from the config store
//	LOGIC:  once per interval — evaluate each enabled signal per account
//	        (ait destination-spread, spend vs baseline, spam anti-evasion
//	        scoring, template drift, velocity, bad numbers, charset), then the
//	        weighted score over those plus the content signals (formatting,
//	        opt-out rate, brand impersonation — inputs only, content.go),
//	        capped at throttle; take the MAX severity per account
//	OUTPUT: warn -> audit_log; throttle -> accounts.throughput cut; suspend ->
//	        accounts.is_disabled=1 — every action audited (actor=abuse-monitor),
//	        repeats suppressed by a per-action cooldown; propagation to the
//	        accept path rides the refdata cache refresh
func (p *Process) Step(ctx context.Context) error {
	if p.hot == nil || p.data == nil || p.data.Windows == nil || p.data.Rules == nil {
		return nil
	}
	// Gateway-wide role: one deployment runs it, the rest stand by
	// (internal/singleton). Two copies of abuse-monitor against one shared store
	// would apply and audit the same suspension twice.
	// Under the supervisor, a lease another deployment holds ends this
	// process instead (singleton.Elsewhere): no idle copy is kept.
	if held, err := p.lease.Held(ctx); !held {
		p.leaseOK.Store(false)
		return err
	}
	p.leaseOK.Store(true)
	// An operator who reset a reputation (config/0143) is looking at the
	// screen. A sweep can take minutes on a store with many day tables, so
	// the look for a reset runs beside it, on its own pace (watchResets).
	p.resetWatch.Do(func() { go p.watchResets(ctx) })
	now := time.Now()
	if !p.lastRun.IsZero() && now.Sub(p.lastRun) < p.interval {
		return nil // throttled
	}
	p.lastRun = now
	p.stats.sweep(now) // [SCOPE P1-STATUS]

	// Once a day: each account's reputation score, for operators to read
	// (config/0107). Shown only — nothing acts on it (ruling 2026-09-09).
	p.repMu.Lock()
	if p.reputationDue(ctx, now) {
		if n, err := p.updateReputation(ctx, now); err != nil {
			p.log.Printf("reputation: %v (%d written)", err, n)
		} else {
			p.log.Printf("reputation: %d account score(s) written", n)
		}
	}
	p.repMu.Unlock()

	rules := p.loadRules(ctx)
	if len(rules) == 0 {
		return nil
	}
	p.over = p.loadOverrides(ctx) // one customer's own thresholds (config/0106)

	findings := map[int64]finding{}
	// Every ladder signal's verdict is also an input to the weighted score,
	// at the rung it reached (content.go).
	contrib := newContributions()
	merge := func(acct int64, f finding) {
		if f.signal != "score" {
			contrib.add(acct, f.signal, severityStrength(f.sev))
		}
		if cur, ok := findings[acct]; !ok || f.sev > cur.sev {
			findings[acct] = f
		}
	}
	// run judges every enabled signal once. The figures come from the
	// message tables, or — while p.view is set — from the accept path's
	// counters (counters.go); each signal's judge is the same either way.
	run := func(tables []string, merge func(int64, finding), contrib *contributions) {
		if raw, ok := rules["ait"]; ok {
			p.evalAIT(ctx, tables, raw, merge)
		}
		if raw, ok := rules["velocity"]; ok {
			p.evalVelocity(ctx, tables, raw, merge)
		}
		if raw, ok := rules["badnum"]; ok {
			p.evalBadNumbers(ctx, tables, raw, merge)
		}
		if raw, ok := rules["charset"]; ok {
			p.evalCharsetChange(ctx, tables, raw, merge)
		}
		if raw, ok := rules["spend"]; ok {
			p.evalSpend(ctx, tables, raw, merge)
		}
		if raw, ok := rules["spam"]; ok {
			p.evalSpam(ctx, tables, raw, merge)
		}
		if raw, ok := rules["template"]; ok {
			p.evalTemplate(ctx, tables, raw, merge)
		}
		// Content signals: score inputs only, never their own action.
		if raw, ok := rules["formatting"]; ok {
			p.evalFormatting(ctx, tables, raw, contrib)
		}
		if raw, ok := rules["optout"]; ok {
			p.evalOptOut(ctx, tables, raw, contrib)
		}
		if raw, ok := rules["brand"]; ok {
			p.evalBrand(ctx, tables, raw, contrib)
		}
		if raw, ok := rules["score"]; ok {
			p.evalScore(raw, contrib, merge)
		}
	}
	src := p.source()
	p.stats.setSource(src)
	if src == sourceCounters {
		// No statement on a message table. Counters that cannot be judged
		// from (not begun, or Redis unreadable) give no verdict this sweep.
		v, ok := p.openCounters(ctx)
		if !ok {
			return nil
		}
		p.view = v
		run(nil, merge, contrib)
		p.view = nil
	} else {
		tables := p.recentTables(ctx, lookBackDays(rules))
		if len(tables) == 0 {
			return nil
		}
		run(tables, merge, contrib)
		if src == sourceCompare {
			p.compareWithCounters(ctx, run, findings)
		}
	}
	for acct, f := range findings {
		p.stats.finding(f.signal) // [SCOPE P1-STATUS]
		if f.sev >= abuse.Warn {
			p.act(ctx, acct, f)
		}
	}
	return nil
}

// loadRules returns enabled abuse_rules params keyed by signal name.
func (p *Process) loadRules(ctx context.Context) map[string]json.RawMessage {
	rows, err := p.data.Rules.Enabled(ctx)
	if err != nil {
		p.log.Printf("load abuse_rules: %v", err)
		return nil
	}
	out := map[string]json.RawMessage{}
	for _, r := range rows {
		out[r.Signal] = json.RawMessage(r.Params)
	}
	return out
}

// recentTables lists hot partitions young enough to hold window/baseline data
// (accepted_at predicates do the precise filtering). days is how far back
// the enabled rules read (lookBackDays).
func (p *Process) recentTables(ctx context.Context, days int) []string {
	all, err := p.hot.ListRecordTables(ctx)
	if err != nil {
		p.log.Printf("list record tables: %v", err)
		return nil
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -days)
	var out []string
	for _, t := range all {
		if !t.Date.After(cutoff) {
			continue
		}
		// MT families only (Hadal cutover fix, 2026-08-19 — same S9-03 filter
		// billing applies): dlrlog/mo_log/rejects must never be scanned by the
		// MT abuse signals. Before this, EVERY listed table was queried with
		// the legacy `direction='MT' AND status<>'rejected'` predicate — which
		// no mtlog_* table has, so the whole signal engine silently 1054'd on
		// the live MT family and scored only historic pre-Hadal records_*.
		// A company's day of outgoing messages: mt_<company>_<YYYYMMDD>.
		if t.Kind == billingstore.KindMT {
			out = append(out, t.Name)
		}
	}
	return out
}

// lookBackDays is how many days of tables the enabled rules need: the
// longest baseline plus its window, and the part-days at both ends. Never
// fewer than 3, which is what was always read.
//
// It was a fixed 3 whatever baseline_hours said, while the sum was still
// divided by baseline_hours: a 96-hour baseline of one message an hour came
// out 0.688 a window instead of 0.99, and four messages became a throttle
// instead of a warn (QA 2026-10-08, B-08).
func lookBackDays(rules map[string]json.RawMessage) int {
	need := 0 // minutes
	for _, raw := range rules {
		var w struct {
			WindowMin     int `json:"window_min"`
			BaselineHours int `json:"baseline_hours"`
		}
		// A rule that does not parse whole still says how far it reads.
		if clean, err := dropBlanks(raw); err == nil {
			_ = json.Unmarshal(clean, &w)
		}
		if m := w.BaselineHours*60 + w.WindowMin; m > need {
			need = m
		}
	}
	days := need/(24*60) + 2
	if days < 3 {
		days = 3
	}
	return days
}

// decodeParams reads one rule's params into dst and checks it has the keys
// it cannot run without (abuse.*Params.Validate). false = the rule is not run
// this sweep, and it says so: in the log and in the role's status section.
//
// It used to be a bare json.Unmarshal whose failure returned in silence. The
// Abuse Rules screen saved an emptied box as "", which does not parse into a
// number, so one emptied box switched the whole signal off with no word
// anywhere (QA 2026-10-08, B-04). An empty or null value now reads as "not
// set"; anything else that does not parse stops the rule, loudly.
func (p *Process) decodeParams(signal string, raw json.RawMessage, dst any) bool {
	if err := decodeRule(raw, dst); err != nil {
		p.note(signal, "rule not run: "+err.Error())
		return false
	}
	p.stats.clearNote(signal)
	return true
}

func decodeRule(raw []byte, dst any) error {
	clean, err := dropBlanks(raw)
	if err == nil {
		err = json.Unmarshal(clean, dst)
	}
	if err != nil {
		return fmt.Errorf("its params do not parse (%v)", err)
	}
	if v, ok := dst.(interface{ Validate() error }); ok {
		return v.Validate()
	}
	return nil
}

// dropBlanks removes the keys whose value is "" or null, in the params and
// in the score rule's weights: a value nobody gave.
func dropBlanks(raw []byte) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	for k, v := range m {
		if isBlank(v) {
			delete(m, k)
			continue
		}
		if k != "weights" {
			continue
		}
		var w map[string]json.RawMessage
		if json.Unmarshal(v, &w) != nil {
			continue
		}
		for name, weight := range w {
			if isBlank(weight) {
				delete(w, name)
			}
		}
		if b, err := json.Marshal(w); err == nil {
			m[k] = b
		}
	}
	return json.Marshal(m)
}

func isBlank(v json.RawMessage) bool {
	s := strings.TrimSpace(string(v))
	return s == `""` || s == "null"
}

// note records something an operator should know about a rule and logs it
// when it is new or has not been said for a while (a sweep runs every
// minute; the same line every minute would bury the log).
func (p *Process) note(key, msg string) {
	if p.stats.setNote(key, msg, time.Now()) && p.log != nil {
		p.log.Printf("%s: %s", key, msg)
	}
}

// readSpend adds up one window's spend rows into into, pricing them: an
// account's spend in its company's BALANCE currency (QA BUG-YK-RC-010). A log
// row keeps the rate version that priced it and the conversion rate into the
// company's currency (issue 16), not an amount: the parts are summed by rate
// and rate (dal.MessageWindows.Spend), and each sum is priced by its rate
// version's gross price per part (billing.Prices). A row priced in the
// company's currency carries no conversion rate and counts as it is.
func (p *Process) readSpend(ctx context.Context, rows []dal.SpendRow, into map[int64]float64) {
	type part struct {
		acct, rate int64
		fx, parts  float64
	}
	var got []part
	var ids []int64
	for _, r := range rows {
		g := part{acct: r.Account, rate: r.Rate, fx: r.FX, parts: r.Parts}
		if g.rate != 0 {
			got = append(got, g)
			ids = append(ids, g.rate)
		}
	}
	prices := p.prices.Of(ctx, ids)
	for _, g := range got {
		if pr, ok := prices[g.rate]; ok {
			fx := g.fx
			if fx <= 0 {
				fx = 1
			}
			into[g.acct] += g.parts * pr.PerPart * fx
		}
	}
}

// keptBase is a signal's baseline as it was last read from the tables.
type keptBase struct {
	at            time.Time
	window, hours int
	data          any
}

// baselineReuse is how long a baseline read from the tables is used before
// it is read again: a hundredth of its span, ten minutes at most.
//
// A baseline is the average of many hours; the minute that passed since the
// last sweep moves it by almost nothing. But reading it is most of what the
// records path costs — every row of the baseline hours of every table, for
// each of three signals, every sweep: with yesterday's 660,000 messages in
// one table, one such read took ten seconds (2026-10-13). A baseline of 24
// hours is now read every ten minutes; one of an hour, as before.
func baselineReuse(hours int) time.Duration {
	d := time.Duration(hours) * time.Hour / 100
	if d > 10*time.Minute {
		d = 10 * time.Minute
	}
	return d
}

// keptBaseline is the baseline kept for a signal, or nil when there is none
// for these values or it is due to be read again.
func (p *Process) keptBaseline(signal string, window, hours int) any {
	k, ok := p.baseKept[signal]
	if !ok || k.window != window || k.hours != hours || time.Since(k.at) >= baselineReuse(hours) {
		return nil
	}
	return k.data
}

// keepBaseline keeps a baseline that was read whole.
func (p *Process) keepBaseline(signal string, window, hours int, data any) {
	if p.baseKept == nil {
		p.baseKept = map[string]keptBase{}
	}
	p.baseKept[signal] = keptBase{at: time.Now(), window: window, hours: hours, data: data}
}

// The window and baseline reads below go through dal.MessageWindows: a
// message log is read through its time key idx_drill (accepted_at,
// vendor_id), and one made before the key existed is read without it and
// remembered (2026-10-13; internal/dal/mysql/abuse.go).

// --- ait: destination-spread / SMS-pumping ---

func (p *Process) evalAIT(ctx context.Context, tables []string, raw json.RawMessage, merge func(int64, finding)) {
	var prm abuse.AITParams
	if !p.decodeParams("ait", raw, &prm) {
		return
	}
	if p.view != nil {
		p.evalAITCounters(ctx, prm, merge) // from the accept path's counters (counters.go)
		return
	}
	accts := map[int64]*aitCount{}
	get := func(id int64) *aitCount {
		a, ok := accts[id]
		if !ok {
			a = &aitCount{prefixes: map[string]int{}}
			accts[id] = a
		}
		return a
	}
	seenIn := map[int64][]string{} // the tables an account has window rows in
	for _, t := range tables {
		// counts + distinct destinations, table by table
		counts, err := p.data.Windows.Counts(ctx, t, prm.WindowMin)
		if err != nil {
			continue
		}
		for _, c := range counts {
			a := get(c.Account)
			a.n += c.Msgs
			a.uniq += c.Uniq
			seenIn[c.Account] = append(seenIn[c.Account], t)
		}
		// single-prefix concentration
		pfxs, err := p.data.Windows.Prefixes(ctx, t, prm.PrefixLen, prm.WindowMin)
		if err != nil {
			continue
		}
		for _, c := range pfxs {
			get(c.Account).prefixes[c.Prefix] += c.Msgs
		}
	}
	// A number sent to on both sides of midnight is in two day tables, and
	// adding the tables' own distinct counts counts it twice: 60 messages to
	// 30 numbers read as 60 different numbers, and the account was suspended
	// (QA 2026-10-08, B-13). Only in the first window_min after 00:00 UTC
	// does an account have two tables in the window; it is recounted over
	// both then.
	for id, ts := range seenIn {
		if len(ts) < 2 {
			continue
		}
		if u, err := p.data.Windows.Distinct(ctx, ts, id, prm.WindowMin); err == nil {
			accts[id].uniq = u
		}
	}
	p.judgeAIT(accts, prm, merge)
}

// aitCount is one account's window as the AIT signal reads it: messages,
// different numbers, and messages per number prefix.
type aitCount struct {
	n, uniq  int
	prefixes map[string]int
}

// judgeAIT is the AIT verdict for each account's counts, wherever the counts
// came from (the records, or the accept path's counters).
func (p *Process) judgeAIT(accts map[int64]*aitCount, prm abuse.AITParams, merge func(int64, finding)) {
	for id, a := range accts {
		pp := prm
		if !p.over.apply("ait", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		top := 0
		topPfx := ""
		for pfx, c := range a.prefixes {
			if pfx == abusecount.OtherPrefix {
				continue // prefixes past the counters' cap: part of the total, never the top one
			}
			if c > top {
				top, topPfx = c, pfx
			}
		}
		if sev := abuse.EvalAIT(a.n, a.uniq, top, pp); sev > abuse.None {
			merge(id, ladder("ait", sev, pp.Common,
				map[string]any{"msgs": a.n, "uniq_dest": a.uniq, "top_prefix": topPfx, "top_prefix_msgs": top,
					"uniq_ratio": ratio(a.uniq, a.n), "prefix_share": ratio(top, a.n), "window_min": pp.WindowMin}))
		}
	}
}

// --- spend: current window vs trailing baseline (P9 sell_price) ---

func (p *Process) evalSpend(ctx context.Context, tables []string, raw json.RawMessage, merge func(int64, finding)) {
	var prm abuse.SpendParams
	if !p.decodeParams("spend", raw, &prm) {
		return
	}
	if p.view != nil {
		p.evalSpendCounters(ctx, prm, merge) // from the accept path's counters (counters.go)
		return
	}
	current := map[int64]float64{}
	// The baseline is read once and used for a while (keptBaseline).
	baseline, kept := p.keptBaseline("spend", prm.WindowMin, prm.BaselineHours).(map[int64]float64)
	if !kept {
		baseline = map[int64]float64{}
	}
	whole := true
	for _, t := range tables {
		rows, err := p.data.Windows.Spend(ctx, t, prm.WindowMin, 0)
		if err == nil {
			p.readSpend(ctx, rows, current)
		}
		if kept {
			continue
		}
		rows, err = p.data.Windows.Spend(ctx, t, prm.WindowMin, prm.BaselineHours*60+prm.WindowMin)
		if err != nil {
			whole = false
			continue
		}
		p.readSpend(ctx, rows, baseline)
	}
	if !kept && whole {
		p.keepBaseline("spend", prm.WindowMin, prm.BaselineHours, baseline)
	}
	p.judgeSpend(ctx, current, baseline, float64(prm.BaselineHours*60)/float64(prm.WindowMin), prm, merge)
}

// judgeSpend is the spend verdict for each account: what it spent in the
// window against what it spent per window over the baseline, both in its
// company's currency, wherever the figures came from.
func (p *Process) judgeSpend(ctx context.Context, current, baseline map[int64]float64, windowsInBaseline float64,
	prm abuse.SpendParams, merge func(int64, finding)) {
	money := p.loadCompanyMoney(ctx)
	unknown := map[string]int{} // currency -> accounts with no rate to the base
	for id, cur := range current {
		pp := prm
		if !p.over.apply("spend", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		// The floor is an amount in the BASE currency; the spend beside it is
		// in the company's own (readSpend). Turn the floor into that currency
		// before comparing: read as a bare number, a floor of 1.0 was one
		// rupee for a PKR company, and a new account at PKR 3.894 a message
		// was warned on its first message, throttled on its second and
		// suspended on its third (production 2026-10-07; QA 2026-10-08, B-03).
		cm, ok := money[id]
		if !ok {
			cm = companyMoney{currency: p.baseCurrency(), toBase: 1, known: true} // no company row: counted as it is
		}
		if !cm.known {
			unknown[cm.currency]++
			continue // a floor that cannot be compared is not a floor: say nothing
		}
		pp.MinBaseline = pp.MinBaseline / cm.toBase
		perWindow := 0.0
		if windowsInBaseline > 0 {
			perWindow = baseline[id] / windowsInBaseline
		}
		if sev := abuse.EvalSpend(cur, perWindow, pp); sev > abuse.None {
			merge(id, ladder("spend", sev, pp.Common,
				map[string]any{"window_spend": cur, "baseline_per_window": perWindow,
					"baseline_hours": pp.BaselineHours, "window_min": pp.WindowMin,
					"currency": cm.currency, "min_baseline": math.Round(pp.MinBaseline*1e6) / 1e6}))
		}
	}
	if len(unknown) == 0 {
		p.stats.clearNote("spend rates")
		return
	}
	var parts []string
	for cur, n := range unknown {
		parts = append(parts, fmt.Sprintf("%d in %s", n, cur))
	}
	sort.Strings(parts)
	p.note("spend rates", fmt.Sprintf("account(s) not judged on spend, because their company's currency has no rate to %s: %s — "+
		"set the company's Base Conversion Rate", p.baseCurrency(), strings.Join(parts, ", ")))
}

// companyMoney is what the spend floor needs to know about an account's
// company: its balance currency and what one unit of it is worth in the base
// currency (billing.ToBase). known is false when no rate can be found.
type companyMoney struct {
	currency string
	toBase   float64
	known    bool
}

func (p *Process) baseCurrency() string { return config.BaseCurrency() }

// loadCompanyMoney answers for every account. A company's rate is worked out
// once, however many accounts it has.
func (p *Process) loadCompanyMoney(ctx context.Context) map[int64]companyMoney {
	out := map[int64]companyMoney{}
	rows, err := p.data.Accounts.Money(ctx)
	if err != nil {
		p.log.Printf("load company currencies: %v", err)
		return out
	}
	base := p.baseCurrency()
	byCompany := map[int64]companyMoney{}
	for _, r := range rows {
		id, co, cur, rate, crp := r.Account, r.Company, r.Currency, r.BaseRate, r.RateProvider
		cm, done := byCompany[co]
		if !done {
			if cur == "" {
				cur = base // an account of no company, or a company with no currency set
			}
			cm = companyMoney{currency: strings.ToUpper(cur)}
			cm.toBase, cm.known = billing.ToBase(ctx, p.fx, base, cur, rate, crp)
			if cm.toBase <= 0 {
				cm.known = false
			}
			byCompany[co] = cm
		}
		out[id] = cm
	}
	return out
}

// evalVelocity scores a burst in message COUNT against the account's own
// recent rate (P1 register 1.2.4, ruled to P10 2026-07-17).
//
// Deliberately the same shape as evalSpend — same two queries, same
// baseline-per-window arithmetic — because the two answer the same question
// about different units, and an operator tuning one should recognise the
// other. What differs is COUNT(*) instead of SUM(sell_price): a spike in cheap
// traffic moves this and can leave spend untouched.
func (p *Process) evalVelocity(ctx context.Context, tables []string, raw json.RawMessage, merge func(int64, finding)) {
	var prm abuse.VelocityParams
	if !p.decodeParams("velocity", raw, &prm) {
		return
	}
	if p.view != nil {
		p.evalVelocityCounters(ctx, prm, merge) // from the accept path's counters (counters.go)
		return
	}
	current := map[int64]int{}
	// The baseline is read once and used for a while (keptBaseline).
	baseline, kept := p.keptBaseline("velocity", prm.WindowMin, prm.BaselineHours).(map[int64]int)
	if !kept {
		baseline = map[int64]int{}
	}
	whole := true
	for _, t := range tables {
		rows, err := p.data.Windows.Messages(ctx, t, prm.WindowMin, 0)
		if err == nil {
			for _, r := range rows {
				current[r.Account] += r.N
			}
		}
		if kept {
			continue
		}
		rows, err = p.data.Windows.Messages(ctx, t, prm.WindowMin, prm.BaselineHours*60+prm.WindowMin)
		if err != nil {
			whole = false
			continue
		}
		for _, r := range rows {
			baseline[r.Account] += r.N
		}
	}
	if !kept && whole {
		p.keepBaseline("velocity", prm.WindowMin, prm.BaselineHours, baseline)
	}
	p.judgeVelocity(current, baseline, float64(prm.BaselineHours*60)/float64(prm.WindowMin), prm, merge)
}

// judgeVelocity is the velocity verdict for each account: its messages in the
// window against its messages per window over the baseline.
func (p *Process) judgeVelocity(current, baseline map[int64]int, windowsInBaseline float64,
	prm abuse.VelocityParams, merge func(int64, finding)) {
	for id, cur := range current {
		pp := prm
		if !p.over.apply("velocity", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		perWindow := 0.0
		if windowsInBaseline > 0 {
			perWindow = float64(baseline[id]) / windowsInBaseline
		}
		if sev := abuse.EvalVelocity(cur, perWindow, pp); sev > abuse.None {
			merge(id, ladder("velocity", sev, pp.Common,
				map[string]any{"window_messages": cur, "baseline_per_window": perWindow,
					"baseline_hours": pp.BaselineHours, "window_min": pp.WindowMin}))
		}
	}
}

// evalBadNumbers scores the share of an account's HLR-consulted destinations
// that came back unknown — the signature of a scraped or purchased list.
//
// hlr_source is already on every MT record ("db", "dip", "miss", "dip_miss",
// "range", "country", "operator", or empty when HLR was not consulted), so
// this needs no new data: the record IS the HLR log. Only answers that
// checked the number itself count: a number range or a country code says the
// number COULD exist, not that it does, so they are left out.
func (p *Process) evalBadNumbers(ctx context.Context, tables []string, raw json.RawMessage, merge func(int64, finding)) {
	var prm abuse.BadNumberParams
	if !p.decodeParams("badnum", raw, &prm) {
		return
	}
	if p.view != nil {
		p.evalBadNumbersCounters(ctx, prm, merge) // from the accept path's counters (counters.go)
		return
	}
	misses := map[int64]int{}
	consulted := map[int64]int{}
	for _, t := range tables {
		// Only rows HLR actually looked at. Counting the ones it skipped would
		// flatter an account that supplies its own operator codes — precisely
		// the account best placed to hide a bad list.
		rows, err := p.data.Windows.Lookups(ctx, t, prm.WindowMin)
		if err != nil {
			continue
		}
		for _, r := range rows {
			misses[r.Account] += r.Misses
			consulted[r.Account] += r.Consulted
		}
	}
	p.judgeBadNumbers(misses, consulted, prm, merge)
}

// judgeBadNumbers is the unknown-number verdict for each account: of the
// numbers the HLR was asked about, how many it did not know.
func (p *Process) judgeBadNumbers(misses, consulted map[int64]int, prm abuse.BadNumberParams, merge func(int64, finding)) {
	for id, c := range consulted {
		pp := prm
		if !p.over.apply("badnum", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		if sev := abuse.EvalBadNumbers(misses[id], c, pp); sev > abuse.None {
			merge(id, ladder("badnum", sev, pp.Common,
				map[string]any{"unknown_numbers": misses[id], "hlr_consulted": c,
					"ratio": float64(misses[id]) / float64(c), "window_min": pp.WindowMin}))
		}
	}
}

// evalCharsetChange scores an account sending in an encoding it has
// essentially never used.
//
// See CharsetParams for the limit this carries: the record stores the
// encoding, not the script, so UCS-2 covers Urdu, Arabic, Chinese and emoji
// alike. It cannot separate a hijacked account from a customer launching a
// legitimate non-English campaign, which is why the seeded thresholds only
// ever warn.
func (p *Process) evalCharsetChange(ctx context.Context, tables []string, raw json.RawMessage, merge func(int64, finding)) {
	var prm abuse.CharsetParams
	if !p.decodeParams("charset", raw, &prm) {
		return
	}
	if p.view != nil {
		p.evalCharsetCounters(ctx, prm, merge) // from the accept path's counters (counters.go)
		return
	}
	window := map[int64]*encCounts{}
	// The baseline is read once and used for a while (keptBaseline).
	baseline, kept := p.keptBaseline("charset", prm.WindowMin, prm.BaselineHours).(map[int64]*encCounts)
	if !kept {
		baseline = map[int64]*encCounts{}
	}
	whole := true
	add := func(m map[int64]*encCounts, id int64, enc string, n int) {
		c := m[id]
		if c == nil {
			c = &encCounts{byEnc: map[string]int{}}
			m[id] = c
		}
		c.byEnc[enc] += n
	}
	for _, t := range tables {
		for i, q := range []struct {
			into  map[int64]*encCounts
			since int // 0 = the window
		}{
			{window, 0},
			{baseline, prm.BaselineHours*60 + prm.WindowMin},
		} {
			isBaseline := i == 1
			if isBaseline && kept {
				continue
			}
			rows, err := p.data.Windows.Encodings(ctx, t, prm.WindowMin, q.since)
			if err != nil {
				if isBaseline {
					whole = false
				}
				continue
			}
			for _, r := range rows {
				add(q.into, r.Account, r.Encoding, r.N)
			}
		}
	}
	if !kept && whole {
		p.keepBaseline("charset", prm.WindowMin, prm.BaselineHours, baseline)
	}
	p.judgeCharset(window, baseline, prm, merge)
}

// encCounts is an account's messages by encoding.
type encCounts struct{ byEnc map[string]int }

// judgeCharset is the unfamiliar-encoding verdict for each account: how much
// of its window is in an encoding it essentially never used over the baseline.
func (p *Process) judgeCharset(window, baseline map[int64]*encCounts, prm abuse.CharsetParams, merge func(int64, finding)) {
	for id, w := range window {
		pp := prm
		if !p.over.apply("charset", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		total, novel := 0, 0
		b := baseline[id]
		baseTotal := 0
		if b != nil {
			for _, n := range b.byEnc {
				baseTotal += n
			}
		}
		for enc, n := range w.byEnc {
			total += n
			// "Essentially never used" rather than "never": one stray message
			// in an encoding should not make it familiar forever.
			prior := 0
			if b != nil {
				prior = b.byEnc[enc]
			}
			if baseTotal == 0 || float64(prior)/float64(baseTotal) < 0.01 {
				novel += n
			}
		}
		if total == 0 {
			continue
		}
		share := float64(novel) / float64(total)
		if sev := abuse.EvalCharsetChange(total, baseTotal, share, pp); sev > abuse.None {
			merge(id, ladder("charset", sev, pp.Common,
				map[string]any{"window_messages": total, "unfamiliar_encoding_share": share,
					"baseline_messages": baseTotal, "window_min": pp.WindowMin}))
		}
	}
}

// --- spam: anti-evasion content scoring (the P1 deferral) ---

func (p *Process) evalSpam(ctx context.Context, tables []string, raw json.RawMessage, merge func(int64, finding)) {
	var prm abuse.SpamParams
	if !p.decodeParams("spam", raw, &prm) {
		return
	}
	rules := p.loadSpamRules(ctx)
	if rules.none() {
		return
	}
	hits := map[int64]int{}
	samples := map[int64]string{}
	for id, msgs := range p.sampleMsgs(ctx, "spam", tables, prm.WindowMin, 0, prm.SampleMsgs, prm.ScanLimit, nil) {
		for _, m := range msgs {
			if label := rules.hit(id, m); label != "" {
				hits[id]++
				if samples[id] == "" {
					samples[id] = label
				}
			}
		}
	}
	for id, h := range hits {
		pp := prm
		if !p.over.apply("spam", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		if sev := abuse.EvalSpam(h, pp); sev > abuse.None {
			merge(id, ladder("spam", sev, pp.Common,
				map[string]any{"hits": h, "sample_keyword": samples[id], "window_min": pp.WindowMin}))
		}
	}
}

// spamRule is one blocked or allowed rule as the monitor reads it.
type spamRule struct {
	refdata.SpamMatcher
	// label is what the audit line shows for a hit: the rule's own pattern,
	// cut short.
	label string
	// words are the rule's alternatives, folded, when the rule is nothing
	// but words joined by "|" — what a keyword list of before GW-17 turns
	// into, and what most rules still are. They keep the whole-word match on
	// closed-up text (abuse.HitsKeyword: "f r e e m o n e y"), which a plain
	// regex on the folded text does not find.
	words []string
}

// matches reports whether the rule applies to this message and its text
// hits it: as it was sent, or folded (abuse.Normalize — the disguises P1's
// exact gate does not see, which is what this signal is for).
func (r spamRule) matches(m msg, norm string) bool {
	if r.Country != "" && !strings.HasPrefix(strings.TrimPrefix(m.dest, "+"), r.Country) {
		return false
	}
	if r.Re.MatchString(m.body) || (norm != "" && r.Re.MatchString(norm)) {
		return true
	}
	for _, w := range r.words {
		if abuse.HitsKeyword(norm, w) {
			return true
		}
	}
	return false
}

// spamRules is every rule the signal counts, and which company each account
// belongs to.
type spamRules struct {
	universal []spamRule
	blocked   map[int64][]spamRule // by company
	allowed   map[int64][]spamRule // by company
	companyOf map[int64]int64      // account -> company
}

func (r spamRules) none() bool { return len(r.universal) == 0 && len(r.blocked) == 0 }

// hit returns the label of the first blocked rule this account's message
// hits, or "". A message that one of the company's ALLOWED rules matches is
// not counted at all — the order P1 follows (pipeline/stage_spam.go). The
// monitor used to read blocked rules only, so ten messages an operator had
// allowed by name became an abuse.warn (QA 2026-10-08, B-10).
func (r spamRules) hit(acct int64, m msg) string {
	norm := abuse.Normalize(m.body)
	co := r.companyOf[acct]
	for _, a := range r.allowed[co] {
		if a.matches(m, norm) {
			return ""
		}
	}
	for _, b := range r.universal {
		if b.matches(m, norm) {
			return b.label
		}
	}
	for _, b := range r.blocked[co] {
		if b.matches(m, norm) {
			return b.label
		}
	}
	return ""
}

// loadSpamRules reads the rules through the reference cache's own loader
// and compiler (refdata.MySQL.LoadSpam, refdata.CompileSpamRules), so a rule
// means here what it means on the accept path.
//
// It had its own reader, which took spam_keywords.keywords as a list of
// strings. Since GW-17 (2026-08-27) a rule is one regex in keyword_regex and
// keywords is [], so the signal saw no rule saved after that day; a list
// saved as [{word, enabled}] objects did not parse either (QA 2026-10-08,
// B-01).
//
// Only rules whose action is "reject" count. "flag" and "require_template"
// (config/0098) are an operator's choice to let the text through — every OTP
// under a require_template rule would be a hit — and nobody has ruled that
// they should count against the account.
func (p *Process) loadSpamRules(ctx context.Context) spamRules {
	if p.ref == nil {
		return spamRulesFrom(refdata.SpamLists{})
	}
	lists, err := p.ref.LoadSpam(ctx)
	if err != nil {
		p.log.Printf("load spam rules: %v", err)
		return spamRulesFrom(refdata.SpamLists{})
	}
	out := spamRulesFrom(lists)
	if len(out.blocked) == 0 && len(out.allowed) == 0 {
		return out // universal rules need no company
	}
	accts, err := p.data.Accounts.Companies(ctx)
	if err != nil {
		p.log.Printf("load account->company map: %v", err)
		return out
	}
	for _, a := range accts {
		if a.Company != 0 {
			out.companyOf[a.Account] = a.Company
		}
	}
	return out
}

// spamRulesFrom compiles the lists as loaded; which company an account
// belongs to is filled in by the caller.
func spamRulesFrom(lists refdata.SpamLists) spamRules {
	out := spamRules{blocked: map[int64][]spamRule{}, allowed: map[int64][]spamRule{}, companyOf: map[int64]int64{}}
	out.universal = spamRulesOf(lists.Universal, true)
	for co, list := range lists.CompanyBlocked {
		if rs := spamRulesOf(list, true); len(rs) > 0 {
			out.blocked[co] = rs
		}
	}
	for co, list := range lists.CompanyAllowed {
		if rs := spamRulesOf(list, false); len(rs) > 0 {
			out.allowed[co] = rs
		}
	}
	return out
}

// spamRulesOf compiles a list. rejectOnly keeps the rules that refuse a
// message; an allowed list is taken whole.
func spamRulesOf(list []refdata.SpamRule, rejectOnly bool) []spamRule {
	var out []spamRule
	for _, r := range list {
		ms := refdata.CompileSpamRules([]refdata.SpamRule{r}) // one in, so the pattern stays beside its matcher
		if len(ms) == 0 || (rejectOnly && ms[0].Action != refdata.SpamActionReject) {
			continue
		}
		label := r.Pattern
		if len(label) > 64 {
			label = label[:64]
		}
		out = append(out, spamRule{SpamMatcher: ms[0], label: label, words: plainWords(r.Pattern)})
	}
	return out
}

// plainWords returns a pattern's alternatives, folded, when it uses nothing
// of regex but "|"; nil for anything else.
func plainWords(pattern string) []string {
	if strings.ContainsAny(pattern, `\.^$*+?()[]{}`) {
		return nil
	}
	var out []string
	for _, w := range strings.Split(pattern, "|") {
		if n := abuse.Normalize(w); n != "" {
			out = append(out, n)
		}
	}
	return out
}

// --- template: drift for template-holding, non-enforced accounts ---

func (p *Process) evalTemplate(ctx context.Context, tables []string, raw json.RawMessage, merge func(int64, finding)) {
	var prm abuse.TemplateParams
	if !p.decodeParams("template", raw, &prm) {
		return
	}
	tpls := p.loadDriftTemplates(ctx)
	if len(tpls) == 0 {
		return
	}
	// Enforced and template-less accounts cannot drift: only the rest is read.
	only := make(map[int64]bool, len(tpls))
	for id := range tpls {
		only[id] = true
	}
	for id, msgs := range p.sampleMsgs(ctx, "template", tables, prm.WindowMin, 0, prm.SampleMsgs, prm.ScanLimit, only) {
		total, miss := len(msgs), 0
		for _, m := range msgs {
			if !matchesAnyTemplate(tpls[id], m) {
				miss++
			}
		}
		pp := prm
		if !p.over.apply("template", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		if sev := abuse.EvalTemplate(miss, total, pp); sev > abuse.None {
			merge(id, ladder("template", sev, pp.Common,
				map[string]any{"scanned": total, "mismatched": miss,
					"mismatch_ratio": ratio(miss, total), "window_min": pp.WindowMin}))
		}
	}
}

// matchesAnyTemplate is P1's own question (pipeline/stage_template.go): does
// one of the templates this sender may use match the text.
func matchesAnyTemplate(tpls []refdata.Template, m msg) bool {
	for _, t := range tpls {
		if t.AllowsSender(m.sender) && t.Re.MatchString(m.body) {
			return true
		}
	}
	return false
}

// loadDriftTemplates returns the approved templates of every account that is
// NOT accept-time enforced (an enforced account cannot drift past P1's 422
// gate): its own, and its company's.
//
// Read through the reference cache's loader and compiler, as P1 reads them.
// The monitor's own query joined templates.account_id, and since GW-16 a
// template is saved for a COMPANY (account_id NULL), so an account sending
// half its traffic off its company's templates gave no action at all (QA
// 2026-10-08, B-02).
func (p *Process) loadDriftTemplates(ctx context.Context) map[int64][]refdata.Template {
	if p.ref == nil {
		return nil
	}
	raw, err := p.ref.LoadTemplates(ctx)
	if err != nil {
		p.log.Printf("load drift templates: %v", err)
		return nil
	}
	if len(raw) == 0 {
		return nil
	}
	compiled := refdata.CompileTemplates(raw)
	rows, err := p.data.Accounts.Unenforced(ctx)
	if err != nil {
		p.log.Printf("load drift templates: accounts: %v", err)
		return nil
	}
	out := map[int64][]refdata.Template{}
	for _, r := range rows {
		id, co := r.Account, r.Company
		own := compiled[id]
		var ofCompany []refdata.Template
		if co != 0 {
			ofCompany = compiled[-co] // a company's templates are keyed by its id negated (refdata.LoadTemplates)
		}
		if len(own)+len(ofCompany) == 0 {
			continue
		}
		list := make([]refdata.Template, 0, len(own)+len(ofCompany))
		out[id] = append(append(list, own...), ofCompany...)
	}
	return out
}

// --- action ladder ---

// act applies one account's worst finding: warn -> audit only; throttle ->
// set abuse_throttle_tps to throttle_pct of what the account is allowed (min
// 1 TPS; an uncapped account gets a fixed 10 TPS ceiling); suspend ->
// is_disabled=1. Every action lands in audit_log; a per-action cooldown
// suppresses repeats (cooldownOf). The monitor only ever downgrades — lifting the throttle
// or re-enabling the account is an operator (P12) action.
//
// It writes abuse_throttle_tps, NOT mt_throughput (config/0123). Two reasons,
// and the first is the one that matters: since throughput became
// per-deployment, nothing reads mt_throughput any more, so cutting it would
// have left the monitor auditing a throttle that did nothing. And it cannot
// cut the per-deployment entries instead — an account assigned to ALL
// deployments has no entry to cut, and that is exactly the account most
// likely to be abusing. A penalty is not configuration; it gets its own
// field, applied across every deployment, and an operator clears it.
func (p *Process) act(ctx context.Context, acct int64, f finding) {
	action := "abuse." + f.sev.String()
	// The cooldown. A read that fails is NOT "no recent action" (2026-10-18,
	// go-live P10 X1): with audit_log unreadable an account inside a
	// 60-minute cooldown was cut 20 → 2 → 1/s in two sweeps. Nothing is done
	// this sweep; the finding is judged again at the next.
	//
	// A warning keeps off for its OWN rule (X5): one rule's warning used to
	// hold back another rule's warning — and its mail — for the whole
	// cooldown. A throttle or a suspension changes the account, and keeps the
	// level's cooldown whatever rule made it: a second rule's throttle would
	// cut again from the limit the first set (go-live P10 D5, a question for
	// the PO).
	n, err := p.data.Actions.Recent(ctx, acct, action, cooldownOf(f), f.sev == abuse.Warn, f.signal)
	if err != nil {
		p.log.Printf("%s acct=%d signal=%s: the cooldown cannot be read (%v) — nothing is done this sweep", action, acct, f.signal, err)
		return
	}
	if n > 0 {
		return // recently acted at this level
	}
	tg, err := p.data.Actions.Target(ctx, acct)
	if err != nil {
		return
	}
	username, deployJSON, already, disabled := tg.Username, tg.Deployments, tg.ThrottleTPS, tg.Disabled
	companyID, companyName := tg.CompanyID, tg.CompanyName
	// What the account is allowed today is the SUM of its per-deployment
	// rates (config/0123) — the number an operator was quoted. A throttle to
	// 20% of that is what an operator would recognise as 20%.
	deps, _ := deployment.Parse(deployJSON)
	throughput := deps.Total(deployment.MT)
	if already.Valid && already.Int64 > 0 && (throughput == 0 || int(already.Int64) < throughput) {
		// Already throttled: measure the next cut from where it stands, so a
		// second finding tightens rather than resetting to a looser value.
		throughput = int(already.Int64)
	}
	// What the account is changed to (nothing for a warning).
	change := dal.ChangeNone
	var newThrottle int
	switch f.sev {
	case abuse.Throttle:
		if disabled {
			return
		}
		newTPS := f.unlimitedTPS // unlimited (0) accounts get an absolute cap (rule param, B-P10-01)
		if newTPS <= 0 {
			newTPS = abuse.DefaultThrottleUnlimitedTPS
		}
		if throughput > 0 {
			newTPS = int(float64(throughput) * throttlePctOf(f) / 100)
			if newTPS < 1 {
				newTPS = 1
			}
			if throughput <= newTPS {
				return // already at or below the target — don't churn
			}
		}
		change, newThrottle = dal.ChangeThrottle, newTPS
		f.detail["prev_throughput"] = throughput
		f.detail["new_throughput"] = newTPS
	case abuse.Suspend:
		if disabled {
			return
		}
		change = dal.ChangeSuspend
		f.detail["prev_throughput"] = throughput
	}
	f.detail["signal"] = f.signal
	det, _ := json.Marshal(f.detail)
	// The audit row and the change of the account go in ONE transaction,
	// the row first (2026-10-18, go-live P10 X1): the account used to be
	// changed and the row written after, so an error between the two cut an
	// account with no row and no mail — and the Accounts mark kept naming the
	// first cut. Now both are there or neither.
	//
	// The row is filed the way the two doors file an account's row
	// (provision.audit, the Admin API's crud layer): under the account's
	// company, entity_id = the account's id, target_label = its name. It was
	// written with no company and with the username in entity_id, and the
	// Audit log shows a limited admin its own companies' rows only — so the
	// admin whose account was suspended could not see the row that said so
	// (QA 2026-10-08, B-15). Rows written before this keep their old shape:
	// the audit chain forbids rewriting them, and the readers take both.
	if err := p.data.Actions.Act(ctx, dal.AbuseAct{Account: acct, CompanyID: companyID, CompanyName: companyName,
		Action: action, EntityID: strconv.FormatInt(acct, 10), Label: username, Detail: string(det),
		Change: change, ThrottleTPS: newThrottle}); err != nil {
		var row *dal.AuditRowError
		if errors.As(err, &row) {
			p.log.Printf("audit %s acct=%d: %v — the account is not changed", action, acct, err)
		} else {
			p.log.Printf("%s acct=%d: %v — nothing is done this sweep", action, acct, err)
		}
		return
	}
	p.stats.action(action) // [SCOPE P1-STATUS]
	p.log.Printf("%s acct=%s(%d) signal=%s detail=%s", action, username, acct, f.signal, det)
}

// cooldownOf is how long the monitor keeps off an account after acting on it
// at one level. A rule that gives none, or 0, gets its own window: the
// traffic that was just acted on is then out of the window before it is
// judged again. With no cooldown at all the monitor acted on every sweep —
// four warn rows in four minutes on unchanged traffic, and a throttled
// account cut from 200/s to 1/s in three (QA 2026-10-08, B-11).
func cooldownOf(f finding) int {
	switch {
	case f.cooldownMin > 0:
		return f.cooldownMin
	case f.windowMin > 0:
		return f.windowMin
	}
	return 60
}

// throttlePctOf is the share of its rate a throttled account keeps. A rule
// that gives none gets abuse.DefaultThrottlePct, not 0% (B-12).
func throttlePctOf(f finding) float64 {
	if f.throttlePct > 0 {
		return f.throttlePct
	}
	return abuse.DefaultThrottlePct
}

// plainBody decrypts a stored body for scanning. On failure the stored value is
// returned unchanged: a signal that cannot read a message should score it as a
// non-match rather than abort the whole sweep. [SCOPE P1-CRYPTO]
func (p *Process) plainBody(ctx context.Context, stored string) string {
	if p.cry == nil {
		return stored
	}
	plain, err := p.cry.Decrypt(ctx, stored)
	if err != nil {
		return stored
	}
	return plain
}

func ratio(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}
