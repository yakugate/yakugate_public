package abusemonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/abuse"
	"github.com/yakugate/yakugate/internal/abusecount"
)

// The signals read from the accept path's counters, not from the message
// tables (2026-10-13, Saleem: "counters, no SQL on messages").
//
// The accept roles count every accepted message into Redis as it is queued
// (internal/abusecount). Here each signal takes its figures from those
// counters and hands them to the same judge the records path uses
// (judgeAIT, judgeSpend, …), so a verdict is formed by one piece of code
// whichever way the figures came.
//
// YAKU_ABUSE_SOURCE says which way:
//
//	records   the message tables, as before (the only way without Redis)
//	counters  the counters; no statement is run on a message table (the
//	          default with Redis, Saleem 2026-10-14: "switch to counters if
//	          redis is there")
//	compare   the records decide and act; the counters are judged beside
//	          them and every verdict that differs is logged
//
// A signal whose counters are still too young gives no verdict until they
// are old enough (Saleem 2026-10-14 chose this over reading the records in
// the meantime): see the first guard below.
//
// Three things keep the counters from acting on what they do not know:
//   - nothing is judged until the counters go back as far as the signal looks
//     (ab:epoch: since when they are whole). An empty baseline is not "no
//     history" — abuse.EvalSpend would read it as a floor and suspend every
//     steady account;
//   - Redis unreadable = no verdict this sweep, said on the status page, and
//     never a quiet return to the heavy SQL;
//   - the count of different numbers is an estimate, read a little low before
//     it can raise a verdict (distinctMargin).
//
// What is different from the records path, and cannot be otherwise: a message
// is counted when it is ACCEPTED, with the price fixed then. The records path
// saw it when it was SENT and its spend when it was RATED, so a backlog or a
// late billing role hid traffic from the window and dropped it into the
// baseline later.

const (
	sourceRecords  = "records"
	sourceCounters = "counters"
	sourceCompare  = "compare"
)

// source is where this sweep's figures come from: the records without Redis
// whatever is asked; with Redis what YAKU_ABUSE_SOURCE says, and the counters
// when it says nothing (or something that is not a source, with one warning).
func (p *Process) source() string {
	if p.counts == nil {
		return sourceRecords
	}
	switch v := strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_ABUSE_SOURCE"))); v {
	case sourceRecords:
		return sourceRecords
	case sourceCompare:
		return sourceCompare
	case sourceCounters, "":
		return sourceCounters
	default:
		unknownSource.Do(func() {
			if p.log != nil {
				p.log.Printf("YAKU_ABUSE_SOURCE=%q is not records, counters or compare: reading the counters", v)
			}
		})
		return sourceCounters
	}
}

// unknownSource says a wrong YAKU_ABUSE_SOURCE once, not every sweep.
var unknownSource sync.Once

// distinctMargin is taken off an estimated count of different numbers of
// distinctExactBelow or more before it is compared (the estimate's standard
// error is under 1%, and the rungs of the seeded AIT rule are 0.90 / 0.95 /
// 0.97 apart): an estimate may miss a verdict, never make one.
const (
	distinctMargin     = 0.02
	distinctExactBelow = 500
)

// baselineEnough is how much of the asked baseline the counters must cover
// for it to be judged on (a part hour at its far end is not read).
const baselineEnough = 0.70

type spanKey struct{ from, to int64 }

type spanTotals struct {
	sum     map[int64]*abusecount.Totals
	covered time.Duration
}

// countView is one sweep's look at the counters.
type countView struct {
	p     *Process
	now   time.Time
	epoch time.Time
	spans map[spanKey]spanTotals        // read once a sweep, shared by the signals
	texts map[int64][]abusecount.Sample // an account's kept texts, read once a sweep
}

// openCounters starts a sweep on the counters. ok=false: they cannot be
// judged from now (said on the status page) and the sweep gives no verdict.
func (p *Process) openCounters(ctx context.Context) (*countView, bool) {
	epoch, ok, err := p.counts.Epoch(ctx)
	switch {
	case err != nil:
		p.note("abuse counters", "the counters could not be read ("+err.Error()+"): no verdict this sweep")
		return nil, false
	case !ok:
		p.note("abuse counters", "nothing has been counted yet (no accept role on a build that counts, or Redis lost the counts): no verdict until there is")
		return nil, false
	}
	p.stats.clearNote("abuse counters")
	return &countView{p: p, now: time.Now(), epoch: epoch, spans: map[spanKey]spanTotals{}, texts: map[int64][]abusecount.Sample{}}, true
}

// winStart is the start of the last w minute buckets, the current one
// included: between w-1 and w minutes of traffic, never more than was asked.
func (v *countView) winStart(w int) time.Time {
	if w < 1 {
		w = 1
	}
	return v.now.Truncate(time.Minute).Add(-time.Duration(w-1) * time.Minute)
}

func (v *countView) totals(ctx context.Context, from, to time.Time) (spanTotals, bool) {
	k := spanKey{from.Unix(), to.Unix()}
	if st, ok := v.spans[k]; ok {
		return st, true
	}
	sum, covered, err := v.p.counts.Totals(ctx, from, to)
	if err != nil {
		v.p.note("abuse counters", "the counters could not be read ("+err.Error()+"): signals that needed them gave no verdict")
		return spanTotals{}, false
	}
	st := spanTotals{sum: sum, covered: covered}
	v.spans[k] = st
	return st, true
}

// waiting says a signal is not judged yet because the counters do not go
// back as far as it looks. The wording does not change from sweep to sweep,
// so it is logged once and then every half hour, not every minute.
func (v *countView) waiting(signal string, need time.Time) {
	back := v.now.Sub(need).Round(time.Minute)
	v.p.note(signal+" counters", fmt.Sprintf("not judged from the counters yet: they began at %s UTC and this signal looks back %s (judged from %s UTC)",
		v.epoch.UTC().Format("2006-01-02 15:04"), back, v.epoch.Add(back).UTC().Format("2006-01-02 15:04")))
}

// window is every account's counts of the last w minutes. ok=false: not
// judged this sweep.
func (v *countView) window(ctx context.Context, signal string, w int) (map[int64]*abusecount.Totals, time.Duration, bool) {
	from := v.winStart(w)
	if v.epoch.After(from) {
		v.waiting(signal, from)
		return nil, 0, false
	}
	st, ok := v.totals(ctx, from, v.now)
	if !ok {
		return nil, 0, false
	}
	return st.sum, st.covered, true
}

// baseline is every account's counts over the `hours` before the window of
// w minutes, and how many such windows that span holds.
func (v *countView) baseline(ctx context.Context, signal string, w, hours int) (map[int64]*abusecount.Totals, float64, bool) {
	to := v.winStart(w)
	from := to.Add(-time.Duration(hours) * time.Hour)
	if v.epoch.After(from) {
		v.waiting(signal, from)
		return nil, 0, false
	}
	st, ok := v.totals(ctx, from, to)
	if !ok {
		return nil, 0, false
	}
	asked := to.Sub(from)
	if asked <= 0 || float64(st.covered) < baselineEnough*float64(asked) {
		v.p.note(signal+" counters", fmt.Sprintf("not judged: the counters cover %s of the %s baseline", st.covered, asked))
		return nil, 0, false
	}
	v.p.stats.clearNote(signal + " counters")
	return st.sum, st.covered.Minutes() / float64(w), true
}

func (p *Process) evalVelocityCounters(ctx context.Context, prm abuse.VelocityParams, merge func(int64, finding)) {
	v := p.view
	win, _, ok := v.window(ctx, "velocity", prm.WindowMin)
	if !ok {
		return
	}
	base, windows, ok := v.baseline(ctx, "velocity", prm.WindowMin, prm.BaselineHours)
	if !ok {
		return
	}
	current, baseline := map[int64]int{}, map[int64]int{}
	for id, t := range win {
		if t.N > 0 {
			current[id] = int(t.N)
		}
	}
	for id, t := range base {
		baseline[id] = int(t.N)
	}
	p.judgeVelocity(current, baseline, windows, prm, merge)
}

func (p *Process) evalSpendCounters(ctx context.Context, prm abuse.SpendParams, merge func(int64, finding)) {
	v := p.view
	win, _, ok := v.window(ctx, "spend", prm.WindowMin)
	if !ok {
		return
	}
	base, windows, ok := v.baseline(ctx, "spend", prm.WindowMin, prm.BaselineHours)
	if !ok {
		return
	}
	current, baseline := map[int64]float64{}, map[int64]float64{}
	for id, t := range win {
		if t.N > 0 {
			current[id] = t.Spend()
		}
	}
	for id, t := range base {
		baseline[id] = t.Spend()
	}
	p.judgeSpend(ctx, current, baseline, windows, prm, merge)
}

func (p *Process) evalBadNumbersCounters(ctx context.Context, prm abuse.BadNumberParams, merge func(int64, finding)) {
	win, _, ok := p.view.window(ctx, "badnum", prm.WindowMin)
	if !ok {
		return
	}
	p.stats.clearNote("badnum counters")
	misses, consulted := map[int64]int{}, map[int64]int{}
	for id, t := range win {
		if t.HLRConsulted > 0 {
			consulted[id], misses[id] = int(t.HLRConsulted), int(t.HLRMiss)
		}
	}
	p.judgeBadNumbers(misses, consulted, prm, merge)
}

func (p *Process) evalCharsetCounters(ctx context.Context, prm abuse.CharsetParams, merge func(int64, finding)) {
	v := p.view
	win, _, ok := v.window(ctx, "charset", prm.WindowMin)
	if !ok {
		return
	}
	base, _, ok := v.baseline(ctx, "charset", prm.WindowMin, prm.BaselineHours)
	if !ok {
		return
	}
	byEnc := func(in map[int64]*abusecount.Totals) map[int64]*encCounts {
		out := map[int64]*encCounts{}
		for id, t := range in {
			if len(t.Enc) == 0 {
				continue
			}
			c := &encCounts{byEnc: map[string]int{}}
			for enc, n := range t.Enc {
				c.byEnc[enc] = int(n)
			}
			out[id] = c
		}
		return out
	}
	p.judgeCharset(byEnc(win), byEnc(base), prm, merge)
}

func (p *Process) evalAITCounters(ctx context.Context, prm abuse.AITParams, merge func(int64, finding)) {
	v := p.view
	// The accept roles count prefixes at the length this rule asks for.
	// After a change of length, what was counted before is not comparable:
	// AIT waits one window.
	since, err := p.counts.AskPrefixLen(ctx, prm.PrefixLen)
	if err != nil {
		p.note("ait counters", "the prefix length could not be given to the accept roles ("+err.Error()+"): not judged")
		return
	}
	from := v.winStart(prm.WindowMin)
	if since.After(from) {
		p.note("ait counters", fmt.Sprintf("not judged yet: number prefixes of %d digits have been counted only since %s",
			prm.PrefixLen, since.UTC().Format("15:04")))
		return
	}
	win, _, ok := v.window(ctx, "ait", prm.WindowMin)
	if !ok {
		return
	}
	prefixes, err := p.counts.Prefixes(ctx, from, v.now, prm.PrefixLen)
	if err != nil {
		p.note("ait counters", "the prefix counts could not be read ("+err.Error()+"): not judged")
		return
	}
	p.stats.clearNote("ait counters")
	accts := map[int64]*aitCount{}
	for id, t := range win {
		if t.N == 0 {
			continue
		}
		a := &aitCount{n: int(t.N), uniq: int(t.N), prefixes: map[string]int{}}
		for pfx, c := range prefixes[id] {
			a.prefixes[pfx] = int(c)
		}
		// Different numbers are asked for only where they can matter: an
		// account that would get no verdict even if every number were
		// different is not asked about.
		probe := prm
		if !p.over.apply("ait", id, &probe) {
			continue
		}
		top := 0
		for pfx, c := range a.prefixes {
			if pfx != abusecount.OtherPrefix && c > top {
				top = c
			}
		}
		if abuse.EvalAIT(a.n, a.n, top, probe) == abuse.None {
			continue
		}
		est, err := p.counts.Distinct(ctx, id, from, v.now)
		if err != nil {
			continue // not known: no verdict for this account this sweep
		}
		uniq := int(est)
		if uniq >= distinctExactBelow {
			uniq = int(float64(est) * (1 - distinctMargin))
		}
		if uniq > a.n {
			uniq = a.n
		}
		a.uniq = uniq
		accts[id] = a
	}
	p.judgeAIT(accts, prm, merge)
}

func (p *Process) evalFormattingCounters(ctx context.Context, prm abuse.FormattingParams, c *contributions) {
	v := p.view
	win, _, ok := v.window(ctx, "formatting", prm.WindowMin)
	if !ok {
		return
	}
	base, _, ok := v.baseline(ctx, "formatting", prm.WindowMin, prm.BaselineHours)
	if !ok {
		return
	}
	for id, t := range win {
		ws, wn := t.Shape()
		if wn == 0 {
			continue
		}
		pp := prm
		if !p.over.apply("formatting", id, &pp) {
			continue // this account's own rules (config/0106)
		}
		var bs abuse.Shape
		bn := 0
		if b := base[id]; b != nil {
			bs, bn = b.Shape()
		}
		if s := abuse.EvalFormatting(ws, bs, wn, bn, pp); s > 0 {
			c.add(id, "formatting", s)
			c.note(id, "formatting", map[string]any{"strength": round3(s),
				"window": shapeNote(ws), "baseline": shapeNote(bs), "window_msgs": wn, "baseline_msgs": bn})
		}
	}
}

// sampleCounters is sampleMsgs from the kept texts: for each account that
// sent in the last fromMin minutes (only = these accounts), its newest
// texts of that window, at most perAcct each and limit in all.
func (p *Process) sampleCounters(ctx context.Context, reader string, fromMin, perAcct, limit int, only map[int64]bool) map[int64][]msg {
	out := map[int64][]msg{}
	v := p.view
	win, _, ok := v.window(ctx, reader, fromMin)
	if !ok {
		return out
	}
	p.stats.clearNote(reader + " counters")
	if perAcct <= 0 || perAcct > abusecount.SampleKeep {
		perAcct = defaultSampleMsgs
	}
	if limit <= 0 {
		limit = 2000
	}
	ids := make([]int64, 0, len(win))
	for id, t := range win {
		if t.N > 0 && (len(only) == 0 || only[id]) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	// When the ceiling cuts a read short, the next sweep starts at the
	// account after the last one read (the records path's own rule).
	n := len(ids)
	if n == 0 {
		return out
	}
	start := p.cursor[reader] % n
	since := v.winStart(fromMin)
	read, next := 0, -1
	for i := 0; i < n; i++ {
		at := (start + i) % n
		if read >= limit {
			next = at
			break
		}
		id := ids[at]
		all, cached := v.texts[id]
		if !cached {
			var err error
			if all, err = p.counts.Samples(ctx, id, time.Time{}, 0); err != nil {
				continue
			}
			v.texts[id] = all
		}
		for _, s := range all {
			if s.At < since.Unix() || len(out[id]) >= perAcct || read >= limit {
				continue
			}
			read++
			out[id] = append(out[id], msg{body: p.plainBody(ctx, s.Body), dest: s.Dest, sender: s.Sender})
		}
	}
	if p.cursor == nil {
		p.cursor = map[string]int{}
	}
	if next >= 0 {
		p.cursor[reader] = next
	} else {
		delete(p.cursor, reader)
	}
	return out
}

// sentCounters is each listed account's messages of the last windowMin
// minutes, from the counters. A long window is read by the hour and scaled
// to the whole of it. ok=false: not judged this sweep.
func (p *Process) sentCounters(ctx context.Context, signal string, windowMin int, only map[int64]bool) (map[int64]int, bool) {
	out := map[int64]int{}
	win, covered, ok := p.view.window(ctx, signal, windowMin)
	if !ok {
		return out, false
	}
	p.stats.clearNote(signal + " counters")
	asked := time.Duration(windowMin) * time.Minute
	scale := 1.0
	if covered > 0 && covered < asked {
		scale = float64(asked) / float64(covered)
	}
	for id := range only {
		if t := win[id]; t != nil {
			out[id] = int(float64(t.N)*scale + 0.5)
		}
	}
	return out, true
}

// compareWithCounters judges the same sweep from the counters and says where
// its verdicts differ from the records' (which are the ones acted on).
func (p *Process) compareWithCounters(ctx context.Context, run func(tables []string, merge func(int64, finding), contrib *contributions),
	records map[int64]finding) {
	v, ok := p.openCounters(ctx)
	if !ok {
		return
	}
	counted := map[int64]finding{}
	contrib := newContributions()
	p.view = v
	run(nil, func(acct int64, f finding) {
		if f.signal != "score" {
			contrib.add(acct, f.signal, severityStrength(f.sev))
		}
		if cur, ok := counted[acct]; !ok || f.sev > cur.sev {
			counted[acct] = f
		}
	}, contrib)
	p.view = nil

	say := func(f finding, ok bool) string {
		if !ok || f.sev == abuse.None {
			return "nothing"
		}
		return f.signal + " " + f.sev.String()
	}
	var lines []string
	seen := map[int64]bool{}
	for id := range records {
		seen[id] = true
	}
	for id := range counted {
		seen[id] = true
	}
	same := 0
	for id := range seen {
		r, rok := records[id]
		c, cok := counted[id]
		if say(r, rok) == say(c, cok) {
			same++
			continue
		}
		detail, _ := json.Marshal(c.detail)
		lines = append(lines, fmt.Sprintf("account %d: the records say %s, the counters say %s %s", id, say(r, rok), say(c, cok), detail))
	}
	sort.Strings(lines)
	for i, l := range lines {
		if i == 20 {
			p.log.Printf("compare: … and %d more", len(lines)-20)
			break
		}
		p.log.Printf("compare: %s", l)
	}
	if len(lines) == 0 {
		p.stats.clearNote("compare")
		return
	}
	p.note("compare", fmt.Sprintf("%d verdict(s) differ between the records and the counters this sweep, %d agree (the records were acted on; see the log)",
		len(lines), same))
}

// refusalDays is how far back the reputation score counts refusals.
const refusalDays = 30

// refusalsFromCounters is each account's refused parts of the last 30 days
// from the accept path's daily counts. ok=false: the monitor reads the
// records, or the counts do not go back 30 days yet — the caller reads the
// rejects tables, as before.
func (p *Process) refusalsFromCounters(ctx context.Context, now time.Time) (map[int64]int64, bool) {
	if p.source() != sourceCounters {
		return nil, false
	}
	epoch, ok, err := p.counts.Epoch(ctx)
	if err != nil || !ok || epoch.After(now.AddDate(0, 0, -refusalDays)) {
		return nil, false
	}
	counted, _, err := p.counts.Refusals(ctx, refusalDays)
	if err != nil {
		return nil, false
	}
	return counted, true
}
