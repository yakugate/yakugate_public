package console

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The console's monitoring, at par with the portal's (Saleem 2026-10-05: "add
// the new monitoring things there as well"): any time picked, Live, every
// deployment told apart, events with the gateway before / at / after them,
// day summaries, and every queue and every MySQL and Redis server with its
// history. The figures come from the same engine code the roles and the
// Admin API use (internal/monitor); all times are UTC.

// monView is what the monitoring screens show.
type monView struct {
	deployment string    // "" = every gateway
	at         time.Time // zero = the latest reports
	liveUntil  time.Time // Live: ask the gateway at every read until then
	liveNote   string    // how the last Live ask went
	known      []string  // the deployments seen reporting, for the picker
	interval   int       // the gateway's reporting interval, as last seen
}

const liveFor = time.Minute

func (v *monView) live() bool { return v.at.IsZero() && time.Now().Before(v.liveUntil) }

func (v *monView) gatewayLabel() string {
	if v.deployment == "" {
		return "all gateways"
	}
	return v.deployment
}

// hotDB opens the hot store once, for the monitoring history (read through
// the MySQL DAL's history statements).
func (p *Process) hotDB() (dal.MonitorStatements, error) {
	if p.hot != nil {
		return p.hot, nil
	}
	if p.cfg.HotBackend != config.BackendMySQL || p.cfg.HotDSN == "" {
		return nil, fmt.Errorf("this needs the monitoring history, kept in a MySQL hot store; this gateway has none set")
	}
	h, err := dal.OpenMonitorReader(config.BackendMySQL, p.cfg.HotDSN)
	if err != nil {
		return nil, err
	}
	p.opened = append(p.opened, h)
	p.hot = h
	return h, nil
}

// overview reads what the screens show now: a picked time from the history,
// Live by asking the gateway, else the latest reports — live from Redis when
// it is on, else the newest rows of the history — for the gateway chosen.
func (p *Process) overview(ctx context.Context) (monitor.Overview, error) {
	v := &p.mon
	rctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if !v.at.IsZero() {
		hot, err := p.hotDB()
		if err != nil {
			return monitor.Overview{}, err
		}
		snaps, nearest, interval, err := monitor.HistoryAt(rctx, hot, v.at, v.deployment)
		if err != nil {
			return monitor.Overview{}, fmt.Errorf("read the monitoring history: %w", err)
		}
		v.interval = interval
		if len(snaps) == 0 {
			return monitor.Overview{Time: v.at}, fmt.Errorf("no report near %s UTC — reports are kept a few days; older days have a summary (Days)",
				v.at.Format("2006-01-02 15:04:05"))
		}
		return monitor.Summarize(snaps, fmt.Sprintf("AT %s UTC, nearest report %s", v.at.Format("2006-01-02 15:04:05"),
			nearest.Format("15:04:05")), v.at), nil
	}
	if v.live() {
		hot, err := p.hotDB()
		if err != nil {
			return monitor.Overview{}, err
		}
		_, answered, of, err := monitor.AskAndWait(rctx, hot, v.deployment, 3*time.Second)
		if err != nil {
			v.liveNote = "the gateway could not be asked: " + err.Error()
		} else {
			v.liveNote = fmt.Sprintf("%d of %d roles answered", answered, of)
		}
		snaps, err := monitor.FromHistoryFor(rctx, hot, 30*time.Second, v.deployment)
		if err != nil {
			return monitor.Overview{}, fmt.Errorf("read the monitoring history: %w", err)
		}
		o := monitor.Summarize(snaps, "LIVE · "+v.liveNote, time.Now())
		p.remember(o)
		return o, nil
	}
	if p.live != nil {
		// Redis gets a short time of its own: when it is slow, the history
		// below must still have its whole budget (it used to be handed what
		// was left of this one — nothing — so the screen showed no figures).
		lctx, lcancel := context.WithTimeout(ctx, 3*time.Second)
		snaps, err := monitor.ClusterStatus(lctx, p.live)
		lcancel()
		if err == nil {
			all := monitor.Summarize(snaps, "live from Redis", time.Now())
			p.remember(all)
			if v.deployment == "" {
				return all, nil
			}
			return monitor.Summarize(monitor.FilterDeployment(snaps, v.deployment), "live from Redis", time.Now()), nil
		}
		p.warnf("\n  Redis did not answer (%v); reading the monitoring history instead.\n", err)
	}
	hot, err := p.hotDB()
	if err != nil {
		return monitor.Overview{}, fmt.Errorf("monitoring needs Redis, or a MySQL hot store for its history; this gateway has neither set")
	}
	hctx, hcancel := context.WithTimeout(ctx, 8*time.Second)
	defer hcancel()
	snaps, err := monitor.FromHistoryFor(hctx, hot, 20*time.Second, v.deployment)
	if err != nil {
		return monitor.Overview{}, fmt.Errorf("read the monitoring history: %w", err)
	}
	o := monitor.Summarize(snaps, "from the history (the last 20 seconds)", time.Now())
	p.remember(o)
	return o, nil
}

// remember keeps the deployments seen, for the gateway picker.
func (p *Process) remember(o monitor.Overview) {
	for _, d := range o.Deployments {
		if d.ID != "" && !containsStr(p.mon.known, d.ID) {
			p.mon.known = append(p.mon.known, d.ID)
		}
	}
	sort.Strings(p.mon.known)
}

func containsStr(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// ── picking a time and a gateway ────────────────────────────────────────────

// parseWhen reads a time typed at the console, in UTC: "2026-10-05 10:00:07",
// "2026-10-05T10:00", "10:00" or "10:00:07" (today), "-15m" or "-2h" (that
// long ago).
func parseWhen(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	now = now.UTC()
	if strings.HasPrefix(s, "-") {
		d, err := time.ParseDuration(s[1:])
		if err != nil || d <= 0 {
			return time.Time{}, fmt.Errorf("%q: write it as -15m, -2h or -1h30m", s)
		}
		return now.Add(-d), nil
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02 15:04", "2006-01-02T15:04"} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t, nil
		}
	}
	for _, layout := range []string{"15:04:05", "15:04"} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC), nil
		}
	}
	return time.Time{}, fmt.Errorf("%q is not a time: write 2026-10-05 10:00:07, 10:00, or -15m (UTC)", s)
}

// askTime asks for a time; ok=false when the person backed out.
func (p *Process) askTime(sc *bufio.Scanner) (time.Time, bool) {
	for {
		v, ok := p.ask(sc, "  Go to (UTC: 2026-10-05 10:00:07, 10:00, -15m; Enter to go back)")
		if !ok || v == "" || v == "0" {
			return time.Time{}, false
		}
		t, err := parseWhen(v, time.Now())
		if err != nil {
			p.errf("  %v\n", err)
			continue
		}
		if t.After(time.Now()) {
			p.errf("  That is in the future.\n")
			continue
		}
		return t, true
	}
}

// chooseGateway asks which deployment to show.
func (p *Process) chooseGateway(sc *bufio.Scanner) {
	if len(p.mon.known) == 0 {
		p.printf("\n  Only one gateway is reporting.\n")
		return
	}
	opts := []opt{item("1", "All gateways")}
	for i, d := range p.mon.known {
		opts = append(opts, wideItem(strconv.Itoa(i+2), d))
	}
	opts[0] = wideItem("1", "All gateways")
	opts = append(opts, wideItem("0", "Back"))
	k, ok := p.choose(sc, "Show", "", opts)
	if !ok || k == "0" || k == "" {
		return
	}
	n, err := strconv.Atoi(k)
	switch {
	case err != nil:
		p.errf("  %q is not one of the choices.\n", k)
	case n == 1:
		p.mon.deployment = ""
	case n >= 2 && n-2 < len(p.mon.known):
		p.mon.deployment = p.mon.known[n-2]
	default:
		p.errf("  %q is not one of the choices.\n", k)
	}
}

// ── a moment, in a few lines ───────────────────────────────────────────────

// printMoment is the gateway at one moment in a few lines: for an event's
// before / at / after.
func (p *Process) printMoment(label string, at time.Time, o monitor.Overview) {
	t := o.Traffic
	waiting, oldest := int64(0), int64(0)
	for _, q := range o.Queues {
		waiting += q.Depth
		oldest = max(oldest, q.OldestSec)
	}
	cust := int64(0)
	for _, q := range o.CustomerQueues {
		cust += q.Depth
	}
	p.printf("\n  %s — %s UTC\n", label, at.Format("15:04:05"))
	p.printf("    roles %d · in %s/s · out %s/s · receipts %s/s\n", len(o.Roles), perS(t.Accepted.PerS), perS(t.Sent.PerS), perS(t.Receipts.PerS))
	line := fmt.Sprintf("    waiting for vendors %d", waiting)
	if waiting > 0 {
		line += ", oldest " + monitor.Ago(time.Duration(oldest)*time.Second)
	}
	p.printf("%s · for customers %d\n", line, cust)
	if s := o.SMPP; s.VendorsTotal > 0 {
		p.printf("    SMPP vendors bound %d of %d\n", s.VendorsBound, s.VendorsTotal)
	}
	for _, st := range o.Stores {
		p.printf("    %s %s %s %.1f ms\n", storeKind(st.Kind), st.Name, st.Health, st.PingMS)
	}
	for i, pr := range o.Problems {
		if i == 3 {
			p.printf("    … and %d more\n", len(o.Problems)-3)
			break
		}
		p.errf("    ✗ %s\n", pr)
	}
}

func storeKind(k string) string {
	return map[string]string{"mysql": "MySQL", "redis": "Redis"}[k]
}

// ── events ──────────────────────────────────────────────────────────────────

// eventsScreen lists the last 24 hours' events, newest first, and opens one.
// It returns a time to go to, when the person asked for one.
func (p *Process) eventsScreen(ctx context.Context, sc *bufio.Scanner) (time.Time, bool) {
	hot, err := p.hotDB()
	if err != nil {
		p.refusal(err)
		return time.Time{}, false
	}
	filter := ""
	for {
		to := time.Now().UTC()
		if !p.mon.at.IsZero() {
			to = p.mon.at.Add(time.Hour)
		}
		evs, err := monitor.ListEvents(ctx, hot, to.Add(-24*time.Hour), to, p.mon.deployment, filter, 60)
		if err != nil {
			p.refusal(fmt.Errorf("read the events: %w", err))
			return time.Time{}, false
		}
		shown := map[string]string{"": "everything", "critical": "critical only", "warning": "warnings and up"}[filter]
		p.printf("\n  Events — the 24 hours to %s UTC, %s, %s (newest first)\n", to.Format("2006-01-02 15:04"), p.mon.gatewayLabel(), shown)
		if len(evs) == 0 {
			p.okf("    nothing happened\n")
		}
		var rows []opt
		for i, e := range evs {
			text := fmt.Sprintf("   %2d  %s  %-8s %-26s %s", i+1, e.At.Format("01-02 15:04:05"), e.Severity,
				trim(e.Kind+" "+e.Phase, 26), trim(e.Detail, 70))
			rows = append(rows, row(int64(i+1), text, ""))
		}
		rows = append(rows, opt{Key: "c", Text: "    c  Critical only"}, opt{Key: "w", Text: "    w  Warnings and up"},
			opt{Key: "a", Text: "    a  Everything"})
		k, ok := p.choose(sc, "Open an event (number), or c / w / a; 0 back", "0", rows)
		if !ok || k == "0" || k == "" {
			return time.Time{}, false
		}
		switch k {
		case "c":
			filter = "critical"
			continue
		case "w":
			filter = "warning"
			continue
		case "a":
			filter = ""
			continue
		}
		n, err := strconv.Atoi(k)
		if err != nil || n < 1 || n > len(evs) {
			p.errf("  %q is not one of the events.\n", k)
			continue
		}
		if at, goTo := p.eventDetail(ctx, sc, hot, evs[n-1]); goTo {
			return at, true
		}
	}
}

// eventDetail shows one event with the gateway before, at and after it.
func (p *Process) eventDetail(ctx context.Context, sc *bufio.Scanner, hot dal.MonitorStatements, e monitor.StoredEvent) (time.Time, bool) {
	_, moments, err := monitor.EventMoments(ctx, hot, e.ID)
	if err != nil {
		p.refusal(err)
		return time.Time{}, false
	}
	p.printf("\n  %s %s — %s on %s, %s UTC\n", e.Kind, e.Phase, e.Severity, e.Deployment, e.At.Format("2006-01-02 15:04:05"))
	p.printf("  %s\n", e.Detail)
	if len(moments) == 0 {
		p.printf("\n  The reports around this event were not kept.\n")
	}
	for _, m := range moments {
		p.printMoment(map[string]string{"before": "Before", "at": "At the event", "after": "After"}[m.Label], m.At,
			monitor.Summarize(m.Snaps, "", m.At))
	}
	k, ok := p.choose(sc, "Choose", "0", []opt{item("1", "Go to this time"), item("0", "Back")})
	if ok && k == "1" {
		return e.At, true
	}
	return time.Time{}, false
}

// ── days ────────────────────────────────────────────────────────────────────

// daysScreen lists the day summaries of the last 10 days and opens one.
func (p *Process) daysScreen(ctx context.Context, sc *bufio.Scanner) (time.Time, bool) {
	hot, err := p.hotDB()
	if err != nil {
		p.refusal(err)
		return time.Time{}, false
	}
	today := time.Now().UTC()
	days, err := monitor.ListDays(ctx, hot, today.AddDate(0, 0, -10).Format("2006-01-02"), today.Format("2006-01-02"), p.mon.deployment)
	if err != nil {
		p.refusal(fmt.Errorf("read the day summaries: %w", err))
		return time.Time{}, false
	}
	for {
		p.printf("\n  Days — one summary per gateway per UTC day, %s (newest first)\n", p.mon.gatewayLabel())
		if len(days) == 0 {
			p.printf("    none yet: a day is summarized once it is over\n")
		}
		var rows []opt
		for i, d := range days {
			text := fmt.Sprintf("   %2d  %s  %-10s accepted %-9s refused %-7s sent ~%-9s events %d critical, %d warning",
				i+1, d.Day, trim(d.Deployment, 10), comma(d.Accepted), comma(d.Rejected), comma(d.Sent), d.Counts["critical"], d.Counts["warning"])
			rows = append(rows, row(int64(i+1), text, ""))
		}
		n, ok := p.chooseID(sc, "Open a day (number), 0 back", rows)
		if !ok || n == 0 {
			return time.Time{}, false
		}
		if n < 1 || int(n) > len(days) {
			p.errf("  %d is not one of the days.\n", n)
			continue
		}
		if at, goTo := p.dayDetail(sc, days[n-1]); goTo {
			return at, true
		}
	}
}

func (p *Process) dayDetail(sc *bufio.Scanner, d monitor.DaySummary) (time.Time, bool) {
	p.printf("\n  %s on %s (UTC) — reported every %d s, %s reports\n", d.Day, d.Deployment, d.IntervalSec, comma(d.Samples))
	p.printf("    accepted %s · refused %s · sent about %s · receipts about %s · MO about %s\n",
		comma(d.Accepted), comma(d.Rejected), comma(d.Sent), comma(d.Receipts), comma(d.MO))
	var in []float64
	for _, h := range d.Hours {
		in = append(in, h.TPSIn)
	}
	p.printf("    incoming, hour by hour  %s\n", spark(in))
	var peaks []string
	for _, k := range []string{"tps_in", "tps_out", "queue_depth", "queue_age_sec", "latency_p95_ms"} {
		if pk, ok := d.Peaks[k]; ok && pk.At != "" {
			peaks = append(peaks, fmt.Sprintf("%s %s at %s", map[string]string{"tps_in": "in/s", "tps_out": "out/s",
				"queue_depth": "waiting", "queue_age_sec": "oldest wait s", "latency_p95_ms": "p95 ms"}[k], trimFloat(pk.Value), clock(pk.At)))
		}
	}
	if len(peaks) > 0 {
		p.printf("    peaks: %s\n", strings.Join(peaks, " · "))
	}
	var roles []string
	for r, pct := range d.RoleUptime {
		roles = append(roles, fmt.Sprintf("%s %.0f%%", r, pct))
	}
	sort.Strings(roles)
	if len(roles) > 0 {
		p.printf("    reporting: %s\n", strings.Join(roles, ", "))
	}
	for i, q := range d.Queues {
		if i == 8 {
			p.printf("    … %d more queues\n", len(d.Queues)-8)
			break
		}
		p.printf("    queue %-22s in %-9s out %-9s most waiting %s at %s, longest wait %ds\n", trim(dayQueueName(q), 22),
			comma(q.In), comma(q.Out), comma(q.PeakWaiting), clock(q.PeakWaitingAt), q.PeakAgeSec)
	}
	for _, s := range d.Stores {
		used := fmt.Sprintf("connections up to %.0f%%", s.MaxConnPct)
		if s.Kind == "redis" {
			used = fmt.Sprintf("memory up to %.0f%%", s.MaxMemPct)
		}
		p.printf("    %s %s: down %ds, warning %ds, worst round trip %.1f ms, %s, restarts %d\n",
			storeKind(s.Kind), s.Addr, s.DownSec, s.WarningSec, s.MaxPingMS, used, s.Restarts)
	}
	if len(d.Events) > 0 {
		p.printf("    events:\n")
		for i, e := range d.Events {
			if i == 15 {
				p.printf("      … %d more\n", len(d.Events)-15)
				break
			}
			p.printf("      %s %-8s %s %s — %s\n", clock(e.At), e.Severity, e.Kind, e.Phase, trim(e.Detail, 80))
		}
	}
	k, ok := p.choose(sc, "Choose", "0", []opt{item("1", "Go to the busiest moment of the day"), item("0", "Back")})
	if ok && k == "1" {
		if pk, ok := d.Peaks["tps_in"]; ok && pk.At != "" {
			if t, err := time.Parse(time.RFC3339, pk.At); err == nil {
				return t, true
			}
		}
		p.printf("  The day has no busiest moment recorded.\n")
	}
	return time.Time{}, false
}

func dayQueueName(q monitor.DayQueue) string {
	return queueName(monitor.QueueLine{Kind: q.Kind, Vendor: q.Queue})
}

// ── queues ──────────────────────────────────────────────────────────────────

// queuesScreen lists every queue — vendors' and customers', idle ones too —
// and opens one with its last hour.
func (p *Process) queuesScreen(ctx context.Context, sc *bufio.Scanner) {
	for {
		o, err := p.overview(ctx)
		if err != nil {
			p.refusal(err)
			return
		}
		all := append(append([]monitor.QueueLine{}, o.Queues...), o.CustomerQueues...)
		// The busiest first, vendors' and customers' alike.
		sort.SliceStable(all, func(i, j int) bool {
			if all[i].Depth != all[j].Depth {
				return all[i].Depth > all[j].Depth
			}
			return all[i].InPerS+all[i].OutPerS > all[j].InPerS+all[j].OutPerS
		})
		p.printf("\n  Queues — %s, %s\n", p.mon.gatewayLabel(), o.Source)
		p.printf("   %-4s %-24s %9s %8s %7s %8s %8s %10s\n", "", "queue", "waiting", "ready", "held", "in/s", "out/s", "oldest")
		var rows []opt
		for i, q := range all {
			old := "—"
			if q.Depth > 0 {
				old = monitor.Ago(time.Duration(q.OldestSec) * time.Second)
			}
			rows = append(rows, row(int64(i+1), fmt.Sprintf("   %2d   %-24s %9d %8d %7d %8s %8s %10s", i+1, trim(queueName(q), 24),
				q.Depth, q.Ready, q.Held, perS(q.InPerS), perS(q.OutPerS), old), ""))
		}
		if len(all) == 0 {
			p.printf("    no queue has reported\n")
		}
		n, ok := p.chooseID(sc, "Open a queue (number), 0 back", rows)
		if !ok || n == 0 {
			return
		}
		if n < 1 || int(n) > len(all) {
			p.errf("  %d is not one of the queues.\n", n)
			continue
		}
		p.queueDetail(ctx, all[n-1])
	}
}

func (p *Process) queueDetail(ctx context.Context, q monitor.QueueLine) {
	p.printf("\n  %s — %d waiting (%d ready, %d held); in %s/s, out %s/s", queueName(q), q.Depth, q.Ready, q.Held, perS(q.InPerS), perS(q.OutPerS))
	if q.Depth > 0 {
		p.printf("; the oldest ready one has waited %s", monitor.Ago(time.Duration(q.OldestSec)*time.Second))
	}
	p.printf("\n")
	hot, err := p.hotDB()
	if err != nil {
		p.refusal(err)
		return
	}
	to := time.Now().UTC()
	if !p.mon.at.IsZero() {
		to = p.mon.at.Add(30 * time.Minute)
	}
	name := q.Vendor
	pts, bucket, err := monitor.QueueSeries(ctx, hot, q.Kind, name, to.Add(-time.Hour), to, p.mon.deployment, max(p.mon.interval, 5), 60)
	if err != nil {
		p.refusal(err)
		return
	}
	p.printf("  the hour to %s UTC, a point every %ds:\n", to.Format("15:04"), bucket)
	for _, s := range []struct{ key, label string }{{"waiting", "waiting"}, {"oldest_sec", "oldest (s)"}, {"in_per_sec", "in /s"}, {"out_per_sec", "out /s"}} {
		vals, last := seriesOf(pts, s.key)
		p.printf("    %-11s %s  %s\n", s.label, spark(vals), trimFloat(last))
	}
}

// ── MySQL and Redis ─────────────────────────────────────────────────────────

// storesScreen lists each MySQL and Redis server and opens one: its whole
// reading and its last hour.
func (p *Process) storesScreen(ctx context.Context, sc *bufio.Scanner) {
	for {
		o, err := p.overview(ctx)
		if err != nil {
			p.refusal(err)
			return
		}
		p.printf("\n  MySQL & Redis — %s, %s\n", p.mon.gatewayLabel(), o.Source)
		if len(o.Stores) == 0 {
			p.printf("    no store reading: the http-server reads MySQL and Redis each time it reports\n")
		}
		var rows []opt
		for i, s := range o.Stores {
			text := fmt.Sprintf("   %2d   %-6s %-26s %-8s %7.1f ms  %s", i+1, storeKind(s.Kind), trim(s.Name, 26), s.Health, s.PingMS, s.Figures)
			note := ""
			if len(s.Reasons) > 0 {
				note = "          " + strings.Join(s.Reasons, " · ")
			}
			rows = append(rows, row(int64(i+1), text, note))
		}
		n, ok := p.chooseID(sc, "Open a store (number), 0 back", rows)
		if !ok || n == 0 {
			return
		}
		if n < 1 || int(n) > len(o.Stores) {
			p.errf("  %d is not one of the stores.\n", n)
			continue
		}
		p.storeDetail(ctx, o.Stores[n-1])
	}
}

func (p *Process) storeDetail(ctx context.Context, s monitor.StoreLine) {
	r := s.Raw
	f := func(m map[string]any, k string) float64 { v, _ := m[k].(float64); return v }
	sub := func(k string) map[string]any { m, _ := r[k].(map[string]any); return m }
	str := func(m map[string]any, k string) string { v, _ := m[k].(string); return v }
	p.printf("\n  %s %s — %s, round trip %.2f ms\n", storeKind(s.Kind), s.Name, s.Health, s.PingMS)
	for _, x := range s.Reasons {
		p.errf("    ✗ %s\n", x)
	}
	if notes, ok := r["notes"].([]any); ok {
		for _, n := range notes {
			p.printf("    · %v\n", n)
		}
	}
	if s.Kind == "mysql" {
		c, rt, in, rp := sub("connections"), sub("rates"), sub("innodb"), sub("replica")
		p.printf("    version %s, up %s\n", str(r, "version"), monitor.Ago(time.Duration(f(r, "uptime_sec"))*time.Second))
		p.printf("    connections %.0f of %.0f (%.1f%%), %.0f running, the most ever %.0f\n", f(c, "current"), f(c, "max"), f(c, "pct"), f(c, "running"), f(c, "max_used"))
		if rt != nil {
			p.printf("    queries %.1f/s (reads %.1f, writes %.1f), slow %.2f/s, refused %.2f/s, deadlocks %.2f/s, row lock waits %.2f/s\n",
				f(rt, "queries"), f(rt, "selects"), f(rt, "writes"), f(rt, "slow"), f(rt, "refused_max_connections"), f(rt, "deadlocks"), f(rt, "row_lock_waits"))
			p.printf("    cache hit %.2f%%, in %s/s, out %s/s\n", f(rt, "buffer_pool_hit_pct"), sizeOf(f(rt, "bytes_in")), sizeOf(f(rt, "bytes_out")))
		}
		p.printf("    buffer pool %s, %.1f%% used\n", sizeOf(f(in, "buffer_pool_bytes")), f(in, "buffer_pool_used_pct"))
		switch {
		case rp == nil || rp["known"] != true:
			p.printf("    replication: unknown (the user lacks REPLICATION CLIENT)\n")
		case rp["is_replica"] != true:
			p.printf("    replication: not a replica\n")
		default:
			p.printf("    replication: io %v, sql %v, behind %v s %s\n", rp["io_running"], rp["sql_running"], rp["lag_sec"], str(rp, "last_error"))
		}
		if dbs, ok := r["databases"].([]any); ok {
			for _, x := range dbs {
				d, _ := x.(map[string]any)
				p.printf("    %-7s %-24s %-5v %6.2f ms  %s  %.0f tables\n", str(d, "use"), str(d, "name"), d["ok"], f(d, "ping_ms"), sizeOf(f(d, "size_bytes")), f(d, "tables"))
			}
		}
	} else {
		m, rt, ps, rp := sub("memory"), sub("rates"), sub("persistence"), sub("replication")
		p.printf("    version %s, %s, up %s, %s\n", str(r, "version"), str(r, "role"), monitor.Ago(time.Duration(f(r, "uptime_sec"))*time.Second), str(r, "addr"))
		limit := "no maxmemory"
		if f(m, "max") > 0 {
			limit = "of " + sizeOf(f(m, "max"))
		}
		p.printf("    memory %s %s (%.1f%%), policy %s, fragmentation %.2f\n", sizeOf(f(m, "used")), limit, f(m, "pct"), str(m, "policy"), f(m, "fragmentation"))
		p.printf("    commands %.0f/s, clients %.0f (%.0f blocked), keys %.0f\n", f(r, "ops_per_sec"), f(r, "clients"), f(r, "blocked_clients"), f(r, "keys"))
		if rt != nil {
			p.printf("    cache hit %.1f%%, evicted %.2f/s, expired %.2f/s, CPU %.1f%%, net in %.1f / out %.1f KB/s\n",
				f(rt, "hit_pct"), f(rt, "evicted"), f(rt, "expired"), f(rt, "cpu_pct"), f(rt, "net_in_kbps"), f(rt, "net_out_kbps"))
		}
		p.printf("    saving: AOF %v, last write ok %v, last snapshot ok %v, %0.f changes since the last save\n",
			ps["aof"], ps["aof_ok"], ps["rdb_ok"], f(ps, "changes_since_save"))
		p.printf("    replicas %.0f, the furthest %.0f s behind %s\n", f(rp, "replicas"), f(rp, "max_lag_sec"), str(rp, "master_link"))
		if sn := sub("sentinel"); sn != nil {
			p.printf("    Sentinels: master %s (%s), %.0f of %.0f answer, quorum %.0f\n", str(sn, "master"), str(sn, "flags"), f(sn, "reachable"), f(sn, "of"), f(sn, "quorum"))
		}
	}
	hot, err := p.hotDB()
	if err != nil {
		p.refusal(err)
		return
	}
	to := time.Now().UTC()
	if !p.mon.at.IsZero() {
		to = p.mon.at.Add(30 * time.Minute)
	}
	pts, bucket, err := monitor.StoreSeries(ctx, hot, s.Key, to.Add(-time.Hour), to, p.mon.deployment, max(p.mon.interval, 5), 60)
	if err != nil {
		p.refusal(err)
		return
	}
	used := "connections %"
	if s.Kind == "redis" {
		used = "memory %"
	}
	p.printf("  the hour to %s UTC, a point every %ds:\n", to.Format("15:04"), bucket)
	for _, x := range []struct{ key, label string }{{"ping_ms", "round trip"}, {"used_pct", used}, {"per_sec", "per second"}} {
		vals, last := seriesOf(pts, x.key)
		p.printf("    %-14s %s  %s\n", x.label, spark(vals), trimFloat(last))
	}
	down := 0
	for _, pt := range pts {
		if v, ok := pt.Values["ok"]; ok && v == 0 {
			down++
		}
	}
	if down > 0 {
		p.errf("    down in %d of %d points\n", down, len(pts))
	}
}

// ── small helpers ───────────────────────────────────────────────────────────

const sparkBars = "▁▂▃▄▅▆▇█"

// spark draws values as one line of bars; a gap ( ) where nothing was
// reported.
func spark(vals []float64) string {
	if len(vals) == 0 {
		return "(no reports)"
	}
	hi := 0.0
	for _, v := range vals {
		if !math.IsNaN(v) && v > hi {
			hi = v
		}
	}
	bars := []rune(sparkBars)
	var b strings.Builder
	for _, v := range vals {
		switch {
		case math.IsNaN(v):
			b.WriteRune(' ')
		case hi == 0:
			b.WriteRune(bars[0])
		default:
			b.WriteRune(bars[int(math.Round(v/hi*float64(len(bars)-1)))])
		}
	}
	return b.String()
}

// seriesOf picks one value from each point (NaN where it was not reported)
// and the last one reported.
func seriesOf(pts []monitor.SeriesPoint, key string) ([]float64, float64) {
	out := make([]float64, len(pts))
	last := math.NaN()
	for i, pt := range pts {
		v, ok := pt.Values[key]
		if !ok {
			out[i] = math.NaN()
			continue
		}
		out[i], last = v, v
	}
	return out, last
}

func trimFloat(v float64) string {
	if math.IsNaN(v) {
		return "—"
	}
	if v == math.Trunc(v) {
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
	return strconv.FormatFloat(v, 'f', 2, 64)
}

func clock(rfc string) string {
	if t, err := time.Parse(time.RFC3339Nano, rfc); err == nil {
		return t.UTC().Format("15:04:05")
	}
	return rfc
}

func comma(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

func sizeOf(b float64) string {
	if b <= 0 {
		return "—"
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	i := 0
	for b >= 1024 && i < len(units)-1 {
		b /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%.0f B", b)
	}
	return fmt.Sprintf("%.1f %s", b, units[i])
}

// liveEvery is how often Live asks the gateway.
const liveEvery = 5 * time.Second

// nextGateway moves the view to the next gateway: all, then each one.
func (p *Process) nextGateway() {
	v := &p.mon
	if len(v.known) == 0 {
		return
	}
	if v.deployment == "" {
		v.deployment = v.known[0]
		return
	}
	for i, d := range v.known {
		if d == v.deployment {
			if i+1 < len(v.known) {
				v.deployment = v.known[i+1]
			} else {
				v.deployment = ""
			}
			return
		}
	}
	v.deployment = ""
}
