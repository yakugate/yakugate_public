package monitor

// Monitoring events (Saleem 2026-10-05, D-126).
//
// "What happened that day" is the first question after an incident, and the
// 5-second reports answer it only while they are kept (3 days by default). The
// events kept in monitor_events say it for longer, and each one carries the
// whole deployment's reports at the interval before, at and after it, so the
// moment can still be opened once its detail rows are gone.
//
// Events are FOUND, not raised: the roles keep reporting exactly as before,
// and this reads their reports back once a minute, compares each interval with
// the ones before it and writes what changed. A role has nothing to call and
// nothing to remember, and a condition is judged on what the whole deployment
// reported, not on one process's view of it.
//
// Two shapes:
//   - a CONDITION (queue stuck, latency high, a vendor connection down…)
//     writes a `start` row when it has held for Hold intervals and an `end`
//     row when it has been gone for Hold intervals, so one blip is not an
//     event and a flapping value is not a hundred;
//   - a ONE-OFF (a role restarted, a role started) writes `once`.
//
// Deterministic by construction: an event's time is the time of the report
// that showed it, so a second pass over the same reports finds the same rows
// and the unique key turns them away. The scan therefore overlaps freely.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// EventRules decide what counts as an event (YAKU_MONITOR_* settings).
type EventRules struct {
	Off           bool    // YAKU_MONITOR_EVENTS=off
	QueueAgeSec   int64   // oldest waiting message older than this: queue_stuck
	QueueDepth    int64   // more waiting than this: queue_deep
	LatencyMS     float64 // ingress p95 over this: latency_high
	RejectPct     float64 // refused share of incoming over this: rejects_high
	RejectMinTPS  float64 // …judged only at this many messages a second or more
	CPUPct        float64 // host CPU over this: cpu_high
	MemPct        float64 // host memory over this: memory_high
	TrafficMinTPS float64 // traffic_stopped only after the previous 10 minutes averaged this much
	SpikeFactor   float64 // incoming over this many times the previous 10 minutes: traffic_spike
	Hold          int     // intervals a condition must hold (and be gone) before it starts (ends)
}

// DefaultEventRules are the settings' defaults.
func DefaultEventRules() EventRules {
	return EventRules{QueueAgeSec: 300, QueueDepth: 10000, LatencyMS: 50, RejectPct: 20, RejectMinTPS: 1,
		CPUPct: 90, MemPct: 90, TrafficMinTPS: 1, SpikeFactor: 3, Hold: 3}
}

// Event is one row of monitor_events (without its context).
type Event struct {
	Deployment  string
	At          time.Time
	Kind        string
	Phase       string // start | end | once
	Severity    string // critical | warning | info
	Subject     string
	Detail      string
	IntervalSec int
}

// sampleRow is the part of one monitor_samples row the detection reads: the
// lifted columns only, never the body.
type sampleRow struct {
	Deployment, Role, Instance string
	At                         time.Time
	Accepted, Rejected         int64
	TPS, TPSOut, TPSRejected   float64
	Latency, CPU, Mem          float64
	QueueDepth, QueueAge       int64
	Up, Total                  int
	Unhealthy                  bool
	Push                       int
	Queues                     *queueDoc             // per-queue figures (hot/0055); nil = none
	Stores                     map[string]storePoint // per-store figures (hot/0056); nil = none
}

// bucket is one interval of one deployment: every reporting instance's latest
// report in it, added up.
type bucket struct {
	At                 time.Time
	TPSIn, TPSRejected float64
	QueueDepth         int64
	QueueAge           int64
	Latency            float64
	CPU, Mem           float64
	VendorUp, VendorOf int
	VendorDegraded     bool
	Instances          int
	baseline           float64 // average incoming over the 10 minutes before
	// Per queue (D-127), keyed kind+"\x00"+name: the worst depth and age any
	// instance reported, rates added up. QueueKinds: the kinds whose depths
	// some instance reported in this interval — a queue not listed then is
	// empty, while a kind nobody reported is unknown.
	Queues     map[string]queuePoint
	QueueKinds map[string]bool
	// Per store (D-129): the worst any instance reported — reachable only
	// when all reached it. A store missing from an interval was not checked
	// then (unknown, not healthy).
	Stores map[string]storePoint
	// SMPP binds per account, added up over the deployment's smpp-servers
	// (2026-10-06). BindsKnown: some smpp-server reported in this interval —
	// only then does a missing account mean "no bind".
	Binds      map[string]int
	BindsKnown bool
	// Status-code rules that matched, added up over the deployment's roles:
	// how often in the last minute, by vendorCodeKey (2026-10-12).
	Codes map[string]int64
	// Each SMPP vendor's connections, [bound, of], added up over the
	// deployment's smpp-vendors (2026-10-18). VSKnown: one reported them.
	VS      map[string][2]int
	VSKnown bool
	// Each HTTP vendor, over the deployment's http-vendors: a failed call in
	// the last minute anywhere, waiting for its session anywhere, the calls
	// accepted added up — and HVFail, worked out against the interval before
	// (buckets): failed calls and none accepted since then.
	HV     map[string]hvPoint
	HVFail map[string]bool
	// The gateway-wide jobs' troubles (JB) and which jobs reported at all.
	JB      map[string]int64
	JBKnown map[string]bool
	// The record queue as the roles see it: the most waiting any reported,
	// and whether one said no db-worker runs. RQKnown: one reported it.
	RQW        int64
	RQNoWorker bool
	RQKnown    bool
}

// eventLookback is how much history before a scan window a pass reads: the
// traffic baseline needs 10 minutes and a condition needs Hold intervals.
const eventLookback = 11 * time.Minute

// restartWindow: a role whose old process stopped and a new one started
// within this is a restart, not a stop and a start.
const restartWindow = 2 * time.Minute

// vendorRoles are the roles whose sessions the vendor_down event ("Vendor
// connection down") is read from. The SMPP vendor role only: an HTTP vendor
// has no connection, so nothing about it is a connection down (Saleem,
// 2026-10-12). Until then an HTTP vendor that was idle, or had one failed
// call in the last minute, raised "vendor connections up 0 of 3".
var vendorRoles = map[string]bool{"smpp-vendor": true}

// detectEvents finds every event whose time falls in (from, to] in rows,
// which must cover [from-eventLookback, now]. Pure: no database, no clock.
func detectEvents(rows []sampleRow, from, to, now time.Time, r EventRules) []Event {
	byDep := map[string][]sampleRow{}
	for _, x := range rows {
		byDep[x.Deployment] = append(byDep[x.Deployment], x)
	}
	deps := make([]string, 0, len(byDep))
	for d := range byDep {
		deps = append(deps, d)
	}
	sort.Strings(deps)
	wide := wideLives(rows, now)
	var out []Event
	for _, d := range deps {
		out = append(out, detectDeployment(d, byDep[d], from, to, now, r, wide)...)
	}
	return out
}

// gatewayWideRoles run in ONE deployment at a time (internal/singleton's
// list; TestGatewayWideRolesMatchTheLease keeps the two together). Such a
// role moves between deployments — the lease holder goes away, the role
// goes back to its home — and a move is not a role stopping.
var gatewayWideRoles = map[string]bool{"abuse-monitor": true, "backup": true, "retention": true}

// wideLife is one process of a gateway-wide role, in any deployment.
type wideLife struct {
	dep         string
	first, last time.Time
	open        bool
}

// wideLives lists every process of each gateway-wide role across all the
// deployments the rows cover.
func wideLives(rows []sampleRow, now time.Time) map[string][]wideLife {
	push := time.Duration(maxInt(modePush(rows), 1)) * time.Second
	type key struct{ dep, role, inst string }
	byKey := map[key]*wideLife{}
	for _, x := range rows {
		if !gatewayWideRoles[x.Role] {
			continue
		}
		k := key{x.Deployment, x.Role, x.Instance}
		l := byKey[k]
		if l == nil {
			l = &wideLife{dep: x.Deployment, first: x.At, last: x.At}
			byKey[k] = l
		}
		if x.At.Before(l.first) {
			l.first = x.At
		}
		if x.At.After(l.last) {
			l.last = x.At
		}
	}
	out := map[string][]wideLife{}
	for k, l := range byKey {
		l.open = now.Sub(l.last) <= 3*push
		out[k.role] = append(out[k.role], *l)
	}
	return out
}

// runsElsewhere names another deployment that ran the gateway-wide role when
// this deployment's process stopped at `stopped` — running then, or started
// within restartWindow after. "" when none did: the role really stopped.
func runsElsewhere(lives []wideLife, dep string, stopped time.Time, p time.Duration) string {
	for _, y := range lives {
		if y.dep != dep && !y.first.After(stopped.Add(restartWindow)) && (y.open || !y.last.Before(stopped.Add(-p))) {
			return y.dep
		}
	}
	return ""
}

// depName is a deployment in words ("" is the unnamed one).
func depName(dep string) string {
	if dep == "" {
		return "the unnamed deployment"
	}
	return dep
}

// backElsewhere is the first process of the gateway-wide role that started in
// another deployment after `stopped`: where and when the role came back.
func backElsewhere(lives []wideLife, dep string, stopped time.Time) (string, time.Time, bool) {
	var best wideLife
	found := false
	for _, y := range lives {
		if y.dep != dep && y.first.After(stopped) && (!found || y.first.Before(best.first)) {
			best, found = y, true
		}
	}
	return best.dep, best.first, found
}

func detectDeployment(dep string, rows []sampleRow, from, to, now time.Time, r EventRules, wide map[string][]wideLife) []Event {
	sort.Slice(rows, func(i, j int) bool { return rows[i].At.Before(rows[j].At) })
	push := modePush(rows)
	p := time.Duration(push) * time.Second
	hold := r.Hold
	if hold < 1 {
		hold = 1
	}
	var out []Event
	emit := func(e Event) {
		if e.At.After(from) && !e.At.After(to) {
			e.Deployment, e.IntervalSec = dep, push
			out = append(out, e)
		}
	}

	series := buckets(rows, push)
	dataStart := rows[0].At

	// The whole deployment silent: no report from any role for 3 intervals.
	type gap struct{ start, end time.Time } // end zero = still silent
	var gaps []gap
	for i := 0; i < len(series); i++ {
		var next time.Time
		if i+1 < len(series) {
			next = series[i+1].At
		}
		last := series[i].At
		switch {
		case !next.IsZero() && next.Sub(last) > 3*p:
			gaps = append(gaps, gap{last, next})
		case next.IsZero() && now.Sub(last) > 3*p:
			gaps = append(gaps, gap{start: last})
		}
	}
	inGap := func(t time.Time) bool { // a role stop or start explained by a whole-deployment silence
		for _, g := range gaps {
			if !t.Before(g.start.Add(-2*p)) && !t.After(g.start.Add(p)) {
				return true
			}
			if !g.end.IsZero() && !t.Before(g.end.Add(-p)) && !t.After(g.end.Add(2*p)) {
				return true
			}
		}
		return false
	}
	for _, g := range gaps {
		emit(Event{At: g.start, Kind: "no_reports", Phase: "start", Severity: "critical", Subject: dep,
			Detail: "no role of this deployment is reporting"})
		if !g.end.IsZero() {
			emit(Event{At: g.end, Kind: "no_reports", Phase: "end", Severity: "info", Subject: dep,
				Detail: "reporting again after " + Ago(g.end.Sub(g.start))})
		}
	}

	// Roles: each process (role + instance) from its first report to its last.
	type life struct {
		role, inst  string
		first, last time.Time
		open        bool // still reporting
	}
	lives := map[string]*life{}
	for _, x := range rows {
		k := x.Role + "|" + x.Instance
		l := lives[k]
		if l == nil {
			l = &life{role: x.Role, inst: x.Instance, first: x.At}
			lives[k] = l
		}
		l.last = x.At
	}
	byRole := map[string][]*life{}
	for _, l := range lives {
		l.open = now.Sub(l.last) <= 3*time.Duration(maxInt(push, 1))*time.Second
		byRole[l.role] = append(byRole[l.role], l)
	}
	roles := make([]string, 0, len(byRole))
	for role := range byRole {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	// Role events are gathered from ALL the data read, then restarts that
	// happened together are folded, and only then is the window applied — so
	// a gateway restart straddling a pass's window is one event in every pass.
	var lifeEvents []Event
	lifeEmit := func(e Event) { e.Deployment, e.IntervalSec = dep, push; lifeEvents = append(lifeEvents, e) }
	for _, role := range roles {
		ls := byRole[role]
		sort.Slice(ls, func(i, j int) bool { return ls[i].first.Before(ls[j].first) })
		explained := map[*life]bool{} // a start already told as a restart or a return
		for _, x := range ls {
			if x.open || inGap(x.last) {
				continue
			}
			var succ *life
			for _, y := range ls {
				if y != x && !y.first.Before(x.last.Add(-p)) && !y.first.After(x.last.Add(restartWindow)) {
					succ = y
					break
				}
			}
			if succ != nil {
				explained[succ] = true
				lifeEmit(Event{At: succ.first, Kind: "role_restarted", Phase: "once", Severity: "warning", Subject: role,
					Detail: fmt.Sprintf("%s restarted: %s stopped reporting, %s took over %s later",
						role, x.inst, succ.inst, Ago(succ.first.Sub(x.last)))})
				continue
			}
			// A gateway-wide role that went on running in another deployment
			// MOVED (2026-10-18, go-live P8, P9 G1, P10 E4): the copy that
			// handed the lease back to its home, or found it held, stopped
			// reporting here, and was told as a critical "Role stopped" that
			// never ended — nothing here starts it again.
			if gatewayWideRoles[role] && runsElsewhere(wide[role], dep, x.last, p) != "" {
				continue
			}
			others := 0
			for _, y := range ls {
				if y != x && !y.first.After(x.last) && (y.open || y.last.After(x.last.Add(3*p))) {
					others++
				}
			}
			sev, detail := "critical", fmt.Sprintf("%s (%s) stopped reporting", role, x.inst)
			if others > 0 {
				sev = "warning"
				detail += fmt.Sprintf("; %d other process(es) of it still running", others)
			}
			lifeEmit(Event{At: x.last, Kind: "role_down", Phase: "start", Severity: sev, Subject: role, Detail: detail})
			ended := false
			for _, y := range ls {
				if y.first.After(x.last.Add(restartWindow)) {
					explained[y] = true
					ended = true
					lifeEmit(Event{At: y.first, Kind: "role_down", Phase: "end", Severity: "info", Subject: role,
						Detail: fmt.Sprintf("%s back (%s) after %s", role, y.inst, Ago(y.first.Sub(x.last)))})
					break
				}
			}
			// A gateway-wide role that comes back in ANOTHER deployment is back
			// too: the end is told where the stop was, so the two pair up.
			if !ended && gatewayWideRoles[role] {
				if where, at, ok := backElsewhere(wide[role], dep, x.last); ok {
					lifeEmit(Event{At: at, Kind: "role_down", Phase: "end", Severity: "info", Subject: role,
						Detail: fmt.Sprintf("%s back on %s after %s", role, depName(where), Ago(at.Sub(x.last)))})
				}
			}
		}
		for _, y := range ls {
			// A process whose first report is the start of the data may have
			// started long before; only a first report well inside it is a start.
			if explained[y] || !y.first.After(dataStart.Add(2*p)) || inGap(y.first) {
				continue
			}
			lifeEmit(Event{At: y.first, Kind: "role_started", Phase: "once", Severity: "info", Subject: role,
				Detail: fmt.Sprintf("%s started (%s)", role, y.inst)})
		}
	}

	// Most of the deployment's roles restarted within a minute: one gateway
	// restart, told once rather than once per role.
	for _, e := range foldRestarts(lifeEvents, len(byRole), dep) {
		emit(e)
	}

	// Conditions over the deployment's intervals.
	conds := []condition{
		{kind: "vendor_down", severity: "warning",
			on: func(b bucket) (bool, bool) {
				return b.VendorOf > 0 && (b.VendorUp < b.VendorOf || b.VendorDegraded), b.VendorOf > 0
			},
			value: func(b bucket) float64 { return float64(b.VendorOf - b.VendorUp) },
			startText: func(b bucket) string {
				return fmt.Sprintf("vendor connections up %d of %d", b.VendorUp, b.VendorOf)
			},
			endText: func(w float64, d time.Duration) string {
				return fmt.Sprintf("every vendor connection up again after %s (worst: %.0f down)", Ago(d), w)
			}},
		{kind: "queue_stuck", severity: "warning",
			on:    func(b bucket) (bool, bool) { return b.QueueDepth > 0 && b.QueueAge > r.QueueAgeSec, true },
			value: func(b bucket) float64 { return float64(b.QueueAge) },
			startText: func(b bucket) string {
				return fmt.Sprintf("the oldest waiting message has waited %s (%d waiting)", Ago(time.Duration(b.QueueAge)*time.Second), b.QueueDepth)
			},
			endText: func(w float64, d time.Duration) string {
				return fmt.Sprintf("queues moving again after %s (oldest wait reached %s)", Ago(d), Ago(time.Duration(w)*time.Second))
			}},
		{kind: "queue_deep", severity: "warning",
			on:    func(b bucket) (bool, bool) { return b.QueueDepth > r.QueueDepth, true },
			value: func(b bucket) float64 { return float64(b.QueueDepth) },
			startText: func(b bucket) string {
				return fmt.Sprintf("%d messages waiting (limit %d)", b.QueueDepth, r.QueueDepth)
			},
			endText: func(w float64, d time.Duration) string {
				return fmt.Sprintf("queue back under %d after %s (peak %.0f)", r.QueueDepth, Ago(d), w)
			}},
		{kind: "latency_high", severity: "warning",
			// 0 = not measured (no traffic): judged as under the limit.
			on:    func(b bucket) (bool, bool) { return b.Latency > r.LatencyMS, true },
			value: func(b bucket) float64 { return b.Latency },
			startText: func(b bucket) string {
				return fmt.Sprintf("acceptance p95 %.0f ms (limit %.0f ms)", b.Latency, r.LatencyMS)
			},
			endText: func(w float64, d time.Duration) string {
				return fmt.Sprintf("acceptance p95 back under %.0f ms after %s (worst %.0f ms)", r.LatencyMS, Ago(d), w)
			}},
		{kind: "rejects_high", severity: "warning",
			on: func(b bucket) (bool, bool) {
				all := b.TPSIn + b.TPSRejected
				if all < r.RejectMinTPS {
					return false, true
				}
				return b.TPSRejected/all*100 > r.RejectPct, true
			},
			value: func(b bucket) float64 {
				if all := b.TPSIn + b.TPSRejected; all > 0 {
					return b.TPSRejected / all * 100
				}
				return 0
			},
			startText: func(b bucket) string {
				all := b.TPSIn + b.TPSRejected
				return fmt.Sprintf("%.0f%% of incoming messages refused (%.1f of %.1f a second)", b.TPSRejected/all*100, b.TPSRejected, all)
			},
			endText: func(w float64, d time.Duration) string {
				return fmt.Sprintf("refusals back under %.0f%% after %s (worst %.0f%%)", r.RejectPct, Ago(d), w)
			}},
		{kind: "cpu_high", severity: "warning",
			on:        func(b bucket) (bool, bool) { return b.CPU > r.CPUPct, true },
			value:     func(b bucket) float64 { return b.CPU },
			startText: func(b bucket) string { return fmt.Sprintf("host CPU %.0f%%", b.CPU) },
			endText: func(w float64, d time.Duration) string {
				return fmt.Sprintf("host CPU back under %.0f%% after %s (peak %.0f%%)", r.CPUPct, Ago(d), w)
			}},
		{kind: "memory_high", severity: "warning",
			on:        func(b bucket) (bool, bool) { return b.Mem > r.MemPct, true },
			value:     func(b bucket) float64 { return b.Mem },
			startText: func(b bucket) string { return fmt.Sprintf("host memory %.0f%% used", b.Mem) },
			endText: func(w float64, d time.Duration) string {
				return fmt.Sprintf("host memory back under %.0f%% after %s (peak %.0f%%)", r.MemPct, Ago(d), w)
			}},
		{kind: "traffic_stopped", severity: "warning",
			on: func(b bucket) (bool, bool) {
				return b.TPSIn == 0 && b.baseline >= r.TrafficMinTPS, true
			},
			value: func(b bucket) float64 { return b.baseline },
			startText: func(b bucket) string {
				return fmt.Sprintf("no incoming messages (the 10 minutes before averaged %.1f a second)", b.baseline)
			},
			endText: func(w float64, d time.Duration) string {
				return fmt.Sprintf("incoming messages again after %s", Ago(d))
			}},
		{kind: "traffic_spike", severity: "info",
			on: func(b bucket) (bool, bool) {
				return b.TPSIn >= r.TrafficMinTPS*r.SpikeFactor && b.TPSIn > b.baseline*r.SpikeFactor, b.baseline > 0
			},
			value: func(b bucket) float64 { return b.TPSIn },
			startText: func(b bucket) string {
				return fmt.Sprintf("incoming %.1f a second, over %.0f× the 10 minutes before (%.1f)", b.TPSIn, r.SpikeFactor, b.baseline)
			},
			endText: func(w float64, d time.Duration) string {
				return fmt.Sprintf("incoming back to its usual level after %s (peak %.1f a second)", Ago(d), w)
			}},
	}
	// Queues one by one (D-127) once the samples carry them; the whole
	// deployment's queue conditions above are then left out, since every
	// one of them is told with its queue's name.
	perQueue, perVendor := false, false
	for _, x := range rows {
		if x.Queues != nil && x.Queues.reports("mt") {
			perQueue = true
		}
		if x.Queues != nil && x.Queues.reports(vendorsKind) {
			perVendor = true
		}
	}
	for _, c := range conds {
		if perQueue && (c.kind == "queue_stuck" || c.kind == "queue_deep") {
			continue
		}
		// Each SMPP vendor's connections are told one vendor at a time once
		// the samples carry them (vendorConditions), so the event names it.
		if perVendor && c.kind == "vendor_down" {
			continue
		}
		runCondition(series, c, dep, hold, emit)
	}
	holdOf := func(c condition) int {
		if n := int(c.holdFor / p); n > hold {
			return n
		}
		return hold
	}
	for _, c := range vendorConditions(series) {
		runCondition(series, c, c.subject, holdOf(c), emit)
	}
	for _, c := range jobConditions(series) {
		runCondition(series, c, c.subject, holdOf(c), emit)
	}
	for _, c := range queueConditions(series, r) {
		runCondition(series, c, c.subject, holdOf(c), emit)
	}
	// An account that was bound over SMPP and has no bind left (2026-10-06).
	for _, c := range bindConditions(series) {
		runCondition(series, c, c.subject, hold, emit)
	}
	// A vendor answering with a code it has a Status Codes rule for (2026-10-12).
	for _, c := range codeConditions(series) {
		runCondition(series, c, c.subject, hold, emit)
	}
	// MySQL and Redis (D-129): down, a warning, restarted.
	for _, c := range storeConditions(series) {
		runCondition(series, c, c.subject, hold, emit)
	}
	for _, e := range storeRestarts(series, push) {
		emit(e)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}

// condition is one thing that can be true or not in each interval.
type condition struct {
	kind, severity string
	subject        string // queue conditions only; the deployment's otherwise
	// holdFor: how long it must hold (and be gone) before it starts (ends),
	// when that is longer than the rules' Hold intervals.
	holdFor   time.Duration
	on        func(b bucket) (on, known bool)
	value     func(b bucket) float64 // what the details report (worst during the run)
	startText func(b bucket) string
	endText   func(worst float64, lasted time.Duration) string
}

// runCondition tells when c started and ended in series: after it has held
// (been gone) for `hold` intervals. The state at the start of the data is
// taken as given and never told.
func runCondition(series []bucket, c condition, subject string, hold int, emit func(Event)) {
	state := false
	decided := false
	run, runStart := 0, -1
	var worst float64
	var startedAt time.Time
	for i, b := range series {
		v, known := c.on(b)
		if !known {
			continue
		}
		if !decided {
			state, decided = v, true
			if state {
				startedAt, worst = b.At, c.value(b)
			}
			continue
		}
		if state {
			if w := c.value(b); w > worst {
				worst = w
			}
		}
		if v == state {
			run, runStart = 0, -1
			continue
		}
		if run == 0 {
			runStart = i
		}
		run++
		if run < hold {
			continue
		}
		at := series[runStart].At
		if !state {
			startedAt, worst = at, c.value(series[runStart])
			for _, x := range series[runStart : i+1] {
				if w := c.value(x); w > worst {
					worst = w
				}
			}
			emit(Event{At: at, Kind: c.kind, Phase: "start", Severity: c.severity, Subject: subject, Detail: c.startText(series[runStart])})
		} else {
			emit(Event{At: at, Kind: c.kind, Phase: "end", Severity: "info", Subject: subject, Detail: c.endText(worst, at.Sub(startedAt))})
		}
		state = !state
		run, runStart = 0, -1
	}
}

// storeConditions are "down" and "a warning" for every store the series
// shows (subject = the store's key). A warning is judged only while the store
// answers, so a store that goes down and comes back warning-free is one down
// event, not a warning too.
func storeConditions(series []bucket) []condition {
	seen := map[string]bool{}
	var keys []string
	for _, b := range series {
		for k := range b.Stores {
			if !seen[k] {
				seen[k] = true
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	var out []condition
	for _, key := range keys {
		point := func(b bucket) (storePoint, bool) { p, ok := b.Stores[key]; return p, ok }
		label := func(b bucket) string { p, _ := point(b); return storeLabel(key, p) }
		lastLabel := key
		for _, b := range series {
			if p, ok := b.Stores[key]; ok {
				lastLabel = storeLabel(key, p)
			}
		}
		out = append(out,
			condition{kind: "store_down", severity: "critical", subject: key,
				on: func(b bucket) (bool, bool) {
					p, ok := point(b)
					return p.W == 2, ok
				},
				value: func(b bucket) float64 { return 0 },
				startText: func(b bucket) string {
					p, _ := point(b)
					return label(b) + " is down" + suffixColon(p.X)
				},
				endText: func(_ float64, d time.Duration) string {
					return fmt.Sprintf("%s answering again after %s", lastLabel, Ago(d))
				}},
			condition{kind: "store_warning", severity: "warning", subject: key,
				on: func(b bucket) (bool, bool) {
					p, ok := point(b)
					return p.W == 1, ok && p.W != 2
				},
				value: func(b bucket) float64 { return 0 },
				startText: func(b bucket) string {
					p, _ := point(b)
					return label(b) + suffixColon(p.X)
				},
				endText: func(_ float64, d time.Duration) string {
					return fmt.Sprintf("%s healthy again after %s", lastLabel, Ago(d))
				}})
	}
	return out
}

// storeRestarts tells each time a store's uptime went back: the server
// restarted between two reports.
func storeRestarts(series []bucket, push int) []Event {
	last := map[string]storePoint{}
	var out []Event
	for _, b := range series {
		keys := make([]string, 0, len(b.Stores))
		for k := range b.Stores {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			p := b.Stores[k]
			if p.OK != 1 || p.U <= 0 {
				continue
			}
			if prev, ok := last[k]; ok && p.U+int64(2*push) < prev.U {
				out = append(out, Event{At: b.At, Kind: "store_restarted", Phase: "once", Severity: "warning", Subject: k,
					Detail: fmt.Sprintf("%s restarted (up %s, was up %s)", storeLabel(k, p),
						Ago(time.Duration(p.U)*time.Second), Ago(time.Duration(prev.U)*time.Second))})
			}
			last[k] = p
		}
	}
	return out
}

func suffixColon(s string) string {
	if s == "" {
		return ""
	}
	return ": " + s
}

// queueConditions are "stuck" and "too deep" for every queue the series
// shows: a vendor's queue (queue_stuck, queue_deep, subject = the queue) and
// a customer's receipts or MO (customer_queue_stuck / _deep, subject
// receipts:<account> or mo:<account>).
func queueConditions(series []bucket, r EventRules) []condition {
	seen := map[string]bool{}
	var keys []string
	for _, b := range series {
		for k := range b.Queues {
			if !seen[k] {
				seen[k] = true
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	var out []condition
	for _, key := range keys {
		kind, name, _ := strings.Cut(key, "\x00")
		point := func(b bucket) (queuePoint, bool) { return b.Queues[key], b.QueueKinds[kind] }
		label, subject, prefix := "queue "+name, name, ""
		switch kind {
		case "dlr":
			label, subject, prefix = "receipts for account "+name, "receipts:"+name, "customer_"
		case "mo":
			label, subject, prefix = "incoming messages for account "+name, "mo:"+name, "customer_"
		case "rec":
			// The DB worker's record lists (2026-10-18, go-live P14 G3: no
			// event for a list falling behind). Their depths come from a
			// db-worker's report, so they are judged only while one reports;
			// none reporting is recordsNoWorker's.
			label, subject, prefix = "records waiting for the DB worker (list "+name+")", "records:"+name, "records_"
		}
		if prefix == "customer_" {
			// "Customer not taking messages" (2026-10-18, go-live P5): judged
			// by what the customer was HANDED, not by the oldest wait. A
			// customer's receipts waiting out their back-off are not ready, so
			// with Redis queues the oldest READY wait read 0 and this never
			// started; with MySQL queues the oldest wait stayed old while the
			// queue drained, so it never ended. Now: something waits and
			// nothing was handed over in the last minute, for QueueAgeSec.
			out = append(out, condition{kind: "customer_queue_stuck", severity: "warning", subject: subject,
				holdFor: time.Duration(r.QueueAgeSec) * time.Second,
				on: func(b bucket) (bool, bool) {
					p, known := point(b)
					return p.W > 0 && p.O == 0, known
				},
				value: func(b bucket) float64 { p, _ := point(b); return float64(p.W) },
				startText: func(b bucket) string {
					p, _ := point(b)
					return fmt.Sprintf("%s: nothing handed over for %s (%d waiting; in %.1f/s)",
						label, Ago(time.Duration(r.QueueAgeSec)*time.Second), p.W, p.I)
				},
				endText: func(w float64, d time.Duration) string {
					return fmt.Sprintf("%s taking messages again after %s (at most %.0f waited)", label, Ago(d), w)
				}})
		} else {
			out = append(out, condition{kind: prefix + "queue_stuck", severity: "warning", subject: subject,
				on: func(b bucket) (bool, bool) {
					p, known := point(b)
					return p.W > 0 && p.A > r.QueueAgeSec, known
				},
				value: func(b bucket) float64 { p, _ := point(b); return float64(p.A) },
				startText: func(b bucket) string {
					p, _ := point(b)
					return fmt.Sprintf("%s: the oldest ready message has waited %s (%d waiting; in %.1f/s, out %.1f/s)",
						label, Ago(time.Duration(p.A)*time.Second), p.W, p.I, p.O)
				},
				endText: func(w float64, d time.Duration) string {
					return fmt.Sprintf("%s moving again after %s (oldest wait reached %s)", label, Ago(d), Ago(time.Duration(w)*time.Second))
				}})
		}
		out = append(out,
			condition{kind: prefix + "queue_deep", severity: "warning", subject: subject,
				on: func(b bucket) (bool, bool) {
					p, known := point(b)
					return p.W > r.QueueDepth, known
				},
				value: func(b bucket) float64 { p, _ := point(b); return float64(p.W) },
				startText: func(b bucket) string {
					p, _ := point(b)
					return fmt.Sprintf("%s: %d waiting (limit %d; in %.1f/s, out %.1f/s)", label, p.W, r.QueueDepth, p.I, p.O)
				},
				endText: func(w float64, d time.Duration) string {
					return fmt.Sprintf("%s back under %d after %s (peak %.0f)", label, r.QueueDepth, Ago(d), w)
				}})
	}
	return out
}

// codeConditions is "this vendor is answering with a code it has a Status
// Codes rule for" (vendor_code, subject = the vendor and the code), for every
// rule the series shows matching at some point.
//
// Go-live test P3 D5 (2026-10-12, Saleem: "custom error was captured but not
// thrown any alert in email or monitoring"). The rule did its work — retry,
// another route, the vendor paused — and nobody was told the vendor had begun
// answering with that code. The figure is "matches in the last minute", so
// one answer keeps the condition on for a minute: long enough to start it.
func codeConditions(series []bucket) []condition {
	seen := map[string]bool{}
	var keys []string
	for _, b := range series {
		for key, n := range b.Codes {
			if n > 0 && !seen[key] {
				seen[key] = true
				keys = append(keys, key)
			}
		}
	}
	sort.Strings(keys)
	out := make([]condition, 0, len(keys))
	for _, key := range keys {
		key := key
		parts := strings.SplitN(key, "|", 4)
		for len(parts) < 4 {
			parts = append(parts, "")
		}
		vendor, scope, code, action := parts[0], parts[1], parts[2], parts[3]
		where := "a send"
		if scope == "dlr" {
			where = "a receipt"
		}
		out = append(out, condition{kind: "vendor_code", severity: "warning", subject: vendor + " code " + code,
			on:    func(b bucket) (bool, bool) { return b.Codes[key] > 0, true },
			value: func(b bucket) float64 { return float64(b.Codes[key]) },
			startText: func(b bucket) string {
				return fmt.Sprintf("vendor %s answers %s with code %s, which its Status Codes turn into \"%s\": %d in the last minute",
					vendor, where, code, action, b.Codes[key])
			},
			endText: func(worst float64, d time.Duration) string {
				return fmt.Sprintf("vendor %s no longer answers with code %s after %s (at most %.0f a minute)", vendor, code, Ago(d), worst)
			}})
	}
	return out
}

// vendorFailingFor is how long an HTTP vendor's calls must fail, with none
// accepted, before it is told as failing — about as long as the gateway gives
// a vendor before its messages are moved (down_after_sec), so the event and
// the move come together.
const vendorFailingFor = 2 * time.Minute

// vendorConditions are each vendor's own (2026-10-18): an SMPP vendor with
// fewer connections up than it should have (vendor_down, subject = its cid —
// the event used to say "vendor connections up 4 of 6" for the deployment and
// never which vendor, go-live P3 G1), and an HTTP vendor whose calls fail and
// none is accepted (vendor_failing, go-live P4 G1: five minutes of failed
// calls raised nothing). An HTTP vendor is never "down" (Saleem, 2026-10-12):
// it is failing, or not ready while it waits for its session.
func vendorConditions(series []bucket) []condition {
	smpp, http := map[string]bool{}, map[string]bool{}
	var smppNames, httpNames []string
	for _, b := range series {
		for cid := range b.VS {
			if !smpp[cid] {
				smpp[cid] = true
				smppNames = append(smppNames, cid)
			}
		}
		for cid := range b.HV {
			if !http[cid] {
				http[cid] = true
				httpNames = append(httpNames, cid)
			}
		}
	}
	sort.Strings(smppNames)
	sort.Strings(httpNames)
	var out []condition
	for _, cid := range smppNames {
		cid := cid
		out = append(out, condition{kind: "vendor_down", severity: "warning", subject: cid,
			on: func(b bucket) (bool, bool) {
				v, ok := b.VS[cid]
				return ok && v[1] > 0 && v[0] < v[1], b.VSKnown
			},
			value: func(b bucket) float64 { v := b.VS[cid]; return float64(v[1] - v[0]) },
			startText: func(b bucket) string {
				v := b.VS[cid]
				return fmt.Sprintf("vendor %s: connections up %d of %d", cid, v[0], v[1])
			},
			endText: func(w float64, d time.Duration) string {
				return fmt.Sprintf("vendor %s: every connection up again after %s (worst: %.0f down)", cid, Ago(d), w)
			}})
	}
	for _, cid := range httpNames {
		cid := cid
		out = append(out, condition{kind: "vendor_failing", severity: "warning", subject: cid, holdFor: vendorFailingFor,
			on: func(b bucket) (bool, bool) {
				_, ok := b.HV[cid]
				return b.HVFail[cid], ok
			},
			value: func(b bucket) float64 { return 0 },
			startText: func(b bucket) string {
				return fmt.Sprintf("HTTP vendor %s is failing: its calls have failed and none was accepted for %s; once it has failed for its down_after_sec its messages go to the next vendor of their rule",
					cid, Ago(vendorFailingFor))
			},
			endText: func(_ float64, d time.Duration) string {
				return fmt.Sprintf("HTTP vendor %s accepts calls again, or is no longer failing, after %s", cid, Ago(d))
			}})
	}
	return out
}

// jobConditions are the gateway-wide jobs' troubles and the DB worker's
// (2026-10-18, go-live P8 G2, P9 G2, P14 G1/G3), from what the roles report —
// no SQL on any operational table. Each is judged only in intervals where its
// role reported.
func jobConditions(series []bucket) []condition {
	job := func(kind, severity, subject, section, key string, holdFor time.Duration,
		start func(n int64) string, end func(d time.Duration) string) condition {
		return condition{kind: kind, severity: severity, subject: subject, holdFor: holdFor,
			on:        func(b bucket) (bool, bool) { return b.JB[key] > 0, b.JBKnown[section] },
			value:     func(b bucket) float64 { return float64(b.JB[key]) },
			startText: func(b bucket) string { return start(b.JB[key]) },
			endText:   func(_ float64, d time.Duration) string { return end(d) }}
	}
	return []condition{
		job("billing_queue_stuck", "critical", "billing", billingQueueKind, jobBillingStuck, 0,
			func(n int64) string {
				if n <= 1 {
					return "money events wait on the billing queue: MySQL refused some of them (Monitoring → Roles → db-worker → billing_queue says which lane)"
				}
				return fmt.Sprintf("money events wait on the billing queue and are not booked: the oldest for %s (the db-worker cannot book them; usage and invoices lag)", Ago(time.Duration(n)*time.Second))
			},
			func(d time.Duration) string { return fmt.Sprintf("the billing queue is booked again after %s", Ago(d)) }),
		{kind: "billing_no_worker", severity: "critical", subject: "billing",
			// Events wait and no db-worker of the deployment reported: nobody
			// books them.
			on: func(b bucket) (bool, bool) {
				return b.JB[jobBillingWaits] > 0 && !b.JBKnown[billingQueueKind], b.JBKnown[billingEventsKind]
			},
			value: func(b bucket) float64 { return float64(b.JB[jobBillingWaits]) },
			startText: func(b bucket) string {
				return fmt.Sprintf("money events wait on the billing queue and no db-worker of this deployment is running to book them (the oldest for %s)",
					Ago(time.Duration(b.JB[jobBillingWaits])*time.Second))
			},
			endText: func(_ float64, d time.Duration) string {
				return fmt.Sprintf("the billing queue is booked again after %s", Ago(d))
			}},
		job("backup_failed", "warning", "backup", "backup", jobBackupBad, 0,
			func(int64) string {
				return "the last backup run is not a whole, verified backup: it left a table or a database out, or did not verify (Monitoring → Roles → backup; yakugate backup-verify)"
			},
			func(d time.Duration) string { return fmt.Sprintf("a whole, verified backup was made after %s", Ago(d)) }),
		job("archive_refused", "warning", "retention", "retention", jobArchiveRefused, 0,
			func(n int64) string {
				return fmt.Sprintf("retention could not move %d day table(s) to the cold store; they stay in hot and are tried again every sweep", n)
			},
			func(d time.Duration) string { return fmt.Sprintf("every due day table moved to cold after %s", Ago(d)) }),
		{kind: "records_no_worker", severity: "critical", subject: "records",
			on:    func(b bucket) (bool, bool) { return b.RQW > 0 && b.RQNoWorker, b.RQKnown },
			value: func(b bucket) float64 { return float64(b.RQW) },
			startText: func(b bucket) string {
				return fmt.Sprintf("%d record(s) wait for the DB worker and no db-worker is running: they are not being written to the database", b.RQW)
			},
			endText: func(w float64, d time.Duration) string {
				return fmt.Sprintf("records are written again after %s (at most %.0f waited)", Ago(d), w)
			}},
	}
}

// bindConditions is "this account has no SMPP bind left" for every account
// the series shows bound at some point (client_bind_lost, subject = the
// account's username). Judged only in intervals where an smpp-server
// reported: a server that is down is a role_down, not every client leaving.
func bindConditions(series []bucket) []condition {
	seen := map[string]bool{}
	var names []string
	for _, b := range series {
		for name, n := range b.Binds {
			if n > 0 && !seen[name] {
				seen[name] = true
				names = append(names, name)
			}
		}
	}
	sort.Strings(names)
	out := make([]condition, 0, len(names))
	for _, name := range names {
		name := name
		out = append(out, condition{kind: "client_bind_lost", severity: "warning", subject: name,
			on:    func(b bucket) (bool, bool) { return b.Binds[name] == 0, b.BindsKnown },
			value: func(b bucket) float64 { return 0 },
			startText: func(b bucket) string {
				return fmt.Sprintf("account %s has no SMPP bind left: its last connection closed", name)
			},
			endText: func(_ float64, d time.Duration) string {
				return fmt.Sprintf("account %s is bound again after %s", name, Ago(d))
			}})
	}
	return out
}

// foldRestarts replaces role restarts that happened together — at least 3
// roles and at least half of those the deployment runs, within a minute — by
// one gateway_restarted.
func foldRestarts(evs []Event, roles int, dep string) []Event {
	var restarts []int
	for i, e := range evs {
		if e.Kind == "role_restarted" {
			restarts = append(restarts, i)
		}
	}
	if len(restarts) < 3 {
		return evs
	}
	sort.Slice(restarts, func(a, b int) bool { return evs[restarts[a]].At.Before(evs[restarts[b]].At) })
	drop := map[int]bool{}
	var add []Event
	for i := 0; i < len(restarts); {
		j := i
		for j+1 < len(restarts) && evs[restarts[j+1]].At.Sub(evs[restarts[i]].At) <= time.Minute {
			j++
		}
		group := restarts[i : j+1]
		if len(group) >= 3 && len(group)*2 >= roles {
			names := make([]string, 0, len(group))
			for _, k := range group {
				drop[k] = true
				names = append(names, evs[k].Subject)
			}
			sort.Strings(names)
			first := evs[group[0]]
			add = append(add, Event{Deployment: first.Deployment, IntervalSec: first.IntervalSec, At: first.At,
				Kind: "gateway_restarted", Phase: "once", Severity: "warning", Subject: dep,
				Detail: fmt.Sprintf("%d roles restarted together: %s", len(group), strings.Join(names, ", "))})
		}
		i = j + 1
	}
	if len(drop) == 0 {
		return evs
	}
	kept := add
	for i, e := range evs {
		if !drop[i] {
			kept = append(kept, e)
		}
	}
	return kept
}

// buckets turns a deployment's reports into its intervals: each instance's
// latest report in an interval, rates added up, the worst of the rest.
func buckets(rows []sampleRow, push int) []bucket {
	if push < 1 {
		push = 1
	}
	type key struct {
		b    int64
		inst string
	}
	latest := map[key]sampleRow{}
	for _, x := range rows {
		k := key{x.At.Unix() / int64(push), x.Role + "|" + x.Instance}
		if cur, ok := latest[k]; !ok || x.At.After(cur.At) {
			latest[k] = x
		}
	}
	byB := map[int64]*bucket{}
	for k, x := range latest {
		bk := byB[k.b]
		if bk == nil {
			bk = &bucket{At: time.Unix(k.b*int64(push), 0).UTC()}
			byB[k.b] = bk
		}
		bk.Instances++
		bk.TPSIn += x.TPS
		bk.TPSRejected += x.TPSRejected
		if x.QueueDepth > bk.QueueDepth {
			bk.QueueDepth = x.QueueDepth
		}
		if x.QueueAge > bk.QueueAge {
			bk.QueueAge = x.QueueAge
		}
		if x.Latency > bk.Latency {
			bk.Latency = x.Latency
		}
		if x.CPU > bk.CPU {
			bk.CPU = x.CPU
		}
		if x.Mem > bk.Mem {
			bk.Mem = x.Mem
		}
		if vendorRoles[x.Role] {
			bk.VendorUp += x.Up
			bk.VendorOf += x.Total
			if x.Unhealthy {
				bk.VendorDegraded = true
			}
		}
		for key, p := range x.Stores {
			if bk.Stores == nil {
				bk.Stores = map[string]storePoint{}
			}
			cur, seen := bk.Stores[key]
			if !seen {
				bk.Stores[key] = p
				continue
			}
			if p.OK < cur.OK {
				cur.OK = p.OK
			}
			if p.W > cur.W {
				cur.W, cur.X = p.W, p.X
			}
			cur.P, cur.C, cur.N, cur.M = math.Max(cur.P, p.P), math.Max(cur.C, p.C), math.Max(cur.N, p.N), math.Max(cur.M, p.M)
			cur.CL, cur.Q, cur.S, cur.E = math.Max(cur.CL, p.CL), math.Max(cur.Q, p.Q), math.Max(cur.S, p.S), math.Max(cur.E, p.E)
			cur.L, cur.U = math.Max(cur.L, p.L), max64(cur.U, p.U)
			if p.H > 0 && (cur.H == 0 || p.H < cur.H) {
				cur.H = p.H
			}
			bk.Stores[key] = cur
		}
		if q := x.Queues; q != nil {
			if bk.Queues == nil {
				bk.Queues, bk.QueueKinds = map[string]queuePoint{}, map[string]bool{}
			}
			for _, k := range q.D {
				bk.QueueKinds[k] = true
				if k == bindsKind {
					bk.BindsKnown = true
				}
			}
			for name, n := range q.B {
				if bk.Binds == nil {
					bk.Binds = map[string]int{}
				}
				bk.Binds[name] += n
			}
			for key, n := range q.VC {
				if bk.Codes == nil {
					bk.Codes = map[string]int64{}
				}
				bk.Codes[key] += n
			}
			for _, k := range q.D {
				switch k {
				case vendorsKind:
					bk.VSKnown = true
				case recordQueueKind:
					bk.RQKnown = true
				case billingQueueKind, billingEventsKind, "backup", "retention":
					if bk.JBKnown == nil {
						bk.JBKnown = map[string]bool{}
					}
					bk.JBKnown[k] = true
				}
			}
			for cid, v := range q.VS {
				if bk.VS == nil {
					bk.VS = map[string][2]int{}
				}
				cur := bk.VS[cid]
				bk.VS[cid] = [2]int{cur[0] + v[0], cur[1] + v[1]}
			}
			for cid, p := range q.HV {
				if bk.HV == nil {
					bk.HV = map[string]hvPoint{}
				}
				cur := bk.HV[cid]
				cur.E, cur.NR = maxInt(cur.E, p.E), maxInt(cur.NR, p.NR)
				cur.A += p.A
				bk.HV[cid] = cur
			}
			for name, n := range q.JB {
				if bk.JB == nil {
					bk.JB = map[string]int64{}
				}
				bk.JB[name] = max64(bk.JB[name], n)
			}
			if q.RQ != nil {
				bk.RQW = max64(bk.RQW, q.RQ.W)
				if q.RQ.NW > 0 {
					bk.RQNoWorker = true
				}
			}
			for kind, m := range map[string]map[string]queuePoint{"mt": q.MT, "dlr": q.DLR, "mo": q.MO, "rec": q.REC} {
				for name, p := range m {
					key := kind + "\x00" + name
					cur := bk.Queues[key]
					cur.W, cur.R, cur.H, cur.T = max64(cur.W, p.W), max64(cur.R, p.R), max64(cur.H, p.H), max64(cur.T, p.T)
					cur.A = max64(cur.A, p.A)
					cur.I += p.I
					cur.O += p.O
					bk.Queues[key] = cur
				}
			}
		}
	}
	out := make([]bucket, 0, len(byB))
	for _, b := range byB {
		out = append(out, *b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	// An HTTP vendor is failing in an interval when it had a failed call in
	// the last minute and accepted none since the interval before (its
	// accepted count did not grow). One waiting for its session is not
	// failing: nothing is asked of it.
	prevA := map[string]int64{}
	for i := range out {
		for cid, p := range out[i].HV {
			before, seen := prevA[cid]
			if out[i].HVFail == nil {
				out[i].HVFail = map[string]bool{}
			}
			out[i].HVFail[cid] = p.E > 0 && p.NR == 0 && seen && p.A <= before
			prevA[cid] = p.A
		}
	}
	// The 10-minute baseline before each interval, for the traffic conditions.
	lo := 0
	var sum float64
	for i := range out {
		for lo < i && out[i].At.Sub(out[lo].At) > 10*time.Minute {
			sum -= out[lo].TPSIn
			lo++
		}
		if n := i - lo; n > 0 {
			out[i].baseline = sum / float64(n)
		}
		sum += out[i].TPSIn
	}
	return out
}

func modePush(rows []sampleRow) int {
	count := map[int]int{}
	best, bestN := int(MinPushInterval/time.Second), 0
	for _, x := range rows {
		if x.Push <= 0 {
			continue
		}
		count[x.Push]++
		if n := count[x.Push]; n > bestN || (n == bestN && x.Push < best) {
			best, bestN = x.Push, n
		}
	}
	return best
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// --- the pass that writes them ----------------------------------------------

const (
	eventsLock      = "yaku_monitor_events"
	eventsWatermark = "events_scanned_to"
	eventsMaxBehind = 6 * time.Hour
	eventsSettle    = time.Minute // reports younger than this are left for the next pass
)

// ScanEvents runs one event pass: from the last scanned time (or 30 minutes
// ago) to a minute ago. Safe to call from every process: one does the work
// (a named lock of the hot store's database, dal.MonitorHistory.Locked).
func (s *SQLSink) ScanEvents(ctx context.Context, now time.Time) (int, error) {
	if s == nil || s.h == nil || s.rules.Off {
		return 0, nil
	}
	written := 0
	_, err := s.h.Locked(ctx, eventsLock, func(q dal.MonitorStatements) error {
		var err error
		written, err = s.scanEvents(ctx, q, now)
		return err
	})
	return written, err
}

// scanEvents is the pass, on the connection that holds the lock.
func (s *SQLSink) scanEvents(ctx context.Context, q dal.MonitorStatements, now time.Time) (int, error) {
	now = now.UTC()
	to := now.Add(-eventsSettle)
	from := to.Add(-30 * time.Minute)
	mark, err := q.State(ctx, eventsWatermark)
	if err == nil {
		if t, perr := time.Parse(time.RFC3339Nano, mark); perr == nil {
			from = t
		}
	} else if err != dal.ErrNoRows {
		return 0, err
	}
	if to.Sub(from) > eventsMaxBehind {
		from = to.Add(-eventsMaxBehind)
	}
	if !to.After(from) {
		return 0, nil
	}
	rows, err := readSamples(ctx, q, from.Add(-eventLookback), now)
	if err != nil {
		return 0, err
	}
	written := 0
	for _, e := range detectEvents(rows, from, to, now, s.rules) {
		ok, err := insertEvent(ctx, q, e)
		if err != nil {
			return written, err
		}
		if ok {
			written++
		}
	}
	if err := q.SetState(ctx, eventsWatermark, to.Format(time.RFC3339Nano)); err != nil {
		return written, err
	}
	return written, nil
}

// readSamples reads the lifted columns of every report in [from, to].
func readSamples(ctx context.Context, q dal.MonitorStatements, from, to time.Time) ([]sampleRow, error) {
	rs, err := q.Samples(ctx, from, to)
	if err != nil {
		return nil, err
	}
	out := make([]sampleRow, 0, len(rs))
	for _, r := range rs {
		x := sampleRow{Deployment: r.Deployment, Role: r.Role, Instance: r.Instance, At: r.At,
			Accepted: r.Accepted, Rejected: r.Rejected, TPS: r.TPS, TPSOut: r.TPSOut, TPSRejected: r.TPSRejected,
			Latency: r.Latency, CPU: r.CPU, Mem: r.Mem, QueueDepth: r.QueueDepth, QueueAge: r.QueueAge,
			Up: r.Up, Total: r.Total, Push: r.Push}
		x.Stores = parseStores(r.Stores)
		x.At = x.At.UTC()
		x.Unhealthy = r.Unhealthy != 0
		x.Queues = parseQueues(r.Queues)
		out = append(out, x)
	}
	return out, nil
}

// insertEvent writes one event once (the unique key turns a repeat away) and,
// when it is new, its context: the deployment at the interval before, at and
// after it.
func insertEvent(ctx context.Context, q dal.MonitorStatements, e Event) (bool, error) {
	id, added, err := q.AddEvent(ctx, dal.MonitorEventRow{Deployment: e.Deployment, At: e.At, Kind: e.Kind,
		Phase: e.Phase, Severity: e.Severity, Subject: truncate(e.Subject, 128), Detail: truncate(e.Detail, 500),
		IntervalSec: e.IntervalSec})
	if err != nil {
		return false, err
	}
	if !added {
		return false, nil
	}
	if id == 0 {
		return true, nil // the store could not say its id: no context
	}
	p := time.Duration(maxInt(e.IntervalSec, 1)) * time.Second
	ctxJSON := map[string]any{"interval_sec": e.IntervalSec}
	var moments []map[string]any
	for _, m := range []struct {
		label string
		at    time.Time
	}{{"before", e.At.Add(-p)}, {"at", e.At}, {"after", e.At.Add(p)}} {
		inst, err := momentAt(ctx, q, e.Deployment, m.at, p)
		if err != nil {
			return true, nil // the event stands without its context
		}
		moments = append(moments, map[string]any{"label": m.label, "at": m.at.Format(time.RFC3339Nano), "instances": inst})
	}
	ctxJSON["moments"] = moments
	raw, err := json.Marshal(ctxJSON)
	if err != nil {
		return true, nil
	}
	gz, err := gzipBytes(raw)
	if err != nil {
		return true, nil
	}
	_ = q.SetEventContext(ctx, id, gz)
	return true, nil
}

// momentAt is the deployment at t: each instance's report nearest to t,
// within one interval either side, whole.
func momentAt(ctx context.Context, q dal.MonitorStatements, dep string, t time.Time, p time.Duration) ([]map[string]any, error) {
	rs, err := q.Moment(ctx, dep, t.Add(-p), t.Add(p))
	if err != nil {
		return nil, err
	}
	type pick struct {
		role, inst string
		at         time.Time
		body       []byte
	}
	best := map[string]pick{}
	for _, r := range rs {
		x := pick{role: r.Role, inst: r.Instance, at: r.At, body: r.Body}
		k := x.role + "|" + x.inst
		if cur, ok := best[k]; !ok || absDur(x.at.Sub(t)) < absDur(cur.at.Sub(t)) {
			best[k] = x
		}
	}
	keys := make([]string, 0, len(best))
	for k := range best {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		x := best[k]
		var snap json.RawMessage
		if raw, err := gunzipBytes(x.body); err == nil && json.Valid(raw) {
			snap = raw
		}
		out = append(out, map[string]any{"role": x.role, "instance": x.inst,
			"sampled_at": x.at.UTC().Format(time.RFC3339Nano), "snapshot": snap})
	}
	return out, nil
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// runEvents runs the event pass once a minute, on its own goroutine.
func (s *SQLSink) runEvents(ctx context.Context, logf func(string, ...any)) {
	if s == nil || s.rules.Off {
		return
	}
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	var lastErr time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c, cancel := context.WithTimeout(ctx, 50*time.Second)
		_, err := s.ScanEvents(c, time.Now())
		cancel()
		if err != nil && logf != nil && time.Since(lastErr) > 10*time.Minute {
			lastErr = time.Now()
			logf("monitor events pass failed (tried again every minute): %v", err)
		}
	}
}

// round2 keeps two decimals for stored figures.
func round2(f float64) float64 { return math.Round(f*100) / 100 }
