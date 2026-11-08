package monitor

// Day summaries (Saleem 2026-10-05, D-126).
//
// Every finished UTC day of every deployment becomes one monitor_days row:
// what passed through (accepted and refused exactly, from the counters; sent,
// receipts and MO about, from the 60-second rates), the peaks and when they
// happened, how much of the day each role was reporting, an hourly series and
// the day's events. It is written while the day's 5-second reports still
// exist — the upkeep makes it BEFORE dropping a day — so the summary outlives
// the detail by YAKU_MONITOR_SUMMARY_DAYS.
//
// A day is summarized only once the event pass has scanned past its end, so
// its last minutes' events are in it.

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// DaySummary is the body of one monitor_days row (and the lifted columns).
type DaySummary struct {
	Deployment  string             `json:"deployment_id"`
	Day         string             `json:"day"`
	IntervalSec int                `json:"interval_sec"`
	Samples     int64              `json:"samples"`
	Accepted    int64              `json:"accepted"`
	Rejected    int64              `json:"rejected"`
	Sent        int64              `json:"sent"`
	Receipts    int64              `json:"receipts"`
	MO          int64              `json:"mo"`
	Peaks       map[string]Peak    `json:"peaks"`
	RoleUptime  map[string]float64 `json:"role_uptime_pct"`
	Hours       []HourLine         `json:"hours"`
	Events      []DayEvent         `json:"events"`
	Counts      map[string]int     `json:"event_counts"`
	Queues      []DayQueue         `json:"queues,omitempty"`
	Stores      []DayStore         `json:"stores,omitempty"`
}

// DayStore is one MySQL or Redis server's day (D-129).
type DayStore struct {
	Kind       string  `json:"kind"` // mysql | redis
	Store      string  `json:"store"`
	Addr       string  `json:"addr,omitempty"`
	DownSec    int64   `json:"down_sec"`
	WarningSec int64   `json:"warning_sec"`
	MaxPingMS  float64 `json:"max_ping_ms"`
	MaxConnPct float64 `json:"max_conn_pct,omitempty"` // MySQL
	MaxMemPct  float64 `json:"max_mem_pct,omitempty"`  // Redis
	PeakPerSec float64 `json:"peak_per_sec"`           // queries (MySQL) / commands (Redis)
	PeakAt     string  `json:"peak_at,omitempty"`
	Restarts   int     `json:"restarts"`
}

// DayQueue is one queue's day (D-127): how many went in and out, and the most
// that waited and the longest wait, with when.
type DayQueue struct {
	Kind          string `json:"kind"` // mt | dlr | mo
	Queue         string `json:"queue"`
	In            int64  `json:"in"`
	Out           int64  `json:"out"`
	PeakWaiting   int64  `json:"peak_waiting"`
	PeakWaitingAt string `json:"peak_waiting_at,omitempty"`
	PeakAgeSec    int64  `json:"peak_age_sec"`
	PeakAgeAt     string `json:"peak_age_at,omitempty"`
}

// dayQueueLimit is the most queues a day summary lists.
const dayQueueLimit = 100

// Peak is the worst value of a figure in the day and when it happened.
type Peak struct {
	Value float64 `json:"value"`
	At    string  `json:"at,omitempty"`
}

// HourLine is one hour of the day.
type HourLine struct {
	Hour       int     `json:"hour"`
	TPSIn      float64 `json:"tps_in"`
	TPSOut     float64 `json:"tps_out"`
	QueueDepth int64   `json:"queue_depth_max"`
	LatencyMS  float64 `json:"latency_p95_max_ms"`
	Reporting  float64 `json:"reporting_pct"`
}

// DayEvent is one event as listed in a day summary.
type DayEvent struct {
	ID       int64  `json:"id"`
	At       string `json:"at"`
	Kind     string `json:"kind"`
	Phase    string `json:"phase"`
	Severity string `json:"severity"`
	Subject  string `json:"subject"`
	Detail   string `json:"detail"`
}

const summarizeDaysBack = 30 // never look further back than this for an unsummarized day

// summarizeDays writes every finished day that has reports and no summary yet.
func (s *SQLSink) summarizeDays(ctx context.Context, q dal.MonitorStatements, now time.Time) (int, error) {
	today := dayStart(now)
	// Not past what the event pass has scanned: a day's last events must be in.
	scanned := today
	if !s.rules.Off {
		if mark, err := q.State(ctx, eventsWatermark); err == nil {
			if t, perr := time.Parse(time.RFC3339Nano, mark); perr == nil {
				scanned = t
			}
		} else if errors.Is(err, dal.ErrNoRows) {
			// No event pass yet (the first start after an upgrade): the days
			// already kept are summarized without events rather than dropped
			// unsummarized.
			scanned = today
		} else {
			return 0, err
		}
	}
	done := 0
	for back := summarizeDaysBack; back >= 1; back-- {
		day := today.AddDate(0, 0, -back)
		if day.AddDate(0, 0, 1).After(scanned) {
			break
		}
		deps, err := q.DeploymentsOn(ctx, day, day.AddDate(0, 0, 1))
		if err != nil {
			return done, err
		}
		for _, dep := range deps {
			n, err := q.DayKept(ctx, dep, day.Format("2006-01-02"))
			if err != nil {
				return done, err
			}
			if n > 0 {
				continue
			}
			sum, err := summarizeDay(ctx, q, dep, day)
			if err != nil {
				return done, err
			}
			if err := writeDay(ctx, q, sum); err != nil {
				return done, err
			}
			done++
		}
	}
	return done, nil
}

func dayStart(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// summarizeDay builds one deployment's summary of one UTC day.
func summarizeDay(ctx context.Context, q dal.MonitorStatements, dep string, day time.Time) (DaySummary, error) {
	end := day.AddDate(0, 0, 1)
	sum := DaySummary{Deployment: dep, Day: day.Format("2006-01-02"), Peaks: map[string]Peak{},
		RoleUptime: map[string]float64{}, Counts: map[string]int{"critical": 0, "warning": 0, "info": 0}}

	// The interval: what most of the day's reports say.
	push := int(MinPushInterval / time.Second)
	{
		p, err := q.DayPush(ctx, dep, day, end)
		if err == nil && p.Valid && p.Int64 > 0 {
			push = int(p.Int64)
		}
	}
	sum.IntervalSec = push

	// Exact totals from the counters: each process counts from its own start,
	// so a process's part of the day is its last count less its first.
	{
		rs, err := q.DayCounts(ctx, dep, day, end)
		if err != nil {
			return sum, err
		}
		for _, c := range rs {
			sum.Samples += c.N
			sum.Accepted += c.Accepted
			sum.Rejected += c.Rejected
		}
	}

	// Per interval: each instance averaged, then added up (rates) or the
	// worst taken (queues, latency) — the Admin API's chart rule.
	type iv struct {
		b               int64
		in, out, rc, mo float64
		depth, age      int64
		lat             float64
		instances       int64
	}
	var ivs []iv
	{
		rs, err := q.DayIntervals(ctx, dep, day, end, push)
		if err != nil {
			return sum, err
		}
		for _, r := range rs {
			ivs = append(ivs, iv{b: r.B, in: r.In, out: r.Out, rc: r.Receipts, mo: r.MO, depth: r.Depth, age: r.Age,
				lat: r.Latency, instances: r.Instances})
		}
	}
	at := func(b int64) string { return time.Unix(b*int64(push), 0).UTC().Format(time.RFC3339) }
	peak := func(name string, v float64, b int64) {
		if cur, ok := sum.Peaks[name]; !ok || v > cur.Value {
			sum.Peaks[name] = Peak{Value: round2(v), At: at(b)}
		}
	}
	perHour := 3600 / push
	hours := make([]struct {
		in, out, lat float64
		depth        int64
		n            int
	}, 24)
	var sent, rc, mo float64
	for _, x := range ivs {
		peak("tps_in", x.in, x.b)
		peak("tps_out", x.out, x.b)
		peak("queue_depth", float64(x.depth), x.b)
		peak("queue_age_sec", float64(x.age), x.b)
		peak("latency_p95_ms", x.lat, x.b)
		sent += x.out * float64(push)
		rc += x.rc * float64(push)
		mo += x.mo * float64(push)
		h := time.Unix(x.b*int64(push), 0).UTC().Hour()
		hours[h].in += x.in
		hours[h].out += x.out
		hours[h].n++
		if x.depth > hours[h].depth {
			hours[h].depth = x.depth
		}
		if x.lat > hours[h].lat {
			hours[h].lat = x.lat
		}
	}
	sum.Sent, sum.Receipts, sum.MO = int64(math.Round(sent)), int64(math.Round(rc)), int64(math.Round(mo))
	for h, x := range hours {
		line := HourLine{Hour: h, QueueDepth: x.depth, LatencyMS: round2(x.lat)}
		if x.n > 0 {
			line.TPSIn, line.TPSOut = round2(x.in/float64(x.n)), round2(x.out/float64(x.n))
			line.Reporting = round2(float64(x.n) / float64(perHour) * 100)
		}
		sum.Hours = append(sum.Hours, line)
	}

	// How much of the day each role was reporting.
	{
		rs, err := q.DayRoles(ctx, dep, day, end, push)
		if err != nil {
			return sum, err
		}
		for _, r := range rs {
			sum.RoleUptime[r.Role] = math.Min(100, round2(float64(r.N)/float64(86400/push)*100))
		}
	}

	qs, err := dayQueues(ctx, q, dep, day, end)
	if err != nil {
		return sum, err
	}
	sum.Queues = qs
	st, err := dayStores(ctx, q, dep, day, end, push)
	if err != nil {
		return sum, err
	}
	sum.Stores = st

	// The day's events.
	{
		rs, err := q.DayEvents(ctx, dep, day, end)
		if err != nil {
			return sum, err
		}
		for _, r := range rs {
			e := DayEvent{ID: r.ID, Kind: r.Kind, Phase: r.Phase, Severity: r.Severity, Subject: r.Subject, Detail: r.Detail}
			e.At = r.At.UTC().Format(time.RFC3339Nano)
			sum.Events = append(sum.Events, e)
			sum.Counts[e.Severity]++
		}
	}
	sort.SliceStable(sum.Events, func(i, j int) bool { return sum.Events[i].At < sum.Events[j].At })
	return sum, nil
}

// dayQueues reads the day's per-queue figures (hot/0055) into one line per
// queue. In and out are each process's last total less its first, added up;
// a process whose first report of the day did not name a queue had moved
// nothing through it yet, so all of its total counts.
func dayQueues(ctx context.Context, q dal.MonitorStatements, dep string, day, end time.Time) ([]DayQueue, error) {
	type span struct {
		first, last [2]int64 // in, out
	}
	lines := map[string]*DayQueue{}
	spans := map[string]map[string]*span{} // instance -> queue key -> its first and last totals
	// A store from before hot/0055 hands no rows.
	err := q.DayQueues(ctx, dep, day, end, func(inst string, at time.Time, raw []byte) error {
		d := parseQueues(raw)
		if d == nil {
			return nil
		}
		mine := spans[inst]
		firstRow := mine == nil
		if firstRow {
			mine = map[string]*span{}
			spans[inst] = mine
		}
		stamp := at.UTC().Format(time.RFC3339)
		for kind, m := range map[string]map[string]queuePoint{"mt": d.MT, "dlr": d.DLR, "mo": d.MO} {
			for name, p := range m {
				key := kind + "\x00" + name
				l := lines[key]
				if l == nil {
					l = &DayQueue{Kind: kind, Queue: name}
					lines[key] = l
				}
				if p.W > l.PeakWaiting {
					l.PeakWaiting, l.PeakWaitingAt = p.W, stamp
				}
				if p.A > l.PeakAgeSec {
					l.PeakAgeSec, l.PeakAgeAt = p.A, stamp
				}
				sp := mine[key]
				if sp == nil {
					sp = &span{}
					if firstRow {
						sp.first = [2]int64{p.IT, p.OT}
					}
					mine[key] = sp
				}
				sp.last = [2]int64{p.IT, p.OT}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, mine := range spans {
		for key, sp := range mine {
			l := lines[key]
			l.In += max64(0, sp.last[0]-sp.first[0])
			l.Out += max64(0, sp.last[1]-sp.first[1])
		}
	}
	out := make([]DayQueue, 0, len(lines))
	for _, l := range lines {
		out = append(out, *l)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PeakWaiting != out[j].PeakWaiting {
			return out[i].PeakWaiting > out[j].PeakWaiting
		}
		if out[i].In != out[j].In {
			return out[i].In > out[j].In
		}
		return out[i].Kind+out[i].Queue < out[j].Kind+out[j].Queue
	})
	if len(out) > dayQueueLimit {
		out = out[:dayQueueLimit]
	}
	return out, nil
}

// dayStores reads the day's store figures (hot/0056) into one line per store:
// how long it was down or warning (intervals where any report said so), its
// worst round trip, connections and memory, its busiest interval, and how
// many times it restarted.
func dayStores(ctx context.Context, q dal.MonitorStatements, dep string, day, end time.Time, push int) ([]DayStore, error) {
	if push < 1 {
		push = 1
	}
	lines := map[string]*DayStore{}
	down, warn := map[string]map[int64]bool{}, map[string]map[int64]bool{}
	lastUp := map[string]int64{}
	// A store from before hot/0056 hands no rows.
	err := q.DayStores(ctx, dep, day, end, func(at time.Time, raw []byte) error {
		b := at.Unix() / int64(push)
		for key, p := range parseStores(raw) {
			l := lines[key]
			if l == nil {
				l = &DayStore{Kind: p.K, Store: key}
				lines[key], down[key], warn[key] = l, map[int64]bool{}, map[int64]bool{}
			}
			if p.A != "" {
				l.Addr = p.A
			}
			switch p.W {
			case 2:
				down[key][b] = true
			case 1:
				warn[key][b] = true
			}
			l.MaxPingMS = math.Max(l.MaxPingMS, p.P)
			l.MaxConnPct = math.Max(l.MaxConnPct, p.C)
			l.MaxMemPct = math.Max(l.MaxMemPct, p.M)
			if p.Q > l.PeakPerSec {
				l.PeakPerSec, l.PeakAt = p.Q, at.UTC().Format(time.RFC3339)
			}
			if p.OK == 1 && p.U > 0 {
				if prev, ok := lastUp[key]; ok && p.U+int64(2*push) < prev {
					l.Restarts++
				}
				lastUp[key] = p.U
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]DayStore, 0, len(lines))
	for key, l := range lines {
		l.DownSec = int64(len(down[key]) * push)
		l.WarningSec = int64(len(warn[key]) * push)
		out = append(out, *l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind+out[i].Store < out[j].Kind+out[j].Store })
	return out, nil
}

func writeDay(ctx context.Context, q dal.MonitorStatements, s DaySummary) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	body, err := gzipBytes(raw)
	if err != nil {
		return err
	}
	pk := func(name string) (float64, *time.Time) {
		p, ok := s.Peaks[name]
		if !ok || p.At == "" {
			return 0, nil
		}
		t, err := time.Parse(time.RFC3339, p.At)
		if err != nil {
			return p.Value, nil
		}
		t = t.UTC()
		return p.Value, &t
	}
	inV, inAt := pk("tps_in")
	outV, outAt := pk("tps_out")
	dV, dAt := pk("queue_depth")
	aV, aAt := pk("queue_age_sec")
	lV, lAt := pk("latency_p95_ms")
	return q.AddDay(ctx, dal.MonitorDay{
		Deployment: s.Deployment, Day: s.Day, IntervalSec: s.IntervalSec, Samples: s.Samples,
		Accepted: s.Accepted, Rejected: s.Rejected, Sent: s.Sent, Receipts: s.Receipts, MO: s.MO,
		PeakTPSIn: inV, PeakTPSInAt: inAt, PeakTPSOut: outV, PeakTPSOutAt: outAt,
		PeakQueueDepth: int64(dV), PeakQueueDepthAt: dAt, PeakQueueAge: int64(aV), PeakQueueAgeAt: aAt,
		PeakLatency: lV, PeakLatencyAt: lAt,
		EventsCritical: s.Counts["critical"], EventsWarning: s.Counts["warning"], EventsInfo: s.Counts["info"],
		Body: body,
	})
}

// trimSummaries drops day summaries and events past their keep: the longer of
// YAKU_MONITOR_SUMMARY_DAYS and the detail's own keep, so an event never goes
// before the reports around it. 0 keeps them for ever.
func (s *SQLSink) trimSummaries(ctx context.Context, q dal.MonitorStatements, now time.Time) (int64, error) {
	keep := s.summaryDays
	if keep <= 0 {
		return 0, nil
	}
	if s.retentionDays > keep {
		keep = s.retentionDays
	}
	cutoff := dayStart(now).AddDate(0, 0, -keep)
	var n int64
	d, err := q.DeleteDaysBefore(ctx, cutoff.Format("2006-01-02"))
	if err != nil {
		return 0, err
	}
	n += d
	for i := 0; i < deleteBatchesMax; i++ {
		d, err := q.DeleteEventsBefore(ctx, cutoff)
		if err != nil {
			return n, err
		}
		n += d
		if d < 1000 {
			break
		}
	}
	return n, nil
}
