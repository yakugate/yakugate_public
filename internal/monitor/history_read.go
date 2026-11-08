package monitor

// Reading the monitoring history back, for the console (Saleem 2026-10-05:
// "have you done the new monitoring things in the console as well?" — "yes,
// add them there as well"). The Admin API reads the same tables for the
// portal (p13-backend/src/routes/monitorRoutes.js); the rules are the same:
// any time picked gets each role's report NEAREST to it within its own
// interval, Live asks the gateway and waits for the answers, series are
// bucketed — the worst of contents, rates added up.

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

func gunzipTo(body []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(zr)
}

// FromHistoryFor is FromHistory for one deployment ("" = every deployment).
func FromHistoryFor(ctx context.Context, hot dal.MonitorStatements, within time.Duration, deployment string) (map[string]json.RawMessage, error) {
	if deployment == "" {
		return FromHistory(ctx, hot, within)
	}
	rows, err := hot.NewestOf(ctx, time.Now().UTC().Add(-within), deployment)
	if err != nil {
		return nil, err
	}
	return readBodies(rows), nil
}

// readBodies opens each report's body, by status key; one that will not open
// is left out.
func readBodies(rows []dal.MonitorBody) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for _, r := range rows {
		raw, err := gunzipTo(r.Body)
		if err != nil {
			continue
		}
		out[statusKeyFor(r.Role, r.Instance)] = raw
	}
	return out
}

// HistoryAt is the gateway as it was at `at`: each instance's report nearest
// to it — before or after — within that instance's own interval. An instance
// with no report that close was not running then. It returns when the nearest
// report was made, and the interval most reports were made at.
func HistoryAt(ctx context.Context, hot dal.MonitorStatements, at time.Time, deployment string) (map[string]json.RawMessage, time.Time, int, error) {
	at = at.UTC()
	rows, err := hot.Near(ctx, at.Add(-10*time.Minute), at.Add(10*time.Minute), deployment)
	if err != nil {
		return nil, time.Time{}, 0, err
	}
	type cand struct {
		id   int64
		off  time.Duration
		push int
		at   time.Time
	}
	best := map[string]cand{}
	pushes := map[int]int{}
	for _, r := range rows {
		c := cand{id: r.ID, at: r.At, push: r.Push}
		role, inst := r.Role, r.Instance
		c.at = c.at.UTC()
		c.off = c.at.Sub(at)
		if c.off < 0 {
			c.off = -c.off
		}
		pushes[c.push]++
		k := role + "|" + inst
		if cur, ok := best[k]; !ok || c.off < cur.off {
			best[k] = c
		}
	}
	interval, n := int(MinPushInterval/time.Second), 0
	for p, c := range pushes {
		if c > n {
			interval, n = p, c
		}
	}
	var ids []int64
	var nearest time.Time
	var nearestOff = time.Duration(math.MaxInt64)
	for _, c := range best {
		push := time.Duration(max(c.push, 1)) * time.Second
		if c.off > push {
			continue
		}
		ids = append(ids, c.id)
		if c.off < nearestOff {
			nearestOff, nearest = c.off, c.at
		}
	}
	if len(ids) == 0 {
		return map[string]json.RawMessage{}, time.Time{}, interval, nil
	}
	rs, err := hot.Bodies(ctx, ids)
	if err != nil {
		return nil, time.Time{}, interval, err
	}
	return readBodies(rs), nearest, interval, nil
}

// AskAndWait is Live for the console: ask every role of the deployment ("" =
// all) to report now, then wait — at most `wait` — until every role seen in
// the last 30 seconds has a report made after the ask. answered/of say how
// many did.
func AskAndWait(ctx context.Context, hot dal.MonitorStatements, deployment string, wait time.Duration) (asked time.Time, answered, of int, err error) {
	asked, err = AskForReportsOn(ctx, hot, deployment)
	if err != nil {
		return asked, 0, 0, err
	}
	since := asked.Add(-500 * time.Millisecond) // clocks of different hosts
	deadline := time.Now().Add(wait)
	for {
		of, answered, err = hot.Answered(ctx, since, time.Now().UTC().Add(-30*time.Second), deployment)
		if err != nil || of == 0 || answered >= of || time.Now().After(deadline) {
			return asked, answered, of, err
		}
		select {
		case <-ctx.Done():
			return asked, answered, of, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// FilterDeployment keeps the snapshots of one deployment ("" = all).
func FilterDeployment(snaps map[string]json.RawMessage, deployment string) map[string]json.RawMessage {
	if deployment == "" {
		return snaps
	}
	out := map[string]json.RawMessage{}
	for k, raw := range snaps {
		var d struct {
			Deployment struct {
				ID string `json:"id"`
			} `json:"deployment"`
		}
		if json.Unmarshal(raw, &d) == nil && d.Deployment.ID == deployment {
			out[k] = raw
		}
	}
	return out
}

// StoredEvent is one monitor_events row.
type StoredEvent = dal.MonitorEvent

var severityRank = map[string]int{"critical": 0, "warning": 1, "info": 2}

// ListEvents reads the events of a time range, newest first, at most limit;
// worstAllowed "critical" keeps critical only, "warning" warnings and up,
// "" everything.
func ListEvents(ctx context.Context, hot dal.MonitorStatements, from, to time.Time, deployment, worstAllowed string, limit int) ([]StoredEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := hot.Events(ctx, from, to, deployment, limit)
	if err != nil {
		return nil, err
	}
	var out []StoredEvent
	cut, filter := severityRank[worstAllowed], worstAllowed != ""
	for _, e := range rows {
		e.At = e.At.UTC()
		if filter && severityRank[e.Severity] > cut {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// EventMoment is the deployment at one of an event's three moments.
type EventMoment struct {
	Label string // before | at | after
	At    time.Time
	Snaps map[string]json.RawMessage
}

// EventMoments reads one event and the deployment before, at and after it.
func EventMoments(ctx context.Context, hot dal.MonitorStatements, id int64) (StoredEvent, []EventMoment, error) {
	e, ctxBlob, err := hot.Event(ctx, id)
	if err != nil {
		return e, nil, err
	}
	e.At = e.At.UTC()
	e.HasContext = len(ctxBlob) > 0
	if !e.HasContext {
		return e, nil, nil
	}
	raw, err := gunzipTo(ctxBlob)
	if err != nil {
		return e, nil, nil
	}
	var doc struct {
		Moments []struct {
			Label     string `json:"label"`
			At        string `json:"at"`
			Instances []struct {
				Role     string          `json:"role"`
				Instance string          `json:"instance"`
				Snapshot json.RawMessage `json:"snapshot"`
			} `json:"instances"`
		} `json:"moments"`
	}
	if json.Unmarshal(raw, &doc) != nil {
		return e, nil, nil
	}
	var out []EventMoment
	for _, m := range doc.Moments {
		at, _ := time.Parse(time.RFC3339Nano, m.At)
		mm := EventMoment{Label: m.Label, At: at.UTC(), Snaps: map[string]json.RawMessage{}}
		for _, in := range m.Instances {
			mm.Snaps[statusKeyFor(in.Role, in.Instance)] = in.Snapshot
		}
		out = append(out, mm)
	}
	return e, out, nil
}

// ListDays reads the day summaries of [fromDay, toDay] (UTC, YYYY-MM-DD),
// newest first.
func ListDays(ctx context.Context, hot dal.MonitorStatements, fromDay, toDay, deployment string) ([]DaySummary, error) {
	rows, err := hot.DaySummaries(ctx, fromDay, toDay, deployment)
	if err != nil {
		return nil, err
	}
	var out []DaySummary
	for _, body := range rows {
		raw, err := gunzipTo(body)
		if err != nil {
			continue
		}
		var d DaySummary
		if json.Unmarshal(raw, &d) == nil {
			out = append(out, d)
		}
	}
	return out, nil
}

// SeriesPoint is one bucket of a queue's or a store's history; a value
// missing from Values was not reported then.
type SeriesPoint = dal.SeriesPoint

var seriesName = regexp.MustCompile(`^[a-z0-9_]{1,160}$`)

// QueueSeries is one queue over [from, to): waiting / ready / held / oldest
// (the worst any instance reported, only from reports that carry that kind's
// depths) and in / out a second (added up), in about `points` buckets never
// finer than interval. kind is mt | dlr | mo; name a vendor queue's key or an
// account id.
func QueueSeries(ctx context.Context, hot dal.MonitorStatements, kind, name string, from, to time.Time, deployment string, interval, points int) ([]SeriesPoint, int, error) {
	if (kind != "mt" && kind != "dlr" && kind != "mo") || !seriesName.MatchString(name) {
		return nil, 0, fmt.Errorf("unknown queue %s/%s", kind, name)
	}
	bucket := seriesBucket(from, to, interval, points)
	out, err := hot.QueueSeries(ctx, kind, name, from, to, deployment, bucket)
	return sortedSeries(out), bucket, err
}

// StoreSeries is one MySQL or Redis server over [from, to): reachable (the
// lowest), health (the worst: 0 ok, 1 warning, 2 down), round trip,
// connections % (MySQL) or memory % (Redis), queries or commands a second.
func StoreSeries(ctx context.Context, hot dal.MonitorStatements, key string, from, to time.Time, deployment string, interval, points int) ([]SeriesPoint, int, error) {
	if !seriesName.MatchString(key) {
		return nil, 0, fmt.Errorf("unknown store %s", key)
	}
	bucket := seriesBucket(from, to, interval, points)
	out, err := hot.StoreSeries(ctx, key, from, to, deployment, bucket)
	return sortedSeries(out), bucket, err
}

// seriesBucket is a series' bucket: about `points` of them over [from, to),
// never finer than interval.
func seriesBucket(from, to time.Time, interval, points int) int {
	if interval < 1 {
		interval = int(MinPushInterval / time.Second)
	}
	if points < 2 {
		points = 60
	}
	span := int(math.Max(1, to.Sub(from).Seconds()))
	return max(interval, int(math.Ceil(float64(span)/float64(points)/float64(interval)))*interval)
}

// sortedSeries puts the points in time order.
func sortedSeries(out []SeriesPoint) []SeriesPoint {
	sort.Slice(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}
