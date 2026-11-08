package monitor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Durable monitoring history.
//
// The LIVE view already existed before this file: StartRedisPush publishes
// every role's snapshot to status:<role>:<instance> on a TTL, so a dead
// instance disappears by itself. That is the right mechanism and this does not
// duplicate it — it hangs a second, durable sink off the same timer.
//
// The two answer different questions. Redis answers "what is happening right
// now" and forgets within seconds. MySQL answers "what happened at 3am", which
// is the question every incident review actually opens with, and which no
// amount of live dashboard can answer after the fact.
//
// gzip is not an optimisation here, it is what makes the retention plan
// possible at all. Measured on a 400-session snapshot: 203,817 bytes raw,
// 2,801 compressed — 72:1, because consecutive samples are nearly identical.
// Raw, fifteen roles at one sample per 5s would be ~250 GB a year and fill a
// 373 GB disk inside eighteen months.
//
// Best-effort by contract. Monitoring must never slow or break the thing it
// monitors: a failed write is counted and dropped, never retried into a
// backlog, and never surfaced as an error to the role.

// --- MySQL history sink -----------------------------------------------------

// SQLSink writes the durable history.
type SQLSink struct {
	// h is the history in the hot store (dal.OpenMonitorHistory).
	h dal.MonitorHistory
	// retentionDays is how long samples are kept (retention.go). 0 = forever.
	retentionDays int
	// summaryDays is how long day summaries and events are kept (events.go,
	// days.go). 0 = forever.
	summaryDays int
	// pushSec is how often this process reports, written on every row so the
	// readers can say how far apart the samples are (hot/0053).
	pushSec int
	// rules decide what counts as an event (events.go).
	rules EventRules
}

// NewSQLSink wraps the history in the hot store (dal.OpenMonitorHistory).
func NewSQLSink(h dal.MonitorHistory) *SQLSink {
	if h == nil {
		return nil
	}
	return &SQLSink{h: h, retentionDays: DefaultRetentionDays, summaryDays: DefaultSummaryDays,
		pushSec: int(MinPushInterval / time.Second), rules: DefaultEventRules()}
}

// WithSummaryDays sets how many days of day summaries and events to keep.
func (s *SQLSink) WithSummaryDays(days int) *SQLSink {
	if s != nil && days >= 0 {
		s.summaryDays = days
	}
	return s
}

// WithEventRules sets what counts as an event.
func (s *SQLSink) WithEventRules(r EventRules) *SQLSink {
	if s != nil {
		s.rules = r
	}
	return s
}

// PublishRaw stores one snapshot: the whole thing gzip'd into body, plus the
// handful of columns a dashboard queries constantly lifted out beside it.
//
// The lifted columns are the point. "Show me CPU and throughput for the last
// six hours" must not decompress 4,320 blobs to draw one line; it reads
// indexed numbers. The blob is for when someone opens a single moment and
// wants everything that was true at it.
func (s *SQLSink) PublishRaw(ctx context.Context, role, instance string, snap map[string]any) error {
	if s == nil || s.h == nil {
		return nil
	}
	// Marshal ONCE, then read the lifted columns from the decoded generic
	// form. The snapshot is assembled from role sections holding concrete Go
	// types (store.VendorQueueStat, monitor.System, monitor.Snap), so a
	// type-switch over the raw map would have to know every one of them and
	// would silently miss any new section a role adds. Going through JSON
	// makes every shape uniform, and the bytes are needed for the body anyway.
	raw, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return err
	}
	body, err := gzipBytes(raw)
	if err != nil {
		return err
	}
	l := lift(generic)
	now := time.Now().UTC()
	// Every queue's figures (hot/0055, D-127) and every store's (hot/0056,
	// D-129); NULL when the sample has none.
	var queues, stores *string
	if q := liftQueues(generic); q != nil {
		v := string(q)
		queues = &v
	}
	if st := liftStores(generic); st != nil {
		v := string(st)
		stores = &v
	}
	// A role that started before a migration was applied (http-server and
	// smpp-server migrate the hot store; the other roles push at once) still
	// keeps its sample without the newer columns (the DAL's AddSample).
	return s.h.AddSample(ctx, dal.MonitorSample{
		At: now, Role: role, Instance: instance,
		// Lifted out of `body` (hot/0047) so "this deployment's roles" is a
		// WHERE clause rather than a JSON read on every row.
		Deployment: DeploymentInfo().ID, State: l.state,
		Accepted: l.accepted, Rejected: l.rejected,
		TPS: l.tps, TPSOut: l.tpsOut, TPSReceipts: l.tpsReceipts, TPSMO: l.tpsMO,
		TPSRejected: l.tpsRejected,
		LatencyP95:  l.p95, VendorP95: l.vendorP95, QueueDepth: l.depth, QueueAge: l.age,
		SessionsUp: l.up, SessionsTotal: l.total, CPU: l.cpu, Mem: l.mem, Unhealthy: l.unhealthy,
		PushSec: s.pushSec, Queues: queues, Stores: stores, Body: body,
	})
}

// lifted are the indexed columns pulled out of a snapshot map.
type lifted struct {
	state                          string
	accepted, rejected, depth, age int64
	up, total                      int
	tps, p95, cpu, mem             float64
	tpsOut, tpsReceipts, tpsMO     float64
	tpsRejected                    float64
	vendorP95                      float64
	unhealthy                      int
}

// lift digs the dashboard columns out of the snapshot.
//
// Every key here was read off a REAL published snapshot rather than assumed —
// the first version looked for "queues" as an array and a "binds" key, and
// found neither, so the dashboard showed zero sessions and zero queue depth
// while the data sat in the body all along.
//
// Defensive throughout: sections differ by role and a missing or unexpected
// field must cost this sample one column, never the whole row. Monitoring that
// records nothing because a role renamed a key is worse than monitoring with a
// gap in one column.
func lift(snap map[string]any) lifted {
	l := lifted{state: "running"}
	if v, ok := snap["state"].(string); ok && v != "" {
		l.state = v
	}
	if sms, ok := snap["sms"].(map[string]any); ok {
		l.accepted = snapTotal(sms["accepted"])
		l.rejected = snapTotal(sms["rejected"])
		l.tps = snapRate(sms["accepted"])
		l.tpsRejected = snapRate(sms["rejected"])
	}
	// Latency is only meaningful once measured; an unknown p95 stays 0 and the
	// snapshot body carries known=false so a reader can tell the difference.
	if lat, ok := snap["latency"].(map[string]any); ok {
		if known, _ := lat["known"].(bool); known {
			l.p95 = toFloat(lat["p95_ms"])
		}
	}
	if sys, ok := snap["system"].(map[string]any); ok {
		// Only a MEASURED figure goes in the column, exactly as latency above
		// only stores p95 when the snapshot says it is known. A platform with
		// no tick source reports 0, and storing that made the history charts
		// draw a flat idle line for a box nobody had measured.
		if cpu, ok := sys["cpu"].(map[string]any); ok {
			if known, _ := cpu["usage_known"].(bool); known {
				l.cpu = toFloat(cpu["usage_percent"])
			}
		}
		if mem, ok := sys["memory"].(map[string]any); ok {
			if known, _ := mem["usage_known"].(bool); known {
				l.mem = toFloat(mem["usage_percent"])
			}
		}
	}
	// What the role moved (flow.go). Ingress fills tps from `sms` above;
	// every other role fills its direction here.
	if f, ok := snap["flow"].(map[string]any); ok {
		l.tpsOut = snapRate(f["out"])
		l.tpsReceipts = snapRate(f["receipts"])
		l.tpsMO = snapRate(f["mo"])
		if vl, ok := f["vendor_latency"].(map[string]any); ok {
			if known, _ := vl["known"].(bool); known {
				l.vendorP95 = toFloat(vl["p95_ms"])
			}
		}
	}
	// queues is a MAP keyed by vendor, not an array (httpserver/server.go).
	// Depth is summed; age is the OLDEST, because one stalled vendor is the
	// incident however healthy the others are, and an average would hide it.
	if q, ok := snap["queues"].(map[string]any); ok {
		for _, e := range q {
			if m, ok := e.(map[string]any); ok {
				l.depth += toInt64(m["depth"])
				if a := toInt64(m["oldest_age_sec"]); a > l.age {
					l.age = a
				}
			}
		}
	}
	// The client-push backlogs (mo.backlog / dlr.backlog) deliberately stay OUT
	// of queue_depth. That column is the VENDOR queue depth, and the summary
	// takes the largest report rather than the sum precisely because every
	// instance reports the same global number — folding a different quantity
	// into it would make one MO backlog masquerade as the vendor queue. The
	// backlogs travel in the snapshot body, and the Admin API lifts them out
	// as mo_backlog / dlr_backlog for the screen (2026-09-16).
	//
	// Sessions live under a per-role key with per-role names. P3 counts bound
	// SMPP vendor sessions; P2 counts client binds. Both answer "how many
	// connections should there be, and how many are there".
	if v, ok := snap["smpp_vendor"].(map[string]any); ok {
		l.up, l.total = int(toInt64(v["sessions_bound"])), int(toInt64(v["sessions_total"]))
		// A degraded session is unhealthy even though the process is fine, so
		// it lifts into the same column a failed dependency uses. The screen
		// leads with "what is wrong", and that has to be answerable from
		// indexed columns rather than by decompressing every snapshot.
		if toInt64(v["sessions_degraded"]) > 0 {
			l.unhealthy = 1
		}
	}
	if v, ok := snap["http_vendor"].(map[string]any); ok {
		// An HTTP vendor has no connection to lose (Saleem, 2026-10-12:
		// "there is no http vendor down"). Until then "up" was the vendors
		// that had carried traffic less the ones with a recent error, so
		// three idle vendors read "0 of 3 up" and one failed call took a
		// vendor "down" for a minute. The only HTTP vendor that is not there
		// is one waiting for its session.
		l.up += int(toInt64(v["vendors_total"]) - toInt64(v["vendors_not_ready"]))
		l.total += int(toInt64(v["vendors_total"]))
		if toInt64(v["vendors_degraded"]) > 0 {
			l.unhealthy = 1
		}
	}
	if v, ok := snap["smpp"].(map[string]any); ok {
		l.up, l.total = int(toInt64(v["connections"])), int(toInt64(v["bind_count"]))
	}
	return l
}

func toFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	}
	return 0
}

// snapTotal/snapRate read a Counter Snapshot without depending on its exact
// concrete type, which differs between the typed path and a JSON round trip.
func snapTotal(v any) int64 {
	switch t := v.(type) {
	case Snap:
		return t.Total
	case map[string]any:
		return toInt64(t["total"])
	}
	return 0
}

func snapRate(v any) float64 {
	switch t := v.(type) {
	case Snap:
		// TPS60 rather than the lifetime average: the dashboard asks "how fast
		// is it going now", and a lifetime figure on a role that has been up
		// for a week answers a question nobody asked.
		return t.TPS60
	case map[string]any:
		if f, ok := t["tps_60s"].(float64); ok {
			return f
		}
	}
	return 0
}

func toInt64(v any) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int64:
		return t
	case float64:
		return int64(t)
	}
	return 0
}

// gzipJSON marshals and compresses. Compression happens HERE rather than in
// MySQL so the cost lands on the publishing role — which is idle at that
// instant by definition — instead of on the shared database.
func gzipJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return gzipBytes(raw)
}

// gzipBytes compresses already-marshalled JSON.
func gzipBytes(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GunzipJSON is the reader's counterpart, used by the Admin API when replaying
// history.
func GunzipJSON(b []byte, v any) error {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer zr.Close()
	return json.NewDecoder(zr).Decode(v)
}

// gunzipBytes opens a gzip'd body.
func gunzipBytes(b []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	var out bytes.Buffer
	if _, err := out.ReadFrom(zr); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
