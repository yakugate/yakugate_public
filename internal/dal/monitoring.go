package dal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// What monitoring keeps (internal/monitor, plan of 29 Oct 2026): the
// monitoring history in the hot store — every process's reports
// (monitor_samples), the events and day summaries made from them
// (monitor_events, monitor_days) and the small state beside them
// (monitor_state) — and the live view in the main Redis: each process's
// newest report, the store-reading turn and the ask to report now. How
// reports are made, judged and summed stays in internal/monitor; what is kept
// and read is here.

// ---- the history (hot store) ----

// MonitorSample is one report as it is written: the lifted columns beside
// the whole report, gzip'd, in Body. Queues and Stores are each queue's and
// each store's figures as JSON text (nil = the report has none).
type MonitorSample struct {
	At                                           time.Time
	Role, Instance, Deployment                   string
	State                                        string
	Accepted, Rejected                           int64
	TPS, TPSOut, TPSReceipts, TPSMO, TPSRejected float64
	LatencyP95, VendorP95                        float64
	QueueDepth, QueueAge                         int64
	SessionsUp, SessionsTotal                    int
	CPU, Mem                                     float64
	Unhealthy                                    int
	PushSec                                      int
	Queues, Stores                               *string
	Body                                         []byte
}

// MonitorSampleRow is the lifted columns of one report as the event pass
// reads them (never the body). Queues and Stores are the JSON as stored, nil
// when the report has none or the store is older than those columns.
type MonitorSampleRow struct {
	Deployment, Role, Instance string
	At                         time.Time
	Accepted, Rejected         int64
	TPS, TPSOut, TPSRejected   float64
	Latency, CPU, Mem          float64
	QueueDepth, QueueAge       int64
	Up, Total                  int
	Unhealthy                  int
	Push                       int
	Queues, Stores             []byte
}

// MonitorBody is one report's whole body (gzip'd JSON) and whose it is.
type MonitorBody struct {
	Role, Instance string
	At             time.Time // MonitorMoment only
	Body           []byte
}

// MonitorSampleRef is one report near a moment: which, whose, when and at
// what interval it was made.
type MonitorSampleRef struct {
	ID             int64
	Role, Instance string
	At             time.Time
	Push           int
}

// MonitorEvent is one monitor_events row as it is read back.
type MonitorEvent struct {
	ID          int64
	Deployment  string
	At          time.Time
	Kind        string
	Phase       string
	Severity    string
	Subject     string
	Detail      string
	IntervalSec int
	HasContext  bool
}

// MonitorEventRow is one event as it is written.
type MonitorEventRow struct {
	Deployment                             string
	At                                     time.Time
	Kind, Phase, Severity, Subject, Detail string
	IntervalSec                            int
}

// SeriesPoint is one bucket of a queue's or a store's history; a value
// missing from Values was not reported then.
type SeriesPoint struct {
	At     time.Time
	Values map[string]float64
}

// MonitorDayCount is one process's counters over a day: its reports, and
// its last accepted and refused counts less its first.
type MonitorDayCount struct {
	N, Accepted, Rejected int64
}

// MonitorDayInterval is one interval of a day: each process averaged, then
// added up (rates) or the worst taken (queues, latency).
type MonitorDayInterval struct {
	B                     int64
	In, Out, Receipts, MO float64
	Depth, Age            int64
	Latency               float64
	Instances             int64
}

// MonitorDayRole is how many intervals of a day a role reported in.
type MonitorDayRole struct {
	Role string
	N    int64
}

// MonitorDayEvent is one event of a day.
type MonitorDayEvent struct {
	ID                                     int64
	At                                     time.Time
	Kind, Phase, Severity, Subject, Detail string
}

// MonitorDay is one monitor_days row as it is written: the lifted figures
// and the whole summary, gzip'd. A peak time that is nil is written NULL.
type MonitorDay struct {
	Deployment, Day                           string
	IntervalSec                               int
	Samples, Accepted, Rejected               int64
	Sent, Receipts, MO                        int64
	PeakTPSIn                                 float64
	PeakTPSInAt                               *time.Time
	PeakTPSOut                                float64
	PeakTPSOutAt                              *time.Time
	PeakQueueDepth                            int64
	PeakQueueDepthAt                          *time.Time
	PeakQueueAge                              int64
	PeakQueueAgeAt                            *time.Time
	PeakLatency                               float64
	PeakLatencyAt                             *time.Time
	EventsCritical, EventsWarning, EventsInfo int
	Body                                      []byte
}

// MonitorStatements is every statement on the history. A MonitorHistory
// runs them on its pool; Locked runs them on the one connection that holds
// a named lock.
type MonitorStatements interface {
	// ---- reports ----

	// AddSample writes one report. A store older than the newest columns
	// keeps the report without them.
	AddSample(ctx context.Context, s MonitorSample) error
	// Samples reads the lifted columns of every report in [from, to], oldest
	// first (a store older than a column reads it as nothing).
	Samples(ctx context.Context, from, to time.Time) ([]MonitorSampleRow, error)
	// Newest is each instance's newest report made after since; NewestOf the
	// same for one deployment.
	Newest(ctx context.Context, since time.Time) ([]MonitorBody, error)
	NewestOf(ctx context.Context, since time.Time, deployment string) ([]MonitorBody, error)
	// Near lists the reports made in [from, to] of a deployment ("" = all).
	Near(ctx context.Context, from, to time.Time, deployment string) ([]MonitorSampleRef, error)
	// Bodies reads reports by id.
	Bodies(ctx context.Context, ids []int64) ([]MonitorBody, error)
	// Moment reads every report of a deployment ("" = those with none) made
	// in [from, to], whole (At set).
	Moment(ctx context.Context, deployment string, from, to time.Time) ([]MonitorBody, error)
	// Answered counts, of the processes of a deployment ("" = all) that
	// reported since recent, how many reported at or after since.
	Answered(ctx context.Context, since, recent time.Time, deployment string) (of, answered int, err error)
	// QueueSeries and StoreSeries are one queue's (kind mt|dlr|mo, name a
	// queue's key) or one store's (key) figures over [from, to) in buckets
	// of `bucket` seconds, by bucket. The names are checked by the caller.
	QueueSeries(ctx context.Context, kind, name string, from, to time.Time, deployment string, bucket int) ([]SeriesPoint, error)
	StoreSeries(ctx context.Context, key string, from, to time.Time, deployment string, bucket int) ([]SeriesPoint, error)
	// DeleteSamplesBefore deletes at most `limit` reports made before cutoff.
	DeleteSamplesBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error)
	// KeepSampleDays keeps a partition of the reports for today and the
	// next days, and drops each day partition that ends at or before cutoff
	// (keepAll: drop none). created is answered even when a later step fails.
	KeepSampleDays(ctx context.Context, today, cutoff time.Time, keepAll bool) (created, dropped []string, err error)

	// ---- events ----

	// AddEvent writes one event once: added=false when it was there
	// already. id is 0 when the store could not say it.
	AddEvent(ctx context.Context, e MonitorEventRow) (id int64, added bool, err error)
	// SetEventContext stores an event's context (gzip'd JSON).
	SetEventContext(ctx context.Context, id int64, gz []byte) error
	// Events reads the events of [from, to] of a deployment ("" = all),
	// newest first, at most limit.
	Events(ctx context.Context, from, to time.Time, deployment string, limit int) ([]MonitorEvent, error)
	// Event reads one event and its context (nil = none). ErrNoRows: no
	// such event.
	Event(ctx context.Context, id int64) (MonitorEvent, []byte, error)
	// DeleteEventsBefore deletes at most 1000 events made before cutoff.
	DeleteEventsBefore(ctx context.Context, cutoff time.Time) (int64, error)

	// ---- day summaries ----

	// DeploymentsOn lists the deployments that reported in [from, to) (""
	// for reports with none).
	DeploymentsOn(ctx context.Context, from, to time.Time) ([]string, error)
	// DayKept counts a deployment's summaries of one day (YYYY-MM-DD).
	DayKept(ctx context.Context, deployment, day string) (int, error)
	// The figures of a deployment's reports ("" = those with none) in
	// [from, to): the interval most reports were made at, each process's
	// counters, the intervals, how many intervals each role reported in.
	DayPush(ctx context.Context, deployment string, from, to time.Time) (NullInt64, error)
	DayCounts(ctx context.Context, deployment string, from, to time.Time) ([]MonitorDayCount, error)
	DayIntervals(ctx context.Context, deployment string, from, to time.Time, push int) ([]MonitorDayInterval, error)
	DayRoles(ctx context.Context, deployment string, from, to time.Time, push int) ([]MonitorDayRole, error)
	// DayQueues and DayStores hand each report's queue figures (with the
	// process, role|instance) or store figures to fn, oldest first. A store
	// older than those columns hands none.
	DayQueues(ctx context.Context, deployment string, from, to time.Time, fn func(inst string, at time.Time, raw []byte) error) error
	DayStores(ctx context.Context, deployment string, from, to time.Time, fn func(at time.Time, raw []byte) error) error
	// DayEvents reads a deployment's events of [from, to), oldest first.
	DayEvents(ctx context.Context, deployment string, from, to time.Time) ([]MonitorDayEvent, error)
	// AddDay writes one day summary, unless there is one.
	AddDay(ctx context.Context, d MonitorDay) error
	// DaySummaries reads the day summaries (gzip'd JSON) of [fromDay,
	// toDay] of a deployment ("" = all), newest first.
	DaySummaries(ctx context.Context, fromDay, toDay, deployment string) ([][]byte, error)
	// DeleteDaysBefore deletes the summaries of days before day.
	DeleteDaysBefore(ctx context.Context, day string) (int64, error)

	// ---- state ----

	// State is one monitor_state value. ErrNoRows: none.
	State(ctx context.Context, name string) (string, error)
	// SetState writes one monitor_state value.
	SetState(ctx context.Context, name, value string) error
}

// MonitorHistory is the monitoring history on its own small pool.
type MonitorHistory interface {
	MonitorStatements
	// Locked takes a named lock of this database, without waiting, on a
	// connection of its own, runs fn with the statements on that
	// connection, and lets the lock go. got=false (fn not run): another
	// process holds it.
	Locked(ctx context.Context, name string, fn func(MonitorStatements) error) (got bool, err error)
	// Close closes the pool.
	Close() error
}

// HistoryOn says where the monitoring history is kept: the hot store when it
// is a SQL database (MySQL or SQL Server) and named; "" = nowhere.
func HistoryOn(cfg config.Config) config.Backend {
	if (cfg.HotBackend == config.BackendMySQL || cfg.HotBackend == config.BackendMSSQL) && cfg.HotDSN != "" {
		return cfg.HotBackend
	}
	return ""
}

// OpenMonitorHistory opens the history on the DAL HistoryOn picks, on a small
// pool of its own (a report every few seconds must never compete with the
// traffic for connections). ErrNotServed when there is nowhere to keep it.
func OpenMonitorHistory(cfg config.Config) (MonitorHistory, error) {
	on := HistoryOn(cfg)
	if on == "" {
		return nil, fmt.Errorf("dal: monitor history: %w", ErrNotServed)
	}
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.MonitorHistory == nil {
		return nil, fmt.Errorf("dal: monitor history on %s: %w", on, ErrNotServed)
	}
	h, err := d.MonitorHistory(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: monitor history on %s: %w", on, err)
	}
	return h, nil
}

// ---- the live view (main Redis) ----

// LiveStatus is the live view: each process's newest report, kept a few
// intervals, and the list of them; the store-reading turn; the ask to
// report now; and the waiting sets an older build kept.
type LiveStatus interface {
	// Publish writes one process's report under key, kept for three
	// intervals, and lists the key.
	Publish(ctx context.Context, key string, snap []byte, every time.Duration) error
	// Statuses reads every listed report, by key.
	Statuses(ctx context.Context) (map[string]json.RawMessage, error)
	// TakeStoreTurn takes a deployment's (scope's) store-reading turn for
	// me when it is free or already mine, for hold; false when another
	// holds it.
	TakeStoreTurn(ctx context.Context, scope, me string, hold time.Duration) (bool, error)
	// Ask is the standing ask to report now; SetAsk writes one, kept for
	// keep. ErrNoRows-like errors (none) are the store's own.
	Ask(ctx context.Context) (string, error)
	SetAsk(ctx context.Context, value string, keep time.Duration) error
	// DropAwaitSets removes the waiting sets an older build kept; it answers
	// how many keys went.
	DropAwaitSets(ctx context.Context) (int, error)
}

// OpenLiveStatus opens the live view on the main Redis, when one is set.
// ErrNotServed when Redis is off.
func OpenLiveStatus(cfg config.Config) (LiveStatus, error) {
	if !cfg.RedisEnabled() {
		return nil, fmt.Errorf("dal: live status: %w", ErrNotServed)
	}
	d, ok := Lookup(config.BackendRedis)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", config.BackendRedis, Registered())
	}
	if d.LiveStatus == nil {
		return nil, fmt.Errorf("dal: live status on %s: %w", config.BackendRedis, ErrNotServed)
	}
	l, err := d.LiveStatus(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: live status on %s: %w", config.BackendRedis, err)
	}
	return l, nil
}
