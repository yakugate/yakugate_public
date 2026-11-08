package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// The monitoring history in the hot store (monitor_samples, monitor_events,
// monitor_days, monitor_state) — moved here unchanged on 29 Oct 2026 (the DAL
// plan) from internal/monitor (publish.go, events.go, days.go, retention.go,
// history_read.go, overview.go, reportnow.go). What a report holds, how events
// are found and how a day is summed stayed in internal/monitor.

// The monitoring history (the hot store, a small pool of its own).
var _ = serves(func(d *dal.Driver) { d.MonitorHistory = openMonitorHistory })

// openMonitorHistory is the history's opener: a small pool of its own on the
// hot store, as cmd/yakugate opened it — a report every few seconds must never
// compete with the traffic path for connections.
func openMonitorHistory(cfg config.Config) (dal.MonitorHistory, error) {
	hdb, err := sql.Open("mysql", dbpool.DSN(cfg.HotDSN))
	if err != nil {
		return nil, err
	}
	// A small pool on purpose: this writes one row every few seconds and
	// must never compete with the traffic path for connections.
	hdb.SetMaxOpenConns(2)
	hdb.SetMaxIdleConns(1)
	hdb.SetConnMaxIdleTime(time.Minute)
	return NewMonitorHistory(hdb), nil
}

// MonitorStmts is every statement of the history, by name. A statement built
// at run time (a column list, an IN list, a series' JSON paths) is registered
// in one representative form; the text a call sends is that form with its
// parts filled in.
var MonitorStmts = stmt.New("mysql")

// MonitorStatements is the registry of these statements (the lint test).
func MonitorStatements() *stmt.Registry { return MonitorStmts }

// lockExpr names a lock for THIS database: GET_LOCK names are server-wide,
// and two gateways with their own databases on one MySQL server must not hold
// each other off (the singleton lesson, 2026-10-02).
const lockExpr = "LEFT(CONCAT(?, '@', DATABASE()), 64)"

const (
	readSamplesCols = `COALESCE(deployment_id,''), role, instance, sampled_at, accepted, rejected,
		tps, tps_out, %s, latency_p95_ms, cpu_pct, mem_pct, queue_depth, queue_age_sec,
		sessions_up, sessions_total, unhealthy, %s, %s, %s`

	// partitionsAhead: today plus two — a missed night's run never leaves
	// new rows homeless in p_max.
	partitionsAhead = 3
	// toDaysEpoch is MySQL TO_DAYS('1970-01-01'). RANGE partitions are bounded
	// in TO_DAYS numbers, and computing them here keeps the DDL a literal.
	toDaysEpoch = 719528
)

var (
	stMonLock    = MonitorStmts.Add("monitor.lock", "SELECT GET_LOCK("+lockExpr+", 0)", false)
	stMonRelease = MonitorStmts.Add("monitor.release", "SELECT RELEASE_LOCK("+lockExpr+")", false)

	stMonAddSample = MonitorStmts.Add("monitor.add_sample",
		"INSERT INTO monitor_samples (sampled_at, role, instance) VALUES (?,?,?)", false)
	stMonSamples = MonitorStmts.Add("monitor.samples", "SELECT "+fmt.Sprintf(readSamplesCols, "tps_rejected", "push_sec", "queues", "stores")+
		" FROM monitor_samples WHERE sampled_at >= ? AND sampled_at <= ? ORDER BY sampled_at", false)
	stMonNewest = MonitorStmts.Add("monitor.newest", `
		SELECT m.role, m.instance, m.body
		  FROM monitor_samples m
		  JOIN (SELECT instance, MAX(sampled_at) t FROM monitor_samples
		         WHERE tier = 0 AND sampled_at > ? GROUP BY instance) n
		    ON n.instance = m.instance AND n.t = m.sampled_at
		 WHERE m.tier = 0`, false)
	stMonNewestOf = MonitorStmts.Add("monitor.newest_of", `
		SELECT m.role, m.instance, m.body
		  FROM monitor_samples m
		  JOIN (SELECT instance, MAX(sampled_at) t FROM monitor_samples
		         WHERE tier = 0 AND sampled_at > ? AND deployment_id = ? GROUP BY instance) n
		    ON n.instance = m.instance AND n.t = m.sampled_at
		 WHERE m.tier = 0 AND m.deployment_id = ?`, false)
	stMonNear = MonitorStmts.Add("monitor.near", `SELECT id, role, instance, sampled_at, push_sec FROM monitor_samples
		WHERE sampled_at BETWEEN ? AND ? AND (? = '' OR deployment_id = ?)`, false)
	stMonBodies = MonitorStmts.Add("monitor.bodies",
		"SELECT role, instance, body FROM monitor_samples WHERE id IN (?)", false)
	stMonMoment = MonitorStmts.Add("monitor.moment", `SELECT role, instance, sampled_at, body FROM monitor_samples
		WHERE deployment_id = ? AND sampled_at >= ? AND sampled_at <= ?`, false)
	stMonAnswered = MonitorStmts.Add("monitor.answered", `SELECT COUNT(*), COALESCE(SUM(newest >= ?), 0)
			FROM (SELECT MAX(sampled_at) AS newest FROM monitor_samples
			       WHERE sampled_at >= ? AND (? = '' OR deployment_id = ?) GROUP BY role, instance) x`, false)
	stMonSeries = MonitorStmts.Add("monitor.series", fmt.Sprintf(seriesSQL,
		`MAX(waiting)`, `AVG(COALESCE(JSON_EXTRACT(queues, '$.mt."q".w') + 0, 0)) AS waiting`, "queues", ""), false)
	stMonDeleteSamples = MonitorStmts.Add("monitor.delete_samples",
		`DELETE FROM monitor_samples WHERE sampled_at < ? LIMIT 5000`, false)
	stMonPartitions = MonitorStmts.Add("monitor.partitions", `
		SELECT PARTITION_NAME, PARTITION_DESCRIPTION
		  FROM information_schema.PARTITIONS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'monitor_samples'
		   AND PARTITION_NAME IS NOT NULL`, false)
	stMonSplitMax = MonitorStmts.Add("monitor.split_max",
		"ALTER TABLE monitor_samples REORGANIZE PARTITION p_max INTO (PARTITION p20260101 VALUES LESS THAN (740000), PARTITION p_max VALUES LESS THAN MAXVALUE)", false)
	stMonDropParts = MonitorStmts.Add("monitor.drop_partitions",
		"ALTER TABLE monitor_samples DROP PARTITION p20260101", false)

	stMonAddEvent = MonitorStmts.Add("monitor.add_event", `INSERT IGNORE INTO monitor_events
		(deployment_id, at, kind, phase, severity, subject, detail, interval_sec)
		VALUES (?,?,?,?,?,?,?,?)`, false)
	stMonEventContext = MonitorStmts.Add("monitor.event_context", "UPDATE monitor_events SET context = ? WHERE id = ?", false)
	stMonEvents       = MonitorStmts.Add("monitor.events", `SELECT id, deployment_id, at, kind, phase, severity, subject, detail, interval_sec,
			context IS NOT NULL FROM monitor_events
		 WHERE at >= ? AND at <= ? AND (? = '' OR deployment_id = ?) ORDER BY at DESC, id DESC LIMIT ?`, false)
	stMonEvent = MonitorStmts.Add("monitor.event", `SELECT id, deployment_id, at, kind, phase, severity, subject, detail, interval_sec, context
		FROM monitor_events WHERE id = ?`, false)
	stMonDeleteEvents = MonitorStmts.Add("monitor.delete_events", "DELETE FROM monitor_events WHERE at < ? LIMIT 1000", false)

	stMonDeployments = MonitorStmts.Add("monitor.day_deployments", `SELECT DISTINCT COALESCE(deployment_id,'') FROM monitor_samples
		WHERE sampled_at >= ? AND sampled_at < ?`, false)
	stMonDayKept = MonitorStmts.Add("monitor.day_kept",
		"SELECT COUNT(*) FROM monitor_days WHERE deployment_id = ? AND day = ?", false)
	stMonDayPush = MonitorStmts.Add("monitor.day_push", "SELECT push_sec FROM monitor_samples"+dayWhereSQL+
		" GROUP BY push_sec ORDER BY COUNT(*) DESC LIMIT 1", false)
	stMonDayCounts = MonitorStmts.Add("monitor.day_counts", `SELECT COUNT(*), MAX(accepted)-MIN(accepted), MAX(rejected)-MIN(rejected)
			FROM monitor_samples`+dayWhereSQL+` GROUP BY role, instance`, false)
	stMonDayIntervals = MonitorStmts.Add("monitor.day_intervals", `SELECT b, SUM(tin), SUM(tout), SUM(trc), SUM(tmo), MAX(qd), MAX(qa), MAX(lat), COUNT(*)
			FROM (SELECT FLOOR(TIMESTAMPDIFF(SECOND, '1970-01-01', sampled_at) / ?) AS b, role, instance,
			             AVG(tps) tin, AVG(tps_out) tout, AVG(tps_receipts) trc, AVG(tps_mo) tmo,
			             MAX(queue_depth) qd, MAX(queue_age_sec) qa, MAX(latency_p95_ms) lat
			        FROM monitor_samples`+dayWhereSQL+` GROUP BY b, role, instance) x
			GROUP BY b ORDER BY b`, false)
	stMonDayRoles = MonitorStmts.Add("monitor.day_roles", `SELECT role, COUNT(DISTINCT FLOOR(TIMESTAMPDIFF(SECOND, '1970-01-01', sampled_at) / ?))
			FROM monitor_samples`+dayWhereSQL+` GROUP BY role`, false)
	stMonDayQueues = MonitorStmts.Add("monitor.day_queues", `SELECT CONCAT(role, '|', instance), sampled_at, queues FROM monitor_samples`+
		dayWhereSQL+` AND queues IS NOT NULL ORDER BY sampled_at`, false)
	stMonDayStores = MonitorStmts.Add("monitor.day_stores", `SELECT sampled_at, stores FROM monitor_samples`+
		dayWhereSQL+` AND stores IS NOT NULL ORDER BY sampled_at`, false)
	stMonDayEvents = MonitorStmts.Add("monitor.day_events", `SELECT id, at, kind, phase, severity, subject, detail FROM monitor_events
			WHERE deployment_id = ? AND at >= ? AND at < ? ORDER BY at, id`, false)
	stMonAddDay = MonitorStmts.Add("monitor.add_day", `INSERT IGNORE INTO monitor_days
		(deployment_id, day, interval_sec, samples, accepted, rejected, sent, receipts, mo,
		 peak_tps_in, peak_tps_in_at, peak_tps_out, peak_tps_out_at, peak_queue_depth, peak_queue_depth_at,
		 peak_queue_age_sec, peak_queue_age_at, peak_latency_ms, peak_latency_at,
		 events_critical, events_warning, events_info, body)
		VALUES (?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?, ?,?,?,?, ?,?,?,?)`, false)
	stMonDaySummaries = MonitorStmts.Add("monitor.day_summaries", `SELECT body FROM monitor_days WHERE day >= ? AND day <= ?
		AND (? = '' OR deployment_id = ?) ORDER BY day DESC, deployment_id`, false)
	stMonDeleteDays = MonitorStmts.Add("monitor.delete_days", "DELETE FROM monitor_days WHERE day < ?", false)

	stMonState    = MonitorStmts.Add("monitor.state", "SELECT value FROM monitor_state WHERE name = ?", false)
	stMonSetState = MonitorStmts.Add("monitor.set_state", `INSERT INTO monitor_state (name, value) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE value = VALUES(value)`, false)
)

// dayWhereSQL is the registered form of a day's WHERE (dayWhere).
const dayWhereSQL = " WHERE deployment_id = ? AND sampled_at >= ? AND sampled_at < ?"

// seriesSQL is a two-level series query over one JSON column: inner per
// instance per bucket, outer per bucket (outer, inner, column, extra).
const seriesSQL = `SELECT b, %s FROM (
		SELECT FLOOR(TIMESTAMPDIFF(SECOND, '1970-01-01', sampled_at) / ?) AS b, role, instance, %s
		  FROM monitor_samples WHERE sampled_at >= ? AND sampled_at < ? AND %s IS NOT NULL%s AND (? = '' OR deployment_id = ?)
		 GROUP BY b, role, instance) x GROUP BY b ORDER BY b`

// monQueryer is a *sql.DB or a *sql.Conn.
type monQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// MonitorHistory is the history on a hot-store pool.
type MonitorHistory struct {
	monStatements
	db *sql.DB
}

// monStatements runs the statements on a pool or on one connection.
type monStatements struct{ q monQueryer }

var (
	_ dal.MonitorHistory    = (*MonitorHistory)(nil)
	_ dal.MonitorStatements = monStatements{}
)

// NewMonitorHistory wraps a hot-store pool. nil db = nil history.
func NewMonitorHistory(db *sql.DB) *MonitorHistory {
	if db == nil {
		return nil
	}
	return &MonitorHistory{monStatements: monStatements{q: db}, db: db}
}

// Close closes the pool.
func (h *MonitorHistory) Close() error { return h.db.Close() }

// Locked takes the named lock on a dedicated connection: a named lock lives
// on ONE connection, so it is taken and released on that one rather than
// whichever the pool hands out next.
func (h *MonitorHistory) Locked(ctx context.Context, name string, fn func(dal.MonitorStatements) error) (bool, error) {
	conn, err := h.db.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, stMonLock.SQL, name).Scan(&got); err != nil {
		return false, err
	}
	if got.Int64 != 1 {
		return false, nil
	}
	defer conn.ExecContext(context.Background(), stMonRelease.SQL, name)
	return true, fn(monStatements{q: conn})
}

// ---- reports ----

func (s monStatements) AddSample(ctx context.Context, x dal.MonitorSample) error {
	var queues, stores any
	if x.Queues != nil {
		queues = *x.Queues
	}
	if x.Stores != nil {
		stores = *x.Stores
	}
	// Every column, then fewer: a role that started before a migration was
	// applied (http-server and smpp-server migrate the hot store; the other
	// roles push at once) still keeps its sample without the newer columns.
	cols := []struct {
		name string
		val  any
	}{
		{"sampled_at", x.At}, {"role", x.Role}, {"instance", x.Instance},
		// Lifted out of `body` (hot/0047) so "this deployment's roles" is a
		// WHERE clause rather than a JSON read on every row.
		{"deployment_id", x.Deployment}, {"tier", 0}, {"state", x.State},
		{"accepted", x.Accepted}, {"rejected", x.Rejected},
		{"tps", x.TPS}, {"tps_out", x.TPSOut}, {"tps_receipts", x.TPSReceipts}, {"tps_mo", x.TPSMO},
		{"tps_rejected", x.TPSRejected},
		{"latency_p95_ms", x.LatencyP95}, {"vendor_p95_ms", x.VendorP95}, {"queue_depth", x.QueueDepth}, {"queue_age_sec", x.QueueAge},
		{"sessions_up", x.SessionsUp}, {"sessions_total", x.SessionsTotal}, {"cpu_pct", x.CPU}, {"mem_pct", x.Mem}, {"unhealthy", x.Unhealthy},
		{"push_sec", x.PushSec}, {"queues", queues}, {"stores", stores}, {"body", x.Body},
	}
	newer := [][]string{{"stores"}, {"stores", "queues"}, {"stores", "queues", "push_sec", "tps_rejected"}}
	var err error
	for attempt := 0; ; attempt++ {
		var names, marks []string
		var args []any
		skip := map[string]bool{}
		if attempt > 0 {
			for _, n := range newer[attempt-1] {
				skip[n] = true
			}
		}
		for _, c := range cols {
			if !skip[c.name] {
				names, marks, args = append(names, c.name), append(marks, "?"), append(args, c.val)
			}
		}
		_, err = s.q.ExecContext(ctx, "INSERT INTO monitor_samples ("+strings.Join(names, ", ")+") VALUES ("+
			strings.Join(marks, ",")+")", args...)
		if err == nil || !strings.Contains(err.Error(), "Unknown column") || attempt == len(newer) {
			break
		}
	}
	return err
}

// Samples reads the lifted columns of every report in [from, to].
func (s monStatements) Samples(ctx context.Context, from, to time.Time) ([]dal.MonitorSampleRow, error) {
	const cols = readSamplesCols
	query := func(extra1, extra2, extra3, extra4 string) (*sql.Rows, error) {
		return s.q.QueryContext(ctx, "SELECT "+fmt.Sprintf(cols, extra1, extra2, extra3, extra4)+
			" FROM monitor_samples WHERE sampled_at >= ? AND sampled_at <= ? ORDER BY sampled_at", from, to)
	}
	rs, err := query("tps_rejected", "push_sec", "queues", "stores")
	if err != nil && strings.Contains(err.Error(), "Unknown column") {
		rs, err = query("tps_rejected", "push_sec", "queues", "NULL") // a store from before hot/0056
	}
	if err != nil && strings.Contains(err.Error(), "Unknown column") {
		rs, err = query("tps_rejected", "push_sec", "NULL", "NULL") // before hot/0055
	}
	if err != nil && strings.Contains(err.Error(), "Unknown column") {
		rs, err = query("0", "5", "NULL", "NULL") // before hot/0053
	}
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []dal.MonitorSampleRow
	for rs.Next() {
		var x dal.MonitorSampleRow
		if err := rs.Scan(&x.Deployment, &x.Role, &x.Instance, &x.At, &x.Accepted, &x.Rejected,
			&x.TPS, &x.TPSOut, &x.TPSRejected, &x.Latency, &x.CPU, &x.Mem, &x.QueueDepth, &x.QueueAge,
			&x.Up, &x.Total, &x.Unhealthy, &x.Push, &x.Queues, &x.Stores); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rs.Err()
}

func (s monStatements) Newest(ctx context.Context, since time.Time) ([]dal.MonitorBody, error) {
	rows, err := s.q.QueryContext(ctx, stMonNewest.SQL, since)
	if err != nil {
		return nil, err
	}
	return readBodies(rows)
}

func (s monStatements) NewestOf(ctx context.Context, since time.Time, deployment string) ([]dal.MonitorBody, error) {
	rows, err := s.q.QueryContext(ctx, stMonNewestOf.SQL, since, deployment, deployment)
	if err != nil {
		return nil, err
	}
	return readBodies(rows)
}

func readBodies(rows *sql.Rows) ([]dal.MonitorBody, error) {
	defer rows.Close()
	var out []dal.MonitorBody
	for rows.Next() {
		var b dal.MonitorBody
		if err := rows.Scan(&b.Role, &b.Instance, &b.Body); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// Near reads the reports near a moment. As before the move, a read cut short
// after its last row is not reported (rows.Err is not asked).
func (s monStatements) Near(ctx context.Context, from, to time.Time, deployment string) ([]dal.MonitorSampleRef, error) {
	rows, err := s.q.QueryContext(ctx, stMonNear.SQL, from, to, deployment, deployment)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.MonitorSampleRef
	for rows.Next() {
		var c dal.MonitorSampleRef
		if err := rows.Scan(&c.ID, &c.Role, &c.Instance, &c.At, &c.Push); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func (s monStatements) Bodies(ctx context.Context, ids []int64) ([]dal.MonitorBody, error) {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	q := "SELECT role, instance, body FROM monitor_samples WHERE id IN (?" + repeatComma(len(ids)-1) + ")"
	rs, err := s.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return readBodies(rs)
}

func repeatComma(n int) string {
	b := make([]byte, 0, n*2)
	for i := 0; i < n; i++ {
		b = append(b, ",?"...)
	}
	return string(b)
}

// Moment reads a deployment's reports in [from, to], whole.
func (s monStatements) Moment(ctx context.Context, dep string, from, to time.Time) ([]dal.MonitorBody, error) {
	depCond, args := "deployment_id = ?", []any{dep}
	if dep == "" {
		depCond, args = "deployment_id IS NULL", nil
	}
	args = append(args, from, to)
	rs, err := s.q.QueryContext(ctx, `SELECT role, instance, sampled_at, body FROM monitor_samples
		WHERE `+depCond+` AND sampled_at >= ? AND sampled_at <= ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []dal.MonitorBody
	for rs.Next() {
		var x dal.MonitorBody
		if err := rs.Scan(&x.Role, &x.Instance, &x.At, &x.Body); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rs.Err()
}

func (s monStatements) Answered(ctx context.Context, since, recent time.Time, deployment string) (of, answered int, err error) {
	err = s.q.QueryRowContext(ctx, stMonAnswered.SQL, since, recent, deployment, deployment).Scan(&of, &answered)
	return of, answered, err
}

// QueueSeries is one queue over [from, to): waiting / ready / held / oldest
// (the worst any instance reported, only from reports that carry that kind's
// depths) and in / out a second (added up).
func (s monStatements) QueueSeries(ctx context.Context, kind, name string, from, to time.Time, deployment string, bucket int) ([]dal.SeriesPoint, error) {
	path := func(f string) string { return fmt.Sprintf(`$.%s."%s".%s`, kind, name, f) }
	has := fmt.Sprintf(`JSON_CONTAINS(COALESCE(JSON_EXTRACT(queues, '$.d'), JSON_ARRAY()), '"%s"')`, kind)
	val := func(f string) string { return fmt.Sprintf(`COALESCE(JSON_EXTRACT(queues, '%s') + 0, 0)`, path(f)) }
	inner := fmt.Sprintf(`AVG(CASE WHEN %[1]s THEN %[2]s END) AS waiting, AVG(CASE WHEN %[1]s THEN %[3]s END) AS ready,
		AVG(CASE WHEN %[1]s THEN %[4]s END) AS held, MAX(%[5]s) AS oldest, AVG(%[6]s) AS tin, AVG(%[7]s) AS tout`,
		has, val("w"), val("r"), val("h"), val("a"), val("i"), val("o"))
	return s.bucketed(ctx, "queues", inner, `MAX(waiting), MAX(ready), MAX(held), MAX(oldest), SUM(tin), SUM(tout)`,
		[]string{"waiting", "ready", "held", "oldest_sec", "in_per_sec", "out_per_sec"}, from, to, deployment, bucket, "")
}

// StoreSeries is one MySQL or Redis server over [from, to): reachable (the
// lowest), health (the worst: 0 ok, 1 warning, 2 down), round trip,
// connections % (MySQL) or memory % (Redis), queries or commands a second.
func (s monStatements) StoreSeries(ctx context.Context, key string, from, to time.Time, deployment string, bucket int) ([]dal.SeriesPoint, error) {
	f := func(n string) string { return fmt.Sprintf(`JSON_EXTRACT(stores, '$.%s.%s') + 0`, key, n) }
	inner := fmt.Sprintf(`MIN(%s) AS ok, MAX(%s) AS w, MAX(%s) AS p, MAX(GREATEST(COALESCE(%s, 0), COALESCE(%s, 0))) AS used, AVG(%s) AS q`,
		f("ok"), f("w"), f("p"), f("c"), f("m"), f("q"))
	return s.bucketed(ctx, "stores", inner, `MIN(ok), MAX(w), MAX(p), MAX(used), MAX(q)`,
		[]string{"ok", "health", "ping_ms", "used_pct", "per_sec"}, from, to, deployment, bucket,
		fmt.Sprintf(` AND JSON_CONTAINS_PATH(stores, 'one', '$.%s')`, key))
}

// bucketed runs a two-level series query over one JSON column: inner per
// instance per bucket, outer per bucket.
func (s monStatements) bucketed(ctx context.Context, column, inner, outer string, names []string, from, to time.Time,
	deployment string, bucket int, extra string) ([]dal.SeriesPoint, error) {
	q := fmt.Sprintf(seriesSQL, outer, inner, column, extra)
	rows, err := s.q.QueryContext(ctx, q, bucket, from.UTC(), to.UTC(), deployment, deployment)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.SeriesPoint
	for rows.Next() {
		var b int64
		vals := make([]sql.NullFloat64, len(names))
		dest := []any{&b}
		for i := range vals {
			dest = append(dest, &vals[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		p := dal.SeriesPoint{At: time.Unix(b*int64(bucket), 0).UTC(), Values: map[string]float64{}}
		for i, v := range vals {
			if v.Valid {
				p.Values[names[i]] = v.Float64
			}
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s monStatements) DeleteSamplesBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	r, err := s.q.ExecContext(ctx,
		`DELETE FROM monitor_samples WHERE sampled_at < ? LIMIT `+strconv.Itoa(limit), cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := r.RowsAffected()
	return n, nil
}

// ---- partitions ----

// KeepSampleDays splits p_max so the next days each have a partition waiting,
// and drops the days that have aged out.
//
// Expiry is a partition DROP, not a DELETE. A week of fifteen roles is ~1.8
// million rows; deleting a day of them row by row is minutes of undo log and
// lock pressure on the same database the traffic path writes to, while
// dropping a partition is a metadata change that takes milliseconds.
//
// That needs daily partitions to exist, and 0033 created none — only p_start
// and p_max, so every row landed in p_max. Splitting an EMPTY p_max is
// instant; the one expensive split is the first, which moves whatever history
// exists into a single partition covering everything up to today.
func (s monStatements) KeepSampleDays(ctx context.Context, today, cutoff time.Time, keepAll bool) (created, dropped []string, perr error) {
	parts, perr := listPartitions(ctx, s.q)
	if perr == nil {
		created, perr = ensureAhead(ctx, s.q, parts, today)
	}
	if perr == nil {
		parts, perr = listPartitions(ctx, s.q)
	}
	if perr == nil {
		dropCutoff := toDays(cutoff)
		if keepAll {
			dropCutoff = 0 // keep every day until its summary is written
		}
		dropped, perr = dropExpired(ctx, s.q, parts, dropCutoff)
	}
	return created, dropped, perr
}

// toDays is MySQL's TO_DAYS for a UTC calendar day.
func toDays(day time.Time) int64 {
	d := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	return toDaysEpoch + d.Unix()/86400
}

func partitionName(day time.Time) string { return "p" + day.UTC().Format("20060102") }

type partition struct {
	name  string
	bound int64 // TO_DAYS bound; -1 for MAXVALUE
}

func listPartitions(ctx context.Context, conn monQueryer) ([]partition, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT PARTITION_NAME, PARTITION_DESCRIPTION
		  FROM information_schema.PARTITIONS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'monitor_samples'
		   AND PARTITION_NAME IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []partition
	for rows.Next() {
		var name string
		var desc sql.NullString
		if err := rows.Scan(&name, &desc); err != nil {
			return nil, err
		}
		p := partition{name: name, bound: -1}
		if desc.Valid && !strings.EqualFold(desc.String, "MAXVALUE") {
			if b, err := strconv.ParseInt(desc.String, 10, 64); err == nil {
				p.bound = b
			}
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("monitor_samples is not partitioned")
	}
	return out, rows.Err()
}

// ensureAhead splits p_max so today and the next days each have a partition.
// Range bounds must strictly increase, so only days past the highest existing
// bound are added — which also makes a second run a no-op.
func ensureAhead(ctx context.Context, conn monQueryer, parts []partition, today time.Time) ([]string, error) {
	var highest int64
	hasMax := false
	for _, p := range parts {
		if p.bound < 0 {
			hasMax = p.name == "p_max"
			continue
		}
		if p.bound > highest {
			highest = p.bound
		}
	}
	if !hasMax {
		return nil, fmt.Errorf("monitor_samples has no p_max partition to split")
	}
	var defs, names []string
	for i := 0; i < partitionsAhead; i++ {
		day := today.AddDate(0, 0, i)
		bound := toDays(day.AddDate(0, 0, 1)) // partition for `day` holds everything before the next day
		if bound <= highest {
			continue
		}
		defs = append(defs, fmt.Sprintf("PARTITION %s VALUES LESS THAN (%d)", partitionName(day), bound))
		names = append(names, partitionName(day))
		highest = bound
	}
	if len(defs) == 0 {
		return nil, nil
	}
	ddl := "ALTER TABLE monitor_samples REORGANIZE PARTITION p_max INTO (" +
		strings.Join(defs, ", ") + ", PARTITION p_max VALUES LESS THAN MAXVALUE)"
	if _, err := conn.ExecContext(ctx, ddl); err != nil {
		return nil, err
	}
	return names, nil
}

// dropExpired drops every day partition whose rows are all older than the
// cutoff. p_start and p_max are structural and never dropped.
func dropExpired(ctx context.Context, conn monQueryer, parts []partition, cutoffDays int64) ([]string, error) {
	var drop []string
	for _, p := range parts {
		if p.name == "p_start" || p.name == "p_max" || p.bound < 0 {
			continue
		}
		if p.bound <= cutoffDays {
			drop = append(drop, p.name)
		}
	}
	if len(drop) == 0 {
		return nil, nil
	}
	sort.Strings(drop)
	if _, err := conn.ExecContext(ctx,
		"ALTER TABLE monitor_samples DROP PARTITION "+strings.Join(drop, ", ")); err != nil {
		return nil, err
	}
	return drop, nil
}

// ---- events ----

func (s monStatements) AddEvent(ctx context.Context, e dal.MonitorEventRow) (int64, bool, error) {
	res, err := s.q.ExecContext(ctx, stMonAddEvent.SQL,
		e.Deployment, e.At, e.Kind, e.Phase, e.Severity, e.Subject, e.Detail, e.IntervalSec)
	if err != nil {
		return 0, false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return 0, false, nil
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, true, nil
	}
	return id, true, nil
}

func (s monStatements) SetEventContext(ctx context.Context, id int64, gz []byte) error {
	_, err := s.q.ExecContext(ctx, stMonEventContext.SQL, gz, id)
	return err
}

func (s monStatements) Events(ctx context.Context, from, to time.Time, deployment string, limit int) ([]dal.MonitorEvent, error) {
	rows, err := s.q.QueryContext(ctx, stMonEvents.SQL, from.UTC(), to.UTC(), deployment, deployment, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.MonitorEvent
	for rows.Next() {
		var e dal.MonitorEvent
		if err := rows.Scan(&e.ID, &e.Deployment, &e.At, &e.Kind, &e.Phase, &e.Severity, &e.Subject, &e.Detail,
			&e.IntervalSec, &e.HasContext); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s monStatements) Event(ctx context.Context, id int64) (dal.MonitorEvent, []byte, error) {
	var e dal.MonitorEvent
	var ctxBlob []byte
	err := s.q.QueryRowContext(ctx, stMonEvent.SQL, id).Scan(&e.ID, &e.Deployment, &e.At, &e.Kind, &e.Phase, &e.Severity,
		&e.Subject, &e.Detail, &e.IntervalSec, &ctxBlob)
	return e, ctxBlob, err
}

func (s monStatements) DeleteEventsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	r, err := s.q.ExecContext(ctx, stMonDeleteEvents.SQL, cutoff)
	if err != nil {
		return 0, err
	}
	d, _ := r.RowsAffected()
	return d, nil
}

// ---- day summaries ----

func (s monStatements) DeploymentsOn(ctx context.Context, from, to time.Time) ([]string, error) {
	rs, err := s.q.QueryContext(ctx, stMonDeployments.SQL, from, to)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var d string
		if err := rs.Scan(&d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rs.Err()
}

func (s monStatements) DayKept(ctx context.Context, deployment, day string) (int, error) {
	var n int
	err := s.q.QueryRowContext(ctx, stMonDayKept.SQL, deployment, day).Scan(&n)
	return n, err
}

// dayWhere is a day's WHERE for one deployment ("" = the reports that name
// none) and its arguments, after `extra`.
func dayWhere(dep string, from, to time.Time, extra ...any) (string, []any) {
	depCond, depArgs := "deployment_id = ?", []any{dep}
	if dep == "" {
		depCond, depArgs = "deployment_id IS NULL", nil
	}
	args := append(append(append([]any{}, extra...), depArgs...), from, to)
	return " WHERE " + depCond + " AND sampled_at >= ? AND sampled_at < ?", args
}

func (s monStatements) DayPush(ctx context.Context, dep string, from, to time.Time) (dal.NullInt64, error) {
	where, args := dayWhere(dep, from, to)
	var p sql.NullInt64
	err := s.q.QueryRowContext(ctx, "SELECT push_sec FROM monitor_samples"+where+
		" GROUP BY push_sec ORDER BY COUNT(*) DESC LIMIT 1", args...).Scan(&p)
	return dal.NullInt64{Int64: p.Int64, Valid: p.Valid}, err
}

// DayCounts reads each process's counters. As before the move, a read cut
// short after its last row is not reported (rows.Err is not asked); the same
// for DayIntervals, DayRoles and DayEvents.
func (s monStatements) DayCounts(ctx context.Context, dep string, from, to time.Time) ([]dal.MonitorDayCount, error) {
	where, args := dayWhere(dep, from, to)
	rs, err := s.q.QueryContext(ctx, `SELECT COUNT(*), MAX(accepted)-MIN(accepted), MAX(rejected)-MIN(rejected)
			FROM monitor_samples`+where+` GROUP BY role, instance`, args...)
	if err != nil {
		return nil, err
	}
	var out []dal.MonitorDayCount
	for rs.Next() {
		var c dal.MonitorDayCount
		if err := rs.Scan(&c.N, &c.Accepted, &c.Rejected); err != nil {
			rs.Close()
			return nil, err
		}
		out = append(out, c)
	}
	rs.Close()
	return out, nil
}

func (s monStatements) DayIntervals(ctx context.Context, dep string, from, to time.Time, push int) ([]dal.MonitorDayInterval, error) {
	where, args := dayWhere(dep, from, to, push)
	rs, err := s.q.QueryContext(ctx, `SELECT b, SUM(tin), SUM(tout), SUM(trc), SUM(tmo), MAX(qd), MAX(qa), MAX(lat), COUNT(*)
			FROM (SELECT FLOOR(TIMESTAMPDIFF(SECOND, '1970-01-01', sampled_at) / ?) AS b, role, instance,
			             AVG(tps) tin, AVG(tps_out) tout, AVG(tps_receipts) trc, AVG(tps_mo) tmo,
			             MAX(queue_depth) qd, MAX(queue_age_sec) qa, MAX(latency_p95_ms) lat
			        FROM monitor_samples`+where+` GROUP BY b, role, instance) x
			GROUP BY b ORDER BY b`, args...)
	if err != nil {
		return nil, err
	}
	var out []dal.MonitorDayInterval
	for rs.Next() {
		var x dal.MonitorDayInterval
		if err := rs.Scan(&x.B, &x.In, &x.Out, &x.Receipts, &x.MO, &x.Depth, &x.Age, &x.Latency, &x.Instances); err != nil {
			rs.Close()
			return nil, err
		}
		out = append(out, x)
	}
	rs.Close()
	return out, nil
}

func (s monStatements) DayRoles(ctx context.Context, dep string, from, to time.Time, push int) ([]dal.MonitorDayRole, error) {
	where, args := dayWhere(dep, from, to, push)
	rs, err := s.q.QueryContext(ctx, `SELECT role, COUNT(DISTINCT FLOOR(TIMESTAMPDIFF(SECOND, '1970-01-01', sampled_at) / ?))
			FROM monitor_samples`+where+` GROUP BY role`, args...)
	if err != nil {
		return nil, err
	}
	var out []dal.MonitorDayRole
	for rs.Next() {
		var r dal.MonitorDayRole
		if err := rs.Scan(&r.Role, &r.N); err != nil {
			rs.Close()
			return nil, err
		}
		out = append(out, r)
	}
	rs.Close()
	return out, nil
}

// isUnknownColumnErr is mtqueue.go's (the same test the old code made).

func (s monStatements) DayQueues(ctx context.Context, dep string, from, to time.Time, fn func(inst string, at time.Time, raw []byte) error) error {
	where, args := dayWhere(dep, from, to)
	rs, err := s.q.QueryContext(ctx, `SELECT CONCAT(role, '|', instance), sampled_at, queues FROM monitor_samples`+
		where+` AND queues IS NOT NULL ORDER BY sampled_at`, args...)
	if err != nil {
		if isUnknownColumnErr(err) {
			return nil // a store from before hot/0055
		}
		return err
	}
	defer rs.Close()
	for rs.Next() {
		var inst string
		var at time.Time
		var raw []byte
		if err := rs.Scan(&inst, &at, &raw); err != nil {
			return err
		}
		if err := fn(inst, at, raw); err != nil {
			return err
		}
	}
	return rs.Err()
}

func (s monStatements) DayStores(ctx context.Context, dep string, from, to time.Time, fn func(at time.Time, raw []byte) error) error {
	where, args := dayWhere(dep, from, to)
	rs, err := s.q.QueryContext(ctx, `SELECT sampled_at, stores FROM monitor_samples`+
		where+` AND stores IS NOT NULL ORDER BY sampled_at`, args...)
	if err != nil {
		if isUnknownColumnErr(err) {
			return nil // a store from before hot/0056
		}
		return err
	}
	defer rs.Close()
	for rs.Next() {
		var at time.Time
		var raw []byte
		if err := rs.Scan(&at, &raw); err != nil {
			return err
		}
		if err := fn(at, raw); err != nil {
			return err
		}
	}
	return rs.Err()
}

func (s monStatements) DayEvents(ctx context.Context, dep string, from, to time.Time) ([]dal.MonitorDayEvent, error) {
	rs, err := s.q.QueryContext(ctx, stMonDayEvents.SQL, dep, from, to)
	if err != nil {
		return nil, err
	}
	var out []dal.MonitorDayEvent
	for rs.Next() {
		var e dal.MonitorDayEvent
		if err := rs.Scan(&e.ID, &e.At, &e.Kind, &e.Phase, &e.Severity, &e.Subject, &e.Detail); err != nil {
			rs.Close()
			return nil, err
		}
		out = append(out, e)
	}
	rs.Close()
	return out, nil
}

// nullAt is a peak time as written: NULL when there is none.
func nullAt(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

func (s monStatements) AddDay(ctx context.Context, d dal.MonitorDay) error {
	_, err := s.q.ExecContext(ctx, stMonAddDay.SQL,
		d.Deployment, d.Day, d.IntervalSec, d.Samples, d.Accepted, d.Rejected, d.Sent, d.Receipts, d.MO,
		d.PeakTPSIn, nullAt(d.PeakTPSInAt), d.PeakTPSOut, nullAt(d.PeakTPSOutAt), d.PeakQueueDepth, nullAt(d.PeakQueueDepthAt),
		d.PeakQueueAge, nullAt(d.PeakQueueAgeAt), d.PeakLatency, nullAt(d.PeakLatencyAt),
		d.EventsCritical, d.EventsWarning, d.EventsInfo, d.Body)
	return err
}

func (s monStatements) DaySummaries(ctx context.Context, fromDay, toDay, deployment string) ([][]byte, error) {
	rows, err := s.q.QueryContext(ctx, stMonDaySummaries.SQL, fromDay, toDay, deployment, deployment)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		out = append(out, body)
	}
	return out, rows.Err()
}

func (s monStatements) DeleteDaysBefore(ctx context.Context, day string) (int64, error) {
	r, err := s.q.ExecContext(ctx, stMonDeleteDays.SQL, day)
	if err != nil {
		return 0, err
	}
	d, _ := r.RowsAffected()
	return d, nil
}

// ---- state ----

func (s monStatements) State(ctx context.Context, name string) (string, error) {
	var v string
	err := s.q.QueryRowContext(ctx, stMonState.SQL, name).Scan(&v)
	return v, err
}

func (s monStatements) SetState(ctx context.Context, name, value string) error {
	_, err := s.q.ExecContext(ctx, stMonSetState.SQL, name, value)
	return err
}

// keep the representative forms referenced (they are the lint test's).
var _ = []*stmt.Stmt{stMonAddSample, stMonSamples, stMonBodies, stMonMoment, stMonSeries, stMonDeleteSamples,
	stMonPartitions, stMonSplitMax, stMonDropParts, stMonDayPush, stMonDayCounts, stMonDayIntervals, stMonDayRoles,
	stMonDayQueues, stMonDayStores}
