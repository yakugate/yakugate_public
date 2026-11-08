package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	mssqldrv "github.com/microsoft/go-mssqldb"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// The monitoring history in the hot store on SQL Server (monitor_samples,
// monitor_events, monitor_days, monitor_state), made like the MySQL DAL's
// internal/dal/mysql/monitoring.go: the same statements, written for SQL
// Server.
//
// What differs, and why:
//   - A SQL Server store starts at the head of the MySQL schema
//     (migrations/hot/mssql/0001_base.sql), so every column the MySQL code
//     falls back without ("Unknown column", a store older than hot/0053–0056)
//     is there: the reads and writes have one form each.
//   - monitor_samples is not partitioned on SQL Server. KeepSampleDays keeps
//     no partitions (it answers none made and none dropped), and the old
//     reports go by DeleteSamplesBefore, in batches of the size asked
//     (DELETE TOP), which internal/monitor runs after it every hour.
//   - The named lock is an application lock (sp_getapplock) owned by the
//     session on a pinned connection, as GET_LOCK is on MySQL. It belongs to
//     the database, so its name needs no database in it.
//   - JSON is read with JSON_VALUE / OPENJSON / JSON_PATH_EXISTS.

// MonitorStmts is every statement of the history, by name. The series'
// statements are built from their JSON paths at run time and registered in
// one representative form, as on MySQL.
var MonitorStmts = newRegistry()

// openMonitorHistory is the history's opener: a small pool of its own on the
// hot store — a report every few seconds must never compete with the
// traffic for connections.
func openMonitorHistory(cfg config.Config) (dal.MonitorHistory, error) {
	db, err := historyPool(cfg.HotDSN)
	if err != nil {
		return nil, err
	}
	return NewMonitorHistory(db), nil
}

// historyPool is a pool of two connections of its own on dsn, named for the
// process as Get's are.
func historyPool(dsn string) (*sql.DB, error) {
	c, err := mssqldrv.NewConnector(named(dsn, dbpool.Program()))
	if err != nil {
		return nil, fmt.Errorf("mssql: %w", err)
	}
	db := sql.OpenDB(c)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(time.Minute)
	return db, nil
}

const sampleCols = `COALESCE(deployment_id, N''), role, instance, sampled_at, accepted, rejected,
		tps, tps_out, tps_rejected, latency_p95_ms, cpu_pct, mem_pct, queue_depth, queue_age_sec,
		sessions_up, sessions_total, unhealthy, push_sec, queues, stores`

// dayWhereSQL is a day's WHERE for one deployment (dayWhere: "deployment_id
// IS NULL" for the reports that name none).
const dayWhereSQL = " WHERE deployment_id = @dep AND sampled_at >= @from AND sampled_at < @to"

// bucketOf is the bucket a report falls in, `@push` seconds wide, as a
// CROSS APPLY (SQL Server cannot GROUP BY an alias).
const bucketOf = " CROSS APPLY (SELECT DATEDIFF_BIG(SECOND, '19700101', sampled_at) / @push AS b) k"

var (
	stMonLock = MonitorStmts.Add("monitor.lock", `SET NOCOUNT ON;
DECLARE @yk_got INT;
EXEC @yk_got = sp_getapplock @Resource = @p1, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0, @DbPrincipal = 'public';
SELECT @yk_got`, false)
	stMonRelease = MonitorStmts.Add("monitor.release",
		"EXEC sp_releaseapplock @Resource = @p1, @LockOwner = 'Session', @DbPrincipal = 'public'", false)

	stMonAddSample = MonitorStmts.Add("monitor.add_sample", `INSERT INTO monitor_samples (sampled_at, role, instance,
		deployment_id, tier, state, accepted, rejected, tps, tps_out, tps_receipts, tps_mo, tps_rejected,
		latency_p95_ms, vendor_p95_ms, queue_depth, queue_age_sec, sessions_up, sessions_total, cpu_pct, mem_pct,
		unhealthy, push_sec, queues, stores, body)
		VALUES (@p1, @p2, @p3, @p4, 0, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12,
		@p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20, @p21, @p22, @p23, @p24, @p25)`, false)
	stMonSamples = MonitorStmts.Add("monitor.samples", "SELECT "+sampleCols+
		" FROM monitor_samples WHERE sampled_at >= @p1 AND sampled_at <= @p2 ORDER BY sampled_at", false)
	stMonNewest = MonitorStmts.Add("monitor.newest", `
		SELECT m.role, m.instance, m.body
		  FROM monitor_samples m
		  JOIN (SELECT instance, MAX(sampled_at) t FROM monitor_samples
		         WHERE tier = 0 AND sampled_at > @p1 GROUP BY instance) n
		    ON n.instance = m.instance AND n.t = m.sampled_at
		 WHERE m.tier = 0`, false)
	stMonNewestOf = MonitorStmts.Add("monitor.newest_of", `
		SELECT m.role, m.instance, m.body
		  FROM monitor_samples m
		  JOIN (SELECT instance, MAX(sampled_at) t FROM monitor_samples
		         WHERE tier = 0 AND sampled_at > @p1 AND deployment_id = @p2 GROUP BY instance) n
		    ON n.instance = m.instance AND n.t = m.sampled_at
		 WHERE m.tier = 0 AND m.deployment_id = @p2`, false)
	stMonNear = MonitorStmts.Add("monitor.near", `SELECT id, role, instance, sampled_at, push_sec FROM monitor_samples
		WHERE sampled_at BETWEEN @p1 AND @p2 AND (@p3 = N'' OR deployment_id = @p3)`, false)
	// The ids come as one JSON array: one parameter, however many (SQL
	// Server takes at most 2,100 parameters a statement).
	stMonBodies = MonitorStmts.Add("monitor.bodies", `SELECT role, instance, body FROM monitor_samples
		WHERE id IN (SELECT CAST(value AS BIGINT) FROM OPENJSON(@p1))`, false)
	stMonMoment = MonitorStmts.Add("monitor.moment", `SELECT role, instance, sampled_at, body FROM monitor_samples
		WHERE deployment_id = @dep AND sampled_at >= @from AND sampled_at <= @to`, false)
	stMonMomentNone = MonitorStmts.Add("monitor.moment_none", `SELECT role, instance, sampled_at, body FROM monitor_samples
		WHERE deployment_id IS NULL AND sampled_at >= @from AND sampled_at <= @to`, false)
	stMonAnswered = MonitorStmts.Add("monitor.answered", `SELECT COUNT(*), COALESCE(SUM(CASE WHEN newest >= @p1 THEN 1 ELSE 0 END), 0)
			FROM (SELECT MAX(sampled_at) AS newest FROM monitor_samples
			       WHERE sampled_at >= @p2 AND (@p3 = N'' OR deployment_id = @p3) GROUP BY role, instance) x`, false)
	stMonSeries = MonitorStmts.Add("monitor.series", fmt.Sprintf(seriesSQL,
		"MAX(waiting)", "AVG(w) AS waiting", monJSONNum("queues", `$.mt."q".w`)+" AS w", "queues", ""), false)
	stMonDeleteSamples = MonitorStmts.Add("monitor.delete_samples",
		"DELETE TOP (@p2) FROM monitor_samples WHERE sampled_at < @p1", false)

	stMonAddEvent = MonitorStmts.Add("monitor.add_event", `SET NOCOUNT ON;
INSERT INTO monitor_events (deployment_id, [at], kind, phase, severity, subject, detail, interval_sec)
OUTPUT INSERTED.id
SELECT @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8
 WHERE NOT EXISTS (SELECT 1 FROM monitor_events WITH (UPDLOCK, HOLDLOCK)
                    WHERE deployment_id = @p1 AND kind = @p3 AND phase = @p4 AND subject = @p6 AND [at] = @p2)`, false)
	stMonEventContext = MonitorStmts.Add("monitor.event_context", "UPDATE monitor_events SET context = @p1 WHERE id = @p2", false)
	stMonEvents       = MonitorStmts.Add("monitor.events", `SELECT TOP (@p4) id, deployment_id, [at], kind, phase, severity, subject, detail, interval_sec,
			CAST(CASE WHEN context IS NULL THEN 0 ELSE 1 END AS BIT) FROM monitor_events
		 WHERE [at] >= @p1 AND [at] <= @p2 AND (@p3 = N'' OR deployment_id = @p3) ORDER BY [at] DESC, id DESC`, false)
	stMonEvent = MonitorStmts.Add("monitor.event", `SELECT id, deployment_id, [at], kind, phase, severity, subject, detail, interval_sec, context
		FROM monitor_events WHERE id = @p1`, false)
	stMonDeleteEvents = MonitorStmts.Add("monitor.delete_events", "DELETE TOP (1000) FROM monitor_events WHERE [at] < @p1", false)

	stMonDeployments = MonitorStmts.Add("monitor.day_deployments", `SELECT DISTINCT COALESCE(deployment_id, N'') FROM monitor_samples
		WHERE sampled_at >= @p1 AND sampled_at < @p2`, false)
	stMonDayKept = MonitorStmts.Add("monitor.day_kept",
		"SELECT COUNT(*) FROM monitor_days WHERE deployment_id = @p1 AND [day] = CAST(@p2 AS DATE)", false)
	stMonDayPush = MonitorStmts.Add("monitor.day_push", "SELECT TOP (1) push_sec FROM monitor_samples"+dayWhereSQL+
		" GROUP BY push_sec ORDER BY COUNT(*) DESC", false)
	stMonDayCounts = MonitorStmts.Add("monitor.day_counts", `SELECT COUNT(*), MAX(accepted)-MIN(accepted), MAX(rejected)-MIN(rejected)
			FROM monitor_samples`+dayWhereSQL+` GROUP BY role, instance`, false)
	stMonDayIntervals = MonitorStmts.Add("monitor.day_intervals", `SELECT b, SUM(tin), SUM(tout), SUM(trc), SUM(tmo), MAX(qd), MAX(qa), MAX(lat), COUNT(*)
			FROM (SELECT k.b, role, instance,
			             AVG(tps) tin, AVG(tps_out) tout, AVG(tps_receipts) trc, AVG(tps_mo) tmo,
			             MAX(queue_depth) qd, MAX(queue_age_sec) qa, MAX(latency_p95_ms) lat
			        FROM monitor_samples`+bucketOf+dayWhereSQL+` GROUP BY k.b, role, instance) x
			GROUP BY b ORDER BY b`, false)
	stMonDayRoles = MonitorStmts.Add("monitor.day_roles", `SELECT role, COUNT(DISTINCT k.b)
			FROM monitor_samples`+bucketOf+dayWhereSQL+` GROUP BY role`, false)
	stMonDayQueues = MonitorStmts.Add("monitor.day_queues", `SELECT CONCAT(role, N'|', instance), sampled_at, queues FROM monitor_samples`+
		dayWhereSQL+` AND queues IS NOT NULL ORDER BY sampled_at`, false)
	stMonDayStores = MonitorStmts.Add("monitor.day_stores", `SELECT sampled_at, stores FROM monitor_samples`+
		dayWhereSQL+` AND stores IS NOT NULL ORDER BY sampled_at`, false)
	stMonDayEvents = MonitorStmts.Add("monitor.day_events", `SELECT id, [at], kind, phase, severity, subject, detail FROM monitor_events
			WHERE deployment_id = @p1 AND [at] >= @p2 AND [at] < @p3 ORDER BY [at], id`, false)
	stMonAddDay = MonitorStmts.Add("monitor.add_day", `INSERT INTO monitor_days
		(deployment_id, [day], interval_sec, samples, accepted, rejected, sent, receipts, mo,
		 peak_tps_in, peak_tps_in_at, peak_tps_out, peak_tps_out_at, peak_queue_depth, peak_queue_depth_at,
		 peak_queue_age_sec, peak_queue_age_at, peak_latency_ms, peak_latency_at,
		 events_critical, events_warning, events_info, body)
		SELECT @p1, CAST(@p2 AS DATE), @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15,
		       @p16, @p17, @p18, @p19, @p20, @p21, @p22, @p23
		 WHERE NOT EXISTS (SELECT 1 FROM monitor_days WITH (UPDLOCK, HOLDLOCK)
		                    WHERE deployment_id = @p1 AND [day] = CAST(@p2 AS DATE))`, false)
	stMonDaySummaries = MonitorStmts.Add("monitor.day_summaries", `SELECT body FROM monitor_days
		WHERE [day] >= CAST(@p1 AS DATE) AND [day] <= CAST(@p2 AS DATE)
		AND (@p3 = N'' OR deployment_id = @p3) ORDER BY [day] DESC, deployment_id`, false)
	stMonDeleteDays = MonitorStmts.Add("monitor.delete_days",
		"DELETE TOP (1000) FROM monitor_days WHERE [day] < CAST(@p1 AS DATE)", false)

	stMonState    = MonitorStmts.Add("monitor.state", "SELECT value FROM monitor_state WHERE name = @p1", false)
	stMonSetState = AddUpsert(MonitorStmts, "monitor.set_state", UpsertSpec{
		Table: "monitor_state", Keys: []string{"name"},
		// updated_at is ON UPDATE CURRENT_TIMESTAMP on MySQL: set here.
		Set:          []string{"[value] = $value", "updated_at = SYSUTCDATETIME()"},
		Insert:       []string{"name", "value", "updated_at"},
		InsertValues: []string{"", "", "SYSUTCDATETIME()"},
	}, false)
)

// seriesSQL is a three-level series query over one JSON column: each
// report's figures, then per instance per bucket, then per bucket (outer,
// per instance, per report, the column, an extra condition). SQL Server
// cannot aggregate a subquery (OPENJSON), so a report's figures are read
// first.
const seriesSQL = `SELECT b, %s FROM (
		SELECT b, role, instance, %s FROM (
			SELECT DATEDIFF_BIG(SECOND, '19700101', sampled_at) / @p1 AS b, role, instance, %s
			  FROM monitor_samples WHERE sampled_at >= @p2 AND sampled_at < @p3 AND %s IS NOT NULL%s
			   AND (@p4 = N'' OR deployment_id = @p4)) r
		 GROUP BY b, role, instance) x GROUP BY b ORDER BY b`

// monJSONNum is a JSON value as a number, as MySQL's JSON_EXTRACT(…) + 0 reads
// it: a number, true 1, false 0, anything else NULL.
func monJSONNum(col, path string) string {
	v := fmt.Sprintf("JSON_VALUE(%s, '%s')", col, strings.ReplaceAll(path, "'", "''"))
	return fmt.Sprintf("COALESCE(TRY_CAST(%[1]s AS FLOAT), CASE %[1]s WHEN 'true' THEN 1e0 WHEN 'false' THEN 0e0 END)", v)
}

// MonitorHistory is the history on a hot-store pool.
type MonitorHistory struct {
	monStatements
	db *sql.DB
}

// monStatements runs the statements on a pool or on one connection.
type monStatements struct{ q Querier }

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

// Locked takes the named lock on a connection of its own, without waiting,
// and lets it go after fn. The lock is the session's, so it lives on that
// one connection; a process that dies lets go of it with its session.
func (h *MonitorHistory) Locked(ctx context.Context, name string, fn func(dal.MonitorStatements) error) (bool, error) {
	conn, err := h.db.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	res := appLockName(name)
	var got int
	if err := conn.QueryRowContext(ctx, stMonLock.SQL, res).Scan(&got); err != nil {
		dropConn(conn) // the lock may have been granted before the error
		return false, err
	}
	if got < 0 {
		return false, nil // -1 held by another; -2/-3/-999 could not say
	}
	defer func() {
		// Background context: the caller's may be cancelled already.
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(c, stMonRelease.SQL, res); err != nil {
			dropConn(conn) // never back to the pool still holding the lock
		}
	}()
	return true, fn(monStatements{q: conn})
}

// appLockName is a name sp_getapplock takes (at most 255 characters): the
// name itself, or its start and a hash of the whole.
func appLockName(name string) string {
	if len(name) <= 255 {
		return name
	}
	return leaseName(name)
}

// ---- reports ----

// monNullText is a JSON text column's value: NULL when there is none.
func monNullText(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

func (s monStatements) AddSample(ctx context.Context, x dal.MonitorSample) error {
	_, err := s.q.ExecContext(ctx, stMonAddSample.SQL,
		Time(x.At), x.Role, x.Instance, x.Deployment, Varchar(x.State), x.Accepted, x.Rejected,
		x.TPS, x.TPSOut, x.TPSReceipts, x.TPSMO, x.TPSRejected, x.LatencyP95, x.VendorP95,
		x.QueueDepth, x.QueueAge, x.SessionsUp, x.SessionsTotal, x.CPU, x.Mem, x.Unhealthy,
		x.PushSec, monNullText(x.Queues), monNullText(x.Stores), x.Body)
	return err
}

// Samples reads the lifted columns of every report in [from, to].
func (s monStatements) Samples(ctx context.Context, from, to time.Time) ([]dal.MonitorSampleRow, error) {
	rs, err := s.q.QueryContext(ctx, stMonSamples.SQL, Time(from), Time(to))
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []dal.MonitorSampleRow
	for rs.Next() {
		var x dal.MonitorSampleRow
		var queues, stores sql.NullString
		if err := rs.Scan(&x.Deployment, &x.Role, &x.Instance, &x.At, &x.Accepted, &x.Rejected,
			&x.TPS, &x.TPSOut, &x.TPSRejected, &x.Latency, &x.CPU, &x.Mem, &x.QueueDepth, &x.QueueAge,
			&x.Up, &x.Total, &x.Unhealthy, &x.Push, &queues, &stores); err != nil {
			return nil, err
		}
		x.Queues, x.Stores = monTextBytes(queues), monTextBytes(stores)
		out = append(out, x)
	}
	return out, rs.Err()
}

// monTextBytes is a text column as the bytes MySQL's JSON column gave (nil for
// NULL).
func monTextBytes(s sql.NullString) []byte {
	if !s.Valid {
		return nil
	}
	return []byte(s.String)
}

func (s monStatements) Newest(ctx context.Context, since time.Time) ([]dal.MonitorBody, error) {
	rows, err := s.q.QueryContext(ctx, stMonNewest.SQL, Time(since))
	if err != nil {
		return nil, err
	}
	return monReadBodies(rows)
}

func (s monStatements) NewestOf(ctx context.Context, since time.Time, deployment string) ([]dal.MonitorBody, error) {
	rows, err := s.q.QueryContext(ctx, stMonNewestOf.SQL, Time(since), deployment)
	if err != nil {
		return nil, err
	}
	return monReadBodies(rows)
}

func monReadBodies(rows *sql.Rows) ([]dal.MonitorBody, error) {
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

// Near reads the reports near a moment. As on MySQL, a read cut short after
// its last row is not reported (rows.Err is not asked).
func (s monStatements) Near(ctx context.Context, from, to time.Time, deployment string) ([]dal.MonitorSampleRef, error) {
	rows, err := s.q.QueryContext(ctx, stMonNear.SQL, Time(from), Time(to), deployment)
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
	if ids == nil {
		ids = []int64{}
	}
	list, _ := json.Marshal(ids)
	rs, err := s.q.QueryContext(ctx, stMonBodies.SQL, string(list))
	if err != nil {
		return nil, err
	}
	return monReadBodies(rs)
}

// Moment reads a deployment's reports in [from, to], whole.
func (s monStatements) Moment(ctx context.Context, dep string, from, to time.Time) ([]dal.MonitorBody, error) {
	q := stMonMoment.SQL
	if dep == "" {
		q = stMonMomentNone.SQL
	}
	rs, err := s.q.QueryContext(ctx, q, sql.Named("dep", dep), sql.Named("from", Time(from)), sql.Named("to", Time(to)))
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
	err = s.q.QueryRowContext(ctx, stMonAnswered.SQL, Time(since), Time(recent), deployment).Scan(&of, &answered)
	return of, answered, err
}

// QueueSeries is one queue over [from, to): waiting / ready / held / oldest
// (the worst any instance reported, only from reports that carry that kind's
// depths) and in / out a second (added up).
func (s monStatements) QueueSeries(ctx context.Context, kind, name string, from, to time.Time, deployment string, bucket int) ([]dal.SeriesPoint, error) {
	path := func(f string) string { return fmt.Sprintf(`$.%s."%s".%s`, kind, name, f) }
	val := func(f string) string { return fmt.Sprintf("COALESCE(%s, 0)", monJSONNum("queues", path(f))) }
	// The report carries this kind's depths: its "d" list names the kind.
	has := fmt.Sprintf(`CASE WHEN EXISTS (SELECT 1 FROM OPENJSON(queues, '$.d') WHERE value = N'%s') THEN 1 ELSE 0 END`,
		strings.ReplaceAll(kind, "'", "''"))
	row := fmt.Sprintf(`%s AS has, %s AS w, %s AS rd, %s AS h, %s AS a, %s AS i, %s AS o`,
		has, val("w"), val("r"), val("h"), val("a"), val("i"), val("o"))
	inst := `AVG(CASE WHEN has = 1 THEN w END) AS waiting, AVG(CASE WHEN has = 1 THEN rd END) AS ready,
		AVG(CASE WHEN has = 1 THEN h END) AS held, MAX(a) AS oldest, AVG(i) AS tin, AVG(o) AS tout`
	return s.bucketed(ctx, "queues", row, inst, `MAX(waiting), MAX(ready), MAX(held), MAX(oldest), SUM(tin), SUM(tout)`,
		[]string{"waiting", "ready", "held", "oldest_sec", "in_per_sec", "out_per_sec"}, from, to, deployment, bucket, "")
}

// StoreSeries is one MySQL or Redis server over [from, to): reachable (the
// lowest), health (the worst: 0 ok, 1 warning, 2 down), round trip,
// connections % (MySQL) or memory % (Redis), queries or commands a second.
func (s monStatements) StoreSeries(ctx context.Context, key string, from, to time.Time, deployment string, bucket int) ([]dal.SeriesPoint, error) {
	f := func(n string) string { return monJSONNum("stores", fmt.Sprintf(`$."%s".%s`, key, n)) }
	row := fmt.Sprintf(`%s AS ok, %s AS w, %s AS p, GREATEST(COALESCE(%s, 0), COALESCE(%s, 0)) AS used, %s AS q`,
		f("ok"), f("w"), f("p"), f("c"), f("m"), f("q"))
	inst := `MIN(ok) AS ok, MAX(w) AS w, MAX(p) AS p, MAX(used) AS used, AVG(q) AS q`
	return s.bucketed(ctx, "stores", row, inst, `MIN(ok), MAX(w), MAX(p), MAX(used), MAX(q)`,
		[]string{"ok", "health", "ping_ms", "used_pct", "per_sec"}, from, to, deployment, bucket,
		fmt.Sprintf(` AND JSON_PATH_EXISTS(stores, '$."%s"') = 1`, strings.ReplaceAll(key, "'", "''")))
}

// bucketed runs a series query over one JSON column.
func (s monStatements) bucketed(ctx context.Context, column, row, inst, outer string, names []string, from, to time.Time,
	deployment string, bucket int, extra string) ([]dal.SeriesPoint, error) {
	q := fmt.Sprintf(seriesSQL, outer, inst, row, column, extra)
	start := time.Now()
	rows, err := s.q.QueryContext(ctx, q, bucket, Time(from), Time(to), deployment)
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
	observe(stMonSeries, start, int64(len(out)))
	return out, rows.Err()
}

func (s monStatements) DeleteSamplesBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	r, err := s.q.ExecContext(ctx, stMonDeleteSamples.SQL, Time(cutoff), limit)
	if err != nil {
		return 0, err
	}
	n, _ := r.RowsAffected()
	return n, nil
}

// KeepSampleDays keeps no partitions: monitor_samples is one table on SQL
// Server (see the top of this file), so there is nothing to make ahead or to
// drop, and a second pass does nothing either. The days that have aged out
// are deleted by DeleteSamplesBefore.
func (s monStatements) KeepSampleDays(ctx context.Context, today, cutoff time.Time, keepAll bool) (created, dropped []string, err error) {
	return nil, nil, nil
}

// ---- events ----

// AddEvent writes the event unless it is there (MySQL's INSERT IGNORE on
// uq_monitor_event). Two writers of one event at once: the second waits on
// the first's key lock, then finds the row; a duplicate key that still gets
// through is "there already".
func (s monStatements) AddEvent(ctx context.Context, e dal.MonitorEventRow) (int64, bool, error) {
	var id int64
	err := s.q.QueryRowContext(ctx, stMonAddEvent.SQL,
		e.Deployment, Time(e.At), e.Kind, Varchar(e.Phase), Varchar(e.Severity), e.Subject, e.Detail, e.IntervalSec).Scan(&id)
	switch {
	case err == sql.ErrNoRows || IsDuplicate(err):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	return id, true, nil
}

func (s monStatements) SetEventContext(ctx context.Context, id int64, gz []byte) error {
	_, err := s.q.ExecContext(ctx, stMonEventContext.SQL, gz, id)
	return err
}

func (s monStatements) Events(ctx context.Context, from, to time.Time, deployment string, limit int) ([]dal.MonitorEvent, error) {
	rows, err := s.q.QueryContext(ctx, stMonEvents.SQL, Time(from), Time(to), deployment, limit)
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
	r, err := s.q.ExecContext(ctx, stMonDeleteEvents.SQL, Time(cutoff))
	if err != nil {
		return 0, err
	}
	d, _ := r.RowsAffected()
	return d, nil
}

// ---- day summaries ----

func (s monStatements) DeploymentsOn(ctx context.Context, from, to time.Time) ([]string, error) {
	rs, err := s.q.QueryContext(ctx, stMonDeployments.SQL, Time(from), Time(to))
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

// dayQuery is a day's statement for one deployment ("" = the reports that
// name none: deployment_id IS NULL) and its named arguments.
func dayQuery(q, dep string, from, to time.Time, push ...int) (string, []any) {
	if dep == "" {
		q = strings.Replace(q, "deployment_id = @dep", "deployment_id IS NULL", 1)
	}
	args := []any{sql.Named("dep", dep), sql.Named("from", Time(from)), sql.Named("to", Time(to))}
	if len(push) > 0 {
		args = append(args, sql.Named("push", push[0]))
	}
	return q, args
}

func (s monStatements) DayPush(ctx context.Context, dep string, from, to time.Time) (dal.NullInt64, error) {
	q, args := dayQuery(stMonDayPush.SQL, dep, from, to)
	var p sql.NullInt64
	err := s.q.QueryRowContext(ctx, q, args...).Scan(&p)
	return dal.NullInt64{Int64: p.Int64, Valid: p.Valid}, err
}

// DayCounts reads each process's counters. As on MySQL, a read cut short
// after its last row is not reported (rows.Err is not asked); the same for
// DayIntervals, DayRoles and DayEvents.
func (s monStatements) DayCounts(ctx context.Context, dep string, from, to time.Time) ([]dal.MonitorDayCount, error) {
	q, args := dayQuery(stMonDayCounts.SQL, dep, from, to)
	rs, err := s.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []dal.MonitorDayCount
	for rs.Next() {
		var c dal.MonitorDayCount
		if err := rs.Scan(&c.N, &c.Accepted, &c.Rejected); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func (s monStatements) DayIntervals(ctx context.Context, dep string, from, to time.Time, push int) ([]dal.MonitorDayInterval, error) {
	q, args := dayQuery(stMonDayIntervals.SQL, dep, from, to, push)
	rs, err := s.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []dal.MonitorDayInterval
	for rs.Next() {
		var x dal.MonitorDayInterval
		if err := rs.Scan(&x.B, &x.In, &x.Out, &x.Receipts, &x.MO, &x.Depth, &x.Age, &x.Latency, &x.Instances); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, nil
}

func (s monStatements) DayRoles(ctx context.Context, dep string, from, to time.Time, push int) ([]dal.MonitorDayRole, error) {
	q, args := dayQuery(stMonDayRoles.SQL, dep, from, to, push)
	rs, err := s.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []dal.MonitorDayRole
	for rs.Next() {
		var r dal.MonitorDayRole
		if err := rs.Scan(&r.Role, &r.N); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func (s monStatements) DayQueues(ctx context.Context, dep string, from, to time.Time, fn func(inst string, at time.Time, raw []byte) error) error {
	q, args := dayQuery(stMonDayQueues.SQL, dep, from, to)
	rs, err := s.q.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rs.Close()
	for rs.Next() {
		var inst, raw string
		var at time.Time
		if err := rs.Scan(&inst, &at, &raw); err != nil {
			return err
		}
		if err := fn(inst, at, []byte(raw)); err != nil {
			return err
		}
	}
	return rs.Err()
}

func (s monStatements) DayStores(ctx context.Context, dep string, from, to time.Time, fn func(at time.Time, raw []byte) error) error {
	q, args := dayQuery(stMonDayStores.SQL, dep, from, to)
	rs, err := s.q.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rs.Close()
	for rs.Next() {
		var at time.Time
		var raw string
		if err := rs.Scan(&at, &raw); err != nil {
			return err
		}
		if err := fn(at, []byte(raw)); err != nil {
			return err
		}
	}
	return rs.Err()
}

func (s monStatements) DayEvents(ctx context.Context, dep string, from, to time.Time) ([]dal.MonitorDayEvent, error) {
	rs, err := s.q.QueryContext(ctx, stMonDayEvents.SQL, dep, Time(from), Time(to))
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []dal.MonitorDayEvent
	for rs.Next() {
		var e dal.MonitorDayEvent
		if err := rs.Scan(&e.ID, &e.At, &e.Kind, &e.Phase, &e.Severity, &e.Subject, &e.Detail); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// monNullAt is a peak time as written: NULL when there is none.
func monNullAt(t *time.Time) any {
	if t == nil {
		return nil
	}
	return Time(*t)
}

// AddDay writes one day summary unless there is one (MySQL's INSERT IGNORE
// on the primary key).
func (s monStatements) AddDay(ctx context.Context, d dal.MonitorDay) error {
	_, err := s.q.ExecContext(ctx, stMonAddDay.SQL,
		d.Deployment, d.Day, d.IntervalSec, d.Samples, d.Accepted, d.Rejected, d.Sent, d.Receipts, d.MO,
		d.PeakTPSIn, monNullAt(d.PeakTPSInAt), d.PeakTPSOut, monNullAt(d.PeakTPSOutAt), d.PeakQueueDepth, monNullAt(d.PeakQueueDepthAt),
		d.PeakQueueAge, monNullAt(d.PeakQueueAgeAt), d.PeakLatency, monNullAt(d.PeakLatencyAt),
		d.EventsCritical, d.EventsWarning, d.EventsInfo, d.Body)
	if IsDuplicate(err) {
		return nil
	}
	return err
}

func (s monStatements) DaySummaries(ctx context.Context, fromDay, toDay, deployment string) ([][]byte, error) {
	rows, err := s.q.QueryContext(ctx, stMonDaySummaries.SQL, fromDay, toDay, deployment)
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

// DeleteDaysBefore deletes the summaries of days before day, a thousand at
// a time (MySQL deletes them in one statement; there are a few a deployment
// a day).
func (s monStatements) DeleteDaysBefore(ctx context.Context, day string) (int64, error) {
	var all int64
	for {
		r, err := s.q.ExecContext(ctx, stMonDeleteDays.SQL, day)
		if err != nil {
			return all, err
		}
		n, _ := r.RowsAffected()
		all += n
		if n < 1000 {
			return all, nil
		}
	}
}

// ---- state ----

func (s monStatements) State(ctx context.Context, name string) (string, error) {
	var v string
	err := s.q.QueryRowContext(ctx, stMonState.SQL, name).Scan(&v)
	return v, err
}

func (s monStatements) SetState(ctx context.Context, name, value string) error {
	_, err := stMonSetState.Exec(ctx, s.q, sql.Named("name", name), sql.Named("value", value))
	return err
}

// The history's opener, and the console's reader of it (console.go on
// MySQL).
var _ = serves(func(d *dal.Driver) {
	d.MonitorHistory = openMonitorHistory
	d.MonitorReader = openMonitorReader
})

// openMonitorReader opens the hot store at dsn on a handle of its own, the
// console's to close (internal/dal/mysql/console.go's monitorReader): the
// history on that handle, whose Close closes it.
func openMonitorReader(dsn string) (dal.MonitorReader, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	return NewMonitorHistory(db), nil
}
