package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/yakugate/yakugate/internal/dal"
)

// What a SQL Server says about itself, for monitoring (internal/storewatch),
// made like the MySQL DAL's internal/dal/mysql/storereadings.go. MySQL's
// figures come from SHOW GLOBAL STATUS / VARIABLES; SQL Server's from its
// dynamic management views (sys.dm_os_performance_counters,
// sys.dm_exec_requests, sys.dm_os_waiting_tasks, sys.dm_os_sys_info,
// sys.dm_hadr_database_replica_states) and catalog (sys.configurations,
// sys.databases, sys.allocation_units, sys.tables).
//
// storewatch asks for MySQL's names, so each SQL Server figure is answered
// under the MySQL name it stands for (statusNames, variableNames below). A
// name SQL Server has no figure for is left out of the answer, as a figure
// MySQL cannot give is: storewatch reads it as 0. Those are
// Max_used_connections, Aborted_connects, Connection_errors_max_connections,
// Com_select / insert / update / delete / replace (SQL Server counts
// batches, not statements of a kind), Slow_queries, Bytes_received,
// Bytes_sent and super_read_only.
//
// The DMVs need VIEW SERVER PERFORMANCE STATE (SQL Server 2022; VIEW SERVER
// STATE before). A login without it reads nothing, as MySQL's figures read
// nothing when they are refused.

// statusNames are the SHOW GLOBAL STATUS names a SQL Server answers, each
// from: a performance counter (object, counter, instance) or a figure of
// its own.
var statusCounters = []struct{ name, object, counter, instance string }{
	{"Threads_connected", "General Statistics", "User Connections", ""},
	{"Questions", "SQL Statistics", "Batch Requests/sec", ""},
	{"Created_tmp_disk_tables", "Access Methods", "Workfiles Created/sec", ""},
	{"Innodb_buffer_pool_read_requests", "Buffer Manager", "Page lookups/sec", ""},
	{"Innodb_buffer_pool_reads", "Buffer Manager", "Page reads/sec", ""},
	{"Innodb_buffer_pool_pages_data", "Buffer Manager", "Database pages", ""},
	{"Innodb_buffer_pool_pages_total", "Buffer Manager", "Target pages", ""},
	{"Innodb_row_lock_waits", "Locks", "Lock Waits/sec", "_Total"},
}

// countersSQL is the VALUES list statusCounters gives the status read.
func countersSQL() string {
	rows := make([]string, len(statusCounters))
	for i, c := range statusCounters {
		rows[i] = "(N'" + c.name + "', N'" + c.object + "', N'" + c.counter + "', N'" + c.instance + "')"
	}
	return strings.Join(rows, ", ")
}

var (
	// The status figures, each under the MySQL name it stands for, the
	// names asked given as one JSON array. A counter's object is
	// "SQLServer:<object>" on a default instance, "MSSQL$<name>:<object>"
	// on a named one. Uptime is from the server's start (its own clock).
	stProbeStatus = MonitorStmts.Add("storewatch.status", `SELECT x.name, x.value FROM (
		SELECT N'Uptime' AS name, CONVERT(NVARCHAR(40), DATEDIFF_BIG(SECOND, sqlserver_start_time, SYSDATETIME())) AS value
		  FROM sys.dm_os_sys_info
		UNION ALL
		SELECT N'Threads_running', CONVERT(NVARCHAR(40), COUNT(*))
		  FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s ON s.session_id = r.session_id
		 WHERE s.is_user_process = 1
		UNION ALL
		SELECT N'Innodb_row_lock_current_waits', CONVERT(NVARCHAR(40), COUNT(*))
		  FROM sys.dm_os_waiting_tasks WHERE wait_type LIKE N'LCK[_]M[_]%'
		UNION ALL
		SELECT N'Innodb_row_lock_time_avg', CONVERT(NVARCHAR(40),
		       CAST(MAX(CASE WHEN counter_name = N'Average Wait Time (ms)' THEN cntr_value END) AS FLOAT)
		       / NULLIF(MAX(CASE WHEN counter_name = N'Average Wait Time Base' THEN cntr_value END), 0))
		  FROM sys.dm_os_performance_counters
		 WHERE RTRIM(object_name) LIKE N'%:Locks' AND RTRIM(instance_name) = N'_Total'
		   AND counter_name IN (N'Average Wait Time (ms)', N'Average Wait Time Base')
		UNION ALL
		SELECT m.name, CONVERT(NVARCHAR(40), pc.cntr_value)
		  FROM sys.dm_os_performance_counters pc
		  JOIN (VALUES `+countersSQL()+`) m (name, obj, counter, inst)
		    ON RTRIM(pc.object_name) LIKE N'%:' + m.obj AND RTRIM(pc.counter_name) = m.counter AND RTRIM(pc.instance_name) = m.inst
		) x WHERE x.name IN (SELECT value FROM OPENJSON(@p1)) AND x.value IS NOT NULL`, false)
	// The variables: the version, the most connections the server takes
	// ("user connections" 0 = the most there can be), whether the database
	// takes writes, and the memory the buffer pool may use.
	stProbeVariables = MonitorStmts.Add("storewatch.variables", `SELECT x.name, x.value FROM (
		SELECT N'version' AS name, CONCAT(N'SQL Server ', CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductVersion')),
		       N' ', CONVERT(NVARCHAR(128), SERVERPROPERTY('Edition'))) AS value
		UNION ALL
		SELECT N'max_connections', CONVERT(NVARCHAR(40), CASE WHEN CONVERT(INT, value_in_use) = 0 THEN @@MAX_CONNECTIONS
		       ELSE CONVERT(INT, value_in_use) END)
		  FROM sys.configurations WHERE name = N'user connections'
		UNION ALL
		SELECT N'read_only', CASE WHEN CONVERT(NVARCHAR(20), DATABASEPROPERTYEX(DB_NAME(), 'Updateability')) = N'READ_ONLY'
		       THEN N'ON' ELSE N'OFF' END
		UNION ALL
		SELECT N'innodb_buffer_pool_size', CONVERT(NVARCHAR(40), CAST(committed_target_kb AS BIGINT) * 1024)
		  FROM sys.dm_os_sys_info
		) x WHERE x.name IN (SELECT value FROM OPENJSON(@p1)) AND x.value IS NOT NULL`, false)
	stProbeDeadlocks = MonitorStmts.Add("storewatch.deadlocks", `SELECT TOP (1) cntr_value FROM sys.dm_os_performance_counters
		WHERE RTRIM(object_name) LIKE N'%:Locks' AND RTRIM(counter_name) = N'Number of Deadlocks/sec' AND RTRIM(instance_name) = N'_Total'`, false)
	// This database's replica in an Availability Group, when it is in one.
	stProbeReplica = MonitorStmts.Add("storewatch.replica", `SELECT TOP (1) CAST(is_primary_replica AS INT),
		       COALESCE(synchronization_state_desc, N''), CAST(is_suspended AS INT), COALESCE(suspend_reason_desc, N''),
		       secondary_lag_seconds
		  FROM sys.dm_hadr_database_replica_states WHERE database_id = DB_ID() AND is_local = 1`, false)
	stProbeDatabases = MonitorStmts.Add("storewatch.databases", `SELECT name FROM sys.databases
		WHERE name IN (SELECT value FROM OPENJSON(@p1)) AND state_desc = N'ONLINE' AND HAS_DBACCESS(name) = 1`, false)
	// One database's part of the sizes, joined by UNION ALL for every
	// database asked (one statement for all).
	stProbeSize = MonitorStmts.Add("storewatch.size", `SELECT N'<database>',
		(SELECT COALESCE(SUM(CAST(a.used_pages AS BIGINT)), 0) * 8192 FROM [<database>].sys.allocation_units a),
		(SELECT COUNT_BIG(*) FROM [<database>].sys.tables t WHERE t.is_ms_shipped = 0)`, false)
)

// ServerProbe is one of the gateway's databases on a small pool of its own,
// apart from the traffic's.
type ServerProbe struct {
	addr, name string
	db         *sql.DB
}

var _ dal.SQLServerProbe = (*ServerProbe)(nil)

var _ = serves(func(d *dal.Driver) { d.SQLServerProbe = openServerProbe })

// openServerProbe reads the server's address from the DSN and opens a small
// pool on it. An error when the DSN cannot be read.
func openServerProbe(dsn string) (dal.SQLServerProbe, error) {
	if _, err := msdsn.Parse(dsn); err != nil {
		return nil, err
	}
	db, err := historyPool(dsn)
	if err != nil {
		return nil, err
	}
	addr, name := serverOf(dsn)
	return &ServerProbe{addr: addr, name: name, db: db}, nil
}

func (p *ServerProbe) Addr() string                   { return p.addr }
func (p *ServerProbe) Database() string               { return p.name }
func (p *ServerProbe) Ping(ctx context.Context) error { return p.db.PingContext(ctx) }
func (p *ServerProbe) Close() error                   { return p.db.Close() }

// Global reads the STATUS or VARIABLES figures for the MySQL names given.
func (p *ServerProbe) Global(ctx context.Context, what string, names []string) map[string]string {
	out := map[string]string{}
	q := stProbeStatus
	if strings.EqualFold(what, "VARIABLES") {
		q = stProbeVariables
	}
	if names == nil {
		names = []string{}
	}
	list, _ := json.Marshal(names)
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := p.db.QueryContext(c, q.SQL, string(list))
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var k, v sql.NullString
		if rows.Scan(&k, &v) == nil {
			out[k.String] = v.String
		}
	}
	return out
}

// Deadlocks is the server's deadlock count since start; ok=false when it
// cannot be read.
func (p *ServerProbe) Deadlocks(ctx context.Context) (float64, bool) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var n sql.NullFloat64
	if err := p.db.QueryRowContext(c, stProbeDeadlocks.SQL).Scan(&n); err != nil || !n.Valid {
		return 0, false
	}
	return n.Float64, true
}

// Replica reads this database's replica in an Availability Group. No row,
// or the primary: not a replica. Refused: not known. A secondary is
// "running" (MySQL's IO and SQL threads both) while its data movement is
// not suspended and it is synchronizing or synchronized.
func (p *ServerProbe) Replica(ctx context.Context) dal.ReplicaStatus {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var primary, suspended int
	var state, reason string
	var lag sql.NullInt64
	err := p.db.QueryRowContext(c, stProbeReplica.SQL).Scan(&primary, &state, &suspended, &reason, &lag)
	switch {
	case err == sql.ErrNoRows:
		return dal.ReplicaStatus{Known: true}
	case err != nil:
		return dal.ReplicaStatus{}
	case primary == 1:
		return dal.ReplicaStatus{Known: true}
	}
	running := suspended == 0 && (state == "SYNCHRONIZING" || state == "SYNCHRONIZED")
	r := dal.ReplicaStatus{Known: true, IsReplica: true, IORunning: running, SQLRunning: running}
	if lag.Valid {
		v := lag.Int64
		r.LagSec = &v
	}
	if suspended != 0 {
		r.LastError = "data movement suspended: " + reason
	} else if !running {
		r.LastError = "synchronization state " + state
	}
	return r
}

// SchemaSizes reads each named database's size (its used pages) and table
// count, for the databases on the server this login may open. Rows that
// will not scan are left out.
func (p *ServerProbe) SchemaSizes(ctx context.Context, schemas []string) (map[string][2]int64, error) {
	if schemas == nil {
		schemas = []string{}
	}
	list, _ := json.Marshal(schemas)
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := p.db.QueryContext(c, stProbeDatabases.SQL, string(list))
	if err != nil {
		return nil, err
	}
	var parts []string
	for rows.Next() {
		var name string
		if rows.Scan(&name) == nil {
			parts = append(parts, sizePart(name))
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sizes := map[string][2]int64{}
	if len(parts) == 0 {
		return sizes, nil
	}
	rows, err = p.db.QueryContext(c, strings.Join(parts, " UNION ALL "))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var size, n int64
		if rows.Scan(&name, &size, &n) == nil {
			sizes[name] = [2]int64{size, n}
		}
	}
	return sizes, nil
}

// sizePart is one database's part of the sizes statement (storewatch.size):
// its name as a literal and as an identifier, each quoted its own way. The
// names are sys.databases' own, never a request's.
func sizePart(name string) string {
	lit := "N'" + strings.ReplaceAll(name, "'", "''") + "'"
	id := quoteIdent(name)
	return strings.NewReplacer("N'<database>'", lit, "[<database>]", id).Replace(stProbeSize.SQL)
}
