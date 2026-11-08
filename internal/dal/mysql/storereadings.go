package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"

	"github.com/yakugate/yakugate/internal/dal"
)

// What a MySQL server says about itself, for monitoring (internal/storewatch,
// D-129) — moved here unchanged on 29 Oct 2026 (the DAL plan). Every figure is
// one the server gives any client: SHOW GLOBAL STATUS / VARIABLES, InnoDB's
// deadlock count, replica status and the sizes of the gateway's databases.

// ServerProbe is one of the gateway's databases on a small pool of its own,
// apart from the traffic's.
type ServerProbe struct {
	addr, name string
	db         *sql.DB
}

var _ dal.SQLServerProbe = (*ServerProbe)(nil)

// The servers' own figures.
var _ = serves(func(d *dal.Driver) { d.SQLServerProbe = openServerProbe })

// openServerProbe reads the server's address from the DSN and opens a small
// pool on it. An error when the DSN cannot be read.
func openServerProbe(dsn string) (dal.SQLServerProbe, error) {
	mc, err := mysqldrv.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	addr := mc.Addr
	if mc.Net == "unix" {
		addr = "unix:" + mc.Addr
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(time.Minute)
	return &ServerProbe{addr: addr, name: mc.DBName, db: db}, nil
}

func (p *ServerProbe) Addr() string                   { return p.addr }
func (p *ServerProbe) Database() string               { return p.name }
func (p *ServerProbe) Ping(ctx context.Context) error { return p.db.PingContext(ctx) }
func (p *ServerProbe) Close() error                   { return p.db.Close() }

// Global reads SHOW GLOBAL STATUS or VARIABLES for the names given.
func (p *ServerProbe) Global(ctx context.Context, what string, names []string) map[string]string {
	db := p.db
	out := map[string]string{}
	in := strings.Repeat("?,", len(names))
	args := make([]any, len(names))
	for i, n := range names {
		args[i] = n
	}
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := db.QueryContext(c, "SHOW GLOBAL "+what+" WHERE Variable_name IN ("+in[:len(in)-1]+")", args...)
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

// Deadlocks is InnoDB's deadlock count since start (INNODB_METRICS, which
// needs the PROCESS privilege); ok=false when it cannot be read.
func (p *ServerProbe) Deadlocks(ctx context.Context) (float64, bool) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var n sql.NullFloat64
	if err := p.db.QueryRowContext(c, "SELECT `COUNT` FROM information_schema.INNODB_METRICS WHERE NAME = 'lock_deadlocks'").Scan(&n); err != nil || !n.Valid {
		return 0, false
	}
	return n.Float64, true
}

// Replica reads SHOW REPLICA STATUS (MySQL 8.0.22+), else SHOW SLAVE STATUS.
// No row: not a replica. Refused: not known.
func (p *ServerProbe) Replica(ctx context.Context) dal.ReplicaStatus {
	db := p.db
	for _, q := range []string{"SHOW REPLICA STATUS", "SHOW SLAVE STATUS"} {
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		rows, err := db.QueryContext(c, q)
		if err != nil {
			cancel()
			if strings.Contains(err.Error(), "1227") || strings.Contains(strings.ToLower(err.Error()), "access denied") {
				return dal.ReplicaStatus{}
			}
			continue // older server: try the older words
		}
		r := dal.ReplicaStatus{Known: true}
		cols, _ := rows.Columns()
		if rows.Next() {
			vals := make([]sql.NullString, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if rows.Scan(ptrs...) == nil {
				get := func(names ...string) (string, bool) {
					for i, c := range cols {
						for _, n := range names {
							if c == n {
								return vals[i].String, vals[i].Valid
							}
						}
					}
					return "", false
				}
				r.IsReplica = true
				io, _ := get("Replica_IO_Running", "Slave_IO_Running")
				sq, _ := get("Replica_SQL_Running", "Slave_SQL_Running")
				r.IORunning, r.SQLRunning = io == "Yes", sq == "Yes"
				if lag, ok := get("Seconds_Behind_Source", "Seconds_Behind_Master"); ok && lag != "" {
					v := int64(probeNum(lag))
					r.LagSec = &v
				}
				if e, _ := get("Last_IO_Error"); e != "" {
					r.LastError = e
				} else if e, _ := get("Last_SQL_Error"); e != "" {
					r.LastError = e
				}
			}
		}
		rows.Close()
		cancel()
		return r
	}
	return dal.ReplicaStatus{}
}

// SchemaSizes reads each named database's size and table count (the
// information schema is not free on a store with thousands of tables: the
// caller asks every 5 minutes). Rows that will not scan are left out.
func (p *ServerProbe) SchemaSizes(ctx context.Context, schemas []string) (map[string][2]int64, error) {
	names := make([]any, len(schemas))
	for i, n := range schemas {
		names[i] = n
	}
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rows, err := p.db.QueryContext(c, `SELECT table_schema, COALESCE(SUM(data_length + index_length), 0), COUNT(*)
			FROM information_schema.tables WHERE table_schema IN (`+strings.TrimRight(strings.Repeat("?,", len(names)), ",")+`)
			GROUP BY table_schema`, names...)
	if err != nil {
		return nil, err
	}
	sizes := map[string][2]int64{}
	for rows.Next() {
		var name string
		var size, n int64
		if rows.Scan(&name, &size, &n) == nil {
			sizes[name] = [2]int64{size, n}
		}
	}
	rows.Close()
	return sizes, nil
}

// probeNum reads a number as storewatch's parseNum does.
func probeNum(s string) float64 {
	s = strings.TrimSpace(s)
	var f float64
	if _, err := fmt.Sscanf(s, "%g", &f); err != nil {
		return 0
	}
	return f
}

// The statements a probe sends, for the lint test (SHOW … is not a SELECT;
// the IN lists are filled at run time).
var (
	_ = MonitorStmts.Add("storewatch.global", "SHOW GLOBAL STATUS WHERE Variable_name IN (?)", false)
	_ = MonitorStmts.Add("storewatch.deadlocks", "SELECT `COUNT` FROM information_schema.INNODB_METRICS WHERE NAME = 'lock_deadlocks'", false)
	_ = MonitorStmts.Add("storewatch.replica", "SHOW REPLICA STATUS", false)
	_ = MonitorStmts.Add("storewatch.sizes", `SELECT table_schema, COALESCE(SUM(data_length + index_length), 0), COUNT(*)
			FROM information_schema.tables WHERE table_schema IN (?)
			GROUP BY table_schema`, false)
)
