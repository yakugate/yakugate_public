// Package dbpool gives a process ONE connection pool per MySQL database.
//
// Why (production, 2026-10-06: "our systems are consuming more than 600
// connections, even these are not enough"). Every part of a role used to open
// its own pool to the same database: the queue store, the reject writer, the
// crypto keyring, the reference loader, dedupe, the reserver, vendor health,
// the audit trail, logging config — 94 pools across the 13 roles, 31 of them
// allowed 50 connections each and keeping 25 idle for half an hour, 17 with
// no limit at all. One busy moment in one role filled the server for every
// other.
//
// Now a process asks Get(dsn) and every caller with the same DSN shares the
// same *sql.DB. A role holds as many pools as it has databases (config,
// queue, hot — the HLR data shares the queue's unless YAKU_HLR_DSN names its
// own), each limited by Limits, and connections nobody uses are closed after
// IdleTime. The limits are settings (YAKU_DB_MAX_OPEN, YAKU_DB_MAX_IDLE,
// YAKU_DB_IDLE_SEC), with a default by role: the roles on the message path
// get more than the housekeeping ones.
//
// A shared pool is never closed by a caller — it lives as long as the
// process. Closers kept for their interfaces (mysql.Store.Close, audit) are
// no-ops for that reason.
//
// Not through here, on purpose: the bootstrap's short-lived migration pools
// (multi-statement DSNs, closed when done), the monitoring history writer
// (two connections of its own, so a saturated traffic pool can never stop the
// gateway reporting that it is saturated), storewatch (it measures the stores
// and must not wait behind traffic), the console and the restore command.
package dbpool

import (
	"database/sql"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"
)

// Limits bound one pool.
type Limits struct {
	MaxOpen  int           // connections at most (in use + idle)
	MaxIdle  int           // idle connections kept ready
	IdleTime time.Duration // an idle connection is closed after this
	Lifetime time.Duration // a connection is replaced after this
}

// Defaults for a role on the message path, and for a housekeeping role.
// Per database, per process. The worst case for one gateway of 13 roles and
// 3 databases is 7x3x16 + 6x3x8 = 480; in practice a role holds a few per
// database, because idle ones are closed.
//
// Idle connections are kept five minutes, and up to 8 (4) of them
// (2026-10-13). They were closed after 30 seconds and only 4 (2) were kept,
// while most of a role's timers run every 30 seconds: each round found its
// connections just closed and opened new ones — 540,000 connections in 20
// hours on the dev box (7.5 a second), each with its handshake and its
// SET time_zone. A gateway at rest now holds about 135 connections.
var (
	TrafficLimits = Limits{MaxOpen: 16, MaxIdle: 8, IdleTime: 5 * time.Minute, Lifetime: 30 * time.Minute}
	LightLimits   = Limits{MaxOpen: 8, MaxIdle: 4, IdleTime: 5 * time.Minute, Lifetime: 30 * time.Minute}
)

// trafficRoles carry or settle messages; every other role is housekeeping.
var trafficRoles = map[string]bool{
	"http-server": true, "smpp-server": true, "http-vendor": true, "smpp-vendor": true,
	"dlr": true, "mo": true, "billing": true, "db-worker": true,
}

// LimitsFor is a role's limits: its default, with any of the three settings
// laid over it. A setting that is not a positive number is ignored.
func LimitsFor(role string) Limits {
	l := LightLimits
	if trafficRoles[role] {
		l = TrafficLimits
	}
	if n := envInt("YAKU_DB_MAX_OPEN"); n > 0 {
		l.MaxOpen = n
	}
	if n := envInt("YAKU_DB_MAX_IDLE"); n > 0 {
		l.MaxIdle = n
	}
	if n := envInt("YAKU_DB_IDLE_SEC"); n > 0 {
		l.IdleTime = time.Duration(n) * time.Second
	}
	if l.MaxIdle > l.MaxOpen {
		l.MaxIdle = l.MaxOpen
	}
	return l
}

func envInt(name string) int {
	n, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil {
		return 0
	}
	return n
}

var (
	mu     sync.Mutex
	pools  = map[string]*sql.DB{}
	limits = TrafficLimits
	// program is what this process calls itself to MySQL ("yakugate-dlr").
	program string
)

// SetProgram names this process on every connection made from now on, so the
// server's own statistics can be read role by role (performance_schema
// session_connect_attrs, program_name). Called once at start, before any
// role is built.
func SetProgram(name string) {
	mu.Lock()
	program = name
	mu.Unlock()
}

// Interpolate says whether a statement's arguments are put into its text by
// the driver (YAKU_DB_INTERPOLATE, on unless it is 0 / false / off / no).
//
// Without it every statement that has a "?" is three round trips — prepare,
// execute, close: 19 million prepares and 19 million closes for 29 million
// executes in 20 hours on the dev box (2026-10-13). With it the statement is
// one round trip. The driver escapes the values itself and falls back to a
// prepare for a statement it cannot fill in. What it changes for a caller: a
// []byte argument is sent as a binary string, which MySQL refuses for a JSON
// column — pass JSON as a string (concat.Assembler was the one place).
func Interpolate() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_DB_INTERPOLATE"))) {
	case "0", "false", "off", "no":
		return false
	}
	return true
}

// DSN is dsn as this process connects with it: the arguments interpolated
// (Interpolate) and the process named (SetProgram). For the few pools that
// are not made by Get. A DSN that cannot be read is returned as it is.
func DSN(dsn string) string {
	mu.Lock()
	defer mu.Unlock()
	return decorate(dsn)
}

// decorate is DSN for a caller that holds mu.
func decorate(dsn string) string {
	c, err := mysqldrv.ParseDSN(dsn)
	if err != nil {
		return dsn
	}
	if Interpolate() {
		c.InterpolateParams = true
	}
	attrs := c.ConnectionAttributes
	if program != "" && !strings.Contains(attrs, "program_name:") {
		if attrs != "" {
			attrs += ","
		}
		attrs += "program_name:" + program
	}
	out := c.FormatDSN()
	if attrs != "" {
		// The driver (v1.8.1) reads connectionAttributes but does not write
		// it back in FormatDSN, so it is added here.
		sep := "?"
		if strings.Contains(out, "?") {
			sep = "&"
		}
		out += sep + "connectionAttributes=" + url.QueryEscape(attrs)
	}
	return out
}

// Configure sets the limits for this process: pools made from now on, and
// those already made. Called once at start, before any role is built.
func Configure(l Limits) {
	if l.MaxOpen <= 0 {
		return
	}
	if l.MaxIdle <= 0 || l.MaxIdle > l.MaxOpen {
		l.MaxIdle = min(2, l.MaxOpen)
	}
	mu.Lock()
	defer mu.Unlock()
	limits = l
	for _, db := range pools {
		apply(db, l)
	}
	for _, db := range readPools {
		apply(db, l)
	}
	for _, p := range others {
		p.Configure(l)
	}
}

// Current is the limits in force.
func Current() Limits {
	mu.Lock()
	defer mu.Unlock()
	return limits
}

// Program is what this process calls itself to its databases (SetProgram),
// "" when it was never set. The SQL Server DAL's pools
// (internal/dal/mssql/pool.go) name their connections with it too.
func Program() string {
	mu.Lock()
	defer mu.Unlock()
	return program
}

// ReadDSN is the read instance's DSN set beside a store's DSN (SetReaders),
// "" when there is none.
func ReadDSN(dsn string) string {
	mu.Lock()
	defer mu.Unlock()
	return readDSNs[dsn]
}

// Pools is another database type's pools, kept by that database's DAL under
// the same settings (internal/dal/mssql/pool.go: SQL Server). dbpool keeps
// the settings — the limits, the program name, the read DSNs — and the
// monitoring list; the other DAL keeps its own *sql.DB, so this package
// needs no driver of that database.
type Pools interface {
	// Configure lays the limits over the pools made so far.
	Configure(Limits)
	// Stats lists them as monitoring shows them.
	Stats() []Stat
}

// others are the Pools other DALs added.
var others []Pools

// AddPools makes another database type's pools follow Configure and appear
// in Stats. Called once, from that DAL's init.
func AddPools(p Pools) {
	mu.Lock()
	defer mu.Unlock()
	others = append(others, p)
}

// Apply lays limits over one pool (for another DAL's Pools).
func Apply(db *sql.DB, l Limits) { apply(db, l) }

func apply(db *sql.DB, l Limits) {
	db.SetMaxOpenConns(l.MaxOpen)
	db.SetMaxIdleConns(l.MaxIdle)
	db.SetConnMaxIdleTime(l.IdleTime)
	db.SetConnMaxLifetime(l.Lifetime)
}

// Get is this process's pool for a MySQL DSN, made on first use. Like
// sql.Open it does not connect: a wrong password or a dead host shows on the
// first query. Never Close what it returns.
func Get(dsn string) (*sql.DB, error) {
	mu.Lock()
	defer mu.Unlock()
	if db, ok := pools[dsn]; ok {
		return db, nil
	}
	db, err := sql.Open("mysql", decorate(dsn))
	if err != nil {
		return nil, err
	}
	apply(db, limits)
	pools[dsn] = db
	return db, nil
}

// Stat is one pool as monitoring shows it. The DSN is never given: only the
// server and database it points at.
type Stat struct {
	Server   string `json:"server"`
	Database string `json:"database"`
	MaxOpen  int    `json:"max_open"`
	Open     int    `json:"open"`
	InUse    int    `json:"in_use"`
	Idle     int    `json:"idle"`
	// Waited is how many times a caller had to wait for a connection since
	// the process started, and WaitedMs for how long in all: a pool too
	// small for its role shows here before anything fails.
	Waited   int64 `json:"waited"`
	WaitedMs int64 `json:"waited_ms"`
	// Use is "read" for a read instance's pool (reader.go); empty for the
	// pool a database is written through. Away says a read instance is not
	// answering and its reads go to the write instance meanwhile.
	Use  string `json:"use,omitempty"`
	Away bool   `json:"away,omitempty"`
}

// Stats lists this process's pools, by server then database.
func Stats() []Stat {
	mu.Lock()
	more := append([]Pools(nil), others...)
	mu.Unlock()
	var theirs []Stat
	for _, p := range more {
		theirs = append(theirs, p.Stats()...)
	}
	mu.Lock()
	defer mu.Unlock()
	out := make([]Stat, 0, len(pools)+len(theirs))
	out = append(out, theirs...)
	for dsn, db := range pools {
		s := db.Stats()
		st := Stat{MaxOpen: s.MaxOpenConnections, Open: s.OpenConnections, InUse: s.InUse, Idle: s.Idle,
			Waited: s.WaitCount, WaitedMs: s.WaitDuration.Milliseconds()}
		if c, err := mysqldrv.ParseDSN(dsn); err == nil {
			st.Server, st.Database = c.Addr, c.DBName
		}
		out = append(out, st)
	}
	for dsn, db := range readPools {
		s := db.Stats()
		st := Stat{MaxOpen: s.MaxOpenConnections, Open: s.OpenConnections, InUse: s.InUse, Idle: s.Idle,
			Waited: s.WaitCount, WaitedMs: s.WaitDuration.Milliseconds(), Use: "read"}
		if c, err := mysqldrv.ParseDSN(dsn); err == nil {
			st.Server, st.Database = c.Addr, c.DBName
		}
		for _, r := range readers {
			if r.read == db {
				st.Away = r.down.Load()
			}
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Server != out[j].Server {
			return out[i].Server < out[j].Server
		}
		return out[i].Database < out[j].Database
	})
	return out
}
