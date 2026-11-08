package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	mssqldrv "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// One connection pool per SQL Server database per process (step 4 of the
// DAL plan): what dbpool.Get is for MySQL, kept here because only the SQL
// Server DAL may import its driver. The SETTINGS stay dbpool's, so one
// process is sized one way whatever its database: the limits
// (YAKU_DB_MAX_OPEN, YAKU_DB_MAX_IDLE, YAKU_DB_IDLE_SEC, laid over the
// role's default by dbpool.Configure), the name the process gives itself
// (dbpool.SetProgram, sent as the connection's "app name", so
// sys.dm_exec_sessions.program_name tells the roles apart) and the read DSN
// beside each store (dbpool.SetReaders, YAKU_<STORE>_READ_DSN). The pools
// show in dbpool.Stats with the MySQL ones.
//
// A read instance on SQL Server is an Availability Group listener reached
// with ApplicationIntent=ReadOnly (D-132's settings, the same names);
// config adds that to a read DSN that does not say it.
//
// A shared pool is never closed by a caller, as on MySQL.

func init() { dbpool.AddPools(pools) }

// poolSet is this process's SQL Server pools and read handles.
type poolSet struct {
	mu      sync.Mutex
	write   map[string]*sql.DB // by the store's DSN
	read    map[string]*sql.DB // by the read DSN
	readers map[string]*Read   // by the store's DSN
}

var pools = &poolSet{write: map[string]*sql.DB{}, read: map[string]*sql.DB{}, readers: map[string]*Read{}}

// Get is this process's pool for a SQL Server DSN, made on first use. Like
// sql.Open it does not connect: a wrong password or a dead host shows on
// the first query. Never Close what it returns.
func Get(dsn string) (*sql.DB, error) {
	pools.mu.Lock()
	defer pools.mu.Unlock()
	if db, ok := pools.write[dsn]; ok {
		return db, nil
	}
	db, err := open(dsn)
	if err != nil {
		return nil, err
	}
	pools.write[dsn] = db
	return db, nil
}

// open makes a pool with the process's limits and name.
func open(dsn string) (*sql.DB, error) {
	c, err := mssqldrv.NewConnector(named(dsn, dbpool.Program()))
	if err != nil {
		return nil, fmt.Errorf("mssql: %w", err)
	}
	db := sql.OpenDB(c)
	dbpool.Apply(db, dbpool.Current())
	return db, nil
}

// named is dsn with the process's name as its "app name", unless the DSN
// names one itself (an operator's choice wins). Only the sqlserver:// form is
// changed; another form is used as it is.
func named(dsn, program string) string {
	if program == "" || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(dsn)), "sqlserver://") {
		return dsn
	}
	u, err := url.Parse(strings.TrimSpace(dsn))
	if err != nil {
		return dsn
	}
	q := u.Query()
	for k := range q {
		if strings.EqualFold(k, msdsn.AppName) {
			return dsn
		}
	}
	q.Set(msdsn.AppName, program)
	u.RawQuery = q.Encode()
	return u.String()
}

// Read is where the reads of one database go when they may be a moment old,
// as dbpool.Read is for MySQL: the read instance's pool, or the write
// instance's when there is none or it is not answering (pinged every two
// seconds; two misses and reads leave it). Ask DB() at every use.
type Read struct {
	write *sql.DB
	read  *sql.DB // nil = no read instance
	mu    sync.Mutex
	down  bool
	where string
}

// DB is the pool to read from now.
func (r *Read) DB() *sql.DB {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.read == nil || r.down {
		return r.write
	}
	return r.read
}

// Write is the pool the database is written through.
func (r *Read) Write() *sql.DB {
	if r == nil {
		return nil
	}
	return r.write
}

// Separate says the database has a read instance of its own.
func (r *Read) Separate() bool { return r != nil && r.read != nil }

// Down says the read instance is not answering and reads go to the write
// instance meanwhile.
func (r *Read) Down() bool {
	if r == nil || r.read == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.down
}

// How often a read instance is pinged, for how long, and how many misses in
// a row send its reads to the write instance (dbpool's numbers).
var (
	readPingEvery = 2 * time.Second
	readPingFor   = 1500 * time.Millisecond
	readFailures  = 2
)

// Reader is the reads' handle for the database dsn names (the store's own
// DSN, the one Get takes).
func Reader(dsn string) (*Read, error) {
	w, err := Get(dsn)
	if err != nil {
		return nil, err
	}
	rd := dbpool.ReadDSN(dsn)
	pools.mu.Lock()
	defer pools.mu.Unlock()
	if r, ok := pools.readers[dsn]; ok {
		return r, nil
	}
	r := &Read{write: w}
	if rd != "" {
		db, err := open(rd)
		if err != nil {
			log.Printf("[dbpool] the read instance of this SQL Server database cannot be used (%v): its reads go to the write instance", err)
		} else {
			r.read = db
			r.where = where(rd)
			pools.read[rd] = db
			go r.watch()
		}
	}
	pools.readers[dsn] = r
	return r, nil
}

// watch pings the read instance for the life of the process.
func (r *Read) watch() {
	failed := 0
	t := time.NewTicker(readPingEvery)
	defer t.Stop()
	for range t.C {
		ctx, cancel := context.WithTimeout(context.Background(), readPingFor)
		err := r.read.PingContext(ctx)
		cancel()
		r.mu.Lock()
		if err == nil {
			failed = 0
			if r.down {
				r.down = false
				log.Printf("[dbpool] the read instance %s is answering again: reads go back to it", r.where)
			}
		} else if failed++; failed >= readFailures && !r.down {
			r.down = true
			log.Printf("[dbpool] the read instance %s is not answering (%v): its reads go to the write instance until it is back", r.where, err)
		}
		r.mu.Unlock()
	}
}

// where is "host:port/database" of a DSN, never its login.
func where(dsn string) string {
	server, database := serverOf(dsn)
	return server + "/" + database
}

// serverOf reads the server (host:port, or host\instance) and the database
// of any DSN form the driver takes.
func serverOf(dsn string) (server, database string) {
	c, err := msdsn.Parse(dsn)
	if err != nil {
		return "", ""
	}
	server = c.Host
	switch {
	case c.Instance != "":
		server += `\` + c.Instance
	case c.Port != 0:
		server = fmt.Sprintf("%s:%d", c.Host, c.Port)
	}
	return server, c.Database
}

// Configure lays the limits over every SQL Server pool made so far
// (dbpool.Configure calls it).
func (p *poolSet) Configure(l dbpool.Limits) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, db := range p.write {
		dbpool.Apply(db, l)
	}
	for _, db := range p.read {
		dbpool.Apply(db, l)
	}
}

// Stats lists the SQL Server pools as monitoring shows them (dbpool.Stats
// calls it).
func (p *poolSet) Stats() []dbpool.Stat {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []dbpool.Stat
	one := func(dsn string, db *sql.DB, use string) dbpool.Stat {
		s := db.Stats()
		st := dbpool.Stat{MaxOpen: s.MaxOpenConnections, Open: s.OpenConnections, InUse: s.InUse, Idle: s.Idle,
			Waited: s.WaitCount, WaitedMs: s.WaitDuration.Milliseconds(), Use: use}
		st.Server, st.Database = serverOf(dsn)
		return st
	}
	for dsn, db := range p.write {
		out = append(out, one(dsn, db, ""))
	}
	for dsn, db := range p.read {
		st := one(dsn, db, "read")
		for _, r := range p.readers {
			if r.read == db {
				st.Away = r.Down()
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
