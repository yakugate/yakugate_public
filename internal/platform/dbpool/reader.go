package dbpool

import (
	"context"
	"database/sql"
	"log"
	"sync/atomic"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"
)

// A read instance beside the write instance (Saleem 2026-10-13: "two separate
// instances, one read, the other write" — an InnoDB Cluster behind MySQL
// Router has an address for writes and another that spreads reads over the
// other nodes).
//
// Get is, as always, the pool a database is WRITTEN through — and read
// through, by everything that has not been moved. Reader is where a caller
// sends the reads that may be a moment old: a read node applies the write
// node's changes a little after they were made, so a read that must see what
// was written a moment ago (a receipt looking for its message's record, a
// claim, a balance) must never go there. Each caller that uses Reader says
// why its read may be old.
//
// With no read instance set for a database, Reader's pool IS Get's pool:
// nothing changes for an install that sets nothing.
//
// A read instance that stops answering is not allowed to stop the gateway:
// it is pinged every two seconds, and while it is away its reads go to the
// write instance. Said in the log when it goes and when it is back.

// readDSNs is, per write DSN, the DSN its reads may use (SetReaders).
var readDSNs = map[string]string{}

// readers are the Read handles made so far, by write DSN.
var readers = map[string]*Read{}

// SetReaders tells the pool which read DSN stands beside each store's DSN
// (config.Config.Readers). Called once at start, before any role is built.
func SetReaders(m map[string]string) {
	mu.Lock()
	defer mu.Unlock()
	readDSNs = map[string]string{}
	for w, r := range m {
		if w != "" && r != "" && w != r {
			readDSNs[w] = r
		}
	}
}

// Read is where the reads of one database go when they may be a moment old.
type Read struct {
	write *sql.DB
	read  *sql.DB // nil = no read instance: DB is the write pool
	down  atomic.Bool
	where string // "host:port/database" of the read instance, for the log
}

// DB is the pool to read from now: the read instance's, or — when there is
// none, or it is not answering — the write instance's. Ask every time; do not
// keep what it returns.
func (r *Read) DB() *sql.DB {
	if r == nil {
		return nil
	}
	if r.read == nil || r.down.Load() {
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
func (r *Read) Down() bool { return r != nil && r.read != nil && r.down.Load() }

type freshKey struct{}

// Fresh marks a context whose reads must see what was written a moment ago,
// for code that reads through Reader most of the time and now and then must
// not (the reference data, when the Admin API has just said "I changed
// something — load it now").
func Fresh(ctx context.Context) context.Context { return context.WithValue(ctx, freshKey{}, true) }

// IsFresh reports whether ctx was marked by Fresh.
func IsFresh(ctx context.Context) bool {
	v, _ := ctx.Value(freshKey{}).(bool)
	return v
}

// How often a read instance is pinged, how long a ping may take, and how
// many must fail in a row before reads leave it.
var (
	readPingEvery = 2 * time.Second
	readPingFor   = 1500 * time.Millisecond
	readFailures  = 2
)

// Reader is the reads' handle for the database dsn names — dsn being the
// store's own DSN, the one Get takes.
func Reader(dsn string) (*Read, error) {
	w, err := Get(dsn)
	if err != nil {
		return nil, err
	}
	mu.Lock()
	defer mu.Unlock()
	if r, ok := readers[dsn]; ok {
		return r, nil
	}
	r := &Read{write: w}
	if rd, ok := readDSNs[dsn]; ok {
		db, err := sql.Open("mysql", decorate(rd))
		if err != nil {
			// A read DSN that cannot be opened: read where it is written.
			log.Printf("[dbpool] the read instance of this database cannot be used (%v): its reads go to the write instance", err)
		} else {
			apply(db, limits)
			r.read = db
			if c, perr := mysqldrv.ParseDSN(rd); perr == nil {
				r.where = c.Addr + "/" + c.DBName
			}
			readPools[rd] = db
			go r.watch()
		}
	}
	readers[dsn] = r
	return r, nil
}

// readPools are the read instances' pools, by read DSN (for Configure and
// Stats).
var readPools = map[string]*sql.DB{}

// watch pings the read instance for the life of the process and moves the
// reads away from it, and back, as it stops and starts answering.
func (r *Read) watch() {
	failed := 0
	t := time.NewTicker(readPingEvery)
	defer t.Stop()
	for range t.C {
		ctx, cancel := context.WithTimeout(context.Background(), readPingFor)
		err := r.read.PingContext(ctx)
		cancel()
		if err == nil {
			failed = 0
			if r.down.Swap(false) {
				log.Printf("[dbpool] the read instance %s is answering again: reads go back to it", r.where)
			}
			continue
		}
		if failed++; failed >= readFailures && !r.down.Swap(true) {
			log.Printf("[dbpool] the read instance %s is not answering (%v): its reads go to the write instance until it is back", r.where, err)
		}
	}
}
