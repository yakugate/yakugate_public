// Package backup implements the "backup" gateway process: scheduled, rotated,
// Go-native logical snapshots of the config/cold/hot stores to the filesystem
// (gzipped JSONL per table). Cold is the queryable archive; these fs snapshots
// are the disaster-recovery copy. [SCOPE P8/D-031]
//
// Definition: types and construction. The workings (input -> logic -> output)
// live in workings.go.
package backup

import (
	"context"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/singleton"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
)

// target is one database to snapshot.
type target struct {
	name string // logical label + subdir (config/cold/hot)
	dsn  string
	// dump is the database as a backup reads it (internal/dal): read
	// through its read instance when it has one (dbpool.Reader,
	// 2026-10-13). A backup reads every row of every table; that is the
	// work a read instance is for, and a backup a moment behind the write
	// instance is still a whole backup.
	dump dal.TableDumps
}

// Process is the "backup" process. [SCOPE P8]
type Process struct {
	cfg      config.Config
	log      *logging.Logger
	root     string        // backup root directory
	keep     int           // rotation: number of run-dirs to retain
	interval time.Duration // min time between backups
	lastRun  time.Time     // when the newest run began (zero: never, or cut short)
	// nextAt is when the next run is due (zero: now). After a whole run it is
	// the interval later; after a run that left something out, or that could
	// not be written whole, it is sooner (retryWait); while a database does
	// not answer, a minute (waitRetry). next is the same moment for the
	// status report, which is read from another goroutine.
	nextAt time.Time
	next   atomic.Int64
	// partials counts the runs in a row that were not whole: the wait before
	// the next try doubles with each (retryWait).
	partials int
	// waiting: a run is due and a database does not answer; said once.
	waiting bool
	// saidRoot: the root's mode was found open to others and said (once).
	saidRoot bool
	// looked: this process has asked the disk when the last complete run was
	// (once, at its first Step — see Step).
	looked bool
	// targets in the order they are dumped: cold, hot, then config (dumpOrder).
	targets []target
	// lease keeps this gateway-wide role running in ONE deployment at a time
	// (internal/singleton). Nil or unreachable = it runs, which is the
	// single-install behaviour it has always had.
	lease  *singleton.Lease
	cry    *crypto.Service // seals body columns in the dump (D-104; nil = warned, plaintext)
	warned bool            // one-time "bodies not encrypted" warning latch
	// fellBack: accounts whose own data key could not be used in this
	// process, so their bodies were sealed under the gateway key (said once).
	fellBack map[int64]bool

	// job reports each backup run (2026-09-11). "When did the last good
	// backup finish" is the one question about backups that matters, and it
	// was answerable only from a log line.
	job monitor.Job
}

// MonitorSections publishes the backup runs: tables and rows per run, tables
// excluded (a table that could not be sealed is left out rather than written
// in plaintext), and when the next run is due.
func (p *Process) MonitorSections() map[string]any {
	sec := p.job.Section()
	sec["keep_runs"] = p.keep
	// The bodies are sealed in the dump only with the gateway key (dumpTable).
	// A crypto service built without one used to read "true" here while every
	// plain body went into the run as it was (go-live P8 A5).
	sec["encrypted_bodies"] = p.cry.HasDefaultKey()
	// When the next run is due: the run's own schedule (a whole run, a retry
	// after a short one, a wait for the databases), not the interval after
	// the last start.
	if n := p.next.Load(); n != 0 {
		next := time.Unix(0, n).UTC()
		sec["next_run_at"] = next.Format(time.RFC3339)
		sec["overdue"] = time.Since(next) > p.interval
	}
	return map[string]any{"backup": sec}
}

// dumpOrder is the order the databases are dumped in. Config goes LAST
// (2026-10-18, go-live P8 B3/C5): sealing a body may make the account's data
// key for the month (dek:<account>:<YYYYMM>) in the config store's data_keys,
// and a config dump taken before that did not hold the key — a run restored
// alone could not open the bodies it had sealed itself.
var dumpOrder = []string{"cold", "hot", "config"}

// New constructs the process from config.
func New(cfg config.Config) worker.Worker {
	p := &Process{
		cfg:      cfg,
		log:      logging.New("backup"),
		root:     env("YAKU_BACKUP_ROOT", "./backups"),
		keep:     envInt("YAKU_BACKUP_KEEP", 7),
		interval: time.Duration(envInt("YAKU_BACKUP_INTERVAL_SEC", 86400)) * time.Second,
	}
	// config + cold + hot (queue is transient/in-flight — excluded by design)
	dsns := map[string]string{"config": cfg.ConfigDSN, "cold": cfg.ColdDSN, "hot": cfg.HotDSN}
	for _, name := range dumpOrder {
		t := target{name: name, dsn: dsns[name]}
		if t.dsn == "" {
			continue
		}
		if d, err := dal.OpenTableDumps(t.dsn); err == nil {
			t.dump = d
			p.targets = append(p.targets, t)
			if t.name == "config" {
				// The lease lives on the config store — the one database
				// every deployment shares by definition.
				p.lease = singleton.New(singleton.LeasesOf(cfg), "backup", func(held bool) {
					p.log.Printf("backup lease %s", leaseWord(held))
				})
			}
		}
	}
	// Body-only artifact encryption (D-104, PO 2026-08-10: "backup should also
	// be encrypted body only"): message bodies are sealed in the dump even when
	// at-rest encryption is off in the source DB. Same keyring wiring as P4.
	if cs, err := dal.OpenDataKeys(cfg); err == nil {
		if c, err := crypto.FromEnv(context.Background(), cs, config.Secrets); err == nil {
			p.cry = c
		} else {
			p.log.Printf("crypto init: %v — backup bodies will NOT be encrypted", err)
		}
	}
	return p
}

// Name returns the process role name.
func (p *Process) Name() string { return "backup" }

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// leaseWord renders the gateway-wide lease change for the log.
func leaseWord(held bool) string {
	if held {
		return "taken — this deployment is running it"
	}
	return "lost — standing by"
}

// WithoutLease makes this copy work whether or not another holds the
// gateway-wide lease. It is for a caller that drives Step itself — a test
// harness beside a running stack, whose own copy of the role holds the lease
// for as long as it lives. Never for a role process: two copies of a
// gateway-wide role do the same work twice.
func (p *Process) WithoutLease() { p.lease = nil }
