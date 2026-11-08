// Package retention implements the "retention" gateway process: it moves aged hot
// MDR partitions into the cold archive database and drops them from hot.
//
// Definition: types and construction. The workings (input -> logic -> output)
// live in workings.go.
package retention

import (
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/singleton"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
)

// Process is the "retention" process (hot -> cold mover). [SCOPE P8]
type Process struct {
	cfg config.Config
	log *logging.Logger
	hot dal.RecordStore
	// upkeep is what this role keeps in the config store (the days kept in
	// hot, the ledger), on the DAL; nil without a config store.
	upkeep dal.RetentionUpkeep
	// lease keeps this gateway-wide role running in ONE deployment at a time
	// (internal/singleton). Nil or unreachable = it runs, which is the
	// single-install behaviour it has always had.
	lease      *singleton.Lease
	coldDB     string // cold archive schema name (same server as hot)
	hotDays    int    // days a company's records stay in hot when it sets none (YAKU_HOT_DAYS)
	saidNoCold bool   // the log has said there is no cold store to archive to
	// coldSrv is the cold DSN's SERVER (no database: it may not exist yet)
	// and hotSrv the hot store; their @@server_uuid must be the same
	// before a month is moved (coldProblem). saidCold is the last reason
	// logged for not moving.
	coldSrv, hotSrv dal.ServerIdentity
	saidCold        string
	concatHours     int           // age after which an unfinished concat group is retired
	interval        time.Duration // min time between sweeps
	lastRun         time.Time

	// job reports each archive pass (2026-09-11). Retention DROPS hot tables,
	// so "did the last pass run, and did any partition refuse verification"
	// is a question someone must be able to answer without reading logs.
	job monitor.Job
}

// MonitorSections publishes the archive passes: tables and rows moved to cold
// per run, verification refusals, and when the next pass is due.
func (p *Process) MonitorSections() map[string]any {
	sec := p.job.Section()
	sec["hot_days"] = p.hotDays
	sec["cold_schema"] = p.coldDB
	return map[string]any{"retention": sec}
}

// New constructs the process from config.
func New(cfg config.Config) worker.Worker {
	p := &Process{
		cfg:         cfg,
		log:         logging.New("retention"),
		coldDB:      schemaFromDSN(cfg.ColdDSN),
		hotDays:     hotDaysSetting(),
		concatHours: envInt("YAKU_CONCAT_STALE_HOURS", 48),
		interval:    time.Duration(envInt("YAKU_RETENTION_INTERVAL_SEC", 3600)) * time.Second,
	}
	if h, err := dal.OpenRecordStore(cfg); err == nil {
		p.hot = h
	}
	// The cold DSN's server and the hot store (when the cold DSN names a
	// server), and the upkeep on the config store's pool: dal.OpenRetention.
	r, err := dal.OpenRetention(cfg)
	if err != nil {
		r = &dal.Retention{}
	}
	p.coldSrv, p.hotSrv = r.ColdServer, r.HotServer
	if r.Upkeep != nil {
		p.upkeep = r.Upkeep
		p.lease = singleton.New(singleton.LeasesOf(cfg), "retention", func(held bool) {
			p.log.Printf("retention lease %s", leaseWord(held))
		})
	}
	return p
}

// Name returns the process role name.
func (p *Process) Name() string { return "retention" }

// schemaFromDSN extracts the bare schema name from a MySQL DSN
// (…/yakugate_cold?params -> yakugate_cold). Empty if the DSN has none.
func schemaFromDSN(dsn string) string {
	i := strings.LastIndex(dsn, "/")
	if i < 0 {
		return ""
	}
	s := dsn[i+1:]
	if j := strings.IndexByte(s, '?'); j >= 0 {
		s = s[:j]
	}
	return s
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
