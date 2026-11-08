// Package abusemonitor implements the "abuse-monitor" process (P10/D-034):
// asynchronous abuse and fraud detection over the traffic stream. Each sweep it
// evaluates the abuse_rules signals (ait / spend / spam / template) over sliding
// windows of hot records, takes the per-account MAX severity, and acts through
// the config store — warn (audit only), throttle (set abuse_throttle_tps), or
// suspend (is_disabled=1) — with every action recorded in audit_log. Actions
// propagate to the accept path via the refdata cache refresh: the monitor
// nudges nobody, the cache's own fingerprint sees its writes
// (refdata.MySQL.ChangeSignature).
//
// Definition: types and construction. The workings (input -> logic -> output)
// live in workings.go.
package abusemonitor

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/abusecount"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/singleton"
)

// Process is the "abuse-monitor" process. [SCOPE P10]
type Process struct {
	cfg config.Config
	log *logging.Logger
	hot dal.RecordStore // ListRecordTables (partition discovery)
	// data is what the monitor reads and writes (dal.AbuseData): the rules,
	// accounts, audit rows and reputation on the config store's pool, the
	// window and aggregate queries over the partitions on the hot store's.
	// Those scans go to the hot store's read instance when it has one
	// (dbpool.Reader, 2026-10-13): they are the heaviest reads the gateway
	// makes, and a window that is a moment old judges the same. Its own
	// writes and the reads behind them (rules, audit rows, suspensions — the
	// config store) stay on the write instance.
	data *dal.AbuseData
	// prices are the rate versions a log row names (issue 16: the rows keep
	// rate ids, not amounts), for the spend signal read from the tables.
	prices *billing.Prices
	// ref is the reference cache's own loader: spam rules and templates are
	// read through it, so the monitor reads them as the accept path does.
	ref *refdata.MySQL
	// fx turns the spend floor, an amount in the base currency, into each
	// company's own (billing.ToBase). It never waits for a rate provider.
	fx *billing.FXResolver
	// cursor is where each reader of message texts starts its next sweep
	// when the last one hit its ceiling (sampleMsgs).
	cursor map[string]int
	// baseKept is each signal's baseline as last read from the tables, used
	// for a while before it is read again (workings.go keptBaseline).
	baseKept map[string]keptBase
	// counts reads the accept path's abuse counters (Redis; nil without).
	// view is the sweep's look at them while a sweep is judged from the
	// counters, nil while it is judged from the records (counters.go).
	counts *abusecount.Reader
	view   *countView
	// lease keeps this gateway-wide role running in ONE deployment at a time
	// (internal/singleton). Nil or unreachable = it runs, which is the
	// single-install behaviour it has always had.
	lease    *singleton.Lease
	interval time.Duration // min time between sweeps
	lastRun  time.Time
	cry      *crypto.Service // decrypt bodies when scanning (nil = plaintext) [P1-CRYPTO]
	stats    sweepStats      // fleet status-page section [SCOPE P1-STATUS]
	over     overrides       // per-account rule overrides, reloaded each sweep (config/0106)
	// lastReputation is when the daily reputation scores were last written
	// (config/0107); zero = read it from the table.
	lastReputation time.Time
	// The look for a reset runs beside the sweep (watchResets): repMu keeps
	// the two from scoring at once, leaseOK tells the watcher this copy is
	// the one running the role, resetWatch starts it once.
	repMu      sync.Mutex
	leaseOK    atomic.Bool
	resetWatch sync.Once
}

// New constructs the process from config.
func New(cfg config.Config) worker.Worker {
	p := &Process{
		cfg:      cfg,
		log:      logging.New("abuse-monitor"),
		interval: time.Duration(envInt("YAKU_ABUSE_INTERVAL_SEC", 60)) * time.Second,
	}
	if h, err := dal.OpenRecordStore(cfg); err == nil {
		p.hot = h
	}
	if d, err := dal.OpenAbuseData(cfg); err == nil {
		p.data = d
		p.lease = singleton.New(singleton.LeasesOf(cfg), "abuse-monitor", func(held bool) {
			p.log.Printf("abuse-monitor lease %s", leaseWord(held))
		})
		if bs, err := dal.OpenBilling(cfg); err == nil {
			p.fx = billing.NewFXResolver(bs.Store).NoWait()
			p.prices = billing.NewPrices(bs.Store)
		} else {
			p.log.Printf("billing prices unavailable: %v", err)
		}
	}
	// The accept roles' counters (dal.CountersOn: the main Redis when set).
	if a, err := dal.OpenAbuseCounts(cfg); err == nil {
		p.counts = abusecount.NewReader(a)
	}
	if r, err := refdata.For(cfg); err == nil {
		p.ref = r
	} else {
		p.log.Printf("reference loader: %v — the spam and template signals cannot read their rules", err)
	}
	// Content signals (spam, template drift) read message bodies. If at-rest
	// encryption is on, an un-decrypting monitor would score ciphertext and
	// silently stop detecting abuse for exactly the accounts that opted into
	// encryption — so the monitor decrypts when it scans. [SCOPE P1-CRYPTO]
	if cs, err := dal.OpenDataKeys(cfg); err == nil {
		if c, err := crypto.FromEnv(context.Background(), cs, config.Secrets); err == nil {
			p.cry = c
		} else {
			p.log.Printf("crypto init: %v", err)
		}
	}
	return p
}

// Name returns the process role name.
func (p *Process) Name() string { return "abuse-monitor" }

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
