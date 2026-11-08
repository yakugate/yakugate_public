// Package console implements the "console" gateway process: the menu an
// operator uses to configure a gateway from a terminal, with no portal.
//
// Definition: types and construction. The workings (input -> logic -> output)
// live in workings.go.
//
// WHY IT IS A PROCESS LIKE THE OTHERS (PO 2026-09-18)
//
// It is a role on the same binary, built the same way as P1..P13 — definition
// here, workings beside it — so there is one place to look for any process and
// one shape to learn. `yakugate console` is its own process: it opens its own
// database handles, does what the operator asks, and exits. It shares nothing
// with the running roles but the file on disk, so it cannot disturb traffic.
//
// It is a Runnable, not a Step worker: it waits on a person, not on a timer.
//
// # WHAT IT CHANGES, AND WHETHER A RESTART IS NEEDED
//
// Accounts, vendors and routes live in the config store, which every role
// re-reads within 30 seconds (refdata's fingerprint check). So a change made
// here reaches a running gateway on its own — no restart, no nudge. Only
// `yakugate.config` (ports, database addresses, keys) needs a restart; the
// Settings menu edits it (settings.go) and says so on every screen.
//
// [SCOPE P14-00]
package console

import (
	"context"
	"fmt"
	"github.com/yakugate/yakugate/internal/supervisor"
	"io"
	"os"
	"strings"

	"github.com/yakugate/yakugate/internal/hlr"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
	"github.com/yakugate/yakugate/internal/provision"

	"github.com/yakugate/yakugate/internal/dal"
)

// Process is the "console" process: an interactive configuration menu. [SCOPE P14]
type Process struct {
	// requireLogin asks for the console password before the menu (auth.go);
	// New sets it, so every real console asks. Tests that drive the menus
	// build a Process directly and leave it off; the login has its own tests.
	requireLogin bool
	authFile     string // the console password file (authPath)

	// gw and startGW are the Gateway menu's supervisor client and its
	// "start the gateway" (gateway.go); nil = the real ones. Tests set fakes.
	gw      *supervisor.Client
	startGW func(exe, cfgFile, logFile string, env []string, idle bool) (int, error)

	cfg   config.Config
	log   *logging.Logger
	store provision.Store

	// in and out are the terminal, held as interfaces so the menus can be
	// driven by a test with a scripted set of answers — a menu nobody can
	// test is a menu that rots.
	in  io.Reader
	out io.Writer

	// opened are the handles this process owns and must close on the way out.
	opened []io.Closer
	// live is the main Redis's live view (monitoring), when Redis is on; the
	// live balance counters are p.store.Live, on the same client.
	live dal.LiveStatus

	// cursor: choices are made with the arrow keys (picker.go). Only possible
	// when tty is set — input and output are both a real terminal; a piped
	// script or a test always gets the numbered menus. keys replaces the
	// terminal's key reader in a test.
	cursor bool
	tty    *os.File
	keys   keySource

	// settingsFile replaces the loaded yakugate.config in a test (settings.go).
	settingsFile string

	// hot is the monitoring history in the hot store, opened only when
	// Redis is off (monitoring.go).
	hot dal.MonitorReader
	// mon is what the monitoring screens show: which gateway, which time,
	// whether Live is on (monitor_view.go).
	mon monView

	// ranges is the gateway's number-range index, loaded on the first
	// number lookup of a session (operators.go).
	ranges *hlr.Ranges

	// ended is set when the input closed (EOF, or Ctrl-C in cursor mode):
	// nothing more can be asked. A flow that keeps a draft through a
	// refusal uses it to tell "the operator backed out of one question"
	// (keep the draft, show it again) from "nobody is there" (stop).
	ended bool
}

// New builds the console. It does NOT open databases yet: a constructor that
// dialled would make `yakugate console` fail before printing anything when a
// store is down, and the operator's first need in that case is to be told
// which store and why. Run does the connecting, and says so.
func New(cfg config.Config) worker.Worker {
	return &Process{
		cfg:          cfg,
		log:          logging.New("console"),
		in:           os.Stdin,
		out:          os.Stdout,
		requireLogin: true,
		authFile:     authPath(),
	}
}

// Name is the role name. [SCOPE P14-00]
func (p *Process) Name() string { return "console" }

// connect opens the config and queue stores. Only MySQL: the console writes
// configuration, and config on MSSQL or Mongo is Phase 2 (GW-40/GW-41), so
// saying that plainly beats a driver error further down.
func (p *Process) connect() error {
	if p.cfg.ConfigBackend != config.BackendMySQL {
		return fmt.Errorf("the console configures a MySQL config store; this gateway is set to %q (MSSQL and Mongo are Phase 2)", p.cfg.ConfigBackend)
	}
	// Say the real cause before the driver says a confusing one. With no
	// settings file the DSN is empty, and MySQL then reports "Access denied
	// for user ''@'localhost'" — which sends an operator hunting for a
	// permissions problem that does not exist (2026-09-18).
	if strings.TrimSpace(p.cfg.ConfigDSN) == "" {
		return fmt.Errorf("%s", p.noSettingsHelp())
	}
	cfgStore, err := dal.OpenConfig(p.cfg)
	if err != nil {
		return fmt.Errorf("open the config store: %w", err)
	}
	p.opened = append(p.opened, cfgStore)
	p.store = provision.Store{DB: cfgStore}

	// The queue store is optional to OPEN but required to create a vendor,
	// because a vendor's queue table lives there. A console that can still
	// manage accounts with the queue store down is more useful than one that
	// refuses to start.
	if p.cfg.QueueStoreMySQL() {
		if q, err := dal.OpenVendorQueueTables(p.cfg); err == nil {
			if c, ok := q.(io.Closer); ok {
				p.opened = append(p.opened, c)
			}
			p.store.Queues = q
		}
	}

	// With Redis on, live balances are held there and written back over
	// MySQL, so a top-up must reach Redis too. The client is opened whenever
	// Redis is configured; if it is then unreachable, a top-up is refused
	// rather than saved to MySQL alone and silently erased 30 seconds later.
	if p.cfg.RedisEnabled() {
		if lp, err := dal.OpenLiveParts(p.cfg); err == nil {
			p.opened = append(p.opened, lp)
			p.live = lp.Status
			p.store.Live = lp.Balances
			// Live on the monitoring screens asks the roles to report now; with
			// Redis they read the ask there (monitor/reportnow.go).
			monitor.UseRedisForAsks(lp.Status)
		}
	}
	return nil
}

// numberRanges is where the number-range index loads from (hlr.Ranges,
// operators.go): the HLR parts of the engine's config on the config store's
// handle under p.store. nil when there is none.
func (p *Process) numberRanges() dal.HLRLookupConfig {
	if s, ok := p.store.DB.(dal.HLRLookupOf); ok {
		return s.HLRLookup()
	}
	return nil
}

// Interactive tells the process framework this one talks to a person: no
// start-up banner above the menu, and no health listener, because there is
// nothing to watch and it ends when the operator does. [SCOPE P14-00]
func (p *Process) Interactive() {}

// Step exists only to satisfy worker.Worker, which the role registry is typed
// on. The framework type-asserts Runnable first and calls Run, so this is
// never reached — the console waits on a person, not on a 500ms timer.
func (p *Process) Step(context.Context) error { return nil }

// close releases every handle this process opened.
func (p *Process) close() {
	for _, h := range p.opened {
		_ = h.Close()
	}
	p.opened = nil
	p.live = nil
}

// noSettingsHelp explains, in an operator's terms, that there is nothing to
// connect to and what to do about it — including where the gateway looked.
func (p *Process) noSettingsHelp() string {
	var b strings.Builder
	if f := config.LoadedFile(); f != "" {
		fmt.Fprintf(&b, "the settings file %s has no YAKU_CONFIG_DSN, so there is no config store to open.", f)
	} else {
		b.WriteString("no settings file was found, so there is no config store to open.\n")
		b.WriteString("  Looked for " + config.FileName + " (or the old name " + config.OldFileName + ") in:\n")
		for _, path := range config.DefaultPaths() {
			b.WriteString("    " + path + "\n")
		}
		b.WriteString("  Give one with --config FILE, or set YAKU_CONFIG_DSN in the environment.")
	}
	return b.String()
}
