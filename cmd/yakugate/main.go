// Command gateway runs any Yakugate process by role:
//
//	yakugate [--config FILE] <role>
//
// Settings come from yakugate.config: the FILE given with --config (or -c), else
// the first yakugate.config found in the start folder, the binary's folder, the
// user's config folder, or /etc/yakugate — in each place the old name,
// sms.config, is still read when there is no yakugate.config there (renamed
// 2026-10-02). Environment variables override any line in it.
//
// One binary, one image; deploy it N times with different roles. Each process
// lives in its own package under internal/process/<name> with its own
// definition (definition.go) and workings (workings.go).
//
// Transport edges are named <protocol><side>: httpserver / smppserver face the
// end-user client; httpvendor / smppvendor face the upstream vendor.
package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/platform/version"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/mysql"
	_ "github.com/yakugate/yakugate/internal/dal/redis"
	// Every DAL this binary can run on registers itself (internal/dal); the
	// SQL Server one serves nothing until step 4 of the DAL plan.
	_ "github.com/yakugate/yakugate/internal/dal/mssql"
	"github.com/yakugate/yakugate/internal/logcfg"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
	"github.com/yakugate/yakugate/internal/process/abusemonitor"
	"github.com/yakugate/yakugate/internal/process/backup"
	"github.com/yakugate/yakugate/internal/process/blackhole"
	"github.com/yakugate/yakugate/internal/process/console"
	"github.com/yakugate/yakugate/internal/process/dbworker"
	"github.com/yakugate/yakugate/internal/process/dlr"
	"github.com/yakugate/yakugate/internal/process/httpserver"
	"github.com/yakugate/yakugate/internal/process/httpvendor"
	moproc "github.com/yakugate/yakugate/internal/process/mo"
	"github.com/yakugate/yakugate/internal/process/retention"
	"github.com/yakugate/yakugate/internal/process/smppserver"
	"github.com/yakugate/yakugate/internal/process/smppvendor"
	"github.com/yakugate/yakugate/internal/singleton"
	"github.com/yakugate/yakugate/internal/storewatch"
	"github.com/yakugate/yakugate/internal/supervisor"
)

// registry maps a role name to its process constructor.
var registry = map[string]func(config.Config) worker.Worker{
	// transport edges
	// [SCOPE P1-00] Submit API role dispatch (endpoint code lives in handler.go)
	"http-server": httpserver.New, // end-user HTTP ingress (Phase 1)
	"smpp-server": smppserver.New, // end-user SMPP ingress
	"http-vendor": httpvendor.New, // upstream HTTP egress + DLR
	"smpp-vendor": smppvendor.New, // upstream SMPP egress + DLR
	"blackhole":   blackhole.New,  // vendor simulator: records + DLRs, never transmits
	"dlr":         dlr.New,        // delivery-receipt receiver: webhook intake + correlate + client push
	"mo":          moproc.New,     // MO receiver: HTTP vendor MO webhook + mo_url delivery (P6)
	// pipeline & lifecycle
	// finalizer dropped (D-030): records move to hot inline at terminal state;
	// the empty router and importer scaffolds were removed 2026-09-17 (PO) —
	// routing lives in http-server, HLR import in the Admin API (D-049).
	"retention": retention.New,
	"backup":    backup.New,
	// The retry and expiry roles are retired (issue 22, 27 Oct 2026): a
	// vendor role refuses a message out of tries or past its validity when it
	// takes it from its queue (internal/giveup; supervisor.Retired).
	// The billing role (P9) is retired (Saleem, 26 Oct 2026): money is
	// booked at the moment it is decided and the db-worker books the billing
	// queue (supervisor.Retired).
	// P14, the DB worker (Saleem 2026-10-13, D-131): the one writer of the
	// records when the roles queue them (recordq, YAKU_RECORDS_VIA=worker).
	"db-worker": dbworker.New,

	// anti-abuse (async)
	"abuse-monitor": abusemonitor.New,

	// operator surface (PO 2026-09-18): the menu that configures a
	// gateway from a terminal, for an install that has no portal. Its own
	// process — it opens its own handles, does what it is asked and exits,
	// sharing nothing with the running roles.
	"console": console.New,
}

// roles lists the services a gateway runs. The console is in the registry —
// it is built like a process — but it is a menu a person opens, not a
// service to deploy, so help lists it with the commands (bug 14, 2026-09-21).
func roles() []string {
	rs := make([]string, 0, len(registry))
	for r := range registry {
		if r == "console" {
			continue
		}
		rs = append(rs, r)
	}
	sort.Strings(rs)
	return rs
}

// errHelp is returned by parseArgs when help was asked for.
var errHelp = errors.New("help")

// errVersion: "yakugate --version" — print what this build is and stop (VERSIONS.md §1.3, PO 2026-09-17).
var errVersion = errors.New("version")

// parseArgs reads "yakugate [--config FILE] <role>". The config flag may come
// before or after the role, and is written --config FILE, --config=FILE,
// -config FILE or -c FILE.
func parseArgs(args []string) (role, cfgPath string, err error) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-h" || a == "--help" || a == "-help" || a == "help":
			return "", "", errHelp
		case a == "-v" || a == "--version" || a == "-version" || a == "version":
			return "", "", errVersion
		case a == "-c" || a == "--config" || a == "-config":
			if i+1 >= len(args) || args[i+1] == "" {
				return "", "", fmt.Errorf("%s needs a file path", a)
			}
			i++
			cfgPath = args[i]
		case strings.HasPrefix(a, "--config=") || strings.HasPrefix(a, "-config="):
			cfgPath = a[strings.IndexByte(a, '=')+1:]
			if cfgPath == "" {
				return "", "", fmt.Errorf("%s needs a file path", a)
			}
		case strings.HasPrefix(a, "-"):
			return "", "", fmt.Errorf("unknown option %q", a)
		default:
			if role != "" {
				return "", "", fmt.Errorf("one role at a time: got %q and %q", role, a)
			}
			role = a
		}
	}
	// No role: the whole gateway (2026-09-29) — every role, supervised.
	return role, cfgPath, nil
}

// takeIdle removes --idle from the arguments: the whole gateway starts with
// every role stopped, for an operator to start them one by one (2026-09-29).
func takeIdle(args []string) ([]string, bool) {
	out := make([]string, 0, len(args))
	idle := false
	for _, a := range args {
		if a == supervisor.IdleFlag {
			idle = true
			continue
		}
		out = append(out, a)
	}
	return out, idle
}

// takeBackground removes --background / -b from the arguments: the whole
// gateway starts in the background and the command returns (2026-10-01).
func takeBackground(args []string) ([]string, bool) {
	out := make([]string, 0, len(args))
	bg := false
	for _, a := range args {
		if a == BackgroundFlags[0] || a == BackgroundFlags[1] {
			bg = true
			continue
		}
		out = append(out, a)
	}
	return out, bg
}

func usage(w *os.File) {
	fmt.Fprintln(w, "usage: yakugate [--config FILE] [--idle] [--background]")
	fmt.Fprintln(w, "                                                the whole gateway: every role, each restarted if it stops")
	fmt.Fprintln(w, "                                                (--idle: start with every role stopped, then start them from the console)")
	fmt.Fprintln(w, "                                                (--background, -b: start it detached, wait until it answers, and return;")
	fmt.Fprintln(w, "                                                 it keeps running after the terminal closes — manage or stop it from")
	fmt.Fprintln(w, "                                                 a console started with the same settings: yakugate [--config FILE] console)")
	fmt.Fprintln(w, "       yakugate [--config FILE] <role>          one role   |   yakugate --version")
	fmt.Fprintln(w, "       yakugate [--config FILE] console          the setup menu")
	fmt.Fprintln(w, "       yakugate [--config FILE] migrate [--seed]")
	fmt.Fprintln(w, "       yakugate [--config FILE] config [list [--all] | get NAME | set NAME=VALUE... [--new] | unset NAME... | path] [--show-secrets]")
	fmt.Fprintln(w, "       yakugate backup-verify <run-dir>")
	fmt.Fprintln(w, "       yakugate [--config FILE] backup-restore <run-dir> --db config|cold|hot [--table NAME]... [--into-nonempty] [--dry-run]")
	fmt.Fprintln(w, "settings: FILE, else the first "+config.FileName+" found in:")
	for _, p := range config.DefaultPaths() {
		fmt.Fprintln(w, "  "+p)
	}
	fmt.Fprintln(w, "(in each folder the old name "+config.OldFileName+" is still read when there is no "+config.FileName+")")
	fmt.Fprintln(w, "environment variables override any line in the file")
	fmt.Fprintln(w, "roles:", roles())
	fmt.Fprintln(w, "the whole gateway runs YAKU_ROLES (default: every role) and is controlled from the console's Gateway menu")
}

func main() {
	// Maintenance commands first: they are not roles and they exit when done.
	// config goes first of all: it is picked by position, while the backup
	// and migrate parsers match their word anywhere — `config set X=migrate`
	// must not run a migration.
	if ct, cerr := parseConfigTool(os.Args[1:]); cerr != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+cerr.Error()))
		usage(os.Stderr)
		os.Exit(2)
	} else if ct != nil {
		os.Exit(ct.run())
	}
	if tool, terr := parseBackupTool(os.Args[1:]); terr != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+terr.Error()))
		usage(os.Stderr)
		os.Exit(2)
	} else if tool != nil {
		os.Exit(tool.run())
	}
	if mt, merr := parseMigrateTool(os.Args[1:]); merr != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+merr.Error()))
		usage(os.Stderr)
		os.Exit(2)
	} else if mt != nil {
		os.Exit(mt.run())
	}
	args, idle := takeIdle(os.Args[1:])
	args, background := takeBackground(args)
	role, cfgPath, err := parseArgs(args)
	if errors.Is(err, errVersion) {
		fmt.Println("yakugate " + version.String())
		return
	}
	if errors.Is(err, errHelp) {
		usage(os.Stdout)
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+err.Error()))
		usage(os.Stderr)
		os.Exit(2)
	}
	if role == "" {
		if background {
			os.Exit(startBackground(cfgPath, idle))
		}
		os.Exit(runGateway(cfgPath, idle))
	}
	if idle {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: --idle is for the whole gateway (no role)"))
		os.Exit(2)
	}
	if background {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: --background is for the whole gateway (no role) — set YAKU_ROLES to run only some roles"))
		os.Exit(2)
	}
	ctor, ok := registry[role]
	if !ok {
		if why, retired := supervisor.Retired[role]; retired {
			// A process manager that still starts it is told why, and the
			// exit is a clean one: there is nothing to restart.
			fmt.Fprintln(os.Stderr, yellow(os.Stderr, fmt.Sprintf("yakugate: the %s role is retired — %s", role, why)))
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, red(os.Stderr, fmt.Sprintf("unknown role %q; roles: %v", role, roles())))
		os.Exit(1)
	}

	// The settings file goes into the environment before anything reads it.
	// A bad file stops the start: running on half the settings, or on some
	// other file than the one named, is worse than not running.
	cfgFile, err := config.LoadFile(cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+err.Error()))
		os.Exit(1)
	}

	useRoleAdminAddr(role)

	cfg := config.Load()
	cfg.Role = role

	// Half a Sentinel setup, or Sentinel mixed with a plain Redis address,
	// stops a role here: running it without Redis, or on a server the
	// Sentinels may have demoted, is worse than not running. The console only
	// warns, so its Settings menu can still be used to put it right.
	if p := cfg.RedisProblem(); p != "" {
		if role != "console" {
			fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+p))
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, yellow(os.Stderr, "yakugate: "+p))
	}
	// The SQL stores: one database type per install, each store's DSN of
	// its type, and a DAL that serves every area (sqlcheck.go). Same rule as
	// Redis above: a role stops, the console warns.
	if p := sqlProblem(cfg, true); p != "" {
		if role != "console" {
			fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+p))
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, yellow(os.Stderr, "yakugate: "+p))
	}

	// One connection pool per database for this whole process, sized for the
	// role (dbpool: YAKU_DB_MAX_OPEN / _MAX_IDLE / _IDLE_SEC lay over the
	// role's default). Set before anything opens a database.
	dbpool.SetProgram("yakugate-" + role)
	dbpool.Configure(dbpool.LimitsFor(role))
	// The read DSN beside each store (YAKU_<STORE>_READ_DSN, one per store):
	// reads that may be a moment old go there (dbpool.Reader).
	dbpool.SetReaders(cfg.Readers())
	// Where the messages waiting for a receipt are kept (D-136, Saleem
	// 2026-10-14: "Redis if set, else MySQL, never both"): with Redis, the
	// receipt facts in the main Redis; without it, open_msgs.
	mysql.SetOpenMessagesInMySQL(!cfg.RedisEnabled())

	// A gateway-wide role the supervisor starts looks first: when another
	// deployment already runs it, it says where on one stderr line and leaves
	// before it builds anything — the supervisor shows it as running there
	// and starts it again in a minute (Saleem 2026-10-01: "don't start, auto
	// takeover"). Run by hand, it stands by as it always has.
	if singleton.GatewayWide(role) && singleton.Supervised() {
		line, leave := startCheck(cfg, role)
		fmt.Fprintln(os.Stderr, line)
		if leave {
			os.Exit(singleton.ExitElsewhere)
		}
	}

	// Process 13 file logging. Set up before the process is built so a
	// constructor's own start-up lines are captured too. An unwritable log
	// directory costs one warning on stderr and nothing else — logging must
	// never be the reason a role fails to start.
	// [1.9.2] Set BEFORE Configure, so even the start-up lines below are
	// covered — including the one that announces the setting itself.
	logging.SetRedaction(cfg.LogRedact)
	// The console is a person at a terminal, not a service. It writes no log
	// file and prints no start-up lines: an operator running it from bin/ was
	// met with a permission warning about /var/log/yakugate and three status
	// lines before the menu appeared (2026-09-18). A role's log tells an
	// operator what a background process did; a console's output IS what it
	// did, and it is on the screen already.
	logDir := cfg.LogDir
	if role == "console" {
		logDir = ""
	}
	logging.Configure(logDir, role, logging.Config{
		Level:         logging.ParseLevel(cfg.LogLevel, logging.LevelInfo),
		MaxSizeMB:     cfg.LogMaxSizeMB,
		RetentionDays: cfg.LogRetentionDays,
	})
	if role != "console" {
		switch {
		case cfgFile == "":
			logging.New(role).Printf("no %s found; settings from environment variables only", config.FileName)
		case config.IsLegacyFile(cfgFile):
			logging.New(role).Printf("settings from %s (old name: rename it to %s)", cfgFile, config.FileName)
		default:
			logging.New(role).Printf("settings from %s", cfgFile)
		}
	}
	if !logging.RedactionEnabled() {
		logging.New(role).Printf("YAKU_LOG_REDACT=off — subscriber numbers and one-time codes are written to the logs in full")
	}
	if rd := cfg.ReadInstances(); len(rd) > 0 && role != "console" {
		// Where reads that may be a moment old go (the address only, never a
		// login). Everything else is read where it is written.
		logging.New(role).Printf("a read instance is set: %s", strings.Join(rd, ", "))
	}
	defer logging.Close()

	w := ctor(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Per-account and per-vendor log levels, refreshed every 30s. Started for
	// EVERY role from here rather than inside each process: P1/P2 have the
	// refdata cache but P3/P4/P5/P6 read vendors with their own queries, and
	// all of them write tenant-scoped log records. MySQL only — the two
	// SELECTs are plain SQL, so a Mongo config store keeps the process-wide
	// default for everything (a known limit, same as MSSQL/Mongo parity
	// elsewhere).
	// Not for the console: it writes no role log and no capture records, and
	// with its databases unreachable these refreshers printed over its setup
	// screen every 30 seconds (2026-10-02).
	if cfg.ConfigBackend == config.BackendMySQL && cfg.ConfigDSN != "" && role != "console" {
		// On the process's one pool for the config store (dal.OpenLogSettings:
		// dbpool), like everything else that reads it: these refreshers ask a
		// few rows every few seconds and need no connections of their own.
		if db, err := dal.OpenLogSettings(cfg); err == nil {
			logcfg.Start(ctx, db, logging.New(role).Printf)
			// Running captures on their own, much faster tick: the shortest
			// window is one minute, so the 30s config cadence would lose half
			// of it (Process 13 Type 3).
			logcfg.StartCaptures(ctx, db, logging.New(role).Printf)
			// Carry capture records to where the portal can read them. The
			// gateway's own capture file is written regardless of this.
			logcfg.StartShipper(ctx, db, logging.New(role).Printf)
		} else {
			fmt.Fprintf(os.Stderr, "[logging] per-account/vendor levels unavailable: %v\n", err)
		}
	}

	// Delete log files past their retention, hourly. Each file is judged by
	// its own account's or vendor's setting, so this has to run after the
	// config refresher above has had its first read.
	logging.StartSweep(ctx, time.Hour)

	// Fleet-wide monitoring (2026-07-26, [SCOPE P1-STATUS]): every role — not
	// just the HTTP edge — gets probes and a status page.
	start := time.Now()
	instance := statusInstance()
	// Both listeners answer /readyz by one rule (bug 16).
	monitor.ReadyCheck = worker.Ready
	// Monitoring: one timer, two sinks. Redis carries the LIVE cluster view
	// (status:<role>:<inst>, TTL 3x the interval, so a dead instance vanishes
	// by itself); MySQL carries the durable HISTORY the portal replays.
	// Either may be absent — a deployment without Redis still records history,
	// one without a hot store still shows live.
	//
	// Not for the console (#26, 2026-09-21): it is a person at a terminal, not
	// a service, and it used to appear among the gateway's running roles.
	if role != "console" {
		every := monitorInterval()
		// The live view: the main Redis (dal.OpenLiveStatus), nil without it.
		var rdb dal.LiveStatus
		if cfg.RedisEnabled() {
			if l, err := dal.OpenLiveStatus(cfg); err == nil {
				rdb = l
			}
			// The receipts owed per vendor are read from the receipt facts'
			// waiting sets by the dlr role (2026-10-15); monitoring keeps no
			// copy of them in Redis any more.
		}
		var hist *monitor.SQLSink
		var histDB dal.MonitorStatements
		if os.Getenv("YAKU_MONITOR_HISTORY") != "off" {
			// The hot store when it is MySQL and named, on a small pool of its
			// own (dal.OpenMonitorHistory): this writes one row every few
			// seconds and must never compete with the traffic path for
			// connections.
			if h, err := dal.OpenMonitorHistory(cfg); err == nil {
				hist = monitor.NewSQLSink(h).WithRetention(monitorRetentionDays()).
					WithSummaryDays(envDays("YAKU_MONITOR_SUMMARY_DAYS", monitor.DefaultSummaryDays)).
					WithEventRules(monitorEventRules())
				histDB = h
			}
		}
		// The health page's gateway overview reads the other roles from the
		// same places (set before the listener starts).
		monitor.SetOverviewSources(rdb, histDB)
		// MySQL and Redis are read by whichever role holds its deployment's
		// turn (monitor/storeturn.go); each role can, so a deployment with no
		// http-server still reports them.
		monitor.SetStoreReader(func() monitor.StoreReader { return storewatch.New(cfg, storewatch.RulesFromEnv()) })
		if rdb != nil || hist != nil {
			go monitor.StartPush(ctx, rdb, hist, role, instance, start, w, every,
				func(f string, a ...any) { fmt.Fprintf(os.Stderr, "[monitor] "+f+"\n", a...) })
		}
	}
	if addr := os.Getenv("YAKU_ADMIN_ADDR"); addr != "" && role != "console" {
		// Per-process admin listener: /healthz, /readyz, /status (key-gated),
		// and the HTML health page at / and /health.
		go func() {
			if err := monitor.ServeAdmin(ctx, addr, role, instance,
				os.Getenv("YAKU_STATUS_KEY"), start, w); err != nil {
				fmt.Fprintf(os.Stderr, "admin listener failed: %v\n", err)
			}
		}()
	}

	err = worker.Run(ctx, w, 500*time.Millisecond)
	// The money the role's last moments decided goes to the billing queue
	// before the process ends (billing.Moments books off the callers'
	// goroutines).
	billing.DrainAll(3 * time.Second)
	if err != nil && err != context.Canceled {
		var ex worker.Exit
		if errors.As(err, &ex) {
			// Ended on purpose (worker.Run has logged why): a gateway-wide
			// role another deployment won while this one was starting, or
			// took over after this one lost its connection.
			if m, ok := ex.(interface{ Marker() string }); ok {
				fmt.Fprintln(os.Stderr, m.Marker())
			}
			stop()
			logging.Close()
			os.Exit(ex.ExitCode())
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// startCheck is the start-up look of a supervised gateway-wide role
// (singleton.AtStart) on its own short-lived connection to the config store,
// where the leases live. Anything that stops it asking — no DSN, a store that
// does not answer within three seconds — lets the role start: fail open, as
// the lease itself does.
func startCheck(cfg config.Config, role string) (line string, leave bool) {
	if cfg.ConfigDSN == "" {
		return singleton.HereMarker(role), false
	}
	// The process's one pool for the config store (dal.OpenLeases: dbpool):
	// the role it is about to build uses the same connection, so asking costs
	// none of its own.
	leases, err := dal.OpenLeases(cfg)
	if err != nil {
		// The deployments register cannot be read: the role waits
		// (2026-10-26), and the supervisor looks again in a minute.
		return singleton.WaitMarker(role), true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return singleton.AtStart(ctx, leases, role)
}

// statusInstance identifies this process in the cluster status view.
func statusInstance() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}

// monitorRetentionDays is how long monitoring history is kept (PO 2026-09-11:
// raw samples for 7 days, no rollups). 0 keeps everything.
func monitorRetentionDays() int {
	if n, err := strconv.Atoi(os.Getenv("YAKU_MONITOR_RETENTION_DAYS")); err == nil && n >= 0 {
		return n
	}
	return monitor.DefaultRetentionDays
}

// monitorInterval is the sampling rate (PO 2026-09-09: every 5 seconds, full
// snapshot). YAKU_STATUS_PUSH_SEC keeps its old name so existing deployments
// are not surprised by a rename.
//
// Never under monitor.MinPushInterval (5 s, Saleem 2026-10-05): the finest
// moment the monitoring screens can show, and a floor that keeps the history
// a reasonable size.
func monitorInterval() time.Duration {
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_STATUS_PUSH_SEC"))); err == nil && n > 0 {
		if d := time.Duration(n) * time.Second; d > monitor.MinPushInterval {
			return d
		}
	}
	return monitor.MinPushInterval
}

// envDays reads a whole number of days; unset or bad = def, 0 = for ever.
func envDays(name string, def int) int {
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name))); err == nil && n >= 0 {
		return n
	}
	return def
}

// monitorEventRules reads what counts as a monitoring event (D-126); each
// setting unset or unreadable keeps its default.
func monitorEventRules() monitor.EventRules {
	r := monitor.DefaultEventRules()
	r.Off = strings.EqualFold(strings.TrimSpace(os.Getenv("YAKU_MONITOR_EVENTS")), "off")
	num := func(name string, dst *float64) {
		if f, err := strconv.ParseFloat(strings.TrimSpace(os.Getenv(name)), 64); err == nil && f >= 0 {
			*dst = f
		}
	}
	whole := func(name string, dst *int64) {
		if n, err := strconv.ParseInt(strings.TrimSpace(os.Getenv(name)), 10, 64); err == nil && n >= 0 {
			*dst = n
		}
	}
	whole("YAKU_MONITOR_QUEUE_AGE_SEC", &r.QueueAgeSec)
	whole("YAKU_MONITOR_QUEUE_DEPTH", &r.QueueDepth)
	num("YAKU_MONITOR_LATENCY_MS", &r.LatencyMS)
	num("YAKU_MONITOR_REJECT_PCT", &r.RejectPct)
	num("YAKU_MONITOR_REJECT_MIN_TPS", &r.RejectMinTPS)
	num("YAKU_MONITOR_CPU_PCT", &r.CPUPct)
	num("YAKU_MONITOR_MEM_PCT", &r.MemPct)
	num("YAKU_MONITOR_TRAFFIC_MIN_TPS", &r.TrafficMinTPS)
	num("YAKU_MONITOR_SPIKE_FACTOR", &r.SpikeFactor)
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_MONITOR_HOLD"))); err == nil && n >= 1 {
		r.Hold = n
	}
	return r
}

// useRoleAdminAddr lets one yakugate.config give every role on a host its own
// admin port (2026-09-21): YAKU_ADMIN_ADDR_HTTP_SERVER=:9080,
// YAKU_ADMIN_ADDR_BILLING=:9088 … Roles on one host cannot share a port, and
// the settings file — not per-role environment files — is where settings live.
// The role's own line wins over a plain YAKU_ADMIN_ADDR.
func useRoleAdminAddr(role string) {
	name := worker.AdminAddrEnv + "_" + roleKey(role)
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		_ = os.Setenv(worker.AdminAddrEnv, v)
	}
}

// roleKey writes a role as a setting-name suffix: http-server → HTTP_SERVER.
func roleKey(role string) string {
	return strings.ToUpper(strings.ReplaceAll(role, "-", "_"))
}
