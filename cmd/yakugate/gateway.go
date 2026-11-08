package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/singleton"
	"github.com/yakugate/yakugate/internal/supervisor"
)

// runGateway is `yakugate` with no role (2026-09-29): every role of this
// gateway, each in its own child process, started again whenever it stops,
// and controlled from the console's Gateway menu over the local control port.
// It returns the exit status.
func runGateway(cfgPath string, idle bool) int {
	cfgFile, err := config.LoadFile(cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+err.Error()))
		return 1
	}
	// Every role would refuse these Redis settings one by one, restarted
	// every minute; say it once, before starting any.
	if p := config.Load().RedisProblem(); p != "" {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+p))
		return 1
	}
	// The same for the SQL stores (sqlcheck.go).
	if p := sqlProblem(config.Load(), true); p != "" {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+p))
		return 1
	}
	chosen, err := supervisor.Pick(roles(), os.Getenv("YAKU_ROLES"))
	if err != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+err.Error()))
		return 1
	}
	for _, r := range supervisor.RetiredIn(os.Getenv("YAKU_ROLES")) {
		fmt.Fprintln(os.Stderr, yellow(os.Stderr, fmt.Sprintf("yakugate: YAKU_ROLES names %q, which is retired and is not started: %s", r, supervisor.Retired[r])))
	}
	// The db-worker books this deployment's billing queue (26 Oct 2026):
	// without one, money events wait in Redis and no usage is booked.
	hasWorker := false
	for _, r := range chosen {
		if r == "db-worker" {
			hasWorker = true
		}
	}
	if !hasWorker {
		where := "Redis"
		if !config.Load().RedisEnabled() {
			where = "MySQL (billing_events)" // no Redis: the queue is the config store's (DAL step 3)
		}
		fmt.Fprintln(os.Stderr, yellow(os.Stderr, "yakugate: the roles this gateway runs (YAKU_ROLES) do not include db-worker — this deployment's billing events wait in "+where+", and no usage is booked, until a db-worker of this deployment runs"))
	}
	// The roles hand their records to the DB worker and this gateway runs
	// none: said once, here. Not refused — another host of the same
	// deployment may run it — but until one does, every record waits in Redis.
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("YAKU_RECORDS_VIA")), "direct") {
		has := false
		for _, r := range chosen {
			if r == "db-worker" {
				has = true
			}
		}
		if !has {
			fmt.Fprintln(os.Stderr, yellow(os.Stderr, "yakugate: the records go through the DB worker (YAKU_RECORDS_VIA is not direct), and the roles this gateway runs (YAKU_ROLES) do not include db-worker — records wait in Redis until a db-worker of this deployment is running"))
		}
	}
	exe, err := supervisor.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+err.Error()))
		return 1
	}
	addr := strings.TrimSpace(os.Getenv("YAKU_CONTROL_ADDR"))
	if addr == "" {
		addr = supervisor.DefaultControlAddr
	}
	// The control port first: a second `yakugate` on this host stops here,
	// before it starts a second copy of every role.
	ln, err := supervisor.Listen(addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+err.Error()))
		return 1
	}

	// Each role runs this binary with its name and reads the settings file
	// itself, with the environment as it was before the file was read — so a
	// change saved to yakugate.config reaches a role when it is restarted.
	// YAKU_SUPERVISED tells a gateway-wide role it was started here: when
	// another deployment runs it, it leaves (singleton.ExitElsewhere) instead
	// of idling as a standby, and the supervisor asks again every minute.
	// Built once — every launch shares the slice and never appends to it.
	env := append(config.OriginalEnviron(), singleton.EnvSupervised+"=1")
	launch := func(role string) *exec.Cmd {
		var args []string
		if cfgFile != "" {
			args = append(args, "--config", cfgFile)
		}
		cmd := exec.Command(exe, append(args, role)...)
		cmd.Env = env
		// Every role's lines carry its own [component] tag, so one stream
		// still reads clearly; each role also keeps its own log file.
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		return cmd
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	sup := supervisor.New(chosen, launch, os.Stdout)
	sup.StartNone = idle
	sup.ConfigFile = cfgFile

	where := cfgFile
	if where == "" {
		where = "environment variables only"
	}
	verb := "running"
	if idle {
		verb = "managing (all stopped until started from the console)"
	}
	fmt.Printf("[gateway] yakugate: %s %d roles (%s); settings from %s; control on %s\n",
		verb, len(chosen), strings.Join(chosen, ", "), where, addr)
	if os.Getenv("YAKU_ADMIN_KEY") == "" && !strings.HasPrefix(addr, "127.") && !strings.HasPrefix(addr, "localhost") {
		fmt.Println(yellow(os.Stdout, "[gateway] YAKU_ADMIN_KEY is not set and the control port is not on 127.0.0.1 — anyone who can reach "+addr+" can stop the gateway"))
	}

	go func() {
		if err := supervisor.Serve(ctx, ln, supervisor.Handler(sup, os.Getenv("YAKU_ADMIN_KEY"), stop)); err != nil {
			fmt.Fprintln(os.Stderr, "[gateway] control port: "+err.Error())
		}
	}()
	_ = sup.Run(ctx)
	fmt.Println("[gateway] every role stopped")
	return 0
}

// BackgroundFlags start the whole gateway in the background and return
// (2026-10-01): `yakugate [--config FILE] [--idle] --background`. It is the
// console's "Start the gateway" (Gateway → Processes) without the console.
var BackgroundFlags = []string{"--background", "-b"}

// startBackground starts the whole gateway detached — in its own session, so
// it outlives this command and the terminal — and waits until it answers on
// its control port. The settings are read here first, as the gateway reads
// them, so a missing file or a bad line is reported now rather than in a log
// nobody is watching. It returns the exit status.
func startBackground(cfgPath string, idle bool) int {
	fail := func(msg string) int {
		fmt.Fprintln(os.Stderr, red(os.Stderr, "yakugate: "+msg))
		return 1
	}
	cfgFile, err := config.LoadFile(cfgPath)
	if err != nil {
		return fail(err.Error())
	}
	if p := config.Load().RedisProblem(); p != "" {
		return fail(p)
	}
	if p := sqlProblem(config.Load(), true); p != "" {
		return fail(p)
	}
	if _, err := supervisor.Pick(roles(), os.Getenv("YAKU_ROLES")); err != nil {
		return fail(err.Error())
	}
	exe, err := supervisor.Executable()
	if err != nil {
		return fail(err.Error())
	}
	// Where the new gateway will answer: its settings file, unless the
	// environment says otherwise — the order the gateway itself reads them in.
	key := os.Getenv("YAKU_ADMIN_KEY")
	c := supervisor.NewClient(strings.TrimSpace(os.Getenv("YAKU_CONTROL_ADDR")), key)
	ctx := context.Background()
	// Anything answering there — a gateway, one with another key, or some
	// other program — means this start could only fail.
	if answers, perr := c.Probe(ctx); answers {
		if perr != nil {
			return fail(perr.Error() + " — stop it first, or set another YAKU_CONTROL_ADDR")
		}
		return fail("a gateway already answers on " + c.Addr + " — manage it with: " + consoleCommand(cfgFile))
	}
	logFile := supervisor.LogFile(strings.TrimSpace(os.Getenv("YAKU_LOG_DIR")))
	if abs, err := filepath.Abs(logFile); err == nil {
		logFile = abs
	}
	// The gateway's own warning goes to its log; the person starting it in
	// the background would never see it there.
	if strings.TrimSpace(key) == "" && !strings.HasPrefix(c.Addr, "127.") && !strings.HasPrefix(c.Addr, "localhost") {
		fmt.Println(yellow(os.Stdout, "yakugate: YAKU_ADMIN_KEY is not set and the control port is not on 127.0.0.1 — anyone who can reach "+c.Addr+" can stop the gateway"))
	}
	// The gateway is started with the environment as it was before the
	// settings file was read, so it reads the file itself — and so does every
	// role it starts, again at each restart.
	pid, err := supervisor.StartDetached(exe, cfgFile, logFile, config.OriginalEnviron(), idle)
	if err != nil {
		return fail(err.Error())
	}
	where := cfgFile
	if where == "" {
		where = "environment variables only"
	}
	fmt.Printf("yakugate: starting the gateway in the background (pid %d)\n", pid)
	fmt.Printf("  settings: %s\n  log:      %s\n  control:  %s\n", where, logFile, c.Addr)
	switch err := supervisor.WaitUp(ctx, c, pid, logFile, 15*time.Second, nil); {
	case err == nil:
	case errors.Is(err, supervisor.ErrNotUp):
		// Never left half-started: one that cannot be reached cannot be
		// managed or stopped from the console either.
		supervisor.StopStarted(pid)
		return fail(fmt.Sprintf("the gateway (pid %d) did not answer on %s within 15 seconds, so it was stopped — look at %s", pid, c.Addr, logFile))
	default:
		return fail(err.Error())
	}
	if idle {
		fmt.Println(green(os.Stdout, "yakugate: the gateway is up with every role stopped — start them from the console (Gateway → Processes)"))
	} else {
		fmt.Println(green(os.Stdout, "yakugate: the gateway is up"))
	}
	fmt.Println("  manage or stop it: " + consoleCommand(cfgFile) + "  → Gateway → Processes")
	return 0
}

// consoleCommand is how to open a console that reaches this gateway: the
// console takes the control address and key from ITS settings file, so it
// must be started with the same one.
func consoleCommand(cfgFile string) string {
	if cfgFile == "" {
		return "yakugate console"
	}
	return "yakugate --config " + cfgFile + " console"
}
