package console

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/supervisor"
)

// Gateway processes (main menu g, 2026-09-29 — Saleem: "I may run the gateway
// and all its roles at once or one by one from the console, and may restart
// them"). The console talks to the supervisor that `yakugate` with no role
// runs (internal/supervisor), over its local control port. If none is
// running, it can start one in the background; leaving the console leaves
// the gateway running.

// gatewayClient is the supervisor at YAKU_CONTROL_ADDR, with YAKU_ADMIN_KEY.
// A test sets p.gw to a fake.
func (p *Process) gatewayClient() *supervisor.Client {
	if p.gw != nil {
		return p.gw
	}
	return supervisor.NewClient(os.Getenv("YAKU_CONTROL_ADDR"), os.Getenv("YAKU_ADMIN_KEY"))
}

func (p *Process) gatewayMenu(ctx context.Context, sc *bufio.Scanner) {
	c := p.gatewayClient()
	for {
		roles, err := c.Status(ctx)
		if err != nil {
			nc, ok := p.gatewayNotRunning(ctx, sc, c)
			if !ok {
				return
			}
			c = nc // it may answer on the port its own settings file names
			continue
		}
		p.printGatewayRoles(roles, c.Addr)
		if c.ConfigFile != "" {
			p.printf("  Settings: %s\n\n", c.ConfigFile)
		} else {
			p.printf("  Settings: environment variables only (no settings file)\n\n")
		}
		choice, ok := p.choose(sc, "Choose", "", []opt{
			item("1", "Restart a role"),
			item("2", "Stop a role"),
			item("3", "Start a role"),
			item("4", "Restart every role"),
			item("5", "Stop every role (the gateway stays up, ready to start them)"),
			item("6", "Start every role"),
			item("7", "Stop the gateway — every role, then the gateway itself"),
			item("w", "Watch live — the table redraws every second (q to stop)"),
			item("0", "Back"),
		})
		if !ok || choice == "0" {
			return
		}
		switch choice {
		case "1", "2", "3":
			action := map[string]string{"1": "restart", "2": "stop", "3": "start"}[choice]
			role, ok := p.pickRole(sc, roles, action)
			if !ok {
				continue
			}
			p.gatewayDo(ctx, c, role, action)
		case "4":
			p.gatewayDo(ctx, c, "all", "restart")
		case "5":
			if p.confirmWord(sc, "Stop every role? Messages stop flowing until they are started. Type yes") {
				p.gatewayDo(ctx, c, "all", "stop")
			}
		case "6":
			p.gatewayDo(ctx, c, "all", "start")
		case "7":
			if !p.confirmWord(sc, "Stop the whole gateway? Type yes") {
				continue
			}
			if err := c.Shutdown(ctx); err != nil {
				p.errf("  %v\n", err)
				continue
			}
			p.printf("  Stopping every role, then the gateway")
			for i := 0; i < 60 && c.Running(ctx); i++ {
				p.printf(".")
				time.Sleep(500 * time.Millisecond)
			}
			p.printf("\n")
			p.okf("  The gateway has stopped.\n")
			return
		case "w", "W":
			if !p.watchGateway(ctx, c) {
				return
			}
		case "":
			// Enter redraws the list.
		default:
			p.errf("  %q is not one of the choices.\n\n", choice)
		}
	}
}

// systemMenu is the main menu's Gateway entry: the gateway's processes, the
// live monitor and its settings, together (Saleem 2026-09-29). false = the
// person pressed Ctrl-C in the live monitor to leave the console.
func (p *Process) systemMenu(ctx context.Context, sc *bufio.Scanner) bool {
	for {
		p.printf("\n  Gateway\n\n")
		choice, ok := p.choose(sc, "Choose", "", []opt{
			item("1", "Processes   start, stop and restart the gateway and its roles"),
			item("2", "Monitor     live: what is running, traffic, queues and problems"),
			item("3", "Settings    yakugate.config: ports, databases, keys — a role reads them when it starts"),
			item("4", "Console password   change it, or set the three security questions"),
			item("5", "Deployments where clients connect: the HTTP address and SMPP host and ports customers are told"),
			item("6", "Receipts    the default receipt policy: what a client is told where its account has no rule"),
			item("0", "Back"),
		})
		if !ok || choice == "0" {
			return true
		}
		switch choice {
		case "1":
			p.gatewayMenu(ctx, sc)
		case "2":
			// Live, full screen and refreshing on any terminal, whichever
			// way menus are chosen; the static screen only for piped input.
			if p.tty != nil {
				if !p.liveMonitor(ctx, sc) {
					return false
				}
				break
			}
			p.monitoringMenu(ctx, sc)
		case "3":
			p.settingsMenu(sc)
		case "4":
			p.consolePasswordMenu(sc)
		case "5":
			p.deploymentsMenu(ctx, sc)
		case "6":
			p.receiptDefaultMenu(ctx, sc)
		case "":
		default:
			p.errf("  %q is not one of the choices.\n\n", choice)
		}
	}
}

// gatewayNotRunning offers to start the gateway. It returns the client to
// use from then on (the gateway answers where ITS settings file says), and
// false for back to the menu.
func (p *Process) gatewayNotRunning(ctx context.Context, sc *bufio.Scanner, c *supervisor.Client) (*supervisor.Client, bool) {
	p.printf("\n  The gateway is not running here (nothing answers on %s).\n", c.Addr)
	p.printf("  Roles started by hand, one per command, are not managed from this menu.\n\n")
	choice, ok := p.choose(sc, "Choose", "", []opt{
		item("1", "Start the gateway — every role, in the background"),
		item("2", "Start the gateway with no roles running — then choose which to start"),
		item("0", "Back"),
	})
	if !ok || (choice != "1" && choice != "2") {
		return c, false
	}
	idle := choice == "2"

	// Which settings file (Saleem 2026-09-29): asked every time, the default
	// being the one this console was started with — its --config, or the
	// yakugate.config it found.
	cfgFile, vals, ok := p.askSettingsFile(sc)
	if !ok {
		return c, false
	}
	// The gateway will answer where that file says, unless the environment
	// says otherwise (environment variables win over the file).
	addr := firstSet(envOf(config.OriginalEnviron(), "YAKU_CONTROL_ADDR"), vals["YAKU_CONTROL_ADDR"])
	key := firstSet(envOf(config.OriginalEnviron(), "YAKU_ADMIN_KEY"), vals["YAKU_ADMIN_KEY"])
	if p.gw == nil {
		c = supervisor.NewClient(addr, key)
		// That file may name another address, where a gateway — started
		// from the shell with -b, say — may already be running: use it
		// rather than start a second one that could only fail on the port.
		if answers, perr := c.Probe(ctx); answers {
			if perr != nil {
				p.errf("  %v\n", perr)
				return c, false
			}
			p.okf("  A gateway already answers on %s (the address in that file) — using it.\n", c.Addr)
			return c, true
		}
	}

	exe, err := supervisor.Executable()
	if err != nil {
		p.errf("  %v\n", err)
		return c, false
	}
	logFile := supervisor.LogFile(firstSet(envOf(config.OriginalEnviron(), "YAKU_LOG_DIR"), vals["YAKU_LOG_DIR"]))
	pid, err := p.startGateway(exe, cfgFile, logFile, config.OriginalEnviron(), idle)
	if err != nil {
		p.errf("  %v\n", err)
		return c, false
	}
	p.printf("  Started (pid %d); its output goes to %s. Waiting for it to answer on %s", pid, logFile, c.Addr)
	// The same judgement as `yakugate --background` (supervisor.WaitUp): up
	// only when this start answers, a gateway that died at once is reported
	// with its log's last lines, and one that never answers is stopped.
	err = p.waitGateway(ctx, c, pid, logFile)
	p.printf("\n")
	switch {
	case err == nil:
		if idle {
			p.okf("  The gateway is up with every role stopped — choose 3 to start one.\n")
		} else {
			p.okf("  The gateway is up.\n")
		}
		return c, true
	case errors.Is(err, supervisor.ErrNotUp):
		supervisor.StopStarted(pid)
		p.errf("  It did not answer within 15 seconds, so it was stopped — look at %s.\n", logFile)
	default:
		p.errf("  %v\n", err)
	}
	return c, false
}

// waitGateway is supervisor.WaitUp with the console's dots. A test's pretend
// start (startGW) has no process behind its pid — which may even be some
// other real process on the box — so then only the port is watched, and
// nothing is waited on or signalled.
func (p *Process) waitGateway(ctx context.Context, c *supervisor.Client, pid int, logFile string) error {
	if p.startGW == nil {
		return supervisor.WaitUp(ctx, c, pid, logFile, 15*time.Second, func() { p.printf(".") })
	}
	for i := 0; i < 30; i++ {
		if c.Running(ctx) {
			return nil
		}
		p.printf(".")
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("it has not answered after 15 seconds — look at %s", logFile)
}

// askSettingsFile asks which settings file the gateway starts with. Enter
// takes the one this console runs on. The file is read and checked first, as
// the gateway would, so a wrong path or a bad line is caught here rather than
// by a gateway that then does not come up. "" = environment variables only.
func (p *Process) askSettingsFile(sc *bufio.Scanner) (string, map[string]string, bool) {
	def := config.LoadedFile()
	for {
		if def != "" {
			p.printf("\n  Settings file: %s\n", def)
			p.printf("  Enter to use it, or type the path of another yakugate.config (0 to go back).\n")
		} else {
			p.printf("\n  This console found no settings file; the gateway would run on environment variables only.\n")
			p.printf("  Type the path of a yakugate.config, or Enter to go on without one (0 to go back).\n")
		}
		v, ok := p.ask(sc, "  Settings file")
		if !ok || v == "0" {
			return "", nil, false
		}
		path := strings.TrimSpace(v)
		if path == "" {
			path = def
		}
		if path == "" {
			return "", map[string]string{}, true
		}
		if strings.HasPrefix(path, "~/") {
			if home, err := os.UserHomeDir(); err == nil {
				path = filepath.Join(home, path[2:])
			}
		}
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
		if info, err := os.Stat(path); err != nil || info.IsDir() {
			p.errf("  There is no file at %s.\n", path)
			continue
		}
		vals, err := config.ReadFile(path)
		if err != nil {
			p.errf("  %v\n", err)
			continue
		}
		p.okf("  Using %s (%d settings).\n", path, len(vals))
		return path, vals, true
	}
}

// envOf is NAME's value in an environment list, "" if it is not there.
func envOf(env []string, name string) string {
	for _, kv := range env {
		if k, v, ok := strings.Cut(kv, "="); ok && k == name {
			return v
		}
	}
	return ""
}

func firstSet(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// startGateway is supervisor.StartDetached; a test replaces it.
func (p *Process) startGateway(exe, cfgFile, logFile string, env []string, idle bool) (int, error) {
	if p.startGW != nil {
		return p.startGW(exe, cfgFile, logFile, env, idle)
	}
	return supervisor.StartDetached(exe, cfgFile, logFile, env, idle)
}

// gatewayDo runs one action and shows every role's state as it changes —
// stopping, starting, running — until the roles it touched have settled
// (Saleem 2026-09-29: "live status … against each role"). On a terminal the
// table is redrawn in place; piped output gets the settled table once.
func (p *Process) gatewayDo(ctx context.Context, c *supervisor.Client, role, action string) {
	what := role
	if role == "all" {
		what = "every role"
	}
	title := fmt.Sprintf("%s %s", strings.ToUpper(action[:1])+action[1:], what)
	errc := make(chan error, 1)
	go func() {
		_, err := c.Do(ctx, role, action)
		errc <- err
	}()

	live := p.tty != nil
	drawn := 0
	var last []supervisor.RoleStatus
	var callErr error
	finished := false
	deadline := time.Now().Add(90 * time.Second)
	for {
		if !finished {
			select {
			case callErr = <-errc:
				finished = true
			default:
			}
		}
		if st, err := c.Status(ctx); err == nil {
			last = st
		}
		settled := finished && (callErr != nil || gatewaySettled(last, role, action))
		if live && last != nil {
			head := "  " + title + " — live"
			if settled {
				head = "  " + title
			}
			drawn = p.redraw(drawn, append([]string{head, ""}, p.gatewayLines(last, c.Addr, role)...))
		}
		if settled || time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !live && last != nil {
		p.printf("\n  %s\n", title)
		for _, ln := range p.gatewayLines(last, c.Addr, role) {
			p.printf("%s\n", ln)
		}
	}
	switch {
	case callErr != nil:
		p.errf("  %v\n", callErr)
	case !finished || !gatewaySettled(last, role, action):
		p.warnf("  Still settling after 90 seconds — Watch live (w) shows where each role is.\n")
	default:
		p.okf("  Done: %s %s.\n", action, what)
	}
}

// gatewaySettled: the roles an action touched have reached where it sends
// them — stopped for stop; running (or crashed and waiting to start again,
// or found running on another deployment, which the table then shows) for
// start and restart.
func gatewaySettled(roles []supervisor.RoleStatus, role, action string) bool {
	for _, r := range roles {
		if role != "all" && r.Role != role {
			continue
		}
		switch action {
		case "stop":
			if r.State != supervisor.StateStopped {
				return false
			}
		default:
			if !(r.State == supervisor.StateRunning && r.PID > 0) && r.State != supervisor.StateWaiting &&
				r.State != supervisor.StateElsewhere {
				return false
			}
		}
	}
	return true
}

// redraw replaces the last `drawn` lines on the terminal with lines, and
// returns how many it drew.
func (p *Process) redraw(drawn int, lines []string) int {
	if drawn > 0 {
		p.printf("\x1b[%dA", drawn)
	}
	for _, ln := range lines {
		p.printf("\r\x1b[2K%s\n", ln)
	}
	return len(lines)
}

// watchGateway keeps the role table on screen, redrawn every second, until q,
// Esc or 0. false = the person pressed Ctrl-C to leave the console.
func (p *Process) watchGateway(ctx context.Context, c *supervisor.Client) bool {
	if p.tty == nil {
		// Piped: one reading, as the menu itself shows.
		if st, err := c.Status(ctx); err == nil {
			for _, ln := range p.gatewayLines(st, c.Addr, "") {
				p.printf("%s\n", ln)
			}
		}
		return true
	}
	keys, _ := p.keys.(timedKeys)
	if keys == nil {
		keys = &ttyKeys{f: p.tty}
	}
	if err := keys.begin(); err != nil {
		return true
	}
	defer keys.end()
	p.printf("\x1b[?1049h\x1b[?25l") // alternate screen, cursor hidden
	defer p.printf("\x1b[?25h\x1b[?1049l")
	for {
		p.printf("\x1b[H\x1b[2J")
		st, err := c.Status(ctx)
		if err != nil {
			p.errf("  %v\n", err)
		} else {
			for _, ln := range p.gatewayLines(st, c.Addr, "") {
				p.printf("\r%s\n", ln)
			}
		}
		p.printf("\r  Redraws every second.  q, Esc or 0: back   Ctrl-C: leave the console\n")
		k, got, err := keys.nextWithin(time.Second)
		if err != nil {
			return true
		}
		if got {
			switch k {
			case "q", "Q", "0", "\x1b":
				return true
			case "\x03", "\x04":
				return false
			}
		}
		if ctx.Err() != nil {
			return true
		}
	}
}

// pickRole lists the roles to act on.
func (p *Process) pickRole(sc *bufio.Scanner, roles []supervisor.RoleStatus, action string) (string, bool) {
	opts := make([]opt, 0, len(roles)+1)
	for i, r := range roles {
		opts = append(opts, wideItem(fmt.Sprint(i+1), fmt.Sprintf("%-14s %s", r.Role, r.State)))
	}
	opts = append(opts, wideItem("0", "Back"))
	choice, ok := p.choose(sc, "Which role to "+action, "", opts)
	if !ok || choice == "0" || choice == "" {
		return "", false
	}
	for i, r := range roles {
		if choice == fmt.Sprint(i+1) || strings.EqualFold(choice, r.Role) {
			return r.Role, true
		}
	}
	p.errf("  %q is not one of the roles.\n", choice)
	return "", false
}

// confirmWord asks for the word yes before something that stops traffic.
func (p *Process) confirmWord(sc *bufio.Scanner, prompt string) bool {
	v, ok := p.ask(sc, "  "+prompt)
	if ok && strings.EqualFold(strings.TrimSpace(v), "yes") {
		return true
	}
	p.printf("  Nothing was changed.\n")
	return false
}

// printGatewayRoles draws the role table once.
func (p *Process) printGatewayRoles(roles []supervisor.RoleStatus, addr string) {
	p.printf("\n")
	for _, ln := range p.gatewayLines(roles, addr, "") {
		p.printf("%s\n", ln)
	}
	p.printf("\n")
}

// gatewayLines is the role table: state, process, how long it has been up
// (or when it starts again), restarts and how it last ended. A role in the
// middle of a change is red while stopping or starting, green once running,
// yellow when stopped, cyan when another deployment runs it (a gateway-wide
// role: billing, retention, backup, abuse-monitor — the line says where, and
// when the gateway looks again). mark names the role an action is on ("all"
// = every one), shown with an arrow.
func (p *Process) gatewayLines(roles []supervisor.RoleStatus, addr, mark string) []string {
	out := []string{
		fmt.Sprintf("  Gateway processes — control on %s", addr),
		"",
		fmt.Sprintf("    %-14s %-11s %8s  %-12s %8s  %s", "ROLE", "STATE", "PID", "UP / NEXT", "RESTARTS", "LAST EXIT"),
	}
	up, down, away := 0, 0, 0
	for _, r := range roles {
		pid, when := "", ""
		if r.PID > 0 {
			pid = fmt.Sprint(r.PID)
		}
		switch r.State {
		case supervisor.StateRunning:
			up++
			when = shortDuration(time.Since(r.Since))
		case supervisor.StateWaiting:
			down++
			when = "in " + shortDuration(time.Until(r.NextStart))
		case supervisor.StateElsewhere:
			away++
			when = "looking now"
			if d := time.Until(r.NextStart); d > 0 {
				when = "look in " + shortDuration(d)
			}
		default:
			down++
		}
		last := r.LastExit
		if last != "" && !r.LastExitAt.IsZero() {
			last += ", " + shortDuration(time.Since(r.LastExitAt)) + " ago"
		}
		if r.State == supervisor.StateElsewhere {
			// Where it runs, or why it is not here, instead of how it last
			// ended: that is the news.
			where := r.Elsewhere
			if where == "" {
				where = "another deployment"
			}
			last = supervisor.ElsewhereWords(where, r.Why) + " — checks every minute"
		}
		arrow := "  "
		if mark == "all" || mark == r.Role {
			arrow = "→ "
		}
		line := fmt.Sprintf("  %s%-14s %-11s %8s  %-12s %8d  %s", arrow, r.Role, r.State, pid, when, r.Restarts, last)
		switch r.State {
		case supervisor.StateRunning:
			line = p.paint(colGreen, line)
		case supervisor.StateStopped:
			line = p.paint(colYellow, line)
		case supervisor.StateElsewhere:
			line = p.paint(colCyan, line)
		default: // starting, stopping, restarting
			line = p.paint(colRed, line)
		}
		out = append(out, line)
	}
	if away > 0 {
		out = append(out, "", fmt.Sprintf("  %d running, %d running on another deployment, %d not running.", up, away, down))
	} else {
		out = append(out, "", fmt.Sprintf("  %d running, %d not running.", up, down))
	}
	return out
}

// shortDuration is a duration as a person reads it: 45s, 12m, 3h 5m, 2d 4h.
func shortDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd %dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}
