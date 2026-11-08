// Package supervisor runs a whole gateway from one command (2026-09-29,
// Saleem: "if no role is given to the binary then run all by default … I may
// run the gateway and all its roles at once or one by one from the console,
// and may restart them").
//
// `yakugate` with no role starts a supervisor. It runs every role as its own
// CHILD PROCESS — the same binary, one role each — so a role that crashes, or
// is restarted, never takes another with it, and each keeps the process it
// has always had: its own connection pools, its own health and its own log.
// (One process for all roles was considered and set aside for now: the roles
// share state that is only safe one role per process — see the 2026-09-29
// audit in CONTINUE.md.)
//
// A role that exits is started again, always, after a wait that doubles from
// one second up to a minute and resets once the role has stayed up for a
// minute. A role an operator STOPPED stays stopped until it is started.
//
// A gateway-wide role (billing, retention, backup, abuse-monitor — one
// deployment at a time, internal/singleton) that another deployment already
// runs is not kept here as an idle copy (Saleem 2026-10-01: "don't start,
// auto takeover"): it says so on a marker line and exits ExitElsewhere, and
// it is shown as running elsewhere ("elsewhere", with where) and started
// again every minute until the other one stops. That is not a crash — no
// restart is counted and the wait does not grow.
//
// The supervisor answers on a local control port (YAKU_CONTROL_ADDR, default
// 127.0.0.1:8099) — the console's Gateway menu uses it:
//
//	GET  /roles                          every role: state, pid, uptime, restarts
//	                                     (and, state "elsewhere", where it runs instead)
//	POST /roles/<role|all>/start|stop|restart
//	POST /shutdown                       stop every role, then the supervisor
//
// With YAKU_ADMIN_KEY set, every request must carry it in yaku-admin-key.
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yakugate/yakugate/internal/singleton"
)

// Role states, as the console shows them.
const (
	StateStarting = "starting"
	StateRunning  = "running"
	StateWaiting  = "restarting" // exited; waiting before it is started again
	StateStopping = "stopping"
	StateStopped  = "stopped"
	// StateElsewhere: a gateway-wide role another deployment runs. Not
	// running here, wanted, looked at again every ElsewhereRecheck.
	StateElsewhere = "elsewhere"
)

// ElsewhereWords says why a role is not running here, in words: "runs on
// lhr-1 (host gw2)", "belongs to tw-1 (the deployments register lists it
// there)", "waits: the deployments register cannot be read".
func ElsewhereWords(where, why string) string {
	switch why {
	case singleton.WhyHome:
		return "belongs to " + where + " (the deployments register lists it there)"
	case singleton.WhyNoList:
		return "waits: the deployments register cannot be read"
	}
	return "runs on " + where
}

// ElsewhereRecheck is how often a role running on another deployment is
// started again to see whether that one has stopped.
const ElsewhereRecheck = time.Minute

// checkSettle is how long a gateway-wide role may run without saying whether
// it stays (singleton.MarkerHere) before it is shown as running anyway.
const checkSettle = 10 * time.Second

// Stable is how long a role must stay up before its restart wait resets.
const Stable = time.Minute

// Launcher builds the command that runs one role. The real one runs this
// binary with the role name; tests give one that runs something harmless.
type Launcher func(role string) *exec.Cmd

// RoleStatus is one role, as GET /roles reports it.
type RoleStatus struct {
	Role       string    `json:"role"`
	State      string    `json:"state"`
	Wanted     bool      `json:"wanted"` // should be running
	PID        int       `json:"pid,omitempty"`
	Since      time.Time `json:"since,omitzero"` // when it entered this state
	Restarts   int       `json:"restarts"`       // times it was started again after exiting
	LastExit   string    `json:"last_exit,omitempty"`
	LastExitAt time.Time `json:"last_exit_at,omitzero"`
	NextStart  time.Time `json:"next_start,omitzero"` // while restarting, or the next look while elsewhere
	// Elsewhere is where the role runs instead, in words ("lhr-1 (host gw2,
	// pid 4121)", or "another deployment"), while the state is elsewhere.
	Elsewhere string `json:"elsewhere,omitempty"`
	// Why says why it is not here (2026-10-26): "home" = the deployments
	// register lists it on Elsewhere's deployment and it never runs here;
	// "nolist" = the register cannot be read and it waits; "" = another
	// session holds its lease.
	Why string `json:"why,omitempty"`
}

type role struct {
	name     string
	wanted   bool
	state    string
	cmd      *exec.Cmd
	proc     *os.Process // set under the lock once the process has started
	since    time.Time
	restarts int
	backoff  time.Duration
	lastExit string
	lastAt   time.Time
	next     time.Time
	// elsewhere: where the role runs instead, while in StateElsewhere; why:
	// why it is not here (RoleStatus.Why).
	elsewhere string
	why       string
	done      chan struct{} // closed when the current process has exited
	wake      chan struct{} // a start request cuts a restart wait short
	// gen changes on every start and stop, so the loop keeping a role alive
	// knows when it has been replaced — a stop then an immediate start must
	// never leave two loops starting the same role.
	gen int
}

// Supervisor runs a set of roles as child processes.
type Supervisor struct {
	launch   Launcher
	out      io.Writer
	stopWait time.Duration
	minWait  time.Duration
	maxWait  time.Duration
	recheck  time.Duration // ElsewhereRecheck
	settle   time.Duration // checkSettle
	// leased says which roles hold a gateway-wide lease, and so may report
	// that they run elsewhere (singleton.GatewayWide; tests give their own).
	leased func(role string) bool

	// StartNone leaves every role stopped when Run begins: the gateway is up
	// and answering, and an operator starts the roles they want one by one
	// (`yakugate --idle`, the console's "start the gateway with no roles").
	StartNone bool
	// ConfigFile is the settings file the roles are started with ("" =
	// environment variables only), reported to the console.
	ConfigFile string

	mu    sync.Mutex
	roles map[string]*role
	order []string
	wg    sync.WaitGroup
	ctx   context.Context
}

// New builds a supervisor for these roles, in this order. Nothing starts
// until Run.
func New(roles []string, launch Launcher, out io.Writer) *Supervisor {
	s := &Supervisor{launch: launch, out: out, stopWait: 20 * time.Second,
		minWait: time.Second, maxWait: time.Minute, recheck: ElsewhereRecheck, settle: checkSettle,
		leased: singleton.GatewayWide, roles: map[string]*role{}}
	for _, r := range roles {
		if _, dup := s.roles[r]; dup {
			continue
		}
		s.roles[r] = &role{name: r, state: StateStopped, wake: make(chan struct{}, 1)}
		s.order = append(s.order, r)
	}
	return s
}

// Roles lists the roles this supervisor manages, in start order.
func (s *Supervisor) Roles() []string { return append([]string(nil), s.order...) }

// Run starts every role and keeps them running until ctx ends; then it stops
// them all and returns.
func (s *Supervisor) Run(ctx context.Context) error {
	s.mu.Lock()
	s.ctx = ctx
	s.mu.Unlock()
	if !s.StartNone {
		for _, name := range s.order {
			if err := s.Start(name); err != nil {
				s.logf("%s: %v", name, err)
			}
		}
	}
	<-ctx.Done()
	s.StopAll()
	s.wg.Wait()
	return nil
}

// Start marks a role wanted and starts it (now, even if it was waiting to be
// restarted). Starting a running role changes nothing.
func (s *Supervisor) Start(name string) error {
	s.mu.Lock()
	r, ok := s.roles[name]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("no role %q (roles: %s)", name, strings.Join(s.order, ", "))
	}
	if s.ctx == nil || s.ctx.Err() != nil {
		s.mu.Unlock()
		return errors.New("the gateway is shutting down")
	}
	if r.wanted {
		// Running, starting or waiting to restart: a wait is cut short.
		select {
		case r.wake <- struct{}{}:
		default:
		}
		s.mu.Unlock()
		return nil
	}
	r.wanted = true
	r.backoff = 0
	r.gen++
	gen := r.gen
	s.wg.Add(1)
	s.mu.Unlock()
	go s.keep(r, gen)
	return nil
}

// Stop stops a role and keeps it stopped until it is started again. It waits
// until the process has exited.
func (s *Supervisor) Stop(name string) error {
	s.mu.Lock()
	r, ok := s.roles[name]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("no role %q (roles: %s)", name, strings.Join(s.order, ", "))
	}
	r.wanted = false
	r.gen++
	proc, done := r.proc, r.done
	if proc != nil && r.state != StateStopped {
		r.state, r.since = StateStopping, time.Now()
	}
	select {
	case r.wake <- struct{}{}: // leave a restart wait at once
	default:
	}
	s.mu.Unlock()
	if proc != nil && done != nil {
		s.terminate(proc, done)
	}
	return nil
}

// Restart stops a role and starts it again.
func (s *Supervisor) Restart(name string) error {
	if err := s.Stop(name); err != nil {
		return err
	}
	s.waitStopped(name)
	return s.Start(name)
}

// StartAll, StopAll and RestartAll act on every role, in order (stopping in
// reverse, so the edge roles that take traffic go first).
func (s *Supervisor) StartAll() {
	for _, n := range s.order {
		_ = s.Start(n)
	}
}

func (s *Supervisor) StopAll() {
	var wg sync.WaitGroup
	for i := len(s.order) - 1; i >= 0; i-- {
		n := s.order[i]
		wg.Add(1)
		go func() { defer wg.Done(); _ = s.Stop(n) }()
	}
	wg.Wait()
}

func (s *Supervisor) RestartAll() {
	s.StopAll()
	for _, n := range s.order {
		s.waitStopped(n)
	}
	s.StartAll()
}

// Status reports every role, in order.
func (s *Supervisor) Status() []RoleStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RoleStatus, 0, len(s.order))
	for _, n := range s.order {
		r := s.roles[n]
		// UTC on the wire, as every other time the gateway answers with.
		st := RoleStatus{Role: n, State: r.state, Wanted: r.wanted, Since: r.since.UTC(), Restarts: r.restarts,
			LastExit: r.lastExit, LastExitAt: r.lastAt.UTC()}
		if r.proc != nil && (r.state == StateRunning || r.state == StateStopping || r.state == StateStarting) {
			st.PID = r.proc.Pid
		}
		if r.state == StateWaiting || r.state == StateElsewhere {
			st.NextStart = r.next.UTC()
		}
		if r.state == StateElsewhere {
			st.Elsewhere, st.Why = r.elsewhere, r.why
		}
		out = append(out, st)
	}
	return out
}

// keep runs one role until it is no longer wanted: start, wait for it to
// exit, wait a little, start again.
func (s *Supervisor) keep(r *role, gen int) {
	defer s.wg.Done()
	// current: this loop still owns the role. Checked under s.mu.
	current := func() bool { return r.gen == gen && r.wanted && s.ctx.Err() == nil }
	for {
		s.mu.Lock()
		if !current() {
			// Stopped, or replaced by a newer start: only the newer loop may
			// set the state then.
			if r.gen == gen || !r.wanted {
				r.state, r.since, r.cmd = StateStopped, time.Now(), nil
			}
			s.mu.Unlock()
			return
		}
		cmd := s.launch(r.name)
		// The role's stderr goes through a line reader that takes out its
		// marker lines — the way a gateway-wide role says whether it stays.
		errOut := newRoleStderr(cmd.Stderr)
		cmd.Stderr = errOut
		if cmd.WaitDelay == 0 {
			// A grandchild still holding the stderr pipe must not hold up
			// Wait once the role itself has gone.
			cmd.WaitDelay = 5 * time.Second
		}
		done := make(chan struct{})
		r.cmd, r.done = cmd, done
		// A gateway-wide role first looks whether another deployment runs
		// it. Until it says, it is shown as starting — or, when the last look
		// found it elsewhere, still as elsewhere, so the minute's look does
		// not flash it up as running.
		checking := s.leased != nil && s.leased(r.name)
		recheck := checking && r.state == StateElsewhere
		if !recheck {
			r.state, r.since = StateStarting, time.Now()
		}
		s.mu.Unlock()

		started := time.Now()
		err := cmd.Start()
		if err == nil {
			s.mu.Lock()
			r.proc = cmd.Process
			stillWanted := current()
			if stillWanted && r.state == StateStarting && !checking {
				r.state = StateRunning
			}
			s.mu.Unlock()
			if !recheck {
				s.logf("%s: started (pid %d)", r.name, cmd.Process.Pid)
			}
			if !stillWanted {
				// Stopped while it was being launched: the stop found no
				// process to signal yet, so it is signalled here.
				go s.terminate(cmd.Process, done)
			} else if checking {
				go s.settleCheck(r, gen, cmd.Process, errOut.here, done, recheck)
			}
			err = cmd.Wait()
		}
		errOut.flush()
		close(done)

		s.mu.Lock()
		r.proc = nil
		r.lastExit, r.lastAt = exitText(err), time.Now()
		if !current() {
			if r.gen == gen || !r.wanted {
				r.state, r.since, r.cmd = StateStopped, time.Now(), nil
			}
			s.mu.Unlock()
			s.logf("%s: stopped", r.name)
			return
		}
		if checking && cmd.ProcessState != nil && cmd.ProcessState.ExitCode() == singleton.ExitElsewhere {
			// Another deployment runs it (Saleem 2026-10-01): not a crash —
			// no restart counted, no growing wait — just another look in a
			// minute, until that one stops.
			where, why := errOut.holder(), errOut.why()
			news := r.state != StateElsewhere || r.elsewhere != where || r.why != why
			if r.state != StateElsewhere {
				r.since = time.Now()
			}
			r.state, r.elsewhere, r.why, r.cmd = StateElsewhere, where, why, nil
			r.lastExit = ElsewhereWords(where, why)
			r.backoff = 0
			wait := s.recheck
			r.next = time.Now().Add(wait)
			s.mu.Unlock()
			if news {
				s.logf("%s: %s — no copy is kept here; looking again every %s", r.name, ElsewhereWords(where, why), every(wait))
			}
			s.pause(r, wait)
			continue
		}
		// Crashed or quit by itself: always started again (Saleem 2026-09-29),
		// after a wait that grows while it keeps failing.
		if time.Since(started) >= Stable {
			r.backoff = 0
		}
		if r.backoff == 0 {
			r.backoff = s.minWait
		} else if r.backoff *= 2; r.backoff > s.maxWait {
			r.backoff = s.maxWait
		}
		r.restarts++
		wait := r.backoff
		r.state, r.since, r.next, r.cmd, r.elsewhere, r.why = StateWaiting, time.Now(), time.Now().Add(wait), nil, "", ""
		s.mu.Unlock()
		s.logf("%s: exited (%s); starting again in %s", r.name, r.lastExit, wait)
		s.pause(r, wait)
	}
}

// pause waits before a role is started again: the wait, or less when a start
// request (or a stop) wakes it, or the gateway shuts down.
func (s *Supervisor) pause(r *role, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-r.wake:
	case <-s.ctx.Done():
	}
}

// settleCheck shows a gateway-wide role as running once it has said it stays
// (its HERE line), or once it has run for settle without saying either way —
// a role that keeps running is running, whatever it printed.
func (s *Supervisor) settleCheck(r *role, gen int, proc *os.Process, here, done <-chan struct{}, recheck bool) {
	t := time.NewTimer(s.settle)
	defer t.Stop()
	select {
	case <-here:
	case <-t.C:
	case <-done:
		return
	}
	s.mu.Lock()
	ok := r.gen == gen && r.wanted && r.proc == proc &&
		(r.state == StateStarting || r.state == StateElsewhere)
	select {
	case <-done: // it has already exited: keep says what happened
		ok = false
	default:
	}
	if ok {
		r.state, r.since, r.elsewhere, r.why = StateRunning, time.Now(), "", ""
	}
	s.mu.Unlock()
	if ok && recheck {
		s.logf("%s: runs here now (pid %d) — the deployment that ran it has let it go", r.name, proc.Pid)
	}
}

// every is an interval for a log line: "minute" rather than "1m0s".
func every(d time.Duration) string {
	if d == time.Minute {
		return "minute"
	}
	return d.String()
}

// terminate asks a process to stop (askToStop: SIGTERM, which every role
// handles by finishing what it is doing; on Windows an immediate end), and
// kills it if it has not gone in time.
func (s *Supervisor) terminate(proc *os.Process, done chan struct{}) {
	if proc == nil {
		return
	}
	askToStop(proc)
	select {
	case <-done:
	case <-time.After(s.stopWait):
		_ = proc.Kill()
		<-done
	}
}

func (s *Supervisor) waitStopped(name string) {
	for i := 0; i < 400; i++ {
		s.mu.Lock()
		st := s.roles[name].state
		s.mu.Unlock()
		if st == StateStopped {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *Supervisor) logf(format string, a ...any) {
	if s.out == nil {
		return
	}
	fmt.Fprintf(s.out, "[gateway] %s "+format+"\n", append([]any{time.Now().UTC().Format("2006/01/02 15:04:05")}, a...)...)
}

// exitText says how a process ended, in words.
func exitText(err error) string {
	if err == nil {
		return "exit 0"
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			return "killed by " + ws.Signal().String()
		}
		return fmt.Sprintf("exit %d", ee.ExitCode())
	}
	return err.Error()
}

// Retired are the roles the engine no longer has, and why. A YAKU_ROLES list
// that still names one is not refused for it: the name is passed over and the
// gateway says so at start (RetiredIn).
var Retired = map[string]string{
	"billing": "money is booked when it is decided and the db-worker of each deployment books the billing queue (26 Oct 2026); take it out of YAKU_ROLES",
	"retry":   "a vendor role refuses a message whose tries ran out when it takes it from its queue (issue 22, 27 Oct 2026); take it out of YAKU_ROLES",
	"expiry":  "a vendor role refuses a message past its validity when it takes it from its queue (issue 22, 27 Oct 2026); take it out of YAKU_ROLES",
}

// RetiredIn lists the retired roles a YAKU_ROLES setting names.
func RetiredIn(setting string) []string {
	var out []string
	for _, f := range strings.FieldsFunc(setting, func(c rune) bool { return c == ',' || c == ' ' }) {
		if _, ok := Retired[strings.TrimSpace(strings.ToLower(f))]; ok {
			out = append(out, strings.TrimSpace(strings.ToLower(f)))
		}
	}
	return out
}

// Pick chooses the roles to run: the YAKU_ROLES list when given (comma or
// space separated), else every role. Unknown names are refused; a retired
// role's name is passed over.
func Pick(all []string, setting string) ([]string, error) {
	setting = strings.TrimSpace(setting)
	if setting == "" {
		out := append([]string(nil), all...)
		sort.Strings(out)
		return out, nil
	}
	known := map[string]bool{}
	for _, r := range all {
		known[r] = true
	}
	var out []string
	seen := map[string]bool{}
	for _, f := range strings.FieldsFunc(setting, func(c rune) bool { return c == ',' || c == ' ' }) {
		f = strings.TrimSpace(strings.ToLower(f))
		if f == "" || seen[f] {
			continue
		}
		if _, retired := Retired[f]; retired && !known[f] {
			continue
		}
		if !known[f] {
			return nil, fmt.Errorf("YAKU_ROLES names %q, which is not a role (roles: %s)", f, strings.Join(all, ", "))
		}
		seen[f] = true
		out = append(out, f)
	}
	if len(out) == 0 {
		return nil, errors.New("YAKU_ROLES names no role")
	}
	return out, nil
}

// ChildEnv is the environment a role is started with: the supervisor's own
// environment as it was BEFORE the settings file was read. A role reads the
// file itself, so a change saved to yakugate.config reaches a role when it is
// restarted — which it could not if the old values were passed down as
// environment variables, since those win over the file.
func ChildEnv(original []string) []string {
	return append([]string(nil), original...)
}

// Executable is this binary's path, for launching roles.
func Executable() (string, error) {
	p, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("cannot find this binary to start the roles: %w", err)
	}
	return p, nil
}
