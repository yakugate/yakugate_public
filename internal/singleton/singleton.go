// Package singleton keeps a gateway-wide role running in exactly one place.
//
// Some roles act on data that every deployment shares, so running two of them
// is not extra capacity — it is a second opinion nobody asked for. Billing
// would charge from two clocks and race the live balance counter,
// abuse-monitor would apply and audit the same suspension twice, retention
// would drop the same partition twice, and two backup runs would write into
// one directory and rotate each other away.
//
// None of that mattered while there was one install. It matters the moment
// there is a second, which is what config/0122 makes routine.
//
// The lease is a named lock of the DAL (dal.Locks; on MySQL an advisory lock
// held on a PINNED connection) for the life of the process. That choice does
// the work: MySQL scopes GET_LOCK to a
// session, so a process that dies — crash, kill, network drop — releases it
// without anything having to notice, and the standby takes over on its next
// pass. There is no heartbeat to tune and no stale lock to clear by hand.
//
// A role that cannot get the lease does not exit. It stands by and keeps
// asking, which is exactly what you want a second deployment's billing role
// to do: idle while the other site is healthy, working within seconds of it
// going away.
//
// Except under the whole-gateway supervisor (Saleem, 2026-10-01: "don't
// start, auto takeover"). A gateway that runs its roles as children keeps no
// idle copy of a role another deployment runs: the child checks at start
// (AtStart) and whenever it would otherwise stand by (Held), and when another
// session definitely holds the lease it prints one marker line naming the
// holder and exits with ExitElsewhere. The supervisor shows the role as
// running elsewhere and starts it again a minute later, so nothing sits idle.
//
// Since 2026-10-26 a role runs only on the deployment the deployments
// register lists it on (home.go, Saleem: "strict"): another deployment never
// takes it over, and a deployment that cannot read the register waits.
// The holder names itself in singleton_holders (config/0135) when it takes
// the lease, so the refused side can say which deployment runs it.
//
// It fails OPEN on its own deployment. For a role the register lists here (or
// nowhere), a lock call that errors leaves Held true and the role does the
// work: no other deployment may run it, so failing open cannot make two. The
// failure we must never have is "billing quietly stopped because a lock query
// failed". A role listed on another deployment never runs here, and one whose
// register cannot be read waits (2026-10-26). A role that is STOPPING is never
// sent into new work: its cancelled context is why the lease cannot be asked
// (2026-10-18).
package singleton

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// Lease is one named gateway-wide claim. The zero value is not usable; call New.
type Lease struct {
	// locks and homes are the named lease and the register beside it
	// (dal.Leases: the config store).
	locks dal.Locks
	homes dal.RoleHomes
	role  string
	name  string
	// leave: started by the whole-gateway supervisor, so a lease another
	// session holds sends the role away (Elsewhere) instead of standing by.
	leave bool

	mu      sync.Mutex
	lock    dal.HeldLock
	held    bool
	lastTry time.Time
	// connID is the holding session's CONNECTION_ID(), the one IS_USED_LOCK
	// names while we hold the lease; recorded says singleton_holders has our
	// row (it may not exist yet — the roles start before http-server applies
	// the migrations), lastRecord paces another try.
	connID     int64
	since      time.Time
	recorded   bool
	lastRecord time.Time
	// onChange is called when the lease is gained or lost, for a log line.
	onChange func(held bool)

	// The role's home (home.go): this deployment's id, the home as last read
	// from the register (and whether it could be read) and when, and when the
	// home last wrote its ask while standing by.
	me        string
	homeID    string
	homeKnown bool
	homeAt    time.Time
	lastAsked time.Time
	// homeWait: when the home, refused, began standing by for its role
	// (zero: not waiting). See homeStandBy.
	homeWait time.Time
}

// retryEvery paces how often a standby asks for the lease. Seconds matter
// here — this is how long a gateway-wide job pauses when the deployment
// holding it goes away — but asking on every 500ms Step would put a needless
// query on the store all day.
const retryEvery = 5 * time.Second

// recordEvery paces another try at writing our singleton_holders row after a
// failed one.
const recordEvery = 30 * time.Second

// New builds a lease named for the role on the leases dal.OpenLeases gave
// (nil: not wired, the role always runs). `notify` may be nil.
func New(leases *dal.Leases, role string, notify func(held bool)) *Lease {
	l := &Lease{role: role, name: lockName(role), leave: Supervised(), onChange: notify, me: deployment.ID()}
	if leases != nil {
		l.locks, l.homes = leases.Locks, leases.Homes
	}
	return l
}

// LeasesOf is the install's leases (dal.OpenLeases, on the config store's
// one pool a process); nil when they cannot be opened, and New on nil builds
// a lease that always runs the role, as a role with no config store pool
// always has.
func LeasesOf(cfg config.Config) *dal.Leases {
	leases, err := dal.OpenLeases(cfg)
	if err != nil {
		return nil
	}
	return leases
}

// lockName is the lease a role holds: "yaku_role_<role>". The DAL adds the
// config database it guards (2026-10-02; on MySQL a lock name belongs to the
// whole SERVER, so two gateways with their own databases on one server held
// each other's roles off) — internal/dal/mysql/locks.go roleLockName.
func lockName(role string) string { return "yaku_role_" + role }

// Held reports whether this process may do the role's work right now, taking
// the lease if it is free. Safe to call on every Step.
//
// held=false with a nil error: stand by. held=false with an *Elsewhere error:
// supervised, and another session holds the lease — the role ends its process
// with the error (worker.Run returns it; main prints the marker and exits
// ExitElsewhere). Never an error when the lease could not be ASKED: that is
// held=true, fail open.
func (l *Lease) Held(ctx context.Context) (bool, error) {
	if l == nil || l.locks == nil {
		return true, nil // not wired: single-deployment behaviour, unchanged
	}
	// A role that is stopping is never told to work (2026-10-18, go-live P8
	// E1). Its context is cancelled, so every call below fails — and failing
	// open then sent a backup that had just let go of its lease into a new,
	// empty run on its way out.
	if ctx.Err() != nil {
		return false, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.held {
		// Still ours as long as the pinned connection lives. A dead
		// connection means the lock went with it, so drop it and re-acquire
		// rather than carry on believing we are the only one.
		if err := l.lock.Alive(ctx); err == nil {
			if !l.recorded && time.Since(l.lastRecord) >= recordEvery {
				l.record(ctx)
			}
			// Moved to another deployment in the register: give it up at
			// once (home.go, 2026-10-26; read again every homeEvery). Between
			// Steps, so no work is cut in half.
			if home := l.awayFrom(ctx); home != "" {
				l.drop()
				l.lastTry = time.Now()
				return l.leaveFor(home)
			}
			return true, nil
		}
		l.drop()
	}

	if time.Since(l.lastTry) < retryEvery {
		return false, nil
	}
	l.lastTry = time.Now()

	// Where the register lists it, before the lock is asked (2026-10-26):
	// another deployment's role is never taken here, and a register that
	// cannot be read means wait.
	home, known := l.home(ctx)
	if !known {
		return l.waitForList()
	}
	if home != "" && home != l.me {
		return l.leaveFor(home)
	}
	// Zero wait: a standby must not block its own Step loop waiting for a
	// lease that may be held for days.
	lock, answer, err := l.locks.TryLock(ctx, l.name)
	if err != nil {
		return failOpen(ctx) // cannot ask
	}
	if answer != dal.LockTaken {
		// Somebody else is running this role. The home asks for it back
		// unless its own deployment is the one running it.
		if answer == dal.LockHeld && home == l.me {
			if h := describe(ctx, l.locks, l.homes, l.role); h.Deployment != l.me {
				if l.leave || time.Since(l.lastAsked) >= claimLookEvery {
					askBack(ctx, l.homes, l.role, l.me)
					l.lastAsked = time.Now()
				}
				// The home stands by while the holder hands the role back
				// (2026-10-18, go-live P8 E3, P9 E3, P10 E1/E4): it used to
				// LEAVE at once, the holder handed back within 15 s, and the
				// role then ran nowhere until the supervisor's next look a
				// minute later — billing ran nowhere for 56 s. Standing by,
				// it asks again every retryEvery and takes the lease within
				// seconds of the hand-back. For homeStandBy at most: a holder
				// that never hands back (an older build) is left alone as
				// before.
				if l.leave {
					if l.homeWait.IsZero() {
						l.homeWait = time.Now()
					}
					if time.Since(l.homeWait) < homeStandBy {
						return false, nil
					}
				}
			}
		}
		if !l.leave || answer != dal.LockHeld {
			// No answer (MySQL's NULL, an error inside GET_LOCK) is not a
			// refusal: only a definite 0 sends a supervised role away.
			return false, nil
		}
		h := describe(ctx, l.locks, l.homes, l.role)
		return false, &Elsewhere{Role: l.role, Holder: h}
	}
	l.lock, l.held = lock, true
	l.connID, l.since = lock.Session(), time.Now().UTC()
	l.recorded = false
	l.homeWait = time.Time{}
	l.record(ctx)
	if home == l.me {
		dropAsk(ctx, l.homes, l.role) // home again: nothing left to ask for
	}
	if l.onChange != nil {
		l.onChange(true)
	}
	return true, nil
}

// failOpen is the answer when the lease could not be asked: run the role —
// unless the role is stopping, whose cancelled context is why it could not
// ask.
func failOpen(ctx context.Context) (bool, error) {
	return ctx.Err() == nil, nil
}

// homeStandBy is how long the home stands by for its role, asking for it
// back, before it leaves like any refused copy.
const homeStandBy = time.Minute

// leaveFor is what a role does when the register lists it on another
// deployment: supervised, it leaves naming that deployment; otherwise it
// stands by.
func (l *Lease) leaveFor(home string) (bool, error) {
	if l.leave {
		return false, &Elsewhere{Role: l.role, Holder: Holder{Deployment: home}, Home: true}
	}
	return false, nil
}

// waitForList is what a role does when the register cannot be read:
// supervised, it leaves to wait (the supervisor looks again in a minute);
// otherwise it stands by and reads the register again every homeEvery.
func (l *Lease) waitForList() (bool, error) {
	if l.leave {
		return false, &Elsewhere{Role: l.role, NoList: true}
	}
	return false, nil
}

// record writes who holds the lease into singleton_holders, so a deployment
// refused it can say where the role runs. Best effort: the lease is the
// GET_LOCK, never this row. Caller holds the mutex.
func (l *Lease) record(ctx context.Context) {
	l.lastRecord = time.Now()
	host, _ := os.Hostname()
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := l.homes.RecordHolder(c, l.role,
		dal.RoleHolder{Deployment: deployment.ID(), Host: host, PID: os.Getpid(), Session: l.connID}, l.since)
	l.recorded = err == nil
}

// Release gives the lease up. Called when the role shuts down; the lock would
// go with the connection anyway, so this only shortens the standby's wait.
func (l *Lease) Release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held && l.recorded {
		// Our row only: a newer holder may have written its own already.
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = l.homes.ForgetHolder(c, l.role, l.connID)
		cancel()
	}
	l.drop()
}

// drop releases and forgets the held lock. Caller holds the mutex.
func (l *Lease) drop() {
	if l.lock != nil {
		// Release lets go on a background context: the role's own may
		// already be cancelled, and a lock left held would idle the standby
		// until this process exits.
		l.lock.Release()
		l.lock = nil
	}
	if l.held && l.onChange != nil {
		l.onChange(false)
	}
	l.held, l.recorded = false, false
}
