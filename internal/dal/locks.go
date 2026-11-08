package dal

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// Named leases (plan of 29 Oct 2026, "Locks"): one holder at a time for a
// name, across every process of the install. internal/singleton keeps a
// gateway-wide role in one place with them; step 3 reuses them for "one
// process at a time" wherever Redis is not there to say so.
//
// A lease is held by a SESSION of the database: a process that dies, or
// whose connection drops, lets go of it without anyone having to notice.
// There is no heartbeat to tune and no stale lease to clear by hand. A
// holder that stops answering (Alive fails) has lost it.

// LockAnswer is what the database said to a request for a lease.
type LockAnswer int

const (
	// LockTaken: the lease is ours until Release, or until our session ends.
	LockTaken LockAnswer = iota
	// LockHeld: another session holds it.
	LockHeld
	// LockNoAnswer: the database said neither yes nor no (MySQL's GET_LOCK
	// answers NULL on an error inside it). Not a refusal.
	LockNoAnswer
)

// Locks takes, holds and lets go of named leases. The name a caller gives is
// scoped by the DAL to the install's own database, so two installs that
// share a database server never hold each other's leases.
type Locks interface {
	// TryLock asks once, without waiting, for the lease called name. Only
	// LockTaken returns a HeldLock. err: the lease could not be asked.
	TryLock(ctx context.Context, name string) (HeldLock, LockAnswer, error)
	// LockHolder is the session that holds the lease now; held=false when
	// nobody does.
	LockHolder(ctx context.Context, name string) (session int64, held bool, err error)
}

// HeldLock is a lease this process holds.
type HeldLock interface {
	// Session is the database session holding it (MySQL's CONNECTION_ID()),
	// the one LockHolder names while we hold it.
	Session() int64
	// Alive fails when the session holding the lease is gone, and the lease
	// with it.
	Alive(ctx context.Context) error
	// Release lets go of the lease and its session. Safe to call once the
	// session is already gone.
	Release()
}

// RoleHolder is who holds a gateway-wide role, as its singleton_holders row
// says; Session is the database session the row was written for.
type RoleHolder struct {
	Deployment string
	Host       string
	PID        int
	Session    int64
}

// RoleHomes is where the gateway-wide roles are expected and who runs them:
// the deployments register (deployment_roles), the holders' names
// (singleton_holders, config/0135) and a home's ask for its role back
// (singleton_claims, config/0139). Every method is best effort: the lease is
// the lock, never these rows.
type RoleHomes interface {
	// RoleHome is the deployment the register lists a role on. known=false
	// when the register cannot be read, or names more than one deployment;
	// home="" with known=true: listed nowhere (a store with no register at
	// all is "listed nowhere").
	RoleHome(ctx context.Context, role string) (home string, known bool)
	// RoleHolder reads a role's singleton_holders row.
	RoleHolder(ctx context.Context, role string) (RoleHolder, error)
	// RecordHolder writes the row of the session holding a role, since a
	// moment.
	RecordHolder(ctx context.Context, role string, h RoleHolder, since time.Time) error
	// ForgetHolder removes a role's row, only when it names session.
	ForgetHolder(ctx context.Context, role string, session int64) error
	// AskBack writes the home's ask for its role.
	AskBack(ctx context.Context, role, deployment, host string, pid int)
	// DropAsk removes the ask once the home has its role again.
	DropAsk(ctx context.Context, role string)
	// Claimant is the deployment asking for a role back within the last
	// `fresh` ("" when nobody has).
	Claimant(ctx context.Context, role string, fresh time.Duration) string
}

// Leases is what OpenLeases built: the named leases and the register they
// are kept beside, on one pool.
type Leases struct {
	Locks Locks
	Homes RoleHomes
}

// OpenLeases opens the leases on the install's SQL database (the config
// store, the one database every deployment shares by definition).
func OpenLeases(cfg config.Config) (*Leases, error) {
	on := SQL(cfg)
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.Leases == nil {
		return nil, fmt.Errorf("dal: leases on %s: %w", on, ErrNotServed)
	}
	l, err := d.Leases(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: leases on %s: %w", on, err)
	}
	return l, nil
}
