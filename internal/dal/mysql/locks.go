package mysql

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// The named leases and the register beside them (internal/dal/locks.go),
// moved here unchanged on 29 Oct 2026 (the DAL plan) from internal/singleton
// (singleton.go, home.go, elsewhere.go).
//
// A lease is a MySQL advisory lock held on a PINNED connection for as long as
// it is held. MySQL scopes GET_LOCK to a session, so a process that dies —
// crash, kill, network drop — releases it without anything having to
// notice.

// The named leases, on the config store's one pool.
var _ = serves(func(d *dal.Driver) { d.Leases = openLeases })

// openLeases opens the leases on the config store's one pool a process.
func openLeases(cfg config.Config) (*dal.Leases, error) {
	db, err := dbpool.Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return NewLeases(db), nil
}

// NewLeases is the leases and the register on db (the config store).
func NewLeases(db *sql.DB) *dal.Leases {
	l := &Locks{db: db}
	return &dal.Leases{Locks: l, Homes: &RoleHomes{db: db}}
}

var (
	stLockDatabase = EngineStmts.Add("locks.database", "SELECT DATABASE()", false)
	stLockTake     = EngineStmts.Add("locks.take", "SELECT GET_LOCK(?, 0), CONNECTION_ID()", false)
	stLockHolder   = EngineStmts.Add("locks.holder", "SELECT IS_USED_LOCK(?)", false)
	stLockRelease  = EngineStmts.Add("locks.release", "DO RELEASE_LOCK(?)", false)

	stRoleHome = EngineStmts.Add("role_homes.home", `SELECT COALESCE(MIN(deployment_id), ''), COUNT(DISTINCT deployment_id)
		FROM deployment_roles WHERE role = ? AND is_singleton = 1`, false)
	stRoleClaimant = EngineStmts.Add("role_homes.claimant", `SELECT deployment_id FROM singleton_claims
		WHERE role = ? AND claimed_at > UTC_TIMESTAMP(3) - INTERVAL ? SECOND`, false)
	stRoleAskBack = EngineStmts.Add("role_homes.ask_back", `INSERT INTO singleton_claims (role, deployment_id, host, pid, claimed_at)
		VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3))
		ON DUPLICATE KEY UPDATE deployment_id = VALUES(deployment_id), host = VALUES(host),
		pid = VALUES(pid), claimed_at = VALUES(claimed_at)`, false)
	stRoleDropAsk = EngineStmts.Add("role_homes.drop_ask", "DELETE FROM singleton_claims WHERE role = ?", false)
	stRoleRecord  = EngineStmts.Add("role_homes.record", `INSERT INTO singleton_holders
	        (role, deployment_id, host, pid, conn_id, since) VALUES (?, ?, ?, ?, ?, ?)
	        ON DUPLICATE KEY UPDATE deployment_id=VALUES(deployment_id), host=VALUES(host),
	        pid=VALUES(pid), conn_id=VALUES(conn_id), since=VALUES(since)`, false)
	stRoleForget = EngineStmts.Add("role_homes.forget", "DELETE FROM singleton_holders WHERE role = ? AND conn_id = ?", false)
	stRoleHolder = EngineStmts.Add("role_homes.holder", `SELECT deployment_id, host, pid, conn_id FROM singleton_holders WHERE role = ?`, false)
)

// Locks is dal.Locks on MySQL's advisory locks.
type Locks struct {
	db *sql.DB

	// names keeps each lease's name on the server once a TryLock has read
	// it: it includes the database, which takes a query (as singleton's
	// Lease kept its own).
	mu    sync.Mutex
	names map[string]string
}

// roleLockName is a lease's name on the server: the name AND the database it
// guards (2026-10-02). MySQL's locks are per SERVER, so a name of the role
// alone made two separate gateways with their own databases on one server
// block each other — the second one's billing never ran. Deployments of one
// gateway share one config database, so they still share the lock. MySQL caps
// a lock name at 64 characters; a database name that would pass it is
// replaced by the start of its SHA-1.
//
// Upgrade note: an older binary takes the name without the database, so
// while old and new processes overlap both may run the role. That is the
// failure the lease already accepts (fails open): every charge is keyed by
// its message, so nothing is taken twice.
//
// (Was singleton.lockName, with the "yaku_role_" prefix there; the caller
// now names the lease in full.)
func roleLockName(name, database string) string {
	if database == "" {
		return name
	}
	if full := name + "@" + database; len(full) <= 64 {
		return full
	}
	sum := sha1.Sum([]byte(database))
	return name + "@" + hex.EncodeToString(sum[:])[:12]
}

// rowQuerier is a *sql.DB or a *sql.Conn.
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// lockNameFor asks the server which database this connection uses.
func lockNameFor(ctx context.Context, q rowQuerier, name string) (string, error) {
	var db sql.NullString
	if err := q.QueryRowContext(ctx, stLockDatabase.SQL).Scan(&db); err != nil {
		return "", err
	}
	return roleLockName(name, db.String), nil
}

// TryLock asks for the lease on a connection of its own, without waiting: a
// standby must not block its own loop waiting for a lease that may be held
// for days.
func (l *Locks) TryLock(ctx context.Context, name string) (dal.HeldLock, dal.LockAnswer, error) {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return nil, dal.LockNoAnswer, err // cannot ask
	}
	l.mu.Lock()
	server, known := l.names[name]
	l.mu.Unlock()
	if !known {
		server, err = lockNameFor(ctx, conn, name)
		if err != nil {
			_ = conn.Close()
			return nil, dal.LockNoAnswer, err // cannot ask
		}
		l.mu.Lock()
		if l.names == nil {
			l.names = map[string]string{}
		}
		l.names[name] = server
		l.mu.Unlock()
	}
	var got, id sql.NullInt64
	if err := conn.QueryRowContext(ctx, stLockTake.SQL, server).Scan(&got, &id); err != nil {
		_ = conn.Close()
		return nil, dal.LockNoAnswer, err // cannot ask
	}
	if !got.Valid {
		// NULL is an error inside GET_LOCK, not a refusal.
		_ = conn.Close()
		return nil, dal.LockNoAnswer, nil
	}
	if got.Int64 != 1 {
		_ = conn.Close()
		return nil, dal.LockHeld, nil
	}
	return &heldLock{conn: conn, name: server, session: id.Int64}, dal.LockTaken, nil
}

// LockHolder asks the server who holds the lease now (IS_USED_LOCK).
func (l *Locks) LockHolder(ctx context.Context, name string) (int64, bool, error) {
	var owner sql.NullInt64
	server, err := lockNameFor(ctx, l.db, name)
	if err != nil {
		return 0, false, err
	}
	if err := l.db.QueryRowContext(ctx, stLockHolder.SQL, server).Scan(&owner); err != nil {
		return 0, false, err
	}
	return owner.Int64, owner.Valid, nil
}

// heldLock is a lease held on its pinned connection.
type heldLock struct {
	conn    *sql.Conn
	name    string
	session int64
}

func (h *heldLock) Session() int64 { return h.session }

// Alive pings the pinned connection: a dead connection means the lock went
// with it.
func (h *heldLock) Alive(ctx context.Context) error { return h.conn.PingContext(ctx) }

// Release lets go of the lock and its connection.
func (h *heldLock) Release() {
	// Background context: the role's own may already be cancelled, and a
	// lock left held would idle the standby until this process exits.
	_, _ = h.conn.ExecContext(context.Background(), stLockRelease.SQL, h.name)
	_ = h.conn.Close()
}

// RoleHomes is dal.RoleHomes on the config store.
type RoleHomes struct {
	db *sql.DB
}

// RoleHome reads the deployments register. MIN with COUNT, so "exactly one"
// is one row read.
func (r *RoleHomes) RoleHome(ctx context.Context, role string) (home string, known bool) {
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var n int
	err := r.db.QueryRowContext(c, stRoleHome.SQL, role).Scan(&home, &n)
	switch {
	case err != nil && strings.Contains(err.Error(), "1146"): // ER_NO_SUCH_TABLE
		return "", true
	case err != nil || n > 1:
		return "", false
	case n == 0:
		return "", true
	}
	return home, true
}

// Claimant reads an ask newer than fresh.
func (r *RoleHomes) Claimant(ctx context.Context, role string, fresh time.Duration) string {
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var dep string
	if err := r.db.QueryRowContext(c, stRoleClaimant.SQL, role, int(fresh/time.Second)).Scan(&dep); err != nil {
		return ""
	}
	return dep
}

// AskBack writes the home's ask for its role.
func (r *RoleHomes) AskBack(ctx context.Context, role, deployment, host string, pid int) {
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, _ = r.db.ExecContext(c, stRoleAskBack.SQL, role, deployment, host, pid)
}

// DropAsk removes the ask.
func (r *RoleHomes) DropAsk(ctx context.Context, role string) {
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, _ = r.db.ExecContext(c, stRoleDropAsk.SQL, role)
}

// RecordHolder writes who holds a role's lease into singleton_holders.
func (r *RoleHomes) RecordHolder(ctx context.Context, role string, h dal.RoleHolder, since time.Time) error {
	_, err := r.db.ExecContext(ctx, stRoleRecord.SQL, role, h.Deployment, h.Host, h.PID, h.Session, since)
	return err
}

// ForgetHolder removes a role's row, only the one naming session.
func (r *RoleHomes) ForgetHolder(ctx context.Context, role string, session int64) error {
	_, err := r.db.ExecContext(ctx, stRoleForget.SQL, role, session)
	return err
}

// RoleHolder reads a role's singleton_holders row.
func (r *RoleHomes) RoleHolder(ctx context.Context, role string) (dal.RoleHolder, error) {
	var h dal.RoleHolder
	err := r.db.QueryRowContext(ctx, stRoleHolder.SQL, role).Scan(&h.Deployment, &h.Host, &h.PID, &h.Session)
	return h, err
}

var (
	_ dal.Locks     = (*Locks)(nil)
	_ dal.RoleHomes = (*RoleHomes)(nil)
)
