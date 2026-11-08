package mssql

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The named leases and the register beside them (internal/dal/locks.go) on
// SQL Server, made like the MySQL DAL's (internal/dal/mysql/locks.go).
//
// A lease is an application lock (sp_getapplock) owned by the SESSION and
// held on a PINNED connection for as long as it is held, as GET_LOCK is on
// MySQL: a process that dies, or whose connection drops, lets go of it
// without anyone having to notice. An application lock belongs to the
// database it is taken in, so two installs with their own databases on one
// server never hold each other's leases — the name needs no database in it
// (MySQL's GET_LOCK names are server-wide, so there it does).
//
// The session a lease names (HeldLock.Session, LockHolder, the conn_id of
// singleton_holders) is the server's session id with its login time:
// sessionID = spid<<32 | login seconds. A session id alone (@@SPID) is used
// again as soon as its session ends, so a holder's row left by a process
// that died could name the next process to get that id.

// openLeases opens the leases on the config store's one pool a process.
func openLeases(cfg config.Config) (*dal.Leases, error) {
	db, err := Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	return NewLeases(db), nil
}

// NewLeases is the leases and the register on db (the config store).
func NewLeases(db *sql.DB) *dal.Leases {
	return &dal.Leases{Locks: &Locks{db: db}, Homes: &RoleHomes{db: db}}
}

const loginSeconds = "DATEDIFF_BIG(SECOND, '19700101', login_time)"

var (
	// The lock and this session, in one round trip. sp_getapplock answers 0
	// (granted) or 1 (granted after a wait) when taken, -1 when another
	// holds it (no wait: @LockTimeout = 0), -2 / -3 / -999 when it could
	// not say (cancelled, a deadlock, an error).
	stLockTake = EngineStmts.Add("locks.take", `SET NOCOUNT ON;
DECLARE @yk_got INT;
EXEC @yk_got = sp_getapplock @Resource = @p1, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0, @DbPrincipal = 'public';
SELECT @yk_got, session_id, `+loginSeconds+` FROM sys.dm_exec_sessions WHERE session_id = @@SPID`, false)
	// Who holds it: the lock's own row in sys.dm_tran_locks, found by the
	// start of its description ("0:[<name>]:(<hash>)", the name cut to 32
	// characters — leaseName keeps every name within that). A login without
	// VIEW SERVER PERFORMANCE STATE sees only its own sessions' locks there;
	// then APPLOCK_TEST still says whether it is held, by a session it cannot
	// name (0).
	stLockHolder = EngineStmts.Add("locks.holder", `SET NOCOUNT ON;
DECLARE @yk_spid BIGINT, @yk_login BIGINT;
SELECT TOP (1) @yk_spid = l.request_session_id, @yk_login = DATEDIFF_BIG(SECOND, '19700101', s.login_time)
  FROM sys.dm_tran_locks AS l JOIN sys.dm_exec_sessions AS s ON s.session_id = l.request_session_id
 WHERE l.resource_type = 'APPLICATION' AND l.resource_database_id = DB_ID()
   AND l.request_owner_type = 'SESSION' AND l.request_status = 'GRANT'
   AND l.resource_description LIKE @p1 ESCAPE '\';
IF @yk_spid IS NULL AND APPLOCK_TEST('public', @p2, 'Exclusive', 'Session') = 0 SET @yk_spid = 0;
SELECT @yk_spid, @yk_login`, false)
	stLockRelease = EngineStmts.Add("locks.release",
		"EXEC sp_releaseapplock @Resource = @p1, @LockOwner = 'Session', @DbPrincipal = 'public'", false)
)

// leaseName is a lease's name on the server: the name itself when it fits
// the 32 characters sys.dm_tran_locks shows of it, else its start and a
// hash of the whole (32 characters), so two long names never share a lock.
func leaseName(name string) string {
	if len(name) <= 32 {
		return name
	}
	sum := sha1.Sum([]byte(name))
	return name[:19] + "~" + hex.EncodeToString(sum[:])[:12]
}

// likeName is the LIKE pattern that finds a lease's row in
// sys.dm_tran_locks: "0:[<name>]:%" (0 = the public principal), with LIKE's
// own characters escaped.
func likeName(server string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`, `[`, `\[`, `]`, `\]`)
	return `0:\[` + r.Replace(server) + `\]:%`
}

// sessionID joins a session id and its login time.
func sessionID(spid, login int64) int64 { return spid<<32 | (login & 0xffffffff) }

// SPID is the server's session id inside a lease's session (for KILL).
func SPID(session int64) int64 { return session >> 32 }

// Locks is dal.Locks on SQL Server's application locks.
type Locks struct {
	db *sql.DB
}

// TryLock asks for the lease on a connection of its own, without waiting: a
// standby must not block its own loop waiting for a lease that may be held
// for days.
func (l *Locks) TryLock(ctx context.Context, name string) (dal.HeldLock, dal.LockAnswer, error) {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return nil, dal.LockNoAnswer, err // cannot ask
	}
	server := leaseName(name)
	var got int
	var spid, login int64
	if err := conn.QueryRowContext(ctx, stLockTake.SQL, server).Scan(&got, &spid, &login); err != nil {
		dropConn(conn) // the lock may have been granted before the error
		return nil, dal.LockNoAnswer, err
	}
	switch {
	case got >= 0:
		return &heldLock{conn: conn, name: server, session: sessionID(spid, login)}, dal.LockTaken, nil
	case got == -1:
		_ = conn.Close()
		return nil, dal.LockHeld, nil
	default:
		_ = conn.Close()
		return nil, dal.LockNoAnswer, nil
	}
}

// LockHolder asks the server who holds the lease now. A session it may not
// see is session 0, held.
func (l *Locks) LockHolder(ctx context.Context, name string) (int64, bool, error) {
	server := leaseName(name)
	var spid, login sql.NullInt64
	if err := l.db.QueryRowContext(ctx, stLockHolder.SQL, likeName(server), server).Scan(&spid, &login); err != nil {
		return 0, false, err
	}
	if !spid.Valid {
		return 0, false, nil
	}
	if spid.Int64 == 0 {
		return 0, true, nil
	}
	return sessionID(spid.Int64, login.Int64), true, nil
}

// heldLock is a lease held on its pinned connection.
type heldLock struct {
	conn    *sql.Conn
	name    string
	session int64
}

func (h *heldLock) Session() int64 { return h.session }

// Alive pings the pinned connection: a dead connection means the lease went
// with it.
func (h *heldLock) Alive(ctx context.Context) error { return h.conn.PingContext(ctx) }

// Release lets go of the lease and its connection. If the release cannot be
// said (the session is gone, or the server refused), the connection is
// thrown away rather than put back in the pool still holding the lock.
func (h *heldLock) Release() {
	// Background context: the role's own may already be cancelled, and a
	// lease left held would idle the standby until this process exits.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := h.conn.ExecContext(ctx, stLockRelease.SQL, h.name); err != nil {
		dropConn(h.conn)
		return
	}
	_ = h.conn.Close()
}

// dropConn closes a pinned connection for good: the pool does not get it
// back (it may hold a lock this process no longer knows of).
func dropConn(c *sql.Conn) {
	_ = c.Raw(func(any) error { return driver.ErrBadConn })
	_ = c.Close()
}

// RoleHomes is dal.RoleHomes on the config store.
type RoleHomes struct {
	db *sql.DB
}

var (
	stRoleHome = EngineStmts.Add("role_homes.home", `SELECT COALESCE(MIN(deployment_id), ''), COUNT(DISTINCT deployment_id)
		FROM deployment_roles WHERE role = @p1 AND is_singleton = 1`, false)
	stRoleClaimant = EngineStmts.Add("role_homes.claimant", `SELECT TOP (1) deployment_id FROM singleton_claims
		WHERE role = @p1 AND claimed_at > DATEADD(SECOND, -@p2, SYSUTCDATETIME())`, false)
	stRoleAskBack = AddUpsert(EngineStmts, "role_homes.ask_back", UpsertSpec{
		Table: "singleton_claims", Keys: []string{"role"},
		Set:          []string{"deployment_id = $deployment_id", "host = $host", "pid = $pid", "claimed_at = SYSUTCDATETIME()"},
		Insert:       []string{"role", "deployment_id", "host", "pid", "claimed_at"},
		InsertValues: []string{"", "", "", "", "SYSUTCDATETIME()"},
	}, false)
	stRoleDropAsk = EngineStmts.Add("role_homes.drop_ask", "DELETE FROM singleton_claims WHERE role = @p1", false)
	stRoleRecord  = AddUpsert(EngineStmts, "role_homes.record", UpsertSpec{
		Table: "singleton_holders", Keys: []string{"role"},
		Set: []string{"deployment_id = $deployment_id", "host = $host", "pid = $pid", "conn_id = $conn_id", "since = $since"},
	}, false)
	stRoleForget = EngineStmts.Add("role_homes.forget", "DELETE FROM singleton_holders WHERE role = @p1 AND conn_id = @p2", false)
	stRoleHolder = EngineStmts.Add("role_homes.holder",
		"SELECT deployment_id, host, pid, conn_id FROM singleton_holders WHERE role = @p1", false)
)

// RoleHome reads the deployments register. MIN with COUNT, so "exactly one"
// is one row read. A store with no register at all is "listed nowhere".
func (r *RoleHomes) RoleHome(ctx context.Context, role string) (home string, known bool) {
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var n int
	err := r.db.QueryRowContext(c, stRoleHome.SQL, Varchar(role)).Scan(&home, &n)
	switch {
	case err != nil && MissingTable(err):
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
	if err := r.db.QueryRowContext(c, stRoleClaimant.SQL, Varchar(role), int(fresh/time.Second)).Scan(&dep); err != nil {
		return ""
	}
	return dep
}

// AskBack writes the home's ask for its role.
func (r *RoleHomes) AskBack(ctx context.Context, role, deployment, host string, pid int) {
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, _ = stRoleAskBack.Exec(c, r.db, sql.Named("role", Varchar(role)), sql.Named("deployment_id", Varchar(deployment)),
		sql.Named("host", Varchar(host)), sql.Named("pid", pid))
}

// DropAsk removes the ask.
func (r *RoleHomes) DropAsk(ctx context.Context, role string) {
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, _ = r.db.ExecContext(c, stRoleDropAsk.SQL, Varchar(role))
}

// RecordHolder writes who holds a role's lease into singleton_holders.
func (r *RoleHomes) RecordHolder(ctx context.Context, role string, h dal.RoleHolder, since time.Time) error {
	_, err := stRoleRecord.Exec(ctx, r.db, sql.Named("role", Varchar(role)), sql.Named("deployment_id", Varchar(h.Deployment)),
		sql.Named("host", Varchar(h.Host)), sql.Named("pid", h.PID), sql.Named("conn_id", h.Session), sql.Named("since", Time(since)))
	return err
}

// ForgetHolder removes a role's row, only the one naming session.
func (r *RoleHomes) ForgetHolder(ctx context.Context, role string, session int64) error {
	_, err := r.db.ExecContext(ctx, stRoleForget.SQL, Varchar(role), session)
	return err
}

// RoleHolder reads a role's singleton_holders row.
func (r *RoleHomes) RoleHolder(ctx context.Context, role string) (dal.RoleHolder, error) {
	var h dal.RoleHolder
	err := r.db.QueryRowContext(ctx, stRoleHolder.SQL, Varchar(role)).Scan(&h.Deployment, &h.Host, &h.PID, &h.Session)
	return h, err
}

var (
	_ dal.Locks     = (*Locks)(nil)
	_ dal.RoleHomes = (*RoleHomes)(nil)
)
