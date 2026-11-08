package mysql

// What instances share: a vendor's auth session, the vendor login lock,
// and leases (named locks taken without waiting).

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// --- vendor_sessions: shared vendor auth-session store (queue/0022) ----------
//
// One row per vendor: the httpadapter auth chain's extracted vars (session id,
// bearer token, ...), shared by every process/instance using that vendor
// (PO 2026-08-10). expires_at round-trips through the driver (UTC) and is
// compared in Go ONLY — never against SQL NOW(), per the 2026-08-10
// next_attempt_at clock-mixing fix. Satisfies httpadapter.SessionStore.

// LoadVendorSession returns the shared session vars and their expiry (zero
// values when no row exists).
func (s *Store) LoadVendorSession(ctx context.Context, vendorID int64) (map[string]string, time.Time, error) {
	var raw string
	var exp time.Time
	err := s.db.QueryRowContext(ctx,
		`SELECT vars, expires_at FROM vendor_sessions WHERE vendor_id=?`, vendorID).Scan(&raw, &exp)
	if err == sql.ErrNoRows {
		return nil, time.Time{}, nil
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	vars := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &vars); err != nil {
		return nil, time.Time{}, err
	}
	return vars, exp, nil
}

// SaveVendorSession upserts the vendor's shared session (last writer wins —
// writes are already serialized by LockVendorLogin on the login path).
func (s *Store) SaveVendorSession(ctx context.Context, vendorID int64, cid string, vars map[string]string, expiresAt time.Time) error {
	b, err := json.Marshal(vars)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO vendor_sessions (vendor_id, cid, vars, expires_at) VALUES (?,?,?,?)
		 ON DUPLICATE KEY UPDATE cid=VALUES(cid), vars=VALUES(vars), expires_at=VALUES(expires_at)`,
		vendorID, cid, string(b), expiresAt.UTC())
	return err
}

// DeleteVendorSession drops the shared session row (revoke / forced re-login).
func (s *Store) DeleteVendorSession(ctx context.Context, vendorID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM vendor_sessions WHERE vendor_id=?`, vendorID)
	return err
}

// LockVendorLogin serializes vendor logins across all instances via a MySQL
// named lock (GET_LOCK pins a dedicated connection until release). Waits up to
// 5s; a timeout returns an error and the caller degrades to an unserialized
// login rather than not sending at all.
func (s *Store) LockVendorLogin(ctx context.Context, vendorID int64) (func(), error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("yaku_vendor_login_%d", vendorID)
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, 5)`, name).Scan(&got); err != nil {
		conn.Close()
		return nil, err
	}
	if !got.Valid || got.Int64 != 1 {
		conn.Close()
		return nil, fmt.Errorf("vendor login lock %s: timed out", name)
	}
	return func() {
		conn.ExecContext(context.Background(), `DO RELEASE_LOCK(?)`, name)
		conn.Close()
	}, nil
}

// ---- record_index partition lifecycle (GW-24, PO 2026-08-27) ----

// TryLease acquires a named cooperative lock WITHOUT waiting (GW-25, PO
// 2026-08-27): N billing instances split the hot partitions by whoever wins
// each table's lock; a held lock means "someone else is on it — skip". The
// lock lives on a pinned connection (MySQL scopes GET_LOCK per session);
// release returns it. ok=false is not an error.
func (s *Store) TryLease(ctx context.Context, name string) (release func(), ok bool, err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	// GET_LOCK names belong to the whole MySQL SERVER, so the name carries
	// this store's database. Without it two gateways with their own databases
	// on one server held each other's billing off, and the one that lost a
	// lease skipped its sweep in silence: on the dev box the running
	// gateway's billing role held "yaku.summary.reconcile" half the time, and
	// a second gateway's summaries were simply not rebuilt (found 2026-10-09
	// through sqa/e2e/go/p9h). The role leases got the same fix on 2026-10-02
	// (singleton.lockName).
	if name, err = s.leaseName(ctx, conn, name); err != nil {
		conn.Close()
		return nil, false, err
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&got); err != nil {
		conn.Close()
		return nil, false, err
	}
	if !got.Valid || got.Int64 != 1 {
		conn.Close()
		return nil, false, nil
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", name)
		conn.Close()
	}, true, nil
}

// leaseName is a lease's name on the server: "<name>@<database>". MySQL
// allows 64 characters; a longer one is replaced by a hash of the whole.
// The database is asked once and kept.
func (s *Store) leaseName(ctx context.Context, conn *sql.Conn, name string) (string, error) {
	s.leaseMu.Lock()
	db, known := s.leaseDB, s.leaseDBKnown
	s.leaseMu.Unlock()
	if !known {
		var d sql.NullString
		if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&d); err != nil {
			return "", err
		}
		db = d.String
		s.leaseMu.Lock()
		s.leaseDB, s.leaseDBKnown = db, true
		s.leaseMu.Unlock()
	}
	return scopedLockName(name, db), nil
}

// scopedLockName joins a lock's name and its database into one GET_LOCK name.
func scopedLockName(name, database string) string {
	if database == "" {
		return name
	}
	full := name + "@" + database
	if len(full) <= 64 {
		return full
	}
	sum := sha1.Sum([]byte(full))
	return "yaku.lease." + hex.EncodeToString(sum[:])
}
