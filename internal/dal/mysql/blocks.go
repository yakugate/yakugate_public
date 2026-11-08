package mysql

import (
	"context"
	"database/sql"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Blocks is the login ladders and the guessing guard in the config store
// (dal.Blocks) for a gateway with no Redis: blocks, block_addresses and
// block_identities (config/0165). A lock set by one process is seen by all,
// and survives a restart, as with Redis. The rules (threshold, rungs, decay)
// are the caller's (dal.LadderRules); times are unix milliseconds of the
// caller's clock.
//
// What Redis keeps in four keys an id (lkf:, lku:, lkl:, lke:) is one row;
// its index of who is locked (lkidx) is the key idx_blocks_locked. A failure
// is ONE statement in the usual case (an UPDATE on the row's primary key that
// hands its answer back, answerOf); the first failure of an id makes its row
// first (two statements, once).
type Blocks struct{ db *sql.DB }

// NewBlocks keeps the ladders on the config store's pool.
func NewBlocks(db *sql.DB) *Blocks { return &Blocks{db: db} }

var (
	// blocks.fail is failScript as one UPDATE. MySQL applies the assignments
	// of a one-table UPDATE left to right, each seeing those before it, so:
	//   fails        the failures of this minute, this one included (a new
	//                minute, 60 s as Redis's lkf:, when the old one ended);
	//   then "fails >= threshold" is the trip, read by every later column;
	//   fails_until  0 on a trip (the next failure starts a new minute), else
	//                the minute's end;
	//   level        on a trip: rung 1 when the level has fallen back, one
	//                rung up when the last lock ended less than "escalate
	//                within" ago (climb_until), at most 4; else as it was;
	//   locked_until, level_until, climb_until  set on a trip from the rung
	//                reached (the lock; the lock + the decay; the lock + the
	//                escalate window);
	//   answer       what this failure answers, handed back through
	//                LAST_INSERT_ID: 1 + 2·locked + 4·level + 32·lock seconds
	//                (never 0, so 0 says no row was there);
	//   ends_at      when all of it has ended, for the sweeper.
	// The ? are, in order: now; T, now, now; T, now, now; T, now, r1..r4;
	// T, decay; T, climb; T, r1..r4, now; id hash.
	stBlockFail = counterStmts.Add("blocks.fail", `
		UPDATE blocks SET
		  fails = IF(fails_until <= ?, 1, fails + 1),
		  fails_until = IF(fails >= ?, 0, IF(fails_until <= ?, ? + 60000, fails_until)),
		  level = IF(fails >= ?,
		             CASE WHEN level = 0 OR level_until <= ? THEN 1
		                  WHEN climb_until > ? AND level < 4 THEN level + 1
		                  ELSE level END,
		             level),
		  locked_until = IF(fails >= ?, ? + 1000 * CASE level WHEN 1 THEN ? WHEN 2 THEN ? WHEN 3 THEN ? ELSE ? END, locked_until),
		  level_until = IF(fails >= ?, locked_until + 1000 * ?, level_until),
		  climb_until = IF(fails >= ?, locked_until + 1000 * ?, climb_until),
		  answer = LAST_INSERT_ID(1 + IF(fails >= ?,
		             2 + 4 * level + 32 * CASE level WHEN 1 THEN ? WHEN 2 THEN ? WHEN 3 THEN ? ELSE ? END,
		             4 * IF(level_until > ?, level, 0))),
		  ends_at = GREATEST(fails_until, locked_until, level_until, climb_until)
		WHERE id_hash = ?`, true)
	stBlockMake      = counterStmts.Add("blocks.make", `INSERT IGNORE INTO blocks (id_hash, id) VALUES (?, ?)`, true)
	stBlockLockedFor = counterStmts.Add("blocks.locked_for", `SELECT locked_until FROM blocks WHERE id_hash = ?`, true)
	stBlockReset     = counterStmts.Add("blocks.reset", `UPDATE blocks SET fails = 0, fails_until = 0 WHERE id_hash = ? AND fails_until > 0`, true)
	stBlockClear     = counterStmts.Add("blocks.clear", `DELETE FROM blocks WHERE id_hash = ?`, false)
	// blocks.locked reads who is locked now through idx_blocks_locked,
	// longest locked first, as Redis reads lkidx.
	stBlockLocked = counterStmts.Add("blocks.locked", `
		SELECT id, level, locked_until FROM blocks WHERE locked_until > ? ORDER BY locked_until DESC LIMIT ?`, false)

	// The identities seen from an address, in one short transaction: the
	// address's row is locked first (an upsert that changes nothing), so two
	// failures from one address are counted one after the other, exactly as
	// Redis's script counts them.
	stSprayLock   = counterStmts.Add("blocks.spray_lock", `INSERT INTO block_addresses (addr_hash, distinct_n, until_ms) VALUES (?, 0, 0) ON DUPLICATE KEY UPDATE addr_hash = addr_hash`, false)
	stSprayRead   = counterStmts.Add("blocks.spray_read", `SELECT distinct_n, until_ms FROM block_addresses WHERE addr_hash = ? FOR UPDATE`, false)
	stSprayForget = counterStmts.Add("blocks.spray_forget", `DELETE FROM block_identities WHERE addr_hash = ?`, false)
	stSprayAdd    = counterStmts.Add("blocks.spray_add", `INSERT IGNORE INTO block_identities (addr_hash, ident_hash) VALUES (?, ?)`, false)
	stSprayKeep   = counterStmts.Add("blocks.spray_keep", `UPDATE block_addresses SET distinct_n = ?, until_ms = ? WHERE addr_hash = ?`, false)
	stSprayClear  = counterStmts.Add("blocks.spray_clear", `UPDATE block_addresses SET distinct_n = 0, until_ms = 0 WHERE addr_hash = ?`, false)
)

// LockedFor answers the lock's time left; 0 = not locked.
func (b *Blocks) LockedFor(ctx context.Context, id string) (time.Duration, error) {
	start := time.Now()
	var until int64
	err := b.db.QueryRowContext(ctx, stBlockLockedFor.SQL, hashOf(id)).Scan(&until)
	if err == sql.ErrNoRows {
		stBlockLockedFor.Observe(time.Since(start), 0)
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	stBlockLockedFor.Observe(time.Since(start), 1)
	if left := until - time.Now().UnixMilli(); left > 0 {
		return time.Duration(left) * time.Millisecond, nil
	}
	return 0, nil
}

// Fail counts one failure of id under r: one statement, two the first time.
func (b *Blocks) Fail(ctx context.Context, id string, r dal.LadderRules, now time.Time) (bool, time.Duration, int, error) {
	h := hashOf(id)
	ms := now.UnixMilli()
	secs := func(d time.Duration) int64 { return int64(d / time.Second) }
	r1, r2, r3, r4 := secs(r.Rungs[0]), secs(r.Rungs[1]), secs(r.Rungs[2]), secs(r.Rungs[3])
	t := r.Threshold
	args := []any{
		ms,
		t, ms, ms,
		t, ms, ms,
		t, ms, r1, r2, r3, r4,
		t, secs(r.DecayAfter),
		t, secs(r.EscalateWithin),
		t, r1, r2, r3, r4, ms,
		h,
	}
	for made := false; ; made = true {
		res, err := exec(ctx, b.db, stBlockFail, args...)
		if err != nil {
			return false, 0, 0, err
		}
		v, err := answerOf(res)
		if err != nil {
			return false, 0, 0, err
		}
		if v > 0 {
			v--
			return v&2 != 0, time.Duration(v>>5) * time.Second, int(v>>2) & 7, nil
		}
		if made {
			// The row was made and is still not there: a ClearBlock in
			// between. Nothing was counted; say so as not locked.
			return false, 0, 0, nil
		}
		if _, err := exec(ctx, b.db, stBlockMake, h, id); err != nil {
			return false, 0, 0, err
		}
	}
}

// ResetFails clears the failure count — the level and the lock stay, as
// with Redis.
func (b *Blocks) ResetFails(ctx context.Context, id string) error {
	_, err := exec(ctx, b.db, stBlockReset, hashOf(id))
	return err
}

// ClearBlock removes everything of id: lock, count, level, escalate window.
func (b *Blocks) ClearBlock(ctx context.Context, id string) error {
	_, err := exec(ctx, b.db, stBlockClear, hashOf(id))
	return err
}

// Blocked lists who is locked now, longest locked first; more = there are
// more than limit.
func (b *Blocks) Blocked(ctx context.Context, limit int) ([]dal.Block, bool, error) {
	if limit < 1 {
		limit = 1
	}
	start := time.Now()
	now := time.Now().UnixMilli()
	rows, err := b.db.QueryContext(ctx, stBlockLocked.SQL, now, limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var out []dal.Block
	for rows.Next() {
		var bl dal.Block
		var until int64
		if err := rows.Scan(&bl.ID, &bl.Level, &until); err != nil {
			return nil, false, err
		}
		if bl.Level < 1 {
			bl.Level = 1
		}
		bl.Remaining = time.Duration(until-now) * time.Millisecond
		out = append(out, bl)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	stBlockLocked.Observe(time.Since(start), int64(len(out)))
	if len(out) > limit {
		return out[:limit], true, nil
	}
	return out, false, nil
}

// SeeIdentity notes a failed identity from addr, in one short transaction:
// added = it was not yet among the address's, distinct = how many there are
// now. Each call keeps the set for window again (Redis: EXPIRE on every
// call); a set whose window ended starts again from nothing.
func (b *Blocks) SeeIdentity(ctx context.Context, addr, identity string, window time.Duration) (bool, int, error) {
	ah, ih := hashOf(addr), hashOf(identity)
	var added bool
	var distinct int
	err := againIfVictimN(ctx, 4, func() error {
		var err error
		added, distinct, err = b.seeOnce(ctx, ah, ih, window)
		return err
	})
	return added, distinct, err
}

func (b *Blocks) seeOnce(ctx context.Context, ah, ih []byte, window time.Duration) (bool, int, error) {
	start := time.Now()
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return false, 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit
	if _, err := tx.ExecContext(ctx, stSprayLock.SQL, ah); err != nil {
		return false, 0, err
	}
	var n int
	var until int64
	if err := tx.QueryRowContext(ctx, stSprayRead.SQL, ah).Scan(&n, &until); err != nil {
		return false, 0, err
	}
	now := time.Now().UnixMilli()
	if until <= now {
		// The window ended (or was cleared): the old identities go.
		if _, err := tx.ExecContext(ctx, stSprayForget.SQL, ah); err != nil {
			return false, 0, err
		}
		n = 0
	}
	res, err := tx.ExecContext(ctx, stSprayAdd.SQL, ah, ih)
	if err != nil {
		return false, 0, err
	}
	got, _ := res.RowsAffected()
	added := got == 1
	if added {
		n++
	}
	if _, err := tx.ExecContext(ctx, stSprayKeep.SQL, n, now+window.Milliseconds(), ah); err != nil {
		return false, 0, err
	}
	if err := tx.Commit(); err != nil {
		return false, 0, err
	}
	stSprayAdd.Observe(time.Since(start), got)
	return added, n, nil
}

// ClearIdentities forgets addr's identities: the set's window is ended, and
// the next failure from addr starts a new one (removing the old rows).
func (b *Blocks) ClearIdentities(ctx context.Context, addr string) error {
	_, err := exec(ctx, b.db, stSprayClear, hashOf(addr))
	return err
}
