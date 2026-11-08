package mssql

import (
	"context"
	"database/sql"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Blocks is the login ladders and the guessing guard in the config store
// (dal.Blocks) for an install with no Redis: blocks, block_addresses and
// block_identities (mirrors internal/dal/mysql/blocks.go). A lock set by one
// process is seen by all. The rules are the caller's (dal.LadderRules);
// times are unix milliseconds of the caller's clock.
//
// A failure is ONE statement in the usual case (an UPDATE on the row's
// primary key that OUTPUTs its answer); the first failure of an id makes its
// row first (two statements, once).
type Blocks struct{ db *sql.DB }

// NewBlocks keeps the ladders on the config store's pool.
func NewBlocks(db *sql.DB) *Blocks { return &Blocks{db: db} }

var (
	// blocks.fail is MySQL's one UPDATE. MySQL applies the assignments left
	// to right, each seeing those before it; SQL Server reads the row as it
	// was in every assignment, so each step is a CROSS APPLY that reads the
	// ones before it:
	//   f  the failures of this minute, this one included (a new minute,
	//      60 s, when the old one ended);
	//   x  the trip: f.fails reached the threshold;
	//   l  fails_until (0 on a trip, else the minute's end) and the level
	//      (on a trip: rung 1 when the level has fallen back, one rung up
	//      when the last lock ended less than "escalate within" ago, at most
	//      4; else as it was);
	//   r  the rung's seconds; k the lock's end; u the level's and the climb
	//      window's ends;
	//   a  what this failure answers: 1 + 2·locked + 4·level + 32·lock
	//      seconds (never 0, so no row OUTPUT says no row was there).
	stBlockFail = CounterStmts.Add("blocks.fail", `
		UPDATE b SET fails = f.fails, fails_until = l.fails_until, level = l.level,
		  locked_until = k.locked_until, level_until = u.level_until, climb_until = u.climb_until,
		  answer = a.answer, ends_at = GREATEST(l.fails_until, k.locked_until, u.level_until, u.climb_until)
		OUTPUT inserted.answer
		FROM dbo.blocks AS b
		CROSS APPLY (SELECT fails = CASE WHEN b.fails_until <= @now THEN 1 ELSE b.fails + 1 END) AS f
		CROSS APPLY (SELECT trip = CASE WHEN f.fails >= @threshold THEN 1 ELSE 0 END) AS x
		CROSS APPLY (SELECT
		    fails_until = CASE WHEN x.trip = 1 THEN 0 WHEN b.fails_until <= @now THEN @now + 60000 ELSE b.fails_until END,
		    level = CASE WHEN x.trip = 0 THEN b.level
		                 WHEN b.level = 0 OR b.level_until <= @now THEN 1
		                 WHEN b.climb_until > @now AND b.level < 4 THEN b.level + 1
		                 ELSE b.level END) AS l
		CROSS APPLY (SELECT rung = CASE l.level WHEN 1 THEN @r1 WHEN 2 THEN @r2 WHEN 3 THEN @r3 ELSE @r4 END) AS r
		CROSS APPLY (SELECT locked_until = CASE WHEN x.trip = 1 THEN @now + 1000 * r.rung ELSE b.locked_until END) AS k
		CROSS APPLY (SELECT
		    level_until = CASE WHEN x.trip = 1 THEN k.locked_until + 1000 * @decay ELSE b.level_until END,
		    climb_until = CASE WHEN x.trip = 1 THEN k.locked_until + 1000 * @climb ELSE b.climb_until END) AS u
		CROSS APPLY (SELECT answer = 1 + CASE WHEN x.trip = 1 THEN 2 + 4 * l.level + 32 * r.rung
		                                      ELSE 4 * CASE WHEN u.level_until > @now THEN l.level ELSE 0 END END) AS a
		WHERE b.id_hash = @hash`, true)
	// blocks.make is MySQL's INSERT IGNORE: the key's range is held while
	// it looks, so two first failures make one row.
	stBlockMake = CounterStmts.Add("blocks.make", `
		INSERT INTO dbo.blocks (id_hash, id) SELECT @p1, @p2
		 WHERE NOT EXISTS (SELECT 1 FROM dbo.blocks WITH (UPDLOCK, SERIALIZABLE) WHERE id_hash = @p1)`, true)
	stBlockLockedFor = CounterStmts.Add("blocks.locked_for", `SELECT locked_until FROM dbo.blocks WHERE id_hash = @p1`, true)
	stBlockReset     = CounterStmts.Add("blocks.reset", `UPDATE dbo.blocks SET fails = 0, fails_until = 0 WHERE id_hash = @p1 AND fails_until > 0`, true)
	stBlockClear     = CounterStmts.Add("blocks.clear", `DELETE FROM dbo.blocks WHERE id_hash = @p1`, false)
	// blocks.locked reads who is locked now through idx_blocks_locked,
	// longest locked first.
	stBlockLocked = CounterStmts.Add("blocks.locked", `
		SELECT TOP (@p2) id, level, locked_until FROM dbo.blocks WHERE locked_until > @p1 ORDER BY locked_until DESC`, false)

	// blocks.see: the identities seen from an address, MySQL's short
	// transaction of five statements as one batch. The address's row is
	// locked first (made when it is not there), so two failures from one
	// address are counted one after the other.
	stSpraySee = CounterStmts.Add("blocks.see", `SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @yk_n INT, @yk_until BIGINT, @yk_added INT;
BEGIN TRAN;
UPDATE t SET @yk_n = t.distinct_n, @yk_until = t.until_ms, distinct_n = t.distinct_n
  FROM dbo.block_addresses AS t WITH (UPDLOCK, SERIALIZABLE) WHERE t.addr_hash = @addr;
IF @@ROWCOUNT = 0
BEGIN
  INSERT INTO dbo.block_addresses (addr_hash, distinct_n, until_ms) VALUES (@addr, 0, 0);
  SELECT @yk_n = 0, @yk_until = 0;
END
IF @yk_until <= @now
BEGIN
  -- The window ended (or was cleared): the old identities go.
  DELETE FROM dbo.block_identities WHERE addr_hash = @addr;
  SET @yk_n = 0;
END
INSERT INTO dbo.block_identities (addr_hash, ident_hash) SELECT @addr, @ident
 WHERE NOT EXISTS (SELECT 1 FROM dbo.block_identities WITH (UPDLOCK, SERIALIZABLE) WHERE addr_hash = @addr AND ident_hash = @ident);
SET @yk_added = @@ROWCOUNT;
SET @yk_n = @yk_n + @yk_added;
UPDATE dbo.block_addresses SET distinct_n = @yk_n, until_ms = @now + @window WHERE addr_hash = @addr;
COMMIT;
SELECT @yk_added, @yk_n;`, false)
	stSprayClear = CounterStmts.Add("blocks.spray_clear", `UPDATE dbo.block_addresses SET distinct_n = 0, until_ms = 0 WHERE addr_hash = @p1`, false)
)

// LockedFor answers the lock's time left; 0 = not locked.
func (b *Blocks) LockedFor(ctx context.Context, id string) (time.Duration, error) {
	start := time.Now()
	var until int64
	err := b.db.QueryRowContext(ctx, stBlockLockedFor.SQL, idHash(id)).Scan(&until)
	if err == sql.ErrNoRows {
		observe(stBlockLockedFor, start, 0)
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	observe(stBlockLockedFor, start, 1)
	if left := until - time.Now().UnixMilli(); left > 0 {
		return time.Duration(left) * time.Millisecond, nil
	}
	return 0, nil
}

// Fail counts one failure of id under r: one statement, two the first time.
func (b *Blocks) Fail(ctx context.Context, id string, r dal.LadderRules, now time.Time) (bool, time.Duration, int, error) {
	h := idHash(id)
	secs := func(d time.Duration) int64 { return int64(d / time.Second) }
	args := []any{
		sql.Named("now", now.UnixMilli()), sql.Named("threshold", int64(r.Threshold)),
		sql.Named("r1", secs(r.Rungs[0])), sql.Named("r2", secs(r.Rungs[1])),
		sql.Named("r3", secs(r.Rungs[2])), sql.Named("r4", secs(r.Rungs[3])),
		sql.Named("decay", secs(r.DecayAfter)), sql.Named("climb", secs(r.EscalateWithin)),
		sql.Named("hash", h),
	}
	for made := false; ; made = true {
		start := time.Now()
		var v int64
		err := b.db.QueryRowContext(ctx, stBlockFail.SQL, args...).Scan(&v)
		if err != nil && err != sql.ErrNoRows {
			return false, 0, 0, err
		}
		if err == nil {
			observe(stBlockFail, start, 1)
			v--
			return v&2 != 0, time.Duration(v>>5) * time.Second, int(v>>2) & 7, nil
		}
		observe(stBlockFail, start, 0)
		if made {
			// The row was made and is still not there: a ClearBlock in
			// between. Nothing was counted; say so as not locked.
			return false, 0, 0, nil
		}
		if _, err := counterExec(ctx, b.db, stBlockMake, h, id); err != nil && !IsDuplicate(err) {
			return false, 0, 0, err
		}
	}
}

// ResetFails clears the failure count — the level and the lock stay, as
// with Redis.
func (b *Blocks) ResetFails(ctx context.Context, id string) error {
	_, err := counterExec(ctx, b.db, stBlockReset, idHash(id))
	return err
}

// ClearBlock removes everything of id: lock, count, level, escalate window.
func (b *Blocks) ClearBlock(ctx context.Context, id string) error {
	_, err := counterExec(ctx, b.db, stBlockClear, idHash(id))
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
	rows, err := b.db.QueryContext(ctx, stBlockLocked.SQL, now, int64(limit+1))
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
	observe(stBlockLocked, start, int64(len(out)))
	if len(out) > limit {
		return out[:limit], true, nil
	}
	return out, false, nil
}

// SeeIdentity notes a failed identity from addr, in one short transaction
// (one round trip): added = it was not yet among the address's, distinct =
// how many there are now. Each call keeps the set for window again; a set
// whose window ended starts again from nothing.
func (b *Blocks) SeeIdentity(ctx context.Context, addr, identity string, window time.Duration) (bool, int, error) {
	ah, ih := idHash(addr), idHash(identity)
	var added, distinct int64
	err := Again(ctx, counterTries, func() error {
		start := time.Now()
		err := b.db.QueryRowContext(ctx, stSpraySee.SQL,
			sql.Named("addr", ah), sql.Named("ident", ih),
			sql.Named("now", time.Now().UnixMilli()), sql.Named("window", window.Milliseconds()),
		).Scan(&added, &distinct)
		if err == nil {
			observe(stSpraySee, start, added)
		}
		return err
	})
	if err != nil {
		return false, 0, err
	}
	return added == 1, int(distinct), nil
}

// ClearIdentities forgets addr's identities: the set's window is ended, and
// the next failure from addr starts a new one (removing the old rows).
func (b *Blocks) ClearIdentities(ctx context.Context, addr string) error {
	_, err := counterExec(ctx, b.db, stSprayClear, idHash(addr))
	return err
}
