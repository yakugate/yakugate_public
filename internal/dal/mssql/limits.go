package mssql

import (
	"context"
	"database/sql"
	"time"
)

// Limits is the per-second buckets in the config store (dal.Limits) for an
// install with no Redis: limit_buckets, one row per key and second (mirrors
// internal/dal/mysql/limits.go). Every process of the gateway draws from the
// same rows, so N processes enforce ONE limit.
//
// Each call is ONE round trip: a batch that updates the row under UPDLOCK
// and SERIALIZABLE (the row, or the gap where it would be, is held), inserts
// it when it was not there, and hands back what it did — MySQL's
// INSERT … ON DUPLICATE KEY UPDATE … LAST_INSERT_ID(expr). Two processes can
// never both take the last units: the second waits on the first's lock and
// then reads what the first left.
//
// The accept path does not call this once a message: ratelimit.Chunked takes
// a chunk of a bucket with Grant and hands it out from memory.
type Limits struct{ db *sql.DB }

// NewLimits keeps the buckets on the config store's pool.
func NewLimits(db *sql.DB) *Limits { return &Limits{db: db} }

// limitUpsert is a bucket's change: the UPDATE assigns @yk to what the call
// answers (every expression reads the row as it was); when there is no row,
// first is what the call answers and the new row's count.
func limitUpsert(set, answer, first string) string {
	return `SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @yk BIGINT;
BEGIN TRAN;
UPDATE t SET @yk = ` + answer + `, used = ` + set + `
  FROM dbo.limit_buckets AS t WITH (UPDLOCK, SERIALIZABLE) WHERE t.bkey = @key AND t.sec = @sec;
IF @@ROWCOUNT = 0
BEGIN
  SET @yk = ` + first + `;
  INSERT INTO dbo.limit_buckets (bkey, sec, used) VALUES (@key, @sec, @yk);
END
COMMIT;
SELECT @yk;`
}

var (
	// limits.spend adds n and answers the count after the add.
	stLimitSpend = CounterStmts.Add("limits.spend",
		limitUpsert("t.used + @n", "t.used + @n", "@n"), true)
	// limits.grant takes as much of @want as still fits under @limit and
	// answers how much that was (0 when the bucket is full, or over: a spend
	// may take it past its limit). Take and Grant are the same statement.
	stLimitGrant = CounterStmts.Add("limits.grant",
		limitUpsert("t.used + GREATEST(0, LEAST(@want, @limit - t.used))",
			"GREATEST(0, LEAST(@want, @limit - t.used))",
			"GREATEST(0, LEAST(@want, @limit))"), true)
	// limits.return hands back up to n, never below zero, and only to a row
	// that is there (a return never makes a bucket).
	stLimitReturn = CounterStmts.Add("limits.return", `
		UPDATE dbo.limit_buckets SET used = used - LEAST(used, @n) WHERE bkey = @key AND sec = @sec AND used > 0`, true)
)

// Spend adds n to the bucket and answers its count after the add.
func (l *Limits) Spend(ctx context.Context, key string, sec int64, n int) (int, error) {
	v, err := counterAnswer(ctx, l.db, stLimitSpend,
		sql.Named("key", counterKey(key)), sql.Named("sec", sec), sql.Named("n", int64(n)))
	return int(v), err
}

// Take takes up to max under limit, in one round trip.
func (l *Limits) Take(ctx context.Context, key string, sec int64, limit, max int) (int, error) {
	return l.grant(ctx, key, sec, limit, max)
}

// Grant takes up to want under limit, in one round trip.
func (l *Limits) Grant(ctx context.Context, key string, sec int64, limit, want int) (int, error) {
	return l.grant(ctx, key, sec, limit, want)
}

func (l *Limits) grant(ctx context.Context, key string, sec int64, limit, want int) (int, error) {
	if want < 0 {
		want = 0
	}
	v, err := counterAnswer(ctx, l.db, stLimitGrant,
		sql.Named("key", counterKey(key)), sql.Named("sec", sec),
		sql.Named("want", int64(want)), sql.Named("limit", int64(limit)))
	return int(v), err
}

// Return hands back unused units to a bucket that is there.
func (l *Limits) Return(ctx context.Context, key string, sec int64, n int) error {
	if n < 1 {
		return nil
	}
	_, err := counterExec(ctx, l.db, stLimitReturn,
		sql.Named("n", int64(n)), sql.Named("key", counterKey(key)), sql.Named("sec", sec))
	return err
}

// Usage is the period counts in the config store (dal.Usage) for an install
// with no Redis: usage_counts, as Redis's qu:<key>. A key's end is set when
// it is made (the life the caller gives); an ended key counted again starts
// from zero. One round trip an add (an upsert) and one a read.
type Usage struct{ db *sql.DB }

// NewUsage keeps the counts on the config store's pool.
func NewUsage(db *sql.DB) *Usage { return &Usage{db: db} }

var (
	// usage.add: both assignments read the row as it was (SQL Server never
	// reads an assignment made in the same UPDATE), so the order of the two
	// does not matter here as it does on MySQL.
	stUsageAdd = AddUpsert(CounterStmts, "usage.add", UpsertSpec{
		Table: "dbo.usage_counts", Keys: []string{"ukey"},
		Set: []string{
			"n = CASE WHEN t.expires_at <= @now THEN $n ELSE t.n + $n END",
			"expires_at = CASE WHEN t.expires_at <= @now THEN $expires_at ELSE t.expires_at END",
		},
		Insert: []string{"ukey", "n", "expires_at"},
	}, true)
	stUsageGet = CounterStmts.Add("usage.get", `
		SELECT n FROM dbo.usage_counts WHERE ukey = @p1 AND expires_at > @p2`, true)
)

// AddUsage adds n to key; a new key lives for life.
func (u *Usage) AddUsage(ctx context.Context, key string, n int, life time.Duration) error {
	now := time.Now().Unix()
	life = max(life, time.Second)
	_, err := stUsageAdd.Exec(ctx, u.db,
		sql.Named("ukey", counterKey(key)), sql.Named("n", int64(n)),
		sql.Named("expires_at", now+int64(life/time.Second)), sql.Named("now", now))
	return err
}

// GetUsage reads key's count, 0 for one never counted or ended.
func (u *Usage) GetUsage(ctx context.Context, key string) (int, error) {
	start := time.Now()
	var n int64
	err := u.db.QueryRowContext(ctx, stUsageGet.SQL, counterKey(key), time.Now().Unix()).Scan(&n)
	if err == sql.ErrNoRows {
		observe(stUsageGet, start, 0)
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	observe(stUsageGet, start, 1)
	return int(n), nil
}
