package mysql

import (
	"context"
	"database/sql"
	"time"
)

// Limits is the per-second buckets in the config store (dal.Limits) for a
// gateway with no Redis: limit_buckets (config/0164), one row per key and
// second, as Redis's rl:<key>:<second>. Every process of the gateway draws
// from the same rows, so N processes enforce ONE limit, and a restart loses
// nothing.
//
// Each call is ONE statement on the row's primary key, and the answer comes
// back in the statement's own reply (LAST_INSERT_ID(expr), answerOf): the
// row is locked while it is read and changed, so two processes can never
// both take the last units — the same guarantee as Redis's scripts.
//
// The accept path does not call this once a message: ratelimit.Chunked takes
// a CHUNK of a bucket (a quarter of the limit, or what one request needs if
// more) with Grant and hands it out from memory, so a busy bucket is asked a
// few times a second by each process (internal/ratelimit allowance.go). What
// a process took and did not use ends with its second; nobody else can have
// it in that second, which is the price of "never more than the limit".
type Limits struct{ db *sql.DB }

// NewLimits keeps the buckets on the config store's pool.
func NewLimits(db *sql.DB) *Limits { return &Limits{db: db} }

var (
	// limits.spend adds n and answers the count after the add. On a new row
	// the VALUES expression hands back n; on an existing one the UPDATE hands
	// back the new count (the later LAST_INSERT_ID wins).
	stLimitSpend = counterStmts.Add("limits.spend", `
		INSERT INTO limit_buckets (bkey, sec, used) VALUES (?, ?, LAST_INSERT_ID(?))
		ON DUPLICATE KEY UPDATE used = LAST_INSERT_ID(used + ?)`, true)
	// limits.grant takes as much of `want` as still fits under `limit` and
	// answers how much that was (0 when the bucket is full, or over: a spend
	// may take it past its limit). Take and Grant are the same statement.
	stLimitGrant = counterStmts.Add("limits.grant", `
		INSERT INTO limit_buckets (bkey, sec, used) VALUES (?, ?, LAST_INSERT_ID(GREATEST(0, LEAST(?, ?))))
		ON DUPLICATE KEY UPDATE used = used + LAST_INSERT_ID(GREATEST(0, LEAST(?, ? - used)))`, true)
	// limits.return hands back up to n, never below zero, and only to a row
	// that is there (a return never makes a bucket).
	stLimitReturn = counterStmts.Add("limits.return", `
		UPDATE limit_buckets SET used = used - LEAST(used, ?) WHERE bkey = ? AND sec = ? AND used > 0`, true)
)

// Spend adds n to the bucket and answers its count after the add.
func (l *Limits) Spend(ctx context.Context, key string, sec int64, n int) (int, error) {
	res, err := exec(ctx, l.db, stLimitSpend, dbKey(key), sec, n, n)
	if err != nil {
		return 0, err
	}
	v, err := answerOf(res)
	return int(v), err
}

// Take takes up to max under limit, in one statement.
func (l *Limits) Take(ctx context.Context, key string, sec int64, limit, max int) (int, error) {
	return l.grant(ctx, key, sec, limit, max)
}

// Grant takes up to want under limit, in one statement.
func (l *Limits) Grant(ctx context.Context, key string, sec int64, limit, want int) (int, error) {
	return l.grant(ctx, key, sec, limit, want)
}

func (l *Limits) grant(ctx context.Context, key string, sec int64, limit, want int) (int, error) {
	if want < 0 {
		want = 0
	}
	res, err := exec(ctx, l.db, stLimitGrant, dbKey(key), sec, want, limit, want, limit)
	if err != nil {
		return 0, err
	}
	v, err := answerOf(res)
	return int(v), err
}

// Return hands back unused units to a bucket that is there.
func (l *Limits) Return(ctx context.Context, key string, sec int64, n int) error {
	if n < 1 {
		return nil
	}
	_, err := exec(ctx, l.db, stLimitReturn, n, dbKey(key), sec)
	return err
}

// Usage is the period counts in the config store (dal.Usage) for a gateway
// with no Redis: usage_counts (config/0164), as Redis's qu:<key>. A key's end
// is set when it is made (the life the caller gives), as Redis's EXPIRE on
// the first INCRBY; an ended key counted again starts from zero.
//
// One statement an add and one a read, as on Redis (an INCRBY, a GET). The
// quota and per-destination stages ask only for accounts that have a quota
// or a cap set; one with none costs nothing.
type Usage struct{ db *sql.DB }

// NewUsage keeps the counts on the config store's pool.
func NewUsage(db *sql.DB) *Usage { return &Usage{db: db} }

var (
	// n is assigned before expires_at on purpose: MySQL applies the
	// assignments left to right, so the second IF still sees the row's old
	// end (as dedupe.check does).
	stUsageAdd = counterStmts.Add("usage.add", `
		INSERT INTO usage_counts (ukey, n, expires_at) VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE
		  n = IF(expires_at <= ?, VALUES(n), n + VALUES(n)),
		  expires_at = IF(expires_at <= ?, VALUES(expires_at), expires_at)`, true)
	stUsageGet = counterStmts.Add("usage.get", `
		SELECT n FROM usage_counts WHERE ukey = ? AND expires_at > ?`, true)
)

// AddUsage adds n to key; a new key lives for life.
func (u *Usage) AddUsage(ctx context.Context, key string, n int, life time.Duration) error {
	now := time.Now().Unix()
	life = max(life, time.Second)
	_, err := exec(ctx, u.db, stUsageAdd, dbKey(key), n, now+int64(life/time.Second), now, now)
	return err
}

// GetUsage reads key's count, 0 for one never counted or ended.
func (u *Usage) GetUsage(ctx context.Context, key string) (int, error) {
	start := time.Now()
	var n int64
	err := u.db.QueryRowContext(ctx, stUsageGet.SQL, dbKey(key), time.Now().Unix()).Scan(&n)
	if err == sql.ErrNoRows {
		stUsageGet.Observe(time.Since(start), 0)
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	stUsageGet.Observe(time.Since(start), 1)
	return int(n), nil
}
