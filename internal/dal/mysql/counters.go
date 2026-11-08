package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// The shared counters and blocks for a gateway with no Redis (DAL plan of
// 29 Oct 2026, step 3: "MySQL also does what Redis does"). With Redis set,
// dal.CountersOn picks the main Redis and none of this is used: MySQL +
// Redis runs exactly as before.
//
// Every area is in the config store (the one database every deployment of
// a gateway shares), on its one pool a process:
//
//	Limits          limit_buckets   (limits.go)      config/0164
//	Usage           usage_counts    (limits.go)      config/0164
//	Blocks          blocks, block_addresses, block_identities (blocks.go) config/0165
//	Dedupe          dedupe_keys     (dedupe.go)      config/0028
//	CampaignWindow  campaign_counts (campaign.go)    config/0166
//	AbuseCounts     abuse_*         (abusecount.go)  config/0166
//
// What Redis ends by itself (a key's TTL) is a column here: a read leaves an
// ended row out, a write over an ended row starts it again, and the
// counters' sweeper (counters_sweep.go) removes ended rows, one process of
// the gateway at a time.

// counterStmts is the statements of the shared counters and blocks.
var counterStmts = stmt.New("mysql")

// CounterStatements is the registry above, for the lint and plan tests.
func CounterStatements() *stmt.Registry { return counterStmts }

// The six counter areas, on the config store's pool.
var _ = serves(func(d *dal.Driver) { d.Counters = OpenCounters })

// OpenCounters opens all six areas on the config store's pool (dbpool, one
// per database per process), and starts this process's turn at the
// counters' sweeper.
func OpenCounters(cfg config.Config) (*dal.Counters, error) {
	db, err := dbpool.Get(cfg.ConfigDSN)
	if err != nil {
		return nil, err
	}
	startCounterSweeper(db)
	return CountersOn(db), nil
}

// CountersOn keeps the six areas on a pool the caller already has (tests).
// No sweeper is started.
func CountersOn(db *sql.DB) *dal.Counters {
	return &dal.Counters{
		Limits:    NewLimits(db),
		Usage:     NewUsage(db),
		Blocks:    NewBlocks(db),
		Dedupe:    NewDedupe(db),
		Campaigns: NewCampaignWindow(db),
		Abuse:     NewAbuseCounts(db),
		// The pool is the process's, shared: never closed here.
	}
}

// keyLimit is the longest key kept as it is; a longer one is kept as '#'
// and its SHA-256 (the columns are VARBINARY(255)).
const keyLimit = 255

// dbKey is a counter's key as its row keeps it.
func dbKey(k string) string {
	if len(k) <= keyLimit {
		return k
	}
	sum := sha256.Sum256([]byte(k))
	return "#" + hex.EncodeToString(sum[:])
}

// hashOf is the SHA-256 a row is found by when its key may be long (a
// ladder's id carries a token).
func hashOf(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

// exec runs one registered statement and counts it.
func exec(ctx context.Context, db *sql.DB, s *stmt.Stmt, args ...any) (sql.Result, error) {
	start := time.Now()
	res, err := db.ExecContext(ctx, s.SQL, args...)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	s.Observe(time.Since(start), n)
	return res, nil
}

// execText runs a statement built at the call from a registered one (its
// rows or its list repeated) and counts it under that statement's name.
func execText(ctx context.Context, db *sql.DB, s *stmt.Stmt, q string, args ...any) (sql.Result, error) {
	start := time.Now()
	res, err := db.ExecContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	s.Observe(time.Since(start), n)
	return res, nil
}

// answerOf reads the number a statement handed back through
// LAST_INSERT_ID(expr): MySQL returns expr in the statement's own answer
// (the OK packet), so a change and what it came to are ONE round trip, read
// under the row's lock — no second statement, no other process in between.
func answerOf(res sql.Result) (int64, error) {
	return res.LastInsertId()
}

// rowsOf repeats a row of n placeholders count times: "(?,?),(?,?)".
func rowsOf(n, count int) string {
	row := "(" + placeholders(n) + ")"
	b := make([]byte, 0, count*(len(row)+1))
	for i := 0; i < count; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, row...)
	}
	return string(b)
}

// rowsPerStatement bounds a multi-row write: one packet each, small enough
// that a lock is held briefly.
const rowsPerStatement = 500

var (
	_ dal.Limits         = (*Limits)(nil)
	_ dal.Usage          = (*Usage)(nil)
	_ dal.Blocks         = (*Blocks)(nil)
	_ dal.CampaignWindow = (*CampaignWindow)(nil)
	_ dal.AbuseCounts    = (*AbuseCounts)(nil)
)
