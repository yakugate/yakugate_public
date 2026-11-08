package mssql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	mssqldrv "github.com/microsoft/go-mssqldb"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The shared counters and blocks on SQL Server, for an install with no
// Redis (mirrors internal/dal/mysql/counters.go). With Redis set,
// dal.CountersOn picks the main Redis and none of this is used.
//
// Every area is in the config store, on its one pool a process:
//
//	Limits          limit_buckets   (limits.go)
//	Usage           usage_counts    (limits.go)
//	Blocks          blocks, block_addresses, block_identities (blocks.go)
//	Dedupe          dedupe_keys     (dedupe.go)
//	CampaignWindow  campaign_counts (campaign.go)
//	AbuseCounts     abuse_*         (abusecount.go)
//
// What Redis ends by itself (a key's TTL) is a column here: a read leaves an
// ended row out, a write over an ended row starts it again, and the
// counters' sweeper (counters_sweep.go) removes ended rows, one process of
// the gateway at a time.
//
// The batch writes (the campaign window, the abuse counters) send their
// rows as a table-valued parameter; the table types are made by the config
// store's migration (counterRowTypes in counters_contract_test.go holds the
// same DDL for a scratch database).

// CounterStmts is the statements of the shared counters and blocks.
var CounterStmts = newRegistry()

var _ = serves(func(d *dal.Driver) { d.Counters = openCounters })

// openCounters opens all six areas on the config store's pool (Get, one per
// database per process), and starts this process's turn at the counters'
// sweeper.
func openCounters(cfg config.Config) (*dal.Counters, error) {
	db, err := Get(cfg.ConfigDSN)
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

// counterKeyLimit is the longest key kept as it is; a longer one is kept as
// '#' and its SHA-256 (the columns are VARBINARY(255)).
const counterKeyLimit = 255

// counterKey is a counter's key as its row keeps it, as the VARBINARY the
// column is (a Go string would go as NVARCHAR and lose the seek).
func counterKey(k string) []byte {
	if len(k) <= counterKeyLimit {
		return []byte(k)
	}
	sum := sha256.Sum256([]byte(k))
	return []byte("#" + hex.EncodeToString(sum[:]))
}

// idHash is the SHA-256 a row is found by when its key may be long (a
// ladder's id carries a token).
func idHash(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

// counterExec runs one registered statement and counts it.
func counterExec(ctx context.Context, q Querier, s *stmt.Stmt, args ...any) (int64, error) {
	start := time.Now()
	res, err := q.ExecContext(ctx, s.SQL, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	observe(s, start, n)
	return n, nil
}

// counterAnswer runs one registered statement that answers one number (a
// batch whose last SELECT hands back what it did).
func counterAnswer(ctx context.Context, q Querier, s *stmt.Stmt, args ...any) (int64, error) {
	start := time.Now()
	var v sql.NullInt64
	if err := q.QueryRowContext(ctx, s.SQL, args...).Scan(&v); err != nil {
		return 0, err
	}
	observe(s, start, 1)
	return v.Int64, nil
}

// upsertRows runs a batch upsert with more named parameters beside its
// rows (@now, the clock its assignments read).
func upsertRows(ctx context.Context, q Querier, b *UpsertBatch, rows any, args ...any) error {
	if empty(rows) {
		return nil
	}
	start := time.Now()
	all := append([]any{sql.Named("rows", mssqldrv.TVP{TypeName: b.Type, Value: rows})}, args...)
	var changed, added int64
	if err := q.QueryRowContext(ctx, b.Stmt.SQL, all...).Scan(&changed, &added); err != nil {
		return err
	}
	observe(b.Stmt, start, changed+added)
	return nil
}

// namedList is "@b1, @b2, …" and its parameters, for a statement's IN list
// written at the call (its registered form has one, "@in").
func namedList(prefix string, vals []int64) (string, []any) {
	names := make([]string, len(vals))
	args := make([]any, len(vals))
	for i, v := range vals {
		n := fmt.Sprintf("%s%d", prefix, i+1)
		names[i] = "@" + n
		args[i] = sql.Named(n, v)
	}
	return strings.Join(names, ", "), args
}

// withList puts a list into a registered statement's "IN (@in)".
func withList(q, list string) string {
	return strings.Replace(q, "IN (@in)", "IN ("+list+")", 1)
}

// counterRowsPerStatement bounds a batch write: small enough that a lock is
// held briefly (the MySQL DAL's rowsPerStatement).
const counterRowsPerStatement = 500

// counterTries is how many times a write a deadlock chose as its victim is
// run, where the MySQL DAL runs it again (againIfVictimN(ctx, 4, …)).
const counterTries = 4

var (
	_ dal.Limits         = (*Limits)(nil)
	_ dal.Usage          = (*Usage)(nil)
	_ dal.Blocks         = (*Blocks)(nil)
	_ dal.Dedupe         = (*Dedupe)(nil)
	_ dal.DedupeSweeper  = (*Dedupe)(nil)
	_ dal.CampaignWindow = (*CampaignWindow)(nil)
	_ dal.AbuseCounts    = (*AbuseCounts)(nil)
)
