package mssql

import (
	"context"
	"database/sql"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
)

// The counters' sweeper (mirrors internal/dal/mysql/counters_sweep.go): what
// Redis ends by itself (a key's TTL) is removed here once it has ended, so
// the tables stay small. Every process that opens the counters on SQL Server
// takes part, and ONE at a time sweeps: each minute a process asks for the
// named lease "counters-sweep" (locks.go, an application lock of the config
// database), sweeps if it got it, and lets go.
//
// Nothing depends on the sweep for a right answer: every read leaves an ended
// row out and every write over an ended row starts it again.

// counterSweepEvery is how often a process asks for its turn.
const counterSweepEvery = time.Minute

// counterSweepBatch rows a statement; a table is swept again while a
// statement removed a full batch, at most counterSweepRounds times a turn.
const (
	counterSweepBatch  = 5000
	counterSweepRounds = 20
	// counterBlockSlack keeps a ladder's row two minutes past its end, for
	// two processes whose clocks differ.
	counterBlockSlack = 2 * time.Minute
	// counterLimitSlack keeps a second's bucket a minute, for a process
	// whose clock is behind.
	counterLimitSlack = 60
)

var (
	stSweepLimits   = CounterStmts.Add("sweep.limits", `DELETE TOP (5000) FROM dbo.limit_buckets WHERE sec < @p1`, false)
	stSweepUsage    = CounterStmts.Add("sweep.usage", `DELETE TOP (5000) FROM dbo.usage_counts WHERE expires_at < @p1`, false)
	stSweepBlocks   = CounterStmts.Add("sweep.blocks", `DELETE TOP (5000) FROM dbo.blocks WHERE ends_at < @p1`, false)
	stSweepCampaign = CounterStmts.Add("sweep.campaign", `DELETE TOP (5000) FROM dbo.campaign_counts WHERE expires_at < @p1`, false)
	stSweepAbuse    = CounterStmts.Add("sweep.abuse_counts", `DELETE TOP (5000) FROM dbo.abuse_counts WHERE expires_at < @p1`, false)
	stSweepNumbers  = CounterStmts.Add("sweep.abuse_numbers", `DELETE TOP (5000) FROM dbo.abuse_numbers WHERE expires_at < @p1`, false)
	stSweepTexts    = CounterStmts.Add("sweep.abuse_texts", `DELETE TOP (5000) FROM dbo.abuse_texts WHERE expires_at < @p1`, false)
	// sweep.addresses removes up to 500 ended address sets with their
	// identities, in one transaction that locks the address rows first: a
	// failure from one of them waits, then finds its set gone and starts a
	// new one. MySQL's four statements, one round trip.
	stSweepAddrs = CounterStmts.Add("sweep.addresses", `SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @yk_ended TABLE (addr_hash BINARY(32) NOT NULL PRIMARY KEY);
DECLARE @yk_n INT;
BEGIN TRAN;
INSERT INTO @yk_ended (addr_hash)
  SELECT TOP (500) addr_hash FROM dbo.block_addresses WITH (UPDLOCK, ROWLOCK)
   WHERE until_ms < @p1 ORDER BY until_ms;
DELETE i FROM dbo.block_identities AS i INNER JOIN @yk_ended AS e ON e.addr_hash = i.addr_hash;
DELETE a FROM dbo.block_addresses AS a INNER JOIN @yk_ended AS e ON e.addr_hash = a.addr_hash;
SET @yk_n = @@ROWCOUNT;
COMMIT;
SELECT @yk_n;`, false)
)

var counterSweepers sync.Map // *sql.DB → true: one sweeper a pool (a process)

// startCounterSweeper starts this process's turn at the sweep, once a pool.
func startCounterSweeper(db *sql.DB) {
	if _, started := counterSweepers.LoadOrStore(db, true); started {
		return
	}
	go func() {
		locks := &Locks{db: db}
		// A first turn a little after start, spread so the roles of a
		// gateway started together do not all ask in the same second.
		wait := 10*time.Second + time.Duration(rand.Int63n(int64(20*time.Second)))
		for {
			time.Sleep(wait)
			wait = counterSweepEvery
			counterSweepTurn(locks, db)
		}
	}()
}

// counterSweepTurn sweeps when this process gets the lease.
func counterSweepTurn(locks *Locks, db *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	held, answer, err := locks.TryLock(ctx, "counters-sweep")
	if err != nil || answer != dal.LockTaken {
		return
	}
	defer held.Release()
	if _, err := SweepCounters(ctx, db); err != nil {
		log.Printf("[counters] sweep of the ended counters stopped: %v", err)
	}
}

// SweepCounters removes the ended rows of every counter table and answers
// how many. It takes no lease (the caller does).
func SweepCounters(ctx context.Context, db *sql.DB) (int64, error) {
	now := time.Now()
	sec, ms := now.Unix(), now.UnixMilli()
	var total int64
	for _, step := range []struct {
		s    *stmt.Stmt
		upTo int64
	}{
		{stSweepLimits, sec - counterLimitSlack},
		{stSweepUsage, sec},
		{stSweepBlocks, ms - counterBlockSlack.Milliseconds()},
		{stSweepCampaign, sec},
		{stSweepAbuse, sec},
		{stSweepNumbers, sec},
		{stSweepTexts, sec},
	} {
		for round := 0; round < counterSweepRounds; round++ {
			n, err := counterExec(ctx, db, step.s, step.upTo)
			if err != nil {
				return total, err
			}
			total += n
			if n < counterSweepBatch {
				break
			}
		}
	}
	for round := 0; round < counterSweepRounds; round++ {
		n, err := counterAnswer(ctx, db, stSweepAddrs, ms-counterBlockSlack.Milliseconds())
		total += n
		if err != nil || n == 0 {
			return total, err
		}
	}
	return total, nil
}
