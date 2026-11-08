package mysql

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

// The counters' sweeper: what Redis ends by itself (a key's TTL) is removed
// here once it has ended, so the tables stay small. Every process that opens
// the counters on MySQL takes part, and ONE at a time sweeps: each minute a
// process asks for the named lease "counters-sweep" (dal.Locks, a GET_LOCK
// named with the database), sweeps if it got it, and lets go. A process that
// does not get it does nothing that minute.
//
// Nothing depends on the sweep for a right answer: every read leaves an ended
// row out and every write over an ended row starts it again. A sweep that
// does not run only lets the tables grow.

// sweepEvery is how often a process asks for its turn.
const sweepEvery = time.Minute

// sweepBatch rows a statement; a table is swept again while a statement
// removed a full batch, at most sweepRounds times a turn.
const (
	sweepBatch  = 5000
	sweepRounds = 20
	// blockSlack keeps a ladder's row two minutes past its end, for two
	// processes whose clocks differ (Redis's lkidx keeps the same slack).
	blockSlack = 2 * time.Minute
	// limitSlack keeps a second's bucket a minute (Redis: 2 s), for a
	// process whose clock is behind.
	limitSlack = 60
)

var (
	stSweepLimits    = counterStmts.Add("sweep.limits", `DELETE FROM limit_buckets WHERE sec < ? LIMIT 5000`, false)
	stSweepUsage     = counterStmts.Add("sweep.usage", `DELETE FROM usage_counts WHERE expires_at < ? LIMIT 5000`, false)
	stSweepBlocks    = counterStmts.Add("sweep.blocks", `DELETE FROM blocks WHERE ends_at < ? LIMIT 5000`, false)
	stSweepCampaign  = counterStmts.Add("sweep.campaign", `DELETE FROM campaign_counts WHERE expires_at < ? LIMIT 5000`, false)
	stSweepAbuse     = counterStmts.Add("sweep.abuse_counts", `DELETE FROM abuse_counts WHERE expires_at < ? LIMIT 5000`, false)
	stSweepNumbers   = counterStmts.Add("sweep.abuse_numbers", `DELETE FROM abuse_numbers WHERE expires_at < ? LIMIT 5000`, false)
	stSweepTexts     = counterStmts.Add("sweep.abuse_texts", `DELETE FROM abuse_texts WHERE expires_at < ? LIMIT 5000`, false)
	stSweepAddrEnded = counterStmts.Add("sweep.addresses_ended", `
		SELECT addr_hash FROM block_addresses WHERE until_ms < ? ORDER BY until_ms LIMIT 500 FOR UPDATE`, false)
	stSweepAddrIdents = counterStmts.Add("sweep.addresses_identities", `DELETE FROM block_identities WHERE addr_hash IN (?)`, false)
	stSweepAddrs      = counterStmts.Add("sweep.addresses", `DELETE FROM block_addresses WHERE addr_hash IN (?)`, false)
)

var sweepers sync.Map // *sql.DB → true: one sweeper a pool (a process)

// startCounterSweeper starts this process's turn at the sweep, once a pool.
func startCounterSweeper(db *sql.DB) {
	if _, started := sweepers.LoadOrStore(db, true); started {
		return
	}
	go func() {
		locks := &Locks{db: db}
		// A first turn a little after start, spread so the roles of a
		// gateway started together do not all ask in the same second.
		wait := 10*time.Second + time.Duration(rand.Int63n(int64(20*time.Second)))
		for {
			time.Sleep(wait)
			wait = sweepEvery
			sweepTurn(locks, db)
		}
	}()
}

// sweepTurn sweeps when this process gets the lease.
func sweepTurn(locks *Locks, db *sql.DB) {
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
		{stSweepLimits, sec - limitSlack},
		{stSweepUsage, sec},
		{stSweepBlocks, ms - blockSlack.Milliseconds()},
		{stSweepCampaign, sec},
		{stSweepAbuse, sec},
		{stSweepNumbers, sec},
		{stSweepTexts, sec},
	} {
		for round := 0; round < sweepRounds; round++ {
			res, err := exec(ctx, db, step.s, step.upTo)
			if err != nil {
				return total, err
			}
			n, _ := res.RowsAffected()
			total += n
			if n < sweepBatch {
				break
			}
		}
	}
	for round := 0; round < sweepRounds; round++ {
		n, err := sweepAddresses(ctx, db, ms-blockSlack.Milliseconds())
		total += n
		if err != nil || n == 0 {
			return total, err
		}
	}
	return total, nil
}

// sweepAddresses removes up to 500 ended address sets with their identities,
// in one transaction that locks the address rows first: a failure from one
// of them waits, then finds its set gone and starts a new one.
func sweepAddresses(ctx context.Context, db *sql.DB, upTo int64) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit
	rows, err := tx.QueryContext(ctx, stSweepAddrEnded.SQL, upTo)
	if err != nil {
		return 0, err
	}
	var ended []any
	for rows.Next() {
		var h []byte
		if err := rows.Scan(&h); err != nil {
			rows.Close()
			return 0, err
		}
		ended = append(ended, h)
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(ended) == 0 {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, inList(stSweepAddrIdents.SQL, len(ended)), ended...); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, inList(stSweepAddrs.SQL, len(ended)), ended...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, tx.Commit()
}
