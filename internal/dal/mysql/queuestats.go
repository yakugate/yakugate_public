package mysql

// How much waits in the queues: the depth of each queue table and the
// pending counts.

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/store"
)

// listQueueTables returns the live per-vendor queue tables (excluding the template).
func (s *Store) listQueueTables(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SHOW TABLES LIKE 'queue\_%'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		if t != "queue_template" {
			out = append(out, t)
		}
	}
	return out, rows.Err()
}

// QueueDepths returns every vendor queue's live pending count + oldest
// pending age — [SCOPE MONITOR, PO 2026-07-27].
//
// It must stay cheap and must never turn "could not count" into 0. Under a
// 2026-10-05 production load test a blackhole queue of 100k+ read as 0 while
// it was still sending: the old single query read every row (COUNT, SUMs and a
// MIN over 150k rows took 0.7 s on an idle box, far more on a loaded one), all
// queues shared one 3 s budget one after another, and a queue whose count ran
// out of time was simply left out of the report — every screen showed it as
// empty. Now:
//   - the counts come from the claim index alone (idx_claim /
//     idx_owner_claim cover status and next_attempt_at), asked once per owner
//     value (this deployment's rows, then the shared ones) so MySQL reads a
//     range of the index, about 6x faster;
//   - the oldest ready message is two lookups at the edge of that index;
//   - the "tried" count needs the rows, so it is read only while a queue
//     holds at most triedReadMax messages, else its last value is kept;
//   - queues are read side by side, each with its own time limit, and one
//     that cannot be read reports its last good figures marked Stale.
func (s *Store) QueueDepths(ctx context.Context) (map[string]store.VendorQueueStat, error) {
	tables, err := s.listQueueTables(ctx)
	if err != nil {
		return nil, err
	}
	type result struct {
		vendor string
		stat   store.VendorQueueStat
		err    error
	}
	results := make(chan result, len(tables))
	sem := make(chan struct{}, 4)
	for _, t := range tables {
		t := t
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			c, cancel := context.WithTimeout(ctx, queueDepthReadTimeout)
			defer cancel()
			st, err := s.queueStat(c, t)
			results <- result{strings.TrimPrefix(t, "queue_"), st, err}
		}()
	}
	out := make(map[string]store.VendorQueueStat, len(tables))
	s.depthMu.Lock()
	defer s.depthMu.Unlock()
	if s.depthLast == nil {
		s.depthLast = map[string]store.VendorQueueStat{}
	}
	for range tables {
		r := <-results
		last, known := s.depthLast[r.vendor]
		if r.err != nil {
			if known {
				last.Stale = true
				out[r.vendor] = last
			}
			continue
		}
		if r.stat.Tried < 0 { // not counted this time (a large queue)
			r.stat.Tried = 0
			if known {
				r.stat.Tried = last.Tried
			}
		}
		s.depthLast[r.vendor] = r.stat
		out[r.vendor] = r.stat
	}
	return out, nil
}

// queueDepthReadTimeout bounds one queue's reading; the monitor asks for all
// queues within about three seconds and they are read side by side.
const queueDepthReadTimeout = 2500 * time.Millisecond

// triedReadMax is the largest queue whose "tried" count is read every time:
// that count needs every row, the others only the index.
const triedReadMax = 20000

// PendingCounts reports how many client_dlr / mo_queue rows are staged
// waiting for a client push, plus each one's oldest pending age — [SCOPE
// MONITOR, PO 2026-07-27]. Every client_dlr row is "pending" by construction
// (it's deleted by AckClientDLR once the client confirms); mo_queue uses the
// same status='queued' convention as the per-vendor MT queues.
func (s *Store) PendingCounts(ctx context.Context) (dlr, mo store.VendorQueueStat, err error) {
	// Ages computed by MySQL on its own clock — see QueueDepths for why
	// time.Since() on these columns was hours off.
	var d, m, dAge, mAge sql.NullInt64
	own, ownArgs := s.ownerCond() // this deployment's (D-121)
	if err = s.db.QueryRowContext(ctx,
		`SELECT COUNT(*), TIMESTAMPDIFF(SECOND, MIN(created_at), NOW()) FROM client_dlr WHERE 1=1`+own, ownArgs...).Scan(&d, &dAge); err != nil {
		return dlr, mo, err
	}
	if err = s.db.QueryRowContext(ctx,
		`SELECT COUNT(*), TIMESTAMPDIFF(SECOND, MIN(received_at), NOW()) FROM mo_queue WHERE status='queued'`+own, ownArgs...).Scan(&m, &mAge); err != nil {
		return dlr, mo, err
	}
	dlr = store.VendorQueueStat{Depth: d.Int64}
	if dAge.Valid && dAge.Int64 > 0 {
		dlr.OldestAgeSec = dAge.Int64
	}
	mo = store.VendorQueueStat{Depth: m.Int64}
	if mAge.Valid && mAge.Int64 > 0 {
		mo.OldestAgeSec = mAge.Int64
	}
	return dlr, mo, nil
}

// PendingByAccount is PendingCounts per account (store.PendingByAccountStats,
// D-127): the accounts with receipts or MO waiting, largest first. Ready rows
// can be pushed now; held ones are being pushed, wait to be tried again, or
// are receipts held back on purpose (a receipt policy's delay). Age is since
// the oldest row was made, as PendingCounts counts it.
func (s *Store) PendingByAccount(ctx context.Context, kind string, limit int) (map[int64]store.VendorQueueStat, error) {
	if limit <= 0 {
		limit = 200
	}
	own, ownArgs := s.ownerCond() // this deployment's and the shared ones (D-121)
	read := func(q string) (map[int64]store.VendorQueueStat, error) {
		rows, err := s.db.QueryContext(ctx, q, append(ownArgs, limit)...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := map[int64]store.VendorQueueStat{}
		for rows.Next() {
			var acct int64
			var depth, ready, tried, age sql.NullInt64
			if err := rows.Scan(&acct, &depth, &ready, &tried, &age); err != nil {
				return nil, err
			}
			st := store.VendorQueueStat{Depth: depth.Int64, Ready: ready.Int64, Held: depth.Int64 - ready.Int64, Tried: tried.Int64}
			if age.Valid && age.Int64 > 0 {
				st.OldestAgeSec = age.Int64
			}
			out[acct] = st
		}
		return out, rows.Err()
	}
	if kind == "mo" {
		return read(`SELECT account_id, COUNT(*), SUM(next_attempt_at IS NULL OR next_attempt_at<=NOW()), SUM(attempts>0),
				TIMESTAMPDIFF(SECOND, MIN(received_at), NOW())
			  FROM mo_queue WHERE status='queued'` + own + ` GROUP BY account_id ORDER BY COUNT(*) DESC LIMIT ?`)
	}
	return read(`SELECT account_id, COUNT(*), SUM(next_attempt_at IS NULL OR next_attempt_at<=NOW()), SUM(attempts>0),
			TIMESTAMPDIFF(SECOND, MIN(created_at), NOW())
		  FROM client_dlr WHERE 1=1` + own + ` GROUP BY account_id ORDER BY COUNT(*) DESC LIMIT ?`)
}
