package mysql

// The client receipt queue: receipts filed for an account, claimed, put
// off, given up and acked.

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queueflow"
)

// EnqueueClientDLR queues a client-facing receipt (smsc leg) for delivery. [SCOPE P5]
func (s *Store) EnqueueClientDLR(ctx context.Context, d model.ClientDLR) error {
	// A lock victim is filed again at once (queuelocks.go).
	start := time.Now()
	_, err := s.writeOne(ctx,
		// A delayed receipt (the account's receipt policy spreading invented
		// answers out) is simply a row nobody can claim yet: created_at moves
		// with next_attempt_at so the stale-claim window and the give-up age
		// both count from when the receipt EXISTS for the client, not from
		// when we decided it.
		// queue_owner (queue/0037, D-121): the deployment that pushes it to
		// the client; NULL = shared.
		// done_at_ms (queue/0042): when the receipt's state was reached.
		stCDLREnqueue.SQL,
		d.AccountID, d.ClientMsgID, d.Status, nsStr(d.Stat), nsStr(d.Receipt), d.BindSlot, nsStr(d.Src), nsStr(d.Dst),
		nsStr(d.Owner), msOf(d.DoneAt), nsStr(cdlrMsgID(d)),
		int(d.Delay.Seconds()), int(d.Delay.Seconds()))
	stCDLREnqueue.Observe(time.Since(start), 1)
	if err == nil {
		queueflow.In(queueflow.DLR, strconv.FormatInt(d.AccountID, 10), 1) // D-127
	}
	return err
}

// EnqueueClientDLRBatch queues many client-facing receipts, in order, as
// multi-row INSERTs of 500 — the rows EnqueueClientDLR writes one at a time.
// An error leaves the chunks already written in place and the rest unwritten.
func (s *Store) EnqueueClientDLRBatch(ctx context.Context, ds []model.ClientDLR) error {
	const row = `(?,?,?,?,?,?,?,?,?,?,?,DATE_ADD(NOW(),INTERVAL ? SECOND),DATE_ADD(NOW(),INTERVAL ? SECOND))`
	for from := 0; from < len(ds); from += 500 {
		chunk := ds[from:min(from+500, len(ds))]
		args := make([]any, 0, len(chunk)*13)
		in := map[int64]int64{}
		for _, d := range chunk {
			args = append(args, d.AccountID, d.ClientMsgID, d.Status, nsStr(d.Stat), nsStr(d.Receipt), d.BindSlot,
				nsStr(d.Src), nsStr(d.Dst), nsStr(d.Owner), msOf(d.DoneAt), nsStr(cdlrMsgID(d)), int(d.Delay.Seconds()), int(d.Delay.Seconds()))
			in[d.AccountID]++
		}
		// One statement a chunk, whole or not at all: a lock victim is filed
		// again at once (queuelocks.go; it was the dlr role's "the queue
		// store refused a client receipt (Deadlock)").
		if _, err := s.writeOne(ctx,
			`INSERT INTO client_dlr (account_id,client_msg_id,status,stat,receipt,bind_slot,src,dst,queue_owner,done_at_ms,msg_id,next_attempt_at,created_at)
			 VALUES `+row+strings.Repeat(","+row, len(chunk)-1), args...); err != nil {
			return err
		}
		for acct, n := range in {
			queueflow.In(queueflow.DLR, strconv.FormatInt(acct, 10), n) // D-127
		}
	}
	return nil
}

// ClientDLRsQueued says, for each receipt, whether the queue holds it already
// (same account, client message id, status and word): an insert the
// connection dropped under may have been written (dlr.QueuedChecker; go-live
// test 18 Oct 2026, P7 F4). A receipt given up still counts: it was filed.
func (s *Store) ClientDLRsQueued(ctx context.Context, ds []model.ClientDLR) ([]bool, error) {
	out := make([]bool, len(ds))
	key := func(acct int64, id, status, stat string) string {
		return strconv.FormatInt(acct, 10) + "\x00" + id + "\x00" + status + "\x00" + stat
	}
	for from := 0; from < len(ds); from += 500 {
		chunk := ds[from:min(from+500, len(ds))]
		// By account first (idx_acct): client_msg_id has no key of its own,
		// and this runs only after the store refused a receipt.
		accts := map[int64]bool{}
		var acctArgs, idArgs []any
		for _, d := range chunk {
			if !accts[d.AccountID] {
				accts[d.AccountID] = true
				acctArgs = append(acctArgs, d.AccountID)
			}
			idArgs = append(idArgs, d.ClientMsgID)
		}
		rows, err := s.db.QueryContext(ctx,
			`SELECT account_id, client_msg_id, status, COALESCE(stat,'') FROM client_dlr WHERE account_id IN (`+placeholders(len(acctArgs))+
				`) AND client_msg_id IN (`+placeholders(len(idArgs))+`)`, append(acctArgs, idArgs...)...)
		if err != nil {
			return nil, err
		}
		have := map[string]bool{}
		for rows.Next() {
			var acct int64
			var id, status, stat string
			if err := rows.Scan(&acct, &id, &status, &stat); err != nil {
				rows.Close()
				return nil, err
			}
			have[key(acct, id, status, stat)] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		for i, d := range chunk {
			out[from+i] = have[key(d.AccountID, d.ClientMsgID, d.Status, d.Stat)]
		}
	}
	return out, nil
}

// ClaimClientDLR leases up to n pending client-DLR jobs for the given accounts
// (the caller holds those binds), bumping next_attempt_at for a 60s lease. [SCOPE P5]
func (s *Store) ClaimClientDLR(ctx context.Context, claims []model.DLRClaim, n int) ([]model.ClientDLR, error) {
	return s.claimClientDLR(ctx, claims, 0, n)
}

// ClaimClientDLRStale leases jobs older than minAgeSec for the given accounts —
// used by the HTTP dlr_url poster so a live SMPP bind gets first crack. [SCOPE P5]
func (s *Store) ClaimClientDLRStale(ctx context.Context, claims []model.DLRClaim, minAgeSec, n int) ([]model.ClientDLR, error) {
	return s.claimClientDLR(ctx, claims, minAgeSec, n)
}

// dlrClaimScope renders the account+attempt-window half of a client_dlr claim
// (P5-B03), and of an mo_queue claim since config/0146 (both tables have
// account_id and attempts). Windows are grouped so accounts with the same
// bounds share one IN list rather than producing one OR term each.
func dlrClaimScope(claims []model.DLRClaim) (string, []any) {
	type bounds struct{ min, max int }
	order := []bounds{}
	byBounds := map[bounds][]int64{}
	for _, c := range claims {
		b := bounds{c.MinAttempts, c.MaxAttempts}
		if _, seen := byBounds[b]; !seen {
			order = append(order, b)
		}
		byBounds[b] = append(byBounds[b], c.AccountID)
	}
	terms := make([]string, 0, len(order))
	args := make([]any, 0, len(claims)+2*len(order))
	for _, b := range order {
		ids := byBounds[b]
		t := fmt.Sprintf("account_id IN (%s)", placeholders(len(ids)))
		for _, id := range ids {
			args = append(args, id)
		}
		if b.min > 0 {
			// Not a plain floor: attempts=0 stays claimable. Under 'both' the
			// poster asks for min=dlr_bind_attempts, but an account with NO
			// live bind never has its attempts incremented at all — a plain
			// `attempts >= min` would starve its receipts forever (caught in
			// the 2026-08-31 e2e run, hours after this shipped). A stale row
			// at 0 means nobody is claiming it, which is exactly the fallback
			// case; 1..min-1 is a live bind actively retrying, and stays its.
			t += " AND (attempts = 0 OR attempts >= ?)"
			args = append(args, b.min)
		}
		if b.max > 0 {
			t += " AND attempts < ?"
			args = append(args, b.max)
		}
		terms = append(terms, "("+t+")")
	}
	return "(" + strings.Join(terms, " OR ") + ")", args
}

func (s *Store) claimClientDLR(ctx context.Context, claims []model.DLRClaim, minAgeSec, n int) ([]model.ClientDLR, error) {
	if len(claims) == 0 {
		return nil, nil
	}
	age := ""
	if minAgeSec > 0 {
		age = fmt.Sprintf(" AND created_at < DATE_SUB(NOW(), INTERVAL %d SECOND)", minAgeSec)
	}
	// Accounts sharing an attempt window share one IN list, so the usual case
	// (every account on the same default) is still a single-term clause.
	scope, args := dlrClaimScope(claims)
	// given_up_at excludes rows the retry policy has already abandoned: they
	// are kept as the record of what a customer lost, never retried again.
	own, ownArgs := s.ownerCond() // my receipts and the shared ones (D-121)
	// By idx_cdlr_claim (queue/0047, stCDLRClaim): one account's rows in id
	// order, read until the page is full — not every row of the account
	// (those out on a lease, those given up) sorted by id first.
	q := fmt.Sprintf(cdlrClaimHead+`
		WHERE %s AND given_up_at IS NULL
		  AND (next_attempt_at IS NULL OR next_attempt_at<=NOW())%s%s
		ORDER BY id LIMIT ?`, scope, age, own)
	args = append(append(args, ownArgs...), n)
	// Read and leased in ONE transaction, the rows locked as they are read
	// (FOR UPDATE SKIP LOCKED), as the vendor queues' claim is. It was a read
	// and then a lease: two dlr roles of one deployment read the same due
	// receipts in the same moment and each pushed them (go-live test 18 Oct
	// 2026, P5 A2: 52 of 1,500 finals twice with MySQL queues).
	// READ COMMITTED (queuelocks.go): under REPEATABLE READ this claim held
	// the gap at the end of client_dlr, and every receipt INSERT deadlocked
	// with its lease UPDATE.
	mins := webhookFloors(claims)
	var out []model.ClientDLR
	err := s.queueTx(ctx, func(tx *sql.Tx) error {
		out = nil
		start := time.Now()
		rows, err := tx.QueryContext(ctx, q+" FOR UPDATE SKIP LOCKED", args...)
		if err != nil {
			return err
		}
		defer func() { stCDLRClaim.Observe(time.Since(start), int64(len(out))) }()
		var ids []any
		var accts []int64
		for rows.Next() {
			var c model.ClientDLR
			var doneMS int64
			if err := rows.Scan(&c.ID, &c.AccountID, &c.ClientMsgID, &c.Status, &c.Stat, &c.Receipt,
				&c.Attempts, &c.CreatedAt, &c.BindSlot, &c.Src, &c.Dst, &c.Owner, &doneMS, &c.MsgID); err != nil {
				rows.Close()
				return err
			}
			c.DoneAt = timeOfMS(doneMS)
			// The try this claim is (D-140, 5 Nov 2026): the count the lease
			// below stores — one more, raised to the account's floor — as Redis
			// answers it. It answered the count before the claim, so a limit of
			// N meant N+1 tries here and N on Redis.
			c.Attempts = max(c.Attempts+1, mins[c.AccountID])
			out = append(out, c)
			ids = append(ids, c.ID)
			accts = append(accts, c.AccountID)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		return leaseClaimedIn(ctx, tx, stCDLRLease, "client_dlr", "id", ids, accts, mins)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// webhookFloors is, per account, the attempt count a webhook's claim raises
// what it takes to: the bind's budget under 'both' (MinAttempts), so what the
// webhook took stays the webhook's (2026-10-15). Its own claim counts an
// attempt, and an item left at 1..N-1 sat in the bind's window: with no bind
// up nobody tried it again until the 24 h give-up, after ONE failed push.
func webhookFloors(claims []model.DLRClaim) map[int64]int {
	out := map[int64]int{}
	for _, c := range claims {
		if c.MinAttempts > 0 {
			out[c.AccountID] = c.MinAttempts
		}
	}
	return out
}

// leaseClaimedIn leases what a claim took for 60 s and counts the attempt —
// raised to the account's webhook floor when it has one (webhookFloors) — on
// x, the claim's own transaction, and gives the first error. One UPDATE per
// floor; nearly always one.
func leaseClaimedIn(ctx context.Context, x dbtx, st *stmt.Stmt, table, idCol string, ids []any, accts []int64, floors map[int64]int) error {
	if len(ids) == 0 {
		return nil // nothing taken: no statement
	}
	byFloor := map[int][]any{}
	for i, id := range ids {
		f := floors[accts[i]]
		byFloor[f] = append(byFloor[f], id)
	}
	var first error
	for f, group := range byFloor {
		set, args := "attempts=attempts+1", group
		if f > 0 {
			set, args = "attempts=GREATEST(attempts+1, ?)", append([]any{f}, group...)
		}
		start := time.Now()
		if _, err := x.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET %s, next_attempt_at=DATE_ADD(NOW(),INTERVAL 60 SECOND) WHERE %s IN (%s)`,
			table, set, idCol, placeholders(len(group))), args...); err != nil && first == nil {
			first = err
		}
		st.Observe(time.Since(start), int64(len(group)))
	}
	return first
}

// RescheduleClientDLR pushes one receipt's next attempt out by the retry
// policy's delay (internal/pushretry) after a failed delivery. Without this the
// only scheduling was the claim's flat 60s lease, which is why an unreachable
// endpoint was hammered once a minute for ever. [P5-B02]
func (s *Store) RescheduleClientDLR(ctx context.Context, id int64, delay time.Duration) error {
	_, err := s.writeOne(ctx, // one row by its primary key (queuelocks.go)
		"UPDATE client_dlr SET next_attempt_at=DATE_ADD(NOW(), INTERVAL ? SECOND) WHERE id=?",
		int(delay.Seconds()), id)
	return err
}

// GiveUpClientDLR marks one receipt abandoned. The row is KEPT (PO ruling
// 2026-08-31): it is the only record that this customer lost this receipt, it
// can be replayed later, and retention cleans it up with everything else. A
// given-up row is never claimed again. [P5-B02]
func (s *Store) GiveUpClientDLR(ctx context.Context, id int64) error {
	_, err := s.writeOne(ctx, "UPDATE client_dlr SET given_up_at=NOW() WHERE id=? AND given_up_at IS NULL", id) // one row by its key
	return err
}

// SweepGiveUpClientDLR abandons receipts nobody is even trying — an account
// with no dlr_url and no live bind is never claimed, so its rows would age
// forever without this backstop and the time limit would never fire. Uses the
// GLOBAL policy window: there is no endpoint here whose override could apply.
// Returns how many were abandoned. [P5-B02]
func (s *Store) SweepGiveUpClientDLR(ctx context.Context, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return 0, nil // no time limit configured: nothing to sweep
	}
	// Mine and the shared ones (D-121): another deployment gives up on its
	// own receipts, when it is back.
	own, ownArgs := s.ownerCond()
	res, err := s.execQueue(ctx, // READ COMMITTED: a range UPDATE locks no gap (queuelocks.go)
		"UPDATE client_dlr SET given_up_at=NOW() WHERE given_up_at IS NULL AND created_at < DATE_SUB(NOW(), INTERVAL ? SECOND)"+own,
		append([]any{int(olderThan.Seconds())}, ownArgs...)...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// AckClientDLR removes delivered client-DLR jobs. [SCOPE P5]
func (s *Store) AckClientDLR(ctx context.Context, ids ...int64) error {
	if len(ids) == 0 {
		return nil
	}
	del := fmt.Sprintf(`DELETE FROM client_dlr WHERE id IN (%s)`, placeholders(len(ids)))
	args := make([]any, len(ids))
	for i, v := range ids {
		args[i] = v
	}
	// One receipt (the dlr role acks each push) is one row by its primary
	// key, on its own; a round of them (the smpp-server) at READ COMMITTED,
	// where a list may be read as a scan (queuelocks.go). A lock victim is
	// made again either way.
	start := time.Now()
	defer func() { stCDLRAck.Observe(time.Since(start), int64(len(ids))) }()
	if len(ids) == 1 {
		_, err := s.writeOne(ctx, del, args...)
		return err
	}
	_, err := s.execQueue(ctx, del, args...)
	return err
}

// cdlrMsgID is the msg_id a receipt row keeps (queue/0050): the message it is
// about, only when the client was told another id (a part's).
func cdlrMsgID(d model.ClientDLR) string {
	if d.MsgID == d.ClientMsgID {
		return ""
	}
	return d.MsgID
}
