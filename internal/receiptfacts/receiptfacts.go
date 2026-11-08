// Package receiptfacts is the Redis side of receipts that are finished
// without the record store (internal/dlr fast.go; Saleem 2026-10-07:
// "receipts should not go to mysql, if they need any data they may use redis
// if it is configured").
//
// Two things live here:
//
//	Store   what a receipt needs to know about its message (dlr.Facts), ONE
//	        key under the vendor's own message id from the moment the vendor
//	        takes the message: SET when it is sent, GETDEL when its final
//	        receipt comes — the one caller GETDEL gives the value to closes
//	        it (store.go; Saleem, 29 Oct 2026).
//
//	Queue   the receipts' database work (dlr.ReceiptWrite) waiting to be
//	        written, as a list. Drain writes it to the record store in
//	        batches. The list is the durability: a receipt whose vendor has
//	        been acknowledged is in Redis until the database has it, so a
//	        database that is slow, or down, delays the record and loses
//	        nothing — and holds nobody up.
//
// Both use the MAIN Redis: they are working data, like the balances and the
// counters, not a queue of messages. What is kept and read is the DAL's
// (dal.ReceiptFacts, dal.ReceiptWrites; internal/dal/redis/receipts.go).
package receiptfacts

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/queuecheck"
)

// ---- the write-behind queue ----

// Settle is how long a receipt's database work waits before it is written:
// long enough for the vendor role's own record of the message — a batch
// flushed every 100 ms — to be there first, so the status has a record to
// land on. Nobody waits for it: the client has its receipt already.
const Settle = 300 * time.Millisecond

// GiveUpAfter is how long a status is owed to a message that has no record
// before it is let go (the receipt's own row is written either way).
// KeepFor is how long a receipt the record store will not take is kept and
// tried again — a database that is down for hours loses nothing; a row it
// refuses for a whole day is counted and let go, so it cannot circle for ever.
const (
	GiveUpAfter = 2 * time.Minute
	KeepFor     = 24 * time.Hour
)

// Batch is how many receipts are written to the record store at a time.
const Batch = 500

// Sink writes a batch to the record store and returns what is still owed —
// rows it could not write, and rows whose message has no record yet.
type Sink func(ctx context.Context, rows []dlr.ReceiptWrite) (owed []dlr.ReceiptWrite, err error)

// Queue is the list of receipts' database work.
type Queue struct {
	l     dal.ReceiptWrites
	owner string // the deployment whose list this is ("" = none named)
	token string
	// Counters for monitoring.
	written, requeued, dropped atomic.Int64
	// moved counts statuses handed to another deployment's list (listOf).
	moved atomic.Int64

	// StillComing, when set, is asked before a status is given up for a
	// message that has no record after GiveUpAfter: true = its record may
	// still be on its way, keep the status. The DB worker sets it — with the
	// records in a queue (recordq), "no record" may only mean the queue is
	// behind, and a status let go then would leave the message `sent` for
	// good. Set before Drain; nil = give up after GiveUpAfter, as before.
	StillComing func(ctx context.Context, w dlr.ReceiptWrite) bool

	// Check is how often the list is checked, busy or idle, and how many rows
	// one check takes (YAKU_QUEUE_RECEIPT_WRITES_*, issue 7; the roles that
	// drain it set it before Drain). Zero: every 50 ms when it was empty,
	// at once after a full batch, as before.
	Check queuecheck.Check
}

// NewQueue builds the queue for one deployment's receipts on its list (the
// list's owner; "" = an install that names no deployment). Every process of
// the deployment adds to the same list; one of them at a time writes it out.
func NewQueue(l dal.ReceiptWrites) *Queue {
	tok := make([]byte, 12)
	_, _ = rand.Read(tok)
	return &Queue{l: l, owner: l.Owner(), token: hex.EncodeToString(tok)}
}

// listOf is the list a status that waits for its message's record waits on:
// the list of the deployment the record is queued on (ReceiptWrite.Owner),
// whose writer knows whether that record is still coming (StillComing reads
// that deployment's record queue). A receipt can come in on any deployment
// the vendor runs on; the record is written where the message was sent.
// Go-live test 18 Oct 2026 (P14 D4, D6): a status waiting on the receiving
// deployment's list was let go after GiveUpAfter while the sending
// deployment's DB worker was behind, and the message stayed `sent`.
//
// It answers the deployment whose list it is.
func (q *Queue) listOf(w dlr.ReceiptWrite) string {
	if w.Owner == "" || w.Owner == q.owner {
		return q.owner
	}
	return w.Owner
}

// Receipt adds one receipt's database work. [dlr.RecordWriter]
func (q *Queue) Receipt(ctx context.Context, w dlr.ReceiptWrite) error {
	b, err := json.Marshal(w)
	if err != nil {
		return err
	}
	return q.l.Push(ctx, []string{string(b)})
}

// Receipts adds many receipts' database work in one round trip.
// [dlr.ManyWrites]
func (q *Queue) Receipts(ctx context.Context, ws []dlr.ReceiptWrite) error {
	if len(ws) == 0 {
		return nil
	}
	vals := make([]string, 0, len(ws))
	for _, w := range ws {
		b, err := json.Marshal(w)
		if err != nil {
			return err
		}
		vals = append(vals, string(b))
	}
	return q.l.Push(ctx, vals)
}

// Later adds receipts whose own row is written already and whose status waits
// for the message's record, to be tried a second from now. It is where the DB
// worker puts a receipt that came off its lane before the record it belongs
// to (recordq.Worker.Later): from here the rules of this list apply — tried
// again every second, given up after GiveUpAfter unless the record is still
// on its way (StillComing).
//
// A status whose record is queued on another deployment waits on THAT
// deployment's list (listOf): its DB worker is the one that can tell whether
// the record is still coming.
func (q *Queue) Later(ctx context.Context, ws []dlr.ReceiptWrite) error {
	if len(ws) == 0 {
		return nil
	}
	after := time.Now().Add(time.Second).UnixMilli()
	byList := map[string][]string{}
	var lists []string
	for _, w := range ws {
		w.After = after
		b, err := json.Marshal(w)
		if err != nil {
			return err
		}
		l := q.listOf(w)
		if _, seen := byList[l]; !seen {
			lists = append(lists, l)
		}
		byList[l] = append(byList[l], string(b))
	}
	return q.l.PushTo(ctx, lists, byList)
}

// Waiting is how many receipts are waiting to be written (monitoring).
func (q *Queue) Waiting(ctx context.Context) int64 {
	n, err := q.l.Len(ctx)
	if err != nil {
		return -1
	}
	return n
}

// Stats are this process's totals: receipts written to the record store,
// statuses put back to wait for their message's record, and statuses let go.
func (q *Queue) Stats() (written, requeued, dropped int64) {
	return q.written.Load(), q.requeued.Load(), q.dropped.Load()
}

// Moved is how many statuses this process handed to another deployment's
// list to wait for their record there (listOf).
func (q *Queue) Moved() int64 { return q.moved.Load() }

// lockFor is how long the writer's turn lasts without being renewed; a batch
// write is given less than that (writeFor), so a turn cannot end under a
// write that is still running.
const (
	lockFor  = 60 * time.Second
	writeFor = 30 * time.Second
)

// The writer's turn (TakeTurn) and the end of a batch (Done: the n rows just
// handled off the head, what is still owed back at the tail, only while the
// turn is still ours, so a writer whose turn ran out cannot cut rows another
// writer has not read) are scripts of the DAL (dal.ReceiptWrites).

// Drain writes the queue to the record store until ctx ends. Safe to run in
// every process of a deployment: one has the turn at a time.
func (q *Queue) Drain(ctx context.Context, sink Sink, logf func(format string, a ...any)) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	var lastSaid time.Time
	say := func(format string, a ...any) {
		if time.Since(lastSaid) >= 10*time.Second {
			lastSaid = time.Now()
			logf(format, a...)
		}
	}
	wait := func(d time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}
	// Leaving, by ANY way out: give the turn up so the next process need not
	// wait for it to run out. The release used to sit after the loop, and the
	// returns inside the loop went past it — which is where a stop nearly
	// always lands (the idle wait) — so every stopped role left the receipts
	// unwritten for lockFor (dry run 2026-10-09).
	defer q.giveUpTurn()
	every, per := q.Check.Every, q.Check.PerInterval
	if every <= 0 {
		every = 50 * time.Millisecond
	}
	if per <= 0 {
		per = math.MaxInt
	}
	for ctx.Err() == nil {
		// One check: batches until it has taken per, or nothing settled is
		// left; then the next check, busy or idle (issue 7).
		failed := false
		for taken := 0; taken < per && ctx.Err() == nil; {
			mine, err := q.l.TakeTurn(ctx, q.token, lockFor)
			if err != nil || mine != 1 {
				failed = true
				break
			}
			ask := min(Batch, per-taken)
			n, err := q.stepMost(ctx, sink, ask)
			if err != nil {
				say("receipts waiting to be written: %v (kept in Redis, tried again)", err)
				failed = true
				break
			}
			taken += n
			if n < ask {
				break
			}
		}
		if failed {
			if !wait(time.Second) {
				return
			}
			continue
		}
		if !wait(every) {
			return
		}
	}
}

// giveUpTurn drops the writer's turn if it is still ours. It has its own
// short deadline: the caller's context is already over when a role stops.
func (q *Queue) giveUpTurn() {
	rel, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = q.l.DropTurn(rel, q.token)
}

// errTurnLost says the writer's turn ended while a batch was being written;
// nothing was removed, and whoever has the turn writes those rows.
var errTurnLost = errors.New("another process took over writing the receipts")

// step writes one batch: the rows at the head that have settled. It returns
// how many rows it took off the head.
func (q *Queue) step(ctx context.Context, sink Sink) (int, error) {
	return q.stepMost(ctx, sink, Batch)
}

// stepMost is step taking at most `most` rows (Batch when 0 or more).
func (q *Queue) stepMost(ctx context.Context, sink Sink, most int) (int, error) {
	if most <= 0 || most > Batch {
		most = Batch
	}
	raw, err := q.l.Head(ctx, most)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	rows := make([]dlr.ReceiptWrite, 0, len(raw))
	taken := 0
	for _, s := range raw {
		var w dlr.ReceiptWrite
		if json.Unmarshal([]byte(s), &w) != nil {
			taken++ // not ours to read: dropped rather than blocking the head for ever
			q.dropped.Add(1)
			continue
		}
		if now.UnixMilli() < readyAt(w) {
			break // the head has not settled yet; what is behind it is younger still
		}
		taken++
		rows = append(rows, w)
	}
	if taken == 0 {
		return 0, nil
	}
	// Asked BEFORE the write, for the receipts old enough to be given up:
	// "is this message's record still on its way?" Asked after, the record
	// could land between the write's look for it and the question — the
	// answer would be "nothing is coming" and the status would be let go for
	// a record that had just arrived.
	var coming map[string]bool
	if q.StillComing != nil {
		for _, w := range rows {
			if time.Duration(now.UnixMilli()-w.At)*time.Millisecond <= GiveUpAfter {
				continue
			}
			if _, asked := coming[w.MsgID]; asked {
				continue
			}
			if coming == nil {
				coming = map[string]bool{}
			}
			coming[w.MsgID] = q.StillComing(ctx, w)
		}
	}
	var owed []dlr.ReceiptWrite
	var sinkErr error
	if len(rows) > 0 {
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeFor)
		owed, sinkErr = sink(wctx, rows)
		cancel()
	}
	// What is still owed goes back to the tail, to be tried a little later; a
	// status owed for longer than GiveUpAfter is let go.
	back := make([]string, 0, len(owed))
	requeued := 0
	var given []dlr.ReceiptWrite   // receipts whose record is not coming, filed as evidence
	moved := map[string][]string{} // statuses that wait on another deployment's list
	var movedTo []string
	for _, w := range owed {
		age := time.Duration(now.UnixMilli()-w.At) * time.Millisecond
		if age > KeepFor {
			q.dropped.Add(1)
			continue
		}
		if l := q.listOf(w); w.Recorded && l != q.owner {
			// Its row is in; its status waits for a record queued on another
			// deployment, on that deployment's list, whose writer knows
			// whether the record is still coming (listOf). Not given up here.
			w.After = now.Add(time.Second).UnixMilli()
			if b, err := json.Marshal(w); err == nil {
				if _, seen := moved[l]; !seen {
					movedTo = append(movedTo, l)
				}
				moved[l] = append(moved[l], string(b))
			}
			continue
		}
		if w.Recorded && age > GiveUpAfter && !coming[w.MsgID] {
			// Its record is not coming: the receipt is kept as evidence,
			// closing nothing (the logs are written once; there is no record
			// to close).
			w.Finalize, w.Recorded = false, false
			given = append(given, w)
			q.dropped.Add(1)
			continue
		}
		w.Tries++
		w.After = now.Add(time.Second).UnixMilli()
		b, err := json.Marshal(w)
		if err != nil {
			continue
		}
		back = append(back, string(b))
		requeued++
	}
	if len(movedTo) > 0 {
		// Before they leave this list: a writer stopped in between leaves a
		// status on both, and writing a status twice changes nothing.
		if err := q.l.PushTo(context.WithoutCancel(ctx), movedTo, moved); err != nil {
			return 0, err
		}
		for _, l := range movedTo {
			q.moved.Add(int64(len(moved[l])))
		}
	}
	ok, err := q.l.Done(context.WithoutCancel(ctx), q.token, taken, back)
	if err != nil {
		return 0, err
	}
	if ok != 1 {
		return 0, errTurnLost
	}
	if len(given) > 0 {
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeFor)
		_, _ = sink(wctx, given) // best effort: they are evidence only
		cancel()
	}
	q.written.Add(int64(len(rows) - len(owed)))
	q.requeued.Add(int64(requeued))
	if sinkErr != nil {
		return taken, sinkErr
	}
	return taken, nil
}

// readyAt is when a row may be written: once it has settled, and not before
// a retry's own wait is over. Never more than a few seconds from now: a row
// stamped by a machine whose clock runs ahead must not hold the head of the
// list for as long as the clocks differ.
func readyAt(w dlr.ReceiptWrite) int64 {
	at := w.At + int64(Settle/time.Millisecond)
	if w.After > at {
		at = w.After
	}
	if limit := time.Now().Add(5 * time.Second).UnixMilli(); at > limit {
		return limit
	}
	return at
}

// ToRecordStore is the Sink that writes batches to the record store.
func ToRecordStore(h dal.ReceiptRecords) Sink {
	return func(ctx context.Context, in []dlr.ReceiptWrite) ([]dlr.ReceiptWrite, error) {
		rows := make([]dal.ReceiptRow, len(in))
		for i, w := range in {
			rows[i] = dal.ReceiptRow{
				MsgID: w.MsgID, AccountID: w.AccountID, VendorID: w.VendorID, VendorMsgID: w.VendorMsgID,
				Table: w.Table, Status: w.Status, Stat: w.Stat, ErrorCode: w.ErrorCode, Raw: w.Raw,
				DoneAt: w.DoneAt, Deployment: w.Deployment, At: time.UnixMilli(w.At), Part: w.Part,
				Finalize: w.Finalize, FinalStatus: w.FinalStatus, RecordedOnly: w.RecordedOnly,
				SellRateID: w.SellRate, BuyRateID: w.BuyRate, Ref: i, Source: w.Source,
			}
		}
		owedRows, err := h.WriteReceiptBatch(ctx, rows)
		owed := make([]dlr.ReceiptWrite, 0, len(owedRows))
		for _, r := range owedRows {
			if r.Ref < 0 || r.Ref >= len(in) {
				continue
			}
			w := in[r.Ref]
			// A closing row the store could not write for want of its
			// record waits for it; any other owed row was refused.
			w.Recorded = r.Recorded
			owed = append(owed, w)
		}
		return owed, err
	}
}
