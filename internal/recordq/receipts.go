package recordq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	redisstore "github.com/yakugate/yakugate/internal/dal/redis"
	"github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
)

// A vendor's receipt on the record queue.
//
// Saleem, 2026-10-13: "per company:vendor_message_id key and message against
// them in main redis if redis is there, else match in db; once matched,
// checked, send to customer and db insertion queue, in both queues at the
// time; make things simple." (The key is the VENDOR and its message id: one
// company may hold several vendors, and their ids can clash.)
//
// The match and the check are internal/dlr's (fast.go). What is here is the
// last step: the receipt's database work (dlr.ReceiptWrite — its own row, and
// its message's final status) is one more task on the message's lane, and it
// is queued in the SAME Redis transaction as the client's receipt
// (ReceiptQueue). The DB worker writes it with the other records.
//
// A receipt's task is written AFTER every record task of its batch, whatever
// their order in the lane: a vendor role files what a receipt needs the moment
// the vendor answers and queues the message's record a batch later, so a fast
// receipt's task can be ahead of its own record's. When the record is not
// there even then, the receipt's row is written and its status waits for the
// record on the receipts' own list (Worker.Later).

// Receipt queues a matched receipt's database work on its message's lane.
// [dlr.RecordWriter]
//
// It is not held back when the queue is at its limit: the limit stops the
// vendor roles sending more, and a receipt is the end of a message that was
// sent already — refused, its message would stay `sent` for good.
func (c *Client) Receipt(ctx context.Context, w dlr.ReceiptWrite) error {
	t, err := task(KindReceipt, w.MsgID, w)
	if err != nil {
		return err
	}
	return c.putNow(ctx, []Task{t})
}

// Receipts queues many receipts' database work in one round trip, each on
// its own message's lane. [dlr.ManyWrites]
func (c *Client) Receipts(ctx context.Context, ws []dlr.ReceiptWrite) error {
	tasks := make([]Task, 0, len(ws))
	for _, w := range ws {
		t, err := task(KindReceipt, w.MsgID, w)
		if err != nil {
			return err
		}
		tasks = append(tasks, t)
	}
	return c.putNow(ctx, tasks)
}

// ClientReceiptTx is a queue store that files client receipts in a
// transaction which also queues record tasks (the Redis store's
// EnqueueClientDLRsWithRecords).
type ClientReceiptTx interface {
	EnqueueClientDLRsWithRecords(ctx context.Context, ds []model.ClientDLR, p dal.RecordPut) error
}

// ReceiptQueue is a deployment's client receipt queue, able to take a vendor
// receipt's database work in the same step (dlr.WriteEnqueuer). It is the
// Redis queue store with that one method added, so a dlr.QueuePusher made on
// it files both outputs of a receipt together.
//
// It needs the client receipts in the same Redis as the record queue, which
// they are whenever the queues are in Redis (both use the queue Redis).
type ReceiptQueue struct {
	*redisstore.Store
	c *Client
}

// WithReceipts gives the queue store the record queue to file receipts' work
// on.
func WithReceipts(q *redisstore.Store, c *Client) *ReceiptQueue {
	return &ReceiptQueue{Store: q, c: c}
}

// EnqueueClientDLRsAndWrites queues the client's receipts and the receipts'
// database work in ONE Redis transaction: both are queued, or neither is.
// [dlr.WriteEnqueuer]
func (r *ReceiptQueue) EnqueueClientDLRsAndWrites(ctx context.Context, ds []model.ClientDLR, ws []dlr.ReceiptWrite) error {
	tasks := make([]Task, 0, len(ws))
	for _, w := range ws {
		t, err := task(KindReceipt, w.MsgID, w)
		if err != nil {
			return err
		}
		tasks = append(tasks, t)
	}
	put, done, err := r.c.stage(ctx, tasks)
	if err != nil {
		return err
	}
	if err := r.Store.EnqueueClientDLRsWithRecords(ctx, ds, put); err != nil {
		return fmt.Errorf("queue %d client receipt(s) with their record work: %w", len(ds), err)
	}
	done()
	return nil
}

// ---- the DB worker's side ----

// receiptBatcher is the Sink capability a worker needs to write receipts
// (dal.ReceiptRecords.WriteReceiptBatch): as much of a batch as it can, and what is
// still owed.
type receiptBatcher interface {
	WriteReceiptBatch(ctx context.Context, rows []dal.ReceiptRow) (owed []dal.ReceiptRow, err error)
}

// receiptOnce is the same for a batch that may be a repeat: a receipt row
// that is there already is not written again.
type receiptOnce interface {
	WriteReceiptBatchOnce(ctx context.Context, rows []dal.ReceiptRow) (owed []dal.ReceiptRow, err error)
}

func receiptRow(w dlr.ReceiptWrite, ref int) dal.ReceiptRow {
	var at time.Time // zero = now, as the store has it
	if w.At > 0 {
		at = time.UnixMilli(w.At)
	}
	return dal.ReceiptRow{
		MsgID: w.MsgID, AccountID: w.AccountID, VendorID: w.VendorID, VendorMsgID: w.VendorMsgID,
		Table: w.Table, Status: w.Status, Stat: w.Stat, ErrorCode: w.ErrorCode, Raw: w.Raw,
		DoneAt: w.DoneAt, Deployment: w.Deployment, At: at,
		Finalize: w.Finalize, FinalStatus: w.FinalStatus, RecordedOnly: w.RecordedOnly, Part: w.Part,
		SellRateID: w.SellRate, BuyRateID: w.BuyRate, Ref: ref, Source: w.Source,
	}
}

// writeReceipts writes the receipt tasks at positions idx of tasks: one
// INSERT per receipt table, one UPDATE per final status.
//
//   - MySQL away: every one of them fails with that error, and the batch
//     stays where it is (apply's away);
//   - a row MySQL refuses: found one at a time, and parked like any refused
//     task;
//   - a message with no record yet: the receipt's row is written and its
//     status goes to later, to wait for the record. That is not a failure.
//
// It returns how many were written.
func (w *Worker) writeReceipts(ctx context.Context, tasks []Task, idx []int, repeat bool, fail func(int, error)) (wrote int) {
	rb, ok := w.sink.(receiptBatcher)
	if !ok {
		for _, i := range idx {
			fail(i, &badTask{"this record store cannot write a receipt task"})
		}
		return 0
	}
	write := rb.WriteReceiptBatch
	if repeat {
		if ro, ok := w.sink.(receiptOnce); ok {
			write = ro.WriteReceiptBatchOnce
		}
	}
	rows := make([]dal.ReceiptRow, 0, len(idx))
	writes := make([]dlr.ReceiptWrite, 0, len(idx))
	at := make([]int, 0, len(idx)) // rows[k] is tasks[at[k]]
	for _, i := range idx {
		var rw dlr.ReceiptWrite
		if err := json.Unmarshal(tasks[i].D, &rw); err != nil || rw.MsgID == "" {
			fail(i, &badTask{fmt.Sprintf("receipt task of %s cannot be read: %v", tasks[i].ID, err)})
			continue
		}
		rows = append(rows, receiptRow(rw, len(rows)))
		writes = append(writes, rw)
		at = append(at, i)
	}
	if len(rows) == 0 {
		return 0
	}
	owed, err := write(ctx, rows)
	if err != nil && isAway(err) {
		for _, i := range at {
			fail(i, err)
		}
		return 0
	}
	refused := map[int]error{} // by position in rows
	var later []dlr.ReceiptWrite
	settle := func(o dal.ReceiptRow, oerr error) (away error) {
		k := o.Ref
		if k < 0 || k >= len(rows) {
			return nil
		}
		if o.Recorded {
			// Its row is in; the status is owed until the record is there.
			lw := writes[k]
			lw.Recorded = true
			later = append(later, lw)
			return nil
		}
		// Its row is not written. With others in the statement nothing says
		// which one MySQL would not take: this one alone does.
		if oerr == nil || len(rows) > 1 {
			one := rows[k]
			var again []dal.ReceiptRow
			again, oerr = write(ctx, []dal.ReceiptRow{one})
			if oerr != nil && isAway(oerr) {
				return oerr
			}
			if oerr == nil {
				for _, a := range again {
					if a.Recorded {
						lw := writes[k]
						lw.Recorded = true
						later = append(later, lw)
					}
				}
				return nil
			}
		}
		refused[k] = oerr
		return nil
	}
	for _, o := range owed {
		if away := settle(o, err); away != nil {
			for _, i := range at {
				fail(i, away)
			}
			return 0
		}
	}
	if len(later) > 0 {
		if lerr := w.later(ctx, later); lerr != nil {
			// The list they wait on is away. Nothing is taken off the lane:
			// the batch is met again as a repeat, where a row that is there
			// is not written twice.
			for _, i := range at {
				fail(i, fmt.Errorf("receipts waiting for their record could not be kept: %w", lerr))
			}
			return 0
		}
	}
	for k, i := range at {
		if rerr, bad := refused[k]; bad {
			fail(i, rerr)
			continue
		}
		wrote++
	}
	return wrote
}

// later hands receipts whose message has no record yet to the list they wait
// on (Worker.Later).
func (w *Worker) later(ctx context.Context, ws []dlr.ReceiptWrite) error {
	if w.Later == nil {
		// No list to wait on: the receipt's row is written, the status is let
		// go. A DB worker role always sets one (dbworker.New).
		w.owedC.Add(int64(len(ws)))
		w.dropped.Add(int64(len(ws)))
		w.logf("%d receipt(s) came before their message's record and this worker has nowhere to keep them: their rows are written, the statuses are let go (first: %s)", len(ws), ws[0].MsgID)
		return nil
	}
	if err := w.Later(ctx, ws); err != nil {
		return err
	}
	w.owedC.Add(int64(len(ws)))
	return nil
}
