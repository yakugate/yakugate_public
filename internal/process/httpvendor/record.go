package httpvendor

import (
	"context"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/recordq"
)

// Filing what a vendor answered: the message's record, what its receipt will
// need, and the reference it is filed under at its vendor.

// vendorRef is what a sent message is filed under at its vendor: the id the
// vendor gave, or — when an accepted answer carried none (2026-10-12) — our
// own message id. A vendor with no id of its own can only ever report on a
// message by something WE sent it, and our id is the one thing that is ours
// and unique: a send call that passes it (parameter source message_id) and a
// receipt callback or status poll that carries it back are matched through
// this, exactly as a vendor's id is. A vendor that reports nothing loses
// nothing by it.
func vendorRef(vendorID, ourID string) string {
	if vendorID != "" {
		return vendorID
	}
	return ourID
}

func isDup(err error) bool { return err != nil && strings.Contains(err.Error(), "Duplicate entry") }

// recordRetryFor is how long a record write is tried again after the vendor
// has answered.
const recordRetryFor = 20 * time.Second

// recordWrite files a message the vendor has ALREADY answered for.
//
// The message left its queue when it was taken (issue 22), so a write that
// fails is tried again until the given time — one time for the whole answer,
// so a store that is down costs a worker 20 s, not 20 s a message — and only
// then given up: the message was sent and has no record.
//
// With the DB worker and the queues in Redis the record is queued and the
// message taken off the vendor queue (cid) in ONE Redis transaction: acked is
// true and the caller must not ack again.
func (p *Process) recordWrite(ctx context.Context, cid string, m model.Message, until time.Time, answers ...model.SubmitAnswer) (acked bool, err error) {
	wait := 50 * time.Millisecond
	txq, oneStep := p.queue.(recordq.TxAcker)
	oneStep = oneStep && p.recQ != nil
	for {
		var err error
		if p.recQ != nil {
			// Through the record queue: one task, written by the DB worker
			// as WriteBatch writes a bulk call's messages — the record with
			// its status and vendor id in one go, and a message sent a
			// second time gets its new vendor id (no separate status write).
			rec := dal.TerminalRecord{Msg: m, VendorMsgID: m.VendorMsgID, ErrorCode: m.ErrorCode, Answers: answers}
			if m.Status == model.StatusSent {
				rec.AckAt = time.Now()
			}
			if oneStep {
				err = p.recQ.RecordAndAck(ctx, txq, cid, []string{m.ID}, []dal.TerminalRecord{rec}, false)
			} else {
				err = p.recQ.WriteBatch(ctx, []dal.TerminalRecord{rec})
			}
		} else {
			rec := dal.TerminalRecord{Msg: m, VendorMsgID: m.VendorMsgID, ErrorCode: m.ErrorCode, Answers: answers}
			if m.Status == model.StatusSent {
				rec.AckAt = time.Now()
			}
			err = p.hot.WriteBatch(ctx, []dal.TerminalRecord{rec})
		}
		if err == nil || isDup(err) {
			return oneStep, nil
		}
		if ctx.Err() != nil || !time.Now().Before(until) {
			return false, err
		}
		select {
		case <-ctx.Done():
			return false, err
		case <-time.After(wait):
		}
		if wait < 2*time.Second {
			wait *= 2
		}
	}
}

// noteSent files what a receipt for this message will need, so the receipt
// can be matched in Redis (dlr.Service.NoteSent). Nothing is filed unless that
// path is on (definition.go). group says the vendor's id names more than this
// message (dlr.GroupParts, dlr.GroupCall; "" = it alone): with Redis set the
// waiting messages are in Redis and nowhere else (D-136), so a long message
// sent as one call and a bulk call's shared id are filed there too, in the
// id's group, and one receipt finds them all.
func (p *Process) noteSent(ctx context.Context, vendorID int64, vendorMsgID string, m model.Message, group string) {
	// A vendor that sends no receipts has nothing waiting (D-136).
	if p.svc == nil || !p.svc.HasFacts() || vendorMsgID == "" || m.VendorSendsNoReceipts {
		return
	}
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	p.svc.NoteSent(fctx, vendorID, vendorMsgID, dlrsvc.Facts{
		MsgID: m.ID, AccountID: m.AccountID,
		NoReceipt: m.NoReceipt, FailureOnly: m.FailureReceiptOnly, WantIntermediate: m.WantsIntermediateReceipt(),
		BindSlot: m.BindSlot, Privacy: dal.PrivacyOf(m),
		MCC: m.MCC, MNC: m.MNC, Src: m.Sender, Dst: m.Destination,
		Owner: m.QueueOwner, Table: dal.MTLogTableFor(m),
		// Kept for as long as its vendor's wait for a receipt (D-136).
		Wait:  m.ReceiptWait(),
		Group: group,
		// What its receipt may still book (billing.Moments.VendorAnswered).
		Bill: dlrsvc.BillOf(m),
		// A final answer the account's policy gave at submit (stampTold):
		// the vendor's own receipt is then recorded and not sent on.
		Told: string(m.ToldStatus),
	})
}

// mark stamps a message with the vendor that takes it and that vendor's
// receipt settings (D-136): its record and its waiting reference are kept
// for the vendor's wait, or not at all for a vendor that sends no receipts.
func (v vendorAdapter) mark(m *model.Message) {
	m.VendorID = v.id
	m.ReceiptWaitHours, m.VendorSendsNoReceipts = v.receiptWaitHours, v.sendsNoReceipts
}
