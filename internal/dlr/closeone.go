package dlr

import (
	"context"
	"log"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/vendorquality"
)

// Closing one message on a final receipt: its row, its record's close, the
// client's receipt and what it books.

// RecordChecker is an optional Store capability: a message's record and its
// status, read before a receipt matched in Redis changes anything — the
// record may not be written yet (mysql.Store.RecordState).
type RecordChecker interface {
	RecordState(ctx context.Context, ref model.RecordRef) (status string, found bool, err error)
}

// ReceiptCloser is an optional Store capability: a final receipt's row and
// its message's close in ONE transaction, so a store that goes away between
// them cannot leave one without the other, or — tried again — the row twice
// (mysql.Store.CloseWithReceipt). mayClose=false records it only; a message
// final already is recorded only too, and its row is not charged.
type ReceiptCloser interface {
	CloseWithReceipt(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode, entryJSON string, mayClose bool) (mine bool, err error)
}

// closeJob is what a final receipt does to one message on the record
// store's path, and how far it got: done again from where it stopped when
// the store went away part way (hold.go).
type closeJob struct {
	ref                    model.RecordRef
	accountID, company     int64
	sender, vendorID       int64
	vendorMsgID, errorCode string
	st                     model.Status
	stat                   Stat
	entry                  string
	told                   string
	invented               *receiptpolicy.Outcome
	noReceipt, failureOnly bool
	bindSlot               int
	mcc, mnc               string
	doneAt                 time.Time
	// ready: false = a part of a long message that does not close it (its
	// row only).
	ready bool
	// partPush: that part's own final (partSt) goes to the client as it
	// comes (part_receipts = 'each'); partPushed once it went.
	partPush, partPushed bool
	partSt               model.Status
	// note is what the message's receipt may book; "" = read it from the
	// store.
	note string
	// What is done: the row (when written apart from the close), the close.
	rowDone, closed bool
	// What its final books (billing.go), worked out once before the close;
	// booked once the close is this receipt's.
	bs             []billing.Booking
	billed, booked bool
}

// closeOne files a final receipt on its message's record, closes the message
// when this receipt is the one that does, and tells the client. Each step is
// noted on j, so calling it again after an error does what is left.
func (s *Service) closeOne(ctx context.Context, j *closeJob) error {
	if !j.ready {
		// Group incomplete: the part's row, and the message's status stays.
		if !j.rowDone {
			if err := s.appendRaw(ctx, j.ref, j.entry); err != nil {
				return err
			}
			j.rowDone = true
		}
		if j.partPush && !j.partPushed {
			part := *j
			part.st = j.partSt
			if err := s.pusher.PushToClient(ctx, j.accountID, s.clientReceipt(ctx, &part)); err != nil {
				return err
			}
			j.partPushed = true
		}
		return nil
	}
	s.billJob(ctx, j)
	if !j.closed {
		mine, err := s.closeRecord(ctx, j)
		if err != nil {
			return err
		}
		if !s.closedBy(ctx, j, mine) {
			return nil
		}
	}
	// smsc leg — hand the receipt off for delivery back to the client (best
	// effort). Each part keeps its own client-facing id, so the client gets a
	// receipt per part exactly as it submitted them.
	//
	// The client's registered_delivery preference gates the PUSH only — the
	// status update, the audit row and the concat fold above all still
	// happen unconditionally, because WE still need delivery truth for retry
	// logic and reporting regardless of what the client asked to be told.
	// [PO 2026-09-03]
	//
	// And the client's own receipt policy gates it as well (PO 2026-09-22): a
	// client already given a final answer is not given a second,
	// contradicting one, and a client whose policy replaces this status is
	// sent the replacement instead of the vendor's word.
	if j.told != "" {
		logging.New("dlr").Info(logging.SystemScope, "dlr_not_pushed",
			"the client was already told about this message, so the vendor's receipt is recorded only",
			logging.Fields{"messageId": j.ref.MsgID, "toldStatus": j.told,
				"vendorStatus": string(j.st), "errorCode": j.errorCode})
		return nil
	}
	if j.invented != nil {
		src, dst := s.addresses(ctx, j.ref)
		s.Tell(ctx, Told{MsgID: j.ref.MsgID, AccountID: j.accountID, CompanyID: j.company, Src: src, Dst: dst, BindSlot: j.bindSlot,
			NoReceipt: j.noReceipt, FailureReceiptOnly: j.failureOnly}, *j.invented)
		return nil
	}
	if s.pusher != nil && model.WantsTerminalReceipt(j.noReceipt, j.failureOnly, j.st) {
		// bindSlot routes it back to the bind the client submitted on (PO
		// 2026-09-05) — for a concat group, the one bind all its parts came
		// from. The client gets OUR receipt, rendered from the facts; the
		// vendor's raw text stays in the record's raw_dlr for audit and never
		// reaches the customer (PO 2026-09-11).
		return s.pusher.PushToClient(ctx, j.accountID, s.clientReceipt(ctx, j))
	}
	return nil
}

// closedBy is what follows a final receipt's close: mine = this receipt
// closed the message. It books what the receipt decided, and gives the
// message its verdict and folds its parts away; false = final already (the
// receipt is recorded only, nothing more is done with it).
func (s *Service) closedBy(ctx context.Context, j *closeJob, mine bool) bool {
	if mine && !j.booked {
		j.booked = true
		s.book(ctx, j.company, j.bs)
	}
	if !mine {
		// Final already: another receipt closed it a moment ago. This
		// one is kept (its row) and changes nothing — no second verdict,
		// no second fold, and above all no second receipt to the client.
		logging.New("dlr").Info(logging.SystemScope, "dlr_late",
			"the message was already final, so this receipt is recorded only",
			logging.Fields{"messageId": j.ref.MsgID, "vendorId": j.vendorID, "vendorMsgId": j.vendorMsgID,
				"vendorStatus": string(j.st), "errorCode": j.errorCode})
		return false
	}
	j.closed = true
	// The message's verdict, if this status is one (delivered, failed,
	// expired): the delivery ratio, the accept-to-delivered time, and the
	// end of its wait in the exact waiting set.
	monitor.Verdicts.Final(j.sender, j.accountID, string(j.st), j.ref.MsgID)
	// And the same verdict for quality routing, per vendor and network
	// (PO 2026-09-14). A no-op in a process that never started counting.
	vendorquality.Note(j.sender, j.mcc, j.mnc, string(j.st))
	// Verdict reached: fold the per-PDU outcome onto the record and retire the
	// correlation rows. Best effort — a failure here leaves the rows for the
	// retention pruner, it must not fail the receipt. [D-040]
	if pf, okPF := s.store.(PartFolder); okPF {
		// Deliberately ignored: folding is bookkeeping, not delivery truth. If
		// it fails the rows simply stay staged and the retention pruner folds
		// them later — failing the receipt here would be worse.
		//
		// Ignored is not the same as unseen, though: this failed on EVERY
		// long message for months (the landing column was missing from
		// mtlog_*) and nothing said so. It is logged now.
		if ferr := pf.FoldConcatParts(ctx, j.ref); ferr != nil {
			log.Printf("[dlr] fold per-part outcome onto %s failed (rows stay staged for the pruner): %v", j.ref.MsgID, ferr)
		}
	}
	return true
}

// clientReceipt is the receipt a final's client is sent. bindSlot routes it
// back to the bind the client submitted on (PO 2026-09-05) — for a concat
// group, the one bind all its parts came from. The client gets OUR receipt,
// rendered from the facts; the vendor's raw text stays in the record's
// raw_dlr for audit and never reaches the customer (PO 2026-09-11).
func (s *Service) clientReceipt(ctx context.Context, j *closeJob) Receipt {
	src, dst := s.addresses(ctx, j.ref)
	return Receipt{
		MsgID: j.ref.MsgID, Status: j.st, Stat: statSent(j.stat, j.st), ErrCode: j.errorCode,
		Src: src, Dst: dst, BindSlot: j.bindSlot,
		// The vendor's delivery time when it gave one; zero = now.
		DoneAt: j.doneAt,
		// Queued for the deployment the client is on (D-121): the
		// receipt may have come in on any gateway the vendor runs on.
		Owner: s.ownerOf(ctx, j.ref),
	}
}

// billJob works out, once, what a final receipt books, and puts it on its
// row: the receipt fee when the client is sent THIS receipt (not when it was
// told already, nor when the account's policy answers in its place — Tell
// books that one), and the vendor's sides charged on delivery.
func (s *Service) billJob(ctx context.Context, j *closeJob) {
	if j.billed {
		return
	}
	j.billed = true
	toClient := j.told == "" && j.invented == nil && s.pusher != nil && model.WantsTerminalReceipt(j.noReceipt, j.failureOnly, j.st)
	var charge string
	note := j.note
	if note == "" {
		note = s.recordNote(ctx, j.ref)
	}
	j.bs, charge = s.finalBill(note, j.ref.MsgID, j.company, j.accountID, j.sender, j.st, toClient, true)
	j.entry = withCharge(j.entry, charge, j.bs)
}

// closeRecord writes a final receipt's row and closes its message when this
// receipt is the one that does (mine). On this path — a gateway without the
// receipt facts in Redis — the record's own check decides (the unique key on
// the closing receipt row).
func (s *Service) closeRecord(ctx context.Context, j *closeJob) (mine bool, err error) {
	if s.writes != nil && j.st.IsTerminal() {
		// Through the queue (worker mode): the row and the close are the DB
		// worker's to write.
		return s.writes.CloseWithReceipt(ctx, j.ref, j.st, j.vendorMsgID, j.errorCode, j.entry, true)
	}
	if rc, ok := s.store.(ReceiptCloser); ok && j.st.IsTerminal() {
		return rc.CloseWithReceipt(ctx, j.ref, j.st, j.vendorMsgID, j.errorCode, j.entry, true)
	}
	// A store that cannot do both in one step: every receipt is recorded for
	// audit, even one that does not close its message, then the close.
	if !j.rowDone {
		if err := s.appendRaw(ctx, j.ref, j.entry); err != nil {
			return false, err
		}
		j.rowDone = true
	}
	if fz, okFZ := s.store.(Finalizer); okFZ && j.st.IsTerminal() {
		return fz.FinalizeOnce(ctx, j.ref, j.st, j.vendorMsgID, j.errorCode)
	}
	if err := s.store.UpdateStatus(ctx, j.ref, j.st, j.vendorMsgID, j.errorCode); err != nil {
		return false, err
	}
	return true, nil
}
