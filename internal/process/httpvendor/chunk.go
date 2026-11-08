package httpvendor

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/ackkeep"
	"github.com/yakugate/yakugate/internal/dal"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/recordq"
	"github.com/yakugate/yakugate/internal/vendorquality"
)

// Filing a bulk call's messages (Saleem, 2026-10-12: "bulk submission is even
// slower than single submission, why? it was meant to be faster as many
// messages had to be submitted in a single hit").
//
// The call WAS one hit — up to 500 messages in one request. What followed was
// not: the role then filed the messages one after another on the worker that
// made the call, and each one cost a record insert, a locator insert, a status
// update, a queue acknowledgement and a client receipt — some six round trips
// a message. A 500-message chunk took seven to ten seconds to file. Single
// sends do the same work for each message, but up to submit_concurrency of
// them at once, so Bulk ran at about 110 messages a second where single sends
// ran at 340 to 460 (his run of 14:38 UTC).
//
// Now a chunk is filed in a few writes for all of it: one record write
// (mysql.Store.WriteBatch — what Write stores for each, in two statements for
// the lot), one queue acknowledgement, one push of the client receipts. What
// is recorded is the same as before, message for message.

// handOnAtOnce is how many messages of a chunk the vendor would not take now
// are handed to the next vendor of their round at the same time.
const handOnAtOnce = 32

// fileChunk files what became of every message of one bulk call: the vendor
// took them (ids holds its id for each, "" where it gave none), or refused
// them for good with `refusal` as the code. It reports whether they were
// filed; false = the record could not be written (the messages left their
// queue when they were taken: they have no record).
func (p *Process) fileChunk(ctx context.Context, v vendorAdapter, chunk []model.Message, ids []string, refusal string, until time.Time, answers []model.SubmitAnswer) bool {
	if len(chunk) == 0 {
		return true
	}
	sent := refusal == ""
	now := time.Now()
	// How many messages of the call each vendor id names: an id that names
	// several is filed as their group (noteSent) — the parts of one long
	// message, or messages the vendor gave one id.
	named := map[string]int{}
	firstOf := map[string]model.Message{}
	parts := map[string]bool{}
	if sent {
		for i, m := range chunk {
			ref := vendorRef(ids[i], m.ID)
			named[ref]++
			if f, seen := firstOf[ref]; !seen {
				firstOf[ref], parts[ref] = m, m.ConcatTotal > 1
			} else if !sameLongMessage(f, m) {
				parts[ref] = false
			}
		}
	}
	filed := make([]stamped, len(chunk))
	recs := make([]dal.TerminalRecord, len(chunk))
	msgIDs := make([]string, len(chunk))
	for i, m := range chunk {
		v.mark(&m)
		msgIDs[i] = m.ID
		// The vendor answered: its side, an account charged on submit, a
		// difference in parts — booked now, the record's price on the message
		// (billing.Moments.VendorAnswered).
		p.bill.VendorAnswered(ctx, &m, v.id, sent)
		if sent {
			m.Status = model.StatusSent
			// The vendor's id goes into the record with the status, in one
			// write (2026-10-09).
			if ids[i] == "" {
				p.noteNoID(v, m.ID)
			}
			m.VendorMsgID = vendorRef(ids[i], m.ID)
			// The account may want its client told the message is done the
			// moment the vendor takes it (config/0119) — stamped on the
			// message so the insert files it beside the real status.
			filed[i] = p.tellOnSubmit(m)
			recs[i] = dal.TerminalRecord{Msg: filed[i].msg, VendorMsgID: m.VendorMsgID, AckAt: now, Answers: answerAt(answers, i)}
			group := ""
			if named[m.VendorMsgID] > 1 {
				group = dlrsvc.GroupCall
				if parts[m.VendorMsgID] {
					group = dlrsvc.GroupParts
				}
			}
			p.noteSent(ctx, v.id, m.VendorMsgID, filed[i].msg, group)
			continue
		}
		m.Status = model.StatusFailed
		// What the client is told about this refusal, if the account has a
		// rule for the code — decided BEFORE the record is written.
		filed[i] = p.tellOnRefusal(m, refusal)
		recs[i] = dal.TerminalRecord{Msg: filed[i].msg, ErrorCode: refusal, Answers: answerAt(answers, i)}
	}
	acked, err := p.recordWriteBatch(ctx, v.cid, msgIDs, recs, until)
	if err != nil {
		p.log.ForVendor(v.cid).Errorf("record", "record write of %d message(s) (first %s): %v (sent or refused, they have NO RECORD)",
			len(chunk), msgIDs[0], err)
		return false
	}
	if !acked { // with the DB worker the ack was made with the records, in one step
		if err := p.ack(ctx, v.cid, msgIDs...); err != nil {
			p.log.ForVendor(v.cid).Warnf("queue_ack", "ack of %d message(s): %v", len(msgIDs), err)
		}
	}
	// Each message's notice or failure receipt, queued together.
	var pushes []dlrsvc.Push
	for _, f := range filed {
		m := f.msg
		if sent {
			monitor.Verdicts.Sent(v.id, m.AccountID, m.ID) // a receipt is now owed
			// registered_delivery bit 4 (PO 2026-09-03), or the account's
			// progress receipt in its place (2026-09-29).
			if p.svc != nil {
				if st, d, ok := p.submitNotice(v, m); ok {
					pushes = append(pushes, dlrsvc.Push{AccountID: m.AccountID, Receipt: dlrsvc.NoticeReceipt(m, st, d)})
				}
			}
			continue
		}
		monitor.Verdicts.Final(v.id, m.AccountID, "failed", m.ID) // the vendor refused it outright
		vendorquality.Note(v.id, m.MCC, m.MNC, "failed")
		// registered_delivery (PO 2026-09-03): only NoReceipt suppresses a
		// failure — FailureReceiptOnly explicitly WANTS this one. A refusal
		// the account's policy answers gets that answer instead (below).
		if f.outcome == nil && p.svc != nil && model.WantsTerminalReceipt(m.NoReceipt, m.FailureReceiptOnly, model.StatusFailed) {
			pushes = append(pushes, dlrsvc.Push{AccountID: m.AccountID, Receipt: dlrsvc.RefusalReceipt(m, refusal)})
		}
	}
	if p.svc != nil {
		p.svc.PushMany(ctx, pushes)
		// What those receipts book (dlr billing.go): the gateway's own ACCEPTD
		// under all_levels, a refusal's receipt.
		for _, x := range pushes {
			m := filedMsg(filed, x.Receipt.MsgID)
			switch {
			case m == nil:
			case sent && x.Receipt.Stat == "" && x.Receipt.Delay == 0:
				p.svc.BookNotice(ctx, *m)
			case !sent:
				p.svc.BookFinal(ctx, *m, model.StatusFailed)
			}
		}
		// What an account's receipt policy answers, after the notices as a
		// single send does it. One at a time: few accounts have a policy.
		for _, f := range filed {
			if f.outcome != nil {
				p.svc.Tell(ctx, dlrsvc.TellFor(f.msg), *f.outcome)
			}
		}
	}
	return true
}

// recordWriteBatch is recordWrite for a whole chunk: the vendor has answered,
// so a record write that fails is tried again until the given time, and only
// then given up.
//
// With the DB worker and the queues in Redis the records are queued and the
// messages taken off the vendor queue in ONE Redis transaction
// (recordq.Client.RecordAndAck): acked is true and the caller must not ack
// again. A stop can then not fall between the record and the ack.
// answerAt is the i-th message's answer as its record's answers (nil when
// none was given).
func answerAt(answers []model.SubmitAnswer, i int) []model.SubmitAnswer {
	if i >= len(answers) || answers[i].MsgID == "" {
		return nil
	}
	return answers[i : i+1]
}

func (p *Process) recordWriteBatch(ctx context.Context, cid string, ids []string, recs []dal.TerminalRecord, until time.Time) (acked bool, err error) {
	wait := 50 * time.Millisecond
	txq, oneStep := p.queue.(recordq.TxAcker)
	oneStep = oneStep && p.recQ != nil
	for {
		var err error
		if oneStep {
			err = p.recQ.RecordAndAck(ctx, txq, cid, ids, recs, false)
		} else {
			err = p.rec().WriteBatch(ctx, recs)
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

// handOn sends each message of a chunk the vendor would not take NOW on to
// the next vendor of its round, several at a time, and says how many could
// not be handed on (each is tried again after its gap, tryAgain).
func (p *Process) handOn(ctx context.Context, v vendorAdapter, chunk []model.Message, code string) int {
	var left atomic.Int64
	units := make([]func(), len(chunk))
	for i, m := range chunk {
		units[i] = func() {
			if !p.nextVendor(v, m, []model.Message{m}, code) {
				left.Add(1)
				p.tryAgain(ctx, v, m)
			}
		}
	}
	runUnits(units, handOnAtOnce)
	return int(left.Load())
}

// ack takes sent messages off the vendor's queue. A refusal — the queue store
// restarting — is kept and made again until the store takes it
// (internal/ackkeep, go-live test P2 E3): left alone, the lease ran out and
// the message was sent to the vendor a second time.
func (p *Process) ack(ctx context.Context, cid string, ids ...string) error {
	p.acksOnce.Do(func() {
		var q ackkeep.Acker
		if p.queue != nil {
			q = p.queue
		}
		p.acks = ackkeep.New(q, func(vendor string, failed bool, text string) {
			if failed {
				p.log.ForVendor(vendor).Errorf("queue_ack", "%s", text)
				return
			}
			p.log.ForVendor(vendor).Warnf("queue_ack", "%s", text)
		})
	})
	return p.acks.Ack(ctx, cid, ids...)
}

// sameLongMessage says two messages are parts of one long message: one
// account, sender, number and part reference, more than one part.
func sameLongMessage(a, b model.Message) bool {
	return a.ConcatTotal > 1 && b.ConcatTotal == a.ConcatTotal && a.ConcatRef == b.ConcatRef &&
		a.AccountID == b.AccountID && a.Sender == b.Sender && a.Destination == b.Destination
}

// filedMsg is the message of a filed chunk with this id.
func filedMsg(filed []stamped, id string) *model.Message {
	for i := range filed {
		if filed[i].msg.ID == id {
			return &filed[i].msg
		}
	}
	return nil
}
