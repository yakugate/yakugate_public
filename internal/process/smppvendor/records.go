// Records of what was sent: the batch writer, the messages taken off the
// queue, and each sent message's receipt facts.

package smppvendor

import (
	"context"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/ackkeep"
	"github.com/yakugate/yakugate/internal/dal"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/recordq"
)

// outcome is one finalized submit result headed for the batch writer.
type outcome struct {
	msg         model.Message // Status already terminal-for-egress (sent/failed)
	vendorMsgID string
	errorCode   string
	// told is the answer the account's receipt policy decided to give the
	// client instead of (or as well as) what really happened — nil for every
	// message of an account with no policy, which is all of them today.
	// msg.Told* already carries it, so the record write files it; this is the
	// copy the batch writer needs to actually send the receipt.
	told *receiptpolicy.Outcome
	// ackAt is when the vendor's submit answer took the message; zero for a
	// refusal. Goes to mtlog.vendor_ack_at (the vendor summary's answer time).
	ackAt time.Time
	// answers are the vendor's answers that go with this outcome — every
	// part of the try that ended the message (sub_, issue 25.4).
	answers []model.SubmitAnswer
	// loose are answers that end nothing (not now, no answer in time, a
	// part's answer after its message was decided): written as rows of
	// their own. An outcome carrying loose answers carries no message.
	loose []model.SubmitAnswer
}

// batchWriter drains outcomes and persists them in batches: one multi-row
// terminal INSERT (+ index upsert) via mysql.WriteTerminalBatch, then ONE
// batched queue Ack. Flush on size (vendor's batch_size column, default 500)
// or 100ms, whichever first. A failed flush is dropped WITHOUT acking — the
// lease expires and the messages are re-claimed; the upsert makes the
// eventual re-write idempotent. [SCOPE P3-BATCH]
func (p *Process) batchWriter(ctx context.Context, v vendorConn, outc <-chan outcome) {
	flushSize := v.batchSize
	if flushSize <= 0 {
		flushSize = 500
	}
	// DB writes must survive the shutdown cancel: gracefulUnbind deliberately
	// drains in-flight submit_sm_resp AFTER ctx is cancelled, and those drained
	// outcomes would otherwise hit ExecContext with a dead context and be
	// thrown to lease-retry — the drain would empty the window without ever
	// persisting what it drained (found by TC-P3-49, 2026-08-10).
	opCtx := context.WithoutCancel(ctx)
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	buf := make([]outcome, 0, flushSize)
	// Answers that end nothing (not now, no answer in time): rows of their
	// own in sub_, written beside the batch, best effort.
	var loose []model.SubmitAnswer
	writeLoose := func() {
		if len(loose) == 0 {
			return
		}
		rec := p.rec()
		if rec == nil {
			loose = loose[:0]
			return
		}
		// Through the DB worker's queue when the records go there (the
		// three-table design: every sub_ row is queued), else the store.
		if err := rec.WriteAnswers(opCtx, loose); err != nil {
			p.log.ForVendor(v.cid).Warnf("record_write", "%d vendor answer(s) not written: %v", len(loose), err)
		}
		loose = loose[:0]
	}
	take := func(o outcome) {
		if o.loose != nil {
			loose = append(loose, o.loose...)
			return
		}
		buf = append(buf, o)
	}
	// When a record write began failing; zero while writes succeed. held is
	// how many of the batch have had their leases pushed out meanwhile.
	var failingSince time.Time
	held := 0
	flush := func() {
		writeLoose()
		if len(buf) == 0 {
			return
		}
		// Settings as of this flush: "own ACCEPTD/ENROUTE" and the batch size
		// apply without a rebind (2026-09-29).
		v = p.withLive(v)
		if v.batchSize > 0 {
			flushSize = v.batchSize
		} else {
			flushSize = 500
		}
		recs := make([]dal.TerminalRecord, len(buf))
		ids := make([]string, len(buf))
		for i, o := range buf {
			recs[i] = dal.TerminalRecord{Msg: o.msg, VendorMsgID: o.vendorMsgID, ErrorCode: o.errorCode, AckAt: o.ackAt, Answers: o.answers}
			ids[i] = o.msg.ID
		}
		acked, err := p.recordBatch(opCtx, v.cid, ids, recs)
		if err != nil {
			// These messages have been SENT and left their queue when they
			// were taken (issue 22): the batch is KEPT and written again on
			// the next tick; the store itself already runs a deadlocked write
			// again. Only a hot store that stays unwritable past
			// recordRetryFor loses their records, and that is said loudly.
			if failingSince.IsZero() {
				failingSince = time.Now()
				held = len(ids)
			} else if len(ids) > held {
				held = len(ids)
			}
			if time.Since(failingSince) < recordRetryFor {
				p.log.ForVendor(v.cid).Warnf("record_write", "terminal batch (%d): %v — kept, trying again", len(buf), err)
				return
			}
			p.log.ForVendor(v.cid).Errorf("record_write", "terminal batch (%d): %v — not written for %s; these messages were sent and have NO RECORD",
				len(buf), err, recordRetryFor)
			failingSince, held = time.Time{}, 0
			buf = buf[:0]
			return
		}
		failingSince, held = time.Time{}, 0
		// A refused ack is kept and made again (p.ack): left alone, the lease
		// ran out and the messages were sent a second time. With the DB
		// worker the ack was made with the records, in one step (recordBatch).
		if !acked {
			if err := p.ack(opCtx, v.cid, ids...); err != nil {
				p.log.ForVendor(v.cid).Warnf("queue_ack", "ack batch: %v", err)
			}
		}
		// Submit-to-network DLR (PO 2026-08-10): a vendor-accepted message owes
		// the client an intermediate ACCEPTD notification. Per message ULID —
		// so an SMPP-ingress multipart group yields one per part.
		if p.svc != nil {
			for _, o := range buf {
				// registered_delivery bit 4 (PO 2026-09-03): the client asked
				// for this notice separately from the final outcome; default
				// off per spec, unlike HTTP ingress which has no such bit.
				// Or the account's progress receipt (ACCEPTD / ENROUTE) in its
				// place, when its receipt policy has one (2026-09-29).
				if o.msg.Status == model.StatusSent {
					if st, d, ok := dlrsvc.SubmitNotice(p.levels(o.msg.AccountID), p.policies.For(o.msg.AccountID),
						o.msg.WantsIntermediateReceipt(), v.ownIntermediateDLR); ok {
						p.svc.PushNotice(opCtx, o.msg, st, d)
					}
				}
				// A submit the vendor refused outright is a terminal outcome the
				// client asked to hear about, the same as an UNDELIV receipt or
				// the retry role's give-up. Until 2026-09-17 it was the one
				// verdict nobody told the client (E2E run 2026-09-16, I-17): the
				// record said failed, the bind heard nothing. Pushed after the
				// record is written, like the HTTP vendor does.
				//
				// Unless the account's receipt policy answered for it: the
				// client is then told that instead, once, and the real
				// outcome stays on the record (config/0119).
				if o.told != nil {
					p.svc.Tell(opCtx, dlrsvc.TellFor(o.msg), *o.told)
					continue
				}
				// The standard word for a vendor's refusal is REJECTD (issue
				// 22; it was UNDELIV), with the vendor's code.
				if o.msg.Status == model.StatusFailed && wantsFailureReceipt(o.msg) {
					p.svc.PushRefusal(opCtx, o.msg, o.errorCode)
				}
			}
		}
		buf = buf[:0]
	}
	for {
		select {
		case <-ctx.Done():
			// Drain anything already queued (the graceful-shutdown window
			// drain pushed outcomes right before cancelling us), then flush.
			for {
				select {
				case o := <-outc:
					take(o)
				default:
					flush()
					return
				}
			}
		case o := <-outc:
			take(o)
			// While a write is failing the batch is retried on the tick, not
			// on every new outcome (that would hammer a store in trouble).
			if len(buf) >= flushSize && failingSince.IsZero() {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}

// recordRetryFor is how long a batch of sent messages is kept and written
// again when the record store refuses it.
const recordRetryFor = 90 * time.Second

// recordBatch files a batch of messages the vendor has answered for.
//
// With the DB worker and the queues in Redis (YAKU_RECORDS_VIA=worker) the
// records are queued and the messages taken off the vendor queue in ONE
// Redis transaction (recordq.Client.RecordAndAck): acked is true and the
// caller must not ack again. A stop can then no longer fall between "its
// record is there" and "it has left the queue", which is what sent a message
// a second time. Otherwise the records are written and the caller acks, as
// it always did.
func (p *Process) recordBatch(ctx context.Context, cid string, ids []string, recs []dal.TerminalRecord) (acked bool, err error) {
	if p.recQ != nil {
		if q, ok := p.queue.(recordq.TxAcker); ok {
			if err := p.recQ.RecordAndAck(ctx, q, cid, ids, recs, true); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, p.rec().WriteTerminalBatch(ctx, recs)
}

// noteSent files a sent message's receipt facts in Redis (dlr.Service.NoteSent).
func (p *Process) noteSent(ctx context.Context, vendorID int64, vendorMsgID string, m model.Message) {
	p.noteFacts(ctx, vendorID, vendorMsgID, m, 0, 0)
}

// notePartSent files one part of a message sent as several PDUs: the
// message's facts, the number of parts, and which part this vendor id is.
func (p *Process) notePartSent(ctx context.Context, vendorID int64, vendorMsgID string, m model.Message, part, parts int) {
	p.noteFacts(ctx, vendorID, vendorMsgID, m, part, parts)
}

func (p *Process) noteFacts(ctx context.Context, vendorID int64, vendorMsgID string, m model.Message, part, parts int) {
	// Not the session's own context: an answer drained during a graceful
	// shutdown still has a receipt coming.
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	p.svc.NoteSent(fctx, vendorID, vendorMsgID, dlrsvc.Facts{
		Parts: parts, Part: part,
		MsgID: m.ID, AccountID: m.AccountID,
		NoReceipt: m.NoReceipt, FailureOnly: m.FailureReceiptOnly, WantIntermediate: m.WantsIntermediateReceipt(),
		BindSlot: m.BindSlot, Privacy: dal.PrivacyOf(m),
		MCC: m.MCC, MNC: m.MNC, Src: m.Sender, Dst: m.Destination,
		Owner: m.QueueOwner, Table: dal.MTLogTableFor(m),
		// Kept for as long as its vendor's wait for a receipt (D-136).
		Wait: m.ReceiptWait(),
		// What its receipt may still book (billing.Moments.VendorAnswered).
		Bill: dlrsvc.BillOf(m),
		// A final answer the account's policy gave at submit (stampTold):
		// the vendor's own receipt is then recorded and not sent on.
		Told: string(m.ToldStatus),
		// Its vendor's first final closes a long message (4 Nov 2026).
		FirstCloses: m.VendorFirstReceiptCloses,
	})
}

// ack takes sent messages off the vendor's queue. A refusal — the queue store
// restarting — is kept and made again until the store takes it
// (internal/ackkeep, go-live test P2 E3).
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

func isDup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Duplicate entry")
}
