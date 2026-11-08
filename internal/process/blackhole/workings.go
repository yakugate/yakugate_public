package blackhole

import (
	"context"
	"encoding/json"
	"github.com/yakugate/yakugate/internal/attemptcap"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/deployment"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"os"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/recordq"
)

// Step runs one simulator cycle:
//
//	INPUT   — claim pending messages from every blackhole vendor's queue
//	LOGIC   — roll each message's outcome UP FRONT (delivered_pct -> delivered;
//	          failed_pct -> failed; remainder -> stays 'sent', no DLR) and
//	          write the TERMINAL record in one batched pass
//	OUTPUT  — Ack the whole claim. The message is NEVER transmitted onward.
//
// 2026-07-26 rework: the original path wrote each record individually
// (Write = 2 statements) then simulated the DLR via UpdateStatus (3 more) —
// 5 MySQL statements per message, measured at 75 msg/s/instance. Outcomes
// are now decided before writing, the batch lands as ONE multi-row terminal
// INSERT (+ one index upsert) per table, acks are batched, and the loop
// DRAINS while full batches return. [SCOPE BLACKHOLE-BATCH]
//
// 2026-10-07: a blackhole is paced at its deployment rate like any vendor
// (budget, below) — it read no rate at all, so a vendor set to 10/s and then
// to 1000/s sent about 75/s both times, which was simply all it could do:
// its client receipts were queued one message at a time. They now go in one
// batch (pushReceipts).
func (p *Process) Step(ctx context.Context) error {
	if p.queue == nil || p.hot == nil || p.traffic == nil {

		return nil // stores unavailable; nothing to do
	}
	if time.Since(p.loadedAt) > 30*time.Second {
		p.loadVendors(ctx)
	}
	if p.recQ.Full() {
		return nil // the record queue is full: nothing is taken that could not be recorded
	}
	batch := envInt("YAKU_BLACKHOLE_BATCH", 500)
	per := p.perCheck(batch)
	for _, v := range p.vendors {
		// One check takes up to YAKU_QUEUE_MT_PER_INTERVAL from each vendor,
		// a batch at a time (issue 7).
		for taken := 0; taken < per; {
			want := p.budget(v, min(batch, per-taken))
			if want == 0 {
				break // this second's rate is spent; the next tick asks again
			}
			msgs, err := p.take(ctx, v.cid, want)
			p.giveBack(v, want-len(msgs)) // what the queue did not have
			msgs = stampSender(msgs)
			if err != nil || len(msgs) == 0 {
				break
			}
			taken += len(msgs)
			recs := make([]dal.TerminalRecord, 0, len(msgs))
			ids := make([]string, 0, len(msgs))
			notes := make(map[string]billing.BillNote, len(msgs))
			for _, m := range msgs {
				m.VendorID = v.id // Claim doesn't carry vendor_id; stamp it for the record
				// The pretend network takes every message, whatever it then
				// says became of it.
				r := dal.TerminalRecord{VendorMsgID: "SIM-" + m.ID, AckAt: time.Now()}
				switch roll := p.rng.Intn(100); {
				case roll < v.deliveredPct:
					m.Status = model.StatusDelivered
				case roll < v.deliveredPct+v.failedPct:
					m.Status = model.StatusFailed
					r.ErrorCode = "SIM_UNDELIV"
				default:
					m.Status = model.StatusSent // no-DLR share: no receipt ever arrives
				}
				// The pretend vendor answered (accepted): its side, an account
				// charged on submit — booked now, the record's price on the
				// message (billing.Moments.VendorAnswered).
				notes[m.ID] = p.bill.VendorAnswered(ctx, &m, v.id, true)
				r.Msg = m
				// What the pretend vendor answered, as a real one's answer is
				// kept (sub_, issue 25.4): it took the message.
				r.Answers = []model.SubmitAnswer{{MsgID: m.ID, AccountID: m.AccountID, CompanyID: m.CompanyID,
					Part: 1, Parts: 1, Attempt: model.TryOf(m), VendorID: v.id, VendorMsgID: r.VendorMsgID,
					VendorProtocol: "blackhole", Result: model.AnswerAccepted, SentAt: r.AckAt, AnsweredAt: r.AckAt,
					Deployment: deployment.ID(), Session: v.cid}}
				recs = append(recs, r)
				ids = append(ids, m.ID)
				// [SCOPE P13-CAPTURE] The simulator stands in for a real
				// vendor, so a capture must show this leg too — otherwise a
				// capture on a dev account stops at "accepted" and never
				// shows what the vendor did with the message.
				acctCap, acctOn := logging.ActiveForAccountID(m.AccountID)
				vendCap, vendOn := logging.ActiveForVendor(v.cid)
				if acctOn || vendOn {
					f := logging.Fields{
						"messageId": m.ID, "vendor": v.cid, "vendorMsgId": r.VendorMsgID,
						"to": m.Destination, "from": m.Sender, "body": m.Body,
						"simulatedStatus": string(m.Status), "errorCode": r.ErrorCode,
					}
					if acctOn {
						logging.CaptureTo(acctCap, p.log, "vendor_response", "simulated vendor result", f)
					}
					if vendOn {
						logging.CaptureTo(vendCap, p.log, "vendor_response", "simulated vendor result", f)
					}
				}
			}
			// With the DB worker and the queues in Redis: the records are
			// queued and the messages leave the vendor queue in ONE Redis
			// transaction, so a stop cannot fall between the two.
			acked := false
			if q, ok := p.queue.(recordq.TxAcker); ok && p.recQ != nil {
				if err := p.recQ.RecordAndAck(ctx, q, v.cid, ids, recs, true); err != nil {
					p.log.Printf("terminal batch (%d msgs): %v", len(recs), err)
					p.putBack(ctx, msgs) // nothing happened: back on the queue, taken again
					break
				}
				acked = true
			} else if err := p.rec().WriteTerminalBatch(ctx, recs); err != nil {
				p.log.Printf("terminal batch (%d msgs): %v", len(recs), err)
				p.putBack(ctx, msgs) // back on the queue, taken again (the write reads before it inserts)
				break
			}
			if !acked {
				if err := p.queue.Ack(ctx, v.cid, ids...); err != nil {
					p.log.Printf("ack batch: %v", err)
				}
			}
			// Monitoring: the simulator stands in for a vendor, so it feeds the
			// same Out and Receipts lines a real one would. Counted once the
			// batch is durable — a batch that failed to write above was never
			// "sent" and is retried. Every message was accepted by the pretend
			// network; only the delivered/failed share produced a receipt.
			monitor.Flow.NoteOut(int64(len(recs)))
			var rcpts int64
			for _, r := range recs {
				if r.Msg.Status == model.StatusDelivered || r.Msg.Status == model.StatusFailed {
					rcpts++
				}
			}
			if rcpts > 0 {
				monitor.Flow.NoteReceipt(rcpts)
			}
			// smsc leg: a simulated terminal outcome owes the client its receipt
			// exactly like a real vendor's would (fix 2026-08-10 — see New).
			// The no-DLR share (status stays 'sent') correctly pushes nothing.
			if p.pusher != nil {
				pushes := make([]dlrsvc.Push, 0, len(recs))
				for _, r := range recs {
					// Submit-to-network DLR first (PO 2026-08-10): every accepted
					// message, including the no-DLR share, was "accepted" by the
					// simulated network. registered_delivery bit 4 (PO 2026-09-03)
					// gates it, same as the real vendor egress paths.
					if r.Msg.WantsIntermediateReceipt() {
						pushes = append(pushes, dlrsvc.Push{AccountID: r.Msg.AccountID, Receipt: dlrsvc.Receipt{MsgID: r.Msg.ID, Status: model.StatusSent,
							Src: r.Msg.Sender, Dst: r.Msg.Destination, BindSlot: r.Msg.BindSlot, Owner: r.Msg.QueueOwner}})
						p.bill.Notice(ctx, r.Msg.ID, r.Msg.CompanyID, r.Msg.AccountID, v.id, notes[r.Msg.ID])
					}
					if r.Msg.Status != model.StatusDelivered && r.Msg.Status != model.StatusFailed {
						continue
					}
					// The pretend vendor's final: what it books (the receipt fee
					// when the client is sent it, a delivery charge point, the
					// vendor's own sides).
					toClient := model.WantsTerminalReceipt(r.Msg.NoReceipt, r.Msg.FailureReceiptOnly, r.Msg.Status)
					p.bill.Final(ctx, billing.Final{MsgID: r.Msg.ID, Company: r.Msg.CompanyID, Account: r.Msg.AccountID, Vendor: v.id,
						Note: notes[r.Msg.ID], Status: string(r.Msg.Status), ToClient: toClient, FromVendor: true, VendorTook: true})
					// registered_delivery bits 0-1: WantsTerminalReceipt reads the
					// SAME two flags WriteTerminalBatch just stamped onto
					// record_index above, so the push and the billing/audit
					// row agree on what the client asked for.
					if !model.WantsTerminalReceipt(r.Msg.NoReceipt, r.Msg.FailureReceiptOnly, r.Msg.Status) {
						continue
					}
					pushes = append(pushes, dlrsvc.Push{AccountID: r.Msg.AccountID, Receipt: dlrsvc.Receipt{MsgID: r.Msg.ID, Status: r.Msg.Status,
						Src: r.Msg.Sender, Dst: r.Msg.Destination, BindSlot: r.Msg.BindSlot, Owner: r.Msg.QueueOwner}})
				}
				p.pushReceipts(ctx, pushes)
			}
			if len(msgs) < want {
				break
			}
		}
	}
	return nil
}

// rateKey is the bucket a vendor's rate is counted in on this deployment —
// the key smpp-vendor and http-vendor use, so a vendor's rate means one thing
// whichever role sends for it.
func rateKey(cid string) string {
	key := "vendor:" + cid
	if id := deployment.ID(); id != "" {
		key = "dep:" + id + "|" + key
	}
	return key
}

// budget says how many messages v may take right now, at most max: what is
// left of this second's rate on this deployment. A vendor with no rate here
// takes max, as every blackhole did before a rate was read.
func (p *Process) budget(v simVendor, max int) int {
	if v.tps <= 0 || p.rate == nil {
		return max
	}
	return p.rate.Grant(rateKey(v.cid), v.tps, max)
}

// giveBack returns n messages of budget the queue did not have, so a short
// claim does not use up the second for the messages that arrive in it.
func (p *Process) giveBack(v simVendor, n int) {
	if n > 0 && v.tps > 0 && p.rate != nil {
		p.rate.Return(rateKey(v.cid), n)
	}
}

// pushReceipts queues a batch's client receipts in one go when the pusher can
// (the queue pusher: a few round trips for the whole batch), else one by one.
func (p *Process) pushReceipts(ctx context.Context, pushes []dlrsvc.Push) {
	if len(pushes) == 0 {
		return
	}
	if b, ok := p.pusher.(interface {
		PushBatch(ctx context.Context, pushes []dlrsvc.Push) error
	}); ok {
		if err := b.PushBatch(ctx, pushes); err != nil {
			p.log.Printf("client dlr push (%d receipts): %v", len(pushes), err)
		}
		return
	}
	for _, x := range pushes {
		if err := p.pusher.PushToClient(ctx, x.AccountID, x.Receipt); err != nil {
			p.log.Printf("client dlr push %s: %v", x.Receipt.MsgID, err)
		}
	}
}

// loadVendors refreshes the list of blackhole vendors and their outcome split.
func (p *Process) loadVendors(ctx context.Context) {
	// The most times a message may be sent (attemptcap), read with the
	// vendors: a take refuses one that has had its tries (issue 22).
	attemptcap.Apply(ctx, p.engine().VendorSends, p.queue, p.log.Printf)
	// Only the vendors placed on THIS deployment (config/0122), like every
	// vendor role: a blackhole is placed and chosen like any other vendor
	// (Saleem 2026-10-02). A vendor placed nowhere is still claimed
	// everywhere, as before. The read is the DAL's (dal.VendorRows).
	var out []simVendor
	err := p.traffic.VendorRows().BlackholeVendors(ctx, deployment.ID(), func(r dal.BlackholeVendorRow, err error) {
		if err != nil {
			return
		}
		v := simVendor{id: r.ID, cid: r.CID}
		cfgJSON, deployJSON := r.DLRConfig, r.Deployments
		// This deployment's share of the vendor's rate (config/0124), read as
		// the other vendor roles read it; none here = no limit.
		deps, _ := deployment.Parse(deployJSON)
		v.tps = deps.Rate(deployment.ID(), deployment.MT)
		var dc struct {
			Delivered int `json:"delivered_pct"`
			Failed    int `json:"failed_pct"`
		}
		_ = json.Unmarshal([]byte(cfgJSON), &dc)
		v.deliveredPct, v.failedPct = dc.Delivered, dc.Failed
		if v.deliveredPct == 0 && v.failedPct == 0 {
			v.deliveredPct = 100 // sane default: deliver everything
		}
		out = append(out, v)
	})
	if err != nil {
		p.log.Printf("load vendors: %v", err)
		return
	}
	p.vendors = out

	p.loadedAt = time.Now()
	// Say each vendor's rate once, and again when it changes: a queue that
	// leaves slowly should not have to be explained from the vendor form.
	said := map[string]int{}
	for _, v := range out {
		said[v.cid] = v.tps
		if was, known := p.saidTPS[v.cid]; known && was == v.tps {
			continue
		}
		if v.tps > 0 {
			p.log.Printf("%s: at most %d messages a second on this deployment", v.cid, v.tps)
		} else {
			p.log.Printf("%s: no rate set for this deployment — sends as fast as it can", v.cid)
		}
	}
	p.saidTPS = said
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// stampSender marks every claimed message with THIS deployment as the one
// that sent it (hot/0046). Stamped at the claim, not at the record write, so
// every path out of this role — sent, failed, retried into a terminal record —
// carries it without six separate call sites having to remember.
//
// The accepting deployment rode the queue row and is already on the message;
// these two are what tell an operator that a submit landed in one site and
// left from another, which is exactly what pinning a vendor to a deployment
// makes routine.
func stampSender(msgs []model.Message) []model.Message {
	id := deployment.ID()
	for i := range msgs {
		msgs[i].SentDeploymentID = id
	}
	return msgs
}
