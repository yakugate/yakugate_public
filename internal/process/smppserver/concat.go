package smppserver

// Long messages sent in parts: each part buffered, the joined whole, and the
// groups that never complete.

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// partAlone readies a client's concat part that goes through the pipeline by
// itself (no assembly buffer, or the buffer refused it): submitText kept a
// GSM-7 escape that ends it, as "\x1b", for concat.Assemble to join with the
// next part — nothing will join it here, and the encoding stage would read the
// control character as UCS-2 (a full 153-character part then fits no PDU). It
// is dropped, as a part decoded alone always dropped it. A whole message has
// none and is left as it is.
func partAlone(msg *model.Message) {
	if msg.ConcatTotal > 1 {
		msg.Body = pdu.MendSplitEscapes(msg.Body)
	}
}

// handleConcatPart buffers one concat part and acks it immediately — the part
// is durable in the DB buffer, which is what "accepted" means for a part
// under assemble-first (D-096 Q3: "we will acknowledge immidiately and will
// do rest later"). The completing part's caller runs the pipeline on the
// assembled whole; a content rejection at that point can only surface via
// records (the parts were already acked) — accepted in the Q3 ruling.
func (p *Process) handleConcatPart(ctx context.Context, s *session, seq uint32, msg *model.Message) {
	complete, whole, err := p.assembler.AddPart(ctx, msg)
	if err != nil {
		// NOT buffered anywhere durable — never ack what we can't hold. Fall
		// back to the legacy inline per-part path (still correct, D-014).
		logging.New(p.Name()).Warn(p.acctScope(msg.AccountID), "concat_buffer_unavailable",
			"assembly buffer unavailable — falling back to the inline per-part path",
			logging.Fields{"messageId": msg.ID, "error": err.Error()})
		p.stats2.concatFallback.Add(1)
		partAlone(msg)
		p.pipelineRespond(ctx, s, seq, msg)
		return
	}
	p.stats2.concatParts.Add(1)
	s.send(pdu.SubmitSMRespPDU(pdu.ESME_ROK, seq, msg.ID))
	if !complete {
		return
	}
	p.stats2.concatCompleted.Add(1)
	// The assembled message carries part 1's id, minted when that part arrived.
	if at, ok := monitor.ULIDTime(whole.ID); ok {
		p.stats2.concatMu.Lock()
		if p.stats2.concatTook == nil {
			p.stats2.concatTook = monitor.NewHourHist(concatBoundsSec)
		}
		p.stats2.concatTook.Add(time.Since(at).Seconds(), time.Now().Unix())
		p.stats2.concatMu.Unlock()
	}
	p.processAssembled(ctx, whole, p.liveAccount(s), s.ip)
}

// processAssembled runs the pipeline once over an assembled whole message.
// No submit_sm_resp is owed here — every part was acked at buffer time.
func (p *Process) processAssembled(ctx context.Context, whole *model.Message, acct *refdata.Account, ip string) {
	// Saved BEFORE the pipeline, so no vendor receipt can arrive ahead of it:
	// every part's id, for the receipt path to answer each (PO 2026-09-11).
	if len(whole.PartIDs) > 1 && p.queue != nil {
		if err := p.queue.SaveClientPartIDs(ctx, whole.ID, whole.PartIDs); err != nil {
			logging.New(p.Name()).Warn(p.acctScope(acct.ID), "part_ids_save_failed",
				"could not save a long message's part ids; only part 1 will get receipts",
				logging.Fields{"messageId": whole.ID, "parts": len(whole.PartIDs), "error": err.Error()})
		}
	}
	e := &pipeline.Envelope{Msg: whole, Account: acct, ClientIP: ip}
	pipeStart := time.Now()
	rej := p.pipe.Run(ctx, e)
	if p.stats != nil {
		took := time.Since(pipeStart)
		p.stats.Pipeline.Add(took)
		p.stats.AcceptLatency(acct.Username, took)
		p.stats.Message(acct.Username, whole.Segments, rej == nil)
		if rej != nil {
			p.stats.RejectReason(reasonCode(rej.Reason))
		}
	}
	if rej != nil {
		if rej.Duplicate {
			// Every part was acked ESME_ROK with its own id, so a duplicate
			// can only be told by its receipts: REJECTD err:408 on every part
			// id (Saleem, 28 Oct 2026), filed in dlr_ with the refusal.
			p.refuseDuplicate(ctx, whole, acct, whole.PartIDs)
		}
		p.recordReject(whole, acct, rej, ip)
		logging.New(p.Name()).Warn(logging.AccountScope(acct.Username), "concat_rejected",
			"assembled multipart message rejected after its parts were acked",
			logging.Fields{"messageId": whole.ID, "reason": rej.Reason})
		if !rej.Duplicate {
			// Every part was already acked ESME_ROK, so the refusal can only
			// reach the client as a receipt: UNDELIV on every part, err = the
			// command_status the refusal maps to (e.g. 402 = no balance).
			p.pushPartReceipts(ctx, whole, model.StatusFailed, fmt.Sprintf("%03X", statusFor(rej.Reason)&0xFFF))
		}
		if p.queue != nil && len(whole.PartIDs) > 1 {
			_ = p.queue.DropClientPartIDs(ctx, whole.ID)
		}
	}
}

// assemblyReaper records incomplete concat groups past the assembly timeout as
// expired and drops them (choice A, PO 2026-08-09: a partial concat can't be
// delivered coherently, so the honest outcome is a clean expiry — NOT a
// per-part flush; supersedes D-096 Q4). Runs on every instance; the buffer's
// atomic claim makes concurrent reapers safe (each group is recorded once).
func (p *Process) assemblyReaper(ctx context.Context, log *logging.Logger) {
	if p.assembler == nil {
		return
	}
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			expired, err := p.assembler.ReleaseExpired(ctx)
			if err != nil {
				log.Error(logging.SystemScope, "concat_reaper", "assembly reaper failed", logging.Fields{"error": err.Error()})
				continue
			}
			p.stats2.concatExpired.Add(int64(len(expired)))
			for _, m := range expired {
				p.recordExpired(m, log)
				// The parts that did arrive were acked; tell the client each
				// one expired rather than leaving it waiting for ever.
				p.pushPartReceipts(ctx, m, model.StatusExpired, "")
			}
		}
	}
}

// recordExpired persists a timed-out concat group for observability, reusing
// the reject-log sink (reason ASSEMBLY_TIMEOUT). Fire-and-forget on its own
// context — the reaper must never block on a slow hot store. No client is
// owed a response: every part was acked at buffer time.
func (p *Process) recordExpired(m *model.Message, log *logging.Logger) {
	if p.rejects == nil || m == nil {
		return
	}
	rec := *m
	rec.Status = model.StatusExpired
	rec.RejectReason = "ASSEMBLY_TIMEOUT"
	rec.Ingress = model.TransportSMPP
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := p.rejects.WriteRejected(ctx, rec); err != nil {
			log.Error(logging.SystemScope, "concat_reaper", "expired-group record write failed", logging.Fields{"messageId": rec.ID, "error": err.Error()})
		}
	}()
}
