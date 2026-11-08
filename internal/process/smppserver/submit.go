package smppserver

// A client's submit_sm: read it, run the pipeline, answer it.

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// handleSubmit decodes submit_sm, runs the shared pipeline, and replies. [SCOPE P2-SUBMIT]
func (p *Process) handleSubmit(ctx context.Context, s *session, pd *pdu.PDU) {
	seq := pd.Header.SequenceNumber
	if s.mode != modeTX && s.mode != modeTRX {
		p.captureSubmitResp(logging.New(p.Name()), s.acctName(), seq, pdu.ESME_RINVBNDSTS, "", "submit on a bind that cannot send")
		s.send(pdu.SubmitSMRespPDU(pdu.ESME_RINVBNDSTS, seq, ""))
		body, _ := pdu.DecodeSubmitSM(pd.Body)
		p.recordSubmitRefusal(s, body, "BIND_CANNOT_SEND", "a submit on a bind that cannot send (not bound, or bound as a receiver)")
		return
	}
	body, err := pdu.DecodeSubmitSM(pd.Body)
	if err != nil {
		s.send(pdu.GenericNackPDU(pdu.ESME_RINVCMDLEN, seq))
		p.recordSubmitRefusal(s, nil, "INVALID_PDU", "the submit_sm could not be read")
		return
	}
	// TON/NPI address-plan check [SCOPE P2-TONNPI, PO 2026-07-27] — reverses
	// the previously documented non-goal ("source_addr passes through without
	// TON/NPI interpretation"). Destination cleaning/junk-skim/digit-convert
	// already runs first, shared with HTTP via pipeline stage 1 (D-064) — this
	// is a separate, SMPP-only protocol-level check ahead of that, mirroring
	// the mode check above (a bare address-plan refusal, no Message exists
	// yet to log a reject against).
	if !validateAddressPlan(body) {
		p.captureSubmitResp(logging.New(p.Name()), s.acctName(), seq, pdu.ESME_RINVDSTADR, "", "destination TON/NPI outside the accepted address plan")
		s.send(pdu.SubmitSMRespPDU(pdu.ESME_RINVDSTADR, seq, ""))
		p.recordSubmitRefusal(s, body, "INVALID_ADDRESS_PLAN", "destination TON/NPI outside the accepted address plan")
		return
	}
	// 8-bit data (data_coding 2 or 4, or a GSM 8-bit class) is not text, and
	// the gateway carries text: it was read as characters and sent on as
	// UCS-2, so the octets that arrived were never the ones sent (encoding
	// test of 19 Oct 2026, enc-71: c0ffee00ff7e41 went out as 00C0 00FF 00EE
	// 0000 …). Refused, and logged with the other refusals.
	if pdu.IsBinaryCoding(body.DataCoding) {
		p.captureSubmitResp(logging.New(p.Name()), s.acctName(), seq, pdu.ESME_RINVDCS, "", "8-bit data is not carried")
		s.send(pdu.SubmitSMRespPDU(pdu.ESME_RINVDCS, seq, ""))
		p.recordSubmitRefusal(s, body, "UNSUPPORTED_DATA_CODING",
			fmt.Sprintf("data_coding 0x%02X is 8-bit data, not text; the gateway carries text", body.DataCoding))
		return
	}
	// The account as it is NOW, not as it was at bind (see liveAccount).
	acct := p.liveAccount(s)
	msg := toMessage(body, acct)
	// Suspended, switched off for SMPP or expired while bound: the bind
	// stays up (the client reconnecting would only be refused at bind), but
	// nothing more is accepted on it.
	if !acct.SMPP || acct.Suspended() || expired(acct) {
		rej := pipeline.NewReject(403, "ACCOUNT_DISABLED")
		if p.stats != nil {
			p.stats.Message(acct.Username, 1, false)
			p.stats.RejectReason(reasonCode(rej.Reason))
		}
		p.recordReject(msg, acct, rej, s.ip)
		p.captureSubmitResp(logging.New(p.Name()), s.acctName(), seq, statusFor(rej.Reason), "", "account suspended, switched off or expired since it bound")
		s.send(pdu.SubmitSMRespPDU(statusFor(rej.Reason), seq, ""))
		return
	}
	fromConnection(msg, s)
	// Assemble-first for concat parts [SCOPE P2-ASSEMBLE, D-096]: buffer the
	// part durably, ack immediately (Q3 ruling), run the pipeline ONCE on the
	// assembled whole message when the group completes. Whole messages (and
	// deployments without the DB buffer) keep the classic inline path below.
	if msg.ConcatTotal > 1 && p.assembler != nil {
		p.handleConcatPart(ctx, s, seq, msg)
		return
	}
	partAlone(msg)
	p.pipelineRespond(ctx, s, seq, msg)
}

// fromConnection puts on a client's message what belongs to its CONNECTION,
// not to the PDU, so toMessage cannot: the bind it came in on, so the
// receipt can go back down the same one (PO 2026-09-05), and the client's
// address, which its record keeps (issue 13). Set before the concat buffer,
// which JSON-serializes the whole message, so an assembled group keeps both.
// The address was set on the long-message path only until 28 Oct 2026: a
// one-part message's mt_ row had no ip_address (Saleem's test that day).
func fromConnection(msg *model.Message, s *session) {
	msg.BindSlot = s.slot
	msg.IPAddress = s.ip
}

// pipelineRespond is the classic inline submit path: run the pipeline, answer
// the submit_sm_resp per the outcome. Shared by whole messages and the
// legacy/fallback per-part flow.
func (p *Process) pipelineRespond(ctx context.Context, s *session, seq uint32, msg *model.Message) {
	acct := p.liveAccount(s)
	e := &pipeline.Envelope{Msg: msg, Account: acct, ClientIP: s.ip}
	pipeStart := time.Now()
	rej := p.pipe.Run(ctx, e)
	if p.stats != nil { // [SCOPE P1-STATUS] same counters as the HTTP edge
		// Pipeline time too, which the SMPP edge never recorded: its latency
		// block and every SMPP customer's figure read empty (PO 2026-09-11).
		took := time.Since(pipeStart)
		p.stats.Pipeline.Add(took)
		p.stats.AcceptLatency(acct.Username, took)
		p.stats.Message(acct.Username, msg.Segments, rej == nil)
		if rej != nil {
			p.stats.RejectReason(reasonCode(rej.Reason)) // reject breakdown, PO 2026-07-27
		}
	}
	if rej != nil {
		// A duplicate is refused with an id of its own (Saleem, 28 Oct
		// 2026): 0x408 with the new id, a REJECTD receipt on it when the
		// client wants receipts, both logged (rejects_, dlr_).
		var refusedID string
		if rej.Duplicate {
			refusedID = msg.ID
			msg.AccountID = acct.ID
			p.refuseDuplicate(ctx, msg, acct, nil)
		}
		p.recordReject(msg, acct, rej, s.ip) // [SCOPE P1-REJECTLOG] parity (2026-07-26)
		// The pipeline already captured WHY; this captures what the client was
		// actually told, which is the only half they can see.
		p.captureSubmitResp(logging.New(p.Name()), s.acctName(), seq, statusFor(rej.Reason), refusedID, rej.Reason)
		s.send(pdu.SubmitSMRespPDU(statusFor(rej.Reason), seq, refusedID))
		return
	}
	id := msg.ID
	p.captureSubmitResp(logging.New(p.Name()), s.acctName(), seq, pdu.ESME_ROK, id, "")
	s.send(pdu.SubmitSMRespPDU(pdu.ESME_ROK, seq, id))
}

// refuseDuplicate sends a refused duplicate's REJECTD receipt to every id its
// client was given (ids; nil = the message's own) when it wants receipts,
// and notes it on m so its refusal row is filed with that receipt's dlr_ row
// (dlr.RefuseDuplicate; Saleem, 28 Oct 2026). The client is bound here.
func (p *Process) refuseDuplicate(ctx context.Context, m *model.Message, acct *refdata.Account, ids []string) {
	if err := dlr.RefuseDuplicate(ctx, p.receipts, m, acct.NoReceipts, ids, deployment.Owner()); err != nil {
		logging.New(p.Name()).Error(p.acctScope(acct.ID), "receipt_enqueue_failed",
			"could not queue a refused duplicate's REJECTD receipt",
			logging.Fields{"messageId": m.ID, "error": err.Error()})
	}
}
