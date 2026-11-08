// The vendor's answers to submit_sm: sent, tried again or refused, and what
// the account's receipt policy tells the client.

package smppvendor

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/giveup"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/vendorquality"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
	"github.com/yakugate/yakugate/lib/smpp/window"
)

// onSubmitResp correlates a submit_sm_resp: on success write the MDR
// (status=sent) with the vendor message id and ack the queue; on a
// vendor-classified retry code leave it to retry; on a permanent error
// record a failure and ack.
func (p *Process) onSubmitResp(ctx context.Context, v vendorConn, key sessionKey, win *window.Window[model.Message], pd *pdu.PDU, outc chan<- outcome) {
	v = p.withLive(v)
	pend, ok := win.Ack(pd.Header.SequenceNumber)
	if !ok {
		return
	}
	if !pend.TimedOut.IsZero() {
		// After its timeout, but its message was held for it (leasehold.go):
		// this is its answer, and the message is not sent again.
		p.log.ForVendor(v.cid).Infof("submit_late", "session %d msg=%s answered %s after it was sent (its timeout had passed); taken as its answer",
			key.Idx, pend.Msg.ID, time.Since(pend.SentAt).Round(time.Millisecond))
	}
	m := pend.Msg
	m.VendorID = v.id
	// How long it waits for its receipt, as its vendor says (D-136): its
	// record and its waiting reference are kept for that long.
	m.ReceiptWaitHours, m.VendorSendsNoReceipts = v.receiptWaitHours, v.sendsNoReceipts
	m.VendorFirstReceiptCloses = v.firstReceiptCloses
	action := p.classify(v.id, pd.Header.CommandStatus)
	now := time.Now()
	ans := answerOf(v, key, pend, pd, action, now)
	// A multi-PDU send gets one vendor id and one receipt PER PDU. Register every
	// PDU so no receipt is dropped and the group status can be aggregated (D-037).
	if pend.WireParts > 1 && action == "success" {
		if vmid := ans.VendorMsgID; vmid != "" {
			if p.svc != nil && p.svc.HasFacts() {
				// With Redis the part tracker is in the main Redis (Saleem,
				// 28 Oct 2026): each part's id is filed with the message's
				// facts at once, and whichever gateway gets the receipt of the
				// part that completes the message closes it. Nothing in MySQL.
				if !v.sendsNoReceipts {
					// Priced first, booking nothing: the part's facts carry the
					// note its message's final books from (BUG-RR-01). The
					// message's own answer books once, in decide.
					priced := m
					p.bill.PriceOnly(ctx, &priced, v.id, true)
					p.notePartSent(ctx, v.id, vmid, priced, pend.WirePart, pend.WireParts)
				}
			} else if err := p.hot.RecordConcatPart(ctx, v.id, vmid, m.Ref(), pend.WirePart, pend.WireParts); err != nil {
				p.log.Printf("register concat part %d/%d of %s: %v", pend.WirePart, pend.WireParts, m.ID, err)
			}
		}
	}
	// [SCOPE P13-CAPTURE] Every PDU's response is recorded, lead or not: a
	// long message is several submit_sm on the wire and the vendor answers
	// each one, so an operator tracing a failure has to see the same units the
	// vendor did. Resolved here rather than carried from the send, because the
	// response arrives later on the reader goroutine with only the pending
	// window entry in hand.
	cap := p.captureFor(m.AccountID, v.cid)
	p.captureSubmitResp(cap, m, v.cid, pd.Header.CommandStatus,
		ans.VendorMsgID, action,
		now.Sub(pend.SentAt).Milliseconds(), pend.WirePart, pend.WireParts)

	p.countResp(key, pd.Header.CommandStatus)

	// A long message is recorded once every part has answered (parts.go,
	// issue 14): a later part's answer waits with its message's group.
	book := p.partsBook(key)
	if !pend.IsLead() {
		if action != "success" {
			p.log.Printf("concat PDU %d/%d of %s failed: SMPP_%02X", pend.WirePart, pend.WireParts, m.ID, pd.Header.CommandStatus)
		}
		d, loose := book.answer(pend, ans)
		if loose {
			outc <- outcome{loose: []model.SubmitAnswer{ans}}
			return
		}
		if d != nil {
			p.recordParts(ctx, v, key, m, d, outc)
		}
		return
	}
	latencyMs := now.Sub(pend.SentAt).Milliseconds()
	if pend.WireParts > 1 && action == "success" {
		if d, found := book.accepted(pend, ans, leadAnswer{pend: *pend, vmid: ans.VendorMsgID, latencyMs: latencyMs}); found {
			if d != nil {
				p.recordParts(ctx, v, key, m, d, outc)
			}
			return
		}
	}
	answers := []model.SubmitAnswer{ans}
	if pend.WireParts > 1 && action != "success" {
		// The first part decided it: every answer its parts gave so far
		// goes with that decision, and those still to come are rows of
		// their own.
		answers = book.decidedByLead(pend, ans)
	}
	p.decide(ctx, v, key, m, pd.Header.CommandStatus, action, ans.VendorMsgID, latencyMs, pend.WireParts, answers, outc)
}

// recordParts records a long message whose parts have all answered: sent,
// when every part was accepted or had no answer; refused at submission, with
// the first part that was not taken, when any one was not (issue 14: "one
// failed packet means the whole message goes to rejected, but keep record of
// each packet's information").
func (p *Process) recordParts(ctx context.Context, v vendorConn, key sessionKey, m model.Message, d *partsDone, outc chan<- outcome) {
	if d.refused == "" {
		p.decide(ctx, v, key, m, pdu.ESME_ROK, "success", d.lead.vmid, d.lead.latencyMs, d.lead.pend.WireParts, d.answers, outc)
		return
	}
	p.bill.VendorAnswered(ctx, &m, v.id, false)
	p.failSubmitAs(key, v, m, d.refused,
		fmt.Sprintf("part %d of %d: %s", d.refusedPart, d.lead.pend.WireParts, refusalText(d.refused)), outc, d.answers)
}

// partsBook is a session's long messages waiting for their parts' answers.
func (p *Process) partsBook(key sessionKey) *partsBook {
	b, _ := p.partBooks.LoadOrStore(key, newPartsBook())
	return b.(*partsBook)
}

// decide is what the vendor's answer to a message does: sent, sent another
// way, tried again, or refused. answers are the vendor's answers that go
// with it (sub_); vmid is the vendor's id for the message (its first part's).
func (p *Process) decide(ctx context.Context, v vendorConn, key sessionKey, m model.Message, status uint32, action, vmid string,
	latencyMs int64, wireParts int, answers []model.SubmitAnswer, outc chan<- outcome) {
	// The vendor answered: its side at its charge point, an account charged
	// on submit, a difference in parts — booked now, and the record's price
	// put on the message (billing.Moments.VendorAnswered).
	p.bill.VendorAnswered(ctx, &m, v.id, action == "success")
	switch action {
	case "success":
		m.Status = model.StatusSent
		p.countAccepted(key, latencyMs) // monitor (item 7)
		p.recordOutcome(v.id, v.cb, true)
		monitor.Verdicts.Sent(v.id, m.AccountID, m.ID) // a receipt is now owed
		// The account may want its client told the message is DONE the moment
		// the vendor takes it (config/0119). The answer is stamped on the
		// message so the record write files it in the same insert as the
		// truth; the receipt itself goes out from the batch writer, after
		// that record exists.
		told := p.tellOnSubmit(ctx, m)
		if vmid == "" {
			// The vendor gave no id: our own stands for it, as http-vendor's
			// vendorRef does, so a receipt that names our id finds the
			// message (Saleem 2026-10-14: "if vendor id is not there then
			// message id").
			vmid = m.ID
		}
		// What this message's receipt will need, filed in Redis NOW — on the
		// goroutine that reads the vendor's answers, so it is there before
		// the receipt behind this answer is read. A message sent as several
		// PDUs has a receipt per PDU and a table that joins them: its
		// receipts keep to the record store.
		// A vendor that sends no receipts has nothing waiting (D-136).
		if wireParts <= 1 && vmid != "" && p.svc.HasFacts() && !v.sendsNoReceipts {
			p.noteSent(ctx, v.id, vmid, told.msg)
		}
		outc <- outcome{msg: told.msg, vendorMsgID: vmid, told: told.outcome, ackAt: time.Now(), answers: answers}
	case "retry", "throttle":
		// A throttle is a pause, not a failure: back off the WHOLE session
		// (item 6), not just this message — pump pauses for throttlePause so
		// we honour the vendor's throttle instead of blasting the next
		// message — and the message is tried again after its gap.
		// throttle is the same thing said another way (PO 2026-09-15); it used
		// to fall to the default below and fail the message.
		//
		// Any other "try again" code sends the message on to the next vendor
		// of its round through the re-route inbox (Saleem 2026-09-30: retries
		// are rounds of Main and backups); a multi-part send cannot move and
		// is tried again here after its gap.
		// Either way the answers are rows of their own: the message goes on.
		outc <- outcome{loose: answers}
		if action == "retry" && status != pdu.ESME_RTHROTTLED {
			if p.handOff(v, m, model.ActionNext, fmt.Sprintf("SMPP_%02X", status), wireParts) {
				return
			}
		}
		p.markThrottled(key)
		// It left its queue when it was taken (issue 22): one more try, back
		// after its gap.
		p.tryAgain(ctx, v, m)
	case model.ActionFailover, model.ActionHLRRetry, model.ActionPauseVendor:
		// Sent another way through the re-route inbox (codeactions.go).
		code := fmt.Sprintf("SMPP_%02X", status)
		if action == model.ActionPauseVendor {
			p.pauseVendorByCode(v, code, m.ID)
		}
		if p.handOff(v, m, action, code, wireParts) {
			outc <- outcome{loose: answers}
			return
		}
		p.failSubmit(key, v, m, code, outc, answers)
	case "block_destination":
		code := fmt.Sprintf("SMPP_%02X", status)
		p.blockByCode(v, m, code)
		p.failSubmit(key, v, m, code, outc, answers)
	default:
		// A refusal is tried again while the message has tries left (Saleem,
		// 27 Oct 2026: "if it has retries then retry else fails it"); its
		// answer is a sub_ row either way. A long message sent as several
		// PDUs is decided by recordParts, not here.
		if wireParts <= 1 && giveup.HasTriesLeft(m, p.maxAttempts(v.cid)) {
			outc <- outcome{loose: answers}
			p.tryAgain(ctx, v, m)
			return
		}
		p.failSubmit(key, v, m, fmt.Sprintf("SMPP_%02X", status), outc, answers)
	}
}

// refusalText is an SMPP refusal code ("SMPP_45") in words, for the record:
// "the vendor refused the submit: ESME_RSUBMITFAIL (0x00000045)".
func refusalText(code string) string {
	n, err := strconv.ParseUint(strings.TrimPrefix(code, "SMPP_"), 16, 32)
	if err != nil || !strings.HasPrefix(code, "SMPP_") {
		return ""
	}
	name := pdu.StatusName(uint32(n))
	if strings.HasPrefix(name, "0x") { // no name for it: the number alone
		return "the vendor refused the submit: " + name
	}
	return fmt.Sprintf("the vendor refused the submit: %s (0x%08X)", name, n)
}

// failSubmit records a message the vendor refused as failed, with its code:
// closed by the gateway's own final receipt (dlr_, source system), with the
// vendor's answers in sub_ (the three-table design, 27 Oct 2026).
func (p *Process) failSubmit(key sessionKey, v vendorConn, m model.Message, code string, outc chan<- outcome, answers []model.SubmitAnswer) {
	p.failSubmitAs(key, v, m, code, refusalText(code), outc, answers)
}

// failSubmitAs is failSubmit with the words the refusal is recorded with.
func (p *Process) failSubmitAs(key sessionKey, v vendorConn, m model.Message, code, text string, outc chan<- outcome, answers []model.SubmitAnswer) {
	m.Status = model.StatusFailed
	// The refusal keeps why: the code, and the status in words.
	m.ErrorCode, m.ErrorText = code, text
	p.countFailed(key) // monitor (item 7)
	p.recordOutcome(v.id, v.cb, false)
	monitor.Verdicts.Final(v.id, m.AccountID, "failed", m.ID) // the vendor refused it outright
	vendorquality.Note(v.id, m.MCC, m.MNC, "failed")
	// The record says failed whatever happens. What the CLIENT hears can be
	// the account's own answer for this refusal code (config/0119) — the
	// commonest being "tell them UNDELIV" in the vendor's place, but a code
	// the operator treats differently gets its own row.
	told := p.tellOnRefusal(context.Background(), m, code)
	outc <- outcome{msg: told.msg, errorCode: code, told: told.outcome, answers: answers}
}

// stamped is a message with the client's version of its outcome written onto
// it, plus that outcome for the caller to send once the record exists.
type stamped struct {
	msg     model.Message
	outcome *receiptpolicy.Outcome
}

// tellOnSubmit applies the account's rule that closes a message the vendor
// has just taken (on_accepted, or a progress rule with a final word).
func (p *Process) tellOnSubmit(ctx context.Context, m model.Message) stamped {
	a, rule := p.policies.For(m.AccountID).Submitted()
	return p.markTold(ctx, stampTold(m, a, rule))
}

// tellOnRefusal applies the account's rule for a refusal code.
func (p *Process) tellOnRefusal(ctx context.Context, m model.Message, code string) stamped {
	a, rule := p.policies.For(m.AccountID).Refused(code)
	return p.markTold(ctx, stampTold(m, a, rule))
}

// markTold keeps "told once" for a final answer the account's policy gave:
// the mark in Redis (the logs are written once, issue 14), so a receipt the
// vendor sends later is recorded and not pushed. Set here, on the goroutine
// that reads the vendor's answers, before that receipt can be read.
func (p *Process) markTold(ctx context.Context, st stamped) stamped {
	if st.outcome != nil && st.outcome.Final() && p.svc != nil {
		p.svc.MarkTold(ctx, st.msg, *st.outcome)
	}
	return st
}

// levels is an account's dlr_levels setting — which intermediate receipts its
// clients get (config/0120). Read from the same per-account cache as the
// receipt policy, so both settings reach this role on one refresh.
func (p *Process) levels(accountID int64) string {
	return p.policies.LevelsFor(accountID)
}

// stampTold decides one answer and writes it onto the message. A nil action
// (no policy, or no rule for this case) leaves the message untouched, so
// everything about a gateway without policies stays exactly as it was.
func stampTold(m model.Message, a *receiptpolicy.Action, rule string) stamped {
	if a == nil {
		return stamped{msg: m}
	}
	o := receiptpolicy.Decide(a, rule, nil)
	if o.Status == "" {
		return stamped{msg: m}
	}
	// A progress word (ACCEPTD, ENROUTE) is a receipt and nothing more: the
	// caller sends it, and the record keeps saying only what really happened
	// (2026-10-12). A final word is what the client was TOLD.
	if o.Final() {
		m.ToldStatus, m.ToldErr, m.ToldBy = o.Status, o.Err, o.Rule
	}
	return stamped{msg: m, outcome: &o}
}

// wantsFailureReceipt is the shared "no receipt, no charge" decision applied
// to a failed submit: only NoReceipt suppresses it — FailureReceiptOnly
// explicitly asks for exactly this one.
func wantsFailureReceipt(m model.Message) bool {
	return model.WantsTerminalReceipt(m.NoReceipt, m.FailureReceiptOnly, model.StatusFailed)
}
