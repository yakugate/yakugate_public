package httpvendor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/crypto"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/vendorquality"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// Single sending: one message per vendor call, a long message's parts sent as
// one call, and the body as the vendor must see it.

// plainBody returns the body as the vendor must see it. Anything never encrypted
// passes straight through, so mixed queues need no flag check. [SCOPE P1-CRYPTO]
func (p *Process) plainBody(ctx context.Context, stored string) (string, error) {
	if p.cry == nil {
		// An encrypted body with no way to open it must never go out: the
		// handset would receive "enc:v1:…" as its text. Refusing leaves the
		// message queued until encryption is fixed or the message expires.
		if crypto.IsCiphertext(stored) {
			return "", errors.New("encrypted body, but encryption failed to start in this process")
		}
		return stored, nil
	}
	return p.cry.Decrypt(ctx, stored)
}

// concatKey identifies one concatenation group within a vendor queue.
func concatKey(m model.Message) string {
	return fmt.Sprintf("%d|%s|%s|%d|%d", m.AccountID, m.Sender, m.Destination, m.ConcatRef, m.ConcatTotal)
}

// deliverGroup reassembles a concat group and sends it as a single vendor call:
// the parts taken this round with the parts still queued (taken now too). If
// parts are still missing they all go back on the queue for partsWait — a
// group that never completes is refused when its validity ends (issue 22).
func (p *Process) deliverGroup(ctx context.Context, v vendorAdapter, taken []model.Message) {
	any := taken[0]
	more, err := p.queue.FetchConcatGroup(ctx, v.cid, any.AccountID, any.Sender, any.Destination,
		any.ConcatRef, any.ConcatTotal)
	parts := withParts(any, append(taken[1:], more...))
	if err != nil {
		p.log.ForVendor(v.cid).Warnf("concat", "concat fetch ref=%d: %v", any.ConcatRef, err)
		p.later(ctx, parts, partsWait)
		return
	}
	if len(parts) < any.ConcatTotal {
		p.log.ForVendor(v.cid).Infof("concat", "concat ref=%d incomplete (%d/%d) — waiting for the rest",
			any.ConcatRef, len(parts), any.ConcatTotal)
		p.later(ctx, parts, partsWait)
		return
	}
	// Decrypt EACH part before joining. Concatenating ciphertexts would produce
	// garbage — each part is sealed independently, so reassembly must happen on
	// plaintext. [SCOPE P1-CRYPTO]
	var sb strings.Builder
	for _, part := range parts {
		plain, err := p.plainBody(ctx, part.Body)
		if err != nil {
			p.log.ForVendor(v.cid).Warnf("decrypt", "decrypt concat part %s: %v (the group is tried again later)", part.ID, err)
			p.tryAgain(ctx, v, parts...)
			return
		}
		sb.WriteString(plain)
	}
	// Each part keeps its OWN segment count (1). Billing sums across the group's
	// records, so an N-part message costs N segments — the same as it would over
	// SMPP egress. Do NOT also stamp the lead with N: P9 rates every record, so
	// that would charge N + (N-1) segments.
	p.log.ForVendor(v.cid).Infof("concat", "reassembled concat ref=%d: %d parts -> 1 request (%d chars)",
		any.ConcatRef, len(parts), len([]rune(sb.String())))
	p.deliverOne(ctx, v, parts[0], sb.String(), parts, nil) // a long message's parts are filed together, here
}

// deliverOne performs the vendor call for `body`, then records and acks every
// message the call covered (one for a single message, all parts for a group —
// each stamped with the same vendor_msg_id so one receipt finalizes the group).
//
// single, when set, is the round's filer: a send that covers ONE message
// hands it what the vendor answered — taken, or refused for good — and the
// filer files it with the others (filer.go). nil, or a long message sent as
// one call, is filed here.
func (p *Process) deliverOne(ctx context.Context, v vendorAdapter, lead model.Message, body string, cover []model.Message, single *filer) {
	// A single message arrives here still sealed (a group was already decrypted
	// part-by-part in deliverGroup, so its body is plaintext and passes through).
	plain, err := p.plainBody(ctx, body)
	if err != nil {
		p.log.ForVendor(v.cid).Warnf("decrypt", "decrypt %s: %v (tried again later)", lead.ID, err)
		p.tryAgain(ctx, v, cover...)
		return
	}
	fields := map[string]string{
		"to": lead.Destination, "from": lead.Sender, "sender": lead.Sender,
		"text": plain, "msgid": lead.ID, "mcc": lead.MCC, "mnc": lead.MNC,
		// GW-15: the endpoint-model source vocabulary also offers these
		"priority": strconv.Itoa(lead.Priority), "encoding": lead.Encoding,
		"segments": strconv.Itoa(lead.Segments),
		"ap1":      lead.AP1, "ap2": lead.AP2, "apg1": lead.APG1, "apg2": lead.APG2,
	}
	// [SCOPE P13-CAPTURE] The vendor exchange belongs to TWO captures: one an
	// operator started on this account, and one they started on this vendor.
	// Both are checked; when neither is running this costs one atomic load.
	acctCap, acctCapOn := logging.ActiveForAccountID(lead.AccountID)
	vendCap, vendCapOn := logging.ActiveForVendor(v.cid)
	if acctCapOn || vendCapOn {
		f := logging.Fields{
			"messageId": lead.ID, "vendor": v.cid,
			"to": lead.Destination, "from": lead.Sender,
			"body":     plain, // the carve-out: see migration config/0084
			"segments": lead.Segments, "encoding": lead.Encoding,
		}
		if acctCapOn {
			logging.CaptureTo(acctCap, p.log, "vendor_request", "sending to vendor", f)
		}
		if vendCapOn {
			logging.CaptureTo(vendCap, p.log, "vendor_request", "sending to vendor", f)
		}
	}
	// Monitoring the call itself (2026-09-10). Timed around Send only, so the
	// figure is the VENDOR's latency and not ours — decryption, capture
	// writes and record keeping all sit outside it, and an operator asking
	// "is this vendor slow" wants the vendor's number, not the sum.
	p.stats.noteSubmit(v.cid, v.ad.SubmitURL(), lead.Encoding, 1)
	startedAt := time.Now()
	// The call as it went is kept with the answer (hot/0077, vendorcall.go).
	callCtx, calls := httpadapter.WithCallRecorder(ctx)
	vmid, sendErr := v.ad.Send(callCtx, fields)
	took := time.Since(startedAt).Milliseconds()
	answeredAt := time.Now()
	sent := p.sendFactsOf(ctx, calls, cover, startedAt, answeredAt)
	p.noteHealth(v, sendErr)
	if sendErr != nil {
		p.stats.noteResult(v.cid, false, took, sendErr.Error(), 1)
	} else {
		p.stats.noteResult(v.cid, true, took, "", 1)
	}
	if acctCapOn || vendCapOn {
		f := logging.Fields{"messageId": lead.ID, "vendor": v.cid, "vendorMsgId": vmid}
		msg := "vendor accepted"
		if sendErr != nil {
			f["error"], msg = sendErr.Error(), "vendor refused"
		}
		if acctCapOn {
			logging.CaptureTo(acctCap, p.log, "vendor_response", msg, f)
		}
		if vendCapOn {
			logging.CaptureTo(vendCap, p.log, "vendor_response", msg, f)
		}
	}
	// The vendor has answered: from here a record that cannot be written is
	// tried again until this time, never left to the lease at once.
	recordUntil := time.Now().Add(recordRetryFor)
	if sendErr != nil {
		// GW-15: a response the profile classifies as PERMANENT (non-success
		// code outside retryableStatusCodes) will never improve — finalize as
		// failed now instead of burning lease retries until expiry.
		if errors.Is(sendErr, httpadapter.ErrPermanent) {
			p.log.ForVendor(v.cid).Errorf("send", "send %s: %v (permanent — failing message)", lead.ID, sendErr)
			refused(cover, sendErr)
			if cover = p.retryRefused(ctx, v, cover, sendErr, sent); len(cover) == 0 {
				return
			}
			if single != nil && len(cover) == 1 {
				single.add(answered{msg: cover[0], refusal: rejectionCode(sendErr),
					ans: httpAnswer(v, cover[0], "", sendErr, sent)})
				return
			}
			for _, m := range cover {
				v.mark(&m)
				p.bill.VendorAnswered(ctx, &m, v.id, false)
				m.Status = model.StatusFailed
				// What the client is told about this refusal, if the account
				// has a rule for the code (config/0119). Decided BEFORE the
				// record is written, so the truth and the client's version
				// land in the same insert.
				told := p.tellOnRefusal(m, rejectionCode(sendErr))
				m = told.msg
				// Refused for good: the gateway's own final receipt closes
				// the message, with the vendor's answer in sub_.
				acked, e := p.recordWrite(ctx, v.cid, m, recordUntil, httpAnswer(v, m, "", sendErr, sent))
				if e != nil {
					p.log.ForVendor(v.cid).Errorf("record", "record write %s: %v (the vendor refused it; it has NO RECORD)", m.ID, e)
					continue
				}
				if !acked { // with the DB worker the ack was made with the record, in one step
					p.ack(ctx, v.cid, m.ID)
				}
				monitor.Verdicts.Final(v.id, m.AccountID, "failed", m.ID) // the vendor refused it outright
				vendorquality.Note(v.id, m.MCC, m.MNC, "failed")
				if told.outcome != nil && p.svc != nil {
					p.svc.Tell(ctx, dlrsvc.TellFor(m), *told.outcome)
					continue
				}
				// registered_delivery (PO 2026-09-03): only NoReceipt suppresses a
				// failure — FailureReceiptOnly explicitly WANTS this one. The
				// standard word for a vendor's refusal is REJECTD (issue 22).
				if p.svc != nil && model.WantsTerminalReceipt(m.NoReceipt, m.FailureReceiptOnly, model.StatusFailed) {
					p.svc.PushRefusal(ctx, m, rejectionCode(sendErr))
				}
			}
			return
		}
		p.log.ForVendor(v.cid).Warnf("send", "send %s: %v", lead.ID, sendErr)
		failed(cover, sendErr)
		lead.ErrorCode, lead.ErrorText = vendorFailure(sendErr)
		// "Not now": a vendor that charges on submission charges this try.
		for _, m := range cover {
			p.bill.VendorAnswered(ctx, &m, v.id, false)
		}
		// The answer is a row of its own: the message goes on.
		p.writeLooseAnswers(ctx, v.cid, httpAnswers(v, cover, nil, sendErr, sent))
		if p.nextVendor(v, lead, cover, lead.ErrorCode) {
			return
		}
		// It left its queue when it was taken (issue 22): one more try, back
		// after its gap.
		for i := range cover {
			cover[i].ErrorCode, cover[i].ErrorText = lead.ErrorCode, lead.ErrorText
		}
		p.tryAgain(ctx, v, cover...)
		return
	}
	// An accepted answer with no vendor id: the message's own id is the
	// reference (one for every part of a long message, as a vendor's id is).
	if single != nil && len(cover) == 1 {
		// fileChunk notes a missing id and files under our own.
		single.add(answered{msg: cover[0], id: vmid, ans: httpAnswer(v, cover[0], vmid, nil, sent)})
		return
	}
	if vmid == "" {
		p.noteNoID(v, lead.ID)
	}
	vmid = vendorRef(vmid, lead.ID)
	for _, m := range cover {
		v.mark(&m)
		p.bill.VendorAnswered(ctx, &m, v.id, true)
		m.Status = model.StatusSent
		m.VendorMsgID = vmid // in the record with the status, one write (2026-10-09)
		// The account's on_accepted rule, stamped before the write as above.
		told := p.tellOnSubmit(m)
		m = told.msg
		// What its receipt will need, filed before the receipt can come. A
		// long message sent as one call: its parts share the vendor's id, and
		// one receipt closes them all (filed as the id's group).
		group := ""
		if len(cover) > 1 {
			group = dlrsvc.GroupParts
		}
		p.noteSent(ctx, v.id, vmid, m, group)
		acked, e := p.recordWrite(ctx, v.cid, m, recordUntil, httpAnswer(v, m, vmid, nil, sent))
		if e != nil {
			p.log.ForVendor(v.cid).Errorf("record", "record write %s: %v (the vendor took it; it has NO RECORD)", m.ID, e)
			continue
		}
		if !acked { // with the DB worker the ack was made with the record, in one step
			p.ack(ctx, v.cid, m.ID)
		}
		monitor.Verdicts.Sent(v.id, m.AccountID, m.ID) // a receipt is now owed
		// Submit-to-network DLR (PO 2026-08-10): per covered part — a
		// reassembled group notifies once per part ULID.
		// registered_delivery bit 4 (PO 2026-09-03) — WantsIntermediateReceipt
		// keeps HTTP ingress unaffected (no such bit exists there); it only
		// changes behaviour for messages that arrived over SMPP.
		// Or the account's progress receipt in its place (2026-09-29).
		if p.svc != nil {
			if st, d, ok := p.submitNotice(v, m); ok {
				p.svc.PushNotice(ctx, m, st, d)
			}
		}
		if told.outcome != nil && p.svc != nil {
			p.svc.Tell(ctx, dlrsvc.TellFor(m), *told.outcome)
		}
	}
}

// submitNotice is the notice a client gets when this vendor takes its
// message: the account's progress receipt, else the gateway's own ACCEPTD
// (dlr.SubmitNotice — the one rule both vendor roles apply).
func (p *Process) submitNotice(v vendorAdapter, m model.Message) (dlrsvc.Stat, time.Duration, bool) {
	return dlrsvc.SubmitNotice(p.levels(m.AccountID), p.policies.For(m.AccountID),
		m.WantsIntermediateReceipt(), v.ownIntermediateDLR)
}
