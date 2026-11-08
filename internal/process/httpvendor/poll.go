package httpvendor

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/logging"
)

// Status polling: a vendor's messages waiting for a receipt, asked about with
// the vendor's status call.

// pollStatus polls delivery status for this vendor's still-'sent' records when
// the profile uses mode=poll, and applies the result.
func (p *Process) pollStatus(ctx context.Context, v vendorAdapter) {
	if !v.ad.PollMode() {
		return // webhook/none: DLRs arrive by the receiver (P5), not here
	}
	// One process polls a vendor at a time (pollturn.go).
	if !p.myPollTurn(ctx, v.id) {
		return
	}
	if p.polls == nil {
		p.polls = newPollSched()
	}
	// Honor status.interval_sec (closes the P4 backlog "decorative field" gap,
	// 2026-08-19): the VENDOR's report endpoint is hit at its configured
	// cadence, not the worker framework's 500ms Step cycle. The local
	// query still runs each cycle so a fresh 'sent' message's FIRST poll is
	// not delayed by an interval stamped on an idle pass — the stamp lands
	// only when the endpoint is actually called. With no interval set, each
	// message keeps its own back-off instead (pollsched.go). Step is
	// single-threaded, so the maps need no lock.
	iv := v.ad.PollInterval()
	if iv > 0 {
		if last, ok := p.lastPoll[v.id]; ok && time.Since(last) < iv {
			return
		}
	}
	// One page of the vendor's waiting messages, walked in the order they
	// were accepted; each is polled only when its own turn has come
	// (pollsched.go — QA EP-021, SOAP-006).
	now := time.Now()
	rows, err := p.awaitingPage(ctx, v, p.polls.start(v.id, now), pollPage)
	if err != nil {
		return
	}
	// A reassembled concat group has every part stamped with the same vendor id, so
	// poll (and apply) once per id — Apply finalizes the whole group. [D-036]
	done := map[string]bool{}
	calls := 0
	for _, r := range rows {
		if calls >= pollBudget {
			break // the rest of the page next cycle, from this row
		}
		p.polls.read(v.id, r.MsgID)
		if done[r.VendorMsgID] || !p.polls.due(v.id, r.VendorMsgID, now) {
			continue
		}
		done[r.VendorMsgID] = true
		calls++
		p.lastPoll[v.id] = now
		// The vendor's error code comes with the status when its poll settings
		// name one ("error_code"); it is looked up in the vendor's receipt
		// codes and kept on the receipt (PO 2026-09-15).
		st, errCode, ok, err := v.ad.StatusDetail(ctx, r.VendorMsgID)
		// [SCOPE P13-CAPTURE] For a poll-mode vendor the outcome comes from
		// HERE, not from a receipt — so without this a capture on such a
		// vendor showed the send and then nothing, and the status simply
		// changed with no visible cause.
		if c, on := logging.ActiveForAccountID(r.AccountID); on {
			logging.CaptureTo(c, p.log, "vendor_poll", pollMessage(st, ok, err), pollFields(v.cid, r, st, err))
		}
		if c, on := logging.ActiveForVendor(v.cid); on {
			logging.CaptureTo(c, p.log, "vendor_poll", pollMessage(st, ok, err), pollFields(v.cid, r, st, err))
		}
		if err != nil || !ok {
			p.polls.polled(v.id, r.VendorMsgID, now, iv)
			continue
		}
		// Finals only, deliberately, even though the receipt path now keeps
		// every level (2026-09-23). A poll answer is a STATE, not an event:
		// the same ENROUTE comes back on every cycle, so forwarding them
		// would send a client one progress receipt per poll interval for as
		// long as the message is in flight. A vendor whose intermediates are
		// worth having sends them as webhooks, which are events.
		//
		// The vendor's word is read the way a receipt's is (dlr.ParseStat),
		// so DELIVRD, UNDELIV, REJECTD … finish the message without a status
		// map; only "delivered", "failed" and "expired" used to (QA EP-021).
		if !pollFinal(st) {
			p.polls.polled(v.id, r.VendorMsgID, now, iv)
			continue
		}
		// centralized DLR application (D-023): status + raw_dlr + client push
		if err := applyPolled(ctx, p, v.id, r.VendorMsgID, st, errCode); err != nil {
			p.log.ForVendor(v.cid).Warnf("poll", "apply status of %s: %v (polled again later)", r.VendorMsgID, err)
		}
		// Backed off like any other poll, never forgotten here: a final word
		// does not always end the message — a receipt-code rule may set its
		// target status to 'sent' (a "retry" rule), or the write may not
		// land — and a forgotten message that is still 'sent' was due again
		// on the very next Step, twice a second, the flood this pacing
		// exists to stop. A message that did finish leaves the page and is
		// forgotten at the end of the pass (wrap).
		p.polls.polled(v.id, r.VendorMsgID, now, iv)
	}
	if calls < pollBudget && len(rows) < pollPage {
		p.polls.wrap(v.id) // every waiting message seen: start the next pass
	}
}

// awaitingPage is one page of a vendor's messages waiting for a receipt: from
// open_msgs on a gateway without Redis, else from the day tables — a message
// the vendor took (sub_) that no receipt has closed (dlr_). The receipt facts
// kept a waiting set per vendor in the main Redis for this until 29 Oct 2026.
func (p *Process) awaitingPage(ctx context.Context, v vendorAdapter, after string, n int) ([]dal.PendingDLR, error) {
	if p.hot == nil {
		return nil, nil
	}
	if dal.OpenMessagesInSQL() {
		return p.hot.SentAwaitingDLRPage(ctx, v.id, after, n)
	}
	// As far back as its messages wait for a receipt (dlr_config.wait_hours).
	wait := time.Duration(model.DefaultReceiptWaitHours) * time.Hour
	if v.receiptWaitHours > 0 {
		wait = time.Duration(v.receiptWaitHours) * time.Hour
	}
	return p.hot.SentAwaitingDLRInDays(ctx, v.id, after, n, dal.AwaitDays(wait))
}

// pollMessage is the human line for one status poll.
func pollMessage(st string, ok bool, err error) string {
	switch {
	case err != nil:
		return "status poll failed"
	case !ok:
		return "vendor has no status for this message yet"
	default:
		return "vendor reports " + st
	}
}

// pollFields is the same detail for both captures a poll may belong to.
func pollFields(cid string, r dal.PendingDLR, st string, err error) logging.Fields {
	f := logging.Fields{"messageId": r.MsgID, "vendor": cid, "vendorMsgId": r.VendorMsgID}
	if err != nil {
		f["error"] = err.Error()
	} else if st != "" {
		f["vendorStatus"] = st
	}
	return f
}
