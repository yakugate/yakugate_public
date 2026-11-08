package smppvendor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// The status-code actions a vendor's submit answer can ask for, beyond
// success / retry / permanent (PO 2026-09-15: "build all four").
//
//   failover           — this vendor cannot deliver this message: it goes to
//                        the re-route inbox and the mover sends it via the
//                        next route (none: it fails with the vendor's code).
//   hlr_retry          — the vendor says the network is wrong: to the inbox,
//                        where the network is looked up again first.
//   pause_vendor       — the vendor is paused (an operator switches it back
//                        on), operators are told, and the message goes to the
//                        inbox to move on like everything else waiting for it.
//   block_destination  — the number goes on the sending account's blocklist,
//                        no expiry, and the message fails.
//   throttle           — handled as retry (the vendor is busy).
//
// A long message that went out in several parts is never sent another way:
// some parts may already have been accepted, and sending the whole message
// again through another vendor would put a duplicate fragment on the handset.
// Such a message fails with the code instead (the vendor is still paused, or
// the number still blocked, when that is what the code asked).

// codeTimeout bounds the store writes a code action makes on the reader path.
const codeTimeout = 5 * time.Second

// handOff puts m in the re-route inbox and takes it off this vendor's queue.
// false = it could not be sent another way; the caller fails it.
func (p *Process) handOff(v vendorConn, m model.Message, action, code string, wireParts int) bool {
	lg := p.log.ForVendor(v.cid)
	if wireParts > 1 {
		lg.Infof("status_code", "message %s: %s asked %s with %s, but it went out in %d parts; not sent another way — it fails",
			m.ID, v.cid, action, code, wireParts)
		return false
	}
	if m.CodeReroute != "" && action != model.ActionPauseVendor && action != model.ActionNext {
		prev, prevCode, prevVendor := model.ParseRerouteAsk(m.CodeReroute)
		lg.Infof("status_code", "message %s: %s asked %s with %s, but %s already sent it another way (%s, %s); a message moves by a code once — it fails",
			m.ID, v.cid, action, code, prevVendor, prev, prevCode)
		return false
	}
	enq, ok := p.queue.(interface {
		Enqueue(context.Context, model.Message) error
	})
	if !ok {
		lg.Warnf("status_code", "message %s: this queue store cannot take a re-route; %s with %s fails it", m.ID, action, code)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), codeTimeout)
	defer cancel()
	in := m
	in.Vendor = model.RerouteInbox
	in.Status = model.StatusQueued
	in.CodeReroute = model.RerouteAsk(action, code, v.cid)
	// What the vendor answered, in words, for the record if its rounds run
	// out (queue/0041): "the vendor refused the submit: ESME_RTHROTTLED (0x00000058)".
	in.ErrorCode, in.ErrorText = code, refusalText(code)
	if err := enq.Enqueue(ctx, in); err != nil {
		lg.Warnf("status_code", "message %s: could not hand it to the re-route inbox: %v; it fails", m.ID, err)
		return false
	}
	if err := p.ack(ctx, v.cid, m.ID); err != nil {
		// It is now in the inbox AND still on this queue: it may be sent
		// twice. Loud, because it needs a person.
		lg.Warnf("status_code", "message %s: handed to the re-route inbox but could not be taken off %s's queue: %v — it may be sent twice",
			m.ID, v.cid, err)
	}
	lg.Infof("status_code", "message %s: %s answered %s (%s); handed to the re-route inbox", m.ID, v.cid, code, action)
	return true
}

// pauseVendorByCode switches the vendor off, as an operator's pause does, and
// says so to operators. Only an active vendor is paused; the pause lasts until
// someone switches it back on.
func (p *Process) pauseVendorByCode(v vendorConn, code, msgID string) {
	lg := p.log.ForVendor(v.cid)
	if p.traffic == nil {
		lg.Warnf("status_code", "%s answered %s (pause_vendor) but this process cannot change vendors", v.cid, code)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), codeTimeout)
	defer cancel()
	acts := p.traffic.CodeActions()
	n, err := acts.PauseVendor(ctx, v.id)
	if err != nil {
		lg.Warnf("status_code", "%s answered %s (pause_vendor) but it could not be paused: %v", v.cid, code, err)
		return
	}
	if n == 0 {
		return // already paused (another answer got here first)
	}
	det, _ := json.Marshal(map[string]any{"code": code, "msg_id": msgID, "action": "pause_vendor",
		"why": "the vendor answered a submit with a code mapped to pause_vendor"})
	if err := acts.AuditVendorPaused(ctx, v.cid, string(det)); err != nil {
		lg.Warnf("status_code", "%s paused by its code %s, but the audit row was not written: %v", v.cid, code, err)
	}
	logging.New("smpp-vendor").Warn(logging.SystemScope, "vendor_paused_by_code",
		"a vendor was paused by its own status code; switch it back on when it is fixed",
		logging.Fields{"vendor": v.cid, "code": code, "msgId": msgID})
}

// blockByCode adds the number to the sending account's blocklist, with no
// expiry (PO 2026-09-15), unless an active block for it is already there.
func (p *Process) blockByCode(v vendorConn, m model.Message, code string) {
	lg := p.log.ForVendor(v.cid)
	if p.traffic == nil || m.Destination == "" || m.AccountID == 0 {
		lg.Warnf("status_code", "message %s: %s asked to block %s with %s, but it could not be recorded here", m.ID, v.cid, m.Destination, code)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), codeTimeout)
	defer cancel()
	reason := "vendor " + v.cid + " answered " + code + " (block_destination)"
	acts := p.traffic.CodeActions()
	n, err := acts.BlockForAccount(ctx, m.Destination, m.AccountID, reason)
	if err != nil {
		lg.Warnf("status_code", "message %s: could not block %s for account %d: %v", m.ID, m.Destination, m.AccountID, err)
		return
	}
	if n == 0 {
		return // already blocked for this account
	}
	det, _ := json.Marshal(map[string]any{"msisdn": m.Destination, "code": code, "vendor": v.cid, "msg_id": m.ID})
	if err := acts.AuditBlockAdded(ctx, m.AccountID, m.Destination, string(det)); err != nil {
		lg.Warnf("status_code", "blocked %s for account %d, but the audit row was not written: %v", m.Destination, m.AccountID, err)
	}
	lg.Infof("status_code", "message %s: %s answered %s — %s is now blocked for account %d", m.ID, v.cid, code, m.Destination, m.AccountID)
}

// classify resolves a submit_sm_resp command_status to "success"/"retry"/
// "permanent", consulting the vendor's vendor_status_codes overrides before
// falling back to the standard SMPP defaults (item 3.2: use status codes).
func (p *Process) classify(vendorID int64, status uint32) string {
	// The vendor's own rows win first, ESME_ROK included (PO 2026-09-17,
	// "override with warning"): a vendor that answers OK to everything and
	// reports the real outcome elsewhere can have its OK turned into retry
	// or failure. The Admin API and the popup warn when such a row is saved,
	// because a mistaken one fails every message. Until then a code-0 row
	// could never fire (TC-P3-61).
	if action, ok := p.statusCodeAction(vendorID, "submit", status); ok {
		return action
	}
	if status == pdu.ESME_ROK {
		return "success"
	}
	if status == pdu.ESME_RTHROTTLED {
		return "retry"
	}
	return "permanent"
}

// loadStatusCodes refreshes the per-vendor command_status/DLR-code override
// table (item 3.2) so onSubmitResp's classify() reflects the latest config
// without a process restart.
// loadStatusCodes refreshes the shared vendor_status_codes cache so
// onSubmitResp's classify() reflects the latest config without a restart.
//
// Delegates to internal/vendorstatus rather than keeping its own map. The
// private loader this replaces scanned `code` into a uint32 while the column
// is VARCHAR, so the only row anyone had configured — vendor 16, submit,
// "0x256" — failed to scan and was dropped by a bare `continue`. The override
// was in the table, was never loaded, and nothing anywhere said so.
func (p *Process) loadStatusCodes(ctx context.Context) {
	if p.vstatus == nil {
		return
	}
	if err := p.vstatus.Reload(ctx); err != nil {
		p.log.Printf("vendor status codes: %v", err)
	}
	// The accounts' receipt policies ride the same refresh: both decide what
	// a submit answer means, and they must not drift apart.
	if p.policies != nil {
		if err := p.policies.Reload(ctx); err != nil {
			p.log.Printf("%v", err)
		}
	}
}

// statusCodeAction is the submit-leg lookup. Not gated by the per-vendor
// opt-in: that ruling covers the RECEIPT leg, and gating this would silently
// switch off overrides an operator already relies on.
func (p *Process) statusCodeAction(vendorID int64, scope string, code uint32) (string, bool) {
	if p.vstatus == nil || scope != "submit" {
		return "", false
	}
	r, ok := p.vstatus.Submit(vendorID, code)
	return r.Action, ok
}
