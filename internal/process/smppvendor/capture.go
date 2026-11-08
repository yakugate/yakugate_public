package smppvendor

// Process 13 Type 3 — the SMPP vendor leg.
//
// P4 (the HTTP vendor) has captured its vendor exchange since the feature
// landed; this side had nothing at all. So an operator capturing an account
// whose traffic routes over SMPP saw the submit, every pipeline stage, the
// reply we sent the customer and the receipt when it arrived — and NOTHING
// about the submit_sm we put on the wire or the submit_sm_resp the vendor
// answered with. That is the middle of the journey, and the part a capture is
// usually started to look at.
//
// Two captures, as on the HTTP side: the exchange belongs both to the account
// whose message it is and to the vendor an operator may be watching. When
// neither is running this costs one atomic load.

import (
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// vendorCapture holds the (up to) two captures one vendor exchange belongs to,
// resolved once at send time and reused for the response.
type vendorCapture struct {
	acct   logging.Capture
	vendor logging.Capture
	acctOn bool
	vendOn bool
}

func (p *Process) captureFor(accountID int64, cid string) vendorCapture {
	var c vendorCapture
	c.acct, c.acctOn = logging.ActiveForAccountID(accountID)
	c.vendor, c.vendOn = logging.ActiveForVendor(cid)
	return c
}

func (c vendorCapture) on() bool { return c.acctOn || c.vendOn }

// write sends one step to whichever captures are running.
func (c vendorCapture) write(log *logging.Logger, step, msg string, f logging.Fields) {
	if c.acctOn {
		logging.CaptureTo(c.acct, log, step, msg, f)
	}
	if c.vendOn {
		logging.CaptureTo(c.vendor, log, step, msg, f)
	}
}

// captureSubmit records one submit_sm as it goes on the wire.
//
// `body` is the DECRYPTED text actually being sent — the carve-out this mode
// exists for (migration config/0084). Without it a capture cannot answer why a
// vendor rejected a message, which is the commonest reason to start one.
//
// Recorded per PDU, not per message: a long message is several submit_sm on
// the wire with their own sequence numbers, and a receipt correlates per PDU,
// so an operator tracing one has to see the same units the vendor did.
func (p *Process) captureSubmit(c vendorCapture, m model.Message, cid string, text string, wireBytes int, seq uint32, part, parts int, dataCoding byte) {
	if !c.on() {
		return
	}
	// The READABLE text, not the PDU payload.
	//
	// The first version recorded the encoded short_message — packed GSM-7 or
	// UCS-2 bytes — which is not text in any encoding and wrote raw bytes into
	// a JSON log file that then would not parse. An operator reading a capture
	// wants the message; the wire form is described by its size and
	// data_coding, which are here instead.
	f := logging.Fields{
		"messageId":  m.ID,
		"vendor":     cid,
		"from":       m.Sender,
		"to":         m.Destination,
		"text":       text,
		"wireBytes":  wireBytes,
		"sequence":   seq,
		"dataCoding": dataCoding,
	}
	msg := "submit_sm to vendor"
	if parts > 1 {
		f["part"], f["parts"] = part, parts
		msg = "submit_sm to vendor (part of a long message)"
	}
	c.write(p.log, "vendor_request", msg, f)
}

// captureSubmitResp records what the vendor answered.
//
// The command_status is rendered as its spec name: a bare 0x58 in a diagnostic
// file is a lookup nobody should have to do. `action` is our own reading of
// that status — success, retry or permanent — after the vendor's own
// vendor_status_codes overrides have been applied, because "the vendor said
// 0x45 and we treated it as retryable" is the pair an operator needs, not
// either half alone.
func (p *Process) captureSubmitResp(c vendorCapture, m model.Message, cid string, status uint32, vendorMsgID, action string, latencyMs int64, part, parts int) {
	if !c.on() {
		return
	}
	f := logging.Fields{
		"messageId": m.ID,
		"vendor":    cid,
		"status":    pdu.StatusName(status),
		"action":    action,
		"latencyMs": latencyMs,
	}
	if vendorMsgID != "" {
		f["vendorMsgId"] = vendorMsgID
	}
	if parts > 1 {
		f["part"], f["parts"] = part, parts
	}
	msg := "vendor accepted"
	switch action {
	case "retry":
		msg = "vendor asked us to slow down or try again"
	case "permanent":
		msg = "vendor refused"
	}
	c.write(p.log, "vendor_response", msg, f)
}
