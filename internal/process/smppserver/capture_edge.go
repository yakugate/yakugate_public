package smppserver

// Process 13 Type 3, the SMPP edge's half — the mirror of the HTTP edge's
// capture_edge.go, and there for the same reason.
//
// The shared pipeline captures the submit and every stage outcome, so an
// SMPP message that reaches it is already covered. What no capture could see
// until now is the protocol layer around it: the bind that never succeeded,
// the submit refused on its address plan or by the ingress window, and the
// command_status the client was actually answered with.
//
// That layer is most of what goes wrong on SMPP. A customer saying "my binds
// keep dropping" or "I get an error I don't recognise" produced an entirely
// empty capture, because nothing they were complaining about ever became a
// message.

import (
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// captureBind records the outcome of a bind attempt against the account's
// capture. name is the system_id as presented — an unknown one resolves to no
// capture and records nothing, which is correct: a capture belongs to an
// account, and a bind for an account we do not have is not that account's
// traffic.
//
// The password is never recorded, on success or failure. The capture file's
// carve-out covers message text; it does not cover credentials.
func (p *Process) captureBind(log *logging.Logger, name, mode, ip string, status uint32, slot int, detail string) {
	if name == "" {
		return
	}
	sc := logging.AccountScope(name)
	if !logging.Capturing(sc) {
		return
	}
	f := logging.Fields{
		"account":  name,
		"mode":     mode,
		"clientIp": ip,
		"status":   pdu.StatusName(status),
	}
	if slot > 0 {
		f["bindSlot"] = slot
	}
	msg := "bind accepted"
	if status != pdu.ESME_ROK {
		msg = "bind refused"
		if detail != "" {
			f["reason"] = detail
		}
	}
	logging.CaptureRecord(log, sc, "bind", msg, f)
}

// captureSubmitResp records the submit_sm_resp the client was actually given:
// the command_status name and, on success, the message id it may quote back at
// us later.
//
// This is the SMPP twin of the HTTP edge's rendered-response capture. The
// pipeline records what we DECIDED; a client only ever sees this.
func (p *Process) captureSubmitResp(log *logging.Logger, acctName string, seq uint32, status uint32, msgID, note string) {
	if acctName == "" {
		return
	}
	sc := logging.AccountScope(acctName)
	if !logging.Capturing(sc) {
		return
	}
	f := logging.Fields{
		"account":  acctName,
		"sequence": seq,
		"status":   pdu.StatusName(status),
	}
	if msgID != "" {
		f["messageId"] = msgID
	}
	if note != "" {
		f["reason"] = note
	}
	msg := "answered submit_sm_resp"
	if status != pdu.ESME_ROK {
		msg = "refused submit_sm"
	}
	logging.CaptureRecord(log, sc, "submit_resp", msg, f)
}
