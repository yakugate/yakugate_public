package smppserver

// Refused submits: their records, and the command_status a client is told.

import (
	"encoding/json"
	"strings"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/rejectlog"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// recordReject persists one refused SMPP submit — parity with P1-REJECTLOG
// (2026-07-26). Fire-and-forget on its own context, same as the HTTP edge: a
// slow hot store must never delay the submit_sm_resp.
func (p *Process) recordReject(src *model.Message, acct *refdata.Account, rej *pipeline.Reject, ip string) {
	if p.rejects == nil || src == nil {
		return
	}
	m := *src
	m.Status = model.StatusRejected
	// Same shape as the HTTP edge: the operator-only detail rides its own
	// column, never onto the wire. An ESME only sees the command_status.
	m.RejectReason = rej.Reason
	m.RejectAudit = rej.Audit
	m.StatusCode = rej.Code
	m.IPAddress = ip
	m.Ingress = model.TransportSMPP
	// The refused text, as the company's privacy keeps it (hot/0057).
	pipeline.KeepRejectText(&m, acct)
	if info, err := json.Marshal(map[string]string{
		"transport": "smpp", "system_id": acct.SMPPSystemID,
	}); err == nil {
		m.RequestInfo = string(info)
	}
	p.putReject(m)
}

// putReject hands one refusal record to the bounded queue (rejectlog), never
// a goroutine and a connection per refusal: an account held to 10/s and
// sending 2000/s kept the hot store's whole pool busy writing these
// (2026-10-06).
func (p *Process) putReject(m model.Message) {
	if p.rejects == nil {
		return
	}
	p.rejectOnce.Do(func() {
		p.rejectQ = rejectlog.New(p.rejects, func(m model.Message, err error) {
			logging.New(p.Name()).Error(logging.SystemScope, "reject_log", "reject log write failed",
				logging.Fields{"messageId": m.ID, "reason": m.RejectReason, "error": err.Error()})
		})
		if p.abuseCounts != nil {
			p.rejectQ.Count(p.abuseCounts)
		}
		p.rejectSeen.Store(p.rejectQ)
	})
	p.rejectQ.Put(m)
}

// recordSubmitRefusal records a submit refused before the accept pipeline
// could run (Saleem 2026-10-14: "every refusal is logged in rejected") — a
// full window, a bind that cannot send, a submit_sm that cannot be read, a
// destination outside the address plan, a submit_multi. It goes to the same
// table and summary as every refusal: the account's company when the bind
// has one, else company 0. body is the decoded submit when it could be read
// (nil otherwise), for the sender and the number.
func (p *Process) recordSubmitRefusal(s *session, body *pdu.SubmitSMBody, code, detail string) {
	if p.rejects == nil {
		return
	}
	m := model.Message{
		Status:       model.StatusRejected,
		RejectReason: code + ": " + detail,
		Ingress:      model.TransportSMPP,
		Segments:     1,
		IPAddress:    s.ip,
	}
	if body != nil {
		m.Sender, m.Destination = body.SourceAddr, body.DestinationAddr
	}
	if acct := p.liveAccount(s); acct != nil {
		m.AccountID, m.CompanyID, m.Username = acct.ID, acct.CompanyID, acct.Username
		if p.stats != nil {
			p.stats.Message(acct.Username, 1, false)
			p.stats.RejectReason(code)
		}
	}
	p.putReject(m)
}

// reasonCode extracts the reason CODE from a "CODE: detail" reject string —
// same convention httpserver's rejectCode uses, for the shared /status reject
// breakdown (monitor.Stats.RejectReason). [PO 2026-07-27]
func reasonCode(reason string) string {
	if i := strings.Index(reason, ":"); i > 0 {
		return strings.TrimSpace(reason[:i])
	}
	return strings.TrimSpace(reason)
}

// statusFor maps a pipeline reject reason to an SMPP command_status. [SCOPE P2-STATUSMAP]
func statusFor(reason string) uint32 {
	switch {
	case strings.HasPrefix(reason, "INVALID_MSISDN"):
		return pdu.ESME_RINVDSTADR
	case strings.HasPrefix(reason, "INVALID_REQUEST"):
		if strings.Contains(reason, "sender") {
			return pdu.ESME_RINVSRCADR
		}
		return pdu.ESME_RINVMSGLEN
	case strings.HasPrefix(reason, "MESSAGE_TOO_LONG"):
		return pdu.ESME_RINVMSGLEN
	case strings.HasPrefix(reason, "SENDER_NOT_ALLOWED"):
		return pdu.ESME_RINVSRCADR
	case strings.HasPrefix(reason, "INVALID_SENDER"): // oversized / non-GSM7 sender (stage_validate) — was falling to RSUBMITFAIL
		return pdu.ESME_RINVSRCADR
	case strings.HasPrefix(reason, "DESTINATION_BLOCKED"):
		return pdu.ESME_RINVDSTADR
	case strings.HasPrefix(reason, "NOT_WHITELISTED"):
		return pdu.VNotWhitelisted
	case strings.HasPrefix(reason, "COUNTRY_BLOCKED"):
		return pdu.VCountryBlocked
	case strings.HasPrefix(reason, "RATE_LIMIT_EXCEEDED"):
		return pdu.ESME_RTHROTTLED
	case strings.HasPrefix(reason, "INSUFFICIENT_BALANCE"):
		return pdu.VInsufficientBal
	case strings.HasPrefix(reason, "QUOTA_EXHAUSTED"):
		return pdu.VQuotaExhausted
	// Its own code, not VQuotaExhausted: a quota is the account's allowance
	// and an ESME should slow down; this one says only that ONE destination is
	// full, so the right reaction is to move on to other recipients.
	case strings.HasPrefix(reason, "DESTINATION_CAP"):
		return pdu.VDestCapReached
	case strings.HasPrefix(reason, "TEMPLATE_MISMATCH"):
		return pdu.VTemplateMismatch
	case strings.HasPrefix(reason, "SPAM_KEYWORD"):
		return pdu.VSpam
	case strings.HasPrefix(reason, "NO_ROUTE"):
		return pdu.VNoRoute
	case strings.HasPrefix(reason, "HLR_UNRESOLVED"):
		return pdu.VHLRUnresolved
	case strings.HasPrefix(reason, "LOSS_PREVENTED"):
		return pdu.VLossPrevented
	case strings.HasPrefix(reason, "QUIET_HOURS"):
		return pdu.VQuietHours
	case strings.HasPrefix(reason, "DUPLICATE_MESSAGE"):
		return pdu.VIdempotencyConf
	case strings.HasPrefix(reason, "ACCOUNT_DISABLED"): // gap closed 2026-07-26
		return pdu.VAccountDisabled
	case strings.HasPrefix(reason, "IP_NOT_ALLOWED"): // gap closed 2026-07-26
		return pdu.VIPNotAllowed
	// Not a message problem and not a rate problem: the ESME is talking to
	// the wrong gateway (config/0122). A generic submit failure would send an
	// integrator hunting through their payload for a fault that is not there.
	case strings.HasPrefix(reason, "RESTRICTED_DEPLOYMENT"):
		return pdu.VWrongDeployment
	case strings.HasPrefix(reason, "QUEUE_UNAVAILABLE"):
		return pdu.ESME_RMSGQFUL
	case strings.HasPrefix(reason, "ENCODING_MISMATCH"):
		return pdu.ESME_RSUBMITFAIL
	default:
		return pdu.ESME_RSUBMITFAIL
	}
}
