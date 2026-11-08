package httpserver

// Process 13 Type 3, the HTTP edge's half.
//
// The shared pipeline already captures the submit, every stage outcome and the
// accept-or-refuse (internal/pipeline). What it CANNOT capture is a submit it
// never sees: everything the edge turns away before a pipeline run exists.
//
// That is not a small set — an unknown password, the wrong submit URL, a
// malformed priority, too many recipients — and it is the exact set an
// operator is most likely to be chasing when they start a capture. Before
// this, turning a capture on for "my submits are failing" showed an EMPTY
// capture whenever the failure was at the door, which reads as "the gateway
// never received anything" rather than "the gateway refused it here, for this
// reason".
//
// Cost when nothing is being captured: one atomic load per refusal (see
// logging.Capturing). Refusals are not the hot path anyway — an accepted
// submit does not pass through here.

import (
	"net/http"

	"github.com/yakugate/yakugate/internal/dialect"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/refdata"
)

// captureEdgeReject records one pre-pipeline refusal against the account's
// capture, when one is running. username may be empty — nothing is recorded
// then, since a capture is always scoped to a named account.
func (p *Process) captureEdgeReject(r *http.Request, username, code, detail string, status int) {
	if username == "" {
		return
	}
	sc := logging.AccountScope(username)
	if !logging.Capturing(sc) {
		return
	}
	logging.CaptureRecord(p.log, sc, "edge_rejected", "refused at the door: "+detail, logging.Fields{
		"account":  username,
		"code":     code,
		"detail":   detail,
		"status":   status,
		"path":     r.URL.Path,
		"method":   r.Method,
		"clientIp": p.ipres.FromRequest(r),
		// Deliberately NOT the request body: a refusal at the door may carry
		// credentials (some dialects put them in the body), and the capture
		// file's carve-out covers message text, not secrets.
		"stage": "edge",
	})
}

// rejectAuthenticated writes a post-auth, pre-pipeline refusal and captures it
// against the account it belongs to. Every refusal between "we know who you
// are" and "the pipeline ran" should go through here rather than calling
// writeReject directly, so none of them can go missing from a capture again.
func (p *Process) rejectAuthenticated(w http.ResponseWriter, r *http.Request, acct *refdata.Account, status int, code, detail string) {
	var d *dialect.Config
	name := ""
	if acct != nil {
		d, name = acct.Dialect, acct.Username
	}
	p.captureEdgeReject(r, name, code, detail, status)
	writeReject(w, d, status, code, detail)
	p.recordRequestRefusal(r, acct, status, code+": "+detail)
}

// recordRequestRefusal records a send request refused as a whole, outside
// the accept pipeline (Saleem 2026-10-14: "every refusal is logged in
// rejected"): one row, under the account when it is known (its company's
// table), else under company 0. reason is "CODE: detail".
func (p *Process) recordRequestRefusal(r *http.Request, acct *refdata.Account, status int, reason string) {
	if p.rejects == nil {
		return
	}
	p.recordReject(&model.Message{}, acct, &pipeline.Reject{Code: status, Reason: reason}, p.ipres.FromRequest(r), r)
}

// captureAuthReject is the same for a refusal where we do NOT yet have an
// account — only whatever username was presented. It resolves that name
// against reference data purely to find a capture to write to; an unknown name
// simply records nothing.
//
// Nothing here reaches the client, so it cannot be used to tell a valid
// username from an invalid one — the response is the same generic,
// dialect-mapped refusal either way.
func (p *Process) captureAuthReject(r *http.Request, fields map[string]any, rej *pipeline.Reject) {
	user := fieldOrHeader(r, fields, "username")
	if user == "" || p.ref == nil {
		return
	}
	if _, ok := p.ref.AccountByUsername(user); !ok {
		return
	}
	p.captureEdgeReject(r, user, rejectCode(rej.Reason), rej.Reason, rej.Code)
}

// captureResponse records the reply exactly as the client receives it —
// after the account's dialect has renamed fields, remapped codes and chosen
// json/text/xml. The pipeline's "accepted"/"rejected" record says what we
// DECIDED; this says what we SENT, and for a migrated customer those two read
// nothing alike.
//
// Body included: it is the customer's own message coming back, already inside
// the capture file's message-text carve-out (config/0084). Credentials never
// appear in a response, so nothing new is exposed.
func (p *Process) captureResponse(acct *refdata.Account, rendered dialect.Rendered) {
	if acct == nil {
		return
	}
	sc := logging.AccountScope(acct.Username)
	if !logging.Capturing(sc) {
		return
	}
	logging.CaptureRecord(p.log, sc, "response", "replied to client", logging.Fields{
		"account":     acct.Username,
		"status":      rendered.StatusCode,
		"contentType": rendered.ContentType,
		"body":        string(rendered.Body),
	})
}
