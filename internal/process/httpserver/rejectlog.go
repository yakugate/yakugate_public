package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/rejectlog"
)

// credentialRelated reports whether an auth reject was a genuine CREDENTIAL
// failure (bad password/token/signature/unknown user) rather than a valid
// credential that failed for another reason (disabled/locked/ip). Only the
// former may be captured (P1 scope finalize 2026-08-09): a valid live
// credential must never be persisted. 401 AUTH_FAILED is the credential class;
// 403 (account state) and 423/429 (lockout/throttle — the credential may have
// been correct) are NOT captured.
func credentialRelated(rej *pipeline.Reject) bool {
	return rej != nil && rej.Code == 401
}

// captureAuthFailure persists the presented credentials to the reject log when
// (1) the failure was credential-related, (2) the presented username resolves
// to a real account, and (3) that account has store_auth_data on. Best-effort
// and fire-and-forget; the response is never affected. The account resolution
// here is server-side only (to read the flag) and never surfaces, so it cannot
// be used to enumerate valid usernames.
func (p *Process) captureAuthFailure(r *http.Request, fields map[string]any, rej *pipeline.Reject) bool {
	if p.rejects == nil || !credentialRelated(rej) {
		return false
	}
	user := fieldOrHeader(r, fields, "username")
	if user == "" {
		return false // no username presented (e.g. token-only) -> nothing to key the account on
	}
	acct, ok := p.ref.AccountByUsername(user)
	if !ok || !acct.StoreAuthData {
		return false
	}
	// Capture the presented credential material. Deliberately minimal and only
	// under the account's explicit opt-in.
	captured, _ := json.Marshal(map[string]string{
		"presented_username":  user,
		"presented_password":  fieldOrHeader(r, fields, "password"),
		"auth_header_present": boolStr(r.Header.Get("yaku-sms-key") != "" || r.Header.Get("Authorization") != ""),
	})
	m := &model.Message{
		AccountID:    acct.ID,
		Username:     acct.Username,
		Status:       model.StatusRejected,
		RejectReason: rejectCode(rej.Reason) + ": " + refusalDetail(rej.Reason),
		StatusCode:   rej.Code,
		IPAddress:    p.ipres.FromRequest(r),
		Ingress:      model.TransportHTTP,
		RequestInfo:  string(captured),
	}
	p.putReject(*m)
	return true
}

// fieldOrHeader reads a value from the request header, then the parsed fields
// (body/query), case-insensitively — the same sources auth itself consults.
func fieldOrHeader(r *http.Request, fields map[string]any, name string) string {
	if v := strings.TrimSpace(r.Header.Get(name)); v != "" {
		return v
	}
	if v := strings.TrimSpace(r.URL.Query().Get(name)); v != "" {
		return v
	}
	for k, v := range fields {
		if strings.EqualFold(k, name) {
			if s, ok := v.(string); ok {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// RejectWriter persists a refused submit. Implemented by the MySQL hot store;
// nil on the Process disables reject logging entirely (other backends, or
// YAKU_LOG_REJECTS=off), which is why every call site tolerates a nil writer.
type RejectWriter interface {
	WriteRejected(ctx context.Context, m model.Message) error
}

// The write itself is bounded by rejectlog.WriteTimeout, and the number of
// writes in flight by rejectlog.Workers: a sick database or a storm of
// refusals cannot pile up goroutines or connections. The client already has
// its response; losing a reject row under pressure is strictly better than
// degrading the accept path.

// recordReject persists one rejected submit for support/audit (proposal S-01).
//
// Deliberately fire-and-forget: the HTTP response is already decided, so this
// runs on its own goroutine with its own timeout and its own context — never
// the request's, which is cancelled the moment the response is written. A
// failure is logged and dropped; it must never turn a clean 4xx into a 5xx.
//
// Only SANITIZED request context is stored (pipeline.RequestInfoJSON): an
// allow-list of headers, never credentials. See saleem-hadal-db-review.md §6.0
// for the reference gateway's habit of persisting live customer tokens.
func (p *Process) recordReject(src *model.Message, acct *refdata.Account, rej *pipeline.Reject, ip string, r *http.Request) {
	if p.rejects == nil || src == nil {
		return
	}
	m := *src // copy: the caller's envelope must not be mutated by logging
	m.Status = model.StatusRejected
	// "CODE: what the client was told": a bare code is stored with the words
	// its reply carried (dialect.RefusalText), so reason_text holds the
	// sentence the client really saw (2026-10-02, found by the CLI e2e run).
	m.RejectReason = rejectCode(rej.Reason) + ": " + refusalDetail(rej.Reason)
	// Its OWN column, never appended to the reason: reason_code is what an
	// operator groups and counts by, so "SPAM_KEYWORD (spam rule: …)" would
	// quietly stop matching WHERE reason_code='SPAM_KEYWORD'.
	m.RejectAudit = rej.Audit
	m.StatusCode = rej.Code
	m.IPAddress = ip
	m.Ingress = model.TransportHTTP
	m.RequestInfo = pipeline.RequestInfoJSON(r.Header, r.Method, r.URL.Path)
	if acct != nil {
		m.AccountID = acct.ID
		m.CompanyID = acct.CompanyID // the refusal's table (D-136)
		m.Username = acct.Username
	}
	// The refused text, as the company's privacy keeps it (hot/0057).
	pipeline.KeepRejectText(&m, acct)

	p.putReject(m)
}

// recordProvenRefusal counts and records a submit refused because of the
// ACCOUNT's state — switched off, suspended (by an operator, its company, or
// the abuse monitor), expired, portal-only — after its credentials were
// proven (Reject.Proven).
//
// Until 2026-10-06 such a refusal over HTTP left no trace: no reject row, no
// rejected count on the account, nothing in the rejected rate — while the
// same account refused on an SMPP bind was recorded in full. An operator who
// suspended a customer saw its SMPP traffic turned away and its HTTP traffic
// simply vanish (load test G2: 487 refusals over HTTP, 0 recorded; 520 over
// SMPP, 520 recorded).
//
// One row per recipient, as the pipeline writes them, up to the recipient
// cap; a request we cannot read still gets one row saying it was refused.
// Nothing here changes the reply, which is the same 403 as before.
func (p *Process) recordProvenRefusal(r *http.Request, fields map[string]any, rej *pipeline.Reject) {
	if rej == nil || rej.Proven == "" || p.ref == nil {
		return
	}
	acct, ok := p.ref.AccountByUsername(rej.Proven)
	if !ok || acct == nil {
		return
	}
	var msgs []*model.Message
	if req, code, _ := finishRequest(r, fields, acct); code == 0 && len(req.Messages) <= p.maxBatch {
		msgs = expandRequest(req, acct)
	}
	if limit := p.maxRcpt; limit > 0 && len(msgs) > limit {
		msgs = msgs[:limit]
	}
	if len(msgs) == 0 {
		msgs = []*model.Message{{}}
	}
	ip := p.ipres.FromRequest(r)
	for _, m := range msgs {
		if p.stats != nil {
			p.stats.Message(acct.Username, 1, false)
			p.stats.NoteAccountResult(acct.Username, rej.Code, "http")
		}
		p.recordReject(m, acct, rej, ip, r)
	}
	// And into a flow capture running on the account — unless the request
	// named the account itself (username + password), which captureAuthReject
	// already writes; a token names nobody, so those showed nothing.
	if fieldOrHeader(r, fields, "username") == "" {
		p.captureEdgeReject(r, acct.Username, rejectCode(rej.Reason), rej.Reason, rej.Code)
	}
}

// putReject hands one reject record to the bounded queue (rejectlog), never
// a goroutine and a connection per refusal: one account refused 2000 times a
// second kept the hot store's whole pool busy writing these (2026-10-06).
func (p *Process) putReject(m model.Message) {
	if p.rejects == nil {
		return
	}
	p.rejectOnce.Do(func() {
		p.rejectQ = rejectlog.New(p.rejects, func(m model.Message, err error) {
			p.log.Error(logging.SystemScope, "reject_log", "reject log write failed",
				logging.Fields{"messageId": m.ID, "account": m.Username, "reason": m.RejectReason, "error": err.Error()})
		})
		if p.abuseCounts != nil {
			p.rejectQ.Count(p.abuseCounts)
		}
		p.rejectSeen.Store(p.rejectQ)
	})
	p.rejectQ.Put(m)
}
