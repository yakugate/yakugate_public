package httpserver

import (
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fmt"
	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/dialect"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/hlr"
	"github.com/yakugate/yakugate/internal/mask"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/msisdn"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/refdata"
)

// maxBodyBytes caps the request body; larger submissions get 413. [SCOPE P1-01]
const maxBodyBytes = 64 << 10 // 64 KB

type resultItem struct {
	To        string `json:"to"`
	MessageID string `json:"messageId,omitempty"`
	Status    string `json:"status"`
	Code      int    `json:"code"`
	Reason    string `json:"reason,omitempty"`
	// AcceptedTime is the RFC3339/UTC accept timestamp, present on accepted
	// recipients only (published upstream contract calls this acceptedTime).
	AcceptedTime string `json:"acceptedTime,omitempty"`
}

// handleSend implements POST|GET /sms/v4/send. [SCOPE P1-00 Submit API; P1-RCPT fan-out]
// Both methods are supported deliberately (chunk-1 ruling 2026-07-31, fixing a
// stale "POST-only" claim here): POST carries a JSON/form/XML body; GET carries
// the same fields as query-string parameters — a legacy-client staple the
// dialect layer depends on. Any other method is rejected 405.
func (p *Process) handleSend(w http.ResponseWriter, r *http.Request) {
	// Bound the body and always drain it, even on early rejects — replying while
	// the client is still uploading resets the connection mid-request.
	//
	// This has to come BEFORE the method check. A GET carrying a body is still a
	// client mid-upload, so answering 405 and returning without draining reset the
	// connection intermittently: the client saw a connection error instead of the
	// 405 it was told to expect. The 405 path was the one early reject that
	// skipped the drain this comment already required.
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer drainBody(r)
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		writeReject(w, nil, 405, "METHOD_NOT_ALLOWED", "method not allowed")
		// Recorded when it was aimed at a send path; any other URL is not a
		// submit at all (scanners).
		if p.ready() && p.ref.KnownSendPath(normalizePath(r.URL.Path)) {
			p.recordRequestRefusal(r, nil, 405, "METHOD_NOT_ALLOWED: method not allowed")
		}
		return
	}
	// Degraded, not broken: if reference data has never loaded (config store
	// unreachable since startup) — or loaded but holds zero accounts, so
	// nobody could authenticate — every key would 401 misleadingly; say 503.
	// Same signal as /readyz. [SCOPE P1-READY]
	if !p.ready() {
		writeReject(w, nil, 503, "SERVICE_UNAVAILABLE", "reference data not loaded, retry later")
		p.recordRequestRefusal(r, nil, 503, "SERVICE_UNAVAILABLE: reference data not loaded, retry later")
		return
	}
	// Cheap pre-filter for the catch-all route [SCOPE P1-CUSTOMPATH]: this
	// handler is also mounted at "/" so per-account custom paths (e.g.
	// /api/v1/send) reach it. Reject anything that isn't the native path or
	// SOMEONE's registered custom path before running auth, so scanner/typo
	// traffic against random paths costs a map lookup, not a full auth cycle.
	// This is only a cost filter — the real ownership check happens after auth,
	// once we know which account is actually authenticating.
	path := normalizePath(r.URL.Path)
	if !p.ref.KnownSendPath(path) {
		writeReject(w, nil, 404, "NOT_FOUND", "no such endpoint")
		return
	}
	// Parse the wire body/query ONCE, before auth — some dialects (the Jazz
	// XML API) carry credentials INSIDE the body, so auth needs the same
	// parsed field map finishRequest later dialect-maps, not a second read of
	// an already-drained r.Body. [2026-07-23]
	// The dialect this URL belongs to, resolved BEFORE the body is read: it
	// names the field this client wraps its request in and what its
	// credentials are called, both of which are needed to parse and
	// authenticate (PO 2026-09-16). nil on the shared native path, where no
	// single account owns the URL and the defaults stand.
	pathDialect := p.ref.DialectForPath(path)
	fields, code, reason := parseFields(r, pathDialect)
	if code != 0 {
		writeReject(w, pathDialect, code, rejectCode(reason), rejectDetail(reason))
		p.recordRequestRefusal(r, nil, code, reason) // nobody is known yet: company 0
		return
	}
	acct, rej := p.auth.FromRequest(r, fields) // [SCOPE P1-AUTH] shared internal/auth
	if rej != nil {
		if p.stats != nil {
			p.stats.AuthFail.Add(1)                      // [SCOPE P1-STATUS]
			p.stats.RejectReason(rejectCode(rej.Reason)) // reject breakdown, PO 2026-07-27
			// Per-account status breakdown, but ONLY when the presented
			// username resolves to a real account. Recording an unknown
			// username would let anyone create dashboard rows by guessing
			// names, and would turn a credential-stuffing run into a wall of
			// junk on the operator's screen.
			//
			// A refusal of an account whose credentials were proven is
			// counted below, against that account, whatever scheme it used.
			if u := fieldOrHeader(r, fields, "username"); u != "" && rej.Proven == "" {
				if _, ok := p.ref.AccountByUsername(u); ok {
					p.stats.NoteAccountResult(u, rej.Code, "http")
				}
			}
		}
		// A switched-off, suspended or expired account that showed the RIGHT
		// credentials: refused, and recorded as every other refusal is.
		p.recordProvenRefusal(r, fields, rej)
		// Opt-in credential capture (P1 scope finalize 2026-08-09): if the
		// presented username identifies an account that has store_auth_data on
		// AND this is a genuine credential failure, persist the presented
		// credentials to the reject log for debugging. The response is
		// unchanged (still the generic dialect-mapped reject), so this cannot
		// be used for enumeration — the capture is server-side only.
		captured := p.captureAuthFailure(r, fields, rej)
		// Every other refusal at the door — wrong credentials, a locked login,
		// an address held back, a guessing guard — is recorded too (Saleem
		// 2026-10-14: "every refusal is logged in rejected"): one row, under
		// no account and company 0, as nobody was proven.
		if rej.Proven == "" && !captured {
			p.recordReject(&model.Message{}, nil, rej, p.ipres.FromRequest(r), r)
		}
		// [SCOPE P13-CAPTURE] and the same refusal into a running flow capture,
		// so "my submits are failing" does not show an empty capture when the
		// failure is the credential itself.
		p.captureAuthReject(r, fields, rej)
		// The path alone determines rendering here (refdata.DialectForPath),
		// NOT the credentials that were presented — a custom path is unique to
		// one account, so this cannot be used to tell a valid username from an
		// invalid one; every caller on this URL sees the same shape regardless
		// of what they sent. PO 2026-07-23: dialect follows the URL even when
		// auth itself fails.
		if rej.RetryAfter > 0 { // 423 lockout / 429 IP throttle carry the ladder's remaining time
			w.Header().Set("Retry-After", strconv.Itoa(rej.RetryAfter))
		}
		writeReject(w, pathDialect, rej.Code, rejectCode(rej.Reason), rejectDetail(rej.Reason))
		return
	}
	// Ownership: an account's ONE valid submit path is its own custom path if
	// configured, else the native path (replaces, not additive — PO decision
	// 2026-07-22). Catches both "an account with a custom path used the old
	// native URL" and "account B authenticated on account A's custom path" —
	// both collapse to the same rule, so no separate identity check is needed.
	// 404 rather than 403/401 so the response doesn't confirm this path exists
	// for a DIFFERENT account to someone probing it.
	if path != acct.ExpectedSendPath() {
		// Captured against the account, but still rendered with no dialect: the
		// 404 must not confirm the path exists for someone else.
		p.captureEdgeReject(r, acct.Username, "NOT_FOUND", "wrong submit path for this account", 404)
		writeReject(w, nil, 404, "NOT_FOUND", "no such endpoint")
		p.recordRequestRefusal(r, acct, 404, "NOT_FOUND: wrong submit path for this account")
		return
	}
	req, code, reason := finishRequest(r, fields, acct)
	if code != 0 {
		p.rejectAuthenticated(w, r, acct, code, rejectCode(reason), rejectDetail(reason))
		return
	}
	// messages[] personalized-bulk cap [P1 scope 2026-08-09]: reject the whole
	// request when the array exceeds YAKU_MAX_BATCH (a per-item overflow is a
	// client-side batching error, not something to silently truncate).
	if len(req.Messages) > p.maxBatch {
		p.rejectAuthenticated(w, r, acct, 400, "INVALID_REQUEST", "too many messages in batch")
		return
	}
	msgs := expandRequest(req, acct)
	if len(msgs) == 0 {
		p.rejectAuthenticated(w, r, acct, 400, "INVALID_REQUEST", "no valid recipient")
		return
	}
	// Recipient cap [SCOPE P1-RCPT]: the global YAKU_MAX_RECIPIENTS is a hard
	// system ceiling; a per-account max_recipients may lower it (never raise it).
	cap := p.maxRcpt
	if acct.MaxRecipients > 0 && acct.MaxRecipients < cap {
		cap = acct.MaxRecipients
	}
	if len(msgs) > cap {
		p.rejectAuthenticated(w, r, acct, 400, "INVALID_REQUEST", "too many recipients")
		return
	}

	ctx := r.Context()
	// Trusted-proxy-resolved (chunk-2 ruling 2.5): the same resolver auth used,
	// so the ip-allow stage and the reject log see the identical client IP.
	ip := p.ipres.FromRequest(r)
	now := time.Now().UTC()
	// respDateTime is a SEPARATE format from AcceptedTime's RFC3339 (see
	// dialect.FieldResponseDateTime's doc comment) — computed once so every
	// recipient in a fan-out reports the same instant.
	respDateTime := now.Format("2006-01-02 15:04:05")
	results := make([]dialect.Result, 0, len(msgs))
	// Soonest Retry-After across the fan-out. The header governs the whole
	// response, so the useful number is the EARLIEST moment a retry could
	// achieve anything: with one recipient rate-limited (1s) and another out of
	// monthly quota (weeks), coming back in a second is right — the first will
	// go through. Emitted below only when the whole request was refused.
	// [PO 2026-09-03]
	soonestRetry := 0
	// allRetryable: every recipient was refused with a 429 (or, since PO
	// 2026-09-17, a 503 QUEUE_UNAVAILABLE) that carries a Retry-After. Only
	// then does the header go on a native reply, whose status line is 200
	// by contract (PO 2026-09-11) — a request we partly accepted must never
	// be told to back off.
	allRetryable := true
	for _, m := range msgs {
		// Captured BEFORE the pipeline runs: the enqueue stage overwrites
		// m.Body in place with ciphertext when at-rest encryption is on
		// (internal/pipeline/stage_enqueue.go, [SCOPE P1-CRYPTO]) — m and
		// e.Msg are the same pointer, so reading m.Body after Run would echo
		// the ciphertext back to the client instead of what they submitted.
		origBody := m.Body
		// Per-account masking (PO 2026-09-08): an account on pii_masked gets
		// the echo masked too, so the same value is shown here as is stored.
		// The message itself is untouched — the handset gets the real text.
		if acct.BodyPrivacy == "pii_masked" {
			origBody = mask.Digits(origBody, maskMinDigits())
		}
		// The client's address rides the message to its record (issue 13): the
		// same one the IP allow-list and the reject log see.
		m.IPAddress = ip
		e := &pipeline.Envelope{Msg: m, Account: acct, ClientIP: ip}
		pipeStart := time.Now()
		rej := p.pipe.Run(ctx, e)
		if p.stats != nil {
			// Pipeline latency per recipient (the SLA unit — P1 scope finalize
			// 2026-08-09 §1), feeding the /metrics p95 gauge.
			took := time.Since(pipeStart)
			p.stats.Pipeline.Add(took)
			p.stats.AcceptLatency(acct.Username, took) // per account (PO 2026-09-11)
		}
		if p.stats != nil { // [SCOPE P1-STATUS] per-recipient outcome counters
			p.stats.Message(acct.Username, m.Segments, rej == nil)
			// The HTTP status this recipient's result carried. Recorded per
			// RESULT rather than per request so a fan-out that half-succeeds
			// shows both codes, which is what actually happened.
			code := 200
			if rej != nil {
				code = rej.Code
			}
			p.stats.NoteAccountResult(acct.Username, code, "http")
			if rej != nil {
				p.stats.RejectReason(rejectCode(rej.Reason)) // [SCOPE P1-STATUS] reject breakdown, PO 2026-07-27
			}
		}
		if rej == nil || (rej.Code != 429 && rej.Code != 503) || rej.RetryAfter <= 0 {
			allRetryable = false
		}
		if rej != nil {
			if rej.RetryAfter > 0 && (soonestRetry == 0 || rej.RetryAfter < soonestRetry) {
				soonestRetry = rej.RetryAfter
			}
			// A duplicate is refused with an id of its own (Saleem, 28 Oct
			// 2026): the client is told 409 DUPLICATE_MESSAGE with the new
			// id, gets a REJECTD receipt on it when it wants receipts, and the
			// refusal and that receipt are logged (rejects_, dlr_).
			var refusedID string
			if rej.Duplicate {
				refusedID = m.ID
				m.AccountID = acct.ID
				if err := dlrsvc.RefuseDuplicate(ctx, p.receipts, e.Msg, acct.NoReceipts, nil, deployment.Owner()); err != nil {
					log.Printf("[httpserver] duplicate %s: its REJECTD receipt was not queued: %v", m.ID, err)
				}
			}
			results = append(results, dialect.Result{
				To:               m.Destination,
				MessageID:        refusedID,
				RequestID:        m.RequestID,
				Reference:        m.IdempotencyKey, // the client's own id, handed back under its own name
				CampaignID:       m.CampaignID,
				Accepted:         false,
				StatusCode:       rej.Code,
				Reason:           rejectCode(rej.Reason),
				Detail:           rejectDetail(rej.Reason),
				Sender:           m.Sender,
				Body:             origBody,
				ResponseDateTime: respDateTime,
				ShowMessage:      acct.ShowMessage,
				Segments:         optSegments(m, acct),
			})
			p.recordReject(e.Msg, acct, rej, ip, r)
		} else {
			// A duplicate is a refusal since 28 Oct 2026 (above): what is
			// here was accepted now.
			mid, acceptedAt := m.ID, now
			cost, currency, remBal, bd := p.priceResult(ctx, m, acct, e)
			results = append(results, dialect.Result{
				To:               m.Destination,
				MessageID:        mid,
				RequestID:        m.RequestID,
				Reference:        m.IdempotencyKey, // the client's own id, handed back under its own name
				CampaignID:       m.CampaignID,
				Accepted:         true,
				StatusCode:       200,
				AcceptedTime:     acceptedAt.UTC().Format(time.RFC3339),
				Sender:           m.Sender,
				Body:             origBody,
				ResponseDateTime: respDateTime,
				Cost:             cost,
				Currency:         currency,
				RemainingBalance: remBal,
				Net:              bd.Net,
				Tax:              bd.Tax,
				TaxPercentage:    bd.TaxPercentage,
				Total:            bd.Total,
				ChargedAt:        chargedLater(acct),
				ShowMessage:      acct.ShowMessage,
				Segments:         optSegments(m, acct),
			})
		}
	}

	// One renderer for every response: it applies the account's dialect (field
	// aliases, per-code error overrides, status override, json/text/xml) and
	// owns the uniform-code rule for multi-recipient batches. A nil dialect
	// yields the native contract, so both paths cannot drift apart.
	rendered := dialect.Render(results, acct.Dialect)
	// Only when the WHOLE request was refused for a retryable reason. A 200 or
	// PARTIAL means some recipients were accepted, and telling a client to back
	// off from a request we largely honoured would be actively misleading.
	if soonestRetry > 0 && (rendered.StatusCode == 429 || allRetryable) {
		w.Header().Set("Retry-After", strconv.Itoa(soonestRetry))
	}
	// [SCOPE P13-CAPTURE] What the client actually received. The pipeline
	// captures our OUTCOME; this captures the reply as rendered — for a
	// customer on their own dialect those are two different documents, and
	// "what did my system get back" is a question only this can answer.
	p.captureResponse(acct, rendered)
	writeRendered(w, rendered)
}

// optSegments returns the per-recipient segment count for the response ONLY
// when the account opted in (ShowSegments) — nil otherwise, so the field is
// omitted rather than rendered as a placeholder, the same discipline as cost
// (P1 scope finalize 2026-08-09). A pointer to a copy, never to m.Segments, so
// a later mutation of the message can't alias the response value.
func optSegments(m *model.Message, acct *refdata.Account) *int {
	if !acct.ShowSegments {
		return nil
	}
	segs := m.Segments
	return &segs
}

// priceResult, remainingAfter: reply_price.go (QA API-004/005, BILL-005).

// priceBreakdown is the optional net/tax/percentage/total split that
// accompanies `cost`. Zero value renders nothing.
type priceBreakdown struct {
	Net, Tax, TaxPercentage, Total *float64
}

// roundMoney rounds to 4 decimal places, matching the DECIMAL(14,4) storage
// columns (balance, rate cards) — avoids float64 arithmetic noise (e.g.
// 9.999999999998) leaking into a customer-facing response.
func roundMoney(v float64) float64 { return math.Round(v*10000) / 10000 }

// handleStatus implements GET /messages/{id} — a delivery-status lookup by
// message id (P1 scope finalize 2026-08-09), reading record_index (the O(1)
// locator). This is the status-PULL companion to DLR push; the status
// TRANSITIONS themselves are owned by P5/P7, this only reports the current
// value. For a freshly accepted message that value is "queued": such a message
// has no record until a vendor takes it, so it is found in its vendor's queue
// instead (status_queued.go, QA BUG-YK-EP-016).
func (p *Process) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, 405, map[string]string{"error": "GET only"})
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/messages/")
	id = strings.Trim(id, "/")
	if id == "" {
		writeJSON(w, 400, map[string]string{"error": "INVALID_REQUEST", "detail": "message id required"})
		return
	}
	rd := p.records()
	if rd == nil {
		writeJSON(w, 503, map[string]string{"error": "SERVICE_UNAVAILABLE", "detail": "status lookup needs the MySQL hot store"})
		return
	}
	// Credentials are REQUIRED, and a client only ever sees its OWN messages
	// (PO 2026-09-08). This endpoint used to answer anyone who knew an id: our
	// ULIDs are unguessable, but they travel in logs, receipts and support
	// tickets, so "unguessable" was the only thing standing between one
	// customer and another's delivery status.
	acct, rej := p.statusAccount(r)
	if acct == nil {
		writeAuthRefusal(w, rej)
		return
	}
	looked := false // the write instance was asked for this id
	// The record is in the account's company's table of the month in the id
	// (D-136): a client asks about its own messages only.
	ref := model.RecordRef{MsgID: id, CompanyID: acct.CompanyID}
	st, ok, err := rd.StatusByID(r.Context(), ref)
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "SERVICE_UNAVAILABLE", "detail": dialect.RefusalText("SERVICE_UNAVAILABLE")})
		return
	}
	// Not one of OUR ids? It may be the id the CLIENT gave (PO 2026-09-06:
	// their reference travels the whole journey, so it should be lookup-able
	// too).
	//
	// This half is AUTHENTICATED-ONLY, and deliberately so. This endpoint has
	// never required credentials — safe enough while the only key was an
	// unguessable 26-character ULID, but a reference is chosen by the customer
	// and looks like "order-1". An unauthenticated lookup by reference would
	// let anyone read another customer's message status by guessing. So:
	// credentials present and valid → we also search that account's own
	// references; no credentials → exactly the previous behaviour, our ids
	// only. (That the endpoint is open at all is a pre-existing gap, reported
	// separately — it is not made worse here.)
	// Someone else's message reads exactly like a message that does not exist:
	// answering 403 would confirm the id is real, which is the enumeration
	// hole the auth is here to close.
	if ok && st.AccountID != 0 && st.AccountID != acct.ID {
		ok = false
	}
	if !ok {
		st, ok, err = rd.StatusByReference(r.Context(), acct.ID, acct.CompanyID, id)
		if err != nil {
			writeJSON(w, 503, map[string]string{"error": "SERVICE_UNAVAILABLE", "detail": dialect.RefusalText("SERVICE_UNAVAILABLE")})
			return
		}
	}
	if !ok && p.queued != nil && isOurID(id) {
		// Accepted but not sent yet: no record, still in its vendor's queue
		// (QA BUG-YK-EP-016, status_queued.go). Someone else's waiting message
		// reads as not found, like everything else here.
		m, waiting, err := p.findQueued(r.Context(), acct, id)
		if err != nil {
			writeJSON(w, 503, map[string]string{"error": "SERVICE_UNAVAILABLE", "detail": dialect.RefusalText("SERVICE_UNAVAILABLE")})
			return
		}
		if waiting {
			writeJSON(w, 200, queuedAnswer(m, time.Now()))
			return
		}
		// It may have left the queue between the two reads: a sender writes
		// the record BEFORE it acks the queue row, so a second look finds it.
		// On the write instance: the record is a moment old at most.
		st, ok, err = freshStatus(r.Context(), rd, ref)
		looked = true
		if err != nil {
			writeJSON(w, 503, map[string]string{"error": "SERVICE_UNAVAILABLE", "detail": dialect.RefusalText("SERVICE_UNAVAILABLE")})
			return
		}
		if ok && st.AccountID != 0 && st.AccountID != acct.ID {
			ok = false
		}
	}
	if !ok && !looked && isOurID(id) && readsApart(rd) {
		// The records have a read instance of their own, and it did not have
		// this message. A read node runs a little behind: look where the
		// record is written before saying it does not exist.
		st, ok, err = freshStatus(r.Context(), rd, ref)
		if err != nil {
			writeJSON(w, 503, map[string]string{"error": "SERVICE_UNAVAILABLE", "detail": dialect.RefusalText("SERVICE_UNAVAILABLE")})
			return
		}
		if ok && st.AccountID != 0 && st.AccountID != acct.ID {
			ok = false
		}
	}
	if !ok && p.pending != nil && isOurID(id) {
		// Sent, and its record still in the DB worker's queue
		// (YAKU_RECORDS_VIA=worker): it has left its vendor queue and is not
		// on the log yet. What the record will say is kept beside its task
		// until it is written (recordq.Queue.Pending). Somebody else's
		// message reads as not found, like everything else here.
		if pr, found, perr := p.pending.Pending(r.Context(), id); perr == nil && found && pr.AccountID == acct.ID {
			st, ok = dal.MsgStatus{MsgID: id, RequestID: pr.RequestID, Status: pr.Status, VendorMsgID: pr.VendorMsgID,
				Reference: pr.Reference, AccountID: pr.AccountID}, true
		}
	}
	if !ok {
		writeJSON(w, 404, map[string]string{"messageId": id, "status": "NOT_FOUND", "detail": "No message with this id was found for this account."})
		return
	}
	out := map[string]any{"messageId": st.MsgID, "status": strings.ToUpper(st.Status)}
	if st.RequestID != "" { // no empty strings in a reply (2026-10-02)
		out["requestId"] = st.RequestID
	}
	if st.VendorMsgID != "" {
		out["vendorMsgId"] = st.VendorMsgID
	}
	// Answer in the caller's own vocabulary as well as ours.
	if st.Reference != "" {
		out["reference"] = st.Reference
	}
	writeJSON(w, 200, out)
}

// handleCacheRefresh is the admin hook for ALL in-memory caches: POST reloads
// the reference cache (accounts/routes/config) and delta-syncs the HLR cache
// now — the alternative to the 5-minute periodic refresh. GET reports stats
// without refreshing.
func (p *Process) handleCacheRefresh(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{}
	if r.Method == http.MethodPost {
		// Redis gate (2026-07-24): ONE refresh URL drives BOTH layers — first
		// force-sync MySQL -> Redis (reference snapshot + HLR delta, lease
		// deliberately bypassed: an operator's explicit refresh must run on
		// whichever instance received it), then reload local memory from the
		// fresh snapshot. Every OTHER instance converges within one watch
		// interval (~3s) via the ref:version bump — no per-instance calls.
		if p.sync != nil {
			if err := p.sync.SyncRef(r.Context()); err != nil {
				writeJSON(w, 500, map[string]string{"error": "REDIS_SYNC_FAILED: " + err.Error()})
				return
			}
			if err := p.sync.SyncHLR(r.Context()); err != nil {
				writeJSON(w, 500, map[string]string{"error": "REDIS_HLR_SYNC_FAILED: " + err.Error()})
				return
			}
		}
		if err := p.ref.RefreshForce(r.Context()); err != nil { // the nudge always reloads
			writeJSON(w, 500, map[string]string{"error": "REFDATA_REFRESH_FAILED: " + err.Error()})
			return
		}
		if p.hlrCache != nil {
			if _, err := p.hlrCache.ForceRefresh(r.Context()); err != nil {
				writeJSON(w, 500, map[string]string{"error": "HLR_REFRESH_FAILED: " + err.Error()})
				return
			}
		}
		out["refreshed"] = true
	}
	// Which gateway answered: the Admin API nudges every deployment in its
	// register and checks each address reached the one it is filed under.
	out["deployment_id"] = deployment.ID()
	if p.redisLoader != nil {
		v, _ := p.redisLoader.Version(r.Context())
		out["redis_snapshot_version"] = v
	}
	ac, tk, rt := p.ref.Counts()
	out["refdata"] = map[string]any{"accounts": ac, "tokens": tk, "routes": rt}
	if p.hlrCache != nil {
		enabled, records, last := p.hlrCache.Stats()
		out["hlr"] = map[string]any{"enabled": enabled, "records": records, "last_sync": last}
	} else {
		out["hlr"] = map[string]string{"cache": "disabled", "mode": "direct reads (redis backend or no delta source)"}
	}
	writeJSON(w, 200, out)
}

// handleBalanceRefresh implements POST /admin/balance/refresh — the top-up
// hook for the Redis prepaid-balance offload. After editing a balance in
// MySQL, call this to drop the live Redis counter(s) so the next reservation
// re-seeds from MySQL. Query params account_id / company_id target one
// counter; neither = drop them all. 404 when the redis gate is off (there is
// no live counter to refresh — MySQL is already authoritative). [2026-07-23]
func (p *Process) handleBalanceRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	if p.balAdmin == nil {
		writeJSON(w, 404, map[string]string{"error": "redis balance offload not enabled (YAKU_REDIS_ADDR unset)"})
		return
	}
	q := r.URL.Query()
	acctID, _ := strconv.ParseInt(q.Get("account_id"), 10, 64)
	coID, _ := strconv.ParseInt(q.Get("company_id"), 10, 64)
	if acctID == 0 && coID == 0 {
		n, err := p.balAdmin.ResyncAll(r.Context())
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"resynced": "all", "dropped": n})
		return
	}
	if err := p.balAdmin.Resync(r.Context(), acctID, coID); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"resynced": map[string]int64{"account_id": acctID, "company_id": coID}})
}

// decryptRequest/decryptResponse are the POST /admin/decrypt wire shapes.
type decryptRequest struct {
	Value string `json:"value"`
}
type decryptResponse struct {
	Plaintext string `json:"plaintext"`
}

// handleDecrypt implements POST /admin/decrypt — the portal-view integration
// point (eocean-multi-tenant-kms-architecture.md's third decrypt point,
// alongside encrypt-at-submit and decrypt-at-vendor-pickup). The Node Admin
// API holds no KMS credential of its own by design: it fetches an
// already-stored value (e.g. a hot-store record's body) over its own DB
// connection, then calls this endpoint with that exact value to get back
// plaintext — every KEK, local or remote, stays confined to this Go process.
//
// A value that isn't ciphertext (plaintext row, at-rest encryption was never
// on for it) passes through unchanged, same as every other Decrypt call site
// — the caller does not need to know in advance whether a given value was
// ever encrypted. [SCOPE P1-KMS-MULTITENANT]
func (p *Process) handleDecrypt(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req decryptRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "INVALID_REQUEST: " + err.Error()})
		return
	}
	if p.cry == nil {
		// At-rest encryption disabled gateway-wide: nothing was ever encrypted,
		// so the input is already plaintext (or empty) — same passthrough
		// semantics crypto.Service.Decrypt itself guarantees when armed.
		writeJSON(w, 200, decryptResponse{Plaintext: req.Value})
		return
	}
	pt, err := p.cry.Decrypt(r.Context(), req.Value)
	if err != nil {
		// Never echo the ciphertext or any key material in the error — only
		// the class of failure (revoked key, wrong KEK, malformed envelope).
		writeJSON(w, 422, map[string]string{"error": "DECRYPT_FAILED: " + err.Error()})
		return
	}
	writeJSON(w, 200, decryptResponse{Plaintext: pt})
}

// maxDecryptBatch bounds one batch call: an export reads in batches of this
// size, so one call never holds the handler for long.
const maxDecryptBatch = 2000

type decryptBatchRequest struct {
	Values []string `json:"values"`
}

// decryptBatchResponse answers value by value, in order. A value that is not
// encrypted comes back as it was; one that cannot be opened comes back as ""
// with its index in Failed and the reason in Errors (class of failure only,
// never key material or the ciphertext).
type decryptBatchResponse struct {
	Plaintexts []string `json:"plaintexts"`
	Failed     []int    `json:"failed,omitempty"`
	Errors     []string `json:"errors,omitempty"`
}

// handleDecryptBatch implements POST /admin/decrypt/batch — many stored
// values opened in one call, for the portal's lists, message details and
// exports (PO 2026-09-14). One bad value never fails the batch.
func (p *Process) handleDecryptBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req decryptBatchRequest
	// A batch of long texts can pass the single-message body cap; allow the
	// batch size times it.
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes*8)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "INVALID_REQUEST: " + err.Error()})
		return
	}
	if len(req.Values) > maxDecryptBatch {
		writeJSON(w, 400, map[string]string{"error": fmt.Sprintf("INVALID_REQUEST: at most %d values per call", maxDecryptBatch)})
		return
	}
	out := decryptBatchResponse{Plaintexts: make([]string, len(req.Values))}
	for i, v := range req.Values {
		if p.cry == nil || !crypto.IsCiphertext(v) {
			out.Plaintexts[i] = v
			continue
		}
		pt, err := p.cry.Decrypt(r.Context(), v)
		if err != nil {
			out.Failed = append(out.Failed, i)
			out.Errors = append(out.Errors, err.Error())
			continue
		}
		out.Plaintexts[i] = pt
	}
	writeJSON(w, 200, out)
}

// handleOperator implements GET /sms/v4/operator?to=<msisdn> — an explicit HLR/
// operator lookup (P1 scope finalize 2026-08-09). Unlike the submit pipeline's
// hlr stage, this endpoint may dip SYNCHRONOUSLY: it is a lookup, not the accept
// path, so its latency is not the submit SLA. Cache first, then (if configured)
// a guarded live dip. Never a hot-path concern for other traffic — the dip's
// own guards still apply.
func (p *Process) handleOperator(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, 405, map[string]string{"error": "GET only"})
		return
	}
	// Credentials required, like the status lookup (PO 2026-09-11). This
	// endpoint answered anyone who could reach the port — and a miss can
	// start a live HLR dip, which is paid per number.
	acct, rej := p.statusAccount(r)
	if acct == nil {
		writeAuthRefusal(w, rej)
		return
	}
	raw := r.URL.Query().Get("to")
	if raw == "" {
		raw = r.URL.Query().Get("msisdn")
	}
	dest := msisdn.Destination(raw)
	if dest == "" {
		writeJSON(w, 400, map[string]string{"error": "INVALID_REQUEST", "detail": "to/msisdn required"})
		return
	}
	// A national number is refused as a send would refuse it, not looked up
	// (perhaps by a paid live dip) as another country's number (2026-10-01).
	if msisdn.National(dest) {
		writeJSON(w, 400, map[string]string{"error": "INVALID_MSISDN", "status": "INVALID_MSISDN", "detail": "national number (starts with 0) — send it with the country code"})
		return
	}
	// The calling account's own HLR order, exactly as sending would resolve
	// it (2026-09-14): our data, ranges, its live providers, the country.
	m := pipeline.ResolveNetwork(r.Context(), pipeline.SubmitDeps{
		HLR: p.hlrSvc, HLRLive: p.hlrLive, HLRRanges: p.hlrRanges,
	}, *acct, dest)
	// A lookup an HLR provider answered is logged (D-136, Saleem 2026-10-14:
	// "from hlr provider all are logged"): this one has no message. Best
	// effort — the answer does not wait on its log line's fate.
	// With the DB worker the row is a task of the record queue, like every
	// hot-store row (28 Oct 2026).
	if m.HLRSource == "dip" || m.HLRSource == "dip_miss" {
		var err error
		switch {
		case p.recQ != nil:
			err = p.recQ.WriteLookup(r.Context(), lookupRow(acct.CompanyID, acct.ID, dest, m))
		case p.hotRead != nil:
			err = p.hotRead.WriteLookup(r.Context(), lookupRow(acct.CompanyID, acct.ID, dest, m))
		}
		if err != nil {
			p.log.Printf("lookup of %s not logged: %v", dest, err)
		}
	}
	if m.MCC != "" && m.MNC != "" {
		res := operatorResult(dest, hlr.Entry{MCC: m.MCC, MNC: m.MNC, Ported: m.HLRPorted})
		res["source"] = m.HLRSource
		writeJSON(w, 200, res)
		return
	}
	writeJSON(w, 404, map[string]string{"to": dest, "status": "NOT_FOUND", "detail": "No network was found for this number."})
}

func operatorResult(dest string, e hlr.Entry) map[string]any {
	return map[string]any{
		"to":       dest,
		"mcc":      e.MCC,
		"mnc":      e.MNC,
		"operator": e.MCC + e.MNC,
		"ported":   e.Ported,
		"status":   "OK",
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// statusAccount authenticates a status lookup, returning nil and the refusal
// when credentials are missing or wrong, or the login or address is held
// back. Never writes a response — the caller does (writeAuthRefusal). Since
// 2026-09-08 the endpoint no longer answers anonymous callers at all.
func (p *Process) statusAccount(r *http.Request) (*refdata.Account, *pipeline.Reject) {
	fields, _, _ := parseFields(r, p.ref.DialectForPath(normalizePath(r.URL.Path)))
	acct, rej := p.auth.FromRequest(r, fields)
	if rej != nil {
		return nil, rej
	}
	return acct, nil
}

// writeAuthRefusal answers a status or network lookup that was refused at
// sign-in with the refusal itself — 423 for a locked login or address, 429
// for an address held back, each with its Retry-After — as a send is
// answered. It answered a flat 401 "The credentials were not accepted." to
// a right token while the address was blocked, with no time to wait (go-live
// test 18 Oct, P1 D2). No refusal known: 401, as before.
func writeAuthRefusal(w http.ResponseWriter, rej *pipeline.Reject) {
	if rej == nil || rej.Code == 0 {
		writeJSON(w, 401, map[string]string{"error": "AUTH_FAILED", "status": "AUTH_FAILED", "detail": dialect.RefusalText("AUTH_FAILED")})
		return
	}
	if rej.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(rej.RetryAfter))
	}
	code := rejectCode(rej.Reason)
	writeJSON(w, rej.Code, map[string]string{"error": code, "status": code, "detail": refusalDetail(rej.Reason)})
}

// maskMinDigits is the shortest digit run hidden by pii_masked
// (YAKU_MASK_MIN_DIGITS, default 4).
func maskMinDigits() int {
	if n, err := strconv.Atoi(env("YAKU_MASK_MIN_DIGITS", "")); err == nil && n > 0 {
		return n
	}
	return mask.DefaultMinDigits
}

// handleBalanceCredit implements POST /admin/balance/credit — a manual balance
// change applied to the live Redis counter (2026-09-18).
//
// The Admin API calls it while its transaction holds the company row, before
// commit, with {company_id, delta_micro, new_total_micro}; {undo:true} reverses
// one whose commit then failed. Without it a portal top-up is ERASED under the
// Redis gate: the reserver's write-back copies the live counter over
// companies.balance every 30 s, and the ledger row is left saying the money
// arrived. /admin/balance/refresh is not a substitute — it drops the counter
// and re-seeds from MySQL, which lags the counter, so every reservation since
// the last write-back would be handed back. The rules are dal/redis CreditLive's.
//
// Redis gate off: 200 {"live":false} — MySQL is the only balance and the
// caller's own write is the whole change.
func (p *Process) handleBalanceCredit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var in struct {
		CompanyID     int64 `json:"company_id"`
		DeltaMicro    int64 `json:"delta_micro"`
		NewTotalMicro int64 `json:"new_total_micro"`
		Undo          bool  `json:"undo"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&in); err != nil {
		writeJSON(w, 400, map[string]string{"error": "body must be JSON {company_id, delta_micro, new_total_micro}"})
		return
	}
	if in.CompanyID <= 0 || in.DeltaMicro == 0 || (!in.Undo && in.NewTotalMicro < 0) {
		writeJSON(w, 400, map[string]string{"error": "company_id and a non-zero delta_micro are required, and the new total cannot be negative"})
		return
	}
	if p.balAdmin == nil {
		writeJSON(w, 200, map[string]any{"live": false, "reason": "no live balance counter: Redis is not configured"})
		return
	}
	var err error
	if in.Undo {
		err = p.balAdmin.Undo(r.Context(), in.CompanyID, in.DeltaMicro)
	} else {
		err = p.balAdmin.Credit(r.Context(), in.CompanyID, in.DeltaMicro, in.NewTotalMicro)
	}
	if err != nil {
		writeJSON(w, 502, map[string]string{"error": "live balance not changed: " + err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"live": true, "undo": in.Undo})
}

// lookupRow is a lookup with no message as its log row (6 Nov 2026: with the
// operator's name and the country calling code it found).
func lookupRow(companyID, accountID int64, dest string, m model.Message) dal.LookupRow {
	return dal.LookupRow{CompanyID: companyID, AccountID: accountID, Destination: dest, Source: m.HLRSource,
		MCC: m.MCC, MNC: m.MNC, Ported: m.HLRPorted, OperatorName: m.HLROperator, CCC: m.HLRCCC, Call: m.HLRCall}
}
