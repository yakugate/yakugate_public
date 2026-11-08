package httpserver

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/yakugate/yakugate/internal/dialect"
)

// handleDialectDryrun previews a dialect config against a sample request
// WITHOUT touching any account row or sending traffic — the onboarding
// dry-run of chunk-3 ruling 3.6 (2026-08-03), the client-dialect counterpart
// of D-085's vendor-profile dry-run idea. It runs the REAL mapper and the one
// real renderer (dialect.MapRequest / dialect.Render), so what it shows is
// exactly what the engine will do — a Node/portal reimplementation would
// inevitably drift. The portal calls this endpoint; gated behind the same
// default-closed status key as /status and /admin/decrypt.
//
// POST body:
//
//	{
//	  "dialect": { ...an api_dialect document... },        // required
//	  "request": { "MSISDN": "9230...", "SMS": "hi" },     // optional sample submission
//	  "reasons": ["INSUFFICIENT_BALANCE", "INVALID_MSISDN"] // optional error previews
//	}
//
// Response: {"valid": true, "canonicalRequest": {...}, "responses": {
// "accepted": {status, contentType, body}, "<REASON>": {...}}} — or
// {"valid": false, "error": "..."} (HTTP 422) when the dialect itself fails
// dialect.Parse validation, which makes this endpoint double as a config
// validator.
func (p *Process) handleDialectDryrun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		Dialect json.RawMessage `json:"dialect"`
		Request map[string]any  `json:"request,omitempty"`
		Reasons []string        `json:"reasons,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if len(req.Dialect) == 0 {
		writeJSON(w, 400, map[string]string{"error": "dialect is required"})
		return
	}
	cfg, err := dialect.Parse(string(req.Dialect))
	if err != nil {
		writeJSON(w, 422, map[string]any{"valid": false, "error": err.Error()})
		return
	}

	out := map[string]any{"valid": true}
	if req.Request != nil {
		out["canonicalRequest"] = dialect.MapRequest(req.Request, cfg)
	}

	// Sample renders: one accepted result plus each requested error reason,
	// through the SAME Render every live response uses.
	now := time.Now().UTC()
	sample := dialect.Result{
		To:               "923001234567",
		MessageID:        "01SAMPLE0MESSAGE0ID0000000",
		RequestID:        "01SAMPLE0REQUEST0ID0000000",
		Accepted:         true,
		StatusCode:       200,
		AcceptedTime:     now.Format(time.RFC3339),
		Sender:           "SAMPLE",
		Body:             "sample message text",
		ResponseDateTime: now.Format("2006-01-02 15:04:05"),
	}
	responses := map[string]any{"accepted": renderPreview([]dialect.Result{sample}, cfg)}
	for _, reason := range req.Reasons {
		rej := sample
		rej.Accepted = false
		rej.Reason = reason
		rej.StatusCode = previewStatus(reason)
		rej.MessageID = ""
		rej.AcceptedTime = ""
		// No detail: the reply's `error` is then the code's own English text
		// (dialect.RefusalText), as a real refusal without a detail reads.
		// It was the code word itself, so a field falling back to `error`
		// previewed "NO_ROUTE" where a live reply says the sentence (6 Nov 2026).
		rej.Detail = ""
		responses[reason] = renderPreview([]dialect.Result{rej}, cfg)
	}
	out["responses"] = responses
	writeJSON(w, 200, out)
}

// renderPreview wraps one Render outcome for the dry-run response.
func renderPreview(results []dialect.Result, cfg *dialect.Config) map[string]any {
	rendered := dialect.Render(results, cfg)
	return map[string]any{
		"status":      rendered.StatusCode,
		"contentType": rendered.ContentType,
		"body":        string(rendered.Body),
	}
}

// previewStatus supplies the native HTTP status a preview rejection would
// carry, mirroring the published contract-error table; unknown reasons get
// the generic 400. Per-reason dialect overrides then apply on top inside
// Render, exactly as they would live.
func previewStatus(reason string) int {
	switch reason {
	case "AUTH_FAILED":
		return 401
	case "INSUFFICIENT_BALANCE":
		return 402
	case "ACCOUNT_DISABLED", "ACCOUNT_SUSPENDED":
		return 403
	case "NOT_FOUND":
		return 404
	case "INVALID_MSISDN":
		return 406
	case "DUPLICATE_MESSAGE":
		return 409
	case "TEMPLATE_MISMATCH":
		return 422
	case "AUTH_LOCKED", "IP_LOCKED":
		return 423
	case "RATE_LIMIT_EXCEEDED", "QUOTA_EXCEEDED", "IP_THROTTLED":
		return 429
	}
	return 400
}
