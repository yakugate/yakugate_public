package httpserver

import (
	"net/http"
	"strings"

	"github.com/yakugate/yakugate/internal/dialect"
)

// writeRendered writes an already-rendered response.
func writeRendered(w http.ResponseWriter, r dialect.Rendered) {
	w.Header().Set("Content-Type", r.ContentType)
	w.WriteHeader(r.StatusCode)
	_, _ = w.Write(r.Body)
}

// writeReject answers a request-level failure — one that happened before or
// instead of per-recipient processing (auth, malformed body, no recipients).
//
// It goes through dialect.Render like every other response, so a request-level
// error is shaped by the account's dialect exactly as a pipeline rejection is.
// A nil dialect yields the native contract.
func writeReject(w http.ResponseWriter, d *dialect.Config, status int, code, detail string) {
	res := dialect.Result{
		Accepted:   false,
		StatusCode: status,
		Reason:     code,
		Detail:     detail,
	}
	writeRendered(w, dialect.Render([]dialect.Result{res}, d))
}

// rejectCode extracts the reason CODE from a reject string of the form
// "CODE: human detail". Reject reasons are built that way throughout the
// pipeline, and the response contract needs the two parts separately: the code
// drives `message` (and any per-code dialect override), the remainder is the
// descriptive `error`.
func rejectCode(reason string) string {
	if i := strings.Index(reason, ":"); i > 0 {
		return strings.TrimSpace(reason[:i])
	}
	return strings.TrimSpace(reason)
}

// rejectDetail is rejectCode's counterpart: the human-readable remainder, or
// empty when the reason is a bare code.
func rejectDetail(reason string) string {
	if i := strings.Index(reason, ":"); i > 0 {
		return strings.TrimSpace(reason[i+1:])
	}
	return ""
}

// refusalDetail is what the client is told in words for a refusal: its own
// detail, else its code's message (dialect.RefusalText) — never empty.
func refusalDetail(reason string) string {
	if d := rejectDetail(reason); d != "" {
		return d
	}
	return dialect.RefusalText(rejectCode(reason))
}
