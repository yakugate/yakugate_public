package httpserver

// GET /admin/audit/verify — has anyone tampered with the audit trail?
//
// The chain is enforced in the database (migration config/0088). This is the
// only way to ASK about it, and without it the chain would be a column nobody
// ever looks at. It is admin-gated like every other /admin route, and audited
// like them too: someone checking the trail is itself worth a row.
//
// A full walk, on demand, never on a request path — a sampled check that
// happens to miss the altered row would report "verified", which is worse than
// not checking at all.

import (
	"encoding/json"
	"net/http"

	"github.com/yakugate/yakugate/internal/audit"
)

func (p *Process) handleAuditVerify(w http.ResponseWriter, r *http.Request) {
	writer, ok := p.aud.(*audit.Trail)
	if !ok || writer.Log() == nil {
		// No audit connection at all. Say so plainly rather than answering
		// "verified" over an empty table — the two are not the same, and only
		// one of them is good news.
		writeJSONStatus(w, http.StatusServiceUnavailable, map[string]any{
			"status": "unavailable",
			"reason": "no audit database connection on this process (config store must be MySQL)",
		})
		return
	}
	rep, err := audit.Verify(r.Context(), writer.Log())
	if err != nil {
		writeJSONStatus(w, http.StatusInternalServerError, map[string]any{
			"status": "error",
			"reason": err.Error(),
		})
		return
	}
	// A broken trail answers 409, not 200. An operator wiring this into a
	// monitor should get a failure from the status code alone, without having
	// to parse and understand the body.
	code := http.StatusOK
	if rep.Status == audit.StatusBroken {
		code = http.StatusConflict
	}
	writeJSONStatus(w, code, rep)
}

// writeJSONStatus writes one JSON body with an explicit status code.
func writeJSONStatus(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
