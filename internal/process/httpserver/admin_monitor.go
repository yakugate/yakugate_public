package httpserver

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/monitor"
)

// handleReportNow asks every role to report now — the portal's Live
// (Saleem 2026-10-05). POST {"deployment_id": "<id>"} asks that deployment's
// roles; empty or no body asks every deployment's. The answer says when the
// ask was written (asked_at, this gateway's clock in UTC); the roles report
// within about a second, into the same history the screens read.
func (p *Process) handleReportNow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		Deployment string `json:"deployment_id"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil {
			writeJSON(w, 400, map[string]string{"error": `send {"deployment_id":"<id>"} or nothing`})
			return
		}
	}
	at, err := monitor.AskForReports(r.Context(), req.Deployment)
	if errors.Is(err, monitor.ErrNoHistory) {
		writeJSON(w, 503, map[string]string{"error": "NO_HISTORY: " + err.Error()})
		return
	}
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "hot store: " + err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"asked_at": at.Format(time.RFC3339Nano), "deployment_id": req.Deployment,
		"answered_by": deployment.ID()})
}
