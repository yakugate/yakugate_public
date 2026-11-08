package httpserver

import "net/http"

// handleVendorHealth answers GET /admin/vendors/health: every vendor's state
// as routing sees it now — up, failing (not down yet), down, paused, disabled,
// or no report — from this process's own vendorhealth.View (2026-09-15). The
// Routing Rules screen shows it beside each vendor, so a vendor the engine is
// routing around no longer shows green because it is still switched on.
// Read from memory; nothing is queried per call.
func (p *Process) handleVendorHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, 405, map[string]string{"error": "GET only"})
		return
	}
	if p.health == nil {
		writeJSON(w, 503, map[string]string{"error": "NO_HEALTH_VIEW: this gateway has no config store to read vendor health from"})
		return
	}
	writeJSON(w, 200, map[string]any{"vendors": p.health.States()})
}
