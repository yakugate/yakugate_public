package httpserver

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/store"
)

// tableSafe is mysql.identRe: what a vendor code keeps in its table name.
var tableSafe = regexp.MustCompile(`[^A-Za-z0-9_]`)

// handleQueueDepth answers how many messages wait in one vendor's queue
// (POST {"vendor":"<cid>"} → {"vendor","depth","oldest_age_sec"}). The Admin
// API asks before it deletes a vendor (2026-09-29): it cannot read the queue
// store itself, by design, and a vendor deleted with messages still queued
// leaves them with no role to send them.
//
// Since each deployment has its own queue (D-121) a plain ask answers for
// THIS deployment: its own messages and the shared ones. {"all":true} counts
// every deployment's (2026-10-02) — what a delete must see, since the Admin
// API asks whichever gateway answers first. The answer names its scope
// ("all" | "this_deployment") so a caller can tell an engine that ignored
// "all" (older than this) from one that counted everything.
func (p *Process) handleQueueDepth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		Vendor string `json:"vendor"`
		All    bool   `json:"all"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil || strings.TrimSpace(req.Vendor) == "" {
		writeJSON(w, 400, map[string]string{"error": `send {"vendor":"<cid>"}`})
		return
	}
	if p.queueDepthSrc == nil {
		writeJSON(w, 503, map[string]string{"error": "NO_QUEUE_DEPTH: this gateway's queue store cannot count its queues"})
		return
	}
	if req.All {
		counter, ok := p.queueDepthSrc.(store.VendorQueueCounter)
		if !ok {
			writeJSON(w, 503, map[string]string{"error": "NO_QUEUE_COUNT: this gateway's queue store cannot count every deployment's queue"})
			return
		}
		n, err := counter.QueuedForVendor(r.Context(), strings.TrimSpace(req.Vendor))
		if err != nil {
			writeJSON(w, 503, map[string]string{"error": "queue store: " + err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"vendor": req.Vendor, "depth": n, "scope": "all", "deployment_id": deployment.ID()})
		return
	}
	depths, err := p.queueDepthSrc.QueueDepths(r.Context())
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "queue store: " + err.Error()})
		return
	}
	// The table is queue_<cid> with anything outside [A-Za-z0-9_] made "_"
	// and lower-cased (mysql.queueTable); QueueDepths keys by what follows
	// "queue_".
	key := strings.ToLower(tableSafe.ReplaceAllString(strings.TrimSpace(req.Vendor), "_"))
	st := depths[key]
	writeJSON(w, 200, map[string]any{"vendor": req.Vendor, "depth": st.Depth, "oldest_age_sec": st.OldestAgeSec,
		"scope": "this_deployment", "deployment_id": deployment.ID()})
}
