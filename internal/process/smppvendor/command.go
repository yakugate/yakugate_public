package smppvendor

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// HandleCommand implements monitor.CommandProvider (item 4.6): an on-demand
// control surface so an operator doesn't have to wait for reloadEvery (30s
// default) to pick up a vendor change, and can manually resume a vendor that
// gave up after max_reconnect_attempts. Mounted at /command/ by
// internal/monitor.ServeAdmin, behind the same status key as /status.
//
//	POST /command/reload                    — re-scan the vendors table now
//	POST /command/vendors/{id}/reconnect     — clear a gave-up vendor's
//	                                            reconnect ceiling and resume
func (p *Process) HandleCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/command/")
	switch {
	case path == "reload":
		p.forceReload()
		writeCommandResult(w, http.StatusOK, "reload requested")
	case strings.HasPrefix(path, "vendors/") && strings.HasSuffix(path, "/reconnect"):
		idStr := strings.TrimSuffix(strings.TrimPrefix(path, "vendors/"), "/reconnect")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			writeCommandResult(w, http.StatusBadRequest, "invalid vendor id")
			return
		}
		p.requestReconnect(id)
		writeCommandResult(w, http.StatusOK, "reconnect requested")
	default:
		http.NotFound(w, r)
	}
}

func writeCommandResult(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": message})
}
