package httpserver

import (
	"net/http"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/campaignwin"
)

// handleCampaign answers GET /admin/campaign?hash=<signature>: how many
// messages with this text signature were accepted in the last hour and day,
// and from which accounts — the live campaign window (P10 register, ruled
// 2026-09-09). Read from Redis only, never from records. The Admin API calls
// it for operators looking at a message; the numbers are gateway-wide, which
// is the point (the same wording under several accounts), so a customer never
// sees them.
func (p *Process) handleCampaign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, 405, map[string]string{"error": "GET only"})
		return
	}
	if p.campaignWin == nil {
		writeJSON(w, 503, map[string]string{"error": "NO_LIVE_WINDOW: the live campaign window needs Redis"})
		return
	}
	hash := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("hash")))
	s, err := campaignwin.Read(r.Context(), p.campaignWin, hash, time.Now())
	if err != nil {
		code := 503
		if strings.Contains(err.Error(), "64 hex") {
			code = 400
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, s)
}
