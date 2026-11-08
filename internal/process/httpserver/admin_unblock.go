package httpserver

// POST /admin/unblock — the operator's manual unblock (P1 scope finalize
// 2026-08-09 §7): clears any lockout, IP throttle, or spray block for an
// identity or a source IP on demand, without waiting out the rung. Gated
// behind the same default-closed status key as every other admin endpoint
// (ONE key gates all — split rejected in the scope review). The Admin API /
// portal call this endpoint; under the Redis gate a clear is cluster-wide and
// also releases P2 bind-auth blocks, since every instance shares the keys.
//
// Body: {"identity": "user-or-system-id", "ip": "1.2.3.4", "ref": "…"} — at
// least one.
//   - identity → identity ladder Clear
//   - ip       → per-IP ladder Clear + spray-guard Clear
//   - ref      → exactly the block GET /admin/blocks listed under that
//                reference (2026-10-09). It is the only way to lift a locked
//                token: the list never shows the token, and a login name does
//                not reach it.
//
// Response: {"cleared": ["identity:...", "ip:...", "token:#1a2b3c4d"]}.
// Clearing something that was never blocked is a no-op success — the
// operator's intent ("this must not be blocked") is satisfied either way. A
// reference that names nothing blocked now (the block ended, or was lifted)
// clears nothing and is left out of "cleared".

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/yakugate/yakugate/internal/seclog"
)

func (p *Process) handleUnblock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		Identity string `json:"identity"`
		IP       string `json:"ip"`
		Ref      string `json:"ref"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if req.Identity == "" && req.IP == "" && req.Ref == "" {
		writeJSON(w, 400, map[string]string{"error": "identity, ip or ref is required"})
		return
	}
	cleared := []string{}
	// What the security log is told. A block lifted by its reference names
	// itself there the way the list showed it — never by a token.
	logAccount, logIdentity, logIP := req.Identity, "", req.IP
	if req.Identity != "" {
		logIdentity = "u:" + req.Identity
	}
	if req.Ref != "" {
		id, found, err := p.blockByRef(req.Ref)
		if err != nil {
			writeJSON(w, 503, map[string]string{"error": "the list of blocks could not be read: " + err.Error()})
			return
		}
		if found {
			p.clearBlock(id)
			cleared = append(cleared, blockLabel(id))
			switch kind, _, value, _ := describeBlock(id); kind {
			case "login", "smpp":
				if logAccount == "" {
					logAccount, logIdentity = value, "u:"+value
				}
			case "address":
				if logIP == "" {
					logIP = value
				}
			default:
				if logIdentity == "" {
					logIdentity = blockLabel(id)
				}
			}
		}
	}
	if req.Identity != "" && p.idUnblock != nil {
		// The identity ladder namespaces its keys by credential scheme:
		// "u:<username>" (password auth) and "t:<token>" (JWT auth). An
		// operator knows the username, not the exact token string, so clear
		// both forms plus the raw value — clearing a key that was never
		// locked is a no-op.
		p.idUnblock.Clear("u:" + req.Identity)
		p.idUnblock.Clear("t:" + req.Identity)
		p.idUnblock.Clear("smpp:" + req.Identity) // P2 bind-auth namespace — shared keys under the Redis gate
		p.idUnblock.Clear(req.Identity)
		cleared = append(cleared, "identity:"+req.Identity)
	}
	if req.IP != "" {
		if p.ipUnblock != nil {
			p.ipUnblock.Clear("ip:" + req.IP) // the per-IP ladder's namespace
		}
		if p.sprayUnblock != nil {
			p.sprayUnblock.Clear(req.IP) // spray guard keys by raw IP
		}
		cleared = append(cleared, "ip:"+req.IP)
	}
	// A manual unblock is an operator undoing a security control, so it is
	// recorded twice on purpose (PO 2026-09-05): here, in the security stream
	// beside the lockouts it cancels — and, via the audited() wrapper on this
	// route, as an audit_log row that survives log rotation and answers "who
	// lifted this, and when" months later.
	if p.secLog != nil && len(cleared) > 0 {
		p.secLog(seclog.Event{
			Type:     seclog.EventUnblocked,
			Account:  logAccount, // the operator names the identity; empty for an IP-only unblock
			Identity: logIdentity,
			IP:       logIP,
			Reason:   "manual unblock: " + strings.Join(cleared, ", "),
		})
	}
	writeJSON(w, 200, map[string]any{"cleared": cleared})
}
