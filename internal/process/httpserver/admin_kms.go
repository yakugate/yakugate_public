package httpserver

// POST /admin/kms/test — "Test connection" on the KMS Provider screen
// (PO 2026-09-13).
//
// Locks a random throwaway key through a company's master key and unlocks it
// again, so an operator knows the provider, its credentials and the key name
// all work BEFORE a single message depends on them. Nothing is stored: no
// data key, no message. The provider's config is re-read first, so a test
// right after an edit tests the edit.
//
// Body: {"providerId": 12, "key": "acme-kek"} for an HTTP key service, or
// {"keyRef": "gcp-kms://projects/…"} for a full ref, or {"gatewayKey": true}
// to ask whether the gateway has a key of its own — what a company set to
// encrypted with no KMS assignment uses (PO 2026-09-13). The Admin API asks
// before letting such a company be saved, since without that key every one
// of its messages would be refused.

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/yakugate/yakugate/internal/kms"
)

func (p *Process) handleKMSTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		ProviderID int64  `json:"providerId"`
		Key        string `json:"key"`
		KeyRef     string `json:"keyRef"`
		GatewayKey bool   `json:"gatewayKey"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if req.GatewayKey {
		// The gateway key is a local AES key, not a provider: having it is
		// the whole test.
		has := p.cry.HasDefaultKey()
		out := map[string]any{"ok": has, "gatewayKey": has}
		if !has {
			out["error"] = "the gateway has no key of its own (YAKU_CRYPTO_KEK_REF)"
		}
		writeJSON(w, 200, out)
		return
	}
	ref := strings.TrimSpace(req.KeyRef)
	key := strings.TrimSpace(req.Key)
	switch {
	case ref != "":
	case strings.Contains(key, "://"):
		ref = key
	case req.ProviderID > 0 && key != "":
		ref = kms.HTTPKeyRef(req.ProviderID, key)
	default:
		writeJSON(w, 400, map[string]string{"error": "give providerId and key, or keyRef"})
		return
	}
	if req.ProviderID > 0 {
		p.cry.ForgetProvider(req.ProviderID)
	}
	wrap, unwrap, err := p.cry.TestKEK(r.Context(), ref)
	out := map[string]any{
		"ok":       err == nil,
		"keyRef":   ref,
		"wrapMs":   wrap.Milliseconds(),
		"unwrapMs": unwrap.Milliseconds(),
	}
	if err != nil {
		out["error"] = err.Error()
	}
	writeJSON(w, 200, out)
}
