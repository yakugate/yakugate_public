package pipeline

import (
	"context"

	"github.com/yakugate/yakugate/internal/seclog"
)

// --- stage 2/18: IP allowlist --- [SCOPE P1-02]
// Matching (exact / CIDR / "*.*.*.*") lives on refdata.Account.IPAllowed so
// internal/auth's lockout skip-table applies the identical rule.
//
// enforce_ip_check was dropped as a separate column (2026-07-28): a non-empty
// AllowedIPs list now IMPLIES enforcement — a config with allowlist entries
// obviously means IPs should be checked.
//
// A refusal here is a SECURITY event, not an ordinary validation reject: it is
// the P13 spec's "unrestricted area visiting" — a caller with correct
// credentials coming from an address the account never approved. seclog is nil
// in tests and anywhere the process did not wire one.
type ipAllowStage struct {
	seclog func(seclog.Event)
}

func (ipAllowStage) Name() string { return "ip-allow" }
func (s ipAllowStage) Run(_ context.Context, e *Envelope) *Reject {
	if len(e.Account.AllowedIPs) == 0 {
		return nil
	}
	// The allowlist carries a scope (P1 scope finalize 2026-08-09 §9,
	// config/0061): 'portal' means the list gates portal access only, so the
	// engine skips it here. ('portal' accounts are refused at auth anyway —
	// this keeps the stage honest if that ever changes.)
	if e.Account.AccessScope == "portal" {
		return nil
	}
	if !e.Account.IPAllowed(e.ClientIP) {
		if s.seclog != nil {
			s.seclog(seclog.Event{
				Type:     seclog.EventIPNotAllowed,
				Account:  e.Account.Username,
				Identity: "u:" + e.Account.Username,
				IP:       e.ClientIP,
				Reason:   "address is not on this account's allow list",
			})
		}
		return reject(403, "IP_NOT_ALLOWED")
	}
	return nil
}
