package pipeline

import "context"

// --- stage 5/18: whitelist-only mode --- [SCOPE P1-05W]
// When the account has any whitelist entries, it may ONLY send to numbers on
// its whitelist; every other destination is blocked (sandbox/DND).
//
// is_whitelist_number was dropped as a separate column (2026-07-28): a
// non-empty WhitelistNumbers set now IMPLIES enforcement, same pattern as
// AllowedIPs in stage_ipallow.go.
type whitelistStage struct{}

func (whitelistStage) Name() string { return "whitelist" }
func (whitelistStage) Run(_ context.Context, e *Envelope) *Reject {
	if len(e.Account.WhitelistNumbers) == 0 {
		return nil
	}
	if _, ok := e.Account.WhitelistNumbers[e.Msg.Destination]; ok {
		return nil
	}
	return reject(403, "NOT_WHITELISTED")
}
