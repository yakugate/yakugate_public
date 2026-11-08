package pipeline

import "context"

// --- stage 12/18: sender-id / shortcode enforcement --- [SCOPE P1-11]
type senderIDStage struct{}

func (senderIDStage) Name() string { return "sender-id" }
func (senderIDStage) Run(_ context.Context, e *Envelope) *Reject {
	a := e.Account
	if senderAllotted(a.AllowAllSC, a.Shortcodes, e.Msg.Sender) {
		return nil
	}
	return reject(403, "SENDER_NOT_ALLOWED")
}
