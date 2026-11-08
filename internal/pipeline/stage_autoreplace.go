package pipeline

import (
	"context"
	"strings"

	"github.com/yakugate/yakugate/internal/refdata"
)

// --- stage 7/18: auto-replace (per-user ordered find->replace) --- [SCOPE P1-08]
// Moved ahead of the template gate 2026-08-03 (chunk-5 ruling 5.4) so both
// content gates (template, spam) see the final, post-replacement text.
type autoReplaceStage struct{ ref *refdata.Mem }

func (autoReplaceStage) Name() string { return "auto-replace" }
func (s autoReplaceStage) Run(_ context.Context, e *Envelope) *Reject {
	if !e.Account.AutoReplace {
		return nil
	}
	body := e.Msg.Body
	for _, r := range s.ref.AutoReplace(e.Account.ID) {
		if r.Match != nil {
			body = r.Match.ReplaceAllString(body, r.Replace)
		} else if r.Literal != "" {
			body = strings.ReplaceAll(body, r.Literal, r.Replace)
		}
	}
	e.Msg.Body = body
	return nil
}
