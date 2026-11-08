package pipeline

import (
	"context"

	"github.com/yakugate/yakugate/internal/refdata"
)

// --- stage 6/18: country / prefix rules --- [SCOPE P1-06]
type countryStage struct{ ref *refdata.Mem }

func (countryStage) Name() string { return "country" }
func (s countryStage) Run(_ context.Context, e *Envelope) *Reject {
	if s.ref.CountryDenied(e.Account.ID, e.Msg.Destination) {
		// 403, not 400: policy refusal, uniform with the other policy blocks
		// (P1 scope finalize 2026-08-09).
		return reject(403, "COUNTRY_BLOCKED")
	}
	return nil
}
