package pipeline

import (
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/refdata"
)

// KeepRejectText says whether a REFUSED message's text may go to the reject
// log (hot/0057, Saleem 2026-10-06: "can't see text message in rejected
// messages"), by the privacy setting of the account's company — the same
// setting that decides what is kept of its accepted messages:
//
//	plain_text      kept
//	pii_masked      kept with its digit runs hidden (Message.MaskBody)
//	all_encrypted   not kept: a refused message is never sealed, so keeping
//	                it would store plain what the company asked to have
//	                encrypted
//
// A refusal before the account is known (acct nil) keeps nothing: there is
// no company whose setting could be asked. Called by both edges where they
// record a refusal, so HTTP and SMPP cannot drift.
func KeepRejectText(m *model.Message, acct *refdata.Account) {
	if m == nil {
		return
	}
	m.KeepRejectBody = false
	if acct == nil {
		return
	}
	switch acct.BodyPrivacy {
	case "all_encrypted":
		return
	case "pii_masked":
		m.MaskBody = true
	}
	m.KeepRejectBody = true
}
