package httpvendor

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// What a client is told when the vendor answers a send: the code of a refusal,
// and the account's receipt policy applied at submit and at refusal.

// rejectionCode is what the record's error_code and the client's receipt
// `err:` carry when the vendor refused the message outright (a PERMANENT
// response, GW-15): the vendor's exact HTTP status as HTTP_<status> — the
// same shape as the SMPP side's SMPP_<hex> (PO 2026-09-17). Until then the
// record said VENDOR_REJECTED and the receipt said 001, and the status was
// in the log only. A permanent error without a status (none expected today)
// keeps the old word.
func rejectionCode(err error) string {
	if code, ok := httpadapter.RejectionCode(err); ok {
		return fmt.Sprintf("HTTP_%d", code)
	}
	return "VENDOR_REJECTED"
}

// refused gives messages the vendor refused for good their code and the
// vendor's own answer, which their records keep (hot/0064, Saleem 2026-10-15:
// "i cant see the reason of failure? i.e any response or error code"). The
// answer is the error's text: "vendor rejected (http 400): <its body>".
func refused(msgs []model.Message, err error) {
	_, text := vendorFailure(err)
	code := rejectionCode(err)
	for i := range msgs {
		msgs[i].ErrorCode, msgs[i].ErrorText = code, text
	}
}

// stamped is a message with the client's version of its outcome written onto
// it (so the record write files it), plus that outcome for the caller to send
// once the record exists. [receipt policy, config/0119]
type stamped struct {
	msg     model.Message
	outcome *receiptpolicy.Outcome
}

// tellOnSubmit applies the account's rule that closes a message this vendor
// has just taken (on_accepted, or a progress rule with a final word). An HTTP
// vendor is the main reason the rule exists: most answer the send call and
// then never send a receipt at all, so without it the customer's system waits
// for a final status that never comes.
func (p *Process) tellOnSubmit(m model.Message) stamped {
	a, rule := p.policies.For(m.AccountID).Submitted()
	return p.markTold(stampTold(m, a, rule))
}

// tellOnRefusal applies the account's rule for the code a refusal carries
// (HTTP_400, VENDOR_REJECTED…).
func (p *Process) tellOnRefusal(m model.Message, code string) stamped {
	a, rule := p.policies.For(m.AccountID).Refused(code)
	return p.markTold(stampTold(m, a, rule))
}

// markTold keeps "told once" for a final answer the account's policy gave:
// the mark in Redis (the logs are written once, issue 14), set before the
// record is written, so a receipt the vendor sends later is recorded and not
// pushed.
func (p *Process) markTold(st stamped) stamped {
	if st.outcome != nil && st.outcome.Final() && p.svc != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		p.svc.MarkTold(ctx, st.msg, *st.outcome)
	}
	return st
}

// levels is an account's dlr_levels setting — which intermediate receipts its
// clients get (config/0120). Read from the same per-account cache as the
// receipt policy, so both settings reach this role on one refresh.
func (p *Process) levels(accountID int64) string {
	return p.policies.LevelsFor(accountID)
}

// stampTold decides one answer and writes it onto the message. A nil action —
// no policy, or no rule for this case — leaves the message untouched, so a
// gateway with no policies behaves exactly as it did before they existed.
func stampTold(m model.Message, a *receiptpolicy.Action, rule string) stamped {
	if a == nil {
		return stamped{msg: m}
	}
	o := receiptpolicy.Decide(a, rule, nil)
	if o.Status == "" {
		return stamped{msg: m}
	}
	// A progress word (ACCEPTD, ENROUTE) is a receipt and nothing more: the
	// caller sends it, and the record keeps saying only what really happened
	// (2026-10-12). A final word is what the client was TOLD.
	if o.Final() {
		m.ToldStatus, m.ToldErr, m.ToldBy = o.Status, o.Err, o.Rule
	}
	return stamped{msg: m, outcome: &o}
}
