package httpvendor

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/giveup"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// Every answer a vendor gives is a row of sub_ (issue 25.4): what a call
// carrying a message answered — accepted, refused, not now, no answer in time.
// An HTTP vendor takes a message in ONE call (a long message too, its parts
// joined): each message the call carried is part 1 of 1.

// httpAnswer is a vendor call's answer for one message it carried. vmid is
// the vendor's id for it ("" when it gave none or refused); err is the call's
// failure (nil = accepted).
func httpAnswer(v vendorAdapter, m model.Message, vmid string, err error, sent sendFacts) model.SubmitAnswer {
	proto := string(m.Transport)
	if proto != "soap" {
		proto = string(model.TransportHTTP)
	}
	a := model.SubmitAnswer{
		MsgID: m.ID, AccountID: m.AccountID, CompanyID: m.CompanyID, Part: 1, Parts: 1, Attempt: model.TryOf(m),
		VendorID: v.id, VendorMsgID: vmid, VendorProtocol: proto, Result: model.AnswerAccepted,
		SentAt: sent.sentAt, AnsweredAt: sent.at, Deployment: deployment.ID(), Session: v.cid,
	}
	if err == nil {
		a.Call = sent.callFor(m.ID, a.Result)
		return a
	}
	a.VendorMsgID = ""
	a.Code, a.Words = vendorFailure(err)
	if n, e := strconv.Atoi(strings.TrimPrefix(a.Code, "HTTP_")); e == nil && strings.HasPrefix(a.Code, "HTTP_") {
		a.HTTPStatus = n
	}
	switch {
	case errors.Is(err, httpadapter.ErrPermanent):
		a.Result = model.AnswerRefused
	case a.Code == "TIMEOUT":
		a.Result = model.AnswerTimeout
	default:
		a.Result = model.AnswerNotNow
	}
	a.Call = sent.callFor(m.ID, a.Result)
	return a
}

// httpAnswers is a call's answer for each message it carried.
func httpAnswers(v vendorAdapter, msgs []model.Message, ids []string, err error, sent sendFacts) []model.SubmitAnswer {
	out := make([]model.SubmitAnswer, len(msgs))
	for i, m := range msgs {
		id := ""
		if i < len(ids) {
			id = ids[i]
		}
		out[i] = httpAnswer(v, m, id, err, sent)
	}
	return out
}

// writeLooseAnswers files answers that end nothing by themselves — "not now":
// the message goes on to the next vendor or back to the queue. Best effort,
// beside the message's own way on.
func (p *Process) writeLooseAnswers(ctx context.Context, cid string, as []model.SubmitAnswer) {
	rec := p.rec()
	if len(as) == 0 || rec == nil {
		return
	}
	// Through the DB worker's queue when the records go there (the
	// three-table design: every sub_ row is queued), else the store.
	if err := rec.WriteAnswers(context.WithoutCancel(ctx), as); err != nil {
		p.log.ForVendor(cid).Warnf("record", "%d vendor answer(s) not written: %v", len(as), err)
	}
}

// retryRefused tries again, at their gap, the messages of a call the vendor
// refused that still have tries left (Saleem, 27 Oct 2026: "if it has retries
// then retry else fails it"): each refusal is a sub_ row, the vendor's
// submission charge is booked, and the message goes back on its queue. It
// answers the messages that have no tries left: they fail.
func (p *Process) retryRefused(ctx context.Context, v vendorAdapter, ms []model.Message, sendErr error, sent sendFacts) []model.Message {
	max := p.maxAttempts(v.cid)
	var fail []model.Message
	var loose []model.SubmitAnswer
	for _, m := range ms {
		if !giveup.HasTriesLeft(m, max) {
			fail = append(fail, m)
			continue
		}
		v.mark(&m)
		p.bill.VendorAnswered(ctx, &m, v.id, false)
		loose = append(loose, httpAnswer(v, m, "", sendErr, sent))
		p.tryAgain(ctx, v, m)
	}
	p.writeLooseAnswers(ctx, v.cid, loose)
	return fail
}
