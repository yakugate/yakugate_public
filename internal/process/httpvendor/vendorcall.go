package httpvendor

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/mask"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// An HTTP vendor's call kept raw with its answer (hot/0077, Saleem 5 Nov
// 2026: "check if we save vendor responses and requests in raw for mt, if
// not then save them too, as they are also https"). The adapter keeps each
// call it makes for the send (httpadapter.WithCallRecorder); the one whose
// answer counted goes on every sub_ row the call answered: method, URL,
// headers and bodies, secrets masked, cut at 16/8 KB.
//
// The request carries the message text, so a message's own privacy holds:
// an encrypted company's call has both bodies (and a GET's query) sealed
// under its company's key — not kept at all without encryption (fail closed);
// a masked account's digits are masked in the bodies and the URL. A bulk call
// carries many messages: when one of them is encrypted every copy is sealed,
// each under its own message's company key; when one is masked every copy is
// masked.

// The modes of YAKU_VENDOR_CALL_LOG: which answers keep their call. The sub_
// row itself is always written.
const (
	vendorCallAll    = "all"
	vendorCallFailed = "failed"
	vendorCallOff    = "off"
)

// vendorCallMode reads YAKU_VENDOR_CALL_LOG; anything else is all.
func vendorCallMode() string {
	switch m := strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_VENDOR_CALL_LOG"))); m {
	case vendorCallFailed, vendorCallOff:
		return m
	}
	return vendorCallAll
}

// sendFacts is one call to a vendor: when it went, when it was answered, and
// the call as it went for each message it carried (nil = not kept).
type sendFacts struct {
	sentAt, at time.Time
	calls      map[string]*model.VendorCall
	failedOnly bool
}

// callFor is the call kept on msgID's answer of this result.
func (s sendFacts) callFor(msgID, result string) *model.VendorCall {
	if s.calls == nil || (s.failedOnly && result == model.AnswerAccepted) {
		return nil
	}
	return s.calls[msgID]
}

// sendFactsOf is a call's facts for the messages it carried: the newest call
// rec kept (the one whose answer counted), with each message's privacy.
func (p *Process) sendFactsOf(ctx context.Context, rec *httpadapter.CallRecorder, msgs []model.Message, sentAt, at time.Time) sendFacts {
	s := sendFacts{sentAt: sentAt, at: at, failedOnly: p.callMode == vendorCallFailed}
	if p.callMode == vendorCallOff {
		return s
	}
	cc, ok := rec.Last()
	if !ok {
		return s
	}
	base := model.VendorCall{Method: cc.Method, URL: cc.URL, ReqHeaders: cc.ReqHeaders, ReqBody: cc.ReqBody, ReqCut: cc.ReqCut,
		RespHeaders: cc.RespHeaders, RespBody: cc.RespBody, RespCut: cc.RespCut, TookMS: cc.Took.Milliseconds()}
	masked, sealed := false, false
	for _, m := range msgs {
		switch dal.PrivacyOf(m) {
		case model.PrivacyEncrypted:
			sealed = true
		case model.PrivacyMasked:
			masked = true
		}
	}
	if masked {
		n := mask.DefaultMinDigits
		base.ReqBody = mask.Digits(base.ReqBody, n)
		base.URL = mask.Digits(base.URL, n)
		base.RespBody = mask.Digits(base.RespBody, n)
	}
	s.calls = make(map[string]*model.VendorCall, len(msgs))
	if !sealed {
		c := base
		for _, m := range msgs {
			s.calls[m.ID] = &c
		}
		return s
	}
	for _, m := range msgs {
		c := p.sealCall(ctx, base, m)
		s.calls[m.ID] = &c
	}
	return s
}

// sealCall is c with both bodies encrypted under m's company key, a GET's
// query moved into the sealed request body ("?<query>"); a body that cannot
// be sealed is not kept (fail closed).
func (p *Process) sealCall(ctx context.Context, c model.VendorCall, m model.Message) model.VendorCall {
	c.Sealed = true
	if base, q, ok := strings.Cut(c.URL, "?"); ok {
		c.URL = base
		if c.ReqBody != "" {
			c.ReqBody = "?" + q + "\n\n" + c.ReqBody
		} else {
			c.ReqBody = "?" + q
		}
	}
	for _, b := range []*string{&c.ReqBody, &c.RespBody} {
		if *b == "" {
			continue
		}
		if p.cry == nil {
			*b = ""
			continue
		}
		s, err := p.seal(ctx, m, *b)
		if err != nil {
			*b = ""
			continue
		}
		*b = s
	}
	return c
}

// seal encrypts text under m's company key.
func (p *Process) seal(ctx context.Context, m model.Message, text string) (string, error) {
	return p.cry.Encrypt(ctx, m.AccountID, m.CompanyID, text)
}
