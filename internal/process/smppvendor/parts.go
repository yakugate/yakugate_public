package smppvendor

import (
	"fmt"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
	"github.com/yakugate/yakugate/lib/smpp/window"
)

// Every answer a vendor gives is a row of sub_ (issue 25.4): per part, per
// try — accepted, refused, not now, or no answer in time. A message sent as
// several PDUs is recorded once EVERY part has answered or timed out
// (issue 14, Saleem 22 and 26 Oct 2026):
//
//   - every part accepted, or a part with no answer and none refused: the
//     message is sent (its mt_ row), with every part's answer;
//   - any one part refused (or not taken: "try again"): the whole message is
//     refused (closed by the gateway's own receipt, or tried again while it
//     has tries left), with every part's answer, the accepted ones too.
//
// Until 26 Oct the record was written on the first part's answer and the
// other parts' answers were only logged.
//
// The first part's answer still decides at once when it is not "accepted"
// (refused, try again, a code action): the message is then refused or sent
// again whatever the others say, and their answers that come after are rows
// of their own.

// answerOf is a vendor's submit_sm_resp to one PDU, as its sub_ row.
func answerOf(v vendorConn, key sessionKey, pend *window.Pending[model.Message], pd *pdu.PDU, action string, now time.Time) model.SubmitAnswer {
	m := pend.Msg
	st := pd.Header.CommandStatus
	a := model.SubmitAnswer{
		MsgID: m.ID, AccountID: m.AccountID, CompanyID: m.CompanyID,
		Part: pend.WirePart, Parts: pend.WireParts, Attempt: model.TryOf(m),
		VendorID: v.id, VendorMsgID: pdu.DecodeSubmitSMResp(pd.Body), VendorProtocol: string(model.TransportSMPP),
		CommandStatus: st, HasStatus: true,
		Result: resultOf(action), SentAt: pend.SentAt, AnsweredAt: now,
		Deployment: deployment.ID(), Session: fmt.Sprintf("%s#%d", v.cid, key.Idx),
	}
	if st != pdu.ESME_ROK || a.Result != model.AnswerAccepted {
		a.Code = fmt.Sprintf("SMPP_%02X", st)
		a.Words = pdu.StatusName(st)
	}
	if a.Part < 1 {
		a.Part, a.Parts = 1, 1
	}
	return a
}

// timeoutAnswer is a PDU that had no answer within the response timeout.
func timeoutAnswer(v vendorConn, key sessionKey, pend *window.Pending[model.Message], now time.Time) model.SubmitAnswer {
	m := pend.Msg
	a := model.SubmitAnswer{
		MsgID: m.ID, AccountID: m.AccountID, CompanyID: m.CompanyID,
		Part: pend.WirePart, Parts: pend.WireParts, Attempt: model.TryOf(m),
		VendorID: v.id, VendorProtocol: string(model.TransportSMPP),
		Result: model.AnswerTimeout, Code: "TIMEOUT", Words: "no answer within the response timeout",
		SentAt: pend.SentAt, AnsweredAt: now,
		Deployment: deployment.ID(), Session: fmt.Sprintf("%s#%d", v.cid, key.Idx),
	}
	if a.Part < 1 {
		a.Part, a.Parts = 1, 1
	}
	return a
}

// resultOf is the sub_ result of a classified answer.
func resultOf(action string) string {
	switch action {
	case "success":
		return model.AnswerAccepted
	case "retry", "throttle":
		return model.AnswerNotNow
	}
	return model.AnswerRefused
}

// partGroup is one long message's answers while its parts are out.
type partGroup struct {
	total   int
	answers map[int]model.SubmitAnswer // by part
	// lead is set once the first part was accepted: what to record when
	// every part is in.
	lead *leadAnswer
	// refused is the first part that was not taken, "" = none so far.
	refused     string
	refusedPart int
	started     time.Time
}

// leadAnswer is what the first part's acceptance decided, kept until the
// other parts are in.
type leadAnswer struct {
	pend      window.Pending[model.Message]
	vmid      string
	latencyMs int64
}

// partsBook holds the answers of one session's long messages until each is
// whole. A message's group is made when its parts go on the wire; answers are
// read on the session's reader goroutine and timeouts on its tick: both write
// here.
type partsBook struct {
	mu     sync.Mutex
	groups map[string]*partGroup
}

func newPartsBook() *partsBook { return &partsBook{groups: map[string]*partGroup{}} }

// start opens a long message's group, before its first PDU is written.
func (b *partsBook) start(msgID string, total int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.groups[msgID] = &partGroup{total: total, answers: map[int]model.SubmitAnswer{}, started: time.Now()}
}

// whole says every part has an answer and the first was accepted.
func (g *partGroup) whole() bool { return g.lead != nil && len(g.answers) >= g.total }

// all is every part's answer, in part order.
func (g *partGroup) all() []model.SubmitAnswer {
	out := make([]model.SubmitAnswer, 0, len(g.answers))
	for i := 1; i <= g.total; i++ {
		if a, ok := g.answers[i]; ok {
			out = append(out, a)
		}
	}
	return out
}

// partsDone is a group that is whole: what to record.
type partsDone struct {
	lead    leadAnswer
	answers []model.SubmitAnswer
	// refused is the code of the first part not taken, "" = every part
	// accepted or had no answer.
	refused     string
	refusedPart int
}

// answer files a part's answer that is not the first part's. It returns the
// group when that made it whole; loose=true when the message is not waiting
// here (its first part decided otherwise, it was recorded already, or it was
// let go): the answer is a row of its own.
func (b *partsBook) answer(pend *window.Pending[model.Message], a model.SubmitAnswer) (d *partsDone, loose bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[pend.Msg.ID]
	if !ok {
		return nil, true
	}
	if prev, had := g.answers[pend.WirePart]; had && prev.Result != model.AnswerTimeout {
		return nil, true // answered already: a copy
	}
	// A late answer after the part's timeout is the part's answer.
	g.answers[pend.WirePart] = a
	if a.Result != model.AnswerAccepted && g.refused == "" {
		g.refused, g.refusedPart = a.Code, pend.WirePart
	}
	return b.takeIfWhole(pend.Msg.ID, g), false
}

// accepted files the first part's acceptance and returns the group when that
// made it whole. found=false: no group waits for this message (it was not
// sent here as parts): the caller records it alone.
func (b *partsBook) accepted(pend *window.Pending[model.Message], a model.SubmitAnswer, lead leadAnswer) (d *partsDone, found bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[pend.Msg.ID]
	if !ok {
		return nil, false
	}
	g.answers[pend.WirePart] = a
	g.lead = &lead
	return b.takeIfWhole(pend.Msg.ID, g), true
}

// decidedByLead takes the group of a message whose first part decided it
// otherwise (refused, try again, a code action): every answer it held,
// the first part's included, for the record that decision writes.
func (b *partsBook) decidedByLead(pend *window.Pending[model.Message], a model.SubmitAnswer) []model.SubmitAnswer {
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[pend.Msg.ID]
	if !ok {
		return []model.SubmitAnswer{a}
	}
	g.answers[pend.WirePart] = a
	delete(b.groups, pend.Msg.ID)
	return g.all()
}

// timedOut files a part that had no answer in time. It returns the group
// when that made it whole; loose=true when nothing waits here for it (a
// single-PDU message, or one recorded already): the timeout is a row of its
// own. A first part's timeout keeps its group waiting: its late answer is
// taken, or the message goes again when its lease ends (letGo).
func (b *partsBook) timedOut(pend *window.Pending[model.Message], a model.SubmitAnswer) (d *partsDone, loose bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[pend.Msg.ID]
	if !ok {
		return nil, true
	}
	if _, had := g.answers[pend.WirePart]; had {
		return nil, false
	}
	g.answers[pend.WirePart] = a
	if pend.IsLead() {
		return nil, false
	}
	return b.takeIfWhole(pend.Msg.ID, g), false
}

// letGo forgets a message whose first part never answered: it goes again
// when its lease ends. What its parts answered is returned, to be written.
func (b *partsBook) letGo(msgID string) []model.SubmitAnswer {
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[msgID]
	if !ok {
		return nil
	}
	delete(b.groups, msgID)
	return g.all()
}

// takeIfWhole removes and returns a group that is whole. Called locked.
func (b *partsBook) takeIfWhole(msgID string, g *partGroup) *partsDone {
	if !g.whole() {
		return nil
	}
	delete(b.groups, msgID)
	return &partsDone{lead: *g.lead, answers: g.all(), refused: g.refused, refusedPart: g.refusedPart}
}

// stale lets go of groups older than d — a session's parts wait no longer
// than their timeout and its late keep — and of every group when the session
// ends (d = 0): their answers are returned, to be written.
func (b *partsBook) stale(d time.Duration, now time.Time) []model.SubmitAnswer {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []model.SubmitAnswer
	for id, g := range b.groups {
		if d <= 0 || now.Sub(g.started) > d {
			out = append(out, g.all()...)
			delete(b.groups, id)
		}
	}
	return out
}
