package smppvendor

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/giveup"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queues"
	"github.com/yakugate/yakugate/lib/smpp/window"
)

// A take removes what it takes from the vendor's queue (issue 22 of the
// design audit, 27 Oct 2026): nothing is leased, so a message is never "the
// queue's again when its lease ends". What this role takes and does not send
// is put back here, by name:
//   - taken and never put on the wire (a session that ended or held it too
//     long, a window with no room for every part): back at once, its attempt
//     as it was (putBack);
//   - sent and not taken by the vendor now (an SMPP "try again" or throttle,
//     a submit never answered, a send that broke): one more attempt, then
//     back after its gap — or refused when its tries are used up (tryAgain).
// A message past its validity or out of tries is refused when it is taken
// (refuseAtTake, internal/giveup).

// The queue store's take, put-back and enqueue (mysql.Store and redis.Store
// have them; a test's fake queue may not).
type splitTaker interface {
	ClaimSplit(ctx context.Context, vendor string, n int, mode queues.Mode) ([]model.Message, error)
}

type returner interface {
	Return(ctx context.Context, ms ...model.Message) error
}

type enqueuer interface {
	Enqueue(ctx context.Context, m model.Message) error
}

type capReader interface {
	MaxAttempts(vendor string) int
}

// putTimeout bounds a put-back, which also runs as a session ends.
const putTimeout = 5 * time.Second

// putBack returns messages taken and not sent to their queues, ready now.
func (p *Process) putBack(ctx context.Context, cid string, ms []model.Message) {
	if len(ms) == 0 {
		return
	}
	r, ok := p.queue.(returner)
	if !ok {
		return
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), putTimeout)
	defer cancel()
	if err := r.Return(cctx, ms...); err != nil {
		p.log.ForVendor(cid).Warnf("queue", "LOST: %d message(s) taken and not sent could not be put back on the queue: %v", len(ms), err)
	}
}

// heldMsgs are the messages of held sends.
func heldMsgs(hs []heldMsg) []model.Message {
	out := make([]model.Message, len(hs))
	for i, h := range hs {
		out[i] = h.msg
	}
	return out
}

// tryAgain counts one more try of a message the vendor did not take now and
// puts it back after its gap, or refuses it when its tries are used up
// (giveup.Deps.Again).
func (p *Process) tryAgain(ctx context.Context, v vendorConn, m model.Message) {
	p.refuser().Again(ctx, m, p.maxAttempts(v.cid))
}

// maxAttempts is the most times a message of the vendor may be sent (0 = no
// cap): its own max_attempts, else YAKU_MAX_ATTEMPTS (attemptcap.Apply).
func (p *Process) maxAttempts(cid string) int {
	if c, ok := p.queue.(capReader); ok {
		return c.MaxAttempts(cid)
	}
	return 0
}

// refuser refuses messages at the take, with this role's record, receipt and
// billing paths.
func (p *Process) refuser() giveup.Deps {
	d := giveup.Deps{Rec: p.rec(), Push: p.svc.Pusher(), Bill: p.bill, Policies: p.policies, Logf: p.log.Printf}
	if q, ok := p.queue.(enqueuer); ok {
		d.Requeue = q.Enqueue
	}
	return d
}

// take takes up to n of a vendor's messages, its rate split between its
// queues the vendor's way, and refuses at once those past their validity or
// out of tries: the rest are to be sent.
func (p *Process) take(ctx context.Context, v vendorConn, n int) ([]model.Message, error) {
	var msgs []model.Message
	var err error
	if st, ok := p.queue.(splitTaker); ok {
		msgs, err = st.ClaimSplit(ctx, v.cid, n, v.split)
	} else {
		msgs, err = p.queue.Claim(ctx, v.cid, n)
	}
	if len(msgs) == 0 {
		return nil, err
	}
	send, expired, exhausted := giveup.Sort(msgs, time.Now(), p.maxAttempts(v.cid))
	if len(expired)+len(exhausted) > 0 {
		d := p.refuser()
		for _, m := range expired {
			d.Expired(ctx, m)
		}
		for _, m := range exhausted {
			d.Exhausted(ctx, m)
		}
	}
	return send, err
}

// unanswered takes every send still without an answer out of a session's
// window and gives their messages, once each.
func unanswered(win *window.Window[model.Message]) []model.Message {
	now := time.Now()
	win.Expired(-1, now)
	seen := map[string]bool{}
	var out []model.Message
	for _, w := range win.DropLate(-1, now) {
		if seen[w.Msg.ID] {
			continue
		}
		seen[w.Msg.ID] = true
		out = append(out, w.Msg)
	}
	return out
}

// lateKeep is how long a send that timed out is kept for a late answer: one
// more response timeout. After it the message is tried again (tryAgain).
func lateKeep(respTimeout time.Duration) time.Duration { return respTimeout }
