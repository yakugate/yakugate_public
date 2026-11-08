package httpvendor

import (
	"context"
	"sort"
	"time"

	"github.com/yakugate/yakugate/internal/giveup"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queues"
)

// A take removes what it takes from the vendor's queue (issue 22 of the
// design audit, 27 Oct 2026): nothing is leased, so a message is never "the
// queue's again when its lease ends". What this role takes and does not see
// through goes back by name:
//   - a message the vendor did not take now (an answer that is not a
//     permanent refusal, a call that failed) and that cannot go to the next
//     vendor of its round, or one whose text cannot be opened: one more
//     attempt, back after its gap, or refused when its tries are used up
//     (tryAgain);
//   - the parts of a long message not all there yet: back a little later,
//     their attempt as it was (later).
// A message past its validity or out of tries is refused when it is taken
// (take, internal/giveup).

type splitTaker interface {
	ClaimSplit(ctx context.Context, vendor string, n int, mode queues.Mode) ([]model.Message, error)
}

type enqueuer interface {
	Enqueue(ctx context.Context, m model.Message) error
}

type capReader interface {
	MaxAttempts(vendor string) int
}

// partsWait is how long the parts of a long message wait on their queue for
// the rest of their parts.
const partsWait = 5 * time.Second

// take takes up to n of a vendor's messages, its rate split between its
// queues the vendor's way, and refuses at once those past their validity or
// out of tries: the rest are to be sent.
func (p *Process) take(ctx context.Context, v vendorAdapter, n int) ([]model.Message, error) {
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

// tryAgain counts one more try of each message and puts it back after its
// gap, or refuses it when its tries are used up.
func (p *Process) tryAgain(ctx context.Context, v vendorAdapter, ms ...model.Message) {
	d, max := p.refuser(), p.maxAttempts(v.cid)
	for _, m := range ms {
		d.Again(ctx, m, max)
	}
}

// later puts messages back for a moment, their attempt as it was.
func (p *Process) later(ctx context.Context, ms []model.Message, after time.Duration) {
	d := p.refuser()
	for _, m := range ms {
		d.Later(ctx, m, after)
	}
}

func (p *Process) maxAttempts(cid string) int {
	if c, ok := p.queue.(capReader); ok {
		return c.MaxAttempts(cid)
	}
	return 0
}

// refuser refuses and puts back messages with this role's record, receipt
// and billing paths.
func (p *Process) refuser() giveup.Deps {
	d := giveup.Deps{Rec: p.rec(), Push: p.svc.Pusher(), Bill: p.bill, Policies: p.policies, Logf: p.log.Printf}
	if q, ok := p.queue.(enqueuer); ok {
		d.Requeue = q.Enqueue
	}
	return d
}

// withParts is one taken part of a long message with its siblings taken in
// the same round, once each, in part order.
func withParts(first model.Message, more []model.Message) []model.Message {
	seen := map[string]bool{}
	var out []model.Message
	for _, m := range append([]model.Message{first}, more...) {
		if !seen[m.ID] {
			seen[m.ID] = true
			out = append(out, m)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ConcatSeq < out[j].ConcatSeq })
	return out
}
