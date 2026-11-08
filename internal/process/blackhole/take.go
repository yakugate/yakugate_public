package blackhole

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/giveup"
	"github.com/yakugate/yakugate/internal/model"
)

// A take removes what it takes from the vendor's queue (issue 22 of the
// design audit, 27 Oct 2026): a message past its validity is refused when it
// is taken (internal/giveup), and a batch whose records cannot be written goes
// back on its queue, as it was, to be taken again.

type enqueuer interface {
	Enqueue(ctx context.Context, m model.Message) error
}

type returner interface {
	Return(ctx context.Context, ms ...model.Message) error
}

type capReader interface {
	MaxAttempts(vendor string) int
}

// take takes up to n of a vendor's messages and refuses at once those past
// their validity or out of tries: the rest are the pretend network's.
func (p *Process) take(ctx context.Context, cid string, n int) ([]model.Message, error) {
	msgs, err := p.queue.Claim(ctx, cid, n)
	if len(msgs) == 0 {
		return nil, err
	}
	max := 0
	if c, ok := p.queue.(capReader); ok {
		max = c.MaxAttempts(cid)
	}
	send, expired, exhausted := giveup.Sort(msgs, time.Now(), max)
	if len(expired)+len(exhausted) > 0 {
		d := giveup.Deps{Rec: p.rec(), Push: p.pusher, Bill: p.bill, Logf: p.log.Printf}
		if q, ok := p.queue.(enqueuer); ok {
			d.Requeue = q.Enqueue
		}
		for _, m := range expired {
			d.Expired(ctx, m)
		}
		for _, m := range exhausted {
			d.Exhausted(ctx, m)
		}
	}
	return send, err
}

// putBack returns a batch whose records could not be written to its queue.
func (p *Process) putBack(ctx context.Context, ms []model.Message) {
	r, ok := p.queue.(returner)
	if !ok || len(ms) == 0 {
		return
	}
	if err := r.Return(context.WithoutCancel(ctx), ms...); err != nil {
		p.log.Printf("LOST: %d message(s) could not be recorded nor put back on their queue: %v", len(ms), err)
	}
}
