package dlr

import (
	"context"
	"sync"
	"time"
)

// The receipts a customer's webhook took are taken off the queue together
// (step 3 of the DAL plan, 30 Oct 2026). Each push used to make its own ack:
// with the queues in MySQL that is one DELETE and one COMMIT a receipt, and a
// commit waits for the disk — 4,230 acks of 62 ms each in one rig run of
// 3 minutes, every one holding a push slot of its account's lane. Now the
// first push of a moment opens a batch, waits ackBatchEvery for the pushes
// beside it, and acks them all in one call; the others only add their id.
// A receipt is acked at most ackBatchEvery later than before, far inside
// its 60 s lease. With Redis the call is two round trips whatever the
// number, as before.

// ackBatchEvery is how long the first receipt of a batch waits for others.
var ackBatchEvery = 20 * time.Millisecond

// ackBatchMost is the most ids one ack call carries; a batch that reaches it
// is acked at once.
const ackBatchMost = 500

// ackBatch gathers the ids of receipts their webhooks took.
type ackBatch struct {
	mu   sync.Mutex
	ids  []int64
	full chan struct{} // closed when the open batch reaches ackBatchMost
	open bool
}

// ackPosted adds id to the batch. The caller that opened the batch waits for it
// and makes the ack (by the role's AckKeeper, so a refused ack is kept and
// made again); the others return at once. It reports whether this caller
// made the ack, how many it carried, and the error of a batch the keeper
// could not even keep.
func (p *Process) ackPosted(id int64) (made bool, n int, err error) {
	b := &p.ackBatch
	b.mu.Lock()
	b.ids = append(b.ids, id)
	if b.open {
		if len(b.ids) == ackBatchMost {
			close(b.full)
		}
		b.mu.Unlock()
		return false, 0, nil
	}
	b.open = true
	full := make(chan struct{})
	b.full = full
	b.mu.Unlock()

	t := time.NewTimer(ackBatchEvery)
	select {
	case <-t.C:
	case <-full:
		t.Stop()
	}
	b.mu.Lock()
	ids := b.ids
	b.ids, b.open, b.full = nil, false, nil
	b.mu.Unlock()
	// Not cut by a stop: the customers have these receipts, and unacked
	// they would be posted again at their lease end.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return true, len(ids), p.acks.Ack(ctx, p.queue, ids...)
}
