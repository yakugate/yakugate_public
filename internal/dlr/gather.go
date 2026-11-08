package dlr

import (
	"context"
	"hash/fnv"
	"time"
)

// Gather applies receipts that arrive ONE AT A TIME, each from a caller of its
// own — a vendor's HTTP callback is one request a receipt — many at a time.
//
// The SMPP vendor role has its receipts waiting on its own workers and hands
// them to ApplyMany together. The dlr role's door has no such queue: every
// callback applied its receipt by itself, five round trips to Redis each, on
// the request's own goroutine (load run of 2026-10-13: the role used most of a
// core at 1,600 receipts a second). Here a caller hands its receipt to the
// worker that owns the vendor's message id and waits for the answer; the
// worker applies what has gathered while it was busy in one call. A receipt
// that comes alone is applied at once, alone — nobody waits to fill a batch.
//
// One worker owns a message id, so a message's receipts are applied in the
// order they arrived.
type Gather struct {
	svc   *Service
	lanes []chan gatherAsk
}

type gatherAsk struct {
	in   ReceiptIn
	done chan gatherAnswer
}

type gatherAnswer struct {
	ok  bool
	err error
}

// gatherBatch is the most receipts one call applies; gatherFor how long one
// batch may take (it belongs to no single request).
const (
	gatherBatch = 200
	gatherFor   = 30 * time.Second
)

// NewGather starts workers for svc. nil svc, or fewer than one worker, gives
// nil — and a nil Gather applies every receipt directly.
func NewGather(svc *Service, workers int) *Gather {
	if svc == nil || workers < 1 {
		return nil
	}
	g := &Gather{svc: svc, lanes: make([]chan gatherAsk, workers)}
	for i := range g.lanes {
		ch := make(chan gatherAsk, 4*gatherBatch)
		g.lanes[i] = ch
		go g.work(ch)
	}
	return g
}

func (g *Gather) work(ch chan gatherAsk) {
	for first := range ch {
		batch := []gatherAsk{first}
	more:
		for len(batch) < gatherBatch {
			select {
			case a := <-ch:
				batch = append(batch, a)
			default:
				break more
			}
		}
		in := make([]ReceiptIn, len(batch))
		for i, a := range batch {
			in[i] = a.in
		}
		ctx, cancel := context.WithTimeout(context.Background(), gatherFor)
		oks, errs := g.svc.ApplyMany(ctx, in)
		cancel()
		for i, a := range batch {
			a.done <- gatherAnswer{ok: oks[i], err: errs[i]}
		}
	}
}

// Apply applies one receipt and returns what Service.Apply returns for it:
// whether it was matched to a message, and an error. If ctx ends while the
// receipt waits it is still applied; the caller is told ctx's error.
func (g *Gather) Apply(ctx context.Context, svc *Service, in ReceiptIn) (bool, error) {
	if g == nil {
		grace := correlateGrace()
		return svc.apply(ctx, receiptKey{vendorID: in.VendorID, id: in.VendorMsgID, alt: in.AltID, number: in.Number},
			in.RawStatus, in.ErrorCode, in.Raw, in.StatusMap, doneTime(in.VendorID, in.VendorMsgID, in.DoneAt), grace, true)
	}
	if in.Arrived.IsZero() {
		in.Arrived = time.Now()
	}
	h := fnv.New32a()
	h.Write([]byte(in.VendorMsgID))
	ask := gatherAsk{in: in, done: make(chan gatherAnswer, 1)}
	select {
	case g.lanes[int(h.Sum32()%uint32(len(g.lanes)))] <- ask:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	select {
	case a := <-ask.done:
		return a.ok, a.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}
