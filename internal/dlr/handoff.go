package dlr

import (
	"context"
	"errors"
)

// A client receipt that arrives at the wrong gateway (issue 5; Saleem, 26 Oct
// 2026).
//
// A receipt goes to the queue of the gateway its client is on (Receipt.Owner,
// D-121), and each gateway may have a queue Redis of its own. A receipt that
// reaches the OTHER gateway — a vendor's callback to the other box, a receipt
// on a bind the other box holds, the gateway-wide status poll or re-match —
// was written into the receiving box's queue Redis, where its owner never
// looks: it stayed there for good. Now such a receipt is PARKED for its owner
// in the main Redis, which both see (internal/handoff), and the owner moves it
// into its own queue — expanding it to the part ids IT holds (issue 6). The
// receipt's database work is this gateway's and is queued here as before.

// HandOff is where a receipt for a client on another gateway goes.
type HandOff interface {
	// Foreign says owner's client queues are in another gateway's queue
	// store, so a receipt for it must be parked, not filed here.
	Foreign(owner string) bool
	// ParkReceipts parks receipts for their owners, in order. Never an error
	// for the caller: what cannot be parked now is held and parked later
	// (a receipt's message is final already; refusing it would lose it).
	ParkReceipts(ctx context.Context, rs []Parked)
}

// Parked is one receipt parked for another gateway, as it was pushed: not
// yet turned into its client receipts (the owner expands it to its own part
// ids), with the hold-back still to wait.
type Parked struct {
	AccountID int64   `json:"a"`
	Receipt   Receipt `json:"r"`
}

// SetHandOff makes the pusher park receipts whose client is on another
// gateway. nil = every receipt is filed here.
func (p *QueuePusher) SetHandOff(h HandOff) {
	if p != nil {
		p.handoff = h
	}
}

// divert takes out the pushes whose client is on another gateway and parks
// their receipts there. What stays is filed here: the rest, and the database
// work of a parked receipt (a write-only push).
func (p *QueuePusher) divert(ctx context.Context, pushes []Push) []Push {
	if p == nil || p.handoff == nil {
		return pushes
	}
	var park []Parked
	keep := make([]Push, 0, len(pushes))
	for _, x := range pushes {
		if x.writeOnly || !p.handoff.Foreign(x.Receipt.Owner) {
			keep = append(keep, x)
			continue
		}
		park = append(park, Parked{AccountID: x.AccountID, Receipt: x.Receipt})
		if x.Write != nil {
			x.writeOnly = true
			keep = append(keep, x)
		}
	}
	if len(park) > 0 {
		p.handoff.ParkReceipts(ctx, park)
	}
	return keep
}

// errNoQueue is a pusher with no queue store.
var errNoQueue = errors.New("no queue store")

// FileNow files receipts parked for THIS gateway into its own client queues,
// as PushBatch would — expanded to the part ids this gateway holds — but
// never keeps them and never parks them again. An error hands them back to
// the caller, which leaves them parked and tries again; a store that filed
// part of the batch before it failed (Redis files 250 a transaction) gives
// those receipts twice. The importer files 50 at a time.
func (p *QueuePusher) FileNow(ctx context.Context, rs []Parked) error {
	if p == nil || p.q == nil {
		return errNoQueue
	}
	pushes := make([]Push, len(rs))
	for i, r := range rs {
		pushes[i] = Push{AccountID: r.AccountID, Receipt: r.Receipt}
	}
	if be, ok := p.q.(BatchEnqueuer); ok {
		return p.pushBatchNow(ctx, be, pushes)
	}
	for _, x := range pushes {
		if err := p.pushOne(ctx, x); err != nil {
			return err
		}
	}
	return nil
}
