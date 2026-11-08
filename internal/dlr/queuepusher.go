package dlr

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// QueuePusher, the default client pusher: a matched receipt queued for its
// client, one at a time or many in a batch.

// Enqueuer is the queue-store surface used by the default client pusher.
type Enqueuer interface {
	EnqueueClientDLR(ctx context.Context, d model.ClientDLR) error
}

// PartIDStore is an optional Enqueuer capability: the ids an SMPP client was
// given for each part of a long message it sent in parts, saved by the
// smpp-server when the parts were joined (PO 2026-09-11). Keyed by the joined
// message's id, which is part 1's.
type PartIDStore interface {
	ClientPartIDs(ctx context.Context, msgID string) ([]string, error)
	DropClientPartIDs(ctx context.Context, msgID string) error
}

// QueuePusher is the default ClientPusher: it enqueues the receipt to client_dlr;
// the smpp-server (for bound clients) and an HTTP dlr_url poster drain it and
// deliver to the client. [SCOPE P5 smsc leg]
//
// A receipt the queue store refuses — it was restarting — is KEPT and filed
// again until the store takes it (keptpush.go). The error used to be handed
// back and only logged: the message was final on its record and the client
// was never told.
type QueuePusher struct {
	q Enqueuer

	mu       sync.Mutex
	kept     []Push    // refused by the queue store, oldest first
	since    time.Time // when the first of them was refused
	flushing bool      // the goroutine that files them again is running
	every    time.Duration
	// handoff parks a receipt whose client is on another gateway (issue 5,
	// handoff.go); nil = every receipt is filed here.
	handoff HandOff
}

// NewQueuePusher wraps a queue store as a ClientPusher.
func NewQueuePusher(q Enqueuer) *QueuePusher { return &QueuePusher{q: q} }

// PushToClient enqueues the client-facing receipt — one per id the client
// holds for this message.
//
// A message an SMPP client sent in parts was joined into one message carrying
// part 1's id, but the client was given an id for EVERY part and waits for a
// receipt on each (PO 2026-09-11). So when the store knows the part ids, each
// gets its own receipt with the same outcome. The lookup is only made for a
// message that came in on an SMPP bind (BindSlot > 0): nothing else can have
// part ids, and HTTP traffic should not pay for a read it can never use.
//
// The part-id row is dropped once a FINAL receipt has been queued for every
// part. A duplicate final receipt after that reaches part 1 only — the parts
// already have theirs.
func (p *QueuePusher) PushToClient(ctx context.Context, accountID int64, r Receipt) error {
	one := p.divert(ctx, []Push{{AccountID: accountID, Receipt: r}})
	if len(one) == 0 {
		return nil // parked for the gateway its client is on
	}
	// Receipts are already waiting for the queue store: this one goes behind
	// them, so a message's receipts stay in the order they were made.
	if queued, err := p.queueBehind(one); queued {
		return err
	}
	if err := p.pushOne(ctx, one[0]); err != nil {
		return p.keep(one, err)
	}
	return nil
}

// WriteEnqueuer is an optional Enqueuer capability: the client's receipts and
// the database work of the vendor receipts they come from, queued in ONE step
// — both are queued, or neither is. The queue store has it when the DB
// worker's queue is in the same Redis (recordq.WithReceipts).
type WriteEnqueuer interface {
	EnqueueClientDLRsAndWrites(ctx context.Context, ds []model.ClientDLR, ws []ReceiptWrite) error
}

// BothPusher is an optional ClientPusher capability: a matched receipt's two
// outputs — the client's receipt, and the receipt's database work — filed
// together (Saleem 2026-10-13: "send to customer and db insertion queue, in
// both queues at the time").
type BothPusher interface {
	// OneStep says whether this pusher's queue can take both together.
	OneStep() bool
	// PushWithWrite queues the client's receipt and w in one step. A queue
	// store that is away keeps both, in order, and files them when it is
	// back (keptpush.go).
	PushWithWrite(ctx context.Context, accountID int64, r Receipt, w ReceiptWrite) error
}

// OneStep: see BothPusher.
func (p *QueuePusher) OneStep() bool {
	_, ok := p.q.(WriteEnqueuer)
	return ok
}

// PushWithWrite is PushToClient with the receipt's database work riding in
// the same step. Only for a pusher whose OneStep is true.
func (p *QueuePusher) PushWithWrite(ctx context.Context, accountID int64, r Receipt, w ReceiptWrite) error {
	one := p.divert(ctx, []Push{{AccountID: accountID, Receipt: r, Write: &w}})
	if queued, err := p.queueBehind(one); queued {
		return err
	}
	if err := p.pushOne(ctx, one[0]); err != nil {
		return p.keep(one, err)
	}
	return nil
}

// errNoOneStep is a push that carries database work for a queue that cannot
// take it: a wiring fault, never a state of the store.
var errNoOneStep = errors.New("this queue cannot take a receipt's database work with it")

// pushOne files one receipt now: PushToClient without the keeping.
func (p *QueuePusher) pushOne(ctx context.Context, x Push) error {
	if x.writeOnly {
		// Its client receipt was parked for another gateway (handoff.go): only
		// its database work, which is this gateway's, is queued here.
		we, ok := p.q.(WriteEnqueuer)
		if !ok || x.Write == nil {
			return errNoOneStep
		}
		return we.EnqueueClientDLRsAndWrites(ctx, nil, []ReceiptWrite{*x.Write})
	}
	accountID, r := x.AccountID, x.Receipt
	ps, canParts := p.q.(PartIDStore)
	var parts []string
	if canParts && r.BindSlot > 0 {
		if got, err := ps.ClientPartIDs(ctx, r.MsgID); err == nil && len(got) > 1 {
			parts = got
		}
	}
	if x.Write != nil {
		we, ok := p.q.(WriteEnqueuer)
		if !ok {
			return errNoOneStep
		}
		if err := we.EnqueueClientDLRsAndWrites(ctx, clientReceipts(accountID, r, parts), []ReceiptWrite{*x.Write}); err != nil {
			return err
		}
	} else {
		for _, d := range clientReceipts(accountID, r, parts) {
			if err := p.q.EnqueueClientDLR(ctx, d); err != nil {
				return err
			}
		}
	}
	if parts != nil && closesParts(r) {
		_ = ps.DropClientPartIDs(ctx, r.MsgID) // best effort: a leftover row is swept after a week
	}
	return nil
}

// clientReceipts is what one receipt becomes in the client's queue: one entry
// for the message's own id, or — for a message an SMPP client sent in parts —
// one for every part id it was given. PushToClient and PushBatch both build
// from here, so the two can never tell a client different things.
func clientReceipts(accountID int64, r Receipt, parts []string) []model.ClientDLR {
	ids := parts
	if len(ids) == 0 {
		ids = []string{r.MsgID}
	}
	done := r.DoneAt
	if done.IsZero() {
		// A held-back receipt reads as if it happened when it is delivered,
		// not when we decided it: a customer comparing the receipt's done
		// date with its arrival time must not see a gap we invented.
		done = time.Now().Add(r.Delay)
	}
	out := make([]model.ClientDLR, 0, len(ids))
	for _, id := range ids {
		out = append(out, model.ClientDLR{
			AccountID:   accountID,
			ClientMsgID: id,
			MsgID:       r.MsgID,
			Status:      string(r.Status),
			Stat:        string(r.Stat),
			Receipt:     ReceiptTextStat(id, r.Status, r.Stat, r.ErrCode, done),
			BindSlot:    r.BindSlot,
			Src:         r.Src,
			Dst:         r.Dst,
			Delay:       r.Delay,
			Owner:       r.Owner,
			DoneAt:      done,
		})
	}
	return out
}

// closesParts says whether r is the last receipt a message's parts will get,
// after which their saved ids are no longer needed.
func closesParts(r Receipt) bool {
	return r.Status.IsTerminal() && (r.Stat == "" || r.Stat.Final())
}

// Push is one receipt for PushBatch: whose it is, and what it says.
type Push struct {
	AccountID int64
	Receipt   Receipt
	// Write, when set, is the database work of the vendor receipt this one
	// comes from: it is queued in the same step as the client's receipt
	// (WriteEnqueuer), and kept with it when the queue store is away.
	Write *ReceiptWrite
	// maybeFiled: the queue store refused this push with an error that does
	// not say nothing was written (the connection dropped during the insert):
	// before it is filed again, the store is asked whether it has it
	// (keptpush.go).
	maybeFiled bool
	// writeOnly: the client receipt was parked for another gateway
	// (handoff.go); only Write is queued here.
	writeOnly bool
}

// BatchEnqueuer is an optional Enqueuer capability: many client receipts
// filed in a few round trips, in the order given.
type BatchEnqueuer interface {
	EnqueueClientDLRBatch(ctx context.Context, ds []model.ClientDLR) error
}

// ManyPartIDStore is an optional PartIDStore capability: the part ids of many
// messages in one read. A message that was not sent in parts is not in the
// answer.
type ManyPartIDStore interface {
	ClientPartIDsMany(ctx context.Context, msgIDs []string) (map[string][]string, error)
}

// PushBatch queues many client receipts at once, in order — what PushToClient
// does for each, in a few round trips for all of them. The blackhole role
// pushed one at a time (a part-id read, an id and a write per message) and
// that alone held it under two hundred messages a second (2026-10-07).
//
// A store that cannot take a batch gets them one by one.
//
// What the queue store refuses is kept and filed again, as in PushToClient.
func (p *QueuePusher) PushBatch(ctx context.Context, pushes []Push) error {
	if pushes = p.divert(ctx, pushes); len(pushes) == 0 {
		return nil
	}
	if queued, err := p.queueBehind(pushes); queued {
		return err
	}
	be, ok := p.q.(BatchEnqueuer)
	if !ok {
		for i, x := range pushes {
			if err := p.pushOne(ctx, x); err != nil {
				return p.keep(pushes[i:], err) // this one and those after it, in order
			}
		}
		return nil
	}
	if err := p.pushBatchNow(ctx, be, pushes); err != nil {
		return p.keep(pushes, err)
	}
	return nil
}

// pushBatchNow files a batch through a store that takes batches: PushBatch
// without the keeping.
func (p *QueuePusher) pushBatchNow(ctx context.Context, be BatchEnqueuer, pushes []Push) error {
	// Only a message that came in on an SMPP bind can have part ids.
	ps, canParts := p.q.(PartIDStore)
	parts := map[string][]string{}
	if canParts {
		var ask []string
		seen := map[string]bool{}
		for _, x := range pushes {
			if x.Receipt.BindSlot > 0 && !seen[x.Receipt.MsgID] {
				seen[x.Receipt.MsgID] = true
				ask = append(ask, x.Receipt.MsgID)
			}
		}
		if many, ok := p.q.(ManyPartIDStore); ok && len(ask) > 0 {
			if got, err := many.ClientPartIDsMany(ctx, ask); err == nil {
				parts = got
			}
		} else {
			for _, id := range ask {
				if got, err := ps.ClientPartIDs(ctx, id); err == nil {
					parts[id] = got
				}
			}
		}
	}
	ds := make([]model.ClientDLR, 0, len(pushes))
	var drop []string
	var writes []ReceiptWrite // the database work that rides with these receipts
	for _, x := range pushes {
		if x.writeOnly {
			// Its client receipt went to another gateway: its database work
			// stays here.
			if x.Write != nil {
				writes = append(writes, *x.Write)
			}
			continue
		}
		var own []string
		if x.Receipt.BindSlot > 0 && len(parts[x.Receipt.MsgID]) > 1 {
			own = parts[x.Receipt.MsgID]
			if closesParts(x.Receipt) {
				drop = append(drop, x.Receipt.MsgID)
			}
		}
		ds = append(ds, clientReceipts(x.AccountID, x.Receipt, own)...)
		if x.Write != nil {
			writes = append(writes, *x.Write)
		}
	}
	if len(writes) > 0 {
		we, ok := p.q.(WriteEnqueuer)
		if !ok {
			return errNoOneStep
		}
		if err := we.EnqueueClientDLRsAndWrites(ctx, ds, writes); err != nil {
			return err
		}
	} else if err := be.EnqueueClientDLRBatch(ctx, ds); err != nil {
		return err
	}
	for _, id := range drop {
		_ = ps.DropClientPartIDs(ctx, id) // best effort, as in PushToClient
	}
	return nil
}
