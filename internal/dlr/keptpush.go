package dlr

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// Client receipts the queue store refused are kept here and filed again.
//
// Go-live test P2 E3 (2026-10-09): Redis was restarted while a client sent
// over SMPP. Nothing in Redis was lost. But 45 receipts arrived from the
// vendor in the seven seconds Redis was away: each message was closed on its
// record, the push to the client's receipt queue failed ("LOADING Redis is
// loading the dataset in memory"), the error was logged, and nobody tried
// again — "accepted were 9810 but dlr recvd were 9770".
//
// So a refused push is kept in memory, in order, and filed when the store
// takes it. While any are kept, new receipts go behind them. The receipts of
// ONE message always keep their order (its ACCEPTD never follows its DELIVRD);
// two receipts of different messages may change places when the store comes
// back in the middle of a pass. What it does not cover: a role that STOPS
// while it is keeping receipts loses them (they are in memory only); the
// messages are final on their records either way.

const (
	// keptRetryEvery is how often kept receipts are filed again.
	keptRetryEvery = 500 * time.Millisecond
	// keptMost bounds what one role keeps. Past it a push fails as it did
	// before, and is logged.
	keptMost = 200000
	// keptChunk bounds one filing. Under what either queue store files in
	// one transaction (Redis 250, MySQL 500), so a store that goes away again
	// half way leaves a chunk of ordinary receipts filed whole or not at all.
	keptChunk = 200
	// keptSayEvery spaces the "still waiting" line of a long outage.
	keptSayEvery = 30 * time.Second
)

// keptNow counts the receipts kept by every pusher of this process, for the
// roles' status pages.
var keptNow atomic.Int64

// KeptClientReceipts is how many client receipts this process is keeping
// because the queue store refused them. 0 in a healthy gateway.
func KeptClientReceipts() int64 { return keptNow.Load() }

// queueBehind files pushes behind the receipts already kept, when there are
// any. queued=false means nothing is kept: push as usual.
func (p *QueuePusher) queueBehind(pushes []Push) (queued bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.kept) == 0 {
		return false, nil
	}
	if len(p.kept)+len(pushes) > keptMost {
		return true, fmt.Errorf("%d client receipt(s) not queued: the queue store is refusing them and %d are kept already", len(pushes), len(p.kept))
	}
	p.kept = append(p.kept, pushes...)
	keptNow.Add(int64(len(pushes)))
	return true, nil
}

// keep takes the pushes the queue store just refused. It returns nil — they
// will be filed — or the store's error when no more can be kept.
func (p *QueuePusher) keep(pushes []Push, cause error) error {
	p.mu.Lock()
	if len(p.kept)+len(pushes) > keptMost {
		p.mu.Unlock()
		return fmt.Errorf("the queue store refused %d client receipt(s) and %d are kept already: %w", len(pushes), keptMost, cause)
	}
	first := len(p.kept) == 0
	for _, x := range pushes {
		x.maybeFiled = true // refused, but perhaps written all the same (notFiledYet)
		p.kept = append(p.kept, x)
	}
	keptNow.Add(int64(len(pushes)))
	if first {
		p.since = time.Now()
	}
	start := !p.flushing
	p.flushing = true
	p.mu.Unlock()
	if first {
		log.Printf("[dlr] the queue store refused a client receipt (%v): kept, and filed again when the store is back", cause)
	}
	if start {
		go p.fileKept()
	}
	return nil
}

// fileKept files the kept receipts, oldest first, until none are left.
func (p *QueuePusher) fileKept() {
	every := p.every
	if every <= 0 {
		every = keptRetryEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	filed, dropped := 0, 0
	said := time.Now()
	for range t.C {
		for {
			p.mu.Lock()
			n := min(len(p.kept), keptChunk)
			batch := append([]Push(nil), p.kept[:n]...)
			p.mu.Unlock()
			if n == 0 {
				break
			}
			// A receipt the store refused may be in it all the same: the
			// connection dropped after the insert reached it (go-live test
			// 18 Oct 2026, P7 F4: a client told twice after a MySQL cut).
			fresh, gone, err := p.notFiledYet(batch)
			if err != nil {
				break // the store is still away: next tick
			}
			if gone > 0 {
				p.mu.Lock()
				// Only this goroutine touches the front of kept.
				p.kept = append(fresh, p.kept[n:]...)
				p.mu.Unlock()
				keptNow.Add(-int64(gone))
				filed += gone
				continue
			}
			batch = fresh
			settled, ok, bad := p.fileChunk(batch)
			filed += ok
			dropped += len(bad)
			if settled > 0 {
				p.mu.Lock()
				// fileChunk may have filed one receipt ahead of its place:
				// batch is the order they were settled in. Only this
				// goroutine touches the front of kept.
				copy(p.kept, batch)
				p.kept = p.kept[settled:]
				p.mu.Unlock()
				keptNow.Add(-int64(settled))
			}
			for _, x := range bad {
				log.Printf("[dlr] client receipt for %s (%s) was refused by the queue store on its own and is dropped: %v",
					x.push.Receipt.MsgID, x.push.Receipt.Status, x.err)
				// Its database work is not the client receipt's fault: it is
				// queued by itself, so the message's record still gets its
				// row and its final status.
				p.writeAlone(x.push)
			}
			if settled < n {
				break // the store is still away: next tick
			}
		}
		p.mu.Lock()
		left, since := len(p.kept), p.since
		if left == 0 {
			p.flushing = false
			p.kept = nil
			p.mu.Unlock()
			log.Printf("[dlr] the queue store is back: %d kept client receipt(s) filed after %s%s", filed,
				time.Since(since).Round(100*time.Millisecond), droppedNote(dropped))
			return
		}
		p.mu.Unlock()
		if time.Since(said) >= keptSayEvery {
			said = time.Now()
			log.Printf("[dlr] the queue store still refuses client receipts: %d kept for %s", left, time.Since(since).Round(time.Second))
		}
	}
}

// QueuedChecker is an optional Enqueuer capability: whether client receipts
// are in the queue already (account, client message id, status, word). The
// MySQL queue store has it; a Redis transaction is filed whole or not at all.
type QueuedChecker interface {
	ClientDLRsQueued(ctx context.Context, ds []model.ClientDLR) ([]bool, error)
}

// notFiledYet asks the store, for the kept pushes it refused (maybeFiled),
// whether it has them after all: those are dropped from the batch (gone), the
// others are filed as usual and not asked about again. A store that cannot
// say keeps the old behaviour.
func (p *QueuePusher) notFiledYet(batch []Push) (fresh []Push, gone int, err error) {
	qc, can := p.q.(QueuedChecker)
	if !can {
		return batch, 0, nil
	}
	var ask []model.ClientDLR
	var at []int
	for i, x := range batch {
		if x.maybeFiled && x.Write == nil {
			ask = append(ask, clientReceipts(x.AccountID, x.Receipt, nil)[0])
			at = append(at, i)
		}
	}
	if len(ask) == 0 {
		return batch, 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	have, err := qc.ClientDLRsQueued(ctx, ask)
	if err != nil {
		return nil, 0, err
	}
	there := map[int]bool{}
	for k, i := range at {
		there[i] = k < len(have) && have[k]
	}
	fresh = make([]Push, 0, len(batch))
	for i, x := range batch {
		if there[i] {
			gone++
			continue
		}
		x.maybeFiled = false
		fresh = append(fresh, x)
	}
	return fresh, gone, nil
}

// writeAlone queues the database work of a push whose client receipt was
// dropped.
func (p *QueuePusher) writeAlone(x Push) {
	if x.Write == nil {
		return
	}
	we, ok := p.q.(WriteEnqueuer)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := we.EnqueueClientDLRsAndWrites(ctx, nil, []ReceiptWrite{*x.Write}); err != nil {
		log.Printf("[dlr] the record work of the receipt for %s could not be queued either and is lost: %v", x.Write.MsgID, err)
	}
}

func droppedNote(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf(" (%d dropped: refused on their own)", n)
}

// refusedPush is one receipt the store would not take while it took others.
type refusedPush struct {
	push Push
	err  error
}

// fileChunk files one chunk, in order. settled is how many receipts from the
// FRONT of the chunk are done with — filed (ok of them) or dropped (bad); the
// rest stay kept for the next tick. It may move one receipt of batch ahead of
// its place (see below): batch is then in the order they were settled.
//
// When the store refuses a receipt, it is either away or that one receipt is
// at fault, and the two must not be confused: keeping a faulty receipt for
// ever would hold every later one back, and dropping a good one because the
// store was away is the loss this file exists to stop. So a refused receipt is
// only dropped when the store takes ANOTHER one and then refuses this one
// AGAIN — it is plainly up, and plainly will not have it. If the other one is
// refused too, the store is away: stop, nothing is dropped.
//
// The other one is the next receipt of another MESSAGE. It is filed before
// the refused one, so it must not be a later receipt of the same message: a
// store that came back between the two calls would then hand the client that
// message's receipts in the wrong order.
func (p *QueuePusher) fileChunk(batch []Push) (settled, ok int, bad []refusedPush) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if be, can := p.q.(BatchEnqueuer); can {
		if err := p.pushBatchNow(ctx, be, batch); err == nil {
			return len(batch), len(batch), nil
		}
		// The batch was refused: the store is away, or one receipt in it is
		// at fault. One by one tells which.
	}
	i := 0
	for i < len(batch) {
		x := batch[i]
		err := p.pushOne(ctx, x)
		if err == nil {
			ok++
			i++
			continue
		}
		j := otherMessage(batch, i)
		if j < 0 {
			break // nothing after it to tell the two apart: kept, tried again
		}
		other := batch[j]
		if p.pushOne(ctx, other) != nil {
			break // the other one is refused too: the store is away
		}
		ok++ // the other one is filed: the store is up
		copy(batch[i+1:j+1], batch[i:j])
		batch[i] = other
		if err := p.pushOne(ctx, x); err != nil {
			bad = append(bad, refusedPush{push: x, err: err})
		} else {
			ok++ // it only met the end of the outage
		}
		i += 2
	}
	return i, ok, bad
}

// otherMessage is the place of the first receipt after i that is about
// another message than batch[i]'s, or -1.
func otherMessage(batch []Push, i int) int {
	for j := i + 1; j < len(batch); j++ {
		if batch[j].AccountID != batch[i].AccountID || batch[j].Receipt.MsgID != batch[i].Receipt.MsgID {
			return j
		}
	}
	return -1
}
