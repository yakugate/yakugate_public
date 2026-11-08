package dlr

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Taking delivered client receipts off their queue, and trying again when
// the queue store refuses (retest of 19 Oct 2026, a8: 55 receipts given twice
// to one SMPP client, 59.6 s apart).
//
// A client receipt is claimed with a lease (60 s), pushed — a deliver_sm the
// client answered, or a webhook that answered 2xx — and then acked: taken off
// the queue. The ack was made once. When the store refused it (with the
// queues in MySQL, a deadlock on client_dlr under load, or no free
// connection within its 5 s), smpp-server said nothing and the dlr role only
// logged it: the receipt stayed claimed, its lease ran out and it was handed
// over to the client a second time. An AckKeeper keeps the refused ids and
// acks them again until the store takes them.

// ClientAcker takes delivered client receipts off their queue (both queue
// stores' AckClientDLR).
type ClientAcker interface {
	AckClientDLR(ctx context.Context, ids ...int64) error
}

// AckKeeper acks delivered client receipts, and acks again what the queue
// store refused. One per role; safe for concurrent use.
type AckKeeper struct {
	// say reports what the keeper did; failed marks a line that needs a
	// person's eye. Nil = silent.
	say func(failed bool, text string)

	every, giveUp time.Duration

	mu      sync.Mutex
	kept    []keptAcks
	n       int
	running bool
}

// keptAcks are ids one ack call was refused for, with the store it is made to.
type keptAcks struct {
	q     ClientAcker
	ids   []int64
	since time.Time
	err   error
}

// Retry timing. A delivered receipt's claim leases it for 60 s: an ack made
// again within that time is in time, and one made later still takes the
// receipt off before a later claim can hand it out, or after (then it removes
// nothing). It is given up when the store has refused it this long.
var (
	keptAckEvery  = 250 * time.Millisecond
	keptAckGiveUp = 10 * time.Minute
)

const (
	// keptAckMost bounds the ids held: past it a refused ack is handed back
	// to the caller as before (the receipt is handed over again at its lease
	// end).
	keptAckMost = 200000
	// keptAckChunk bounds one ack call.
	keptAckChunk = 1000
)

// NewAckKeeper makes a keeper. say may be nil.
func NewAckKeeper(say func(failed bool, text string)) *AckKeeper {
	return &AckKeeper{say: say, every: keptAckEvery, giveUp: keptAckGiveUp}
}

// Ack takes the delivered receipts off q. When the store refuses, they are
// kept and acked again in the background, and nil is returned: the receipts
// are delivered and the caller has nothing more to do. An error means they
// could not even be kept (too many are kept already), and the caller's
// receipts are handed over again at their lease end, as before.
func (k *AckKeeper) Ack(ctx context.Context, q ClientAcker, ids ...int64) error {
	if len(ids) == 0 || q == nil {
		return nil
	}
	err := q.AckClientDLR(ctx, ids...)
	if err == nil || k == nil {
		return err
	}
	k.mu.Lock()
	if k.n+len(ids) > keptAckMost {
		k.mu.Unlock()
		return fmt.Errorf("ack of %d delivered receipt(s) refused and %d are kept already: %w", len(ids), keptAckMost, err)
	}
	first := k.n == 0
	k.kept = append(k.kept, keptAcks{q: q, ids: append([]int64(nil), ids...), since: time.Now(), err: err})
	k.n += len(ids)
	start := !k.running
	k.running = true
	k.mu.Unlock()
	if first {
		k.note(false, fmt.Sprintf("the queue store refused to take %d delivered client receipt(s) off the queue (%v): kept, and taken off when it does — the client is not given them again", len(ids), err))
	}
	if start {
		go k.loop()
	}
	return nil
}

// Kept is how many delivered receipts wait to be taken off their queue.
func (k *AckKeeper) Kept() int {
	if k == nil {
		return 0
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.n
}

// Flush makes the kept acks once more, now — a role that stops calls it last
// (what it still keeps is lost with it, and those receipts are handed over
// again at their lease end). It returns how many are still kept.
func (k *AckKeeper) Flush(ctx context.Context) int {
	if k == nil {
		return 0
	}
	k.pass(ctx)
	return k.Kept()
}

func (k *AckKeeper) note(failed bool, text string) {
	if k.say != nil {
		k.say(failed, text)
	}
}

// loop acks what is kept until nothing is, then ends.
func (k *AckKeeper) loop() {
	t := time.NewTicker(k.every)
	defer t.Stop()
	for range t.C {
		k.pass(context.Background())
		k.mu.Lock()
		if k.n == 0 {
			k.running = false
			k.mu.Unlock()
			return
		}
		k.mu.Unlock()
	}
}

// pass acks every kept group, oldest first, a chunk at a time; a group the
// store still refuses stays (or is given up when it has been refused for
// giveUp). Groups of one store are joined first: a store that is slow makes
// many small refusals, and one call each would only add to its load.
func (k *AckKeeper) pass(ctx context.Context) {
	k.mu.Lock()
	groups := joinKept(k.kept)
	k.kept = nil
	k.mu.Unlock()
	var left []keptAcks
	acked := 0
	var since time.Time
	for _, g := range groups {
		done := 0
		var err error
		for done < len(g.ids) {
			end := min(done+keptAckChunk, len(g.ids))
			actx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = g.q.AckClientDLR(actx, g.ids[done:end]...)
			cancel()
			if err != nil {
				break
			}
			done = end
		}
		acked += done
		if since.IsZero() || g.since.Before(since) {
			since = g.since
		}
		if done == len(g.ids) {
			continue
		}
		g.ids, g.err = g.ids[done:], err
		if time.Since(g.since) >= k.giveUp {
			k.note(true, fmt.Sprintf("%d delivered client receipt(s) could not be taken off the queue for %s (%v): the client is given them AGAIN when their lease ends",
				len(g.ids), k.giveUp, err))
			k.mu.Lock()
			k.n -= len(g.ids)
			k.mu.Unlock()
			continue
		}
		left = append(left, g)
	}
	k.mu.Lock()
	k.n -= acked
	k.kept = append(left, k.kept...) // kept meanwhile go behind
	k.mu.Unlock()
	if acked > 0 && len(left) == 0 {
		k.note(false, fmt.Sprintf("%d delivered client receipt(s) taken off the queue after the store refused them (in %s)", acked, time.Since(since).Round(time.Millisecond)))
	}
}

// joinKept joins the groups of one store into one, in the order kept, with
// the oldest group's time and the last error. A store that cannot be told
// apart from another (not comparable) leaves the groups as they are.
func joinKept(groups []keptAcks) (joined []keptAcks) {
	if len(groups) < 2 {
		return groups
	}
	defer func() {
		if recover() != nil {
			joined = groups
		}
	}()
	var out []keptAcks
	at := map[ClientAcker]int{}
	for _, g := range groups {
		i, ok := at[g.q]
		if !ok {
			at[g.q] = len(out)
			out = append(out, keptAcks{q: g.q, ids: append([]int64(nil), g.ids...), since: g.since, err: g.err})
			continue
		}
		out[i].ids = append(out[i].ids, g.ids...)
		if g.since.Before(out[i].since) {
			out[i].since = g.since
		}
		out[i].err = g.err
	}
	return out
}
