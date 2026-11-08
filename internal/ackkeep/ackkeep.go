// Package ackkeep takes sent messages off their vendor queue, and keeps
// trying when the queue store refuses.
//
// A vendor role sends a message, writes its record, then acks it on the
// queue. When that ack failed — the queue store was restarting — the error
// was logged and nothing more: the message stayed claimed, its lease ran out
// 30 seconds later and it was claimed and SENT AGAIN. Go-live test P2 E3
// (2026-10-09) sent 7 messages twice that way during a seven-second Redis
// restart. A Keeper holds the refused ids and acks them again until the store
// takes them.
package ackkeep

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Acker is the queue call a Keeper makes.
type Acker interface {
	Ack(ctx context.Context, vendor string, msgIDs ...string) error
}

const (
	// retryEvery is how often kept acks are made again.
	retryEvery = 250 * time.Millisecond
	// giveUpAfter is how long an ack is kept. A claimed message is the
	// queue's again when its lease ends, so much later than this the ack
	// would take away a message someone else is now sending.
	giveUpAfter = 90 * time.Second
	// keepMost bounds the ids held, over all vendors.
	keepMost = 500000
	// ackChunk bounds one ack call.
	ackChunk = 1000
)

// Keeper acks, and acks again what was refused.
type Keeper struct {
	q Acker
	// Say reports what the keeper did for one vendor; failed marks a line
	// that needs a person's eye. Nil = silent.
	Say func(vendor string, failed bool, text string)

	every, giveUp time.Duration

	mu      sync.Mutex
	kept    map[string][]string // vendor -> ids not yet acked
	since   map[string]time.Time
	n       int
	running bool
}

// New makes a Keeper over a queue. q may be nil (a role with no queue): Ack
// then does nothing.
func New(q Acker, say func(vendor string, failed bool, text string)) *Keeper {
	return &Keeper{q: q, Say: say, every: retryEvery, giveUp: giveUpAfter,
		kept: map[string][]string{}, since: map[string]time.Time{}}
}

// Ack takes the messages off the vendor's queue. When the queue store refuses
// they are kept and acked again in the background, and nil is returned: the
// caller has nothing more to do. An error means they could not even be kept.
func (k *Keeper) Ack(ctx context.Context, vendor string, msgIDs ...string) error {
	if k == nil || k.q == nil || len(msgIDs) == 0 {
		return nil
	}
	err := k.q.Ack(ctx, vendor, msgIDs...)
	if err == nil {
		return nil
	}
	k.mu.Lock()
	if k.n+len(msgIDs) > keepMost {
		k.mu.Unlock()
		return fmt.Errorf("ack of %d message(s) refused and too many are kept already (%d): %w", len(msgIDs), keepMost, err)
	}
	first := len(k.kept[vendor]) == 0
	k.kept[vendor] = append(k.kept[vendor], msgIDs...)
	k.n += len(msgIDs)
	if first {
		k.since[vendor] = time.Now()
	}
	start := !k.running
	k.running = true
	k.mu.Unlock()
	if first {
		k.say(vendor, false, fmt.Sprintf("the queue store refused an ack (%v): the sent messages are kept and taken off the queue when it is back", err))
	}
	if start {
		go k.loop()
	}
	return nil
}

// Kept is how many acks are waiting.
func (k *Keeper) Kept() int {
	if k == nil {
		return 0
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.n
}

func (k *Keeper) say(vendor string, failed bool, text string) {
	if k.Say != nil {
		k.Say(vendor, failed, text)
	}
}

// loop acks what is kept until nothing is, then ends.
func (k *Keeper) loop() {
	t := time.NewTicker(k.every)
	defer t.Stop()
	for range t.C {
		k.mu.Lock()
		vendors := make([]string, 0, len(k.kept))
		for v := range k.kept {
			vendors = append(vendors, v)
		}
		k.mu.Unlock()
		for _, v := range vendors {
			k.retry(v)
		}
		k.mu.Lock()
		if k.n == 0 {
			k.running = false
			k.mu.Unlock()
			return
		}
		k.mu.Unlock()
	}
}

// retry acks one vendor's kept ids, a chunk at a time, stopping at the first
// refusal.
func (k *Keeper) retry(vendor string) {
	k.mu.Lock()
	ids := append([]string(nil), k.kept[vendor]...)
	since := k.since[vendor]
	k.mu.Unlock()
	done := 0
	var err error
	for done < len(ids) {
		end := min(done+ackChunk, len(ids))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = k.q.Ack(ctx, vendor, ids[done:end]...)
		cancel()
		if err != nil {
			break
		}
		done = end
	}
	gaveUp := err != nil && time.Since(since) >= k.giveUp
	k.mu.Lock()
	// Ids kept since the copy was taken stay; the first `done` are acked.
	rest := k.kept[vendor][done:]
	if gaveUp {
		rest = k.kept[vendor][len(ids):]
	}
	k.n -= len(k.kept[vendor]) - len(rest)
	if len(rest) == 0 {
		delete(k.kept, vendor)
		delete(k.since, vendor)
	} else {
		k.kept[vendor] = append([]string(nil), rest...)
		if gaveUp {
			k.since[vendor] = time.Now()
		}
	}
	k.mu.Unlock()
	switch {
	case gaveUp:
		k.say(vendor, true, fmt.Sprintf("%d sent message(s) could not be taken off the queue for %s (%v): they will be SENT AGAIN when their lease ends",
			len(ids)-done, k.giveUp, err))
	case err == nil && len(ids) > 0:
		k.say(vendor, false, fmt.Sprintf("%d sent message(s) taken off the queue after the queue store came back", len(ids)))
	}
}
