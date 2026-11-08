package dlr

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Receipts the record store could not take, kept and applied again (go-live
// test of 18 Oct 2026).
//
// On the record store's path a receipt was applied once: an error was handed
// back to whoever took it from the vendor, which logged it — the dlr role's
// door answered the vendor 200 all the same, and the receipt was gone. Two
// errors did it: the message's record was not written yet ("sql: no rows":
// its vendor role's write a batch away, the DB worker behind, the record
// queued on the other deployment) and MySQL away ("driver: bad connection").
// 2,090 of 9,000 messages kept `sent` for good in one test.
//
// Now nothing of a receipt is let go:
//
//   - its record is not there yet: the receipt is filed as an orphan, with
//     the reason, and the re-match sweep applies it once the record is there
//     (Service.keepForLater) — nothing of it was done, so applying it again
//     from the start is safe;
//   - the record store cannot be reached, before anything was done: the same
//     orphan, and when the orphan cannot be written either, its filing waits
//     here and is tried again;
//   - the record store went away part way, after the receipt took its
//     message's closing mark: what is left of the work waits here, with what
//     was done already, and is finished when the store is back (closeJob).
//
// What waits here is in memory: a role STOPPED while MySQL is away loses it
// (the count is on the role's status, and said when it stops).

const (
	// heldRetryEvery is how often held work is tried again.
	heldRetryEvery = 2 * time.Second
	// heldMost bounds what one process holds. Past it the receipt's error
	// goes back to its caller, as before.
	heldMost = 100000
	// heldKeepFor is how long held work is tried before it is let go, loudly.
	heldKeepFor = 24 * time.Hour
	// heldSayEvery spaces the "still holding" line of a long outage.
	heldSayEvery = 30 * time.Second
)

// heldNow counts the work held by every Service of this process.
var heldNow atomic.Int64

// HeldReceipts is how many receipts this process holds because the record
// store could not take them. 0 in a healthy gateway.
func HeldReceipts() int64 { return heldNow.Load() }

// held is one piece of a receipt's work, done again until it succeeds.
type held struct {
	what  string // for the log: which receipt
	since time.Time
	do    func(ctx context.Context) error
}

// holdList is a Service's held work.
type holdList struct {
	mu      sync.Mutex
	work    []*held
	running bool
	every   time.Duration // 0 = heldRetryEvery (tests set less)
}

// hold keeps work to be done again. It returns an error only when no more
// can be held: the caller then reports the receipt's own error, as before.
func (s *Service) hold(what string, cause error, do func(ctx context.Context) error) error {
	h := &s.held
	h.mu.Lock()
	if len(h.work) >= heldMost {
		h.mu.Unlock()
		return fmt.Errorf("%s: %d receipt(s) are held for the record store already: %w", what, heldMost, cause)
	}
	first := len(h.work) == 0
	h.work = append(h.work, &held{what: what, since: time.Now(), do: do})
	heldNow.Add(1)
	start := !h.running
	h.running = true
	h.mu.Unlock()
	if first {
		log.Printf("[dlr] the record store could not take a receipt (%v): %s is held, and applied when the store takes it", cause, what)
	}
	if start {
		go s.doHeld()
	}
	return nil
}

// doHeld tries the held work again, oldest first, until none is left. A pass
// stops at the first that fails: the store is still away.
func (s *Service) doHeld() {
	h := &s.held
	every := h.every
	if every <= 0 {
		every = heldRetryEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	said, done := time.Now(), 0
	for range t.C {
		for {
			h.mu.Lock()
			if len(h.work) == 0 {
				h.running = false
				h.mu.Unlock()
				log.Printf("[dlr] the record store is back: %d held receipt(s) applied", done)
				return
			}
			w := h.work[0]
			h.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := w.do(ctx)
			cancel()
			if err != nil && time.Since(w.since) < heldKeepFor {
				break // still away: next tick
			}
			if err != nil {
				log.Printf("[dlr] %s was held for %s and is let go: %v", w.what, heldKeepFor, err)
			} else {
				done++
			}
			h.mu.Lock()
			h.work = h.work[1:]
			h.mu.Unlock()
			heldNow.Add(-1)
		}
		h.mu.Lock()
		left := len(h.work)
		h.mu.Unlock()
		if left > 0 && time.Since(said) >= heldSayEvery {
			said = time.Now()
			log.Printf("[dlr] the record store still cannot take receipts: %d held", left)
		}
	}
}

// heldCount is how many pieces of work this Service holds (tests).
func (s *Service) heldCount() int {
	s.held.mu.Lock()
	defer s.held.mu.Unlock()
	return len(s.held.work)
}
