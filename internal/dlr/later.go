package dlr

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// A receipt whose facts are not in Redis is looked for again (Saleem, 29 Oct
// 2026: "if receipt has no key in redis, wait for 5 sec and check again total
// 3 time check, 2 retries, if not found put in orphans, no mysql calls").
//
// Looks are made at 0, 5 and 10 s. The receipt waits in memory meanwhile, on
// a list a goroutine of the service works through; the reader that brought it
// in is never held. A receipt that beat its message's facts by a flush (the
// vendor answered and receipted in the same moment) is found on the second
// look. One looked for while Redis could not be read waits for Redis, its
// look not counted.

const (
	// lookTries is how many looks a receipt gets before it is an orphan.
	lookTries = 3
	// lookAgainEvery is the time between two looks.
	lookAgainEvery = 5 * time.Second
	// laterMost bounds what one process keeps waiting for a look. Past it a
	// receipt is filed as an orphan at once.
	laterMost = 200000
)

// laterNow counts the receipts waiting for a look in this process.
var laterNow atomic.Int64

// ReceiptsLookedForAgain is how many receipts this process keeps waiting to be
// looked for again.
func ReceiptsLookedForAgain() int64 { return laterNow.Load() }

// laterItem is one receipt waiting for its next look.
type laterItem struct {
	r    ReceiptIn
	look int
	due  time.Time
}

// laterList is a Service's receipts waiting for a look.
type laterList struct {
	mu      sync.Mutex
	items   []laterItem
	running bool
	// every is the time between looks (0 = lookAgainEvery; tests set less).
	every time.Duration
}

// SetLookAgain changes the time between two looks for a receipt whose facts
// are not there yet (tests).
func (s *Service) SetLookAgain(d time.Duration) { s.later.every = d }

func (l *laterList) wait() time.Duration {
	if l.every > 0 {
		return l.every
	}
	return lookAgainEvery
}

// lookLater keeps r for its next look. trouble: Redis could not be read —
// the look it waits for is the same one again.
func (s *Service) lookLater(r ReceiptIn, look int, trouble bool) error {
	if look > lookTries && !trouble {
		look = lookTries
	}
	l := &s.later
	l.mu.Lock()
	if len(l.items) >= laterMost {
		l.mu.Unlock()
		err := fmt.Errorf("%d receipts already wait for a look", laterMost)
		if !trouble {
			return s.fileOrphan(context.Background(), r, "no message with this vendor_msg_id")
		}
		return err
	}
	if r.Arrived.IsZero() {
		r.Arrived = time.Now()
	}
	l.items = append(l.items, laterItem{r: r, look: look, due: time.Now().Add(l.wait())})
	laterNow.Add(1)
	start := !l.running
	l.running = true
	l.mu.Unlock()
	if start {
		go s.looks()
	}
	return nil
}

// looks works through the receipts waiting for a look, until none is left.
func (s *Service) looks() {
	l := &s.later
	tick := l.wait() / 10
	if tick < 5*time.Millisecond {
		tick = 5 * time.Millisecond
	}
	if tick > time.Second {
		tick = time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		l.mu.Lock()
		var due []laterItem
		kept := l.items[:0]
		for _, it := range l.items {
			if now.Before(it.due) {
				kept = append(kept, it)
				continue
			}
			due = append(due, it)
		}
		l.items = kept
		if len(l.items) == 0 && len(due) == 0 {
			l.running = false
			l.mu.Unlock()
			return
		}
		l.mu.Unlock()
		laterNow.Add(-int64(len(due)))
		for _, it := range due {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if _, err := s.applyRedis(ctx, it.r, it.look); err != nil {
				log.Printf("[dlr] receipt %s looked for again: %v", it.r.VendorMsgID, err)
			}
			cancel()
		}
	}
}

// laterCount is how many receipts this Service keeps for a look (tests).
func (s *Service) laterCount() int {
	s.later.mu.Lock()
	defer s.later.mu.Unlock()
	return len(s.later.items)
}
