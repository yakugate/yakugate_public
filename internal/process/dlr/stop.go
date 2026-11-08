package dlr

import (
	"context"
	"sync"
	"time"

	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
)

// stopFor is how long a stopping dlr role waits for what it has in flight:
// vendor callbacks being answered, receipt pushes to customers' dlr_url out,
// a no-receipt answer or an orphan being applied. Under the supervisor's own
// 20 s.
const stopFor = 12 * time.Second

// pushStopFor bounds the push lanes inside stopFor, so their log line comes
// before the role's.
const pushStopFor = stopFor - time.Second

// Stop lets the work in flight finish before the process ends
// (worker.Stopper; Saleem 2026-10-14). Run used to return the moment the
// door's listener closed, and the process went away with pushes out: a push
// cut half way was never acked, so the customer got the receipt again when
// its lease ended — and the door's own 5 s drain was cut too.
func (p *Process) Stop() {
	// The tries made so far are written before the role goes.
	defer p.tries.Close()
	if !waitFor(&p.drain, stopFor) {
		p.log.Printf("stopping with work still in flight after %s: a push not answered is sent again when its lease ends", stopFor)
	}
	// Acks the queue store refused are made once more; what is still kept
	// goes with the process, and those receipts are posted again.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if n := p.acks.Flush(ctx); n > 0 {
		p.log.Printf("stopping with %d delivered receipt(s) the queue store would not take off: they are posted again when their lease ends", n)
	}
	cancel()
	// Receipts the record store could not take wait in memory (dlr hold.go).
	if n := dlrsvc.HeldReceipts(); n > 0 {
		p.log.Printf("STOPPING WITH %d RECEIPT(S) HELD for the record store, which could not take them: they are lost with this stop", n)
	}
}

// waitFor waits for wg up to limit and reports whether it finished.
func waitFor(wg *sync.WaitGroup, limit time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	t := time.NewTimer(limit)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}
