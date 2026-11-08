package mo

import (
	"sync"
	"time"
)

// stopFor is how long a stopping mo role waits for what it has in flight:
// vendor MO callbacks being answered and pushes to customers' mo_url out.
// Under the supervisor's own 20 s.
const stopFor = 12 * time.Second

// pushStopFor bounds the push lanes inside stopFor, so their log line comes
// before the role's.
const pushStopFor = stopFor - time.Second

// Stop lets the work in flight finish before the process ends
// (worker.Stopper; Saleem 2026-10-14). Run used to return the moment the
// door's listener closed, and the process went away with pushes out: a push
// cut half way was never recorded, so the customer got the message again
// when its lease ended — and the door's own 5 s drain was cut too.
func (p *Process) Stop() {
	// The tries made so far are written before the role goes.
	defer p.tries.Close()
	if waitFor(&p.drain, stopFor) {
		return
	}
	p.log.Printf("stopping with work still in flight after %s: a push not answered is sent again when its lease ends", stopFor)
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
