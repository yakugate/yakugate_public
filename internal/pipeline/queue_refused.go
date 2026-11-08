package pipeline

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// queueRefusedSayEvery spaces the log lines of refused queue writes.
const queueRefusedSayEvery = 10 * time.Second

// queueRefused says in the log why the queue store refused a message: the
// client is told QUEUE_UNAVAILABLE and nothing more, and until the retest of
// 19 Oct 2026 the store's own error was written nowhere — a deadlock, a lock
// wait, a full pool and a store that is down all looked the same. One line
// at most every queueRefusedSayEvery, with how many it stood for.
type queueRefusedLog struct {
	mu    sync.Mutex
	last  time.Time
	quiet int
	print func(format string, args ...any)
}

var queueRefused = &queueRefusedLog{print: log.Printf}

func (q *queueRefusedLog) say(vendor string, err error) {
	q.mu.Lock()
	now := time.Now()
	if !q.last.IsZero() && now.Sub(q.last) < queueRefusedSayEvery {
		q.quiet++
		q.mu.Unlock()
		return
	}
	quiet := q.quiet
	q.quiet, q.last = 0, now
	q.mu.Unlock()
	more := ""
	if quiet > 0 {
		more = fmt.Sprintf(" (and %d more refused since the last line)", quiet)
	}
	q.print("[pipeline] the queue store refused a message for vendor %s: %v — the client was told QUEUE_UNAVAILABLE%s", vendor, err, more)
}

// acceptRecordFailed says in the log that an accepted message's record (mt_)
// could not be written at acceptance (the three-table design, 27 Oct 2026):
// the message is queued and the client has its id, and the vendor role's
// first answer writes the record. One line at most every
// queueRefusedSayEvery, with how many it stood for.
var acceptRecordFailed = &acceptRecordLog{queueRefusedLog{print: log.Printf}}

type acceptRecordLog struct{ q queueRefusedLog }

func (a *acceptRecordLog) say(vendor string, err error) {
	q := &a.q
	q.mu.Lock()
	now := time.Now()
	if !q.last.IsZero() && now.Sub(q.last) < queueRefusedSayEvery {
		q.quiet++
		q.mu.Unlock()
		return
	}
	quiet := q.quiet
	q.quiet, q.last = 0, now
	q.mu.Unlock()
	more := ""
	if quiet > 0 {
		more = fmt.Sprintf(" (and %d more since the last line)", quiet)
	}
	q.print("[pipeline] an accepted message for vendor %s is queued, but its record could not be written: %v — its vendor's first answer writes it%s", vendor, err, more)
}
