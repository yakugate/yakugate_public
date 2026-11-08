package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// QueueHealth says whether the queue store answers, for readiness. Both
// accept roles use it: the http-server's /readyz and the smpp-server's
// ReadyForTraffic.
//
// Go-live test of 18 Oct 2026, P1 E3 (bug P1-8): /readyz said ready while the
// queue store was down — in all four outage runs (Redis cut, Redis stopped,
// MySQL queue cut, MySQL queue held) — and every send was refused 503, so a
// load balancer kept sending a gateway work it could not take. The first fix
// was the http-server's alone; the smpp-server answered ready the same way
// until the retest of 19 Oct (hand-off C-H2).
//
// The store is asked in the background (QueuePingEvery, each ask bounded by
// QueuePingTimeout), never on a request: readiness must answer at once even
// when the store hangs. Two failed asks in a row make the role not ready; one
// answer makes it ready again. A nil *QueueHealth is always up.
type QueueHealth struct {
	ping func(context.Context) error

	mu    sync.Mutex
	fails int
	err   error
}

const (
	QueuePingEvery   = 2 * time.Second
	QueuePingTimeout = time.Second
	queueDownAfter   = 2
)

// ErrNoQueueStore is the reason given when no queue store could be built (the
// DAL's own, which a MySQL queue store with no pool answers too).
var ErrNoQueueStore = dal.ErrNoQueueStore

// NewQueueHealth asks the queue store as it can be asked: its Ping (a Redis
// store's PING, a MySQL store's pool — the MySQL store's Ping is the ping of
// its pool this asked by hand until 29 Oct 2026). nil (or a store that cannot
// be asked) gives nil: readiness is then what it was before, up.
func NewQueueHealth(q any) *QueueHealth {
	switch s := q.(type) {
	case nil:
		return nil
	case interface{ Ping(context.Context) error }:
		return &QueueHealth{ping: s.Ping}
	}
	return nil
}

// NoQueueStore is the health of a role that could build no queue store at
// all: every send is refused, so it is never ready.
func NoQueueStore() *QueueHealth {
	return &QueueHealth{ping: func(context.Context) error { return ErrNoQueueStore }, fails: queueDownAfter, err: ErrNoQueueStore}
}

// Up reports the latest answer: false after two failed asks in a row.
func (h *QueueHealth) Up() bool {
	if h == nil {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fails < queueDownAfter
}

// Why is the last error while the store is taken as down, for /readyz; ""
// while it answers.
func (h *QueueHealth) Why() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err == nil || h.fails < queueDownAfter {
		return ""
	}
	return h.err.Error()
}

// Check asks the store once.
func (h *QueueHealth) Check(ctx context.Context) {
	if h == nil {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, QueuePingTimeout)
	err := h.ping(cctx)
	cancel()
	h.mu.Lock()
	if err != nil {
		h.fails++
		h.err = err
	} else {
		h.fails, h.err = 0, nil
	}
	h.mu.Unlock()
}

// Run asks the store every QueuePingEvery until ctx ends.
func (h *QueueHealth) Run(ctx context.Context) {
	if h == nil {
		return
	}
	h.Check(ctx)
	t := time.NewTicker(QueuePingEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.Check(ctx)
		}
	}
}
