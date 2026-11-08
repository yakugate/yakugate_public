package smppserver

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// How many submits this process works on at once (load test of 30 Oct 2026,
// finding 1). Every submit read used to start its pipeline run at once: with
// 5,000 unanswered on each of 5 binds, 25,000 runs waited together for the
// ~40 connections of a Redis pool, and the ones that waited past the pool's
// 4 s were refused "balance reservation failed" (237,351 of them).
//
// The limit is the process's, shared by all its binds. Over it a submit is
// held (the default) or answered ESME_RTHROTTLED at once (throttle).
//
// Held: it waits in its bind's queue, in the order it came, and nothing is
// done for it. The read loop goes on reading, so enquire_link, unbind and the
// answers to our deliver_sm are never kept waiting behind it; how many a bind
// may hold is its own window (each held submit has its window slot). A held
// submit whose bind ends before its turn is dropped unanswered: nothing was
// done for it, and the client, with no answer, sends it again.
//
// Fair: each bind has at most one submit waiting for a slot (its queue's
// first), and the slots that free go to the waiters in turn — so the binds
// that want slots take them in turn, whatever one of them has queued. In
// throttle mode, where nothing waits, a bind holding its share of the slots
// (the limit over the binds working) is refused before the others are.
type acceptLimit struct {
	limit    int
	throttle bool

	mu      sync.Mutex
	used    int
	peak    int
	byBind  map[*session]int // slots each bind has
	waiters []*slotWaiter    // in turn; one at most a bind

	waiting atomic.Int64 // submits held now, all binds
	// held counts the submits that had to wait, throttled those answered
	// 0x58 for the limit, dropped the held ones whose bind ended first.
	held, throttled, dropped monitor.Counter
}

type slotWaiter struct {
	s  *session
	ch chan struct{}
}

func newAcceptLimit(limit int, throttle bool) *acceptLimit {
	return &acceptLimit{limit: limit, throttle: throttle, byBind: map[*session]int{}}
}

// slotsPerConn and callHold size the limit from the Redis pool. A submit
// waits for a connection behind at most limit ÷ pool calls; the pool gives up
// after its timeout. 25 a connection waits out calls of up to 160 ms at the
// 4 s timeout — the main Redis answered pings in 39–86 ms with both boxes
// overloaded on 30 Oct — and is 1,000 on 4 CPUs (a pool of 40): 4,000 a
// second at 250 ms a submit, against 10,000 a box that refused none that day
// and 25,000 that refused 10–14%. The most allowed is calls of 40 ms.
const (
	slotsPerConn = 25
	callHold     = 40 * time.Millisecond
)

// inFlightLimit reads YAKU_SMPP_SERVER_MAX_IN_FLIGHT: "off" = no limit, a
// whole number above 0 = that, anything else = the default. lowered is set
// when the number asked for is above what the pool can serve.
func inFlightLimit(raw string, pool int, poolWait time.Duration) (limit int, lowered bool) {
	if pool < 1 {
		pool = 1
	}
	most := pool * int(poolWait/callHold)
	if most < pool {
		most = pool
	}
	raw = strings.TrimSpace(raw)
	if strings.EqualFold(raw, "off") {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		n = pool * slotsPerConn
	}
	if n > most {
		return most, true
	}
	return n, false
}

// limitOf is the limit of n submits (nil when n is 0: no limit).
func limitOf(n int, over string) *acceptLimit {
	if n < 1 {
		return nil
	}
	return newAcceptLimit(n, overLimitThrottle(over))
}

// overLimitThrottle reads YAKU_SMPP_SERVER_OVER_LIMIT: throttle, else hold.
func overLimitThrottle(raw string) bool {
	return strings.EqualFold(strings.TrimSpace(raw), "throttle")
}

// take gives s a slot when one is free and nobody waits for one. In throttle
// mode a bind with its share already gets none.
func (l *acceptLimit) take(s *session) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.used >= l.limit || len(l.waiters) > 0 {
		return false
	}
	if l.throttle && l.byBind[s] >= l.share(s) {
		return false
	}
	l.grant(s)
	return true
}

// share is a bind's part of the slots: the limit over the binds that have
// some, s counted. Caller holds mu.
func (l *acceptLimit) share(s *session) int {
	n := len(l.byBind)
	if l.byBind[s] == 0 {
		n++
	}
	return (l.limit + n - 1) / n
}

// grant gives s a slot. Caller holds mu.
func (l *acceptLimit) grant(s *session) {
	l.used++
	l.byBind[s]++
	if l.used > l.peak {
		l.peak = l.used
	}
}

// wait takes a slot for s, waiting its turn; false when done closes first.
func (l *acceptLimit) wait(s *session, done <-chan struct{}) bool {
	l.mu.Lock()
	if l.used < l.limit && len(l.waiters) == 0 {
		l.grant(s)
		l.mu.Unlock()
		return true
	}
	w := &slotWaiter{s: s, ch: make(chan struct{})}
	l.waiters = append(l.waiters, w)
	l.mu.Unlock()
	select {
	case <-w.ch:
		return true
	case <-done:
	}
	l.mu.Lock()
	for i, x := range l.waiters {
		if x == w {
			l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
			l.mu.Unlock()
			return false
		}
	}
	l.mu.Unlock()
	l.give(s) // given a slot as the bind ended: pass it on
	return false
}

// give frees one of s's slots; the first waiter is given it.
func (l *acceptLimit) give(s *session) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n := l.byBind[s] - 1; n > 0 {
		l.byBind[s] = n
	} else {
		delete(l.byBind, s)
	}
	if len(l.waiters) > 0 {
		w := l.waiters[0]
		l.waiters[0] = nil
		l.waiters = l.waiters[1:]
		l.byBind[w.s]++
		close(w.ch)
		return
	}
	l.used--
}

// inUse is the slots taken now, the most ever taken, and s's own.
func (l *acceptLimit) inUse(s *session) (used, peak, mine int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used, l.peak, l.byBind[s]
}

// section is the limit on the role's status.
func (l *acceptLimit) section(uptime float64) map[string]any {
	used, peak, _ := l.inUse(nil)
	mode := "hold"
	if l.throttle {
		mode = "throttle"
	}
	return map[string]any{
		"limit":       l.limit,
		"over_limit":  mode,
		"in_progress": used,
		"peak":        peak,
		"waiting":     l.waiting.Load(),
		"held":        l.held.Snapshot(uptime),
		"throttled":   l.throttled.Snapshot(uptime),
		"dropped":     l.dropped.Snapshot(uptime),
	}
}

// heldQueue is a bind's submits waiting for a slot, in the order they came.
type heldQueue struct {
	mu   sync.Mutex
	q    []*pdu.PDU
	wake chan struct{}
	n    atomic.Int64
}

func (h *heldQueue) put(pd *pdu.PDU) {
	h.n.Add(1)
	h.mu.Lock()
	h.q = append(h.q, pd)
	h.mu.Unlock()
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *heldQueue) next() *pdu.PDU {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.q) == 0 {
		return nil
	}
	pd := h.q[0]
	h.q[0] = nil
	h.q = h.q[1:]
	return pd
}

// startSubmit runs one submit that has its window slot (and its limit slot,
// when there is a limit).
func (p *Process) startSubmit(ctx context.Context, s *session, pd *pdu.PDU, limited bool) {
	s.inflight.Add(1)
	go func() {
		defer s.inflight.Done()
		defer s.release()
		if limited {
			defer p.limit.give(s)
		}
		p.handleSubmit(ctx, s, pd)
	}()
}

// admitSubmit is a submit with its window slot, under the limit: run now,
// held, or (throttle) refused.
func (p *Process) admitSubmit(ctx context.Context, log *logging.Logger, s *session, pd *pdu.PDU) {
	l := p.limit
	if l.throttle {
		if l.take(s) {
			p.startSubmit(ctx, s, pd, true)
			return
		}
		s.release()
		l.throttled.Add(1)
		seq := pd.Header.SequenceNumber
		p.captureSubmitResp(log, s.acctName(), seq, pdu.ESME_RTHROTTLED, "", "in-flight limit reached")
		s.send(pdu.SubmitSMRespPDU(pdu.ESME_RTHROTTLED, seq, ""))
		body, _ := pdu.DecodeSubmitSM(pd.Body)
		p.recordSubmitRefusal(s, body, "IN_FLIGHT_LIMIT",
			"the gateway was already working on as many submits as it may at once ("+strconv.Itoa(l.limit)+")")
		return
	}
	h := s.held.Load()
	if h == nil || h.n.Load() == 0 {
		if l.take(s) {
			p.startSubmit(ctx, s, pd, true)
			return
		}
	}
	if h == nil {
		h = &heldQueue{wake: make(chan struct{}, 1)}
		s.held.Store(h)
		s.inflight.Add(1)
		go p.runHeld(ctx, s, h)
	}
	l.held.Add(1)
	l.waiting.Add(1)
	h.put(pd)
}

// heldNow is how many of s's submits wait for a slot.
func (s *session) heldNow() int64 {
	if h := s.held.Load(); h != nil {
		return h.n.Load()
	}
	return 0
}

// runHeld starts a bind's held submits one at a time as slots come to it,
// until the bind ends; what is still held then is dropped.
func (p *Process) runHeld(ctx context.Context, s *session, h *heldQueue) {
	defer s.inflight.Done()
	l := p.limit
	for {
		pd := h.next()
		if pd == nil {
			select {
			case <-h.wake:
				continue
			case <-s.done:
			}
			pd = h.next()
			if pd == nil {
				return
			}
		}
		select {
		case <-s.done:
		default:
			if l.wait(s, s.done) {
				h.n.Add(-1)
				l.waiting.Add(-1)
				p.startSubmit(ctx, s, pd, true)
				continue
			}
		}
		// The bind ended: drop this one and the rest.
		for ; pd != nil; pd = h.next() {
			h.n.Add(-1)
			l.waiting.Add(-1)
			l.dropped.Add(1)
			s.release()
		}
		return
	}
}
