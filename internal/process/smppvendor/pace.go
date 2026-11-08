// How fast a session sends: its share of the vendor's rate, the answer
// pace, and the holds while receipts or answers are behind.

package smppvendor

import (
	"time"

	"github.com/yakugate/yakugate/internal/deployment"
)

// shareFor is how much of a vendor's rate one session asks for in one check
// that comes every d: the rate for that long, shared between the vendor's
// sessions, and a tenth more so a check that came late is made up by the
// next. The limiter still holds the total to the rate. 0 = no rate set.
//
// At a 150 ms interval a second's allowance is spread over the second (go-live
// test P3 F1, 2026-10-12: a session that asked for the whole second at once
// sent it in a burst and then nothing — "gives high tps and then stop for a
// second"). At 1000 ms, the default since issue 7, one check asks for the
// whole second, and sends it in one go.
func shareFor(tps, sessions int, d time.Duration) int {
	if tps <= 0 {
		return 0
	}
	if sessions < 1 {
		sessions = 1
	}
	if d <= 0 {
		d = pumpEvery
	}
	share := float64(tps) * d.Seconds() * 1.1 / float64(sessions)
	n := int(share)
	if float64(n) < share {
		n++
	}
	if n < 1 {
		n = 1
	}
	return n
}

// answerPace is the pair window.Pace steers by: answers within `quick` mean
// the path can carry more, an answer older than `slow` means it is carrying
// too much. A twelfth of the response timeout (5 s at the default 60 s),
// never under two seconds; quick is a fifth of that.
func answerPace(respTimeout time.Duration) (quick, slow time.Duration) {
	if respTimeout <= 0 {
		respTimeout = 60 * time.Second
	}
	slow = respTimeout / 12
	if slow < 2*time.Second {
		slow = 2 * time.Second
	}
	return slow / 5, slow
}

// answerLagLimit is how old a session's oldest unanswered send may be before
// the session stops claiming more: a quarter of the response timeout (60 s
// when the vendor sets none), never under two seconds.
func answerLagLimit(respTimeout time.Duration) time.Duration {
	if respTimeout <= 0 {
		respTimeout = 60 * time.Second
	}
	if lim := respTimeout / 4; lim > 2*time.Second {
		return lim
	}
	return 2 * time.Second
}

// noteSendHeld counts a tick on which a session did not claim — because the
// receipt workers were behind ("receipts"), because its own answers were
// coming back late ("answers") or because the record queue is full
// ("records") — and says so in the vendor's log at most once
// every ten seconds: an operator looking at a queue that is not emptying must
// be able to read WHY without a profiler.
func (p *Process) noteSendHeld(cid, why string) {
	p.sendHolds.Add(1)
	now := time.Now().UnixNano()
	last := p.sendHoldSaid.Load()
	if now-last < int64(10*time.Second) || !p.sendHoldSaid.CompareAndSwap(last, now) {
		return
	}
	if why == "records" {
		p.log.ForVendor(cid).Warnf("send_held", "sending is waiting for the DB worker: the record queue is full (%v)", p.recQ.Stats())
		return
	}
	if why == "answers" {
		p.log.ForVendor(cid).Warnf("send_held", "sending is waiting for answers: the oldest unanswered send is over a quarter of the response timeout old, so no more is sent until it is answered (the window is larger than what this path answers a second; receipts waiting for a worker: %d of %d)",
			p.inbound.queued(), p.inbound.capacity())
		return
	}
	p.log.ForVendor(cid).Warnf("send_held", "sending is waiting for receipts: %d of %d waiting for a worker (the database is behind, or raise YAKU_SMPP_VENDOR_INBOUND_WORKERS); it goes on as they are applied",
		p.inbound.queued(), p.inbound.capacity())
}

// sendBudget reports how many sends this vendor may take right now, at most
// max. A limit of 0 (every vendor today) means max — no cap, no change from
// before this column was read.
//
// Counted one at a time rather than asked for in bulk because the limiter
// records what it grants: asking for 10 and being refused would spend nothing
// and tell us nothing about how many were actually available.
// sendBudget paces submits to one vendor against THIS DEPLOYMENT's share of
// its rate (config/0124). The key carries the deployment id, so each gateway
// counts its own — and with Redis on, every smpp-vendor process in that
// deployment shares one counter, which is what makes a carrier's contracted
// total something you allocate rather than multiply by however many
// processes happen to be running.
func (p *Process) sendBudget(cid string, limit, max int) int {
	if limit <= 0 {
		return max
	}
	key := "vendor:" + cid
	if id := deployment.ID(); id != "" {
		key = "dep:" + id + "|" + key
	}
	// The shared (Redis) limiter gives the whole budget in one call; asking
	// for it one message at a time was a round trip per message.
	if t, ok := p.rate.(interface {
		Take(key string, limit, max int) int
	}); ok {
		return t.Take(key, limit, max)
	}
	n := 0
	for n < max && p.rate.Allow(key, limit, 1) {
		n++
	}
	return n
}

// spendOne records one more unit against the same bucket sendBudget uses, so
// the two can never drift onto different keys.
func (p *Process) spendOne(cid string, limit int) {
	if limit <= 0 {
		return
	}
	key := "vendor:" + cid
	if id := deployment.ID(); id != "" {
		key = "dep:" + id + "|" + key
	}
	p.rate.Allow(key, limit, 1)
}
