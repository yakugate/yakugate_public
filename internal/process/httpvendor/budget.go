package httpvendor

import (
	"time"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/ratelimit"
)

// A vendor's rate budget: what a round may send this second, the part it did
// not use given back, and how long until there is budget again.

// capWait is the longest a round says to wait when its vendor's rate budget
// for this second is spent. From 2026-10-12 until issue 7 the worker came
// back that soon (it had slept laneIdle, so a vendor allowed 1,000 a second
// sent two thirds of its rate); it now checks on its fixed interval.
const capWait = 100 * time.Millisecond

// untilBudget is how long until the vendor's rate has budget again: the start
// of the next second (both limiters count by the second), at most capWait.
func untilBudget(now time.Time) time.Duration {
	d := now.Truncate(time.Second).Add(time.Second).Sub(now) + 2*time.Millisecond
	if d > capWait {
		return capWait
	}
	return d
}

// budget is what a round was granted of a vendor's rate, and where it came
// from, so the part it did not use can go back.
type budget struct {
	n   int    // units granted
	per int    // messages one unit pays for (a bulk call that counts as one)
	key string // "" = nothing to give back to
}

// sendBudget paces calls to one vendor against THIS DEPLOYMENT's share of
// its rate (config/0124). The key carries the deployment id, so each gateway
// counts its own — and with Redis on, every vendor process in that deployment
// shares one counter, which is what makes the carrier's contracted total
// something you can allocate rather than multiply.
//
// ONE call for the whole round (2026-10-12). It used to ask one unit at a
// time, up to the claim batch, stopping when the limiter said no. Against
// Redis each ask is a round trip, and once a round trip took a millisecond a
// vendor allowed 1,000 a second was never told no: a new second had begun
// before the thousandth ask. With a claim batch of 50,000 the loop then ran
// 50,000 round trips — 107 seconds on a busy box — before it claimed its
// first message (Saleem: "http vendor takes too long to start submitting
// messages"). The SMPP vendor role was moved to one call on 2026-10-07.
func (p *Process) sendBudget(cid string, limit, max int) budget {
	if limit <= 0 {
		return budget{n: max}
	}
	key := budgetKey(cid)
	// Both limiters the role runs with (in memory, and Redis) grant a round's
	// budget in one step and take the unused part back (ratelimit.Granter —
	// what the MO, receipt and blackhole drainers already use).
	if g, ok := p.rate.(ratelimit.Granter); ok {
		return budget{n: g.Grant(key, limit, max), key: key}
	}
	// A limiter that only answers one unit at a time (a test's stand-in).
	n := 0
	for n < max && p.rate.Allow(key, limit, 1) {
		n++
	}
	return budget{n: n}
}

// budgetKey names a vendor's rate bucket: this deployment's share of it.
func budgetKey(cid string) string {
	key := "vendor:" + cid
	if id := deployment.ID(); id != "" {
		key = "dep:" + id + "|" + key
	}
	return key
}

// giveBack returns the part of a round's budget that paid for nothing: the
// round claimed `used` messages. Without it an idle worker spent a whole
// second's budget every time it looked at an empty queue, and the first
// message to arrive in that second waited for the next one.
func (p *Process) giveBack(b budget, used int) {
	if b.key == "" || b.n <= 0 {
		return
	}
	per := b.per
	if per < 1 {
		per = 1
	}
	back := b.n - (used+per-1)/per
	if back <= 0 {
		return
	}
	if g, ok := p.rate.(ratelimit.Granter); ok {
		g.Return(b.key, back)
	}
}
