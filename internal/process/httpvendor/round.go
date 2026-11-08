package httpvendor

import (
	"context"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/vendorhealth"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// One round of a vendor's worker: how much it takes, and the calls it starts
// for what it took. A call's outcome is turned into the vendor's health here too.

// runUnits runs the calls of one claimed batch, at most n at a time. With n = 1
// it runs them in order on this goroutine, exactly as before concurrency.
func runUnits(units []func(), n int) {
	if n <= 1 {
		for _, u := range units {
			u()
		}
		return
	}
	sem := make(chan struct{}, n)
	var wg sync.WaitGroup
	for _, u := range units {
		sem <- struct{}{}
		wg.Add(1)
		go func(u func()) {
			defer wg.Done()
			defer func() { <-sem }()
			u()
		}(u)
	}
	wg.Wait()
}

// deliver claims a batch for one vendor and sends it via the adapter. Single-part
// messages go one-to-one; a concatenation group that arrived over SMPP ingress is
// REASSEMBLED into one request first (D-036) — HTTP vendors have no UDH, so
// sending the parts separately would land N fragments on the handset.
//
// It reports whether it took a FULL batch, i.e. whether more is probably
// waiting and the worker should come straight back.
func (p *Process) deliver(ctx context.Context, v vendorAdapter) bool {
	more, _ := p.round(ctx, v)
	return more
}

// round is deliver, and when the worker should NOT come straight back it says
// how long to wait: laneIdle when there was nothing to send, a moment when
// the vendor's rate for this second is spent.
//
// It is one round on a lane of its own, finished before it returns: every
// call is back and everything is filed. A vendor's worker uses roundOn, whose
// calls may still be in flight when it returns.
func (p *Process) round(ctx context.Context, v vendorAdapter) (more bool, wait time.Duration) {
	ln := &sendLane{}
	defer ln.finish()
	return p.roundOn(ctx, v, ln)
}

// roundOn is one round of a vendor's worker: it claims a batch and STARTS its
// calls on the worker's lane. It returns when the last call has started (see
// sendLane), so the worker claims again while those calls are in flight.
func (p *Process) roundOn(ctx context.Context, v vendorAdapter, ln *sendLane) (more bool, wait time.Duration) {
	// stop is the worker's own context: when it ends, nothing more is claimed
	// or started. The calls and what they answer run on ctx, which a stop
	// does not cancel — a call the vendor is answering must be recorded, not
	// cut (Process.Stop).
	stop := ctx
	ctx = context.WithoutCancel(ctx)
	if stop.Err() != nil {
		return false, laneIdle
	}
	claim := roundClaim(v.batch, v.ad.BulkChunkSize())
	// One check takes at most YAKU_QUEUE_MT_PER_INTERVAL (issue 7).
	if per := p.mt.PerInterval; per > 0 && claim > per {
		claim = per
	}
	// The send row's circuit breaker (QA EP-018): too many failed calls
	// pause new sends for its open time; the messages stay queued.
	if p.breakers.open(v.id, time.Now()) {
		return false, laneIdle
	}
	// The record queue is full (YAKU_RECORDS_VIA=worker): what is sent now
	// could not be recorded. The messages stay queued, unclaimed, until the
	// DB worker has written some; the record queue's client says so in the log.
	if p.recQ.Full() {
		return false, laneIdle
	}
	// A vendor whose calls have kept failing for its down_after_sec is down
	// (PO 2026-09-14): send nothing, so its waiting messages can move to the
	// next route. Every recheck_after_sec ONE real message is let through;
	// its answer decides whether traffic returns.
	gate := p.health.Gate(v.id, v.downAfter, v.recheckAfter)
	switch gate {
	case vendorhealth.Closed:
		return false, laneIdle
	case vendorhealth.Trial:
		claim = 1 // one message, even for a bulk vendor: one call, one answer
	}
	// [SCOPE P4-TPS] Take the vendor's rate budget BEFORE claiming.
	//
	// Refusing after the claim burns the rows' leases and pushes them into
	// exponential backoff, so the vendor drains far below its configured rate
	// and every skipped message has its attempts counter inflated for a send
	// that never happened. Proven on P3 by running it.
	//
	// A bulk vendor spends ONE unit for the whole chunk (that is the call the
	// vendor meters), so its budget is counted in calls and its claim is the
	// chunk size per allowed call. TPS Count Mode "Total messages": every
	// message of a chunk is one unit, so the budget is counted in messages.
	//
	// The budget is asked for what this round can USE — calls for a bulk
	// vendor that counts calls — and what it did not use goes back
	// (2026-10-12, sendBudget).
	var took budget
	if v.submitTPS > 0 {
		per := 1
		if n := v.ad.BulkChunkSize(); n > 1 && !v.ad.BulkCountsMessages() && gate != vendorhealth.Trial {
			per = n
		}
		took = p.sendBudget(v.cid, v.submitTPS, (claim+per-1)/per)
		if took.n <= 0 {
			return false, untilBudget(time.Now()) // at the cap this second; rows stay queued and unclaimed
		}
		took.per = per
		if c := took.n * per; c < claim {
			claim = c
		}
	}
	msgs, err := p.take(ctx, v, claim)
	msgs = stampSender(msgs)
	if err != nil || len(msgs) == 0 {
		p.giveBack(took, 0) // nothing was waiting: the budget was not used
		return false, laneIdle
	}
	if gate != vendorhealth.Trial {
		msgs = p.gatherBulk(ctx, v, msgs, claim)
	}
	p.giveBack(took, len(msgs))
	// Each vendor call of the batch is one unit; up to v.concurrency of them
	// run at once (D-088 submit_concurrency). Units are built in the order
	// the old loop sent them, so a concurrency of 1 behaves exactly as before.
	// The parts of one long message taken this round go as one unit (a
	// take leaves no lease, issue 22: a part passed over would be lost).
	groups := map[string][]model.Message{}
	for _, m := range msgs {
		if m.ConcatTotal > 1 {
			groups[concatKey(m)] = append(groups[concatKey(m)], m)
		}
	}
	seen := map[string]bool{}    // group keys already handled in this batch
	var bulkable []model.Message // GW-15 slice 1: single-part messages for chunking
	var units []func()
	var unitMsgs [][]model.Message // the messages of each unit, to put back if it never starts
	var single *filer              // files this round's single sends; nil when it has none
	for _, m := range msgs {
		if m.ConcatTotal > 1 {
			key := concatKey(m)
			if seen[key] {
				continue // a sibling already sent the whole group
			}
			seen[key] = true
			taken := groups[key]
			units = append(units, func() { p.deliverGroup(ctx, v, taken) })
			unitMsgs = append(unitMsgs, taken)
			continue
		}
		if v.ad.BulkMode() {
			bulkable = append(bulkable, m)
			continue
		}
		// A single send hands what the vendor answered to the round's
		// filer, which files many at a time (filer.go).
		if single == nil {
			single = ln.filerFor(p, ctx, v)
		}
		units = append(units, func() { p.deliverOne(ctx, v, m, m.Body, []model.Message{m}, single) })
		unitMsgs = append(unitMsgs, []model.Message{m})
	}
	// Grouped by the text as the vendor will see it: an encrypted body is
	// sealed afresh for every message, so grouping by the stored body put
	// every message of an encrypted account in a chunk of its own (QA
	// EP-011). A body that cannot be opened keeps its stored form, and its
	// chunk is left on the queue by deliverChunk as before.
	textOf := func(m model.Message) string {
		if plain, err := p.plainBody(ctx, m.Body); err == nil {
			return plain
		}
		return m.Body
	}
	chunks := bulkChunks(v, bulkable, textOf)
	p.noteBulkAlone(v, len(bulkable), len(chunks))
	for _, chunk := range chunks {
		// A small call is filed with the others of the round (filer.go).
		if len(chunk) < fileTogetherUnder && single == nil {
			single = ln.filerFor(p, ctx, v)
		}
		units = append(units, func() { p.deliverChunk(ctx, v, chunk, single) })
		unitMsgs = append(unitMsgs, chunk)
	}
	// The calls are started and the round returns: the worker claims again
	// while they are in flight. A vendor on trial sends its one message and
	// waits for the answer, which decides whether traffic returns.
	if started := ln.start(stop, units, v.concurrency, gate == vendorhealth.Trial); started < len(units) {
		// The role is stopping: what was taken and not sent goes back on
		// its queue, its attempt as it was.
		var back []model.Message
		for _, ms := range unitMsgs[started:] {
			back = append(back, ms...)
		}
		p.later(ctx, back, 0)
	}
	return len(msgs) >= claim, laneIdle
}

// sendBudget reports how many CALLS this vendor may take right now, at most
// max. A limit of 0 (every vendor today) means max — no cap, unchanged from
// before this column was read.
//
// maxRoundClaim is the most messages one round takes off a vendor's queue.
//
// The claim batch is the send call's rate_limit (D-088), and nothing bounded
// it: with 50,000 there, one round took 50,000 messages and sent them all with
// the settings it started with — an edit to the call (Bulk switched on) was
// not used for over a minute, and every message waited out a 30-second lease
// in this process's hands (2026-10-12). A round is a second or two of work;
// the worker comes straight back for the next one.
const maxRoundClaim = 2000

// roundClaim is how many messages a round asks the queue for: the send call's
// claim batch (50 when unset), at most maxRoundClaim — and never less than
// one bulk chunk, which a bulk vendor swallows in one call.
func roundClaim(batch, chunk int) int {
	claim := batch
	if claim < 1 {
		claim = 50
	}
	if claim > maxRoundClaim {
		claim = maxRoundClaim
	}
	if chunk > claim {
		claim = chunk
	}
	return claim
}

// noteHealth turns one vendor call's outcome into health (PO 2026-09-14). An
// answer — accepted, or refused outright (ErrPermanent) — means the vendor is
// working; a call that got no usable answer (network error, timeout, a retry
// code such as 5xx) is a sign it is not.
//
// An answer the call's own rule does not accept is an ANSWER too
// (httpadapter.NotAccepted, 2026-10-12): a 200 that fails the success rule,
// or one with no id where the call reads it. It used to be counted with the
// calls that got no answer, so a vendor that answered every call was shown
// as down after its down_after_sec — and then held to one trial message per
// recheck — because our own rule was written wrong. The message is still not
// sent; the vendor is not blamed for it.
//
// A call that could not be made for want of a session is a third thing: the
// vendor is not READY (vendorhealth.NotReady), not down.
func (p *Process) noteHealth(v vendorAdapter, sendErr error) {
	answered := sendErr == nil || httpadapter.Answered(sendErr)
	// The same outcome feeds the send row's circuit breaker (QA EP-018).
	if trip := p.breakers.note(v.id, v.breaker, !answered, time.Now()); trip != "" {
		p.log.ForVendor(v.cid).Errorf("breaker", "circuit breaker OPEN (%s); pausing new sends for %s",
			trip, v.breaker.cooldown)
	}
	switch {
	case answered:
		p.health.OK(v.id)
	case httpadapter.NoSession(sendErr):
		p.health.NotReady(v.id, sendErr.Error())
	default:
		p.health.Fail(v.id, sendErr.Error())
	}
}

// stampSender marks every claimed message with THIS deployment as the one
// that sent it (hot/0046). Stamped at the claim, not at the record write, so
// every path out of this role — sent, failed, retried into a terminal record —
// carries it without six separate call sites having to remember.
//
// The accepting deployment rode the queue row and is already on the message;
// these two are what tell an operator that a submit landed in one site and
// left from another, which is exactly what pinning a vendor to a deployment
// makes routine.
func stampSender(msgs []model.Message) []model.Message {
	id := deployment.ID()
	for i := range msgs {
		msgs[i].SentDeploymentID = id
	}
	return msgs
}
