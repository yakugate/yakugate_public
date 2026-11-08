// The pump: taking messages off the vendor's queue and sending them as
// submit_sm while the window has room.

package smppvendor

import (
	"context"
	"errors"
	"time"

	"github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/lib/smpp/session"
	"github.com/yakugate/yakugate/lib/smpp/window"
)

// pumpEvery is how often a session looks after its window: submit timeouts,
// late answers, lease holds. A Process a test builds by hand also checks its
// vendor's queue this often (mtCheck).
const pumpEvery = 150 * time.Millisecond

// mtCheck is how often a session checks its vendor's queue and how much one
// check takes: YAKU_QUEUE_MT_*, read in New; a Process a test builds by hand
// checks every pumpEvery and takes what the window and the rate allow.
func (p *Process) mtCheck() queuecheck.Check {
	if p.mt.Every > 0 {
		return p.mt
	}
	return queuecheck.Check{Every: pumpEvery}
}

// heldMsg is a message this session has claimed and could not put on the wire
// in the same tick: the window had room for the count claimed, but a long
// message takes a slot per part, so the tail of a claim can be left over.
//
// Until 2026-10-06 that tail was simply dropped ("left on queue"): its lease
// kept everyone else off it for 30 s or more, and the claim had counted an attempt
// for a send that never happened (the lease is 30 s on a first claim and
// doubles each time). A vendor coming back to a backlog did this on every
// tick — in the load test a thousand messages of 54,000 waited 30 to 118 s
// behind a vendor that was up and idle. Now the session keeps the tail
// and sends it first on its next tick, 150 ms later.
type heldMsg struct {
	msg model.Message
	at  time.Time // when it was claimed
}

// heldFor is how long a session keeps a claimed message it has not sent.
// Well under the queue's lease — 30 s on a message's first claim, doubling
// each time: past this the session lets go, and the message is claimed again
// — by any session — when its lease ends. Longer, and another session could
// claim it while this one still meant to send it.
const heldFor = 10 * time.Second

// pump is one check of the vendor's queue: it submits what this session
// already holds, then claims and submits as many more messages as the window,
// the vendor's rate and YAKU_QUEUE_MT_PER_INTERVAL allow. It returns what is
// claimed and still not on the wire, for the next check.
//
// every is how often the session checks (the share of the vendor's rate it
// may ask for). more reports that work is probably still waiting — the claim
// came back full, or claimed messages are held for want of room.
func (p *Process) pump(ctx context.Context, v vendorConn, key sessionKey, cl *session.Client, win *window.Window[model.Message], held []heldMsg, every time.Duration) (left []heldMsg, more bool) {
	// Let go of what has been held too long, whatever stops this tick below:
	// back on its queue for any session (issue 22: a take leaves no lease).
	now := time.Now()
	kept := held[:0]
	var stale []model.Message
	for _, h := range held {
		if now.Sub(h.at) < heldFor {
			kept = append(kept, h)
		} else {
			stale = append(stale, h.msg)
		}
	}
	held = kept
	p.putBack(ctx, v.cid, stale)
	// The settings a live bind can take are read fresh every tick (rate,
	// validity, encodings, multipart mode, TON/NPI, fair share, breaker).
	v = p.withLive(v)
	// Circuit breaker (item: configurable threshold): a vendor whose error rate
	// tripped the breaker keeps its bind up (the fault is usually upstream
	// routing/balance, not this TCP session) but stops receiving new work until
	// the cooldown elapses. [SCOPE P3-BREAKER]
	if p.breakerOpen(v.id) {
		return held, false
	}
	// Session-level throttle backoff (item 6): after an ESME_RTHROTTLED the whole
	// session pauses for throttlePause, so we honour the vendor's "slow down"
	// instead of immediately blasting the next message and risking a bind kill.
	if p.paused(key) {
		return held, false
	}
	// How many may be outstanding follows how the answers are coming back
	// (window.Pace): a window set far above what this path answers a second
	// would otherwise fill in a few ticks, and every send in it then waits
	// tens of seconds for its answer.
	quick, slow := answerPace(v.respTimeout)
	win.Pace(now, quick, slow)
	free := win.Free()
	if free <= 0 {
		return held, true // full: there is more to send the moment answers come
	}
	// [SCOPE P3-TPS] Take the vendor's rate budget BEFORE claiming, and claim
	// only what we may actually send.
	//
	// The first version checked the rate after claiming and returned when it
	// was over — which burned the claimed rows' leases and pushed them into
	// exponential backoff, so a vendor capped at 5/s actually drained at about
	// 5 per ten seconds and every skipped message had its attempts counter
	// inflated for a send that never happened. Caught by running it, not by
	// reading it.
	//
	// What the session already holds goes first and needs no new budget: it
	// was counted when it was claimed.
	work := held
	// Sending waits while the receipt workers are behind (inboundPool.behind):
	// the reader that would bring this session's submit answers is the one
	// that waits on those workers, so more sent now is more left unanswered.
	// What is already claimed still goes out.
	// It eases off as their work piles up rather than stopping dead when half
	// their queues are taken (inboundPool.ease): stop-and-go was what made the
	// rate out uneven.
	room := free - len(held)
	if room > 0 {
		switch ease := p.inbound.ease(); {
		case ease <= 0:
			p.noteSendHeld(v.cid, "receipts")
			room = 0
		case ease < 1:
			if room = int(float64(room) * ease); room < 1 {
				room = 1
			}
		}
	}
	// …and while its own answers are coming back late. The window says how
	// many sends may be outstanding; it does not say they will be answered in
	// time. A window set far above what is answered a second (20,000 on a
	// path doing 400/s) fills in seconds, every answer is then tens of
	// seconds behind, and past the response timeout each one is counted lost
	// and the message sent again. So once the OLDEST unanswered send is a
	// quarter of the timeout old, nothing more is claimed until it is
	// answered: what is outstanding can still be answered well inside the
	// timeout, however large the window was set.
	if room > 0 && win.OldestAge(now) > answerLagLimit(v.respTimeout) {
		p.noteSendHeld(v.cid, "answers")
		room = 0
	}
	// …and while the record queue is full (YAKU_RECORDS_VIA=worker): what is
	// sent now could not be recorded. The messages stay queued, unclaimed,
	// until the DB worker has written some.
	if room > 0 && p.recQ.Full() {
		p.noteSendHeld(v.cid, "records")
		room = 0
	}
	// One check takes at most YAKU_QUEUE_MT_PER_INTERVAL (issue 7).
	if per := p.mt.PerInterval; per > 0 && room > per {
		room = per
	}
	if room > 0 {
		// One check asks for its interval's share of the vendor's rate
		// (shareFor).
		if share := shareFor(v.submitTPS, v.sessions, every); share > 0 && share < room {
			room = share
		}
		if budget := p.sendBudget(v.cid, v.submitTPS, room); budget > 0 {
			// At the cap this second the rows stay queued and unclaimed.
			if msgs, err := p.claim(ctx, v, budget); err == nil {
				work = make([]heldMsg, 0, len(held)+len(msgs))
				work = append(work, held...)
				for _, m := range msgs {
					work = append(work, heldMsg{msg: m, at: now})
				}
				// A claim that came back full: the queue probably holds more.
				more = len(msgs) >= budget
			}
		}
	}
	if len(work) == 0 {
		return nil, false
	}
	// rest is what stays claimed and unsent from position i on.
	rest := func(i int) []heldMsg { return append([]heldMsg(nil), work[i:]...) }
	for i := range work {
		m := work[i].msg
		// Decrypt for the WIRE only. m itself keeps the stored (possibly encrypted)
		// body, so the MDR record written from the window stays encrypted at rest.
		// This must happen before wireParts, which splits/encodes the text. [P1-CRYPTO]
		wm := m
		plain, err := p.plainBody(ctx, m.Body)
		if err != nil {
			p.log.Printf("decrypt %s: %v (tried again later)", m.ID, err)
			p.tryAgain(ctx, v, m)
			continue
		}
		wm.Body = plain
		parts, dc, esm, sars := wireParts(wm, v)
		// What the vendor receives, for its side of the price (PO
		// 2026-09-21): it bills the PDUs it gets, which can be fewer than
		// the gateway's own count (SAR parts carry 160, not 153) or more (a
		// vendor that takes Latin-1 as UCS-2). The client is still charged
		// by the gateway's count.
		m.SentParts = vendorParts(wm, v, len(parts))
		// A long message needs one window slot per part; if the window can't take
		// the whole group now, leave it queued rather than send a partial message.
		if win.Free() < len(parts) {
			return rest(i), true // this one and those after it wait for room, not a lease
		}
		reg := byte(1) // request a delivery receipt
		if len(parts) > 1 {
			// Its record waits for every part's answer (parts.go).
			p.partsBook(key).start(m.ID, len(parts))
		}
		// [SCOPE P13-CAPTURE] Resolved once per message, reused for every PDU
		// of a long one. One atomic load when nothing is being captured.
		cap := p.captureFor(m.AccountID, v.cid)
		for n, short := range parts {
			// The PDU index is WINDOW bookkeeping — kept off the message so the
			// record still persists the message's real concatenation group. [D-038]
			// One PDU is one send, the way a carrier meters it — so the parts
			// AFTER the first each cost another unit. Spent, never gated: a
			// long message that stopped halfway would leave the handset with
			// a fragment, which is worse than briefly exceeding the cap. The
			// spend is still recorded, so the next second self-corrects.
			if n > 0 {
				p.spendOne(v.cid, v.submitTPS)
			}
			seq, ok := win.ReservePart(m, time.Now(), n+1, len(parts))
			if !ok {
				// Part of this message may be out already, so it is not
				// held (it would be sent whole again): it is tried again
				// after its gap; those after it are held.
				p.tryAgain(ctx, v, m)
				return rest(i + 1), true
			}
			opt := v.submitOpt
			opt.ValidityPeriod = validityFor(v.validityMode, m.ValidUntil, time.Now())
			sm := submitPDU(v, m, short, dc, esm, sars[n], reg, seq, opt)
			if err := cl.Write(sm); err != nil {
				win.Ack(seq) // send failed; release the slot
				// The session is going down: nothing is held past it. What
				// was not sent goes back on its queue; a message with a part
				// out already goes back with the session's unanswered sends.
				back := heldMsgs(work[i+1:])
				if n == 0 {
					back = append(back, m)
				}
				p.putBack(ctx, v.cid, back)
				return nil, false
			}
			// Captured AFTER the write succeeds: a PDU that never reached the
			// socket did not go to the vendor, and recording it as though it
			// had would send an operator looking for a response that can never
			// arrive. wm.Body is the decrypted wire text (see config/0084).
			p.captureSubmit(cap, m, v.cid, wm.Body, len(short), seq, n+1, len(parts), dc)
		}
		p.countSubmitted(key, 1) // monitor: one message fully on the wire (item 7)
		p.countEncoding(key, wm.Encoding)
	}
	return nil, more
}

// claim takes up to n of the vendor's messages to send (take: its rate
// split between its short codes the vendor's way; those past their validity
// or out of tries are refused there, issue 22).
func (p *Process) claim(ctx context.Context, v vendorConn, n int) ([]model.Message, error) {
	msgs, err := p.take(ctx, v, n)
	return stampSender(msgs), err
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

// plainBody returns the body as the vendor must see it. Values that were never
// encrypted pass straight through, so mixed plaintext/ciphertext queues work with
// no flag check on the read path. [SCOPE P1-CRYPTO]
func (p *Process) plainBody(ctx context.Context, stored string) (string, error) {
	if p.cry == nil {
		// An encrypted body with no way to open it must never go out: the
		// handset would receive "enc:v1:…" as its text. Refusing leaves the
		// message queued until encryption is fixed or the message expires.
		if crypto.IsCiphertext(stored) {
			return "", errors.New("encrypted body, but encryption failed to start in this process")
		}
		return stored, nil
	}
	return p.cry.Decrypt(ctx, stored)
}
