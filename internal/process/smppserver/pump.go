package smppserver

import (
	"context"
	"sync"
	"time"

	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// A window of deliveries per bind (Saleem, 28 Oct 2026).
//
// Production, tw-1, that evening: a client with 14 binds submitted ~935/s on
// ten of them while its receipts piled up to 95,000 and were handed over at
// 1–19 a second; when it stopped submitting they went out at 2,200 a second.
// The legs handed receipts and MO over in rounds of 200 and waited up to
// 250 ms for the answers; one answer still out kept the whole ACCOUNT out of
// the next claims until the next check, a second later — so one bind busy
// submitting held back all fourteen — and the leg wrote every deliver_sm
// itself, so a bind the client read slowly blocked every other bind too.
//
// Now each receive-capable bind has a pump: an outbox of what is to go down
// it, and at most a window of deliver_sm out without an answer (the account's
// smpp_config.deliver_window, else YAKU_SMPP_DELIVER_WINDOW) — receipts and
// MO together, one setting. It sends while it has room and frees a place on
// each answer (or after respTimeout), so a slow bind slows only itself and
// the legs claim as fast as the binds take. One bind's TCP stream keeps the
// order of what goes down it, and a message's receipts all go down one bind
// (deliverySession), so they reach the client in order.

// delivery is one thing a pump sends: a receipt (one PDU) or an MO (one PDU a
// part). build makes its PDUs for the bind it goes down (packing, payload TLV
// are the bind's own) with sequence numbers from next; done is told once what
// the client answered (ok: every PDU ESME_ROK) — never, by name, for a
// delivery dropped unsent (the bind ended, the leg stopped): dropped is
// called then.
type delivery struct {
	seq     *seqGen
	build   func(s *session, next func() uint32) [][]byte
	done    func(s *session, a answer)
	dropped func()
}

// answer is what the client said to one delivery (a try, D-140): ok when
// every PDU was answered ESME_ROK; status the deliver_sm_resp status of the
// first PDU refused, else ESME_ROK — nil when one went unanswered (timeout);
// sent when its PDUs went down the bind, took until the last answer.
type answer struct {
	ok     bool
	status *uint32
	sent   time.Time
	took   time.Duration
}

// pump is one bind's outbox and window.
type pump struct {
	s        *session
	window   func() int
	timeouts func()
	freed    func() // tells the legs a place came free

	mu    sync.Mutex
	queue []queued
	out   int // PDUs sent without an answer
	wake  chan struct{}
}

type queued struct {
	d  delivery
	wg *sync.WaitGroup
}

// seqGen hands out a leg's sequence numbers to every bind's pump.
type seqGen struct {
	mu   sync.Mutex
	seq  uint32
	base uint32
}

// band puts the counter in its own band (wrapSeq), before its first number.
func (g *seqGen) band(base uint32) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.base = base
}

func (g *seqGen) next() uint32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return wrapSeq(&g.seq, g.base)
}

func newPump(s *session, window func() int, timeouts, freed func()) *pump {
	return &pump{s: s, window: window, timeouts: timeouts, freed: freed, wake: make(chan struct{}, 1)}
}

func (u *pump) poke() {
	select {
	case u.wake <- struct{}{}:
	default:
	}
}

// room is how many more deliveries this bind can be given now: its window
// less what it has out and what waits in its outbox.
func (u *pump) room() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.window() - u.out - len(u.queue)
}

// load is what the bind has out and what waits, for its status.
func (u *pump) load() (out, waiting int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.out, len(u.queue)
}

// push puts a delivery in the outbox; wg is counted until it is answered,
// timed out or dropped.
func (u *pump) push(d delivery, wg *sync.WaitGroup) {
	wg.Add(1)
	u.mu.Lock()
	u.queue = append(u.queue, queued{d: d, wg: wg})
	u.mu.Unlock()
	u.poke()
}

// run sends from the outbox while the window has room, until the bind ends
// or ctx does; what is still in the outbox then is dropped (its lease brings
// it back).
func (u *pump) run(ctx context.Context) {
	defer u.dropAll()
	for {
		u.mu.Lock()
		var q queued
		ok := len(u.queue) > 0 && (u.out < u.window() || u.out == 0)
		if ok {
			q = u.queue[0]
			u.queue = u.queue[1:]
		}
		u.mu.Unlock()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-u.s.done:
				return
			case <-u.wake:
			}
			continue
		}
		if ctx.Err() != nil || u.s.ending.Load() {
			u.drop(q)
			return
		}
		u.send(ctx, q)
	}
}

func (u *pump) send(ctx context.Context, q queued) {
	pdus := q.d.build(u.s, q.d.seq.next)
	if len(pdus) == 0 {
		u.drop(q)
		return
	}
	seqs := make([]uint32, len(pdus))
	chans := make([]<-chan uint32, len(pdus))
	for i, b := range pdus {
		seqs[i] = seqOf(b)
		chans[i] = u.s.expect(seqs[i])
	}
	u.mu.Lock()
	u.out += len(pdus)
	u.mu.Unlock()
	sent := time.Now()
	for _, b := range pdus {
		u.s.send(b)
	}
	go func() {
		ok := true
		var status *uint32
		for i, ch := range chans {
			select {
			case st := <-ch:
				s := st
				status = &s
				if st != pdu.ESME_ROK {
					ok = false
				}
			case <-time.After(respTimeout):
				u.s.forget(seqs[i])
				ok = false
				status = nil
				if u.timeouts != nil {
					u.timeouts()
				}
			}
			if !ok {
				for _, later := range seqs[i+1:] {
					u.s.forget(later)
				}
				break
			}
		}
		u.mu.Lock()
		u.out -= len(pdus)
		u.mu.Unlock()
		u.poke()
		if u.freed != nil {
			u.freed()
		}
		q.d.done(u.s, answer{ok: ok, status: status, sent: sent, took: time.Since(sent)})
		q.wg.Done()
	}()
}

// seqOf reads a PDU's sequence number: octets 12 to 15 of its header.
func seqOf(b []byte) uint32 {
	if len(b) < 16 {
		return 0
	}
	return uint32(b[12])<<24 | uint32(b[13])<<16 | uint32(b[14])<<8 | uint32(b[15])
}

func (u *pump) drop(q queued) {
	if q.d.dropped != nil {
		q.d.dropped()
	}
	q.wg.Done()
}

func (u *pump) dropAll() {
	u.mu.Lock()
	left := u.queue
	u.queue = nil
	u.mu.Unlock()
	for _, q := range left {
		u.drop(q)
	}
}

// pumpOf is a bind's pump, started the first time it is asked for. Its
// window is read at every send: a change of the account's deliver_window
// reaches a live bind at the next reference reload.
func (p *Process) pumpOf(s *session) *pump {
	s.pumpOnce.Do(func() {
		var acct int64
		if s.acct != nil {
			acct = s.acct.ID
		}
		s.pmp = newPump(s, func() int { return p.deliveryWindow(acct) },
			func() { p.stats2.respTimeouts.Add(1) }, p.notifyFreed)
		ctx := p.pumpCtx
		if ctx == nil {
			ctx = context.Background()
		}
		go s.pmp.run(ctx)
	})
	return s.pmp
}

// deliveryWindow is how many deliver_sm one bind of acct may have out without
// an answer: the account's own smpp_config.deliver_window, else the process
// default (YAKU_SMPP_DELIVER_WINDOW).
func (p *Process) deliveryWindow(acct int64) int {
	if p.ref != nil {
		if a, ok := p.ref.AccountByID(acct); ok && a.SMPPDeliverWindow > 0 {
			return a.SMPPDeliverWindow
		}
	}
	if p.deliverWindow > 0 {
		return p.deliverWindow
	}
	return 64
}

// notifyFreed wakes both legs: a bind has room again.
func (p *Process) notifyFreed() {
	p.initFreed()
	for _, c := range []chan struct{}{p.dlrFreed, p.moFreed} {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

// roomOf is how many more deliveries acct's binds can be given now.
func (p *Process) roomOf(acct int64) int {
	n := 0
	for _, s := range p.binds.receivers(acct) {
		if r := p.pumpOf(s).room(); r > 0 {
			n += r
		}
	}
	return n
}
