package smppserver

// One client connection: its state, its submit window, its read loop and
// the keepalive.

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// bindMode is the session's bound mode.
type bindMode int

const (
	unbound bindMode = iota
	modeTX
	modeRX
	modeTRX
)

// session is one SMPP connection's state.
type session struct {
	conn net.Conn
	w    *bufio.Writer
	mu   sync.Mutex // serialize writes
	mode bindMode
	acct *refdata.Account
	ip   string
	// ifaceVersion is what the client declared at bind (0x34 = SMPP 3.4).
	// message_payload is a 3.4 TLV, so a long message goes out that way
	// only when the account asks for it AND the bind is 3.4 or later.
	ifaceVersion byte
	// slot is this bind's number within its account (1, 2, 3 …), assigned by
	// bindRegistry.add and released on teardown. It is stamped onto every
	// message submitted here so the receipt can come back down the same bind
	// (PO 2026-09-05). Only meaningful once the bind has authenticated.
	slot int
	// bound-at, for the status page's bind list. [SCOPE P1-STATUS]
	boundAt time.Time
	// deliver_sm_resp correlation [SCOPE P2-RESP] (2026-07-26): outbound
	// pushes register a waiter per sequence number; the read loop resolves it
	// with the client's command_status. An ack (queue Ack / MO finalize) may
	// only happen after the client CONFIRMS — a socket flush is not delivery.
	respMu  sync.Mutex
	waiters map[uint32]chan uint32
	// done signals the session's own goroutines (currently just
	// enquireLinkLoop) to stop — closed exactly once, in serve()'s cleanup.
	// [SCOPE P2-KEEPALIVE]
	done chan struct{}

	// --- ingress window [SCOPE P2-WINDOW], PO 2026-09-04 ---
	//
	// submit_sm used to be handled INLINE in the read loop: the next PDU was
	// not even read until the previous one had been through the whole
	// pipeline and answered. That made one bind's ceiling 1/latency —
	// measured at ~320 msg/s — however fast the engine got, and it made the
	// spec's ESME_RTHROTTLED backpressure unreachable, since a window of one
	// can never be exceeded.
	//
	// sem bounds how many submits this bind may have in flight. A submit that
	// cannot get a slot is answered ESME_RTHROTTLED (0x58) immediately, which
	// is the status a well-behaved ESME backs off on. inflight lets serve()
	// wait for running submits before it tears the connection down.
	sem      chan struct{}
	inflight sync.WaitGroup
	// held is this bind's submits waiting for a slot of the process's limit
	// (inflight.go); made the first time one has to wait.
	held atomic.Pointer[heldQueue]

	// Per-bind monitoring (2026-09-11, Hadal's smppState). Atomics, because
	// they are bumped from the reader, the submit goroutines and the delivery
	// legs at once, and read by the status push.
	// onSubmitResp, set at bind, reports each submit_sm answer to the
	// account's monitoring counters (by SMPP status).
	onSubmitResp func(status uint32)
	submitsOK    atomic.Int64 // submit_sm answered ESME_ROK
	submitsErr   atomic.Int64 // submit_sm answered anything else (Hadal's `errors`)
	pushedDLR    atomic.Int64 // receipts this client confirmed on this bind
	pushedMO     atomic.Int64 // MO this client confirmed on this bind
	lastActive   atomic.Int64 // unix nanos of the last PDU from the client
	lastEnq      atomic.Int64 // unix nanos of the last enquire_link either way

	// ending is set once the gateway has decided to end this bind (endBind),
	// so it is told to unbind once. unknown counts the checks in a row that
	// found no account for it (see watchBinds).
	ending  atomic.Bool
	unknown int

	// pump sends this bind's receipts and MO within its delivery window
	// (pump.go); made the first time the bind is given one.
	pumpOnce sync.Once
	pmp      *pump
}

// openWindow sizes this session's ingress window. Called once the bind has
// resolved an account, so the account's own window_size can apply.
func (s *session) openWindow(n int) {
	if n < 1 {
		n = 1
	}
	s.sem = make(chan struct{}, n)
}

// acquire takes a window slot, waiting up to grace for one to free before
// giving up. Reporting false is what becomes an ESME_RTHROTTLED.
//
// The wait matters. Refusing the instant the window is full punishes a client
// whose own window is merely larger than ours: measured against a load
// generator that never backs off, an instant refusal turned 328k submits into
// 9k accepted and 319k refused. Waiting briefly instead stops reading from
// the socket, which is ordinary TCP backpressure and exactly how this server
// paced clients before it had a window at all. Only a client that stays over
// the limit for longer than the grace period is actually told to slow down.
func (s *session) acquire(grace time.Duration) bool {
	if s.sem == nil {
		return true // not bound yet: the bind path is serial anyway
	}
	select {
	case s.sem <- struct{}{}:
		return true
	default:
	}
	if grace <= 0 {
		return false
	}
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case s.sem <- struct{}{}:
		return true
	case <-t.C:
		return false
	case <-s.done:
		return false
	}
}

func (s *session) release() {
	if s.sem == nil {
		return
	}
	select {
	case <-s.sem:
	default:
	}
}

func (s *session) send(b []byte) {
	s.countOutgoing(b)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.w.Write(b)
	_ = s.w.Flush()
}

// countOutgoing tallies this bind's submit_sm_resp outcomes from the PDU
// header on the way out. Done HERE, at the one choke point every response
// passes, rather than at the eight places that build one: a count kept at
// each call site is a count that misses the ninth.
func (s *session) countOutgoing(b []byte) {
	if len(b) < 16 {
		return
	}
	if binary.BigEndian.Uint32(b[4:8]) != pdu.SubmitSMResp {
		return
	}
	status := binary.BigEndian.Uint32(b[8:12])
	if status == pdu.ESME_ROK {
		s.submitsOK.Add(1)
	} else {
		s.submitsErr.Add(1)
	}
	// The account's own line on Monitoring: which status its submits got.
	if s.onSubmitResp != nil {
		s.onSubmitResp(status)
	}
}

// expect registers a waiter for the deliver_sm_resp carrying seq.
func (s *session) expect(seq uint32) <-chan uint32 {
	ch := make(chan uint32, 1)
	s.respMu.Lock()
	if s.waiters == nil {
		s.waiters = map[uint32]chan uint32{}
	}
	s.waiters[seq] = ch
	s.respMu.Unlock()
	return ch
}

// resolve delivers a deliver_sm_resp status to its waiter (if any).
func (s *session) resolve(seq, status uint32) {
	s.respMu.Lock()
	ch := s.waiters[seq]
	delete(s.waiters, seq)
	s.respMu.Unlock()
	if ch != nil {
		ch <- status
	}
}

// forget drops a waiter whose push timed out.
func (s *session) forget(seq uint32) {
	s.respMu.Lock()
	delete(s.waiters, seq)
	s.respMu.Unlock()
}

// windowFor resolves how many submit_sm one bind may have in flight: the
// account's own smpp_config.window_size when set, else the process-wide
// default (YAKU_SMPP_WINDOW). The same per-account-then-env shape every other
// tunable in this engine uses (D-069). A value of 1 restores the old
// strictly-serial behaviour, for a client that cannot cope with out-of-order
// responses. [SCOPE P2-WINDOW]
func windowFor(a *refdata.Account, def int) int {
	if a != nil && a.SMPPWindow > 0 {
		return a.SMPPWindow
	}
	if def > 0 {
		return def
	}
	return 1
}

// serve runs one connection's read loop. [SCOPE P2-SESSION]
func (p *Process) serve(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	defer atomic.AddInt64(&p.connCount, -1)
	log := logging.New(p.Name())
	ip := conn.RemoteAddr().String()
	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}
	s := &session{conn: conn, w: bufio.NewWriter(conn), ip: ip, done: make(chan struct{})}
	p.conns.add(s)
	defer func() {
		close(s.done)
		// Let running submits finish before the connection goes: each still
		// wants to write its submit_sm_resp, and each holds a pipeline run
		// that has already reserved balance and may have enqueued. Dropping
		// them here would answer nothing while the message went out.
		// [SCOPE P2-WINDOW]
		s.inflight.Wait()
		p.conns.remove(s)
		if s.acct != nil {
			// [SCOPE P13-LOG] The other half of bind_ok: "when did this client
			// last connect" is only answerable if the disconnect is recorded
			// too. P2 logged nothing here before 2026-09-05. Unauthenticated
			// connections are deliberately silent — a port scan must not be
			// able to write log lines.
			p.logBind(log, "bind_end", "client unbound", s.acct.Username,
				logging.Fields{"ip": s.ip, "mode": modeName(s.mode), "bindSlot": s.slot,
					"boundForSec": int(time.Since(s.boundAt) / time.Second)})
			p.binds.remove(s.acct.ID, s)
		}
	}()
	r := bufio.NewReader(conn)

	for {
		// Unauthenticated connections get a SHORT deadline [SCOPE P2-CONNCAP]:
		// a socket that hasn't bound within 30s is a scanner or a stall, and
		// pre-auth sockets must not be able to camp on the connection cap.
		// Bound connections use the account's own configured idle timeout
		// (accounts.smpp_config.idle_timeout_sec, default 90s) — [SCOPE
		// P2-KEEPALIVE, PO 2026-07-27].
		if s.acct == nil {
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		} else {
			// The account as it is NOW: an idle timeout changed on the account
			// reaches a bind that is already up.
			_ = conn.SetReadDeadline(time.Now().Add(time.Duration(idleTimeoutSec(p.liveAccount(s))) * time.Second))
		}
		pd, err := pdu.Read(r)
		if err != nil {
			// A length no PDU can have: the stream cannot be read past it, so
			// say so and close (pitfall 12; the client guide promises
			// generic_nack with ESME_RINVCMDLEN). The connection used to be
			// closed with no answer at all (dry run 2026-10-09). send writes
			// and flushes before this returns, so the answer leaves first.
			var bad *pdu.BadLengthError
			if errors.As(err, &bad) {
				s.send(pdu.GenericNackPDU(pdu.ESME_RINVCMDLEN, bad.Sequence))
			}
			return // EOF / timeout / a length that is not a PDU's
		}
		s.lastActive.Store(time.Now().UnixNano())
		switch pd.Header.CommandID {
		case pdu.BindTransmitter, pdu.BindReceiver, pdu.BindTransceiver:
			p.handleBind(s, pd, log)
		case pdu.SubmitSM:
			// [SCOPE P2-WINDOW] Concurrent, bounded by this bind's window.
			// Responses may now come back out of order, which is what SMPP
			// 3.4 specifies (a resp is matched by sequence_number, not by
			// position) and what our own vendor sender already does. A client
			// that cannot handle it sets window_size=1 for its account.
			if !s.acquire(p.windowGrace) {
				p.countThrottled()
				// [SCOPE P13-CAPTURE] Backpressure is the one refusal a customer
				// argues about most ("you are dropping my traffic"), and it never
				// reaches the pipeline — the window refused it first.
				p.captureSubmitResp(log, s.acctName(), pd.Header.SequenceNumber, pdu.ESME_RTHROTTLED, "", "ingress window full")
				s.send(pdu.SubmitSMRespPDU(pdu.ESME_RTHROTTLED, pd.Header.SequenceNumber, ""))
				// A refusal like any other: in the Rejected log and summary
				// (Saleem 2026-10-14). Decoded only for its addresses.
				body, _ := pdu.DecodeSubmitSM(pd.Body)
				p.recordSubmitRefusal(s, body, "WINDOW_FULL", "the bind already had its whole window of submits waiting for answers")
				break
			}
			// The process's limit on submits worked on at once (inflight.go).
			if p.limit != nil && s.sem != nil {
				p.admitSubmit(ctx, log, s, pd)
				break
			}
			p.startSubmit(ctx, s, pd, false)
		case pdu.DeliverSMResp: // client confirming a pushed receipt/MO [SCOPE P2-RESP]
			s.resolve(pd.Header.SequenceNumber, pd.Header.CommandStatus)
		case pdu.EnquireLink:
			s.lastEnq.Store(time.Now().UnixNano())
			s.send(pdu.EnquireLinkRespPDU(pd.Header.SequenceNumber))
		case pdu.EnquireLinkResp: // client confirming OUR keepalive [SCOPE P2-KEEPALIVE] — no reply needed
			s.lastEnq.Store(time.Now().UnixNano())

		case pdu.Unbind:
			s.send(pdu.UnbindRespPDU(pd.Header.SequenceNumber))
			return
		case pdu.UnbindResp:
			// The client's answer to OUR unbind (a bind the gateway ended, or
			// the shutdown broadcast): the session is over. It used to fall
			// to the default below and be answered with a generic_nack.
			return
		case pdu.SubmitMulti:
			s.send(pdu.GenericNackPDU(pdu.ESME_RINVCMDID, pd.Header.SequenceNumber))
			p.recordSubmitRefusal(s, nil, "UNSUPPORTED_COMMAND", "submit_multi is not supported: send one submit_sm per number")
		default:
			s.send(pdu.GenericNackPDU(pdu.ESME_RINVCMDID, pd.Header.SequenceNumber))
		}
	}
}

// enquireLinkLoop proactively sends enquire_link to a bound client at its
// configured interval until the session ends. [SCOPE P2-KEEPALIVE]
func (p *Process) enquireLinkLoop(s *session) {
	every := enquireLinkSec(s.acct)
	t := time.NewTicker(time.Duration(every) * time.Second)
	defer t.Stop()
	seq := seqEnquireLink
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			seq = wrapSeq(&seq, seqEnquireLink)
			s.send(pdu.EnquireLinkPDU(seq))
			// An interval changed on the account reaches a bind already up.
			if now := enquireLinkSec(p.liveAccount(s)); now != every {
				every = now
				t.Reset(time.Duration(every) * time.Second)
			}
		}
	}
}

// enquireLinkSec/idleTimeoutSec resolve the account's own configured values
// (accounts.smpp_config), falling back to the previous hardcoded defaults
// when unset. [SCOPE P2-KEEPALIVE, PO 2026-07-27]
func enquireLinkSec(a *refdata.Account) int {
	if a != nil && a.SMPPEnquireLinkSec > 0 {
		return a.SMPPEnquireLinkSec
	}
	return 30
}

func idleTimeoutSec(a *refdata.Account) int {
	if a != nil && a.SMPPIdleTimeoutSec > 0 {
		return a.SMPPIdleTimeoutSec
	}
	return 90
}

// respTimeout bounds how long a pushed deliver_sm waits for the client's
// deliver_sm_resp before the job is left for redelivery. [SCOPE P2-RESP]
const respTimeout = 10 * time.Second

// packedGSM7 reports whether this bind's account wants its GSM-7 packed into
// septets. It is read from the account as it was at bind time, like the rest
// of the bind's settings: a change takes effect when the client binds again.
func (s *session) packedGSM7() bool { return s != nil && s.acct != nil && s.acct.SMPPPackedGSM7 }

// payloadTLV reports whether a long inbound message should reach this bind as
// one deliver_sm with message_payload: the account asked for it and the bind
// declared SMPP 3.4 or later (the TLV does not exist before 3.4).
func (s *session) payloadTLV() bool {
	return s != nil && s.acct != nil && s.acct.SMPPMultipartMode == "tlv" && s.ifaceVersion >= 0x34
}
