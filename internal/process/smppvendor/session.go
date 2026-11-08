// One bound session: serving it, reading what the vendor sends, and
// unbinding it at the end.

package smppvendor

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
	"github.com/yakugate/yakugate/lib/smpp/session"
	"github.com/yakugate/yakugate/lib/smpp/window"
)

const (
	endStopped = "stopped" // we ended it
	endClosed  = "closed"  // the vendor closed the connection, or it broke
	endSilent  = "silent"  // nothing was heard from the vendor for too long
)

// serveVendor runs the read loop, submit pump and keepalive for one bound
// conn, and says why it ended.
//
// A bind the vendor has gone quiet on is ended here (2026-10-08). The session
// sent enquire_link and never looked for the answer: a vendor that stopped
// answering — or a firewall between that began dropping packets — left the
// session "bound" for as long as the machine's TCP stack took to notice, with
// every message handed to it timing out. Now, when NOTHING has come from the
// vendor (no answer to a keepalive, to a submit, no receipt) for one keepalive
// interval plus the response timeout, the bind is closed and made again.
func (p *Process) serveVendor(ctx context.Context, v vendorConn, key sessionKey, cl *session.Client) string {
	win := window.New[model.Message](v.window)
	p.setBound(key, win)        // monitor: bind is up (item 7)
	defer p.setUnbound(key, "") // monitor: bind is down on any exit
	// Batch record writer (2026-07-26 rework): onSubmitResp used to persist
	// each outcome inline on the READ goroutine — 5 SQL statements + 1 ack per
	// message, measured 41 msg/s, and while the reader sat in MySQL the window
	// never freed, so the configured window was a fiction. Outcomes now flow
	// through this channel to a writer goroutine that flushes multi-row
	// terminal batches (mysql.WriteTerminalBatch — proven 10.8x on blackhole)
	// plus ONE batched ack. The reader only frees window slots and classifies.
	// [SCOPE P3-BATCH]
	outc := make(chan outcome, 4096)
	// The writer must outlive the parent cancel: gracefulUnbind drains
	// submit_sm_resp after ctx is cancelled and those outcomes still need
	// persisting. wcancel (deferred: after the drain) is its only stop signal.
	wctx, wcancel := context.WithCancel(context.WithoutCancel(ctx))
	// A session is over only when its last outcomes are RECORDED. The writer
	// was told to stop and the session returned at once, so a role that was
	// being stopped could end before the writer's last flush: the messages
	// answered in the last tenth of a second were sent, never recorded, and
	// sent again when their lease ran out (restart drill of 2026-10-13: 35
	// messages, 37 PDUs the vendor received twice, after one restart at 300 a
	// second).
	wdone := make(chan struct{})
	defer func() {
		wcancel()
		select {
		case <-wdone:
		case <-time.After(writerStopFor):
			p.log.ForVendor(v.cid).Warnf("shutdown", "session %d: its last records were not written within %s", key.Idx, writerStopFor)
		}
	}()
	go func() {
		defer close(wdone)
		p.batchWriter(wctx, v, outc)
	}()
	// Claimed by this session and not yet on the wire (see heldMsg).
	var held []heldMsg
	// What the session took and did not see through goes back on its queue
	// when it ends (issue 22: a take leaves no lease): its sends with no
	// answer — no answer can come on a connection that is gone — and what it
	// held unsent.
	defer func() { p.putBack(ctx, v.cid, append(unanswered(win), heldMsgs(held)...)) }()
	// The long messages still waiting for parts when the session ends go
	// back on their queue (with the unanswered sends); what their parts
	// answered is written.
	// Runs before the writer is told to stop (defers run last first).
	defer func() {
		if as := p.partsBook(key).stale(0, time.Now()); len(as) > 0 {
			select {
			case outc <- outcome{loose: as}:
			default:
			}
		}
		p.partBooks.Delete(key)
	}()
	dead := make(chan struct{})
	var heardAt atomic.Int64 // when the vendor last sent anything at all
	heardAt.Store(time.Now().UnixNano())
	// The vendor's deliver_sm handed to the workers and not yet answered:
	// a session that is stopping waits for them before it unbinds.
	var due atomic.Int64
	go func() {
		defer close(dead)
		for {
			pd, err := cl.Read()
			if err != nil {
				return
			}
			heardAt.Store(time.Now().UnixNano())
			p.handlePDU(ctx, v, key, cl, win, pd, outc, &due)
		}
	}()
	respTimeout := v.respTimeout
	if respTimeout <= 0 {
		respTimeout = 60 * time.Second
	}
	enquireEvery := v.enquireLinkEvery
	if enquireEvery <= 0 {
		enquireEvery = 20 * time.Second
	}
	// The vendor's queue is checked on a fixed interval, busy or idle, and a
	// check takes at most YAKU_QUEUE_MT_PER_INTERVAL (issue 7; Saleem, 26 Oct
	// 2026: "fixed interval always", 1000 ms by default). From 13 Oct until
	// then a session also sent again as soon as answers freed its window,
	// and checked an empty queue every 150 ms: 133 Redis commands a second
	// with no traffic. The window's own tick (timeouts, late answers, lease
	// holds) does no queue work.
	mt := p.mtCheck()
	check := time.NewTicker(mt.Every)
	tick := time.NewTicker(pumpEvery)
	keep := time.NewTicker(enquireEvery)
	defer check.Stop()
	defer tick.Stop()
	defer keep.Stop()
	var kaSeq uint32 = 0x40000000
	for {
		select {
		case <-check.C:
			held, _ = p.pump(ctx, v, key, cl, win, held, mt.Every)
		case <-ctx.Done():
			// Graceful shutdown (item 4): stop sending, let the read loop drain
			// outstanding submit_sm_resp, then unbind cleanly before the caller
			// closes the socket. In-flight sends are never abandoned mid-window.
			p.gracefulUnbind(v, cl, win, &due)
			return endStopped
		case <-dead:
			return endClosed
		case <-keep.C:
			if quiet := time.Since(time.Unix(0, heardAt.Load())); quiet > enquireEvery+respTimeout {
				p.log.ForVendor(v.cid).Warnf("bind_silent", "session %d: nothing heard from the vendor for %s (keepalive every %s, answers due in %s); closing the bind",
					key.Idx, quiet.Round(time.Second), enquireEvery, respTimeout)
				return endSilent
			}
			kaSeq++
			if cl.Write(pdu.EnquireLinkPDU(kaSeq)) != nil {
				return endClosed
			}
		case <-tick.C:
			now := time.Now()
			book := p.partsBook(key)
			for _, exp := range win.Expired(respTimeout, now) {
				p.log.ForVendor(v.cid).Warnf("submit_timeout", "session %d submit timeout msg=%s (not retransmitted; a late answer is still taken for %s)",
					key.Idx, exp.Msg.ID, lateKeep(respTimeout))
				// Counted for monitoring as well as logged: a rising timeout
				// count is a vendor that has stopped answering, which shows up
				// here long before it shows up as a delivery complaint.
				p.countTimeout(key)
				// No answer in time is an answer of its own (sub_): a part of
				// a long message counts as answered with it (a part with no
				// answer and none refused: the message is sent, Saleem 26 Oct
				// 2026). A late answer is still taken for lateKeep; after
				// that the message is tried again (DropLate below).
				ta := timeoutAnswer(v, key, exp, now)
				if d, loose := book.timedOut(exp, ta); loose {
					outc <- outcome{loose: []model.SubmitAnswer{ta}}
				} else if d != nil {
					p.recordParts(ctx, v, key, exp.Msg, d, outc)
				}
			}
			if gone := win.DropLate(lateKeep(respTimeout), now); len(gone) > 0 {
				p.log.ForVendor(v.cid).Warnf("submit_timeout", "session %d: %d send(s) had no answer %s after their timeout; each is tried again after its gap",
					key.Idx, len(gone), lateKeep(respTimeout))
				// A submit that was never answered: a vendor that charges on
				// submission charges it (billing.Moments.VendorAnswered).
				for _, g := range gone {
					if g.IsLead() {
						m := g.Msg
						p.bill.VendorAnswered(ctx, &m, v.id, false)
						// A long message goes again: what its parts answered
						// this try is kept as rows of their own.
						if as := book.letGo(m.ID); len(as) > 0 {
							outc <- outcome{loose: as}
						}
						// It left its queue when it was taken (issue 22).
						p.tryAgain(ctx, v, m)
					}
				}
			}
		}
	}
}

// gracefulUnbind drains outstanding sends (up to drainTimeout) then sends an SMPP
// unbind so the vendor sees a clean teardown, not a dropped socket. The read-loop
// goroutine is still running here and keeps acking submit_sm_resp, which is what
// lets the window drain. [SCOPE P3] item 4
//
// The vendor's receipts and MO still with the workers (due) are waited for in
// the same time: answered before the unbind, they are not sent again on the
// next bind. One that is not answered by then the vendor does send again.
func (p *Process) gracefulUnbind(v vendorConn, cl *session.Client, win *window.Window[model.Message], due *atomic.Int64) {
	deadline := time.Now().Add(p.drainTimeout)
	for (win.Depth() > 0 || due.Load() > 0) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if left := win.Depth(); left > 0 {
		p.log.ForVendor(v.cid).Warnf("shutdown", "shutdown with %d sends still outstanding (put back on the queue)", left)
	}
	if left := due.Load(); left > 0 {
		p.log.ForVendor(v.cid).Warnf("shutdown", "shutdown with %d receipt(s)/MO not yet answered: the vendor sends them again", left)
	}
	_ = cl.Write(pdu.UnbindPDU(0x7F000001))
}

// handlePDU dispatches an inbound PDU from the vendor.
func (p *Process) handlePDU(ctx context.Context, v vendorConn, key sessionKey, cl *session.Client, win *window.Window[model.Message], pd *pdu.PDU, outc chan<- outcome, due *atomic.Int64) {
	switch pd.Header.CommandID {
	case pdu.SubmitSMResp:
		p.onSubmitResp(ctx, v, key, win, pd, outc)
	case pdu.DeliverSM:
		p.onDeliverSM(ctx, v, key, cl, pd, due)
	case pdu.EnquireLink:
		cl.Write(pdu.EnquireLinkRespPDU(pd.Header.SequenceNumber))
	case pdu.Unbind:
		cl.Write(pdu.UnbindRespPDU(pd.Header.SequenceNumber))
		cl.Close() // triggers reader EOF -> reconnect
	}
}
