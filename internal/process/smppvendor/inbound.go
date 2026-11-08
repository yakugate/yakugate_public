package smppvendor

import (
	"context"
	"hash/fnv"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
	"github.com/yakugate/yakugate/lib/smpp/session"
)

// inboundPool applies the receipts and MO a vendor sends us, off the session's
// read goroutine.
//
// The reader used to write every deliver_sm to the database itself, and the
// same goroutine reads the vendor's submit_sm_resp. Under a slow database the
// responses queued behind those writes: the window stayed full, sending
// stalled, and "vendor latency" measured our own backlog — 15 s against a mock
// that answers instantly (load run 2026-09-10). Now the reader hands the work
// here and goes straight back to reading; the worker answers the deliver_sm
// once the work is kept (deliverAnswer, 18 Oct 2026).
//
// Work is split by a key — the vendor message id for a receipt, the
// subscriber's number for an MO — so everything for one key runs on ONE
// worker, in arrival order. That keeps a message's ENROUTE ahead of its
// DELIVRD even when the two arrive on different binds of the same vendor. The
// pool is process-wide for the same reason.
//
// Nothing is dropped. A full worker makes the reader wait — the old behaviour,
// now only under real overload — and close drains every queued item before it
// returns. An item lost all the same (the process killed) was never answered,
// so its vendor sends it again.
//
// Receipts are applied MANY AT A TIME (2026-10-13): a worker takes the receipt
// at the head of its queue together with the receipts waiting behind it, up
// to receiptBatch, and hands them to applyMany in one call
// (dlr.Service.ApplyMany: one read, one close, one filing for all of them).
// One at a time each receipt was five round trips to Redis, and the workers'
// pace was the pace a session could send at. The order on a worker is kept:
// a batch ends at the first item that is not a receipt.
type inboundPool struct {
	mu     sync.RWMutex // close takes it for writing; submit for reading
	closed bool
	lanes  []chan inboundItem
	wg     sync.WaitGroup
	// applyMany applies a worker's waiting receipts. nil = each receipt's
	// own fn is run (a pool made without one, as in tests).
	applyMany func(batch []inboundReceipt)
}

// inboundItem is one piece of work on a worker's queue: fn, or a receipt.
type inboundItem struct {
	fn func()
	rc *inboundReceipt
}

// inboundReceipt is a receipt waiting for its worker.
type inboundReceipt struct {
	in  dlrsvc.ReceiptIn
	cid string // the vendor, for the log line when it cannot be applied
	// answer is the vendor's deliver_sm_resp, sent once the receipt is kept.
	answer deliverAnswer
}

// deliverAnswer is the answer owed to one deliver_sm. It is sent when what
// the deliver_sm carried is KEPT — a receipt applied, an MO staged — and not
// before (go-live test of 18 Oct 2026, P6 E9, P5 E9 and E7).
//
// The reader used to answer ESME_ROK the moment a deliver_sm arrived and hand
// the work to a worker: a kill -9 lost what the workers still had (3 of 34
// answered MO, 22 of 1,500 receipts), and a receipt the store refused was
// only logged (21 lost with MySQL queues) — the vendor had its answer and
// would never send it again. Now a receipt or MO that could not be kept is
// answered ESME_RX_T_APPN, which tells the SMSC to send it again, and one
// still being worked on when the process dies is not answered at all: the
// SMSC sends it again on the next bind. The vendor's own window of
// unanswered deliver_sm is the brake on how many wait.
type deliverAnswer struct {
	cl  *session.Client
	seq uint32
	// due counts the session's deliver_sm not yet answered, so a session
	// that is stopping can wait for them before it unbinds. nil in tests.
	due *atomic.Int64
}

// newAnswer is the answer owed to the deliver_sm seq on cl, counted as due.
func newAnswer(cl *session.Client, seq uint32, due *atomic.Int64) deliverAnswer {
	if due != nil {
		due.Add(1)
	}
	return deliverAnswer{cl: cl, seq: seq, due: due}
}

// send answers the deliver_sm: ESME_ROK when what it carried is kept,
// ESME_RX_T_APPN when it is not. A connection already gone is not an error
// here: the vendor sends again what it has no answer to.
func (a deliverAnswer) send(kept bool) {
	if a.cl == nil {
		return
	}
	status := pdu.ESME_ROK
	if !kept {
		status = pdu.ESME_RX_T_APPN
	}
	_ = a.cl.Write(pdu.DeliverSMRespStatusPDU(status, a.seq))
	if a.due != nil {
		a.due.Add(-1)
	}
}

// receiptBatch is the most receipts a worker applies in one call.
const receiptBatch = 200

// inboundWorkers is YAKU_SMPP_VENDOR_INBOUND_WORKERS (default 4, PO
// 2026-09-11). Without Redis the default is 16 (DAL plan, step 3, 30 Oct
// 2026): a receipt is then applied in the record store one at a time, about
// three commits each, and four workers held receipts near 30 a second.
func inboundWorkers(redis bool) int {
	if n, err := strconv.Atoi(os.Getenv("YAKU_SMPP_VENDOR_INBOUND_WORKERS")); err == nil && n > 0 {
		return n
	}
	if !redis {
		return 16
	}
	return 4
}

// inboundDepth is how many items one worker may have waiting before its
// reader waits. Small on purpose: every queued item is a deliver_sm its
// vendor is waiting to have answered.
const inboundDepth = 256

func newInboundPool(workers int) *inboundPool {
	if workers < 1 {
		workers = 1
	}
	ip := &inboundPool{lanes: make([]chan inboundItem, workers)}
	for i := range ip.lanes {
		ch := make(chan inboundItem, inboundDepth)
		ip.lanes[i] = ch
		ip.wg.Add(1)
		go func() {
			defer ip.wg.Done()
			ip.work(ch)
		}()
	}
	return ip
}

// work is one worker: it runs its queue in order, receipts a batch at a time.
func (ip *inboundPool) work(ch chan inboundItem) {
	for it := range ch {
		if it.rc == nil {
			it.fn()
			continue
		}
		batch := []inboundReceipt{*it.rc}
		var next *inboundItem // the first item behind the receipts that is not one
	gather:
		for len(batch) < receiptBatch {
			select {
			case more, ok := <-ch:
				if !ok {
					break gather
				}
				if more.rc == nil {
					next = &more
					break gather
				}
				batch = append(batch, *more.rc)
			default:
				break gather
			}
		}
		ip.applyMany(batch)
		if next != nil {
			next.fn()
		}
	}
}

// submitReceipt queues a receipt on the worker that owns its key (the
// vendor's message id), to be applied with the others waiting there. Without
// applyMany, or after close, it is applied by itself through one.
func (ip *inboundPool) submitReceipt(key string, rc inboundReceipt, one func()) {
	if ip == nil || ip.applyMany == nil {
		ip.submit(key, one)
		return
	}
	ip.mu.RLock()
	if ip.closed {
		ip.mu.RUnlock()
		one()
		return
	}
	ip.lanes[laneOf(ip, key)] <- inboundItem{rc: &rc}
	ip.mu.RUnlock()
}

// submit queues fn on the worker that owns key. After close — a reader still
// finishing as the process stops — it runs fn on the caller instead.
func (ip *inboundPool) submit(key string, fn func()) {
	if ip == nil {
		fn()
		return
	}
	ip.mu.RLock()
	if ip.closed {
		ip.mu.RUnlock()
		fn()
		return
	}
	ip.lanes[laneOf(ip, key)] <- inboundItem{fn: fn}
	ip.mu.RUnlock()
}

// laneOf is the worker that owns key.
func laneOf(ip *inboundPool, key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(ip.lanes)))
}

// behind reports that the workers are not keeping up: half of all the room in
// front of them is taken. A reader waits when ONE worker's queue is full, and
// the reader is the same goroutine that reads the vendor's submit answers —
// so a session that keeps sending while the workers are behind fills its
// window with messages whose answers nobody is reading, they pass the
// response timeout, and every one of them is sent again (load test
// 2026-10-07: window 20,000, 182,000 timeouts, nothing leaving the queue).
// The submit pump asks this before it claims more: sending waits for the
// receipts, instead of out-running them.
func (ip *inboundPool) behind() bool {
	if ip == nil || len(ip.lanes) == 0 {
		return false
	}
	return ip.queued()*2 >= len(ip.lanes)*inboundDepth
}

// ease is how much of its room a session may use while the workers have work
// waiting: all of it (1) up to an eighth of their queues, none (0) from half —
// where behind says stop — and in step between the two.
//
// behind alone made sending stop and go (go-live test P3 F1, 2026-10-12,
// Saleem: "throughput for out is not constant, gives high tps and then stop
// for a second or two then again gives tps"). A session sent at full speed
// until half the queues were taken, stopped dead until they had drained, and
// began again — 78 stops in one fifteen-minute run. Easing off as the work
// piles up lets sending settle at the rate the receipts are applied at.
func (ip *inboundPool) ease() float64 {
	if ip == nil || len(ip.lanes) == 0 {
		return 1
	}
	total := float64(len(ip.lanes) * inboundDepth)
	waiting := float64(ip.queued())
	lo, hi := total/8, total/2
	switch {
	case waiting <= lo:
		return 1
	case waiting >= hi:
		return 0
	}
	return (hi - waiting) / (hi - lo)
}

// capacity is how many items the workers' queues hold in all.
func (ip *inboundPool) capacity() int {
	if ip == nil {
		return 0
	}
	return len(ip.lanes) * inboundDepth
}

// queued is how many items are waiting across the workers, for monitoring.
func (ip *inboundPool) queued() int {
	if ip == nil {
		return 0
	}
	n := 0
	for _, ch := range ip.lanes {
		n += len(ch)
	}
	return n
}

// close stops taking work and waits until every queued item has run.
func (ip *inboundPool) close() {
	if ip == nil {
		return
	}
	ip.mu.Lock()
	if ip.closed {
		ip.mu.Unlock()
		return
	}
	ip.closed = true
	for _, ch := range ip.lanes {
		close(ch)
	}
	ip.mu.Unlock()
	ip.wg.Wait()
}

// applyReceipts applies the receipts a worker had waiting, in one call
// (dlr.Service.ApplyMany), and answers each one's deliver_sm: ESME_ROK for a
// receipt kept (matched, or filed as an orphan), ESME_RX_T_APPN for one the
// stores refused, which the vendor sends again. Detached from any session's
// context: a stop must finish the work, not cancel it.
func (p *Process) applyReceipts(batch []inboundReceipt) {
	if len(batch) == 0 {
		return
	}
	if p.svc == nil {
		for _, b := range batch {
			b.answer.send(true) // nothing here keeps receipts (tests)
		}
		return
	}
	in := make([]dlrsvc.ReceiptIn, len(batch))
	for i, b := range batch {
		in[i] = b.in
	}
	_, errs := p.svc.ApplyMany(context.Background(), in)
	for i, err := range errs {
		if err != nil {
			p.log.Printf("dlr apply %s: %v — not kept; the vendor is told to send it again", batch[i].in.VendorMsgID, err)
		}
		batch[i].answer.send(err == nil)
	}
}
