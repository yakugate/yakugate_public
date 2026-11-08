package httpvendor

import (
	"context"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// Filing single sends many at a time (load test of 2026-10-13, Saleem: "out
// throughput is not satisfactory").
//
// A bulk call's messages have been filed in one pass since 2026-10-12
// (fileChunk). A single send still filed itself: when its call came back, the
// goroutine that made it queued the record with its ack, the receipt facts
// and the client's notice — some five round trips to Redis a message, one
// message at a time, however many calls were in flight. On a busy Redis that
// filing, not the vendor, set the rate out.
//
// Now a single send hands what the vendor answered to its round's filer and
// goes back for the next call. The filer files what it holds every fileEvery,
// or as soon as it holds fileAtMost: the same writes fileChunk makes for a
// bulk call — one record write with the ack for all of them, one push of
// their notices. What is recorded for a message is what it always was.
//
// A message is never held long: it is claimed for 30 s on its first claim,
// and its record and ack are queued within fileEvery of the vendor's answer.
// A worker that stops waits until its filer has filed everything
// (sendLane.finish).
//
// A SMALL bulk call goes the same way (deliverChunk): a vendor set to Bulk
// whose messages each have a text of their own — one-time codes — makes a
// call a message, and each of those was a filing of its own. The messages of
// one call are always filed in the same batch: fileChunk reads off the batch
// whether a vendor id names one message or several.

// fileEvery is how long an answered message may wait for others to be filed
// with it; fileAtMost how many are filed in one write (the record queue's own
// batch). Variables for tests.
// fileTogetherUnder: a bulk call with fewer messages than this is filed with
// the others of its round; a larger one is a full batch by itself already.
var (
	fileEvery         = 20 * time.Millisecond
	fileAtMost        = 500
	fileTogetherUnder = 100
)

// answered is one single send the vendor has answered for good: it took the
// message (id is its own id, "" when it gave none), or refused it outright
// with refusal as the code.
type answered struct {
	msg     model.Message
	id      string
	refusal string
	ans     model.SubmitAnswer // the vendor's answer, its sub_ row
}

// filer collects a vendor's answered sends and files them in batches. It
// lives as long as the vendor's worker (sendLane): calls outlive the round
// that started them.
type filer struct {
	p    *Process
	ctx  context.Context
	in   chan []answered // one entry = the messages of one vendor call
	done chan struct{}

	mu sync.Mutex
	v  vendorAdapter // the vendor as its last round read it
}

// newFiler starts a filer. close must be called when the calls that feed it
// are all back.
//
// What a vendor has answered is filed even while the worker is being
// stopped: the context's cancel is not passed on (a write that fails is
// still given up at its own time, recordRetryFor).
func (p *Process) newFiler(ctx context.Context, v vendorAdapter) *filer {
	f := &filer{p: p, v: v, ctx: context.WithoutCancel(ctx), in: make(chan []answered, 2*fileAtMost), done: make(chan struct{})}
	go f.run()
	return f
}

// setVendor gives the filer the vendor as the newest round read it (a reload
// may have changed its settings).
func (f *filer) setVendor(v vendorAdapter) {
	f.mu.Lock()
	f.v = v
	f.mu.Unlock()
}

func (f *filer) vendor() vendorAdapter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.v
}

// add hands one answered send over. It waits only when the filer is far
// behind.
func (f *filer) add(a answered) { f.in <- []answered{a} }

// addCall hands over the messages of ONE bulk call the vendor answered:
// taken (ids holds its id for each, "" where it gave none) or refused for
// good with refusal as the code. They are filed together.
func (f *filer) addCall(msgs []model.Message, ids []string, refusal string, answers []model.SubmitAnswer) {
	call := make([]answered, len(msgs))
	for i, m := range msgs {
		call[i] = answered{msg: m, refusal: refusal}
		if i < len(answers) {
			call[i].ans = answers[i]
		}
		if refusal == "" && i < len(ids) {
			call[i].id = ids[i]
		}
	}
	f.in <- call
}

// close files what is left and returns when it is filed.
func (f *filer) close() {
	close(f.in)
	<-f.done
}

func (f *filer) run() {
	defer close(f.done)
	buf := make([]answered, 0, fileAtMost)
	var due <-chan time.Time
	flush := func() {
		due = nil
		if len(buf) == 0 {
			return
		}
		f.file(buf)
		buf = buf[:0]
	}
	for {
		select {
		case call, ok := <-f.in:
			if !ok {
				flush()
				return
			}
			// A call's messages stay in one batch: what is held is filed
			// first when this call would not fit beside it.
			if len(buf) > 0 && len(buf)+len(call) > fileAtMost {
				flush()
			}
			buf = append(buf, call...)
			if due == nil {
				due = time.After(fileEvery)
			}
			if len(buf) >= fileAtMost {
				flush()
			}
		case <-due:
			flush()
		}
	}
}

// file files one batch: the messages the vendor took in one pass, and those
// it refused in one pass per refusal code (fileChunk files a call's messages
// under ONE outcome).
func (f *filer) file(batch []answered) {
	until := time.Now().Add(recordRetryFor)
	var sent []model.Message
	var ids []string
	var sentAns []model.SubmitAnswer
	refused := map[string][]model.Message{}
	refusedAns := map[string][]model.SubmitAnswer{}
	var codes []string // in the order first met
	for _, a := range batch {
		if a.refusal == "" {
			sent = append(sent, a.msg)
			ids = append(ids, a.id)
			sentAns = append(sentAns, a.ans)
			continue
		}
		if _, seen := refused[a.refusal]; !seen {
			codes = append(codes, a.refusal)
		}
		refused[a.refusal] = append(refused[a.refusal], a.msg)
		refusedAns[a.refusal] = append(refusedAns[a.refusal], a.ans)
	}
	v := f.vendor()
	if len(sent) > 0 {
		f.p.fileChunk(f.ctx, v, sent, ids, "", until, sentAns)
	}
	for _, code := range codes {
		f.p.fileChunk(f.ctx, v, refused[code], nil, code, until, refusedAns[code])
	}
}

// sendLane is what one vendor's worker keeps from round to round: the calls
// it has in flight and the filer of what they answered.
//
// Until 2026-10-13 a round WAITED for every call it had started before the
// worker claimed again. One slow answer — the vendor's slowest of two
// thousand — therefore left all the other calls-in-flight places empty until
// it came: with 256 places and a vendor answering in 35 ms on average, a
// round still took as long as its slowest call. Now a round returns when its
// calls are all STARTED. The places are the lane's, so never more than the
// send call's concurrency are in flight, and the worker claims the next
// batch while the last calls of this one are still out.
type sendLane struct {
	n     int           // calls that may be in flight
	sem   chan struct{} // one place a call in flight
	wg    sync.WaitGroup
	filer *filer // nil until the first round that files through it
}

// filerFor is the lane's filer, made at first use.
func (ln *sendLane) filerFor(p *Process, ctx context.Context, v vendorAdapter) *filer {
	if ln.filer == nil {
		ln.filer = p.newFiler(ctx, v)
	} else {
		ln.filer.setVendor(v)
	}
	return ln.filer
}

// start runs the calls of one claimed batch, at most n in flight counting
// those of earlier rounds, and returns when the last one has STARTED.
//
// With n = 1, or inOrder set (a vendor on trial sends ONE message and waits
// for its answer), it first waits for whatever is in flight and then runs the
// calls one after another on this goroutine, as it always did.
//
// When stop ends (the worker is being stopped) no more calls are started and
// the calls already out are left to finish. It returns how many units it
// started: the caller puts the messages of the others back on their queue
// (a take leaves no lease, issue 22).
func (ln *sendLane) start(stop context.Context, units []func(), n int, inOrder bool) (started int) {
	if n <= 1 || inOrder {
		ln.wg.Wait()
		for i, u := range units {
			if stop.Err() != nil {
				return i
			}
			u()
		}
		return len(units)
	}
	if ln.sem == nil || ln.n != n {
		ln.wg.Wait() // the calls of the old setting end first
		ln.sem, ln.n = make(chan struct{}, n), n
	}
	for i, u := range units {
		select {
		case ln.sem <- struct{}{}:
		case <-stop.Done():
			return i
		}
		ln.wg.Add(1)
		go func(u func()) {
			defer ln.wg.Done()
			defer func() { <-ln.sem }()
			u()
		}(u)
	}
	return len(units)
}

// finish waits for the calls in flight and for everything they answered to
// be filed. A worker calls it when it stops.
func (ln *sendLane) finish() {
	ln.wg.Wait()
	if ln.filer != nil {
		ln.filer.close()
		ln.filer = nil
	}
}
