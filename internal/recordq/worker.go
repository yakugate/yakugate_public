package recordq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/queueflow"
)

// Sink is the record store as the DB worker writes to it (dal.RecordStore).
type Sink interface {
	Write(ctx context.Context, m model.Message) error
	WriteBatch(ctx context.Context, recs []dal.TerminalRecord) error
	WriteTerminalBatch(ctx context.Context, recs []dal.TerminalRecord) error
	UpdateStatus(ctx context.Context, ref model.RecordRef, s model.Status, vendorMsgID, errorCode string) error
	WriteRejected(ctx context.Context, m model.Message) error
	AppendRawDLR(ctx context.Context, ref model.RecordRef, entry string) error
	// The three-table design (27 Oct 2026): an outgoing message's mt_ row at
	// acceptance, and the vendor's answers (sub_) on their own.
	WriteAccepted(ctx context.Context, msgs []model.Message) error
	WriteAnswers(ctx context.Context, as []model.SubmitAnswer) error
}

// rejectBatcher is the optional Sink capability to write many reject rows in
// one statement.
type rejectBatcher interface {
	WriteRejectedBatch(ctx context.Context, ms []model.Message) error
}

// Two writes of the record store add a row every time they are called: a
// reject row, and a receipt row the gateway makes itself. A task can be seen
// twice (a worker stopped between its write and taking the tasks off the
// list), so the store offers each in a form that leaves a row that is there
// already alone. The reject one reads the table first, so it is used only
// for a batch that may be a repeat (the lane's "doing" mark); the receipt one
// is one small read and is always used. Every other write is safe to repeat
// as it is.
// Charger prices message records as they are written and settles what they
// charge (internal/process/billing Charger — the billing role's own pricing
// and charging). With one set, and a store that can write a record together
// with its price (ratedWriter), a sent message is written already rated: the
// billing role does not have to read it back and update it.
type Charger interface {
	// Price gives each record it can a Rating; the others are written
	// unrated and left to the billing role.
	Price(ctx context.Context, recs []dal.TerminalRecord)
	// Settle applies the billing batches Price started, for the records
	// whose rating was really written.
	Settle(ctx context.Context, recs []dal.TerminalRecord, outs []dal.RatedOutcome)
}

// ratedWriter is the optional Sink capability to write records with their
// price (dal.SubmitRecords.WriteRated).
type ratedWriter interface {
	WriteRated(ctx context.Context, recs []dal.TerminalRecord, receiptRow bool) ([]dal.RatedOutcome, error)
}

type rejectOnce interface {
	WriteRejectedBatchOnce(ctx context.Context, ms []model.Message) error
}
type rawDLROnce interface {
	AppendRawDLROnce(ctx context.Context, ref model.RecordRef, entry string) error
}

// lockFor is how long a lane's turn lasts without being renewed. It is
// renewed on every pass of the lane's loop and, while a batch is being
// written, every few seconds beside the write (keepTurn) — so it can be
// short: a worker that is KILLED holds its lanes for this long and no more,
// and the worker started in its place writes on (it was 60 s, the time one
// batch may take; a restart then left every lane unwritten for a minute).
// writeFor is how long one batch may take.
const (
	lockFor  = 15 * time.Second
	writeFor = 30 * time.Second
	idleWait = 100 * time.Millisecond
	// parkEvery is how often the parked tasks of a lane are tried again.
	parkEvery = time.Second
	// aliveEvery / aliveFor: how often a running DB worker says so in Redis,
	// and how long that is believed. A worker that stops takes it back at
	// once (leave); aliveFor is for one that is killed (it was 15 s).
	aliveEvery = 3 * time.Second
	aliveFor   = 10 * time.Second
)

// The turn, the batch's end and the park list's end are scripts of the
// record queue's DAL (dal.RecordLanes: TakeTurn, Done, ParkDone, Leave), each
// one round trip as before.

var errTurnLost = errors.New("another DB worker took over this lane")

// Worker writes the lanes out. One per DB worker process.
type Worker struct {
	q    *Queue
	sink Sink
	logf func(string, ...any)

	// Idle is how often a lane is checked, busy or idle (YAKU_QUEUE_RECORDS_
	// EVERY_MS since issue 7; idleWait when zero). A gateway whose roles write
	// their own records sets it long: the lanes are only looked at in case
	// something is left.
	Idle time.Duration
	// PerCheck is the most tasks one check of a lane takes, a batch at a time
	// (YAKU_QUEUE_RECORDS_PER_INTERVAL); 0 = what is waiting.
	PerCheck int

	// Charger, when set before Run, prices and charges the message records
	// this worker writes (see Charger). nil = they are written unrated and
	// the billing role charges them, as before.
	Charger Charger

	// Later, set before Run, takes the receipts whose message has no record
	// yet: their own row is written, the status waits there for the record
	// (the DB worker role gives it the receipts' own list, which it also
	// writes out — receiptfacts.Queue). See receipts.go.
	Later func(ctx context.Context, ws []dlr.ReceiptWrite) error
	// owedC counts the receipts handed to Later.
	owedC atomic.Int64

	written, putBack, dropped atomic.Int64
	// repeats counts batches written the careful way because the one before
	// did not end cleanly.
	repeats atomic.Int64
	mu      sync.Mutex
	lanes   map[int]*laneState

	// What Monitoring shows of the worker's work, with rates (2026-10-13,
	// Saleem: "db worker does not show per second stats"): records written,
	// by what they are, the batches they went in, and how long MySQL took.
	started  time.Time
	writtenC monitor.Counter
	batchesC monitor.Counter
	kindC    map[string]*monitor.Counter // by kindName
	tmMu     sync.Mutex
	lastMs   int64 // the last batch's write, in ms
	maxMs    int64 // the longest since start
	sumMs    int64
	lastSize int64 // records in the last batch

	// What StillComing last read of the lanes' heads (see there).
	headMu sync.Mutex
	headAt int64
	headOK bool
	headRd time.Time
}

type laneState struct {
	waiting  int64
	parked   int64 // tasks MySQL would not take, waiting to be tried again
	oldestMs int64 // how long the task at the head has waited
	mine     bool
	lastErr  string
	parkTry  time.Time // when the parked tasks were last tried
}

// NewWorker makes the DB worker's writer.
func NewWorker(q *Queue, sink Sink, logf func(string, ...any)) *Worker {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	w := &Worker{q: q, sink: sink, logf: logf, lanes: map[int]*laneState{}, started: time.Now(),
		kindC: map[string]*monitor.Counter{}}
	for _, name := range []string{"sent_messages", "single_records", "status_updates", "refused_submits",
		"incoming_messages", "receipt_rows", "vendor_receipts", "other"} {
		w.kindC[name] = &monitor.Counter{}
	}
	return w
}

// Run writes every lane out until ctx ends. A lane that turns up later (a
// role started with more lanes) gets its own writer within ten seconds.
// While it runs it says so in Redis (aliveKey), which is how a role that
// queues records knows somebody is writing them.
func (w *Worker) Run(ctx context.Context) {
	running := map[int]bool{}
	var wg sync.WaitGroup
	start := func() {
		lanes, err := w.q.usedLanes(ctx)
		if err != nil {
			return
		}
		for _, l := range lanes {
			if running[l] {
				continue
			}
			running[l] = true
			wg.Add(1)
			go func(lane int) {
				defer wg.Done()
				w.drain(ctx, lane)
			}(l)
		}
	}
	start()
	w.beat(ctx)
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	alive := time.NewTicker(aliveEvery)
	defer alive.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			w.leave()
			return
		case <-t.C:
			start()
		case <-alive.C:
			w.beat(ctx)
		}
	}
}

// beat writes "a DB worker is running" for this deployment: the worker's own
// token, believed for aliveFor. Another worker of the deployment writes the
// same key; the last one to beat holds it.
func (w *Worker) beat(ctx context.Context) {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = w.q.l.Beat(c, w.q.token, aliveFor)
}

// leave takes the sign of life back when the worker stops, once its last
// batch is written. Before (go-live retest, 19 Oct 2026) it was left to run
// out, and the roles waited NoWorkerAfter on top: smpp-server, smpp-vendor,
// dlr and retry still said db_worker_running true 29 s after the worker had
// stopped. Only while the key is its own: when another worker of the
// deployment beat last it is left alone, and when this one did, the other
// writes it again within aliveEvery — inside the roles' WorkerGoneAfter.
func (w *Worker) leave() {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = w.q.l.Leave(c, w.q.token)
}

func (w *Worker) state(lane int) *laneState {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.lanes[lane]
	if s == nil {
		s = &laneState{}
		w.lanes[lane] = s
	}
	return s
}

func (w *Worker) drain(ctx context.Context, lane int) {
	var lastSaid time.Time
	say := func(format string, a ...any) {
		if time.Since(lastSaid) >= 10*time.Second {
			lastSaid = time.Now()
			w.logf(format, a...)
		}
	}
	wait := func(d time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}
	// Leaving, by any way out: give the turn up, so the next DB worker need
	// not wait for it to run out.
	defer func() {
		rel, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = w.q.l.DropTurn(rel, lane, w.q.token)
	}()
	st := w.state(lane)
	for ctx.Err() == nil {
		mine, err := w.q.l.TakeTurn(ctx, lane, w.q.token, lockFor)
		w.mu.Lock()
		st.mine = err == nil && mine == 1
		w.mu.Unlock()
		if err != nil || mine != 1 {
			if !wait(time.Second) {
				return
			}
			continue
		}
		// The parked tasks first, at most once a second: they are older than
		// anything in the lane.
		if time.Since(st.parkTry) >= parkEvery {
			moved, err := w.parkStep(ctx, lane, st)
			if err != nil && !errors.Is(err, errTurnLost) {
				say("records waiting to be tried again (lane %d): %v — kept in Redis", lane, err)
			}
			if !moved {
				st.parkTry = time.Now() // nothing to gain by asking again at once
			}
		}
		// One check: batches until it has taken PerCheck, or the lane is
		// empty; then the next check, busy or idle (issue 7).
		per := w.PerCheck
		if per <= 0 {
			per = math.MaxInt
		}
		failed := false
		for taken := 0; taken < per && ctx.Err() == nil; {
			if taken > 0 {
				// A check of many batches keeps its turn between them.
				if mine, err := w.q.l.TakeTurn(ctx, lane, w.q.token, lockFor); err != nil || mine != 1 {
					break
				}
			}
			ask := min(Batch, per-taken)
			n, err := w.step(ctx, lane, st, ask)
			if err != nil {
				w.mu.Lock()
				st.lastErr = err.Error()
				w.mu.Unlock()
				say("records waiting to be written (lane %d): %v — kept in Redis, tried again", lane, err)
				failed = true
				break
			}
			w.mu.Lock()
			st.lastErr = ""
			w.mu.Unlock()
			taken += n
			if n < ask {
				break // the lane is empty
			}
		}
		if failed {
			if !wait(time.Second) {
				return
			}
			continue
		}
		idle := w.Idle
		if idle <= 0 {
			idle = idleWait
		}
		if !wait(idle) {
			return
		}
	}
}

// keepTurn renews a lane's turn every few seconds until the returned stop is
// called: a batch may take longer to write than the turn lasts.
func (w *Worker) keepTurn(ctx context.Context, lane int) (stop func()) {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(lockFor / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				// Renews only what is still ours (TakeTurn); a turn that was
				// lost is found out by the script that takes the batch off.
				_, _ = w.q.l.TakeTurn(ctx, lane, w.q.token, lockFor)
			}
		}
	}()
	return func() { close(done) }
}

// read decodes what LRANGE gave. A task that cannot be read is counted and
// left out; it is taken off with the rest.
func (w *Worker) read(raw []string, lane int) []Task {
	tasks := make([]Task, 0, len(raw))
	for _, r := range raw {
		var t Task
		if json.Unmarshal([]byte(r), &t) != nil || t.Kind == "" {
			w.dropped.Add(1)
			w.logf("a record task that cannot be read is dropped (lane %d): %.120s", lane, r)
			continue
		}
		tasks = append(tasks, t)
	}
	return tasks
}

// step writes one batch of a lane: at most `most` tasks at its head (Batch
// when 0 or more). It returns how many it took off.
//
// A lane is first in, first out and nothing is ever put back into it. A task
// MySQL refuses is PARKED (parkKey), and so is every later task of the same
// message — in the batch, and in later batches for as long as the message
// has anything parked (heldKey says so). That keeps a message's records in
// the order they were queued even when one of them fails.
func (w *Worker) step(ctx context.Context, lane int, st *laneState, most int) (int, error) {
	if most <= 0 || most > Batch {
		most = Batch
	}
	raw, heldNow, doing, err := w.q.l.Batch(ctx, lane, most)
	if err != nil {
		return 0, err
	}
	if len(raw) == 0 {
		w.mu.Lock()
		st.waiting, st.oldestMs = 0, 0
		w.mu.Unlock()
		return 0, nil
	}
	// The "doing" mark: set before anything is written, cleared by the script
	// that takes the batch off. Found set, the batch before this one did not
	// end cleanly — a worker stopped, a write failed half way, a turn lost —
	// and some of these tasks may be written already: this batch is written
	// the careful way (apply).
	repeat := doing
	if !repeat {
		if err := w.q.l.MarkDoing(ctx, lane, w.q.token); err != nil {
			return 0, err
		}
	} else {
		w.repeats.Add(1)
	}
	tasks := w.read(raw, lane)
	// notWritten is by position in tasks, so what is parked keeps queue order.
	notWritten := map[int]error{}
	todo := make([]Task, 0, len(tasks))
	at := make([]int, 0, len(tasks))
	for i, t := range tasks {
		if _, held := heldNow[t.ID]; held {
			notWritten[i] = errEarlier
			continue
		}
		todo = append(todo, t)
		at = append(at, i)
	}
	wctx, cancel := context.WithTimeout(ctx, writeFor)
	kept := w.keepTurn(ctx, lane)
	began := time.Now()
	failed, wrote, away := w.apply(wctx, todo, repeat)
	took := time.Since(began)
	kept()
	cancel()
	if away != nil {
		// MySQL is away, or busy: not a fault of these tasks. Nothing is taken
		// off, and the mark stays: what was written is met again as a repeat.
		return 0, fmt.Errorf("%d task(s) not written: %w", len(failed), away)
	}
	for j, err := range failed {
		notWritten[at[j]] = err
	}
	now := time.Now().UnixMilli()
	park := make([][2]string, 0, len(notWritten))
	for i, t := range tasks {
		err, bad := notWritten[i]
		if !bad {
			continue
		}
		t.Tries++
		if t.Failed == 0 {
			t.Failed = now
		}
		b, _ := json.Marshal(t)
		park = append(park, [2]string{t.ID, string(b)})
		if err != errEarlier {
			w.logf("a record of %s (%s) was refused and waits to be tried again: %v", t.ID, t.Kind, err)
		}
	}
	// The messages whose record is now written: a status pull is answered by
	// the record from here. One that still has a task parked keeps its entry.
	// A receipt's task says nothing of that: it can be written while its
	// message's record is still further down the lane.
	var written []string
	for i, t := range tasks {
		if _, bad := notWritten[i]; !bad && t.ID != "" && t.Kind != KindReceipt {
			written = append(written, t.ID)
		}
	}
	ok, err := w.q.l.Done(ctx, lane, w.q.token, len(raw), park, written)
	if err != nil {
		return 0, err
	}
	if ok != 1 {
		return 0, errTurnLost
	}
	w.written.Add(int64(wrote))
	w.putBack.Add(int64(len(notWritten)))
	var done []Task
	for i, t := range tasks {
		if _, bad := notWritten[i]; !bad {
			done = append(done, t)
		}
	}
	w.noteBatch(lane, done, took)
	left, _ := w.q.l.Len(ctx, lane)
	w.mu.Lock()
	st.waiting = left
	if len(tasks) > 0 {
		st.oldestMs = now - tasks[0].At
	}
	st.parked += int64(len(notWritten))
	w.mu.Unlock()
	return len(raw), nil
}

// errEarlier is why a task waits when nothing is wrong with it.
var errEarlier = errors.New("an earlier record of this message is still waiting")

// parkStep tries the parked tasks of a lane again: the ones at the head of
// the park list, in their order. It returns true when it wrote any, so the
// caller comes straight back while there is progress to make. What is
// refused again goes back to the head, so the list keeps its order; a task
// refused for KeepFor is let go, loudly.
func (w *Worker) parkStep(ctx context.Context, lane int, st *laneState) (bool, error) {
	raw, err := w.q.l.Parked(ctx, lane, Batch)
	if err != nil {
		return false, err
	}
	if len(raw) == 0 {
		w.mu.Lock()
		st.parked = 0
		w.mu.Unlock()
		return false, nil
	}
	tasks := w.read(raw, lane)
	wctx, cancel := context.WithTimeout(ctx, writeFor)
	// Always the careful way: a parked task is tried many times, and a try
	// that wrote it may not have lived to take it off.
	kept := w.keepTurn(ctx, lane)
	began := time.Now()
	failed, wrote, away := w.apply(wctx, tasks, true)
	took := time.Since(began)
	kept()
	cancel()
	if away != nil {
		return false, fmt.Errorf("%d parked task(s) not written: %w", len(failed), away)
	}
	now := time.Now().UnixMilli()
	var back, gone []string
	var done []Task
	lost := 0
	for i, t := range tasks {
		ferr, bad := failed[i]
		if !bad {
			gone = append(gone, t.ID)
			done = append(done, t)
			continue
		}
		if t.Failed > 0 && now-t.Failed > int64(KeepFor/time.Millisecond) {
			lost++
			w.dropped.Add(1)
			w.logf("a record of %s (%s) was refused for %s and is dropped: %v", t.ID, t.Kind, KeepFor, ferr)
			gone = append(gone, t.ID)
			continue
		}
		t.Tries++
		if t.Tries%300 == 0 && ferr != errEarlier {
			w.logf("a record of %s (%s) is still refused (try %d): %v", t.ID, t.Kind, t.Tries, ferr)
		}
		b, _ := json.Marshal(t)
		back = append(back, string(b))
	}
	ok, err := w.q.l.ParkDone(ctx, lane, w.q.token, len(raw), back, gone)
	if err != nil {
		return false, err
	}
	if ok != 1 {
		return false, errTurnLost
	}
	w.written.Add(int64(wrote))
	w.noteBatch(lane, done, took)
	// A record given up on has left the queue too.
	queueflow.Out(queueflow.REC, fmt.Sprint(lane), int64(lost))
	left, _ := w.q.l.ParkedLen(ctx, lane)
	w.mu.Lock()
	st.parked = left
	w.mu.Unlock()
	return wrote > 0, nil
}

// StillComing says whether the record of msgID, queued before atMs (unix
// milliseconds), may still be waiting to be written: the message's record is
// on a lane (its pending entry, recq:pend), the lanes have not been written
// up to that moment yet, or the message has a task parked.
//
// It is the answer the receipt writer needs before it gives a status up for a
// message "that has no record" (receiptfacts.Queue.StillComing): with the
// records in a queue, no record yet may only mean the queue is behind.
// A Redis that cannot be read answers true — nothing is given up on a guess.
//
// The pending entry is asked first because the times alone missed it (go-live
// run, 14 Oct 2026): at 889 messages a second the vendor role queued a
// message's record more than 5 s after its receipt came, the worker was two
// minutes behind, and once every lane's head was past "receipt + 5 s" the
// answer was "nothing is coming" while the record still waited — 227 messages
// kept `sent` though their clients had DELIVRD.
func (w *Worker) StillComing(ctx context.Context, msgID string, atMs int64) bool {
	if _, queued, err := w.q.Pending(ctx, msgID); err != nil || queued {
		return true
	}
	const slack = 5000 // clocks of two hosts, and a role's own batching
	w.headMu.Lock()
	if time.Since(w.headRd) > 500*time.Millisecond {
		w.headAt, w.headOK = w.q.oldestHead(ctx)
		w.headRd = time.Now()
	}
	headAt, ok := w.headAt, w.headOK
	w.headMu.Unlock()
	if !ok {
		return true
	}
	if headAt != 0 && headAt <= atMs+slack {
		return true
	}
	lanes, err := w.q.usedLanes(ctx)
	if err != nil {
		return true
	}
	held, err := w.q.l.HeldIn(ctx, lanes, msgID)
	if err != nil {
		return true
	}
	return held
}

// apply writes a batch of tasks. It returns the ones that were not written,
// by their position in tasks, and how many were.
//
// away is set when a write failed because MySQL could not be reached or was
// busy (isAway) — not a fault of the task. The caller then takes nothing off
// its list: every write here leaves a record that is already there as it is,
// so writing the batch again is safe.
//
// Tasks of different messages may be written in any order, so each kind goes
// to MySQL in one statement. Two tasks of the SAME message must keep their
// order (a message sent, then failed by the retry role): the batch is taken
// in rounds — the first task of each message, then the second of each, and
// so on — and a round holds one task a message.
//
// repeat says some of these tasks may have been written before (step); the
// writes that would add a second row then look first.
func (w *Worker) apply(ctx context.Context, tasks []Task, repeat bool) (failed map[int]error, wrote int, away error) {
	failed = map[int]error{}
	seen := map[string]int{}
	var rounds [][]int
	// The vendors' receipts are written last, after every round: a receipt's
	// status lands on its message's record, and that record may be a task of
	// this same batch that was queued a moment AFTER the receipt (receipts.go).
	var receipts []int
	for i, t := range tasks {
		if t.Kind == KindReceipt {
			receipts = append(receipts, i)
			continue
		}
		r := seen[t.ID]
		seen[t.ID] = r + 1
		if r == len(rounds) {
			rounds = append(rounds, nil)
		}
		rounds[r] = append(rounds[r], i)
	}
	held := map[string]bool{} // a message whose earlier task failed: its later ones wait too
	fail := func(i int, err error) {
		failed[i] = err
		held[tasks[i].ID] = true
		if away == nil && isAway(err) {
			away = err
		}
	}
	for _, round := range rounds {
		var accepted, terminal, batch, answers, rejects, orphans, matched, lookups, mos, tries, single []int
		for _, i := range round {
			t := tasks[i]
			if held[t.ID] {
				failed[i] = errEarlier
				continue
			}
			switch t.Kind {
			case KindAccepted:
				accepted = append(accepted, i)
			case KindAnswers:
				answers = append(answers, i)
			case KindTerminal:
				terminal = append(terminal, i)
			case KindBatch:
				batch = append(batch, i)
			case KindReject:
				rejects = append(rejects, i)
			case KindOrphan:
				orphans = append(orphans, i)
			case KindOrphanMatched:
				matched = append(matched, i)
			case KindLookup:
				lookups = append(lookups, i)
			case KindMO:
				mos = append(mos, i)
			case KindTry, KindMOOutcome:
				tries = append(tries, i)
			default:
				single = append(single, i)
			}
		}
		pick := func(idx []int) []Task {
			ts := make([]Task, len(idx))
			for k, i := range idx {
				ts[k] = tasks[i]
			}
			return ts
		}
		for _, g := range []struct {
			idx   []int
			write func([]Task) error
		}{
			// A message's mt_ row first (acceptance), then what its vendor said.
			{accepted, func(ts []Task) error { return w.writeAccepted(ctx, ts) }},
			{terminal, func(ts []Task) error { return w.writeRecords(ctx, ts, true) }},
			{batch, func(ts []Task) error { return w.writeRecords(ctx, ts, false) }},
			{answers, func(ts []Task) error { return w.writeAnswers(ctx, ts) }},
			{rejects, func(ts []Task) error { return w.writeRejects(ctx, ts, repeat) }},
			{orphans, func(ts []Task) error { return w.writeOrphans(ctx, ts, repeat) }},
			{lookups, func(ts []Task) error { return w.writeLookups(ctx, ts, repeat) }},
			// An incoming message's mo_ row before its tries (a message's
			// final try rides a later round anyway: one task a message a round).
			{mos, func(ts []Task) error { return w.writeMOs(ctx, ts, repeat) }},
			{tries, func(ts []Task) error { return w.writeTries(ctx, ts, repeat) }},
			{matched, func(ts []Task) error { return w.writeMatched(ctx, ts) }},
		} {
			if len(g.idx) == 0 {
				continue
			}
			err := g.write(pick(g.idx))
			if err == nil {
				wrote += len(g.idx)
				continue
			}
			if isAway(err) || len(g.idx) == 1 {
				for _, i := range g.idx {
					fail(i, err)
				}
				continue
			}
			// MySQL refused the statement: one of its rows is at fault. One at
			// a time tells which.
			for _, i := range g.idx {
				if err := g.write([]Task{tasks[i]}); err != nil {
					fail(i, err)
					continue
				}
				wrote++
			}
		}
		for _, i := range single {
			if err := w.writeOne(ctx, tasks[i]); err != nil {
				fail(i, err)
				continue
			}
			wrote++
		}
	}
	if len(receipts) > 0 {
		todo := receipts[:0:0]
		for _, i := range receipts {
			if held[tasks[i].ID] {
				failed[i] = errEarlier // its message's record was refused: it waits behind it
				continue
			}
			todo = append(todo, i)
		}
		wrote += w.writeReceipts(ctx, tasks, todo, repeat, fail)
	}
	return failed, wrote, away
}

// isAway says whether a write failed for a reason that is not the task's:
// MySQL could not be reached, was busy, is read-only, lacks a table or a
// grant. Such a failure is waited out with the tasks where they are — an
// operator mends it and nothing was lost. Only an error that names the ROW
// (rowFault) or a task that cannot be read parks the task, where it is tried
// again for KeepFor and then let go. Anything unknown counts as "away": a
// lane that waits is seen on Monitoring; a record let go is gone.
func isAway(err error) bool {
	if err == nil {
		return false
	}
	var bad *badTask
	if errors.As(err, &bad) {
		return false
	}
	// The rest is the record store's to say (dal.RecordAway): the message the
	// task is about has no record yet — the task's own matter, so it is
	// parked and tried again every second (a record queued on another lane or
	// deployment lands later), not a MySQL that is away, which would hold
	// the whole lane for good — or an error about the values of the ROW.
	return dal.RecordAway(err)
}

// badTask is a task that cannot be turned into a write at all.
type badTask struct{ why string }

func (b *badTask) Error() string { return b.why }

func (w *Worker) writeRecords(ctx context.Context, ts []Task, receiptRow bool) error {
	recs := make([]dal.TerminalRecord, 0, len(ts))
	for _, t := range ts {
		var r dal.TerminalRecord
		if err := json.Unmarshal(t.D, &r); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		recs = append(recs, r)
	}
	if w.Charger != nil {
		if rw, ok := w.sink.(ratedWriter); ok {
			// Priced, written with the price, then charged. A record that
			// was on the log already keeps what it had (WriteRated says
			// which), so a task seen twice is charged once.
			w.Charger.Price(ctx, recs)
			outs, err := rw.WriteRated(ctx, recs, receiptRow)
			if err != nil {
				return err
			}
			w.Charger.Settle(ctx, recs, outs)
			return nil
		}
	}
	if receiptRow {
		return w.sink.WriteTerminalBatch(ctx, recs)
	}
	return w.sink.WriteBatch(ctx, recs)
}

// writeAccepted writes the mt_ rows of messages accepted (kind m): one
// message a task. A message already there is left as it is.
func (w *Worker) writeAccepted(ctx context.Context, ts []Task) error {
	ms := make([]model.Message, 0, len(ts))
	for _, t := range ts {
		var m model.Message
		if err := json.Unmarshal(t.D, &m); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		ms = append(ms, m)
	}
	return w.sink.WriteAccepted(ctx, ms)
}

// writeAnswers writes vendor answers that came on their own (kind a): a
// message's answers a task. An answer there already is left as it is.
func (w *Worker) writeAnswers(ctx context.Context, ts []Task) error {
	var as []model.SubmitAnswer
	for _, t := range ts {
		var one []model.SubmitAnswer
		if err := json.Unmarshal(t.D, &one); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		as = append(as, one...)
	}
	return w.sink.WriteAnswers(ctx, as)
}

func (w *Worker) writeRejects(ctx context.Context, ts []Task, repeat bool) error {
	ms := make([]model.Message, 0, len(ts))
	var once []model.Message // put again after an unclear answer (Task.Once)
	for _, t := range ts {
		var m model.Message
		if err := json.Unmarshal(t.D, &m); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		if t.Once && !repeat {
			once = append(once, m)
			continue
		}
		ms = append(ms, m)
	}
	if len(once) > 0 {
		// After the others: a first copy of the same refusal in this batch is
		// written first, and the second then finds its row.
		if err := w.writeRejectsPlain(ctx, ms, false); err != nil {
			return err
		}
		return w.writeRejectsPlain(ctx, once, true)
	}
	return w.writeRejectsPlain(ctx, ms, repeat)
}

// writeRejectsPlain writes refusals' rows; careful = nothing that is there
// already (a batch seen twice, or a task put again).
func (w *Worker) writeRejectsPlain(ctx context.Context, ms []model.Message, careful bool) error {
	if len(ms) == 0 {
		return nil
	}
	if careful {
		if ro, ok := w.sink.(rejectOnce); ok {
			return ro.WriteRejectedBatchOnce(ctx, ms)
		}
	}
	if rb, ok := w.sink.(rejectBatcher); ok && len(ms) > 1 {
		return rb.WriteRejectedBatch(ctx, ms)
	}
	for _, m := range ms {
		if err := w.sink.WriteRejected(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// closeOnce is the sink's careful CloseWithReceipt: a row of the receipt
// there already changes nothing (a task seen twice).
type closeOnce interface {
	CloseWithReceiptOnce(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode, entryJSON string, mayClose bool) (bool, error)
}

// The rows the roles wrote to MySQL themselves until 28 Oct 2026 (orphans,
// orphans matched, lookups with no message): the sink's batch writers, the
// careful way (nothing there already) for a batch that may be a repeat or a
// task put again.
type orphanSink interface {
	WriteOrphans(ctx context.Context, rows []dal.OrphanRow, once bool) error
	MarkOrphansMatched(ctx context.Context, ms []dal.OrphanMatch) error
	WriteLookups(ctx context.Context, rows []dal.LookupRow, once bool) error
}

func (w *Worker) orphanSink() (orphanSink, error) {
	if os, ok := w.sink.(orphanSink); ok {
		return os, nil
	}
	return nil, &badTask{"this record store cannot write orphans or lookups"}
}

func (w *Worker) writeOrphans(ctx context.Context, ts []Task, repeat bool) error {
	sink, err := w.orphanSink()
	if err != nil {
		return err
	}
	rows := make([]dal.OrphanRow, 0, len(ts))
	careful := repeat
	for _, t := range ts {
		var r dal.OrphanRow
		if err := json.Unmarshal(t.D, &r); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		careful = careful || t.Once
		rows = append(rows, r)
	}
	return sink.WriteOrphans(ctx, rows, careful)
}

func (w *Worker) writeLookups(ctx context.Context, ts []Task, repeat bool) error {
	sink, err := w.orphanSink()
	if err != nil {
		return err
	}
	rows := make([]dal.LookupRow, 0, len(ts))
	careful := repeat
	for _, t := range ts {
		var r dal.LookupRow
		if err := json.Unmarshal(t.D, &r); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		careful = careful || t.Once
		rows = append(rows, r)
	}
	return sink.WriteLookups(ctx, rows, careful)
}

// writeMOs files incoming messages, many in one go (D-140).
func (w *Worker) writeMOs(ctx context.Context, ts []Task, repeat bool) error {
	sink, ok := w.sink.(dal.MORecords)
	if !ok {
		return &badTask{"this record store cannot write incoming messages in batches"}
	}
	ms := make([]model.Message, 0, len(ts))
	careful := repeat
	for _, t := range ts {
		var m model.Message
		if err := json.Unmarshal(t.D, &m); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		careful = careful || t.Once
		ms = append(ms, m)
	}
	return sink.WriteMOs(ctx, ms, careful)
}

// writeTries files tries to give clients an MO or a receipt, many in one go
// (D-140). A KindMOOutcome task (queued by a build before it) is what became
// of an MO: its final try, the message with it.
func (w *Worker) writeTries(ctx context.Context, ts []Task, repeat bool) error {
	sink, ok := w.sink.(dal.ClientTries)
	if !ok {
		return &badTask{"this record store cannot write client tries"}
	}
	tries := make([]dal.ClientTry, 0, len(ts))
	careful := repeat
	for _, t := range ts {
		careful = careful || t.Once
		if t.Kind == KindMOOutcome {
			var m model.Message
			if err := json.Unmarshal(t.D, &m); err != nil {
				return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
			}
			tries = append(tries, outcomeTry(m))
			continue
		}
		var tr dal.ClientTry
		if err := json.Unmarshal(t.D, &tr); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		if tr.CompanyID == 0 {
			tr.CompanyID = t.Co
		}
		tries = append(tries, tr)
	}
	return sink.WriteClientTries(ctx, tries, careful)
}

// outcomeTry is an MO's outcome as an older build queued it, as a final try:
// its try id the MO's own (written once however often it is read).
func outcomeTry(m model.Message) dal.ClientTry {
	m.Direction = model.DirectionMO
	t := dal.ClientTry{TryID: m.ID, Kind: dal.TryMO, MsgID: m.ID, CompanyID: m.CompanyID, AccountID: m.AccountID,
		Result: dal.TryResultOf(m.Status), Final: true, ErrorCode: m.ErrorCode, At: time.Now(), Message: &m}
	if m.Ingress == model.TransportSMPP || m.Ingress == model.TransportHTTP {
		t.Channel = string(m.Ingress)
	}
	return t
}

// writeMatched notes filed receipts as applied; safe to repeat as it is.
func (w *Worker) writeMatched(ctx context.Context, ts []Task) error {
	sink, err := w.orphanSink()
	if err != nil {
		return err
	}
	ms := make([]dal.OrphanMatch, 0, len(ts))
	for _, t := range ts {
		var m dal.OrphanMatch
		if err := json.Unmarshal(t.D, &m); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		ms = append(ms, m)
	}
	return sink.MarkOrphansMatched(ctx, ms)
}

func (w *Worker) writeOne(ctx context.Context, t Task) error {
	switch t.Kind {
	case KindWrite:
		var m model.Message
		if err := json.Unmarshal(t.D, &m); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		// A record that is already there is left as it is, as the roles
		// that write this way have always had it (retry, expiry, MO).
		if err := w.sink.Write(ctx, m); err != nil && !isDup(err) {
			return err
		}
		return nil
	case KindStatus:
		var s statusChange
		if err := json.Unmarshal(t.D, &s); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		return w.sink.UpdateStatus(ctx, model.RecordRef{MsgID: t.ID, CompanyID: t.Co}, s.Status, s.VendorMsgID, s.ErrorCode)
	case KindRawDLR:
		var entry string
		if err := json.Unmarshal(t.D, &entry); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		ref := model.RecordRef{MsgID: t.ID, CompanyID: t.Co}
		if ro, ok := w.sink.(rawDLROnce); ok {
			return ro.AppendRawDLROnce(ctx, ref, entry)
		}
		return w.sink.AppendRawDLR(ctx, ref, entry)
	case KindClose:
		var k closeTask
		if err := json.Unmarshal(t.D, &k); err != nil {
			return &badTask{fmt.Sprintf("task of %s cannot be read: %v", t.ID, err)}
		}
		ref := model.RecordRef{MsgID: t.ID, CompanyID: t.Co}
		if co, ok := w.sink.(closeOnce); ok {
			_, err := co.CloseWithReceiptOnce(ctx, ref, k.Status, k.VendorMsgID, k.ErrorCode, k.Entry, k.May)
			return err
		}
		if c, ok := w.sink.(interface {
			CloseWithReceipt(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode, entryJSON string, mayClose bool) (bool, error)
		}); ok {
			_, err := c.CloseWithReceipt(ctx, ref, k.Status, k.VendorMsgID, k.ErrorCode, k.Entry, k.May)
			return err
		}
		return &badTask{"this record store cannot close a message with its receipt"}
	}
	return &badTask{fmt.Sprintf("a record task of a kind this build does not know: %q", t.Kind)}
}

// isDup reports the record store's duplicate-key error (MySQL's 1062).
func isDup(err error) bool { return dal.RecordDuplicate(err) }

// Section is the DB worker's side for its status page: per lane how many
// tasks wait, how many are parked (MySQL would not take them; tried again
// every second) and how long the one at the head has waited, and since the
// process started how many were written, parked, and dropped.
func (w *Worker) Section() map[string]any {
	w.mu.Lock()
	// The lanes as a list, in order: a screen draws a list as a table. Every
	// list this deployment has is named, also one nothing has gone through
	// yet — an empty queue is shown as empty, not left out.
	seen := map[int]bool{}
	ids := make([]int, 0, len(w.lanes)+w.q.lanes)
	for l := 0; l < w.q.lanes; l++ {
		ids, seen[l] = append(ids, l), true
	}
	for l := range w.lanes {
		if !seen[l] {
			ids = append(ids, l)
		}
	}
	sort.Ints(ids)
	lanes := make([]map[string]any, 0, len(ids))
	var waiting, parked, oldest int64
	for _, l := range ids {
		s := w.lanes[l]
		if s == nil {
			s = &laneState{}
		}
		lanes = append(lanes, map[string]any{"lane": l, "waiting": s.waiting, "parked": s.parked,
			"oldest_ms": s.oldestMs, "written_by_this_worker": s.mine, "last_error": s.lastErr})
		waiting += s.waiting
		parked += s.parked
		if s.oldestMs > oldest {
			oldest = s.oldestMs
		}
	}
	w.mu.Unlock()
	up := 0.0
	if !w.started.IsZero() {
		up = time.Since(w.started).Seconds()
	}
	byKind := map[string]any{}
	for name, c := range w.kindC {
		byKind[name] = c.Snapshot(up)
	}
	batches := w.batchesC.Snapshot(up)
	w.tmMu.Lock()
	last, most, sum, size := w.lastMs, w.maxMs, w.sumMs, w.lastSize
	w.tmMu.Unlock()
	sec := map[string]any{
		"waiting": waiting, "parked": parked, "oldest_ms": oldest, "lanes": lanes,
		"written": w.writtenC.Snapshot(up), "written_by_kind": byKind, "batches": batches,
		"last_batch_records": size, "last_write_ms": last, "longest_write_ms": most,
		"put_back": w.putBack.Load(), "dropped": w.dropped.Load(),
		"repeat_batches": w.repeats.Load(),
		// Receipts that came before their message's record: row written,
		// status handed on to wait for the record.
		"receipts_before_their_record": w.owedC.Load(),
	}
	if batches.Total > 0 {
		sec["average_write_ms"] = float64(sum) / float64(batches.Total)
		sec["average_batch_records"] = float64(w.written.Load()) / float64(batches.Total)
	}
	return sec
}

// kindName is how a task's kind reads on a screen.
func kindName(k string) string {
	switch k {
	case KindBatch, KindTerminal:
		return "sent_messages"
	case KindWrite:
		return "single_records"
	case KindStatus:
		return "status_updates"
	case KindReject:
		return "refused_submits"
	case KindMO, KindMOOutcome:
		return "incoming_messages"
	case KindTry:
		return "client_tries"
	case KindRawDLR:
		return "receipt_rows"
	case KindReceipt:
		return "vendor_receipts"
	}
	return "other"
}

// noteBatch counts one batch that was written and taken off its list: the
// records, by what they are, the time MySQL took, and — for Monitoring's
// Queues tab — what left the list.
func (w *Worker) noteBatch(lane int, done []Task, took time.Duration) {
	if len(done) == 0 {
		return
	}
	n := int64(len(done))
	w.writtenC.Add(n)
	w.batchesC.Add(1)
	by := map[string]int64{}
	for _, t := range done {
		by[kindName(t.Kind)]++
	}
	for name, c := range by {
		if k := w.kindC[name]; k != nil {
			k.Add(c)
		}
	}
	ms := took.Milliseconds()
	w.tmMu.Lock()
	w.lastMs, w.lastSize = ms, n
	w.sumMs += ms
	if ms > w.maxMs {
		w.maxMs = ms
	}
	w.tmMu.Unlock()
	queueflow.Out(queueflow.REC, fmt.Sprint(lane), n)
}
