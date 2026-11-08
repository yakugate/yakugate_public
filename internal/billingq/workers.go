package billingq

import (
	"context"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/monitor"
)

// Workers books a deployment's two billing queues, the selling and the
// buying one (Saleem, 27 Oct 2026), each on its own lanes and turns, so a
// side that cannot be booked (MySQL refusing its rows, a lane held by a
// stopped worker) never holds up the other.
//
// Since 29 Oct 2026 the two queues are in the deployment's queue Redis (the
// local one; Saleem: "an event is sent to the selling queue in local
// redis"). What an earlier build left in the MAIN Redis — the one mixed
// queue of 26 Oct and, when the queue Redis is another server, the sell and
// buy queues of 27 Oct — is booked once and let go when it is empty: nothing
// writes there any more.
type Workers struct {
	sides []*Worker // sell, buy: the live queues
	old   []*Worker // queues of earlier builds, booked until empty
	start time.Time
	// booked and batches are counted across the sides: the status says one
	// rate for the deployment's booking, as before.
	booked, batches *monitor.Counter
}

// NewWorkers books both sides of owner's billing queue ("" = shared) on qc
// (the deployment's queue Redis) into cfgDB (the usage books), and what
// earlier builds left on rc (the main Redis's queues). separate says qc is
// another server than rc (config.QueueRedisSeparate): only then are the main
// Redis's sell and buy queues an earlier build's.
func NewWorkers(rc, qc dal.BillingQueue, separate bool, cfgDB dal.UsageBooks, owner string, lanes int, logf func(string, ...any)) *Workers {
	if qc == nil {
		qc, separate = rc, false
	}
	ws := &Workers{start: time.Now(), booked: &monitor.Counter{}, batches: &monitor.Counter{}}
	mk := func(c dal.BillingQueue, side string) *Worker {
		w := NewWorker(c, cfgDB, owner, side, lanes, logf)
		w.booked, w.batchC = ws.booked, ws.batches
		return w
	}
	for _, side := range billing.Sides {
		ws.sides = append(ws.sides, mk(qc, side))
	}
	if rc == nil {
		// No Redis (plan of 29 Oct 2026, step 3): the queues are the SQL
		// database's (qc); no earlier build left anything in a Redis.
		return ws
	}
	mixed := mk(rc, billing.SideMixed)
	mixed.untilEmpty, mixed.where = true, "the main Redis (one queue for both sides, 26 Oct)"
	ws.old = append(ws.old, mixed)
	if separate {
		for _, side := range billing.Sides {
			w := mk(rc, side)
			w.untilEmpty, w.label, w.where = true, "mixed", "the main Redis ("+side+" side, 27 Oct)"
			ws.old = append(ws.old, w)
		}
	}
	return ws
}

// SetPace sets every side's check interval and batch (issue 7's settings).
func (ws *Workers) SetPace(idle time.Duration, perCheck int) {
	for _, w := range ws.all() {
		w.Idle, w.PerCheck = idle, perCheck
	}
}

func (ws *Workers) all() []*Worker { return append(append([]*Worker{}, ws.sides...), ws.old...) }

// Queue is the queue the live sides are in (the deployment's queue Redis).
func (ws *Workers) Queue() dal.BillingQueue { return ws.sides[0].Queue() }

// Run books both sides until ctx ends, and each queue of an earlier build
// until it is empty.
func (ws *Workers) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, w := range ws.all() {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			w.Run(ctx)
			if w.untilEmpty && ctx.Err() == nil {
				w.logf("the billing queue of an earlier build in %s is booked and empty: its keys are gone", w.where)
			}
		}(w)
	}
	wg.Wait()
}

// Section is the billing queues on the db-worker's status
// (db_worker.billing_queue): every lane of both sides (each says its side),
// the totals of the two, each side's own totals under `sides`, and the
// booking figures of the deployment's worker. Every count is the live length
// of the queues themselves (ZCARD, LLEN): nothing is counted anywhere else.
func (ws *Workers) Section(ctx context.Context) map[string]any {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	lanes := []laneOut{}
	var waiting, delayed, doing, oldest int64
	var lastErr string
	sides := map[string]map[string]any{}
	for _, w := range ws.all() {
		r := w.read(c)
		lanes = append(lanes, r.lanes...)
		waiting += r.waiting
		delayed += r.delayed
		doing += r.doing
		if r.oldest > oldest {
			oldest = r.oldest
		}
		if r.lastErr != "" {
			lastErr = r.lastErr
		}
		if w.untilEmpty && len(r.lanes) == 0 {
			continue
		}
		s, ok := sides[w.sideName()]
		if !ok {
			s = map[string]any{"waiting": int64(0), "delayed": int64(0), "doing": int64(0), "oldest_ms": int64(0), "last_error": ""}
			sides[w.sideName()] = s
		}
		s["waiting"] = s["waiting"].(int64) + r.waiting
		s["delayed"] = s["delayed"].(int64) + r.delayed
		s["doing"] = s["doing"].(int64) + r.doing
		if r.oldest > s["oldest_ms"].(int64) {
			s["oldest_ms"] = r.oldest
		}
		if r.lastErr != "" {
			s["last_error"] = r.lastErr
		}
	}
	var repeat, dupes int64
	var lastN int
	var lastMS int64
	var lastAt time.Time
	for _, w := range ws.all() {
		w.mu.Lock()
		repeat += w.repeat
		dupes += w.dupes
		if w.lastAt.After(lastAt) {
			lastAt, lastN, lastMS = w.lastAt, w.lastN, w.lastMS
		}
		w.mu.Unlock()
	}
	uptimeSec := time.Since(ws.start).Seconds()
	out := map[string]any{}
	for k, v := range sides {
		out[k] = v
	}
	return map[string]any{
		"lanes": lanes, "sides": out, "waiting": waiting, "delayed": delayed, "doing": doing, "oldest_ms": oldest,
		"booked": ws.booked.Snapshot(uptimeSec), "batches": ws.batches.Snapshot(uptimeSec),
		"repeat_batches": repeat, "already_booked": dupes,
		"last_batch_events": lastN, "last_book_ms": lastMS, "last_error": lastErr,
	}
}
