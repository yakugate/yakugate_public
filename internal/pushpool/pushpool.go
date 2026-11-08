// Package pushpool drives the pushes to customers' own endpoints — receipts to
// a dlr_url (P5) and MO to an mo_url (P6) — with one lane per account.
//
// Before this, each role pushed from ONE loop: claim 100 jobs across every
// account once a second, then POST them one at a time. That was a hard ceiling
// of 100 pushes a second for the whole gateway, and one customer whose
// endpoint hung for its full timeout held up every other customer's receipts
// behind it. The endpoint's own rate_limit and submit_concurrency — fields the
// portal has always let an operator set — were read by nothing.
//
// Now every account with an endpoint gets its own lane, paced by that
// endpoint's settings (PO 2026-09-11): at most RatePerSec pushes a second,
// Concurrency of them in flight, each bounded by Timeout. A lane keeps
// claiming while its backlog lasts instead of waiting for the next tick, and a
// slow endpoint slows only its own lane.
package pushpool

import (
	"context"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/yakugate/yakugate/internal/platform/outhttp"
)

// Limits is how hard one endpoint may be pushed.
type Limits struct {
	RatePerSec  int           // pushes a second; <= 0 means unlimited
	Concurrency int           // pushes in flight at once; < 1 means 1
	Timeout     time.Duration // one push; <= 0 means DefaultTimeout
}

// DefaultTimeout bounds a push whose endpoint names no timeout — the value the
// single loop used for everyone.
const DefaultTimeout = 10 * time.Second

// dispatchBudget is how long a lane keeps starting pushes from one claim. The
// stores lease a claimed job for 60 seconds; past that the job is claimable
// again, and a lane still working through an old batch would race whoever
// takes it (the SMPP bind, under dlr_channel='both') into a second delivery.
// Stopping at half the lease leaves the rest of the batch to lapse and come
// back to this lane on its next claim.
const dispatchBudget = 30 * time.Second

// leaseFor is how long the stores lease a claimed job (client_dlr, mo_queue:
// 60 s), and leaseMargin what is kept clear of its end. A push must END
// inside the lease: one still out when it runs out is claimed again and
// pushed a second time while the first is still waiting for its answer
// (go-live test 18 Oct 2026, P5 A3: a webhook set to 90 s got its receipt
// twice, 60 s apart). So a push's timeout is at most maxTimeout, and a push
// starts only while it can still end in time (budgetFor).
const (
	leaseFor    = 60 * time.Second
	leaseMargin = 5 * time.Second
	maxTimeout  = leaseFor - leaseMargin - 5*time.Second
)

// budgetFor is how long after a claim a push with this timeout may still
// start: dispatchBudget, or less when the timeout is long.
func budgetFor(timeout time.Duration) time.Duration {
	if b := leaseFor - leaseMargin - timeout; b < dispatchBudget {
		return b
	}
	return dispatchBudget
}

// Pool runs one lane per account. J is whatever claim returns; the pool never
// looks inside it.
type Pool[J any] struct {
	claim func(ctx context.Context, account int64, n int) ([]J, error)
	push  func(ctx context.Context, client *http.Client, job J)
	idle  time.Duration
	// perCheck is the most jobs one check of a lane takes (SetCheck); 0 = a
	// check takes what is waiting, a batch at a time.
	perCheck int
	// order, when set, names the job's key and its rank: a key's jobs of one
	// claim are pushed one after another, lowest rank first (Order).
	order func(J) (key string, rank int)

	mu    sync.Mutex
	lanes map[int64]*lane
	// running counts every lane goroutine, also one Sync replaced: Stop waits
	// for its pushes too (a lane restarted for new limits just before a stop
	// was not waited for).
	running sync.WaitGroup
}

type lane struct {
	limits Limits
	cancel context.CancelFunc
	done   chan struct{}
}

// New builds a pool. claim takes up to n of one account's due jobs; push
// delivers one, and owns its outcome (ack, reschedule, give up). idle is how
// long a lane waits after finding its backlog empty.
func New[J any](claim func(ctx context.Context, account int64, n int) ([]J, error),
	push func(ctx context.Context, client *http.Client, job J), idle time.Duration) *Pool[J] {
	if idle <= 0 {
		idle = time.Second
	}
	return &Pool[J]{claim: claim, push: push, idle: idle, lanes: map[int64]*lane{}}
}

// SetCheck makes every lane check its account's queue every `every`, busy or
// idle, and take at most perCheck jobs a check (issue 7 of the design audit,
// 26 Oct 2026: YAKU_QUEUE_DLR_* / YAKU_QUEUE_MO_*). Without it a lane checks
// again at once after a full batch and waits idle when the backlog is drained.
// Set it before the first Sync.
func (p *Pool[J]) SetCheck(every time.Duration, perCheck int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if every > 0 {
		p.idle = every
	}
	p.perCheck = perCheck
}

// Order keeps the order of the jobs that share a key — the receipts of one
// message: within a claim they are pushed one after another, lowest rank
// first and then in the order claimed, while jobs of other keys go side by
// side as before. Set it before the first Sync.
//
// Go-live test 18 Oct 2026 (P1 B6, P5 B5/C1): a lane pushed every job of a
// claim at once, up to the endpoint's concurrency, so a message's ACCEPTD and
// its DELIVRD — both ready after the 5 s a bind gets first — raced, and the
// customer got 'sent' after 'delivered' (11% at a 20 ms vendor, every time
// for an account told on acceptance). A rank puts a progress receipt before
// the final one even when they were queued the other way round.
func (p *Pool[J]) Order(key func(J) (string, int)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.order = key
}

// units groups one claim's jobs: each a unit of its own, or with Order the
// jobs of one key together, lowest rank first, in the order their keys
// first came.
func (p *Pool[J]) units(jobs []J) [][]J {
	p.mu.Lock()
	order := p.order
	p.mu.Unlock()
	out := make([][]J, 0, len(jobs))
	if order == nil {
		for _, j := range jobs {
			out = append(out, []J{j})
		}
		return out
	}
	at := map[string]int{}
	ranks := make([][]int, 0, len(jobs))
	for _, j := range jobs {
		k, r := order(j)
		i, seen := at[k]
		if !seen {
			i = len(out)
			at[k] = i
			out = append(out, nil)
			ranks = append(ranks, nil)
		}
		out[i] = append(out[i], j)
		ranks[i] = append(ranks[i], r)
	}
	for i, u := range out {
		if len(u) > 1 {
			rk := ranks[i]
			idx := make([]int, len(u))
			for n := range idx {
				idx[n] = n
			}
			sort.SliceStable(idx, func(a, b int) bool { return rk[idx[a]] < rk[idx[b]] })
			sorted := make([]J, len(u))
			for n, k := range idx {
				sorted[n] = u[k]
			}
			out[i] = sorted
		}
	}
	return out
}

// Sync makes the running lanes match targets: a lane starts for a new account,
// stops for one that is gone, and restarts when its limits change. A stopped
// lane finishes the pushes already in flight; jobs it had claimed but not
// started lapse back to the queue with their lease.
func (p *Pool[J]) Sync(ctx context.Context, targets map[int64]Limits) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for acct, l := range p.lanes {
		if want, ok := targets[acct]; !ok || want != l.limits {
			l.cancel()
			delete(p.lanes, acct)
		}
	}
	for acct, lim := range targets {
		if _, ok := p.lanes[acct]; ok {
			continue
		}
		lctx, cancel := context.WithCancel(ctx)
		l := &lane{limits: lim, cancel: cancel, done: make(chan struct{})}
		p.lanes[acct] = l
		p.running.Add(1)
		go func(acct int64, l *lane) {
			defer p.running.Done()
			defer close(l.done)
			p.run(lctx, acct, l.limits)
		}(acct, l)
	}
}

// Stop ends every lane and waits for their in-flight pushes.
func (p *Pool[J]) Stop() { p.StopWithin(0) }

// StopWithin ends every lane and waits up to limit (0 = for as long as it
// takes) for the pushes already out, also those of lanes Sync replaced. It
// reports whether every lane had ended. A role that is stopping uses it: a
// customer's endpoint may take minutes to answer (timeout_ms), and the
// supervisor kills a role 20 s after asking it to stop. A push still out at
// the limit is not acked; its job comes back with its lease and is pushed
// again.
func (p *Pool[J]) StopWithin(limit time.Duration) bool {
	p.mu.Lock()
	lanes := p.lanes
	p.lanes = map[int64]*lane{}
	p.mu.Unlock()
	for _, l := range lanes {
		l.cancel()
	}
	done := make(chan struct{})
	go func() { p.running.Wait(); close(done) }()
	if limit <= 0 {
		<-done
		return true
	}
	t := time.NewTimer(limit)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

// Lanes reports how many accounts are being pushed to.
func (p *Pool[J]) Lanes() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.lanes)
}

func (p *Pool[J]) run(ctx context.Context, acct int64, lim Limits) {
	conc := lim.Concurrency
	if conc < 1 {
		conc = 1
	}
	timeout := lim.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	if timeout > maxTimeout {
		timeout = maxTimeout // a push must end inside its job's lease
	}
	budget := budgetFor(timeout)
	// On the shared connection pool (outhttp): with several pushes in flight
	// to one customer a plain client opened a new connection for most of them.
	client := outhttp.Client(timeout)
	pace := rate.NewLimiter(rate.Inf, conc)
	if lim.RatePerSec > 0 {
		// Burst of one slot per concurrent push (never more than the rate), so
		// every slot can start at once without a second's pushes exceeding it.
		pace = rate.NewLimiter(rate.Limit(lim.RatePerSec), min(lim.RatePerSec, conc))
	}
	n := batchSize(lim.RatePerSec, conc)
	sem := make(chan struct{}, conc)
	p.mu.Lock()
	every, per := p.idle, p.perCheck
	p.mu.Unlock()
	if per <= 0 {
		per = math.MaxInt
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for ctx.Err() == nil {
		// One check: batches until it has taken per, or the backlog is drained.
		for taken := 0; taken < per && ctx.Err() == nil; {
			ask := min(n, per-taken)
			jobs, err := p.claim(ctx, acct, ask)
			if err != nil || len(jobs) == 0 {
				break
			}
			taken += len(jobs)
			p.pushAll(ctx, jobs, client, pace, sem, budget)
			if len(jobs) < ask {
				break // backlog drained
			}
		}
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}
}

// pushAll pushes one claim's jobs, up to the lane's concurrency at a time and
// at its pace, and returns when they are all done.
func (p *Pool[J]) pushAll(ctx context.Context, jobs []J, client *http.Client, pace *rate.Limiter, sem chan struct{}, budget time.Duration) {
	started := time.Now()
	var wg sync.WaitGroup
	for _, u := range p.units(jobs) {
		if time.Since(started) > budget {
			break // the rest lapse back with their lease; see dispatchBudget
		}
		if pace.Wait(ctx) != nil {
			break
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(u []J) {
			defer wg.Done()
			defer func() { <-sem }()
			for i, j := range u {
				// A key's later jobs wait for the one before them, and
				// for their own place in the pace. One that can no longer
				// end inside its lease lapses back with the rest of its
				// key's jobs — after the ones already pushed, so the order
				// holds.
				if i > 0 && (time.Since(started) > budget || pace.Wait(ctx) != nil) {
					return
				}
				// The push runs to completion even if the lane is stopped
				// mid-flight: abandoning a POST half-way leaves the customer
				// not knowing whether they received it.
				p.push(context.WithoutCancel(ctx), client, j)
			}
		}(u)
	}
	wg.Wait()
}

// batchSize is how many jobs one claim takes: about two seconds of work at the
// endpoint's rate, never fewer than can be in flight at once, and never so
// many that one claim holds a large share of the backlog under lease.
func batchSize(ratePerSec, conc int) int {
	n := conc * 25
	if ratePerSec > 0 {
		n = ratePerSec * 2
	}
	if n < conc {
		n = conc
	}
	if n > 500 {
		n = 500
	}
	return n
}
