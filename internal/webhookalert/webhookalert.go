// Package webhookalert keeps webhook_delivery_alerts — the one row per
// customer endpoint that says "this endpoint is not answering", which the
// Admin API's alert poller mails the customer about — without a config-store
// write for every message.
//
// It used to be written by the dlr and mo roles themselves: an upsert for
// every failed push and a DELETE for every push that succeeded. A DELETE per
// delivered receipt is a config-store round trip on the delivery path — 500
// receipts a second was 500 of them, each finding nothing to delete — and an
// endpoint that is down turned every waiting message into an upsert on ONE
// row, all queued on its lock (2026-10-06: 181,000 waiting MOs for a dead
// webhook had the mo role wait 98,000 times for a config connection).
//
// The row means the same as before; it is written less often:
//
//   - a failure is counted in memory and written at most once a second per
//     endpoint, the counts added up (the first one at once, so an outage
//     shows without delay);
//   - a success clears the row at once when this process had recorded
//     failures for the endpoint; otherwise at most once every ClearEvery,
//     because the failures may have been another process's (a second
//     deployment, or this role before a restart).
//
// Flush writes what is still counted; a role calls it from its own loop so
// the last failures of a burst are not left waiting for the next one.
package webhookalert

import (
	"context"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// FlushEvery is the least time between two writes of one endpoint's failures.
const FlushEvery = time.Second

// ClearEvery is how often a success clears the row of an endpoint this
// process has seen no failure for.
const ClearEvery = 10 * time.Second

// Tracker writes one kind's rows ("dlr" or "mo").
type Tracker struct {
	db   dal.WebhookAlerts
	kind string
	logf func(string, ...any)
	now  func() time.Time

	mu  sync.Mutex
	eps map[int64]*endpoint
}

type endpoint struct {
	failing   bool // this process wrote (or holds) failures since the last clear
	failed    int  // counted, not yet written
	gaveUp    int
	lastErr   string
	url       string
	account   int64
	lastWrite time.Time
	lastClear time.Time
}

// New makes a tracker on the config store's rows (dal.WebhookAlerts); a nil
// db makes one that does nothing.
func New(db dal.WebhookAlerts, kind string, logf func(string, ...any)) *Tracker {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Tracker{db: db, kind: kind, logf: logf, now: time.Now, eps: map[int64]*endpoint{}}
}

func (t *Tracker) ep(id int64) *endpoint {
	e := t.eps[id]
	if e == nil {
		e = &endpoint{}
		t.eps[id] = e
	}
	return e
}

// Failure records one push the endpoint did not take. gaveUp: the message was
// abandoned with it.
func (t *Tracker) Failure(ctx context.Context, endpointID, accountID int64, url, failure string, gaveUp bool) {
	if t == nil || t.db == nil || endpointID == 0 {
		return
	}
	if len(failure) > 255 {
		failure = failure[:255]
	}
	t.mu.Lock()
	e := t.ep(endpointID)
	e.failing = true
	e.failed++
	if gaveUp {
		e.gaveUp++
	}
	e.lastErr, e.url, e.account = failure, url, accountID
	w, ok := t.take(e)
	t.mu.Unlock()
	if ok {
		t.write(ctx, endpointID, w)
	}
}

// OK records that the endpoint answered: it is not failing.
func (t *Tracker) OK(ctx context.Context, endpointID int64) {
	if t == nil || t.db == nil || endpointID == 0 {
		return
	}
	now := t.now()
	t.mu.Lock()
	e := t.ep(endpointID)
	clear := e.failing || now.Sub(e.lastClear) >= ClearEvery
	if clear {
		// Whatever was counted and not written belongs to the outage that
		// just ended: it goes with the row.
		e.failing, e.failed, e.gaveUp, e.lastErr = false, 0, 0, ""
		e.lastClear = now
	}
	t.mu.Unlock()
	if !clear {
		return
	}
	if err := t.db.Clear(ctx, endpointID, t.kind); err != nil {
		t.logf("clear webhook failure endpoint=%d: %v", endpointID, err)
	}
}

// Flush writes every endpoint's counted failures, whatever their age.
func (t *Tracker) Flush(ctx context.Context) {
	if t == nil || t.db == nil {
		return
	}
	type job struct {
		id int64
		w  pending
	}
	var jobs []job
	t.mu.Lock()
	for id, e := range t.eps {
		if e.failed > 0 {
			jobs = append(jobs, job{id, t.takeNow(e)})
		}
	}
	t.mu.Unlock()
	for _, j := range jobs {
		t.write(ctx, j.id, j.w)
	}
}

type pending struct {
	failed, gaveUp int
	lastErr, url   string
	account        int64
}

// take hands back what is counted when it is time to write it.
func (t *Tracker) take(e *endpoint) (pending, bool) {
	if e.failed == 0 || t.now().Sub(e.lastWrite) < FlushEvery {
		return pending{}, false
	}
	return t.takeNow(e), true
}

func (t *Tracker) takeNow(e *endpoint) pending {
	w := pending{failed: e.failed, gaveUp: e.gaveUp, lastErr: e.lastErr, url: e.url, account: e.account}
	e.failed, e.gaveUp = 0, 0
	e.lastWrite = t.now()
	return w
}

func (t *Tracker) write(ctx context.Context, endpointID int64, w pending) {
	// One upsert of the endpoint's row, the counts added (dal.WebhookAlerts).
	err := t.db.Failed(ctx, dal.WebhookAlert{EndpointID: endpointID, AccountID: w.account, Kind: t.kind,
		URL: w.url, LastError: w.lastErr, Failed: w.failed, GaveUp: w.gaveUp})
	if err != nil {
		t.logf("record webhook failure endpoint=%d: %v", endpointID, err)
	}
}
