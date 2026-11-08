package vendorhealth

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
)

const (
	// LoadEvery is how often a routing process re-reads vendor health.
	LoadEvery = 3 * time.Second
	// staleAfter: a report older than this is from a process that stopped
	// reporting, and says nothing about the vendor today.
	staleAfter = 4 * ReportEvery
)

// View is a routing process's in-memory answer to "is this vendor down?",
// refreshed from vendor_health every LoadEvery. Reading it costs a map lookup.
type View struct {
	db  dal.VendorHealth
	now func() time.Time

	mu      sync.RWMutex
	down    map[int64]string // vendor id -> why (health only)
	vendors []Vendor         // every vendor whose queue should move now
	states  []State          // every vendor, as the screens show it
}

// State is one vendor as routing sees it right now — what the Routing Rules
// screen shows beside each vendor (2026-09-15), so the screen and the engine
// cannot disagree about which vendor is down.
type State struct {
	ID  int64  `json:"vendorId"`
	CID string `json:"cid"`
	// "down" is said of an SMPP vendor only. An HTTP vendor has no connection
	// to lose, so it is never down (Saleem, 2026-10-12: "there is no http
	// vendor down"): one whose calls get no answer is "failing", and one
	// without its session "not_ready" — for as long as that lasts.
	State string `json:"state"` // up | failing | down | not_ready | paused | disabled | no_report
	// Why, for operators: the failure detail, how long it has lasted, and for
	// "failing" how long until it counts as down.
	Why string `json:"why,omitempty"`
	// Routed is whether routing sends it traffic now. Skipped: down, paused,
	// disabled, and a vendor that has been failing or not ready for its "down
	// after" time. One still inside that time, or with no report (no running
	// process serves it), keeps getting traffic.
	Routed bool `json:"routed"`
}

// Vendor is a vendor whose waiting messages should move to another route.
type Vendor struct {
	ID  int64
	CID string // the queue's name
	Why string // for operators: "no working connection for 45s: ..." / "paused by an operator"
	// Switch is true when the reason is the operator's paused/disabled
	// switch rather than health. The view reads the switch every few seconds,
	// so right after a vendor is switched ON it can still say paused; the
	// mover checks the switch again before it acts on this one.
	Switch bool
}

// NewView reads from the config store (dal.VendorHealth). db nil = nothing is
// ever down, which is the behaviour before this existed.
func NewView(db dal.VendorHealth) *View {
	return &View{db: db, now: time.Now, down: map[int64]string{}}
}

// Run loads now and every LoadEvery until ctx ends. A failed load keeps the
// last answer: a database blip must not make every vendor look up, or down.
func (v *View) Run(ctx context.Context, logf func(string, ...any)) {
	if v == nil || v.db == nil {
		return
	}
	var lastErr time.Time
	tick := time.NewTicker(LoadEvery)
	defer tick.Stop()
	for {
		c, cancel := context.WithTimeout(ctx, LoadEvery)
		err := v.Load(c)
		cancel()
		if err != nil && ctx.Err() == nil && logf != nil && time.Since(lastErr) >= time.Minute {
			lastErr = time.Now()
			logf("vendor health read failed (keeping the last view): %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// row is one vendor joined with one of its reports (report fields NULL when
// no process has reported on it).
type row struct {
	id           int64
	cid, status  string
	transport    string // smpp | http | soap | blackhole
	deployment   string // the reporting instance's deployment, "" = it did not say
	downAfter    time.Duration
	failingSince dal.NullTime
	reportedAt   dal.NullTime
	detail       string
}

// Load reads every vendor and its reports once.
func (v *View) Load(ctx context.Context) error {
	got, err := v.db.Rows(ctx)
	if err != nil {
		return err
	}
	rows := make([]row, 0, len(got))
	for _, g := range got {
		rows = append(rows, row{id: g.ID, cid: g.CID, status: g.Status, transport: g.Transport,
			deployment: deploymentOf(g.Instance), downAfter: time.Duration(g.DownAfterSec) * time.Second,
			failingSince: g.FailingSince, reportedAt: g.ReportedAt, detail: g.Detail})
	}
	down, movable, states := assessFor(rows, v.now(), deployment.ID())
	v.mu.Lock()
	v.down, v.vendors, v.states = down, movable, states
	v.mu.Unlock()
	return nil
}

// decide turns vendor rows into the down set and the list of queues to move.
//
// Health-down: at least one fresh report, every fresh report failing, and the
// most recent of those failures began at least down_after ago — that is, every
// instance has been failing for the whole down_after. No fresh report at all
// means no news, and no news is up: a vendor served by no running process is
// not something routing can learn anything about.
//
// Paused and disabled vendors are not health-down (routing already skips them
// by their status) but their queues move too: "down" includes the operator's
// switch (PO 2026-09-14).
func decide(rows []row, now time.Time) (map[int64]string, []Vendor) {
	down, movable, _ := assess(rows, now)
	return down, movable
}

// assess is decide plus every vendor's State, from the same arithmetic.
func assess(rows []row, now time.Time) (map[int64]string, []Vendor, []State) {
	return assessFor(rows, now, "")
}

// assessFor is assess as the deployment `own` must see it.
//
// A vendor counted as down only when EVERY vendor-role instance that reported
// said so — one instance with a live bind was enough, "the queue is shared,
// and that instance can drain it". That is true inside one deployment and not
// across two: a message accepted on a deployment waits in THAT deployment's
// queue, and only its own vendor roles send it. With the vendor's SMSC
// unreachable from one deployment and bound from the other, the first went on
// routing to it — its messages could not be sent and failed there, while the
// rule's backup was never used (go-live test P3 E6, 2026-10-12, Saleem: "on
// deployment 1 it fails messages, as both are independent").
//
// So when this deployment's own vendor roles report on a vendor, only their
// reports are read. A vendor none of them reports on (it is placed on another
// deployment, or this gateway names none) is judged by every report, as
// before.
func assessFor(rows []row, now time.Time, own string) (map[int64]string, []Vendor, []State) {
	if own != "" {
		mine := map[int64]bool{}
		for _, r := range rows {
			if r.deployment == own && r.reportedAt.Valid && now.Sub(r.reportedAt.Time) <= staleAfter {
				mine[r.id] = true
			}
		}
		if len(mine) > 0 {
			kept := make([]row, 0, len(rows))
			for _, r := range rows {
				// Another deployment's report on a vendor we report on
				// ourselves says nothing about what WE can send: the vendor's
				// own row is kept (its switch, its settings), its report is not.
				if mine[r.id] && r.deployment != own && r.reportedAt.Valid {
					continue
				}
				kept = append(kept, r)
			}
			rows = kept
		}
	}
	type agg struct {
		cid, status   string
		http          bool
		downAfter     time.Duration
		fresh         int
		failing       int
		latestFailing time.Time
		detail        string
	}
	by := map[int64]*agg{}
	var order []int64
	for _, r := range rows {
		a := by[r.id]
		if a == nil {
			a = &agg{cid: r.cid, status: r.status, downAfter: r.downAfter, http: overHTTP(r.transport)}
			by[r.id] = a
			order = append(order, r.id)
		}
		if !r.reportedAt.Valid || now.Sub(r.reportedAt.Time) > staleAfter {
			continue
		}
		a.fresh++
		if r.failingSince.Valid {
			a.failing++
			if r.failingSince.Time.After(a.latestFailing) {
				a.latestFailing = r.failingSince.Time
			}
			if a.detail == "" {
				a.detail = r.detail
			}
		}
	}
	down := map[int64]string{}
	var movable []Vendor
	states := make([]State, 0, len(order))
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	for _, id := range order {
		a := by[id]
		switch a.status {
		case "paused":
			movable = append(movable, Vendor{ID: id, CID: a.cid, Why: "paused by an operator", Switch: true})
			states = append(states, State{ID: id, CID: a.cid, State: "paused", Why: "paused by an operator"})
			continue
		case "disabled":
			movable = append(movable, Vendor{ID: id, CID: a.cid, Why: "disabled", Switch: true})
			states = append(states, State{ID: id, CID: a.cid, State: "disabled", Why: "disabled"})
			continue
		}
		if a.fresh == 0 {
			states = append(states, State{ID: id, CID: a.cid, State: "no_report", Routed: true,
				Why: "no health report: nothing sent through it lately, or no vendor process serves it; routing treats it as up"})
			continue
		}
		if a.failing < a.fresh {
			states = append(states, State{ID: id, CID: a.cid, State: "up", Routed: true})
			continue
		}
		failingFor := now.Sub(a.latestFailing)
		// Not ready (2026-10-12): the vendor needs a session and has none. It
		// was never asked to send, so it is not "down" and is never called
		// that — but nothing can go through it, so once it has been without
		// a session for its down_after, routing skips it and its waiting
		// messages move, exactly as for a vendor that is down.
		if strings.HasPrefix(a.detail, NotReadyPrefix) {
			why := fmt.Sprintf("not ready for %s: %s", failingFor.Truncate(time.Second), a.detail)
			if failingFor < a.downAfter {
				states = append(states, State{ID: id, CID: a.cid, State: "not_ready", Routed: true,
					Why: fmt.Sprintf("%s; routing skips it after %s", why, a.downAfter)})
				continue
			}
			down[id] = why
			movable = append(movable, Vendor{ID: id, CID: a.cid, Why: why})
			states = append(states, State{ID: id, CID: a.cid, State: "not_ready", Why: why})
			continue
		}
		// An HTTP vendor has no connection, so it cannot be down. Its calls
		// are getting no answer: it is FAILING, for as long as that lasts.
		// Once that has lasted its "down after" time routing uses the next
		// vendor and its waiting messages move (PO 2026-09-14) — the vendor
		// is skipped, and is still not called down.
		if a.http {
			why := fmt.Sprintf("its calls have had no answer for %s", failingFor.Truncate(time.Second))
			if a.detail != "" {
				why += ": " + a.detail
			}
			if failingFor < a.downAfter {
				states = append(states, State{ID: id, CID: a.cid, State: "failing", Routed: true,
					Why: fmt.Sprintf("%s; routing uses the next vendor after %s", why, a.downAfter)})
				continue
			}
			down[id] = why
			movable = append(movable, Vendor{ID: id, CID: a.cid, Why: why})
			states = append(states, State{ID: id, CID: a.cid, State: "failing", Why: why})
			continue
		}
		why := fmt.Sprintf("no working connection for %s", failingFor.Truncate(time.Second))
		if a.detail != "" {
			why += ": " + a.detail
		}
		if failingFor < a.downAfter {
			states = append(states, State{ID: id, CID: a.cid, State: "failing", Routed: true,
				Why: fmt.Sprintf("%s; counts as down after %s", why, a.downAfter)})
			continue
		}
		down[id] = why
		movable = append(movable, Vendor{ID: id, CID: a.cid, Why: why})
		states = append(states, State{ID: id, CID: a.cid, State: "down", Why: why})
	}
	return down, movable, states
}

// overHTTP reports whether a vendor is reached by calls (HTTP, SOAP) and so
// has no connection that could be down.
func overHTTP(transport string) bool {
	switch strings.ToLower(strings.TrimSpace(transport)) {
	case "http", "soap":
		return true
	}
	return false
}

// States is every vendor's current state, as of the last load.
func (v *View) States() []State {
	if v == nil {
		return nil
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return append([]State(nil), v.states...)
}

// Down reports whether a vendor has had no working connection for its
// down_after_sec, with the reason. Paused/disabled vendors are NOT reported
// here — routing reads their status from the vendor row itself.
func (v *View) Down(vendorID int64) (bool, string) {
	if v == nil {
		return false, ""
	}
	v.mu.RLock()
	why, ok := v.down[vendorID]
	v.mu.RUnlock()
	return ok, why
}

// Movable lists every vendor whose waiting messages should move now: down by
// health, paused or disabled.
func (v *View) Movable() []Vendor {
	if v == nil {
		return nil
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return append([]Vendor(nil), v.vendors...)
}

// NewFixedView returns a view that never loads: down and movable are what it
// answers. For tests, and for tools that decide health themselves.
func NewFixedView(down map[int64]string, movable []Vendor) *View {
	if down == nil {
		down = map[int64]string{}
	}
	return &View{now: time.Now, down: down, vendors: movable}
}
