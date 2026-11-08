// Package vendorhealth says whether a vendor is really working, and shares
// the answer between the processes that talk to vendors and the processes
// that route messages to them (PO 2026-09-14, routing rules review).
//
// Until this existed a vendor was "down" for routing only when an operator
// paused or disabled it. The processes that could see an outage — smpp-vendor
// with its binds and error breaker, http-vendor with its failing calls — kept
// that knowledge to themselves, so messages went on queueing for a vendor
// nobody could reach.
//
// Three parts:
//
//   - Tracker, inside a vendor role: what this one process sees of each vendor
//     it serves, and (for HTTP) what it may send while a vendor is down.
//   - Publisher: writes the Tracker to vendor_health every few seconds.
//   - View, inside the routing processes: reads vendor_health and the vendors'
//     own settings, and answers "is this vendor down?" from memory.
//
// A vendor counts as down only when EVERY vendor-role instance that reported
// recently says it is failing, and has for at least the vendor's
// down_after_sec. One instance with a live bind is enough to keep it up: the
// queue is shared, and that instance can drain it.
package vendorhealth

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// Tracker is one vendor-role process's view of the vendors it serves.
type Tracker struct {
	mu  sync.Mutex
	m   map[int64]*track
	now func() time.Time
}

type track struct {
	failingSince time.Time // zero = working
	detail       string
	// Probation (HTTP): after "recheck after", a down vendor is reported as
	// working again until the first real answer. outageStart remembers when
	// the outage began, so a failed trial puts it straight back down instead
	// of waiting another "down after".
	outageStart time.Time
	lastTrial   time.Time
}

// NewTracker returns an empty tracker.
func NewTracker() *Tracker {
	return &Tracker{m: map[int64]*track{}, now: time.Now}
}

func (t *Tracker) get(id int64) *track {
	s := t.m[id]
	if s == nil {
		s = &track{}
		t.m[id] = s
	}
	return s
}

// Fail records a sign that the vendor is not working: no bound session, an
// open error breaker, a call that got no usable answer. The first sign of an
// outage starts the clock; later ones keep it. A failure while on probation
// ends the probation: the vendor is down again at once.
func (t *Tracker) Fail(id int64, detail string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.get(id)
	now := t.now()
	switch {
	case !s.outageStart.IsZero():
		s.failingSince, s.outageStart, s.lastTrial = s.outageStart, time.Time{}, now
	case s.failingSince.IsZero():
		s.failingSince = now
	}
	s.detail = detail
}

// NotReadyPrefix opens the detail of a vendor that is not READY rather than
// down: it needs a session and none could be got (its login fails, or has not
// answered yet). The vendor was never asked to send anything, so nothing says
// it is down — and the screens must not say so (Saleem, 2026-10-12: "only if
// it has session and no session yet then it can be not ready but not down").
// The View reads the prefix off vendor_health.detail; no column carries it.
const NotReadyPrefix = "no session: "

// NotReady records that the vendor cannot be sent to because it has no
// session. For routing it runs the same clock as Fail — a vendor that stays
// without a session for its down_after is skipped like one that is down, for
// nothing can be sent through it — but it is REPORTED as not ready.
func (t *Tracker) NotReady(id int64, detail string) {
	t.Fail(id, NotReadyPrefix+detail)
}

// Ready ends a not-ready state: the vendor has its session. It ends ONLY
// that. A vendor that is failing for another reason (its calls get no
// answer) stays failing: holding a session says nothing about whether its
// send call is answered.
func (t *Tracker) Ready(id int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.m[id]
	if s == nil || !strings.HasPrefix(s.detail, NotReadyPrefix) {
		return
	}
	s.failingSince, s.outageStart, s.detail = time.Time{}, time.Time{}, ""
}

// NotReadyNow reports whether the vendor is waiting for its session right
// now, for the role's own report (its status word, and how many of its
// vendors are ready).
func (t *Tracker) NotReadyNow(id int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.m[id]
	return s != nil && !s.failingSince.IsZero() && strings.HasPrefix(s.detail, NotReadyPrefix)
}

// OK records that the vendor works: a bind is up, a call was answered. It
// ends any outage and any probation.
func (t *Tracker) OK(id int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.get(id)
	s.failingSince, s.outageStart, s.detail = time.Time{}, time.Time{}, ""
}

// Set is Fail or OK for a condition checked on a timer (smpp-vendor: "is any
// session bound, and is the breaker shut?").
func (t *Tracker) Set(id int64, failing bool, detail string) {
	if failing {
		t.Fail(id, detail)
		return
	}
	t.OK(id)
}

// Down reports whether THIS process sees the vendor as down: failing for at
// least downAfter. A vendor on probation is not down.
func (t *Tracker) Down(id int64, downAfter time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.m[id]
	return s != nil && !s.failingSince.IsZero() && t.now().Sub(s.failingSince) >= downAfter
}

// Gate is what an HTTP vendor's worker may send right now.
type Gate int

const (
	// Open: send as usual. The vendor works, or has not been failing long
	// enough to count as down.
	Open Gate = iota
	// Trial: the vendor was down and is being tested — send ONE message. Its
	// answer decides: accepted ends the outage, refused puts it back down.
	Trial
	// Closed: the vendor is down; send nothing. Its waiting messages move to
	// the next route, or wait for it when there is none.
	Closed
)

// Gate decides what an HTTP vendor may send. Once the vendor has been down
// for recheck (counted from when it went down, or from its last failed
// trial), it goes on probation: reported as working so traffic can reach it,
// sending one message at a time until the first answer.
func (t *Tracker) Gate(id int64, downAfter, recheck time.Duration) Gate {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.m[id]
	if s == nil {
		return Open
	}
	if !s.outageStart.IsZero() {
		return Trial
	}
	if s.failingSince.IsZero() {
		return Open
	}
	now := t.now()
	if now.Sub(s.failingSince) < downAfter {
		return Open
	}
	from := s.failingSince.Add(downAfter)
	if s.lastTrial.After(from) {
		from = s.lastTrial
	}
	if now.Sub(from) >= recheck {
		s.outageStart, s.failingSince, s.detail = s.failingSince, time.Time{}, ""
		return Trial
	}
	return Closed
}

// Keep forgets every vendor not in ids — one that left this process (paused,
// removed, moved to another transport). Its row goes with it at the next
// publish.
func (t *Tracker) Keep(ids map[int64]bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.m {
		if !ids[id] {
			delete(t.m, id)
		}
	}
}

// Report is one vendor's state as this process sees it.
type Report struct {
	VendorID     int64
	FailingSince time.Time // zero = working
	Detail       string
}

// Snapshot returns every tracked vendor, in id order.
func (t *Tracker) Snapshot() []Report {
	t.mu.Lock()
	out := make([]Report, 0, len(t.m))
	for id, s := range t.m {
		out = append(out, Report{VendorID: id, FailingSince: s.failingSince, Detail: s.detail})
	}
	t.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].VendorID < out[j].VendorID })
	return out
}
