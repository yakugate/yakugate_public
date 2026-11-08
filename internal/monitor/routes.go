package monitor

import (
	"sort"
	"sync"
	"time"
)

// Routes counts which route the accept pipeline chose for each message, and
// when it had to fail over to get one (PO 2026-09-11: which route carried
// what, and whether a failover fired). Only the ingress roles run the route
// stage, so only they publish it.
//
// A failover is either of the two ways the stage falls past its first choice:
// every vendor in the best priority tier was down so a lower tier won, or all
// of an account's own routes were down so a global route won. Counted against
// the route that WON, since that is where the traffic went; the fallen route
// shows by carrying less.
//
// Counted at the route stage. The two stages after it (dedupe, enqueue) can
// still refuse a message, so this is "routed", a hair above "accepted".
var Routes = &routeStats{byID: map[int64]*routeTally{}}

type routeStats struct {
	mu       sync.Mutex
	byID     map[int64]*routeTally
	noMatch  Counter // no route matched the message at all
	allDown  Counter // routes matched, every one's vendor was down
	lastSeen time.Time
}

type routeTally struct {
	name, vendor string
	routed       Counter
	failovers    Counter
}

// Picked records one routed message.
func (r *routeStats) Picked(routeID int64, name, vendor string, failover bool) {
	r.mu.Lock()
	t := r.byID[routeID]
	if t == nil {
		t = &routeTally{}
		r.byID[routeID] = t
	}
	t.name, t.vendor = name, vendor // a renamed route shows its current name
	r.lastSeen = time.Now()
	r.mu.Unlock()
	t.routed.Add(1)
	if failover {
		t.failovers.Add(1)
	}
}

// NoRoute records a message refused NO_ROUTE. allDown distinguishes "every
// matching vendor is down" (an outage) from "nothing matches" (a config gap).
func (r *routeStats) NoRoute(allDown bool) {
	r.mu.Lock()
	r.lastSeen = time.Now()
	r.mu.Unlock()
	if allDown {
		r.allDown.Add(1)
	} else {
		r.noMatch.Add(1)
	}
}

// RouteRow is one route's counts, JSON-ready.
type RouteRow struct {
	RouteID   int64  `json:"route_id"`
	Name      string `json:"name,omitempty"`
	Vendor    string `json:"vendor,omitempty"`
	Routed    Snap   `json:"routed"`
	Failovers Snap   `json:"failovers"`
}

func (r *routeStats) section(uptimeSec float64) map[string]any {
	r.mu.Lock()
	if r.lastSeen.IsZero() {
		r.mu.Unlock()
		return nil
	}
	rows := make([]RouteRow, 0, len(r.byID))
	for id, t := range r.byID {
		rows = append(rows, RouteRow{RouteID: id, Name: t.name, Vendor: t.vendor})
	}
	tallies := make(map[int64]*routeTally, len(r.byID))
	for id, t := range r.byID {
		tallies[id] = t
	}
	r.mu.Unlock()
	for i := range rows {
		t := tallies[rows[i].RouteID]
		rows[i].Routed = t.routed.Snapshot(uptimeSec)
		rows[i].Failovers = t.failovers.Snapshot(uptimeSec)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Routed.Total > rows[j].Routed.Total })
	return map[string]any{
		"routes":           rows,
		"no_route":         r.noMatch.Snapshot(uptimeSec),
		"all_vendors_down": r.allDown.Snapshot(uptimeSec),
	}
}
