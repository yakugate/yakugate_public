package monitor

import "time"

// Snapshot is one role's complete picture at one instant — the unit that is
// published every few seconds, stored, and replayed by the portal.
//
// Shaped after the Hadal reference, with four deliberate departures.
//
//  1. ONE ROLE PER SNAPSHOT, not one giant document. Hadal returns server and
//     client together because it is one Node process. Yakugate is fifteen Go
//     roles that may sit on different hosts, so each publishes its own and the
//     collector assembles them. A role that dies simply stops publishing,
//     which is a clearer signal than a poller's connection error.
//
//  2. SUMMARY BEFORE DETAIL. The reference payload carries 56 near-identical
//     idle sessions for a single connector and was already 50KB truncated. At
//     4,000 msg/s across many vendors that becomes megabytes per sample. Every
//     collection here therefore has a rollup beside it, so a dashboard can
//     render from the summary and fetch detail only when someone opens a row.
//
//  3. NO INTERNAL STATE ON THE WIRE. Hadal exposes `_tps`, the private
//     rolling-window struct behind its TPS figure. Only computed values here.
//
//  4. HONEST ABSENCE. Rate carries Known, and every liveness is three-state,
//     so "quiet" and "broken" never render the same. See liveness.go.
type Snapshot struct {
	// Role is the process role: http-server, smpp-vendor, dlr, ...
	Role string `json:"role"`
	// Instance distinguishes several processes of the same role. host:pid,
	// because a role may run many instances and "which one is wedged" is a
	// question an operator has to be able to answer.
	Instance string    `json:"instance"`
	Host     string    `json:"host"`
	At       time.Time `json:"at"`
	UpSince  time.Time `json:"up_since"`
	// State is the role's own liveness, derived from its last traffic.
	State Liveness `json:"state"`

	Traffic  *Traffic         `json:"traffic,omitempty"`
	Queues   []QueueStat      `json:"queues,omitempty"`
	Sessions *SessionRollup   `json:"sessions,omitempty"`
	Accounts []AccountTraffic `json:"accounts,omitempty"`
	System   *System          `json:"system,omitempty"`
	Health   []HealthCheck    `json:"health,omitempty"`
	Extra    map[string]any   `json:"extra,omitempty"`
}

// Traffic is the message-flow summary for a role.
type Traffic struct {
	Accepted  int64 `json:"accepted"`
	Rejected  int64 `json:"rejected"`
	Delivered int64 `json:"delivered"`
	Failed    int64 `json:"failed"`
	Segments  int64 `json:"segments"`
	// TPS is measured over the recent window, not since start.
	TPS float64 `json:"tps"`
	// LatencyP50/P95 in milliseconds. p95 rather than an average because an
	// average hides exactly the tail that customers complain about.
	LatencyP50Ms float64 `json:"latency_p50_ms"`
	LatencyP95Ms float64 `json:"latency_p95_ms"`
	// RejectsByReason answers "is a client misbehaving or did we break
	// something" without opening the logs.
	RejectsByReason map[string]int64 `json:"rejects_by_reason,omitempty"`
	LastActivity    time.Time        `json:"last_activity"`
}

// QueueStat is one vendor queue's depth and age.
type QueueStat struct {
	Vendor string `json:"vendor"`
	Depth  int64  `json:"depth"`
	// OldestAgeSec is the more important number of the two. Depth alone cannot
	// tell a busy queue from a stuck one: 10,000 messages draining fast is
	// healthy, 12 messages that have sat for an hour is an outage.
	OldestAgeSec int64 `json:"oldest_age_sec"`
}

// SessionRollup summarises a role's connections, with the individual sessions
// alongside for drill-down.
type SessionRollup struct {
	Total      int `json:"total"`
	Up         int `json:"up"`
	Down       int `json:"down"`
	Connecting int `json:"connecting"`
	// Vendors groups sessions by the thing an operator actually thinks in —
	// "is Jazz up", not "is session 37 up".
	Vendors []VendorSessions `json:"vendors,omitempty"`
}

// VendorSessions is one vendor's or account's connections.
type VendorSessions struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"` // SMPP | HTTP | SOAP
	Up      int      `json:"up"`
	Total   int      `json:"total"`
	State   Liveness `json:"state"`
	Success Rate     `json:"success"`
	TPS     float64  `json:"tps"`
	// UnderStress is Hadal's isUnderStress: the window is full or the endpoint
	// is pushing back, so this vendor is the constraint right now.
	UnderStress bool      `json:"under_stress"`
	Sessions    []Session `json:"sessions,omitempty"`
}

// Session is one bind or connector.
type Session struct {
	Index    int    `json:"index"`
	Type     string `json:"type"`
	Host     string `json:"host"`
	Status   string `json:"status"` // CONNECTED | FAILED | CONNECTING | READY | DISCONNECTED
	BindMode string `json:"bind_mode,omitempty"`
	// WindowSize/Depth is the SMPP in-flight window: depth approaching size is
	// the earliest warning that a vendor has stopped acknowledging.
	WindowSize   int   `json:"window_size,omitempty"`
	WindowDepth  int   `json:"window_depth,omitempty"`
	Accepted     int64 `json:"accepted"`
	Failed       int64 `json:"failed"`
	Success      Rate  `json:"success"`
	LatencyAvgMs int64 `json:"latency_avg_ms"`
	LatencyMaxMs int64 `json:"latency_max_ms"`
	// Reconnections and Timeouts are churn. A bind that is "up" having
	// reconnected 400 times is not up in any sense that matters.
	Reconnections int64 `json:"reconnections"`
	Timeouts      int64 `json:"timeouts"`
	Errors        int64 `json:"errors"`
	UptimeSec     int64 `json:"uptime_sec"`
	DowntimeSec   int64 `json:"downtime_sec"`
	// Encodings splits GSM-7 from UCS-2: a sudden swing means either a new
	// campaign or a content problem, and it drives segment cost.
	Encodings        map[string]int64 `json:"encodings,omitempty"`
	LastStatusChange time.Time        `json:"last_status_change"`
	LastActivity     time.Time        `json:"last_activity"`
	State            Liveness         `json:"state"`
}

// AccountTraffic is per-tenant flow, so "the gateway is slow" can be answered
// with "for whom".
type AccountTraffic struct {
	Account  string    `json:"account"`
	Accepted int64     `json:"accepted"`
	Rejected int64     `json:"rejected"`
	Segments int64     `json:"segments"`
	TPS      float64   `json:"tps"`
	LastSeen time.Time `json:"last_seen"`
	State    Liveness  `json:"state"`
}

// HealthCheck is one dependency this role needs, and whether it has it.
//
// Separate from the counters on purpose: a role can be up, publishing happily,
// and useless because its database is unreachable. That is invisible in
// traffic numbers — the traffic simply stops — and it is the first thing
// anyone should see.
type HealthCheck struct {
	Name      string `json:"name"` // config-store, queue-store, hot-store, redis
	OK        bool   `json:"ok"`
	Detail    string `json:"detail,omitempty"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
}
