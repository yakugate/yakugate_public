// Package monitor holds in-memory traffic counters for the /status endpoint —
// the Yakugate equivalent of Kannel's bearerbox status page, JSON instead of
// text. A Counter is a 300-second ring buffer: cheap Add on the hot path,
// Kannel-style rate triplets (last 60s, last 300s, lifetime average) plus the
// lifetime 1-second peak on read. Single-process scope by design: each role
// reports its own numbers; cluster aggregation is the Admin API's job (P12).
package monitor

import (
	"strconv"
	"sync"
	"time"
)

// window is the ring size in seconds — rates are computable over any span up
// to this. 300s matches Kannel's longest displayed average.
const window = 300

// Counter counts events per second over a sliding 300s window plus lifetime.
type Counter struct {
	mu      sync.Mutex
	total   int64
	peak1s  int64
	sec     int64 // unix second buckets[head] belongs to
	head    int
	buckets [window]int64
}

// advance rotates the ring forward to `now`, zeroing skipped seconds.
// Caller holds mu.
func (c *Counter) advance(now int64) {
	if c.sec == 0 {
		c.sec = now
		return
	}
	gap := now - c.sec
	if gap <= 0 {
		return
	}
	if gap >= window {
		c.buckets = [window]int64{}
		c.sec = now
		return
	}
	for i := int64(0); i < gap; i++ {
		c.head = (c.head + 1) % window
		c.buckets[c.head] = 0
	}
	c.sec = now
}

// Add records n events now.
func (c *Counter) Add(n int64) {
	now := time.Now().Unix()
	c.mu.Lock()
	c.advance(now)
	c.buckets[c.head] += n
	c.total += n
	if c.buckets[c.head] > c.peak1s {
		c.peak1s = c.buckets[c.head]
	}
	c.mu.Unlock()
}

// Snap is one counter's JSON-ready view.
type Snap struct {
	Total    int64   `json:"total"`
	TPS60    float64 `json:"tps_60s"`
	TPS300   float64 `json:"tps_300s"`
	Peak1s   int64   `json:"peak_1s"`
	Lifetime float64 `json:"tps_lifetime"`
}

// Snapshot computes rates as of now; uptimeSec scales the lifetime average.
func (c *Counter) Snapshot(uptimeSec float64) Snap {
	now := time.Now().Unix()
	c.mu.Lock()
	c.advance(now)
	var s60, s300 int64
	for i := 0; i < window; i++ {
		v := c.buckets[(c.head-i+window)%window]
		if i < 60 {
			s60 += v
		}
		s300 += v
	}
	snap := Snap{Total: c.total, Peak1s: c.peak1s,
		TPS60: float64(s60) / 60, TPS300: float64(s300) / 300}
	c.mu.Unlock()
	if uptimeSec > 0 {
		snap.Lifetime = float64(snap.Total) / uptimeSec
	}
	return snap
}

// AccountStats is one account's live counters.
type AccountStats struct {
	Accepted Counter
	Rejected Counter
	Segments Counter
	mu       sync.Mutex
	lastSeen time.Time
	// byStatus counts responses by the HTTP status this account received, and
	// byIngress by the transport it arrived on. Hadal's `users` block carries
	// the same idea and it earns its place: "account X is being refused" is a
	// different conversation from "account X is quiet", and the status
	// breakdown is what tells them apart without opening the reject log.
	byStatus  map[int]int64
	byIngress map[string]int64
	// bySMPP counts submit_sm answers by SMPP status name (2026-10-06): an
	// SMPP client has no HTTP status, so its refusals — "account throttled"
	// is ESME_RTHROTTLED — showed nowhere on the account's line.
	bySMPP map[string]int64
	// accept is this account's own accept-path time over the last hour
	// (milliseconds), under mu. The gateway-wide p95 cannot say WHICH
	// customer is being served slowly (PO 2026-09-11).
	accept *HourHist
}

// Stats is the per-process registry the /status endpoint reads.
type Stats struct {
	start    time.Time
	Accepted Counter // messages accepted (durably enqueued or idempotent replay)
	Rejected Counter // messages rejected by the pipeline (per recipient)
	Segments Counter // segments of accepted messages (the rate-limit currency)
	AuthFail Counter // request-level authentication failures
	Pipeline Latency // per-recipient pipeline durations, for the /metrics p95 gauge
	mu       sync.RWMutex
	accounts map[string]*AccountStats
	// rejectMu/rejectByReason back RejectReason/RejectBreakdown — "tell a
	// client is misbehaving from we broke something" via a per-reason-code
	// counter, same 300s-window Counter every other rate uses. [SCOPE
	// P1-STATUS, PO 2026-07-27]
	rejectMu       sync.RWMutex
	rejectByReason map[string]*Counter
}

// NoteAccountResult records one answered request for an account: the HTTP
// status it received and the transport it came in on.
//
// Separate from Accepted/Rejected because those count MESSAGES (a fan-out to
// 100 recipients is 100) while this counts REQUESTS, which is what an HTTP
// status describes. Conflating them makes the status breakdown lie by a factor
// of the recipient count.
func (s *Stats) NoteAccountResult(username string, status int, ingress string) {
	if username == "" {
		return
	}
	a := s.account(username)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.byStatus == nil {
		a.byStatus = map[int]int64{}
	}
	if a.byIngress == nil { // its own check: an SMPP answer may have made it first
		a.byIngress = map[string]int64{}
	}
	a.byStatus[status]++
	if ingress != "" {
		a.byIngress[ingress]++
	}
	a.lastSeen = time.Now()
}

// NoteAccountSMPP records one submit_sm answered for an account over SMPP:
// the status it was given, by name ("ESME_ROK", "ESME_RTHROTTLED"; hex for a
// code with no name). The SMPP twin of NoteAccountResult — it counts PDUs
// answered, and marks the account as arriving over SMPP.
func (s *Stats) NoteAccountSMPP(username, status string) {
	if username == "" {
		return
	}
	a := s.account(username)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.byIngress == nil {
		a.byIngress = map[string]int64{}
	}
	if a.bySMPP == nil {
		a.bySMPP = map[string]int64{}
	}
	a.bySMPP[status]++
	a.byIngress["smpp"]++
	a.lastSeen = time.Now()
}

// LastActivity is the most recent moment ANY account on this role saw
// traffic. Feeds the three-state liveness on the snapshot: it is the
// difference between "quiet at 3am" and "stopped working at 3am".
func (s *Stats) LastActivity() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var newest time.Time
	for _, a := range s.accounts {
		a.mu.Lock()
		if a.lastSeen.After(newest) {
			newest = a.lastSeen
		}
		a.mu.Unlock()
	}
	return newest
}

func New() *Stats {
	return &Stats{start: time.Now(), accounts: map[string]*AccountStats{}, rejectByReason: map[string]*Counter{}}
}

// RejectReason records one rejection under its reason code (e.g.
// "INVALID_MSISDN", stripped of any ": detail" suffix by the caller).
func (s *Stats) RejectReason(reason string) {
	if reason == "" {
		return
	}
	s.rejectMu.RLock()
	c := s.rejectByReason[reason]
	s.rejectMu.RUnlock()
	if c == nil {
		s.rejectMu.Lock()
		if c = s.rejectByReason[reason]; c == nil {
			c = &Counter{}
			s.rejectByReason[reason] = c
		}
		s.rejectMu.Unlock()
	}
	c.Add(1)
}

// RejectBreakdown snapshots every reason seen since process start, sorted by
// reason code for stable output — "reject breakdown by reason" for /status.
func (s *Stats) RejectBreakdown(uptimeSec float64) map[string]Snap {
	s.rejectMu.RLock()
	defer s.rejectMu.RUnlock()
	out := make(map[string]Snap, len(s.rejectByReason))
	for reason, c := range s.rejectByReason {
		out[reason] = c.Snapshot(uptimeSec)
	}
	return out
}

func (s *Stats) Start() time.Time { return s.start }

func (s *Stats) account(user string) *AccountStats {
	s.mu.RLock()
	a := s.accounts[user]
	s.mu.RUnlock()
	if a != nil {
		return a
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if a = s.accounts[user]; a == nil {
		a = &AccountStats{}
		s.accounts[user] = a
	}
	return a
}

// AcceptLatency records one message's accept-path time for an account.
func (s *Stats) AcceptLatency(user string, d time.Duration) {
	if user == "" {
		return
	}
	a := s.account(user)
	a.mu.Lock()
	if a.accept == nil {
		a.accept = NewHourHist(AcceptBoundsMs)
	}
	a.accept.Add(float64(d)/float64(time.Millisecond), time.Now().Unix())
	a.mu.Unlock()
}

// Message records one recipient-level outcome for an account.
func (s *Stats) Message(user string, segments int, accepted bool) {
	a := s.account(user)
	a.mu.Lock()
	a.lastSeen = time.Now()
	a.mu.Unlock()
	if accepted {
		s.Accepted.Add(1)
		a.Accepted.Add(1)
		if segments > 0 {
			s.Segments.Add(int64(segments))
			a.Segments.Add(int64(segments))
		}
	} else {
		s.Rejected.Add(1)
		a.Rejected.Add(1)
	}
}

// AccountSnap is one account's JSON-ready view.
type AccountSnap struct {
	Username     string `json:"username"`
	LastActivity string `json:"last_activity"`
	Accepted     Snap   `json:"accepted"`
	Rejected     Snap   `json:"rejected"`
	Segments     Snap   `json:"segments"`
	// ByStatus is requests answered, keyed by HTTP status as a STRING so the
	// JSON object reads "200"/"429" rather than being re-keyed by a client.
	// Counts REQUESTS, not messages — see NoteAccountResult.
	ByStatus map[string]int64 `json:"by_status,omitempty"`
	// ByIngress splits http from smpp for accounts that use both.
	ByIngress map[string]int64 `json:"by_ingress,omitempty"`
	// BySMPP is submit_sm answers by SMPP status name (see NoteAccountSMPP).
	BySMPP map[string]int64 `json:"by_smpp_status,omitempty"`
	// State is three-state liveness for this ACCOUNT. A customer who has sent
	// nothing for an hour is the question support is about to be asked.
	State string `json:"state"`
	// Accept-path time over the last hour: bucket counts (bounds in
	// accept_latency_bounds_ms, beside "accounts") so instances can be summed,
	// plus this instance's own p50/p95 read off them.
	AcceptCounts []int64  `json:"accept_latency_counts,omitempty"`
	AcceptP50Ms  *float64 `json:"accept_p50_ms,omitempty"`
	AcceptP95Ms  *float64 `json:"accept_p95_ms,omitempty"`
}

// Accounts snapshots every account seen since process start, sorted by
// username for stable output.
func (s *Stats) Accounts(uptimeSec float64) []AccountSnap {
	s.mu.RLock()
	names := make([]string, 0, len(s.accounts))
	for u := range s.accounts {
		names = append(names, u)
	}
	s.mu.RUnlock()
	// insertion sort — account lists are small
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	out := make([]AccountSnap, 0, len(names))
	for _, u := range names {
		s.mu.RLock()
		a := s.accounts[u]
		s.mu.RUnlock()
		a.mu.Lock()
		last := a.lastSeen
		byStatus := make(map[string]int64, len(a.byStatus))
		for code, n := range a.byStatus {
			byStatus[strconv.Itoa(code)] = n
		}
		byIngress := make(map[string]int64, len(a.byIngress))
		for k, n := range a.byIngress {
			byIngress[k] = n
		}
		var bySMPP map[string]int64
		if len(a.bySMPP) > 0 {
			bySMPP = make(map[string]int64, len(a.bySMPP))
			for k, n := range a.bySMPP {
				bySMPP[k] = n
			}
		}
		var acceptCounts []int64
		if a.accept != nil {
			acceptCounts = a.accept.Counts(time.Now().Unix())
		}
		a.mu.Unlock()
		out = append(out, AccountSnap{
			Username:     u,
			LastActivity: last.UTC().Format(time.RFC3339),
			Accepted:     a.Accepted.Snapshot(uptimeSec),
			Rejected:     a.Rejected.Snapshot(uptimeSec),
			Segments:     a.Segments.Snapshot(uptimeSec),
			ByStatus:     byStatus,
			ByIngress:    byIngress,
			BySMPP:       bySMPP,
			State:        string(State(last, time.Now())),
		})
		if p50, _, ok := HistQuantile(AcceptBoundsMs, acceptCounts, 0.50); ok {
			p95, _, _ := HistQuantile(AcceptBoundsMs, acceptCounts, 0.95)
			snap := &out[len(out)-1]
			snap.AcceptCounts, snap.AcceptP50Ms, snap.AcceptP95Ms = acceptCounts, &p50, &p95
		}
	}
	return out
}
