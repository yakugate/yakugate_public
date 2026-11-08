package smppvendor

import (
	"context"

	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
	"time"

	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/lib/smpp/window"
)

// vendorState is one SESSION's live status, guarded by Process.mu (2026-07-29:
// a vendor can run more than one concurrent bind, so state is keyed per
// sessionKey, not per vendor id — see definition.go). It backs the
// throttle-pause/circuit-breaker logic and the /status monitor surface.
// [SCOPE P3-STATUS, P3-HADAL richer stats]
type vendorState struct {
	cid            string
	addr           string
	bound          bool
	boundSince     time.Time
	lastChangeAt   time.Time // when `bound` last flipped either direction
	lastError      string
	win            *window.Window[model.Message] // non-nil only while a bind is live
	throttledUntil time.Time                     // session paused until this instant (throttle backoff)
	throttlePause  time.Duration                 // this vendor's configured throttle_pause_sec

	submitted int64 // PDUs/messages put on the wire
	accepted  int64 // submit_sm_resp classified "success"
	failed    int64 // submit_sm_resp classified "permanent"
	throttled int64 // submit_sm_resp classified "retry" (incl. ESME_RTHROTTLED)

	latencySumMs int64 // accepted-only submit->ack latency, for the average
	latencyMaxMs int64
	latencyCount int64

	encodingCounts map[string]int64 // gsm7/ucs2/latin1 -> submitted count

	cumUpMs   int64 // accumulated bound time, excluding the still-open interval
	cumDownMs int64 // accumulated unbound time, excluding the still-open interval

	// Churn counters (Hadal parity, 2026-09-10). A session reported "bound"
	// having reconnected 400 times is not up in any sense an operator cares
	// about, and neither counter is derivable from the point-in-time state —
	// they have to be accumulated as it happens.
	reconnections int64
	timeouts      int64
	// lastActivity is the last submit on this session, for three-state
	// liveness. Distinct from lastChangeAt, which tracks the BIND flipping: a
	// session can be bound for hours and carrying nothing, and those are
	// different situations.
	lastActivity time.Time

	// Inbound on this session (2026-09-11). Receipts and MO arrive on the same
	// bind the submits leave on, so "which vendor stopped sending receipts" is
	// a per-session question — a vendor that accepts everything and confirms
	// nothing looks perfect on every other counter here.
	receipts int64
	moIn     int64
	// respCodes counts every submit_sm_resp command_status, lead PDU or not
	// (Hadal's onSubmitStatuses). The accepted/failed/throttled split above is
	// OUR classification; this is what the vendor actually said, which is what
	// has to be quoted back to them in a ticket.
	respCodes map[uint32]int64

	tpsBuckets   [60]int64 // per-second submit counts, ring-indexed by unix-second%60
	tpsBucketSec [60]int64 // which unix-second each bucket currently holds
}

// --- state mutators (all take Process.mu) ---

func (p *Process) ensureState(key sessionKey, cid, addr string, throttlePause time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.state[key]
	if s == nil {
		// The host is set here once: the session says which host it is on
		// from then (setAddr) — it moves to the vendor's next host after an
		// attempt that failed, and this is called again at every reload.
		s = &vendorState{lastChangeAt: time.Now(), encodingCounts: map[string]int64{}, addr: addr}
		p.state[key] = s
	}
	s.cid, s.throttlePause = cid, throttlePause
}

// setAddr records the host this session is trying or bound to: a vendor with
// several hosts moves to the next after an attempt that failed.
func (p *Process) setAddr(key sessionKey, addr string) {
	p.mu.Lock()
	if s := p.state[key]; s != nil {
		s.addr = addr
	}
	p.mu.Unlock()
}

func (p *Process) setBound(key sessionKey, win *window.Window[model.Message]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.state[key]
	if s == nil {
		return
	}
	now := time.Now()
	if !s.bound {
		s.cumDownMs += now.Sub(s.lastChangeAt).Milliseconds()
		// Counted on the DOWN->UP transition, and only when this session has
		// been up before (lastChangeAt set by a previous bind). The first bind
		// after start is not a reconnection, and counting it as one would make
		// every healthy restart look like churn.
		if !s.lastChangeAt.IsZero() && !s.boundSince.IsZero() {
			s.reconnections++
		}
	}
	s.bound, s.win, s.boundSince, s.lastError = true, win, now, ""
	s.lastChangeAt = now
}

func (p *Process) setUnbound(key sessionKey, err string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.state[key]
	if s == nil {
		return
	}
	now := time.Now()
	if s.bound {
		s.cumUpMs += now.Sub(s.lastChangeAt).Milliseconds()
	}
	s.bound, s.win = false, nil
	s.lastChangeAt = now
	if err != "" {
		s.lastError = err
	}
}

// markThrottled pauses this session for its configured throttle_pause_sec
// (default 1s when unset) and counts it.
func (p *Process) markThrottled(key sessionKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s := p.state[key]; s != nil {
		tp := s.throttlePause
		if tp <= 0 {
			tp = time.Second
		}
		s.throttledUntil = time.Now().Add(tp)
		s.throttled++
	}
}

// paused reports whether this session is currently in throttle backoff.
func (p *Process) paused(key sessionKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.state[key]
	return s != nil && time.Now().Before(s.throttledUntil)
}

func (p *Process) countSubmitted(key sessionKey, n int64) {
	p.bump(key, func(s *vendorState) {
		s.submitted += n
		s.lastActivity = time.Now()
		sec := time.Now().Unix()
		idx := sec % 60
		if s.tpsBucketSec[idx] != sec {
			s.tpsBuckets[idx] = 0
			s.tpsBucketSec[idx] = sec
		}
		s.tpsBuckets[idx] += n
	})
}

func (p *Process) countAccepted(key sessionKey, latencyMs int64) {
	monitor.Flow.NoteOut(1)
	monitor.Flow.NoteVendorLatency(time.Duration(latencyMs) * time.Millisecond)
	p.bump(key, func(s *vendorState) {
		s.accepted++
		s.latencySumMs += latencyMs
		s.latencyCount++
		if latencyMs > s.latencyMaxMs {
			s.latencyMaxMs = latencyMs
		}
	})
}

func (p *Process) countFailed(key sessionKey) {
	monitor.Flow.NoteOutFailed(1)
	p.bump(key, func(s *vendorState) { s.failed++ })
}

// countTimeout records a submit whose response never arrived. Kept apart from
// countFailed because they mean different things to an operator: a failure is
// the vendor answering "no", a timeout is the vendor not answering at all, and
// the second is the shape of a vendor about to stop working entirely.
func (p *Process) countTimeout(key sessionKey) {
	monitor.Flow.NoteOutFailed(1)
	p.bump(key, func(s *vendorState) { s.timeouts++ })
}

// countResp records the vendor's own answer to one submit_sm.
func (p *Process) countResp(key sessionKey, status uint32) {
	p.bump(key, func(s *vendorState) {
		if s.respCodes == nil {
			s.respCodes = map[uint32]int64{}
		}
		s.respCodes[status]++
	})
}

// countReceipt / countMO record inbound traffic on a session.
func (p *Process) countReceipt(key sessionKey) {
	monitor.Flow.NoteReceipt(1)
	p.bump(key, func(s *vendorState) { s.receipts++ })
}

func (p *Process) countMO(key sessionKey) {
	monitor.Flow.NoteMO(1)
	p.bump(key, func(s *vendorState) { s.moIn++ })
}

// respCodeNames renders the response-code counts under their spec names
// ("ESME_ROK", "ESME_RTHROTTLED"), hex for anything we have no name for.
func respCodeNames(m map[uint32]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for code, n := range m {
		out[pdu.StatusName(code)] = n
	}
	return out
}

// copyCounts hands a status report its own copy of a session's counts. The
// report is turned into JSON after p.mu is let go, while submits go on adding
// to the session's map: the live map in the report crashed the role under
// load (2026-10-14, "fatal error: concurrent map iteration and map write" in
// monitor.StartPush, a Go fatal error that no recover() can catch).
func copyCounts(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, n := range m {
		out[k] = n
	}
	return out
}

func (p *Process) countEncoding(key sessionKey, enc string) {
	if enc == "" {
		enc = "gsm7"
	}
	p.bump(key, func(s *vendorState) {
		if s.encodingCounts == nil {
			s.encodingCounts = map[string]int64{}
		}
		s.encodingCounts[enc]++
	})
}

func (p *Process) bump(key sessionKey, f func(*vendorState)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s := p.state[key]; s != nil {
		f(s)
	}
}

// upDownMs reports cumulative bound/unbound time including the still-open
// current interval, as of now.
func (s *vendorState) upDownMs(now time.Time) (upMs, downMs int64) {
	upMs, downMs = s.cumUpMs, s.cumDownMs
	if s.bound {
		upMs += now.Sub(s.lastChangeAt).Milliseconds()
	} else {
		downMs += now.Sub(s.lastChangeAt).Milliseconds()
	}
	return
}

// windowedTPS sums the trailing-60s ring buckets — a real windowed
// throughput figure rather than a lifetime average (item 4.5).
func (s *vendorState) windowedTPS(now time.Time) float64 {
	nowSec := now.Unix()
	var sum int64
	for i := 0; i < 60; i++ {
		if nowSec-s.tpsBucketSec[i] < 60 {
			sum += s.tpsBuckets[i]
		}
	}
	return float64(sum) / 60.0
}

// --- monitor capability interfaces (internal/monitor) [SCOPE P3-STATUS] ---

// ReadyForTraffic reports the role's readiness: at least one session is bound.
// An egress process with no live vendor bind cannot deliver, so it is not ready.
func (p *Process) ReadyForTraffic() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range p.state {
		if s.bound {
			return true
		}
	}
	return false
}

// MonitorSections adds P3's own section to /status: per-session bind state,
// window depth, delivery counters, plus the Hadal-derived richer stats (item
// 4.5) — latency avg/max, success rate, per-encoding counts, uptime/downtime,
// and windowed TPS — grouped under a vendor-level rollup since a vendor may
// now run more than one concurrent session.
func (p *Process) MonitorSections() map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	sessions := make([]map[string]any, 0, len(p.state))
	vendorBound := map[int64]bool{}
	sessionsBound := 0
	for key, s := range p.state {
		wsize, wdepth, wlimit := 0, 0, 0
		if s.win != nil {
			wsize, wdepth, wlimit = s.win.Size(), s.win.Depth(), s.win.Limit()
		}
		if s.bound {
			sessionsBound++
			vendorBound[key.VendorID] = true
		}
		upMs, downMs := s.upDownMs(now)
		// monitor.SuccessRate rather than a bare percentage: a session that
		// has handled nothing must not report the same 0.00% as one where
		// every submit failed. Those want opposite reactions — one is idle,
		// the other is an outage — and the Hadal reference conflates them.
		success := monitor.SuccessRate(s.accepted, s.failed)
		// UNDER STRESS is Hadal's isUnderStress, made specific: this vendor is
		// the constraint right now, either because it is pushing back
		// (throttle pause) or because its in-flight window is nearly full and
		// the next submit will block.
		underStress := now.Before(s.throttledUntil)
		if wsize > 0 && wdepth*10 >= wsize*8 {
			underStress = true
		}
		latencyAvg := int64(0)
		if s.latencyCount > 0 {
			latencyAvg = s.latencySumMs / s.latencyCount
		}
		row := map[string]any{
			"vendor_id":    key.VendorID,
			"session_idx":  key.Idx,
			"cid":          s.cid,
			"addr":         s.addr,
			"bound":        s.bound,
			"window_size":  wsize,
			"window_depth": wdepth,
			// How many may be outstanding NOW: under the size while answers
			// are coming back late (window.Pace).
			"window_limit":    wlimit,
			"submitted":       s.submitted,
			"accepted":        s.accepted,
			"failed":          s.failed,
			"throttled":       s.throttled,
			"throttle_paused": now.Before(s.throttledUntil),
			// status is a WORD, not a bool. "bound" answered "is the socket
			// up"; an operator asks "what is this session doing", and
			// connecting, throttled and failed are different answers.
			"status":           sessionStatus(s, now),
			"state":            string(monitor.State(s.lastActivity, now)),
			"success":          success,
			"success_rate_pct": round2(success.Percent),
			"under_stress":     underStress,
			"reconnections":    s.reconnections,
			"timeouts":         s.timeouts,
			"latency_avg_ms":   latencyAvg,
			"latency_max_ms":   s.latencyMaxMs,
			"tps_60s":          round2(s.windowedTPS(now)),
			"uptime_sec":       upMs / 1000,
			"downtime_sec":     downMs / 1000,
			"encodings":        copyCounts(s.encodingCounts),
			"receipts":         s.receipts,
			"mo":               s.moIn,
			"submit_statuses":  respCodeNames(s.respCodes),
		}
		if !s.boundSince.IsZero() && s.bound {
			row["bound_since"] = s.boundSince.UTC().Format(time.RFC3339)
		}
		if s.lastError != "" {
			row["last_error"] = s.lastError
		}
		if !s.lastActivity.IsZero() {
			row["last_activity"] = s.lastActivity.UTC().Format(time.RFC3339)
		}
		if !s.lastChangeAt.IsZero() {
			row["last_status_change"] = s.lastChangeAt.UTC().Format(time.RFC3339)
		}
		sessions = append(sessions, row)
	}
	vendorsTotal := len(p.current)
	// Counted here rather than in the portal so every reader agrees on what
	// "unhealthy" means, and so the value can be lifted into an indexed
	// column without decompressing a snapshot.
	degraded := 0
	for _, row := range sessions {
		if b, _ := row["bound"].(bool); !b {
			degraded++
			continue
		}
		if st, _ := row["under_stress"].(bool); st {
			degraded++
		}
	}
	out := map[string]any{
		"smpp_vendor": map[string]any{
			"vendors_total":     vendorsTotal,
			"vendors_bound":     len(vendorBound),
			"sessions_total":    len(p.state),
			"sessions_bound":    sessionsBound,
			"sessions_degraded": degraded,
			"vendors":           sessions,
			// Receipts and MO acked to the vendor but not yet written. Near
			// zero is healthy; a number that keeps climbing is the database
			// falling behind, no longer hidden inside "vendor latency".
			"inbound_queued": p.inbound.queued(),
			// The room in front of the receipt workers, and how often a session
			// held back its sending because half of it was taken: a rising
			// count is the answer to "why is this queue not emptying".
			"inbound_capacity": p.inbound.capacity(),
			"sends_held":       p.sendHolds.Load(),
			// Receipts finished in Redis (and the ones the record store still
			// took), and their records waiting to be written in batches.
			"receipts": p.receiptStatus(),
		},
	}
	// Status-code rules that matched in the last minute (vendorstatus/hits.go).
	if codes := p.vstatus.Section(); codes != nil {
		out["vendor_codes"] = codes
	}
	// Client receipts the queue store refused, kept in memory until it takes
	// them (dlr/keptpush.go). Said only while there are some.
	if n := dlrsvc.KeptClientReceipts(); n > 0 {
		out["client_receipts_kept"] = n
	}
	return p.recQ.AddTo(out)
}

func round2(f float64) float64 {
	return float64(int64(f*100)) / 100
}

// sessionStatus names what a session is doing, in the vocabulary an operator
// already uses from the vendor's own tooling.
func sessionStatus(s *vendorState, now time.Time) string {
	switch {
	case s.bound && now.Before(s.throttledUntil):
		return "THROTTLED"
	case s.bound:
		return "CONNECTED"
	case s.lastError != "":
		return "FAILED"
	case s.lastChangeAt.IsZero():
		return "CONNECTING"
	}
	return "DISCONNECTED"
}

// --- vendor health for routing (config/0103, PO 2026-09-14) ---

// timing is a vendor's down/recheck settings as last loaded — read at use
// time, so an edit applies without restarting its binds.
func (p *Process) timing(vendorID int64) vendorConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current[vendorID]
}

// healthEvery is how often bind and breaker state is turned into health.
const healthEvery = 2 * time.Second

// watchHealth turns each vendor's bind and breaker state into health every
// healthEvery: failing while none of its sessions in this process is bound,
// or while its error breaker is open. Vendors this process no longer serves
// are forgotten, so their rows leave vendor_health.
func (p *Process) watchHealth(ctx context.Context) {
	t := time.NewTicker(healthEvery)
	defer t.Stop()
	for {
		p.checkHealth()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (p *Process) checkHealth() {
	p.mu.Lock()
	ids := make(map[int64]bool, len(p.current))
	bound := map[int64]bool{}
	lastErr := map[int64]string{}
	for id := range p.current {
		ids[id] = true
	}
	for key, s := range p.state {
		if s.bound {
			bound[key.VendorID] = true
		} else if s.lastError != "" {
			lastErr[key.VendorID] = s.lastError
		}
	}
	p.mu.Unlock()
	for id := range ids {
		switch {
		case !bound[id]:
			why := "no bound session"
			if e := lastErr[id]; e != "" {
				why += ": " + e
			}
			p.health.Fail(id, why)
		case p.breakerOpen(id):
			p.health.Fail(id, "error breaker open")
		default:
			p.health.OK(id)
		}
	}
	p.health.Keep(ids)
}

// receiptStatus reports the Redis receipt path for monitoring: how many
// receipts were finished there and how many still took the record store, and
// the write-behind — waiting now, written, put back, let go.
func (p *Process) receiptStatus() map[string]any {
	fast, full := dlrsvc.FastStats()
	out := map[string]any{"in_redis": p.svc.Fast(), "fast": fast, "record_store": full}
	if p.rcptQueue != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		written, requeued, dropped := p.rcptQueue.Stats()
		out["waiting"] = p.rcptQueue.Waiting(ctx)
		out["written"], out["put_back"], out["let_go"] = written, requeued, dropped
	}
	return out
}
