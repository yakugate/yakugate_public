package httpvendor

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// P4 monitoring — the HTTP vendor leg.
//
// Until 2026-09-10 this process reported NOTHING beyond "I am alive": no
// counters, no latency, no per-vendor state. Every HTTP vendor was invisible,
// which is a strange gap given the SMPP leg has carried per-session detail for
// weeks and the two legs fail in equally expensive ways.
//
// Deliberately shaped like P3's section rather than inventing a second
// vocabulary. An operator should not have to learn two dashboards because one
// vendor speaks SMPP and another speaks HTTP — the questions are identical
// ("is it up, is it fast, is it accepting, is it the bottleneck") and only the
// transport differs. Where a field cannot mean anything over HTTP (a bind
// window) it is simply absent rather than faked.

// httpVendorState is one HTTP vendor's live counters.
type httpVendorState struct {
	cid string
	// host is the vendor's submit URL, trimmed to scheme+host. The full URL
	// can carry a key in the query string, and a dashboard is exactly where
	// that must not appear.
	host string

	// calls is vendor round trips; submitted/accepted/failed count MESSAGES.
	// They differ only for a bulk vendor, where one call carries a chunk — and
	// the ratio between them is the one number that says whether bulk is
	// actually batching or has degraded to a chunk of one.
	calls     int64
	submitted int64
	accepted  int64
	failed    int64
	// timeouts are calls that never answered, kept apart from failures: a
	// refusal is the vendor saying no, a timeout is the vendor saying nothing,
	// and the second is what a vendor about to fall over looks like.
	timeouts int64
	// reauths counts session re-logins (D-103 reactive re-auth). A vendor
	// re-authenticating constantly is working but wasting a round trip on
	// every send, and nothing else would show it.
	reauths int64

	latencySumMs int64
	latencyMaxMs int64
	latencyCount int64

	encodingCounts map[string]int64

	// respCodes counts the HTTP status of every send request, 0 meaning no
	// response at all (Hadal's onSubmitStatuses). accepted/failed are OUR
	// verdict; this is what the vendor actually answered, which is what an
	// operator quotes back to the vendor.
	respCodes map[int]int64

	lastActivity time.Time
	lastError    string
	lastErrorAt  time.Time

	tpsBuckets   [60]int64
	tpsBucketSec [60]int64
}

// skipped is a vendor that is configured but could not be loaded.
type skipped struct {
	cid    string
	reason string
	at     time.Time
}

// vendorStats holds P4's per-vendor counters. Its own mutex rather than the
// Process's: Step holds no lock while calling a vendor, and monitoring must
// never be able to block a send.
type vendorStats struct {
	mu      sync.Mutex
	m       map[string]*httpVendorState
	skipped map[string]skipped
	// active is the vendor set of the latest reload, cid -> its adapter (for
	// session details). Held here, under this lock, rather than read from
	// Process.vendors: the monitoring goroutine must never read a slice the
	// send loop is replacing. It also decides which rows are published — a
	// vendor paused or removed drops off the dashboard instead of lingering
	// with frozen counters; its counters are kept in case it comes back.
	active map[string]*httpadapter.Adapter
}

func newVendorStats() *vendorStats {
	return &vendorStats{m: map[string]*httpVendorState{}, skipped: map[string]skipped{},
		active: map[string]*httpadapter.Adapter{}}
}

// setActive records the vendor set a reload produced.
func (v *vendorStats) setActive(m map[string]*httpadapter.Adapter) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.active = m
}

// noteResponse records the HTTP status of one send request (0 = no response).
func (v *vendorStats) noteResponse(cid string, code int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	s := v.get(cid, "")
	if s.respCodes == nil {
		s.respCodes = map[int]int64{}
	}
	s.respCodes[code]++
}

// respCodeNames keys the counts by status as text, "no response" for 0.
// copyCounts hands a status report its own copy of a vendor's counts. The
// report is turned into JSON after the lock is let go, while sends go on
// adding to the live map; the smpp-vendor crashed on exactly that under load
// (2026-10-14, "fatal error: concurrent map iteration and map write").
func copyCounts(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, n := range m {
		out[k] = n
	}
	return out
}

func respCodeNames(m map[int]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for code, n := range m {
		k := strconv.Itoa(code)
		if code == 0 {
			k = "no response"
		}
		out[k] = n
	}
	return out
}

// noteSkipped records a vendor that could not be loaded, and why. Replaces the
// previous entry for that cid: only the CURRENT reason matters, and a vendor
// whose config is fixed should disappear from the list on the next reload.
func (v *vendorStats) noteSkipped(cid, reason string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.skipped[cid] = skipped{cid: cid, reason: trimErr(reason), at: time.Now()}
}

// clearSkipped resets the list at the start of a reload, so a vendor fixed
// since the last pass stops being reported as broken.
func (v *vendorStats) clearSkipped() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.skipped = map[string]skipped{}
}

func (v *vendorStats) get(cid, url string) *httpVendorState {
	s := v.m[cid]
	if s == nil {
		s = &httpVendorState{cid: cid, host: hostOf(url), encodingCounts: map[string]int64{}}
		v.m[cid] = s
	}
	if s.host == "" {
		s.host = hostOf(url)
	}
	return s
}

// hostOf trims a submit URL to scheme://host. A vendor URL routinely carries
// an API key in its query string, and this value is rendered on a screen and
// stored in monitoring history — neither is a place for a credential.
func hostOf(raw string) string {
	if raw == "" {
		return ""
	}
	if i := strings.Index(raw, "://"); i >= 0 {
		rest := raw[i+3:]
		if j := strings.IndexAny(rest, "/?"); j >= 0 {
			return raw[:i+3] + rest[:j]
		}
		return raw
	}
	if j := strings.IndexAny(raw, "/?"); j >= 0 {
		return raw[:j]
	}
	return raw
}

// noteSubmit records one call carrying n messages (1 unless the vendor is bulk).
func (v *vendorStats) noteSubmit(cid, url, encoding string, n int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	s := v.get(cid, url)
	s.calls++
	s.submitted += n
	s.lastActivity = time.Now()
	if encoding != "" {
		s.encodingCounts[encoding] += n
	}
	sec := time.Now().Unix()
	idx := sec % 60
	if s.tpsBucketSec[idx] != sec {
		s.tpsBuckets[idx] = 0
		s.tpsBucketSec[idx] = sec
	}
	s.tpsBuckets[idx] += n
}

// noteResult records the outcome of one call carrying n messages.
//
// The outcome is counted per MESSAGE, because submitted is: a bulk call that
// succeeds accepted its whole chunk, and counting it as one would put a 25-
// message chunk at 25 submitted / 1 accepted — a 4% success rate on a vendor
// that is perfectly healthy. Latency stays per CALL: it is the vendor's round
// trip, and a bulk call is one round trip however many recipients it carries.
func (v *vendorStats) noteResult(cid string, ok bool, latencyMs int64, errText string, n int64) {
	// The gateway-wide Out line (monitor/flow.go) is fed from the same point,
	// so the role card and the overview chart can never disagree.
	if ok {
		monitor.Flow.NoteOut(n)
		monitor.Flow.NoteVendorLatency(time.Duration(latencyMs) * time.Millisecond)
	} else {
		monitor.Flow.NoteOutFailed(n)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	s := v.get(cid, "")
	if ok {
		s.accepted += n
		s.latencySumMs += latencyMs
		s.latencyCount++
		if latencyMs > s.latencyMaxMs {
			s.latencyMaxMs = latencyMs
		}
		return
	}
	// A timeout is classified from the error text because that is all the
	// adapter returns. Crude, and better than losing the distinction: "context
	// deadline exceeded" and "vendor said 403" are not the same incident.
	if isTimeout(errText) {
		s.timeouts += n
	} else {
		s.failed += n
	}
	s.lastError, s.lastErrorAt = trimErr(errText), time.Now()
}

// noteReauth records a session re-login.
func (v *vendorStats) noteReauth(cid string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.get(cid, "").reauths++
}

func isTimeout(e string) bool {
	e = strings.ToLower(e)
	return strings.Contains(e, "timeout") || strings.Contains(e, "deadline exceeded") ||
		strings.Contains(e, "context canceled")
}

// trimErr bounds what reaches a screen and the history blob. A vendor can
// return a whole HTML error page, and none of it belongs in a monitoring row.
func trimErr(e string) string {
	e = strings.TrimSpace(e)
	if len(e) > 160 {
		return e[:160] + "…"
	}
	return e
}

func (s *httpVendorState) windowedTPS(now time.Time) float64 {
	cutoff := now.Unix() - 60
	var total int64
	for i := range s.tpsBuckets {
		if s.tpsBucketSec[i] > cutoff {
			total += s.tpsBuckets[i]
		}
	}
	return float64(total) / 60
}

// MonitorSections publishes P4's per-vendor picture.
func (p *Process) MonitorSections() map[string]any {
	if p.stats == nil {
		return nil
	}
	// Which vendors wait for a session: the one way an HTTP vendor can be
	// "not there" (2026-10-12). Read before the stats lock, never inside it.
	waiting := map[string]bool{}
	if p.health != nil {
		p.vmu.Lock()
		for _, v := range p.vendors {
			if p.health.NotReadyNow(v.id) {
				waiting[v.cid] = true
			}
		}
		p.vmu.Unlock()
	}
	p.stats.mu.Lock()
	defer p.stats.mu.Unlock()
	now := time.Now()
	cids := make([]string, 0, len(p.stats.active))
	for cid := range p.stats.active {
		cids = append(cids, cid)
	}
	sort.Strings(cids) // stable order, so the table does not reshuffle every 5s
	rows := make([]map[string]any, 0, len(cids))
	degraded := 0
	for _, cid := range cids {
		s := p.stats.get(cid, "")
		success := monitor.SuccessRate(s.accepted, s.failed+s.timeouts)
		state := monitor.State(s.lastActivity, now)
		// UNDER STRESS over HTTP means the vendor is answering slowly or
		// refusing. There is no in-flight window to watch as there is on
		// SMPP, so the signal is behavioural: a recent error, or an average
		// latency past two seconds, which for an SMS submit API is already
		// far outside normal.
		underStress := (!s.lastErrorAt.IsZero() && now.Sub(s.lastErrorAt) < time.Minute)
		avg := int64(0)
		if s.latencyCount > 0 {
			avg = s.latencySumMs / s.latencyCount
			if avg > 2000 {
				underStress = true
			}
		}
		if underStress {
			degraded++
		}
		row := map[string]any{
			"cid":              s.cid,
			"type":             "HTTP",
			"host":             s.host,
			"status":           httpVendorStatus(s, now, waiting[cid]),
			"state":            string(state),
			"calls":            s.calls,
			"submitted":        s.submitted,
			"accepted":         s.accepted,
			"failed":           s.failed,
			"timeouts":         s.timeouts,
			"reauths":          s.reauths,
			"success":          success,
			"success_rate_pct": round2(success.Percent),
			"latency_avg_ms":   avg,
			"latency_max_ms":   s.latencyMaxMs,
			"tps_60s":          round2(s.windowedTPS(now)),
			"under_stress":     underStress,
			"encodings":        copyCounts(s.encodingCounts),
			"http_statuses":    respCodeNames(s.respCodes),
		}
		// Session-auth vendors only (Hadal's sessionDetails): is the login
		// alive, when does it lapse, how often has it had to log in again.
		if ad := p.stats.active[cid]; ad != nil {
			if si, ok := ad.SessionInfo(); ok {
				row["session"] = si
			}
		}
		if !s.lastActivity.IsZero() {
			row["last_activity"] = s.lastActivity.UTC().Format(time.RFC3339)
		}
		if s.lastError != "" {
			row["last_error"] = s.lastError
			row["last_error_at"] = s.lastErrorAt.UTC().Format(time.RFC3339)
		}
		rows = append(rows, row)
	}
	// Reporting = has carried traffic. A loaded vendor that has sent nothing
	// yet is listed (as IDLE) but is not yet telling us anything.
	reporting := 0
	for _, r := range rows {
		if r["submitted"].(int64) > 0 {
			reporting++
		}
	}
	skips := make([]map[string]any, 0, len(p.stats.skipped))
	for _, sk := range p.stats.skipped {
		skips = append(skips, map[string]any{
			"cid": sk.cid, "reason": sk.reason, "at": sk.at.UTC().Format(time.RFC3339),
		})
	}
	notReady := 0
	for _, cid := range cids {
		if waiting[cid] {
			notReady++
		}
	}
	out := map[string]any{
		"http_vendor": map[string]any{
			"vendors_total":     len(cids),
			"vendors_reporting": reporting,
			"vendors_degraded":  degraded,
			// An HTTP vendor has no connection, so it is never "down"
			// (Saleem, 2026-10-12). The only one that cannot be sent to is
			// one that needs a session and has none; everything that counts
			// HTTP vendors as "up" reads total minus this.
			"vendors_not_ready": notReady,
			// A configured vendor that could not be loaded is carrying no
			// traffic and nobody has been told. This is the difference between
			// "we have one vendor" and "we have three, two of them broken".
			"vendors_skipped": len(skips),
			"skipped":         skips,
			"vendors":         rows,
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
	// Receipts finished in Redis against receipts that took the record
	// store's path (with the DB worker; dlr fast.go) — this role's own
	// receipts: the answers of its status polls.
	if p.svc != nil && p.svc.Fast() {
		fast, full := dlrsvc.FastStats()
		out["receipts"] = map[string]any{"in_redis": true, "fast": fast, "record_store": full}
	}
	return p.recQ.AddTo(out)
}

// httpVendorStatus names what this vendor is doing. HTTP is stateless, so
// there is no CONNECTED to report — the honest equivalents are whether recent
// calls are working.
func httpVendorStatus(s *httpVendorState, now time.Time, waitingForSession bool) string {
	switch {
	case waitingForSession:
		// It needs a session and has none: nothing can be sent yet. Not
		// "down" — nothing was asked of it.
		return "NOT_READY"
	case s.submitted == 0:
		return "IDLE"
	case !s.lastErrorAt.IsZero() && now.Sub(s.lastErrorAt) < time.Minute:
		return "ERRORS"
	}
	return "READY"
}

func round2(f float64) float64 { return float64(int64(f*100)) / 100 }
