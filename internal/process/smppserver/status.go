package smppserver

import (
	"context"
	"sort"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/ratelimit"
	"github.com/yakugate/yakugate/internal/rejectlog"
	"github.com/yakugate/yakugate/internal/store"
)

// Monitoring capability interfaces (internal/monitor) — the SMPP edge's
// readyz/status parity with P1 ([SCOPE P1-STATUS], 2026-07-26). The admin
// listener and the Redis cluster push discover these via cmd/yakugate; this
// role has no HTTP API port of its own.

// MonitorStats exposes the shared traffic counters (monitor.StatsProvider) —
// same accepted/rejected/segments/auth_failed set as the HTTP edge.
func (p *Process) MonitorStats() *monitor.Stats { return p.stats }

// ReadyForTraffic (monitor.ReadyProvider): same strict rule as P1 — reference
// data loaded successfully AND at least one account exists. A bind against an
// empty cache could only fail auth; that is 503-not-ready, not 401-noise.
//
// And the prices have been read at least once (2026-10-12): a role that
// cannot price a message refuses it (pipeline.RaterPricer.Ready).
//
// And the queue store answers (19 Oct 2026, queue_ready.go): with it down
// every submit is refused ESME_RMSGQFUL.
func (p *Process) ReadyForTraffic() bool {
	if !p.ref.Ready() {
		return false
	}
	if !p.rater.Loaded() {
		return false
	}
	if !p.queueOK.Up() {
		return false
	}
	ac, _, _ := p.ref.Counts()
	return ac > 0
}

// MonitorSections (monitor.SectionProvider): the numbers an operator needs
// from an SMPP edge — who is bound right now, and how the delivery legs are
// doing (Kannel's "Box connections" section, in JSON).
func (p *Process) MonitorSections() map[string]any {
	// One row per bind (Hadal's per-user smppState). Beyond who/where/since:
	// what the bind has carried, whether the client is still answering
	// (last enquire_link), and how full its ingress window is — a client
	// sitting at its window ceiling is being throttled by us, and that is the
	// first thing they will ask about.
	type bindInfo struct {
		Account        string `json:"account"`
		Slot           int    `json:"slot"`
		Mode           string `json:"mode"`
		IP             string `json:"ip"`
		Since          string `json:"since"`
		State          string `json:"state"`
		SubmitsOK      int64  `json:"submits_ok"`
		SubmitsRefused int64  `json:"submits_refused"`
		ReceiptsPushed int64  `json:"receipts_pushed"`
		MOPushed       int64  `json:"mo_pushed"`
		WindowSize     int    `json:"window_size"`
		WindowInFlight int    `json:"window_in_flight"`
		// Under the process's limit (inflight.go): this bind's submits being
		// worked on, and those waiting for a slot.
		InProgress int   `json:"in_progress,omitempty"`
		Held       int64 `json:"held,omitempty"`
		// The delivery window (pump.go): receipts and MO this bind may have
		// unanswered, how many are out, how many wait to go down it.
		DeliverWindow   int    `json:"deliver_window,omitempty"`
		DeliverOut      int    `json:"deliver_out"`
		DeliverWaiting  int    `json:"deliver_waiting"`
		LastActivity    string `json:"last_activity,omitempty"`
		LastEnquireLink string `json:"last_enquire_link,omitempty"`
	}
	modeName := map[bindMode]string{modeTX: "TX", modeRX: "RX", modeTRX: "TRX"}
	stamp := func(n int64) string {
		if n == 0 {
			return ""
		}
		return time.Unix(0, n).UTC().Format(time.RFC3339)
	}
	now := time.Now()
	// Empty, not nil: no binds is a list with nothing in it, and JSON null
	// makes every reader special-case it.
	binds := []bindInfo{}
	for _, s := range p.conns.all() {
		if s.acct == nil {
			continue
		}
		bi := bindInfo{
			Account:         s.acct.Username,
			Slot:            s.slot,
			Mode:            modeName[s.mode],
			IP:              s.ip,
			Since:           s.boundAt.UTC().Format(time.RFC3339),
			SubmitsOK:       s.submitsOK.Load(),
			SubmitsRefused:  s.submitsErr.Load(),
			ReceiptsPushed:  s.pushedDLR.Load(),
			MOPushed:        s.pushedMO.Load(),
			LastActivity:    stamp(s.lastActive.Load()),
			LastEnquireLink: stamp(s.lastEnq.Load()),
		}
		if s.sem != nil {
			bi.WindowSize, bi.WindowInFlight = cap(s.sem), len(s.sem)
		}
		if p.limit != nil {
			_, _, bi.InProgress = p.limit.inUse(s)
			bi.Held = s.heldNow()
		}
		if s.canReceive() {
			bi.DeliverWindow = p.deliveryWindow(s.acct.ID)
			bi.DeliverOut, bi.DeliverWaiting = p.pumpOf(s).load()
		}
		last := s.boundAt
		if n := s.lastActive.Load(); n > 0 {
			last = time.Unix(0, n)
		}
		bi.State = string(monitor.State(last, now))
		binds = append(binds, bi)
	}
	sort.Slice(binds, func(i, j int) bool {
		if binds[i].Account != binds[j].Account {
			return binds[i].Account < binds[j].Account
		}
		return binds[i].Since < binds[j].Since
	})
	uptime := time.Since(p.stats.Start()).Seconds()
	delivery := map[string]any{
		"receipts_confirmed": p.stats2.receipts.Snapshot(uptime),
		"mo_confirmed":       p.stats2.mo.Snapshot(uptime),
		"resp_timeouts":      p.stats2.respTimeouts.Snapshot(uptime),
		"bind_auth_failures": p.stats2.authFails.Snapshot(uptime),
		"window_throttled":   p.stats2.throttled.Snapshot(uptime),
	}
	// Delivered receipts whose ack the queue store refused, still to be
	// taken off (dlr.AckKeeper). Shown only while there are some.
	if n := p.acks.Load().Kept(); n > 0 {
		delivery["receipt_acks_kept"] = n
	}
	// DLR/MO pending-correlation counts (PO 2026-07-27): how many receipts/MO
	// are staged waiting for a client push, plus oldest-pending age. Only the
	// backends that implement store.PendingCountStats (MySQL, and Redis since 2026-09-11) report
	// it — others simply omit these two keys.
	if ps, ok := p.queue.(store.PendingCountStats); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		dlrPending, moPending, err := ps.PendingCounts(ctx)
		cancel()
		if err == nil {
			delivery["dlr_pending"] = dlrPending
			delivery["mo_pending"] = moPending
		}
	}
	smpp := map[string]any{
		"connections": atomic.LoadInt64(&p.connCount),
		"binds":       binds,
		"bind_count":  len(binds),
		"delivery":    delivery,
		"concat":      p.concatSection(uptime),
	}
	// The limit on submits worked on at once: its size, what is taken, and
	// the submits held or refused for it (inflight.go).
	if p.limit != nil {
		smpp["in_flight"] = p.limit.section(uptime)
	}
	// rate_held: per account, the checks its MO / receipt rate on this
	// gateway gave nothing (4 Nov 2026) — a customer queue held by its rate
	// is not one that nothing takes out.
	if held := rateHeld(&p.moHeld, &p.dlrHeld); held != nil {
		smpp["rate_held"] = held
	}
	// reject_log: refusals' rows written, dropped and failed (rejectlog);
	// zeros until the first refusal makes the queue, so Monitoring reads 0,
	// not nothing.
	sections := p.recQ.AddTo(map[string]any{"smpp": smpp})
	// client_tries: the tries on binds written, waiting and dropped (D-140).
	if st := p.tryLog().Stats(); st != nil {
		sections["client_tries"] = st
	}
	if q := p.rejectSeen.Load(); q != nil {
		return q.AddTo(sections)
	}
	if p.rejects != nil {
		sections["reject_log"] = rejectlog.Stats{}
	}
	return sections
}

// concatSection is multipart assembly health (PO 2026-09-11): how many parts
// arrive, how many groups complete or time out, how long assembly takes, and
// — from the Redis buffer only, since monitoring reads no tables — how many
// groups are waiting right now and for how long.
func (p *Process) concatSection(uptime float64) map[string]any {
	out := map[string]any{
		"parts_buffered":   p.stats2.concatParts.Snapshot(uptime),
		"groups_completed": p.stats2.concatCompleted.Snapshot(uptime),
		"groups_expired":   p.stats2.concatExpired.Snapshot(uptime),
		"buffer_fallbacks": p.stats2.concatFallback.Snapshot(uptime),
		"buffer":           "none",
	}
	p.stats2.concatMu.Lock()
	if p.stats2.concatTook != nil {
		counts := p.stats2.concatTook.Counts(time.Now().Unix())
		if p50, _, ok := monitor.HistQuantile(concatBoundsSec, counts, 0.50); ok {
			p95, over, _ := monitor.HistQuantile(concatBoundsSec, counts, 0.95)
			out["assembly_p50_sec"], out["assembly_p95_sec"], out["assembly_p95_over"] = p50, p95, over
		}
	}
	p.stats2.concatMu.Unlock()
	switch a := p.assembler.(type) {
	case nil:
	case interface {
		Pending(context.Context) (int64, int64, error)
	}:
		out["buffer"] = "redis"
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		groups, oldest, err := a.Pending(ctx)
		cancel()
		if err == nil {
			out["pending_groups"], out["oldest_pending_sec"] = groups, oldest
		}
	default:
		out["buffer"] = "mysql" // counters only: the pending count would be a table query
	}
	return out
}

// rateHeld is the rate_held section: {"mo": {acct: …}, "dlr": {acct: …}}.
func rateHeld(mo, dlr *ratelimit.Holds) map[string]any {
	out := map[string]any{}
	if m := mo.Section(); m != nil {
		out["mo"] = m
	}
	if d := dlr.Section(); d != nil {
		out["dlr"] = d
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
