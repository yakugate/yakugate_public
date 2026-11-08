package httpserver

// Prometheus-style /metrics endpoint (P1 scope finalize 2026-08-09): scrapeable
// counters for accept TPS, rejects (with a per-reason breakdown), segments,
// auth failures, and uptime. Open by default (a metrics endpoint carries no
// customer PII, unlike /status which names accounts) — deployments that want it
// gated can front it with their own reverse-proxy auth.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func (p *Process) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	uptime := time.Since(p.stats.Start()).Seconds()
	accepted := p.stats.Accepted.Snapshot(uptime)
	rejected := p.stats.Rejected.Snapshot(uptime)
	segments := p.stats.Segments.Snapshot(uptime)
	authFail := p.stats.AuthFail.Snapshot(uptime)

	var b strings.Builder
	metric := func(name, help string, val any) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s %v\n", name, help, name, name, val)
	}
	gauge := func(name, help string, val any) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s %v\n", name, help, name, name, val)
	}
	metric("yaku_accepted_total", "Messages accepted (durably enqueued or replay).", accepted.Total)
	metric("yaku_rejected_total", "Requests/recipients rejected.", rejected.Total)
	metric("yaku_segments_total", "Segments accepted.", segments.Total)
	metric("yaku_auth_failed_total", "Request-level authentication failures.", authFail.Total)
	gauge("yaku_accept_tps_60s", "Accept rate over the last 60s (segments/s not implied).", accepted.TPS60)
	gauge("yaku_uptime_seconds", "Process uptime in seconds.", int64(uptime))

	// Per-reason reject breakdown, one series per reason code.
	fmt.Fprint(&b, "# HELP yaku_reject_reason_total Rejects by reason code.\n# TYPE yaku_reject_reason_total counter\n")
	for reason, snap := range p.stats.RejectBreakdown(uptime) {
		fmt.Fprintf(&b, "yaku_reject_reason_total{reason=%q} %d\n", reason, snap.Total)
	}

	// p95 pipeline latency (P1 scope finalize 2026-08-09 §2): the SLA gauge —
	// per-recipient pipeline duration over the most recent samples. Emitted
	// only once traffic has been seen (a 0 at idle would read as "fast").
	if p95, ok := p.stats.Pipeline.Quantile(0.95); ok {
		gauge("yaku_pipeline_latency_p95_seconds", "p95 per-recipient pipeline latency (recent samples).", p95)
	}
	if p50, ok := p.stats.Pipeline.Quantile(0.50); ok {
		gauge("yaku_pipeline_latency_p50_seconds", "median per-recipient pipeline latency (recent samples).", p50)
	}

	// Reference-cache age (scope §2): time since the last successful refresh.
	if p.ref != nil {
		if age, ok := p.ref.Age(); ok {
			gauge("yaku_cache_age_seconds", "Seconds since the reference cache last refreshed successfully.", int64(age.Seconds()))
		}
	}

	// Per-vendor queue depth (scope §2), from the same optional backend
	// interface /status uses; backends without depth stats simply omit it.
	if p.queueDepthSrc != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if depths, err := p.queueDepthSrc.QueueDepths(ctx); err == nil && len(depths) > 0 {
			fmt.Fprint(&b, "# HELP yaku_queue_depth Queued messages per vendor queue.\n# TYPE yaku_queue_depth gauge\n")
			for vendor, st := range depths {
				fmt.Fprintf(&b, "yaku_queue_depth{vendor=%q} %d\n", vendor, st.Depth)
			}
		}
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b.String()))
}
