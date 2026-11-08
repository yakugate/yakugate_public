package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/yakugate/yakugate/internal/platform/dbpool"
)

// Optional capability interfaces a process (worker.Worker / worker.Runnable)
// may implement to enrich its admin surface. A role that implements neither
// still gets a useful /healthz + /readyz + minimal /status for free.

// StatsProvider exposes a role's traffic counters on its /status page.
type StatsProvider interface{ MonitorStats() *Stats }

// ReadyProvider exposes a role's real readiness signal. Roles without one
// are considered ready while running (liveness == readiness for plain
// workers that poll a queue).
type ReadyProvider interface{ ReadyForTraffic() bool }

// SectionProvider lets a role add its own sections to /status (e.g. P2's
// SMPP bind list, P3's vendor sessions) without monitor knowing about them.
type SectionProvider interface{ MonitorSections() map[string]any }

// CommandProvider lets a role accept admin-triggered commands under
// /command/ (e.g. P3's per-vendor reload/reconnect — item 4.6) without
// monitor knowing anything about what commands exist. Gated by the same
// statusKey as /status; a role that doesn't implement it gets a 404 there,
// same as /status/pprof when no key is configured.
type CommandProvider interface {
	HandleCommand(w http.ResponseWriter, r *http.Request)
}

// ReadyCheck, when set, is the process's readiness rule (cmd/yakugate sets it
// to worker.Ready, so this listener and the health listener agree — bug 16).
// Unset: the role's own ReadyForTraffic, else ready.
var ReadyCheck func(w any) bool

func isReady(w any) bool {
	if ReadyCheck != nil {
		return ReadyCheck(w)
	}
	if rp, ok := w.(ReadyProvider); ok {
		return rp.ReadyForTraffic()
	}
	return true
}

// StatusSnapshot builds the JSON-ready status document for a role — the ONE
// shape used by the admin listener, the API-port /status (P1), and the Redis
// push, so all three can never drift apart.
func StatusSnapshot(role, instance string, start time.Time, w any) map[string]any {
	uptime := time.Since(start).Seconds()
	body := map[string]any{
		"gateway": map[string]any{
			"role":       role,
			"instance":   instance,
			"status":     map[bool]string{true: "ready", false: "not ready"}[isReady(w)],
			"started":    start.UTC().Format(time.RFC3339),
			"uptime_sec": int64(uptime),
			"time":       time.Now().UTC().Format(time.RFC3339),
		},
	}
	// This process's database pools (dbpool): one per database, with how many
	// connections are open, in use and idle, and how often a caller had to
	// wait for one. Every role reports them, so Monitoring can say where the
	// gateway's connections are (production, 2026-10-06: 600+ and no way to
	// see which role held them).
	if pools := dbpool.Stats(); len(pools) > 0 {
		body["db_pools"] = pools
	}
	if sp, ok := w.(StatsProvider); ok {
		if s := sp.MonitorStats(); s != nil {
			body["sms"] = map[string]any{
				"accepted":    s.Accepted.Snapshot(uptime),
				"rejected":    s.Rejected.Snapshot(uptime),
				"segments":    s.Segments.Snapshot(uptime),
				"auth_failed": s.AuthFail.Snapshot(uptime),
				// reject_by_reason (PO 2026-07-27): top rejection codes —
				// separates "a client is misbehaving" from "we broke
				// something" at a glance, without grepping the reject log.
				"reject_by_reason": s.RejectBreakdown(uptime),
			}
			body["accounts"] = s.Accounts(uptime)
			body["accept_latency_bounds_ms"] = AcceptBoundsMs
			// Pipeline latency was measured for the /metrics gauge and never
			// reached the snapshot, so monitoring could show throughput but
			// not whether the gateway was getting SLOWER — which is the shape
			// of nearly every real degradation. p50 beside p95 on purpose: p95
			// alone cannot tell "everything slowed a little" from "a few
			// requests fell off a cliff", and those have different causes.
			lat := map[string]any{"known": false}
			if p95, ok := s.Pipeline.Quantile(0.95); ok {
				p50, _ := s.Pipeline.Quantile(0.50)
				lat = map[string]any{
					"known":   true,
					"p50_ms":  p50 * 1000,
					"p95_ms":  p95 * 1000,
					"samples": s.Pipeline.Len(),
				}
			}
			body["latency"] = lat
		}
	}
	// The machine itself. Until 2026-09-09 the engine reported nothing about
	// its host, so the first question of any slowdown — "was it us or the
	// box?" — had no answer anywhere. Cheap: a handful of /proc reads.
	// Which gateway is this. Cheap (cached) and it is what makes a dashboard
	// showing several deployments legible.
	body["deployment"] = DeploymentInfo()
	if fs := Flow.section(uptime); fs != nil {
		body["flow"] = fs
	}
	// Delivery verdicts (receipt senders and appliers) and route choices
	// (ingress), each only where this process has seen any (PO 2026-09-11).
	if vs := Verdicts.section(); vs != nil {
		body["verdicts"] = vs
	}
	if rs := Routes.section(uptime); rs != nil {
		body["routing"] = rs
	}
	// Messages this process put into and took out of each queue (D-127).
	if qf := queueFlowSection(); qf != nil {
		body["queue_flow"] = qf
	}
	body["system"] = ReadSystem()
	// Three-state liveness (the best idea in the Hadal reference): a role with
	// no traffic at 3am is quiet, not broken, and a binary up/down cannot say
	// so. A role that is publishing at all is at least running; when it has
	// traffic counters, its last activity decides.
	body["state"] = string(roleState(w))
	if xp, ok := w.(SectionProvider); ok {
		for k, v := range xp.MonitorSections() {
			body[k] = v
		}
	}
	addSections(body)
	return body
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// ServeAdmin runs the per-process admin listener: /healthz (liveness),
// /readyz (readiness), /status (counters; gated by statusKey exactly like
// P1's API-port /status — empty key disables the endpoint). Blocks until ctx
// is cancelled. Every role gets this via cmd/yakugate when YAKU_ADMIN_ADDR is
// set, so probes and monitoring work identically across the fleet.
func ServeAdmin(ctx context.Context, addr, role, instance, statusKey string, start time.Time, w any) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, _ *http.Request) {
		writeJSON(rw, 200, map[string]string{"status": "ok", "role": role})
	})
	// The HTML health page, for a person with a browser (2026-09-21).
	mux.HandleFunc("/", healthPage(role, instance, statusKey, start, w))
	mux.HandleFunc("/readyz", func(rw http.ResponseWriter, _ *http.Request) {
		if isReady(w) {
			writeJSON(rw, 200, map[string]string{"status": "ready", "role": role})
			return
		}
		writeJSON(rw, 503, map[string]string{"status": "not ready", "role": role})
	})
	mux.HandleFunc("/status", func(rw http.ResponseWriter, r *http.Request) {
		if statusKey == "" {
			http.NotFound(rw, r)
			return
		}
		if r.Header.Get("yaku-status-key") != statusKey && r.URL.Query().Get("key") != statusKey {
			writeJSON(rw, 401, map[string]string{"error": "AUTH_FAILED"})
			return
		}
		writeJSON(rw, 200, StatusSnapshot(role, instance, start, w))
	})
	// Profiling (2026-07-26): pprof on the admin port, behind the SAME status
	// key — finding a hot path costs one `go tool pprof` against a URL instead
	// of a rebuild. Disabled (404) whenever the status key is unset, like /status.
	mux.HandleFunc("/debug/pprof/", func(rw http.ResponseWriter, r *http.Request) {
		if statusKey == "" || (r.Header.Get("yaku-status-key") != statusKey && r.URL.Query().Get("key") != statusKey) {
			http.NotFound(rw, r)
			return
		}
		pprof.Index(rw, r)
	})
	mux.HandleFunc("/debug/pprof/profile", func(rw http.ResponseWriter, r *http.Request) {
		if statusKey == "" || (r.Header.Get("yaku-status-key") != statusKey && r.URL.Query().Get("key") != statusKey) {
			http.NotFound(rw, r)
			return
		}
		pprof.Profile(rw, r)
	})
	// Admin command surface (item 4.6): only mounted when the role implements
	// CommandProvider, gated by the same status key as /status.
	if cp, ok := w.(CommandProvider); ok {
		mux.HandleFunc("/command/", func(rw http.ResponseWriter, r *http.Request) {
			if statusKey == "" || (r.Header.Get("yaku-status-key") != statusKey && r.URL.Query().Get("key") != statusKey) {
				http.NotFound(rw, r)
				return
			}
			cp.HandleCommand(rw, r)
		})
	}
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// roleState derives running/dormant/inactive for a role.
//
// A role with no traffic counters of its own — ten of the fifteen — reports
// running rather than inactive. It is publishing, so it is alive, and marking
// two thirds of the gateway "inactive" would train an operator to ignore the
// column entirely.
func roleState(w any) Liveness {
	// The newest of the two activity sources wins: ingress counts accepts in
	// Stats, every other role counts what it moves in Flow. A role that has
	// recorded neither is publishing, so it is at least running.
	var last time.Time
	if sp, ok := w.(StatsProvider); ok {
		if s := sp.MonitorStats(); s != nil {
			last = s.LastActivity()
		}
	}
	if fl := Flow.LastActivity(); fl.After(last) {
		last = fl
	}
	if last.IsZero() {
		return Running
	}
	return State(last, time.Now())
}
