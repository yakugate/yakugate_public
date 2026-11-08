package httpserver

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/bootstrap"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/rejectlog"
)

// envSeconds reads a positive integer env var as a duration in seconds.
func envSeconds(key string, def int) time.Duration {
	if n, err := strconv.Atoi(env(key, strconv.Itoa(def))); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return time.Duration(def) * time.Second
}

// Monitoring capability interfaces (internal/monitor) — make P1's /status,
// every role's admin-port /status, and the Redis cluster push share ONE
// snapshot shape. [SCOPE P1-STATUS]

// MonitorStats exposes the traffic counters (monitor.StatsProvider).
func (p *Process) MonitorStats() *monitor.Stats { return p.stats }

// ReadyForTraffic exposes the readiness signal (monitor.ReadyProvider).
func (p *Process) ReadyForTraffic() bool { return p.readyForTraffic() }

// readyForTraffic is what /readyz and monitoring say: ready(), and the queue
// store answers (queueHealth, go-live test 18 Oct, P1 E3). The submit gate
// keeps to ready(): while the queue store is away a send is refused by the
// enqueue stage with QUEUE_UNAVAILABLE and its Retry-After, as before.
func (p *Process) readyForTraffic() bool { return p.ready() && p.queueOK.Up() }

// MonitorSections adds P1's own extras (monitor.SectionProvider).
func (p *Process) MonitorSections() map[string]any {
	ac, tk, rt := p.ref.Counts()
	sections := map[string]any{
		"refdata": map[string]int{"accounts": ac, "tokens": tk, "routes": rt},
	}
	// Queue depth/age per vendor (PO 2026-07-27) — the earliest-warning signal
	// for a stalled vendor: rising depth AND rising age together means
	// stalled, not just busy. Omitted when the backend doesn't support it.
	if p.queueDepthSrc != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if depths, err := p.queueDepthSrc.QueueDepths(ctx); err == nil {
			sections["queues"] = depths
		}
	}
	// What MySQL and Redis say about themselves (D-129) is no longer this
	// role's: the one process of the deployment holding the turn adds it to
	// its report (monitor/storeturn.go, 2026-10-26).
	// Reject records: how many wait for a writer, and how many were dropped
	// because a storm of refusals outran the few writers (rejectlog).
	if q := p.rejectSeen.Load(); q != nil {
		sections["reject_log"] = q.Stats()
	} else if p.rejects != nil {
		sections["reject_log"] = rejectlog.Stats{} // zeros until the first refusal
	}
	// With YAKU_RECORDS_VIA=worker the reject rows go to the DB worker: how
	// many wait there, and whether one is running. Asked of the queue itself:
	// p.rejects is a recordq.RejectsOrDirect (D-135), or nil with reject
	// logging off while the re-route inbox still queues through it.
	sections = p.recQ.AddTo(sections)
	// Messages moved off down vendors, and how many still wait per vendor
	// (PO 2026-09-14).
	if st := p.mover.Stats(); st != nil {
		sections["reroute"] = st
	}
	return sections
}

// handleStatusPage renders GET /status: the Kannel-bearerbox-status
// equivalent as JSON — gateway identity/uptime, message totals with TPS
// rates (60s / 300s / lifetime + 1s peak), and every account seen since
// start with its own counters. Same snapshot the admin listener and the
// Redis cluster push serve. [SCOPE P1-STATUS]
func (p *Process) handleStatusPage(w http.ResponseWriter) {
	writeJSON(w, 200, monitor.StatusSnapshot(p.Name(), statusInstance(), p.stats.Start(), p))
}

// handleClusterStatus renders GET /status/cluster: every live instance's
// pushed snapshot from Redis (status:<role>:<instance>, TTL-expired entries
// drop out on their own) — the gateway-wide view Kannel never had. With no
// Redis (step 3) it is read from the monitoring history (monitor_samples):
// each instance's newest report within three push intervals, the same
// snapshots by the same keys; 503 only when there is no history either.
func (p *Process) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if p.statusRedis == nil {
		snaps, err := monitor.ClusterFromHistory(r.Context())
		if err != nil {
			writeJSON(w, 503, map[string]string{"error": "CLUSTER_VIEW_UNAVAILABLE: " + err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"instances": snaps, "count": len(snaps), "source": "history"})
		return
	}
	snaps, err := monitor.ClusterStatus(r.Context(), p.statusRedis)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": "REDIS_READ_FAILED: " + err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"instances": snaps, "count": len(snaps)})
}

// statusInstance mirrors cmd/yakugate's instance naming (host-pid) so the
// API-port /status and the pushed cluster snapshots agree on identity.
func statusInstance() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}

// ready is THE readiness signal, shared by /readyz and the submit 503 gate:
// reference data has loaded successfully at least once AND at least one
// account exists. A load that succeeded but returned zero accounts is NOT
// ready — an engine that can authenticate nobody must fail closed with 503,
// not hand out misleading 401s. [SCOPE P1-READY]
//
// AND the prices have been read at least once (2026-10-12, go-live test P1
// E4). With Redis the reference data comes from the copy there, so a role
// restarted while the config store was down was "ready" with no rate cards,
// and took sends it could not price.
func (p *Process) ready() bool {
	if !p.ref.Ready() {
		return false
	}
	if !p.rater.Loaded() {
		return false
	}
	ac, _, _ := p.ref.Counts()
	return ac > 0
}

// Run starts the HTTP server and blocks until the context is cancelled. [SCOPE P1-00]
// (This makes Process a worker.Runnable — a server-style process.)
func (p *Process) Run(ctx context.Context) error {
	log := logging.New(p.Name())

	// [SCOPE P1-BOOT] self-provision &
	// if missing. No-op once everything exists. Disable via YAKU_AUTO_MIGRATE=false.
	if err := bootstrap.Ensure(ctx, p.cfg); err != nil {
		log.Error(logging.SystemScope, "bootstrap", "bootstrap failed", logging.Fields{"error": err.Error()})
	}

	// Cache topology depends on the Redis gate (PO decision 2026-07-24):
	//
	//   GATED:   MySQL --(elected syncer, 30s)--> ref:snapshot + hlr:* in Redis
	//            Redis --(version watch, 3s)--> in-memory refdata.Mem
	//            HLR reads go DIRECT to Redis per lookup (no in-memory HLR).
	//   UNGATED: the original topology — in-memory caches loaded straight
	//            from MySQL every 5 minutes (YAKU_CACHE_REFRESH_SEC).
	//
	// POST /admin/cache/refresh drives BOTH layers when gated: force-sync
	// MySQL->Redis, reload local memory; every other instance converges within
	// one watch interval via the ref:version bump.
	if p.sync != nil {
		if err := p.sync.SyncRef(ctx); err != nil {
			log.Error(logging.SystemScope, "refdata_sync", "initial redis reference sync failed", logging.Fields{"error": err.Error()})
		}
		if err := p.sync.SyncHLR(ctx); err != nil {
			log.Error(logging.SystemScope, "hlr_sync", "initial redis HLR sync failed", logging.Fields{"error": err.Error()})
		}
		syncEvery := envSeconds("YAKU_REDIS_SYNC_SEC", 30)
		watchEvery := envSeconds("YAKU_REDIS_WATCH_SEC", 3)
		go p.sync.Start(ctx, syncEvery)
		go p.ref.StartRedisWatch(ctx, p.redisLoader, watchEvery, log.Printf)
		log.Info(logging.SystemScope, "refdata_mode", "reference data via redis snapshot", logging.Fields{"mysqlToRedis": syncEvery.String(), "redisToMemory": watchEvery.String()})
	}

	if err := p.ref.RefreshForce(ctx); err != nil {
		log.Error(logging.SystemScope, "refdata_refresh", "reference cache refresh failed", logging.Fields{"error": err.Error()})
	}
	ac, tk, rt := p.ref.Counts()
	log.Info(logging.SystemScope, "refdata_loaded", "reference cache loaded", logging.Fields{"accounts": ac, "tokens": tk, "routes": rt})

	// Cost rater (SCOPE P1-COST): reads rate_cards/rate_card_rates/routes
	// straight from the config store, independent of the Redis reference-
	// snapshot gate above — same refresh policy (YAKU_CACHE_REFRESH_SEC) as
	// the reference cache, just a separate ticker since Rater isn't a
	// refdata.Mem. nil rater (config store unreachable at startup) simply
	// means the cost/balance fields stay omitted from every response.
	if p.rater != nil {
		if err := p.rater.Reload(ctx); err != nil {
			log.Error(logging.SystemScope, "rater_reload", "cost rater initial reload failed", logging.Fields{"error": err.Error()})
		}
		// Prices follow the reference data (QA BILL-001): every reload of it
		// — fingerprint moved, the Admin API's nudge, a new Redis snapshot —
		// reloads the rate cards too when they changed. The timer below is
		// only a backstop now.
		p.ref.OnReload(p.rater.FollowReferenceData(log.Printf))
		// Not read yet (the config store was down at start): try every two
		// seconds — the role is not ready until it has them.
		go p.rater.KeepTrying(ctx, 2*time.Second, log.Printf)
		raterRefresh := envSeconds("YAKU_CACHE_REFRESH_SEC", 300)
		go func() {
			t := time.NewTicker(raterRefresh)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := p.rater.Reload(ctx); err != nil {
						log.Error(logging.SystemScope, "rater_reload", "cost rater reload failed", logging.Fields{"error": err.Error()})
					}
				}
			}
		}()
	}

	// Routing rules review (PO 2026-09-14): vendor health every few seconds,
	// quality counts every 30s, and the mover for messages waiting on a down
	// vendor. Each is a no-op when its dependency is missing.
	go p.health.Run(ctx, log.Printf)
	go p.queueOK.Run(ctx) // is the queue store answering, for /readyz
	go p.ref.StartQualityRefresh(ctx, 30*time.Second, log.Printf)
	go p.mover.Run(ctx)

	if p.sync == nil {
		// The reference cache (accounts/routes/blocklist/rates) looks for
		// changes every 30 s — a look that finds none costs one cheap query
		// per table (QA SMPP-004: at 5 minutes a number a vendor's code had
		// just blocked kept being sent). The HLR in-memory reference keeps its
		// 5-minute delta (PO decision 2026-07-17). POST /admin/cache/refresh
		// syncs everything now; YAKU_CACHE_REFRESH_SEC overrides both.
		go p.ref.StartRefresh(ctx, refdata.RefreshEvery(), log.Printf)
		refresh := 5 * time.Minute
		if n, err := strconv.Atoi(env("YAKU_CACHE_REFRESH_SEC", "300")); err == nil && n > 0 {
			refresh = time.Duration(n) * time.Second
		}
		if p.hlrCache != nil {
			// prepare shards, precheck memory vs records, full load, delta loop
			p.hlrCache.Start(ctx, refresh)
		}
	}

	// Packet capture (P13 Type 4): the runner, and the 24-hour deletion sweep.
	p.startPcap(ctx)

	// Admin endpoints carry their OWN credential (PO 2026-09-08) — see
	// admin_gate.go for why it is separate from the read-only status key, and
	// why it is not accepted in the query string.
	admin := adminAuth{
		token: env("YAKU_ADMIN_KEY", ""),
		user:  env("YAKU_ADMIN_USER", ""),
		pass:  env("YAKU_ADMIN_PASSWORD", ""),
	}
	if !admin.configured() {
		p.log.Warn(logging.SystemScope, "config",
			"no admin credential configured — every /admin/* endpoint is DISABLED. Set YAKU_ADMIN_KEY (or YAKU_ADMIN_USER + YAKU_ADMIN_PASSWORD) to enable them",
			nil)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/sms/v4/send", p.handleSend)
	mux.HandleFunc("/sms/v4/operator", p.handleOperator)
	mux.HandleFunc("/messages/", p.handleStatus)
	mux.HandleFunc("/admin/cache/refresh", p.audited("config.cache_refresh", "cache", adminGate(admin, p.handleCacheRefresh)))         // hook: refresh ALL caches now
	mux.HandleFunc("/admin/balance/credit", p.audited("config.balance_credit", "balance", adminGate(admin, p.handleBalanceCredit)))    // a manual top-up applied to the live Redis counter, under the caller's row lock
	mux.HandleFunc("/admin/balance/refresh", p.audited("config.balance_refresh", "balance", adminGate(admin, p.handleBalanceRefresh))) // redis gate: re-seed live balance after a top-up
	// Catch-all so a per-account custom send path (e.g. /api/v1/send,
	// PO 2026-07-22) actually reaches handleSend instead of Go's bare-text
	// default 404. The exact registrations above still win — ServeMux always
	// prefers the most specific match — so this only ever receives requests
	// for paths not otherwise claimed. handleSend itself rejects anything that
	// isn't the native path or a registered custom path (internal/refdata
	// Mem.KnownSendPath), so this is not an open catch-all in practice.
	// [SCOPE P1-CUSTOMPATH]
	mux.HandleFunc("/", p.handleSend)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { // liveness: 200 while the process serves
		writeJSON(w, 200, map[string]string{"status": "ok", "role": p.Name()})
	})
	// Readiness: 200 only once reference data has loaded successfully — the
	// same signal that gates submit with 503 SERVICE_UNAVAILABLE. Self-healing:
	// if the config store was down at startup, the first successful background
	// refresh flips this to ready. An account table that is genuinely empty
	// stays not-ready (fail-closed — an engine that can authenticate nobody
	// must not report ready). [SCOPE P1-READY]
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		ac, tk, rt := p.ref.Counts()
		body := map[string]any{
			"role":    p.Name(),
			"refdata": map[string]int{"accounts": ac, "tokens": tk, "routes": rt},
		}
		if why := p.queueOK.Why(); why != "" {
			body["queue_store"] = "not answering: " + why
		}
		if p.readyForTraffic() {
			body["status"] = "ready"
			writeJSON(w, 200, body)
			return
		}
		body["status"] = "not ready"
		writeJSON(w, 503, body)
	})
	// Monitoring: GET /status — the Kannel-status equivalent in JSON
	// (uptime, totals, TPS triplets, per-account activity). It names customer
	// accounts, so it is DEFAULT-CLOSED: without YAKU_STATUS_KEY configured
	// the endpoint 404s like any unknown path; with it, callers must present
	// the key (header yaku-status-key or ?key=). [SCOPE P1-STATUS]
	statusKey := env("YAKU_STATUS_KEY", "")
	statusGate := func(next func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if statusKey == "" {
				http.NotFound(w, r)
				return
			}
			if r.Header.Get("yaku-status-key") != statusKey && r.URL.Query().Get("key") != statusKey {
				writeJSON(w, 401, map[string]string{"error": "AUTH_FAILED"})
				return
			}
			next(w, r)
		}
	}
	mux.HandleFunc("/status", statusGate(func(w http.ResponseWriter, _ *http.Request) {
		p.handleStatusPage(w)
	}))
	// Cluster view: every role pushes its snapshot to Redis (cmd/yakugate);
	// this endpoint reads them all — the whole gateway from one URL.
	mux.HandleFunc("/status/cluster", statusGate(p.handleClusterStatus))
	// Portal-view decrypt (eocean-multi-tenant-kms-architecture.md's third
	// integration point, alongside encrypt-at-submit and decrypt-at-vendor-
	// pickup): the Node Admin API has no KEK access of its own by design —
	// it calls this endpoint with an already-fetched stored value instead,
	// keeping every KMS credential confined to the Go process. Reuses the
	// SAME status-key gate as /status: default-closed, decrypting message
	// content is at least as sensitive as the account activity /status
	// already requires a key for. [SCOPE P1-KMS-MULTITENANT]
	mux.HandleFunc("/admin/decrypt", p.audited("data.decrypt", "message", adminGate(admin, p.handleDecrypt)))
	// Batch open for the portal's own reads (PO 2026-09-14: text shown in the
	// portal and in exports is always readable; encryption is for storage
	// only, and reading is NOT audited per read). Same admin gate; the Admin
	// API is the only caller, opening the text of lists, message details and
	// export batches. Deliberately not wrapped in p.audited — one audit_log
	// row per page view or per export batch is exactly the per-read trail the
	// ruling removed. The single-value /admin/decrypt above keeps its row.
	mux.HandleFunc("/admin/decrypt/batch", adminGate(admin, p.handleDecryptBatch))
	// The live campaign window for one text signature (read-only, Redis).
	mux.HandleFunc("/admin/campaign", adminGate(admin, p.handleCampaign))
	// Every vendor's state as routing sees it (read-only, memory).
	mux.HandleFunc("/admin/vendors/health", adminGate(admin, p.handleVendorHealth))
	// How many messages wait for one vendor — asked by the Admin API before a
	// vendor delete (2026-09-29). Read-only, so not audited.
	mux.HandleFunc("/admin/queue/depth", adminGate(admin, p.handleQueueDepth))
	// Live on the Monitoring page: every role reports now (Saleem 2026-10-05).
	mux.HandleFunc("/admin/monitor/report-now", adminGate(admin, p.handleReportNow))
	// Dialect onboarding dry-run [chunk-3 ruling 3.6, D-092]: validates a
	// dialect document and previews request mapping + rendered responses via
	// the REAL mapper/renderer, without touching any account. Same
	// default-closed gate — it is an operator tool, called by the portal.
	mux.HandleFunc("/admin/dialect/dryrun", p.audited("config.dialect_dryrun", "dialect", adminGate(admin, p.handleDialectDryrun)))
	mux.HandleFunc("/admin/unblock", p.audited("security.unblock", "lockout", adminGate(admin, p.handleUnblock))) // manual unblock (P1 scope 2026-08-09 §7)
	mux.HandleFunc("/admin/blocks", adminGate(admin, p.handleBlocks))                                             // who is locked right now (read only, so not audited)
	// Regex authority [GW-34]: the Admin API asks THIS binary whether a rule
	// pattern compiles, instead of guessing with JavaScript's different regex
	// language. Same operator-tool gate.
	mux.HandleFunc("/admin/regex/check", p.audited("config.regex_check", "regex", adminGate(admin, p.handleRegexCheck)))
	// Tools (PO 2026-09-12). The "Will this send?" and price checks run the
	// real pipeline in dry-run mode — nothing sent, queued, charged or
	// counted. The webhook tester sends one sample receipt or MO to a URL,
	// built by the same code the dlr and mo roles use. Same operator-tool gate.
	mux.HandleFunc("/admin/send/check", p.audited("tools.send_check", "message", adminGate(admin, p.handleSendCheck)))
	mux.HandleFunc("/admin/webhook/test", p.audited("tools.webhook_test", "webhook", adminGate(admin, p.handleWebhookTest)))
	// An inbound door's "Try it" (config/0125): read a pasted sample with the
	// real callback reader. Reads nothing stored, changes nothing — gated,
	// not audited, like the regex check it sits beside.
	mux.HandleFunc("/admin/callback/try", adminGate(admin, p.handleCallbackTry))
	// KMS "Test connection" (PO 2026-09-13): lock and unlock a throwaway key
	// through a company's master key. Stores nothing. Audited: it reaches out
	// to a key service with the gateway's credentials.
	mux.HandleFunc("/admin/kms/test", p.audited("security.kms_test", "kms", adminGate(admin, p.handleKMSTest)))
	// Packet capture of one listener port (P13 Type 4, PO 2026-09-17):
	// operators only, 1/5/15 minutes, sealed with the gateway key, deleted
	// after 24 hours. See admin_pcap.go. The listener list is read-only and
	// polled by the screen, so it is gated but not audited.
	mux.HandleFunc("/admin/pcap/listeners", adminGate(admin, p.handlePcapListeners))
	mux.HandleFunc("/admin/pcap/start", p.audited("pcap.start", "log_captures", adminGate(admin, p.handlePcapStart)))
	mux.HandleFunc("/admin/pcap/download", p.audited("pcap.download", "log_captures", adminGate(admin, p.handlePcapDownload)))
	mux.HandleFunc("/admin/pcap/delete", p.audited("pcap.delete", "log_captures", adminGate(admin, p.handlePcapDelete)))
	// Checking the trail is itself an admin action, so it is audited too —
	// and lands in the very chain it just verified.
	mux.HandleFunc("/admin/audit/verify", p.audited("security.audit_verify", "audit", adminGate(admin, p.handleAuditVerify)))
	// Prometheus scrape (P1 scope finalize 2026-08-09).
	// /metrics is admin too (PO 2026-09-08): it publishes volume, reject
	// reasons, queue depth and latency on the customer-facing port, so it goes
	// behind the same credential as the rest of /admin.
	//
	// Gated but deliberately NOT audited: a scraper hits this every few
	// seconds, and one audit_log row per scrape would bury the handful of rows
	// that matter — an operator decrypting a message body, or someone probing
	// with a wrong key — under thousands of routine reads a day.
	mux.HandleFunc("/metrics", adminGate(admin, p.handleMetrics))

	// CORS wrap (P1 scope finalize 2026-08-09): browser clients are expected.
	// nil policy (YAKU_CORS_ORIGINS unset) leaves the mux untouched.
	var handler http.Handler = mux
	if cors := newCORSPolicy(env("YAKU_CORS_ORIGINS", "")); cors != nil {
		handler = cors.wrap(mux)
	}
	srv := &http.Server{
		Addr:              p.addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	// TLS listener (PO 2026-09-08), opt-in by cert+key path — see tls.go.
	// Built before either server starts so a bad certificate is a clear
	// start-up error rather than a listener that quietly never appears.
	tlsSet := readTLSSettings("API")
	var tlsSrv *http.Server
	if tlsSet.enabled() {
		cfg, notAfter, err := tlsConfigFor(tlsSet)
		if err != nil {
			log.Error(logging.SystemScope, "listener", "TLS cert load failed; TLS listener disabled",
				logging.Fields{"error": err.Error(), "cert": tlsSet.cert})
		} else {
			tlsSrv = &http.Server{
				Addr:              tlsSet.addr,
				Handler:           handler,
				ReadHeaderTimeout: 10 * time.Second,
				TLSConfig:         cfg,
			}
			f := logging.Fields{"addr": tlsSet.addr, "minVersion": "TLS1.2", "cert": tlsSet.cert,
				"clientCertRequired": tlsSet.clientCA != ""}
			if tlsSet.clientCA != "" {
				f["clientCA"] = tlsSet.clientCA
			}
			if !notAfter.IsZero() {
				// Renewal means a restart (ruled), so say when that is due.
				f["certExpires"] = notAfter.UTC().Format(time.RFC3339)
				f["certExpiresInDays"] = int(time.Until(notAfter).Hours() / 24)
			}
			log.Info(logging.SystemScope, "listener", "listening for HTTPS", f)
		}
	}

	// On a stop the listeners close at once and the calls already being
	// answered are given drainFor to finish. Run must WAIT for that — see the
	// end of Run.
	drained := drainOnStop(ctx, drainFor, srv, tlsSrv)

	// Say plainly what is listening and how — an operator should never have to
	// guess whether a port is encrypted (PO 2026-09-08: "add logs that which
	// ports and servers are running").
	log.Info(logging.SystemScope, "listener", "listening for HTTP", logging.Fields{"addr": p.addr, "tls": false})
	if !tlsSet.enabled() {
		reason := "no certificate configured (YAKU_API_TLS_CERT/KEY)"
		switch {
		case tlsSet.off:
			reason = "disabled by YAKU_API_TLS=off"
		case tlsSet.addr == "":
			reason = "no listen address configured (YAKU_API_TLS_ADDR)"
		}
		log.Warn(logging.SystemScope, "listener",
			"HTTPS is NOT enabled — client credentials, the admin key and message bodies cross this port in the clear",
			logging.Fields{"reason": reason})
	}

	if tlsSrv != nil {
		errc := make(chan error, 1)
		go func() {
			// Paths are already in TLSConfig; ListenAndServeTLS("","") uses it.
			if err := tlsSrv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				errc <- err
			}
		}()
		select {
		case err := <-errc:
			return err
		default:
		}
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	// ListenAndServe returns the moment Shutdown is CALLED, not when it is
	// done. Run used to return here, the process ended a tenth of a second
	// after the stop signal, and every call still being answered was cut
	// (go-live test P1 E1, 2026-10-12: "a TERM ends the role in 0.1 s and
	// cuts the calls under way", 4 calls in 3 restarts under 8 clients). The
	// caller of a cut call cannot know whether its message was taken.
	if ctx.Err() != nil {
		select {
		case <-drained:
		case <-time.After(drainFor + time.Second):
		}
	}
	return nil
}

// drainOnStop shuts the servers down when ctx ends — no new calls, and the
// ones being answered get up to `wait` to finish (http.Server.Shutdown) — and
// returns a channel that closes when that is over. Nil servers are skipped.
func drainOnStop(ctx context.Context, wait time.Duration, servers ...*http.Server) <-chan struct{} {
	drained := make(chan struct{})
	go func() {
		<-ctx.Done()
		defer close(drained)
		sctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		var wg sync.WaitGroup
		for _, sv := range servers {
			if sv == nil {
				continue
			}
			wg.Add(1)
			go func(sv *http.Server) {
				defer wg.Done()
				_ = sv.Shutdown(sctx)
			}(sv)
		}
		wg.Wait()
	}()
	return drained
}

// drainFor is how long calls already being answered get to finish when the
// role is stopped. Well inside the supervisor's wait before it kills a role
// (20 s), and above the longest ordinary call (500 numbers in one request
// took 3.5 s on a laptop).
const drainFor = 8 * time.Second
