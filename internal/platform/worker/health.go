package worker

import (
	"context"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/platform/logging"
)

// Readier is optional: a role that knows its own readiness (bound, cache
// loaded, …) answers /readyz from it. Without it, a worker is ready after
// its first step finished without error, and a server as soon as it runs.
type Readier interface{ Ready() bool }

// HealthAddrEnv names the listener address; unset = no listener. One address
// per role process (each role is its own process), e.g. ":9101".
const HealthAddrEnv = "YAKU_HEALTH_ADDR"

// AdminAddrEnv is the per-process admin listener (monitor.ServeAdmin, started
// by cmd/yakugate), which serves /healthz and /readyz as well as /status.
const AdminAddrEnv = "YAKU_ADMIN_ADDR"

// processReady is this process's "can do its job" flag, set by Run. Package
// level because a process runs ONE role, and the admin listener — started
// outside Run — must answer /readyz by the same rule (bug 16, 2026-09-21).
var processReady atomic.Bool

// Ready is the one readiness rule both listeners answer /readyz with: the role
// is running (a worker has finished a step without error; a server has
// started), or says so itself (Readier), AND — when it can tell — it is ready
// for traffic (ReadyForTraffic: bound, cache loaded, …). The two listeners
// used to disagree: one waited for the first step, the other said "ready" at
// once for any role without ReadyForTraffic.
func Ready(w any) bool { return readyWith(w, &processReady) }

// readyWith is Ready against a given step flag (the health server is handed
// its flag, so a test can drive it).
func readyWith(w any, flag *atomic.Bool) bool {
	ok := flag.Load()
	if r, has := w.(Readier); has {
		ok = r.Ready()
	}
	if rt, has := w.(interface{ ReadyForTraffic() bool }); has {
		ok = ok && rt.ReadyForTraffic()
	}
	return ok
}

// serveHealth starts the /healthz + /readyz listener when configured
// (PO 2026-09-17: every worker role gets one). It never blocks the role.
func serveHealth(ctx context.Context, w Worker, ready *atomic.Bool, log *logging.Logger) {
	addr := os.Getenv(HealthAddrEnv)
	if addr == "" {
		return
	}
	// One listener for the same checks (bug 16): when the admin listener is
	// on, it answers /healthz and /readyz by the same rule, so this one is
	// not opened — two ports answering one question only invites a probe
	// pointed at the one that later goes away.
	if admin := os.Getenv(AdminAddrEnv); admin != "" {
		log.Printf("health checks are on %s=%s; %s=%s is not opened", AdminAddrEnv, admin, HealthAddrEnv, addr)
		return
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("health listener %s: %v (no /healthz for this role)", addr, err)
		return
	}
	srv := healthServer(w, ready)
	go func() { _ = srv.Serve(ln) }()
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
	}()
	log.Printf("health at http://%s/healthz and /readyz", ln.Addr())
}

// healthServer is the handler set, separate so tests can drive it without a
// port: /healthz says the process is alive, /readyz says it can do its job.
func healthServer(w Worker, ready *atomic.Bool) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "text/plain")
		_, _ = rw.Write([]byte("ok " + w.Name() + "\n"))
	})
	mux.HandleFunc("/readyz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "text/plain")
		if !readyWith(w, ready) {
			rw.WriteHeader(http.StatusServiceUnavailable)
			_, _ = rw.Write([]byte("not ready " + w.Name() + "\n"))
			return
		}
		_, _ = rw.Write([]byte("ready " + w.Name() + "\n"))
	})
	return &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}
