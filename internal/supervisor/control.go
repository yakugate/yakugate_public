package supervisor

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// DefaultControlAddr is where the supervisor listens when YAKU_CONTROL_ADDR
// is not set: this host only.
const DefaultControlAddr = "127.0.0.1:8099"

// KeyHeader carries YAKU_ADMIN_KEY, as on the engine's own admin endpoints.
const KeyHeader = "yaku-admin-key"

// Handler is the control API over a supervisor. shutdown is called by
// POST /shutdown to end the whole gateway.
func Handler(s *Supervisor, key string, shutdown func()) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/roles", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, 405, map[string]string{"error": "GET only"})
			return
		}
		writeJSON(w, 200, map[string]any{"roles": s.Status(), "at": time.Now().UTC(), "config_file": s.ConfigFile})
	})
	mux.HandleFunc("/roles/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "POST only"})
			return
		}
		parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/roles/"), "/"), "/")
		if len(parts) != 2 {
			writeJSON(w, 404, map[string]string{"error": "use /roles/<role|all>/start|stop|restart"})
			return
		}
		name, action := parts[0], parts[1]
		var err error
		switch {
		case name == "all" && action == "start":
			s.StartAll()
		case name == "all" && action == "stop":
			s.StopAll()
		case name == "all" && action == "restart":
			s.RestartAll()
		case action == "start":
			err = s.Start(name)
		case action == "stop":
			err = s.Stop(name)
		case action == "restart":
			err = s.Restart(name)
		default:
			writeJSON(w, 404, map[string]string{"error": "the action is start, stop or restart"})
			return
		}
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"roles": s.Status()})
	})
	mux.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]string{"error": "POST only"})
			return
		}
		writeJSON(w, 202, map[string]string{"status": "stopping every role, then the gateway"})
		if shutdown != nil {
			go shutdown()
		}
	})
	return guard(key, mux)
}

// guard checks the admin key when one is set. Without one, only the bind
// address protects the port — which is why the default is 127.0.0.1.
func guard(key string, next http.Handler) http.Handler {
	// HTTP trims a header value's ends on both sides, so a key written with a
	// space at either end ("abc " quoted in yakugate.config) could never match:
	// every caller, the console included, would be refused (2026-10-01).
	key = strings.TrimSpace(key)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if key != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get(KeyHeader)), []byte(key)) != 1 {
			writeJSON(w, 401, map[string]string{"error": "the admin key is missing or wrong"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Listen opens the control port. It fails when the port is taken, which is
// how a second `yakugate` on the same host learns one is already running.
func Listen(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("control port %s: %w — is a gateway already running on this host? (the console's Gateway menu controls it)", addr, err)
	}
	return ln, nil
}

// Serve answers control requests until ctx ends.
func Serve(ctx context.Context, ln net.Listener, h http.Handler) error {
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
