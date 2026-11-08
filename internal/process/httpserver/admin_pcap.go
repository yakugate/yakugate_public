package httpserver

// Process 13 Type 4 — packet capture of one gateway listener port.
// [SCOPE P13-T4] (PO rulings 2026-09-17; the runner is internal/pcap.)
//
//	GET  /admin/pcap/listeners         the ports an operator may pick, read
//	                                   from settings, and whether this host
//	                                   can capture at all
//	POST /admin/pcap/start             {"id": 12, "listener": "smpp-server",
//	                                    "window_minutes": 5}
//	GET  /admin/pcap/download?id=12    the capture, decrypted on the way out
//	POST /admin/pcap/delete            {"id": 12}
//
// The Admin API is the only caller. It owns the log_captures row (type =
// 'traffic'), the operator-only rule, the required reason and the audit row
// that names the person. This side runs tcpdump on THIS host, seals the file
// with the gateway key as it is produced, reports how the capture ended onto
// the row, and deletes the file 24 hours after the window closed — writing
// that deletion to audit_log itself, since no person is there to do it.
//
// tcpdump sees every port on this host, not only this role's, so the
// http-server can capture the smpp-server, dlr and mo listeners when they run
// on the same host. A listener on another host is out of reach from here.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/audit"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/pcap"
	"github.com/yakugate/yakugate/internal/platform/logging"
)

// pcapSweepEvery is how often old capture files are looked for.
const pcapSweepEvery = time.Minute

func hostName() string {
	h, _ := os.Hostname()
	return h
}

// startPcap builds the capture runner and starts the deletion sweep.
func (p *Process) startPcap(ctx context.Context) {
	maxMB, err := strconv.Atoi(env("YAKU_PCAP_MAX_MB", "256"))
	if err != nil || maxMB < 0 {
		maxMB = 256
	}
	r := &pcap.Runner{
		Dir:      env("YAKU_PCAP_DIR", filepath.Join(p.cfg.LogDir, "pcap")),
		Tcpdump:  env("YAKU_PCAP_TCPDUMP", "tcpdump"),
		Iface:    env("YAKU_PCAP_INTERFACE", "any"),
		MaxBytes: int64(maxMB) << 20,
		Sealer:   p.cry,
		OnDone:   p.pcapDone,
	}
	p.pcap = r

	// A capture that was running when this process last stopped died with it.
	p.pcapExec(ctx, func(c context.Context, rows dal.PacketCaptures) error { return rows.FailRunningOnHost(c, hostName()) })

	go func() {
		t := time.NewTicker(pcapSweepEvery)
		defer t.Stop()
		for {
			p.pcapSweep(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

// pcapExec makes one change to the capture rows (dal.PacketCaptures), with
// five seconds to do it; a change that fails is logged.
func (p *Process) pcapExec(ctx context.Context, change func(context.Context, dal.PacketCaptures) error) {
	if p.traffic == nil {
		return
	}
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := change(c, p.traffic.PacketCaptures()); err != nil {
		p.log.Warn(logging.SystemScope, "pcap", "capture row not updated", logging.Fields{"error": err.Error()})
	}
}

// pcapDone writes how a capture ended onto its row.
func (p *Process) pcapDone(res pcap.Result) {
	ctx := context.Background()
	if res.Err != nil {
		p.pcapExec(ctx, func(c context.Context, rows dal.PacketCaptures) error {
			return rows.Failed(c, res.ID, clip(res.Err.Error(), 500))
		})
		return
	}
	// expires_at moves only BACK (a capture stopped early, or at the size
	// limit); purge_at is left alone, so the file never outlives its 24 hours.
	p.pcapExec(ctx, func(c context.Context, rows dal.PacketCaptures) error {
		return rows.Ended(c, res.ID, res.FileBytes, res.Truncated)
	})
}

// pcapSweep deletes capture files past their 24 hours, marks their rows and
// writes each deletion to audit_log with the reason the capture was taken for.
func (p *Process) pcapSweep(ctx context.Context) {
	if p.pcap == nil {
		return
	}
	for _, id := range p.pcap.Sweep() {
		var listener, reason string
		if p.traffic != nil {
			c, cancel := context.WithTimeout(ctx, 5*time.Second)
			listener, reason, _ = p.traffic.PacketCaptures().Of(c, id)
			cancel()
		}
		p.pcapExec(ctx, func(c context.Context, rows dal.PacketCaptures) error { return rows.Purged(c, id) })
		if p.aud != nil {
			p.aud.Record(audit.Entry{
				Actor: p.Name(), Action: "pcap.delete", Entity: "log_captures",
				EntityID: strconv.FormatInt(id, 10), TargetLabel: "packet capture " + listener,
				Summary: "Deleted packet capture " + listener + " (24 hours passed)",
				Reason:  "automatic: 24 hours after the capture window closed",
				Detail:  map[string]any{"capture_reason": reason, "listener": listener, "host": hostName()},
			})
		}
	}
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func (p *Process) handlePcapListeners(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, 405, map[string]string{"error": "GET only"})
		return
	}
	out := map[string]any{
		"listeners": pcap.Listeners(os.Getenv),
		"windows":   pcap.Windows,
		"host":      hostName(),
		"available": true,
	}
	if err := p.pcapReady(); err != nil {
		out["available"] = false
		out["problem"] = err.Error()
	}
	writeJSON(w, 200, out)
}

func (p *Process) pcapReady() error {
	if p.pcap == nil {
		return errors.New("packet capture is not started on this gateway")
	}
	return p.pcap.Check()
}

func (p *Process) handlePcapStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		ID            int64  `json:"id"`
		Listener      string `json:"listener"`
		WindowMinutes int    `json:"window_minutes"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	l, ok := pcap.Find(pcap.Listeners(os.Getenv), req.Listener)
	switch {
	case req.ID <= 0:
		writeJSON(w, 400, map[string]string{"error": "id is required"})
		return
	case !ok:
		writeJSON(w, 400, map[string]string{"error": "unknown listener: " + req.Listener})
		return
	case !pcap.ValidWindow(req.WindowMinutes):
		writeJSON(w, 400, map[string]string{"error": "window_minutes must be 1, 5 or 15"})
		return
	}
	if err := p.pcapReady(); err != nil {
		writeJSON(w, 503, map[string]string{"error": err.Error()})
		return
	}
	if err := p.pcap.Start(req.ID, l.Port, time.Duration(req.WindowMinutes)*time.Minute); err != nil {
		status := 503
		if errors.Is(err, pcap.ErrBusy) {
			status = 409
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "id": req.ID, "listener": l.Key, "port": l.Port, "host": hostName()})
}

func (p *Process) handlePcapDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, 405, map[string]string{"error": "GET only"})
		return
	}
	id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	if err != nil || id <= 0 || p.pcap == nil {
		writeJSON(w, 400, map[string]string{"error": "id is required"})
		return
	}
	if p.pcap.Running(id) {
		writeJSON(w, 409, map[string]string{"error": pcap.ErrRunning.Error()})
		return
	}
	if _, ok := p.pcap.Size(id); !ok {
		writeJSON(w, 404, map[string]string{"error": pcap.ErrNotFound.Error()})
		return
	}
	first := &firstWrite{w: w, id: id}
	if err := p.pcap.Open(r.Context(), id, first); err != nil {
		if !first.began {
			writeJSON(w, 503, map[string]string{"error": "the capture could not be opened: " + err.Error()})
			return
		}
		// Part of the file has gone out already. Cut the connection so the
		// caller sees a broken download, never a short capture that looks whole.
		panic(http.ErrAbortHandler)
	}
	if !first.began { // an empty capture is still a valid answer
		first.header()
	}
}

// firstWrite sends the download headers only once the first bytes have
// decrypted, so a file that cannot be opened is still answered as JSON.
type firstWrite struct {
	w     http.ResponseWriter
	id    int64
	began bool
}

func (f *firstWrite) header() {
	f.began = true
	f.w.Header().Set("Content-Type", "application/vnd.tcpdump.pcap")
	f.w.Header().Set("Content-Disposition", `attachment; filename="capture_`+strconv.FormatInt(f.id, 10)+`.pcap"`)
	f.w.Header().Set("Cache-Control", "no-store")
	f.w.WriteHeader(200)
}

func (f *firstWrite) Write(b []byte) (int, error) {
	if !f.began {
		f.header()
	}
	return f.w.Write(b)
}

func (p *Process) handlePcapDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil || req.ID <= 0 || p.pcap == nil {
		writeJSON(w, 400, map[string]string{"error": "id is required"})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "deleted": p.pcap.Delete(req.ID)})
}
