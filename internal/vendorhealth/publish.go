package vendorhealth

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
)

// ReportEvery is how often a vendor role writes its view to vendor_health.
const ReportEvery = 5 * time.Second

// Publisher writes one process's Tracker to vendor_health (config store).
//
// Best effort, like monitoring: a failed write is logged and tried again next
// tick, never an error for the role. If a process cannot write for long
// enough, its rows go stale and readers ignore them — which reads as "no
// news", the same as before this existed.
type Publisher struct {
	db       dal.VendorHealth
	role     string
	instance string
	t        *Tracker
	logf     func(string, ...any)
	now      func() time.Time
}

// NewPublisher wires a tracker to the config store (dal.VendorHealth). db
// nil = a no-op.
func NewPublisher(db dal.VendorHealth, role, instance string, t *Tracker, logf func(string, ...any)) *Publisher {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	// The instance is filed with the deployment it runs in ("host-pid@lhr-1"),
	// so a routing process can read the reports of its own deployment
	// (2026-10-12, view.go assessFor).
	return &Publisher{db: db, role: role, instance: WithDeployment(instance, deployment.ID()), t: t, logf: logf, now: time.Now}
}

// WithDeployment names a vendor-role instance together with the deployment it
// runs in: "gw1-4242@lhr-1". A gateway that names no deployment keeps the
// plain name. It rides the instance column — the table has no column of its
// own for it, and a name is all a reader needs.
func WithDeployment(instance, dep string) string {
	if dep == "" || strings.Contains(instance, "@") {
		return instance
	}
	return instance + "@" + dep
}

// deploymentOf reads the deployment back from an instance name; "" = the
// instance did not say (a gateway with no deployment, or a role from before
// this was written).
func deploymentOf(instance string) string {
	if i := strings.LastIndex(instance, "@"); i >= 0 {
		return instance[i+1:]
	}
	return ""
}

// Run writes every ReportEvery until ctx ends, then removes this instance's
// rows so a clean stop leaves nothing for readers to age out.
func (p *Publisher) Run(ctx context.Context) {
	if p == nil || p.db == nil {
		return
	}
	// A previous process with the same host-pid (a container restart can
	// reuse a pid) must not leave its rows behind as ours.
	_ = p.db.Forget(ctx, p.instance)
	tick := time.NewTicker(ReportEvery)
	defer tick.Stop()
	var lastErr time.Time
	var lastPrune time.Time
	for {
		c, cancel := context.WithTimeout(ctx, ReportEvery)
		err := p.Write(c)
		cancel()
		if err != nil && ctx.Err() == nil && time.Since(lastErr) >= time.Minute {
			lastErr = time.Now()
			p.logf("vendor health write failed (routing keeps its last view): %v", err)
		}
		if time.Since(lastPrune) >= time.Hour {
			lastPrune = time.Now()
			// Rows from processes that died without cleaning up. Readers
			// already ignore them; this only keeps the table small.
			_ = p.db.Prune(ctx, p.now().UTC().Add(-24*time.Hour))
		}
		select {
		case <-ctx.Done():
			c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = p.db.Forget(c, p.instance)
			cancel()
			return
		case <-tick.C:
		}
	}
}

// Write puts the tracker's current view in vendor_health and drops this
// instance's rows for vendors it no longer serves.
func (p *Publisher) Write(ctx context.Context) error {
	reps := p.t.Snapshot()
	now := p.now().UTC().Truncate(time.Millisecond)
	if len(reps) == 0 {
		return p.db.Forget(ctx, p.instance)
	}
	out := make([]dal.VendorHealthReport, len(reps))
	for i, r := range reps {
		out[i] = dal.VendorHealthReport{VendorID: r.VendorID, FailingSince: r.FailingSince}
		if r.Detail != "" {
			out[i].Detail = trunc(r.Detail, 190)
		}
	}
	// One multi-row upsert, then one DELETE of this instance's other rows.
	return p.db.Report(ctx, p.instance, p.role, now, out)
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	r := []rune(s)
	for len(string(r)) > n {
		r = r[:len(r)-1]
	}
	return string(r)
}

// Instance names this process in vendor_health: host and pid, the same form
// the monitoring status view uses.
func Instance() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}
