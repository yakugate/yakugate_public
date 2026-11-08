package monitor

import (
	"sort"
	"sync"
	"time"
)

// Two shapes shared by the receipt role (P5) and the MO role (P6), which do
// the same job for different traffic: a vendor calls IN with something, and we
// push it OUT to the customer's endpoint with retries. One vocabulary for both
// so an operator reads the two panels the same way.

// Inbound counts callbacks vendors make to us, per vendor and per outcome.
//
// The outcome is the point. "Receipts received: 10,000" says nothing; "8,000
// applied, 2,000 unmatched" says a vendor is sending ids we never issued, and
// "refused" says someone is posting to us without the vendor's secret.
type Inbound struct {
	mu       sync.Mutex
	byVendor map[string]*inboundVendor
}

type inboundVendor struct {
	counts map[string]int64
	last   time.Time
}

// Note records one callback from vendor with the given outcome.
func (in *Inbound) Note(vendor, outcome string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.byVendor == nil {
		in.byVendor = map[string]*inboundVendor{}
	}
	v := in.byVendor[vendor]
	if v == nil {
		v = &inboundVendor{counts: map[string]int64{}}
		in.byVendor[vendor] = v
	}
	v.counts[outcome]++
	v.last = time.Now()
}

// Section lists each vendor's outcome counts, a total, and when it last
// called. A vendor absent from the list has not called since the role started.
func (in *Inbound) Section() []map[string]any {
	in.mu.Lock()
	defer in.mu.Unlock()
	names := make([]string, 0, len(in.byVendor))
	for n := range in.byVendor {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]map[string]any, 0, len(names))
	now := time.Now()
	for _, n := range names {
		v := in.byVendor[n]
		var total int64
		counts := make(map[string]int64, len(v.counts))
		for k, c := range v.counts {
			counts[k] = c
			total += c
		}
		out = append(out, map[string]any{
			"vendor":   n,
			"total":    total,
			"outcomes": counts,
			"last_at":  v.last.UTC().Format(time.RFC3339),
			"state":    string(State(v.last, now)),
		})
	}
	return out
}

// Delivery counts pushes to customer endpoints (dlr_url, mo_url).
//
// Failed ATTEMPTS and given-up MESSAGES are kept apart on purpose: a
// customer's endpoint blipping for a minute produces failed attempts that all
// succeed on retry, which is healthy; given-up messages are receipts or MO the
// customer will never see, which is a loss.
type Delivery struct {
	Delivered Counter
	Failed    Counter
	GaveUp    Counter
	// Latency is the customer endpoint's response time. A slow customer
	// endpoint is the commonest reason this leg falls behind, and it is not
	// our fault — which is exactly why it has to be visible.
	Latency Latency

	mu        sync.Mutex
	lastErr   string
	lastErrAt time.Time
	lastOK    time.Time
}

// NoteOK records one successful push and the endpoint's response time.
func (d *Delivery) NoteOK(took time.Duration) {
	d.Delivered.Add(1)
	d.Latency.Add(took)
	d.mu.Lock()
	d.lastOK = time.Now()
	d.mu.Unlock()
}

// NoteFailed records one failed attempt; the message may still be retried.
func (d *Delivery) NoteFailed(reason string, took time.Duration) {
	d.Failed.Add(1)
	if took > 0 {
		d.Latency.Add(took)
	}
	if len(reason) > 160 {
		reason = reason[:160] + "…"
	}
	d.mu.Lock()
	d.lastErr, d.lastErrAt = reason, time.Now()
	d.mu.Unlock()
}

// NoteGaveUp records n messages abandoned for good.
func (d *Delivery) NoteGaveUp(n int64) {
	if n > 0 {
		d.GaveUp.Add(n)
	}
}

// Section is the push leg's snapshot block.
func (d *Delivery) Section() map[string]any {
	out := map[string]any{
		"delivered": d.Delivered.Snapshot(0),
		"failed":    d.Failed.Snapshot(0),
		"gave_up":   d.GaveUp.Snapshot(0),
	}
	lat := map[string]any{"known": false}
	if p95, ok := d.Latency.Quantile(0.95); ok {
		p50, _ := d.Latency.Quantile(0.50)
		lat = map[string]any{"known": true, "p50_ms": p50 * 1000, "p95_ms": p95 * 1000, "samples": d.Latency.Len()}
	}
	out["endpoint_latency"] = lat
	d.mu.Lock()
	if !d.lastOK.IsZero() {
		out["last_delivered_at"] = d.lastOK.UTC().Format(time.RFC3339)
	}
	if d.lastErr != "" {
		out["last_error"] = d.lastErr
		out["last_error_at"] = d.lastErrAt.UTC().Format(time.RFC3339)
	}
	d.mu.Unlock()
	return out
}
