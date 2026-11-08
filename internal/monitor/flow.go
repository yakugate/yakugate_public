package monitor

import (
	"sync/atomic"
	"time"
)

// Flow is what a role MOVES, as opposed to what it accepts.
//
// Until 2026-09-11 only the two ingress roles published throughput, through
// Stats (the `sms` section). Every other role published "I am alive" and
// nothing else, so the overview showed the gateway accepting 500/s and nothing
// leaving it — while P3 was sending ~360/s and P5 was pushing receipts.
//
// Four directions, matching the lines on the overview chart (PO 2026-09-11,
// "separate lines"). A gap between In and Out is a queue building, readable at
// a glance, which a single summed line can never show:
//
//	Out       a vendor ACCEPTED the message (submit_sm_resp OK / HTTP success)
//	OutFailed a send attempt that did not go out (refused, unreachable)
//	Receipts  a delivery receipt arrived from a vendor and was applied
//	MO        an inbound message arrived from a vendor
//
// "In" is not here: ingress already counts it in Stats.Accepted, and a second
// counter for the same event would be a second number to disagree with.
//
// Package-level because a process runs exactly one role; a role simply calls
// monitor.Flow.NoteOut(n) where the event happens. Untouched counters are not
// published, so a role that moves nothing adds nothing to its snapshot.
type FlowSet struct {
	Out       Counter
	OutFailed Counter
	Receipts  Counter
	MO        Counter
	// VendorLatency is the vendor's round trip: submit to submit_sm_resp over
	// SMPP, request to response over HTTP. Kept apart from the pipeline
	// latency on purpose — P1's p95 is milliseconds and a saturated SMPP
	// window is seconds, and one column holding both would make the overview's
	// latency line meaningless the moment a vendor slowed down.
	VendorLatency Latency

	used atomic.Bool
	last atomic.Int64 // unix nanos of the newest event, for liveness
}

// Flow is this process's flow counters.
var Flow = &FlowSet{}

func (f *FlowSet) touch() {
	f.used.Store(true)
	f.last.Store(time.Now().UnixNano())
}

// NoteOut records n messages a vendor accepted.
func (f *FlowSet) NoteOut(n int64) { f.Out.Add(n); f.touch() }

// NoteOutFailed records n messages whose send attempt did not go out.
func (f *FlowSet) NoteOutFailed(n int64) { f.OutFailed.Add(n); f.touch() }

// NoteReceipt records n delivery receipts received from a vendor.
func (f *FlowSet) NoteReceipt(n int64) { f.Receipts.Add(n); f.touch() }

// NoteMO records n inbound messages received from a vendor.
func (f *FlowSet) NoteMO(n int64) { f.MO.Add(n); f.touch() }

// NoteVendorLatency records one vendor round trip. It does not count as
// activity on its own: a latency is always recorded beside an Out/OutFailed.
func (f *FlowSet) NoteVendorLatency(d time.Duration) { f.VendorLatency.Add(d) }

// LastActivity is when this role last moved anything (zero if never).
func (f *FlowSet) LastActivity() time.Time {
	n := f.last.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// section is the snapshot's `flow` block, or nil for a role that has moved
// nothing since it started.
func (f *FlowSet) section(uptimeSec float64) map[string]any {
	if !f.used.Load() {
		return nil
	}
	out := map[string]any{
		"out":        f.Out.Snapshot(uptimeSec),
		"out_failed": f.OutFailed.Snapshot(uptimeSec),
		"receipts":   f.Receipts.Snapshot(uptimeSec),
		"mo":         f.MO.Snapshot(uptimeSec),
	}
	lat := map[string]any{"known": false}
	if p95, ok := f.VendorLatency.Quantile(0.95); ok {
		p50, _ := f.VendorLatency.Quantile(0.50)
		lat = map[string]any{
			"known":   true,
			"p50_ms":  p50 * 1000,
			"p95_ms":  p95 * 1000,
			"samples": f.VendorLatency.Len(),
		}
	}
	out["vendor_latency"] = lat
	return out
}
