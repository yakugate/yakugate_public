// Package vendorquality counts how well each vendor delivers to each network,
// for route_type='quality' (PO 2026-09-14, routing rules review).
//
// The processes that see a message's verdict — the receipt path (smpp-vendor,
// http-vendor, dlr), a vendor refusing a message outright, retry's dead-letter
// and expiry — call Note. Counts build up in memory per minute and are added to
// a shared store every few seconds: Redis where Redis runs, else a small
// per-minute table in the hot store (hot/0036). The sending processes read the
// last hour back and route by it.
//
// It replaces a SQL count over record_index that every sending process ran
// every 30 seconds (PO rule: live figures come from what the engine counts,
// not from queries on operational tables), and it adds the network: a vendor
// that delivers well to one network and badly to another was one blended
// number before.
package vendorquality

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/netcode"
)

// Key is one vendor to one network. MNC is normalised ("03", never "003"),
// so every writer and reader names a network the same way.
type Key struct {
	VendorID int64
	MCC, MNC string
}

// KeyFor builds the Key for a message's vendor and network.
func KeyFor(vendorID int64, mcc, mnc string) Key {
	return Key{VendorID: vendorID, MCC: mcc, MNC: netcode.Norm(mnc)}
}

// Counts is a vendor's record to one network: how many messages got a verdict
// and how many of those were delivered. Counts rather than a ratio, so a reader
// can tell how MUCH evidence there is as well as how good it looks.
type Counts struct {
	Delivered int64
	// Judged is every verdict: delivered + failed + expired. A message still
	// waiting has no verdict and is not counted (PO 2026-09-04: failed and
	// expired both count against a vendor).
	Judged int64
}

// Ratio is the delivered share, 0..1. Nothing judged reads as 1: a vendor with
// no record yet is treated as good, so it gets traffic and can earn one.
func (c Counts) Ratio() float64 {
	if c.Judged <= 0 {
		return 1
	}
	return float64(c.Delivered) / float64(c.Judged)
}

// Bucket is one minute's counts for one Key — the unit written to a store.
type Bucket struct {
	Minute time.Time // UTC, truncated to the minute
	Key
	Counts
}

// Sink is where a counting process adds its buckets.
type Sink interface {
	Add(ctx context.Context, bs []Bucket) error
}

// Source is where a routing process reads the window back.
type Source interface {
	Load(ctx context.Context, window time.Duration) (map[Key]Counts, error)
}

// FlushEvery is how often counts leave memory for the shared store.
const FlushEvery = 10 * time.Second

// Counter keeps one process's counts until they are flushed. Note is a no-op
// until Start: a process that never wires a store must not grow a map forever.
type Counter struct {
	on      atomic.Bool
	mu      sync.Mutex
	pending map[minuteKey]*Counts
	now     func() time.Time
}

type minuteKey struct {
	minute int64 // unix seconds of the minute's start
	Key
}

// New returns a stopped counter.
func New() *Counter {
	return &Counter{pending: map[minuteKey]*Counts{}, now: time.Now}
}

// Default is the process's counter: roles call Note on it and start it once.
var Default = New()

// Note records one verdict: "delivered", "failed" or "expired"; anything else
// is ignored. vendorID 0 (no vendor ever took the message) is ignored too —
// there is no vendor to judge.
func Note(vendorID int64, mcc, mnc, status string) { Default.Note(vendorID, mcc, mnc, status) }

// Note records one verdict on this counter.
func (c *Counter) Note(vendorID int64, mcc, mnc, status string) {
	if !c.on.Load() || vendorID <= 0 {
		return
	}
	var d int64
	switch status {
	case "delivered":
		d = 1
	case "failed", "expired":
	default:
		return
	}
	now := c.now().UTC()
	k := minuteKey{minute: now.Unix() - now.Unix()%60, Key: KeyFor(vendorID, mcc, mnc)}
	c.mu.Lock()
	n := c.pending[k]
	if n == nil {
		n = &Counts{}
		c.pending[k] = n
	}
	n.Delivered += d
	n.Judged++
	c.mu.Unlock()
}

// Start turns counting on and flushes to sink every FlushEvery until ctx ends
// (one last flush then). A nil sink leaves the counter off.
func (c *Counter) Start(ctx context.Context, sink Sink, logf func(string, ...any)) {
	if sink == nil {
		return
	}
	c.on.Store(true)
	go func() {
		t := time.NewTicker(FlushEvery)
		defer t.Stop()
		var lastErr time.Time
		for {
			select {
			case <-ctx.Done():
				fc, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_ = c.Flush(fc, sink)
				cancel()
				return
			case <-t.C:
			}
			fc, cancel := context.WithTimeout(ctx, FlushEvery)
			err := c.Flush(fc, sink)
			cancel()
			if err != nil && logf != nil && time.Since(lastErr) >= time.Minute {
				lastErr = time.Now()
				logf("vendor quality counts not written (kept for the next try): %v", err)
			}
		}
	}()
}

// keepFailed is how old unwritten counts may get before they are dropped: past
// the reading window they no longer change any routing decision.
const keepFailed = time.Hour

// Flush writes everything pending. On failure the counts go back into memory
// for the next try, unless they are older than an hour.
func (c *Counter) Flush(ctx context.Context, sink Sink) error {
	c.mu.Lock()
	batch := c.pending
	c.pending = map[minuteKey]*Counts{}
	c.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}
	bs := make([]Bucket, 0, len(batch))
	for k, n := range batch {
		bs = append(bs, Bucket{Minute: time.Unix(k.minute, 0).UTC(), Key: k.Key, Counts: *n})
	}
	err := sink.Add(ctx, bs)
	if err == nil {
		return nil
	}
	cutoff := c.now().UTC().Add(-keepFailed).Unix()
	c.mu.Lock()
	for k, n := range batch {
		if k.minute < cutoff {
			continue
		}
		cur := c.pending[k]
		if cur == nil {
			cur = &Counts{}
			c.pending[k] = cur
		}
		cur.Delivered += n.Delivered
		cur.Judged += n.Judged
	}
	c.mu.Unlock()
	return err
}
