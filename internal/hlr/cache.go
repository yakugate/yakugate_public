package hlr

import (
	"context"
	"sync"
	"time"
)

// approxBytesPerEntry is the rough in-memory cost of one cached Entry (map
// bucket + key + strings). Used only for the startup memory precheck.
const approxBytesPerEntry = 250

// DeltaSource is a Store whose backend can also bulk-load changed rows so the
// Cache can keep an in-memory HLR reference in sync. SQL/Mongo backends go
// through the Cache; Redis backends stay direct-read (fast enough per message)
// and are never wrapped.
type DeltaSource interface {
	Store
	// Prepare upgrades live hlr_<first4> shards to the cache schema
	// (self-updating indexed last_check, is_deleted). Idempotent.
	Prepare(ctx context.Context) error
	// Count returns the total number of HLR rows across all shards.
	Count(ctx context.Context) (int64, error)
	// LoadSince returns entries with last_check >= since across all shards and
	// the maximum last_check seen. A zero since means full load (skips
	// soft-deleted rows; deltas include them so the cache can evict).
	LoadSince(ctx context.Context, since time.Time) ([]Entry, time.Time, error)
}

// Cache is the in-memory HLR reference over a DeltaSource: one full load at
// startup, then delta-only refresh (changed records) every interval, plus an
// on-demand hook (ForceRefresh, exposed via POST /admin/cache/refresh). Before
// caching it checks record count against available memory; if the data set
// does not fit it stays in passthrough mode and every Lookup hits the source.
type Cache struct {
	src  DeltaSource
	logf func(string, ...any)

	mu       sync.RWMutex
	m        map[string]Entry
	lastSync time.Time
	enabled  bool
}

// NewCache wraps a DeltaSource. Call Start to run the precheck + refresh loop.
func NewCache(src DeltaSource, logf func(string, ...any)) *Cache {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Cache{src: src, logf: logf}
}

// Lookup serves from memory when caching is enabled, else falls through to the
// source (passthrough mode). Satisfies hlr.Store.
func (c *Cache) Lookup(msisdn string) (Entry, bool) {
	c.mu.RLock()
	if !c.enabled {
		c.mu.RUnlock()
		return c.src.Lookup(msisdn)
	}
	e, ok := c.m[msisdn]
	c.mu.RUnlock()
	return e, ok
}

// Put inserts or updates one entry in the in-memory cache — used by the HLR
// live dip (P1 scope finalize 2026-08-09) so a freshly dipped number is usable
// on the very NEXT message, not only after the periodic delta refresh. A no-op
// in passthrough mode (caching disabled): there is no map to hold it, and the
// DB persister the dipper also writes to makes it visible on the next load.
func (c *Cache) Put(e Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabled || c.m == nil {
		return
	}
	c.m[e.MSISDN] = e
}

// Start prepares shard schemas, prechecks memory vs record count, performs the
// initial full load, and launches the periodic delta refresh. On any failure
// it logs and leaves the Cache in passthrough mode — never blocks serving.
func (c *Cache) Start(ctx context.Context, interval time.Duration) {
	if err := c.src.Prepare(ctx); err != nil {
		c.logf("hlr cache: shard prepare failed: %v — passthrough mode", err)
		return
	}
	n, err := c.src.Count(ctx)
	if err != nil {
		c.logf("hlr cache: count failed: %v — passthrough mode", err)
		return
	}
	need := uint64(n) * approxBytesPerEntry
	avail := availableMemoryBytes()
	if avail > 0 && need > avail/2 {
		c.logf("hlr cache: precheck REFUSED — records=%d need~%dMB but only %dMB available — passthrough mode",
			n, need>>20, avail>>20)
		return
	}
	c.logf("hlr cache: precheck ok — records=%d est=%dMB available=%dMB", n, need>>20, avail>>20)

	applied, err := c.sync(ctx)
	if err != nil {
		c.logf("hlr cache: initial load failed: %v — passthrough mode", err)
		return
	}
	c.mu.Lock()
	c.enabled = true
	c.mu.Unlock()
	c.logf("hlr cache: loaded %d entries; delta refresh every %s (hook: POST /admin/cache/refresh)", applied, interval)

	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n, err := c.sync(ctx); err != nil {
					c.logf("hlr cache: delta refresh failed: %v (serving previous snapshot)", err)
				} else if n > 0 {
					c.logf("hlr cache: delta refresh applied %d change(s)", n)
				}
			}
		}
	}()
}

// ForceRefresh is the admin hook: run a delta sync now instead of waiting for
// the periodic tick. Returns how many changed records were applied.
func (c *Cache) ForceRefresh(ctx context.Context) (int, error) {
	return c.sync(ctx)
}

// Stats reports cache mode, resident entry count, and last sync watermark.
func (c *Cache) Stats() (enabled bool, records int, lastSync time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.enabled, len(c.m), c.lastSync
}

// sync pulls entries changed since the watermark and applies them: soft-deleted
// entries are evicted, everything else upserted. Idempotent (>= boundary rows
// may re-apply).
func (c *Cache) sync(ctx context.Context) (int, error) {
	c.mu.RLock()
	since := c.lastSync
	c.mu.RUnlock()
	entries, maxTS, err := c.src.LoadSince(ctx, since)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	if c.m == nil {
		c.m = make(map[string]Entry, len(entries))
	}
	for _, e := range entries {
		if e.Deleted {
			delete(c.m, e.MSISDN)
		} else {
			c.m[e.MSISDN] = e
		}
	}
	if maxTS.After(c.lastSync) {
		c.lastSync = maxTS
	}
	c.mu.Unlock()
	return len(entries), nil
}
