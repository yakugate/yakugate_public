// Vendor queue depths and delivery quality: the live figures Least busy and
// quality routing pick by.

package refdata

import (
	"context"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/vendorquality"
)

// QueueDepth is one vendor's live pending-queue depth, for capacity-based
// routing (route_type='capacity', 2026-07-28 routes review). Mirrors
// store.VendorQueueStat's shape without importing internal/store from this
// package.
type QueueDepth struct {
	Depth        int64
	OldestAgeSec int64
}

// VendorQuality is a vendor's recent delivery record to one network, for
// route_type='quality': counts of delivered and judged messages. It is the
// vendorquality package's Counts; the alias keeps the name routing code uses.
type VendorQuality = vendorquality.Counts

// QualitySource is where the cache reads delivery counts from — the Redis or
// hot-store counts the verdict-seeing roles keep (vendorquality, PO
// 2026-09-14). Set by the process with SetQualitySource; without one, quality
// routing has no counts and a quality level keeps list order.
type QualitySource interface {
	Load(ctx context.Context, window time.Duration) (map[vendorquality.Key]vendorquality.Counts, error)
}

// QueueDepthLoader is OPTIONALLY implemented by a Loader to expose live
// per-vendor queue depth — not every queue backend can report this yet (only
// MySQL today, same caveat as store.QueueDepthStats). Mem type-asserts for
// it in Refresh; a Loader that doesn't implement it simply means
// route_type='capacity' has no depth data to pick by and falls back to
// first-match, exactly like an unmatched/empty tier would. [SCOPE P1-15]
type QueueDepthLoader interface {
	LoadQueueDepths(ctx context.Context) (map[string]QueueDepth, error)
}

// QueueDepthSource reads queue depths from where the queues really are — the
// role's own queue store, MySQL or Redis (SetQueueDepthSource). It wins over
// a Loader's LoadQueueDepths, which reads the database the HLR data lives in:
// once YAKU_HLR_DSN named its own database, or the queues moved to Redis,
// that held no queue tables and Least busy compared nothing (2026-10-06).
type QueueDepthSource func(ctx context.Context) (map[string]QueueDepth, error)

// QueueDepthKey is how a vendor's queue is keyed for Least busy: lower case,
// anything but a-z 0-9 _ as "_" — the MySQL table suffix (queue_<key>), so a
// depth keyed by table name and one keyed by vendor cid meet.
func QueueDepthKey(vendor string) string {
	b := []byte(strings.ToLower(vendor))
	for i, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			b[i] = '_'
		}
	}
	return string(b)
}

// normDepths keys a depth map by QueueDepthKey, adding up any two names that
// meet (a vendor's shared and own Redis queues are already one entry).
func normDepths(in map[string]QueueDepth) map[string]QueueDepth {
	if in == nil {
		return nil
	}
	out := make(map[string]QueueDepth, len(in))
	for k, d := range in {
		key := QueueDepthKey(k)
		cur, ok := out[key]
		if !ok {
			out[key] = d
			continue
		}
		cur.Depth += d.Depth
		if d.OldestAgeSec > cur.OldestAgeSec {
			cur.OldestAgeSec = d.OldestAgeSec
		}
		out[key] = cur
	}
	return out
}

// SetQueueDepthSource makes Least busy read depths from src (the role's queue
// store); nil returns to the Loader's own reading.
func (m *Mem) SetQueueDepthSource(src QueueDepthSource) {
	m.mu.Lock()
	m.queueDepthSrc = src
	m.mu.Unlock()
}

// loadQueueDepths is the depth reading: the queue store when one was set,
// else the Loader's, else none.
func (m *Mem) loadQueueDepths(ctx context.Context) (map[string]QueueDepth, error) {
	m.mu.RLock()
	src := m.queueDepthSrc
	m.mu.RUnlock()
	if src != nil {
		d, err := src(ctx)
		return normDepths(d), err
	}
	if qdl, ok := m.loader.(QueueDepthLoader); ok {
		d, err := qdl.LoadQueueDepths(ctx)
		return normDepths(d), err
	}
	return nil, nil
}

// QualityFor returns a vendor's recent delivery ratio (0..1) to one network,
// and whether it rests on enough evidence to act on. The network is the
// message's own (PO 2026-09-14): a vendor good for one network and poor for
// another is judged on the network the message is going to.
//
// minSample is the number of judged messages a vendor needs before its ratio
// is trusted. Below that the ratio still comes back, but ok is false — the
// PO's ruling (2026-09-04) is that a thin vendor is treated as GOOD rather
// than unknown, so a brand new vendor actually receives traffic and can earn
// a real score. Treating it as unknown instead would starve it for good: no
// traffic, so no score, so never chosen.
func (m *Mem) QualityFor(vendorID int64, mcc, mnc string, minSample int64) (ratio float64, proven bool) {
	m.mu.RLock()
	q, ok := m.quality[vendorquality.KeyFor(vendorID, mcc, mnc)]
	m.mu.RUnlock()
	if !ok || q.Judged < minSample {
		return 1, false
	}
	return q.Ratio(), true
}

// RefreshQuality reloads the delivery counts behind route_type='quality'. An
// error keeps what the cache had: routing must not suddenly read every vendor
// as unproven over one failed read.
func (m *Mem) RefreshQuality(ctx context.Context) error {
	m.mu.RLock()
	src, win := m.qualitySrc, m.qualityWindow
	m.mu.RUnlock()
	if src == nil {
		return nil
	}
	q, err := src.Load(ctx, win)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.quality = q
	m.mu.Unlock()
	return nil
}

// StartQualityRefresh reloads quality every `every` until ctx ends — on its
// own timer, because the reference data may refresh only every few minutes
// (or only when the Redis snapshot changes), and delivery moves faster.
func (m *Mem) StartQualityRefresh(ctx context.Context, every time.Duration, logf func(string, ...any)) {
	if every <= 0 {
		every = 30 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	var lastErr time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := m.RefreshQuality(ctx); err != nil && logf != nil && time.Since(lastErr) >= time.Minute {
			lastErr = time.Now()
			logf("quality counts not read (keeping the last ones): %v", err)
		}
		m.RefreshQueueDepths(ctx)
	}
}

// RefreshQueueDepths re-reads the vendor queue depths "Least busy" compares.
// They used to be read only with the rest of the reference data, which
// reloads only when the configuration changes — so a quiet configuration
// left "Least busy" choosing on depths hours old (2026-09-30).
func (m *Mem) RefreshQueueDepths(ctx context.Context) {
	d, err := m.loadQueueDepths(ctx)
	if err != nil || d == nil {
		return // keep the last depths
	}
	m.mu.Lock()
	m.queueDepths = d
	m.mu.Unlock()
}

// SetQualitySource says where quality counts come from and how far back to
// look (window <= 0 = one hour).
func (m *Mem) SetQualitySource(src QualitySource, window time.Duration) {
	if window <= 0 {
		window = time.Hour
	}
	m.mu.Lock()
	m.qualitySrc, m.qualityWindow = src, window
	m.mu.Unlock()
}

// QueueDepthFor returns a vendor's live pending-queue depth (route_type=
// 'capacity', 2026-07-28 routes review). found=false means no depth data is
// available for this vendor (backend doesn't support QueueDepthLoader, or
// the vendor has no queue table yet) — callers must treat that as "unknown,"
// not "zero/empty."
func (m *Mem) QueueDepthFor(vendorCID string) (QueueDepth, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.queueDepths[QueueDepthKey(vendorCID)]
	return d, ok
}
