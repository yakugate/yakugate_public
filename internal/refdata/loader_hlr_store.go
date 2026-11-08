// What the loader reads from the HLR store: HLR numbers (hlr.Store and
// hlr.DeltaSource) and the vendor queues' depths.

package refdata

import (
	"context"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/hlr"
)

// Lookup resolves one MSISDN from its first-4-digit shard (hlr_<first4>).
// Soft-deleted rows (is_deleted=1) are misses. Used directly only in cache
// passthrough mode; normally hlr.Cache serves lookups from memory.
func (l *MySQL) Lookup(msisdn string) (hlr.Entry, bool) {
	if l.hlr == nil {
		return hlr.Entry{}, false // HLR is not in a store this loader can read
	}
	r, ok := l.hlr.Lookup(hlr.Shard(msisdn), msisdn)
	if !ok {
		return hlr.Entry{}, false
	}
	return hlrEntry(r), true
}

// hlrEntry is a row of the HLR store as the hlr package holds it.
func hlrEntry(r dal.HLRRow) hlr.Entry {
	return hlr.Entry{MSISDN: r.MSISDN, MCC: r.MCC, MNC: r.MNC, OperatorName: r.OperatorName, CCC: r.CCC,
		Ported: r.Ported, Deleted: r.Deleted, CheckedAt: r.CheckedAt}
}

// --- hlr.DeltaSource (in-memory HLR cache backing) ---------------------------
// last_check is the change-tracking column (self-updating TIMESTAMP, indexed);
// is_deleted is the soft-delete flag. Migration queue/0005 shapes hlr_template;
// Prepare upgrades pre-existing dynamic shards to the same shape (the DAL's
// HLRNumbers does the reading and the upgrading).

// Prepare upgrades each live shard to the cache schema. Idempotent: checks
// information_schema before altering, so restarts are no-ops.
func (l *MySQL) Prepare(ctx context.Context) error {
	if l.hlr == nil {
		return nil // no queue store: no shards, and every caller handles empty
	}
	return l.hlr.Prepare(ctx)
}

// Count totals HLR rows across all shards (memory precheck input).
func (l *MySQL) Count(ctx context.Context) (int64, error) {
	if l.hlr == nil {
		return 0, nil
	}
	return l.hlr.Count(ctx)
}

// LoadSince returns entries changed at/after `since` across all shards plus the
// max last_check seen. Zero since = initial full load (soft-deleted rows are
// skipped; delta loads include them so the cache can evict). Rows AT `since`
// are read again only for a few seconds of the store's clock (the DAL's
// HLRNumbers.LoadSince says why).
func (l *MySQL) LoadSince(ctx context.Context, since time.Time) ([]hlr.Entry, time.Time, error) {
	if l.hlr == nil {
		return nil, since, nil
	}
	rows, maxTS, err := l.hlr.LoadSince(ctx, since)
	if err != nil {
		return nil, maxTS, err
	}
	var out []hlr.Entry
	for _, r := range rows {
		out = append(out, hlrEntry(r))
	}
	return out, maxTS, nil
}

// LoadQueueDepths implements QueueDepthLoader — one live COUNT(*)+MIN(created_at)
// per vendor queue table (QUEUE store), mirroring internal/store/mysql.Store's
// QueueDepths exactly (same query, same "skip a bad/locked table rather than
// fail the whole read" posture) since that type isn't reachable from here
// without a layering-violating import. Feeds route_type='capacity' (routes
// review, 2026-07-28) via the periodic reference-cache refresh — never a
// per-message query. [SCOPE P1-15]
func (l *MySQL) LoadQueueDepths(ctx context.Context) (map[string]QueueDepth, error) {
	if l.hlr == nil {
		return nil, nil // capacity routing has no depth data without a queue store
	}
	tables, err := l.hlr.QueueTables(ctx)
	if err != nil {
		return nil, err
	}

	// Cheap and never a silent 0 (production load test, 2026-10-05): the
	// count comes from the claim index alone (idx_claim covers status), the
	// oldest age from the first rows by id, each queue has its own time
	// limit, and a queue that cannot be read keeps its last depth. Ages are
	// worked out by MySQL on its own clock (time.Since on created_at was off
	// by the server's UTC offset).
	l.depthMu.Lock()
	defer l.depthMu.Unlock()
	if l.depthLast == nil {
		l.depthLast = map[string]QueueDepth{}
	}
	out := make(map[string]QueueDepth, len(tables))
	for _, t := range tables {
		vendor := strings.TrimPrefix(t, "queue_")
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		depth, age, err := l.hlr.QueueDepth(c, t)
		cancel()
		if err != nil {
			if last, ok := l.depthLast[vendor]; ok {
				out[vendor] = last
			}
			continue // one bad/locked table doesn't blank the whole report
		}
		qd := QueueDepth{Depth: depth.Int64}
		if age.Valid && age.Int64 > 0 {
			qd.OldestAgeSec = age.Int64
		}
		l.depthLast[vendor] = qd
		out[vendor] = qd
	}
	return out, nil
}
