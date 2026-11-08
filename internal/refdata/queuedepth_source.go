package refdata

import (
	"context"

	"github.com/yakugate/yakugate/internal/store"
)

// DepthsFromQueue is a QueueDepthSource reading the role's own queue store —
// MySQL tables or Redis zsets, wherever YAKU_QUEUE_BACKEND put the queues —
// through the same QueueDepths the Monitoring screen reads: this deployment's
// queues and the shared ones (D-121), cheap, and a queue that cannot be
// counted keeps its last figures. nil when the store cannot count.
func DepthsFromQueue(q any) QueueDepthSource {
	qs, ok := q.(store.QueueDepthStats)
	if !ok || qs == nil {
		return nil
	}
	return func(ctx context.Context) (map[string]QueueDepth, error) {
		stats, err := qs.QueueDepths(ctx)
		if err != nil {
			return nil, err
		}
		out := make(map[string]QueueDepth, len(stats))
		for vendor, st := range stats {
			out[vendor] = QueueDepth{Depth: st.Depth, OldestAgeSec: st.OldestAgeSec}
		}
		return out, nil
	}
}
