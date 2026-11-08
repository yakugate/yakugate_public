package redis

import (
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The Redis DAL (internal/dal, plan of 29 Oct 2026). It serves the queue
// family from the queues' own Redis (YAKU_QUEUE_REDIS_*), else the main one —
// what runtimequeue.Build gave the roles.
func init() {
	dal.Register(dal.Driver{
		Name: config.BackendRedis,
		Queues: func(cfg config.Config) (dal.Queues, error) {
			return FromConfig(cfg.ForQueueRedis()), nil
		},
		Counters: func(cfg config.Config) (*dal.Counters, error) {
			return OpenCounters(cfg), nil
		},
		// The live balances on the main Redis, one client a process; the
		// billing queues on the queue Redis (QueueClient), and the main
		// Redis's queues of earlier builds.
		LiveBalances: func(cfg config.Config) (dal.LiveBalances, error) {
			return NewLiveBalances(mainBillingClient(cfg)), nil
		},
		BillingQueue: func(cfg config.Config, main bool) (dal.BillingQueue, error) {
			if main {
				return NewBillingQueue(mainBillingClient(cfg)), nil
			}
			return NewBillingQueue(QueueClient(cfg)), nil
		},
		// The record queue (queue Redis), the receipts and the hand-off
		// (main Redis), long messages' parts and vendor quality (main
		// Redis): records.go, receipts.go, concat.go, quality.go.
		RecordLanes:   openRecordLanes,
		Receipts:      openReceipts,
		HandOffs:      openHandOffs,
		ConcatBuffer:  openConcatBuffer,
		VendorQuality: openVendorQuality,
		// Monitoring's live view (main Redis) and the servers' own figures:
		// monitoring.go, storereadings.go.
		LiveStatus:       openLiveStatus,
		RedisServerProbe: openServerProbe,
		// The HLR numbers' copies (main Redis): hlr.go.
		HLRCopies: openHLRCopies,
		// The HTTP vendors' poll turns, on the main Redis (pollturn.go).
		PollTurns: openPollTurns,
		// The console's live balances and live view, on one client of its
		// own on the main Redis (console.go).
		LiveParts: openLiveParts,
	})
}

var _ dal.Queues = (*Store)(nil)
