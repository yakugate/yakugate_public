// Package blackhole implements the "blackhole" gateway process — a full vendor
// SIMULATOR. A blackhole vendor (vendors.transport='blackhole') receives routed
// messages like any vendor: they enqueue, records + DLRs are written exactly as a
// real vendor would, driven by dlr_config percentages — but the message is NEVER
// transmitted onward. Useful for exercising the records + DLR path end-to-end
// before the real vendor senders (P3/P4) exist. [SCOPE blackhole-sim]
//
// Definition: types + construction. The Step loop lives in workings.go.
package blackhole

import (
	"context"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/internal/recordq"
	"math/rand"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/runtimequeue"
	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
	"github.com/yakugate/yakugate/internal/ratelimit"
)

// simVendor is a loaded blackhole vendor and its DLR outcome split. The no-DLR
// share is the remainder (100 - delivered - failed).
type simVendor struct {
	id           int64
	cid          string
	deliveredPct int
	failedPct    int
	// tps is this deployment's share of the vendor's rate, in messages a
	// second (its deployments entry's mt_throughput); 0 = no limit.
	tps int
}

// Process claims blackhole-vendor queues, writes MDR records + DLRs per the
// configured percentages, and Acks — without ever forwarding the message.
// simQueue is what the simulator needs from the per-vendor queue — satisfied
// by the MySQL store and, under the Redis hot-path gate, the Redis store
// (gap found 2026-07-26: blackhole missed the 2026-07-23/24 gate sweep and
// silently drained NOTHING when queues lived on Redis).
type simQueue interface {
	Claim(ctx context.Context, vendor string, n int) ([]model.Message, error)
	Ack(ctx context.Context, vendor string, msgIDs ...string) error
}

type Process struct {
	cfg   config.Config
	log   *logging.Logger
	queue simQueue        // per-vendor pending queue (Claim/Ack)
	hot   dal.RecordStore // MDR records (Write/UpdateStatus)
	// recs is where this role's records go: the record store itself, or —
	// with YAKU_RECORDS_VIA=worker — the queue the DB worker writes out
	// (recordq). recQ is that queue's client, nil when the role writes its own.
	recs recordq.Recorder
	recQ *recordq.Client
	// traffic is the config store's small parts this role reads (the
	// blackhole vendors and their dlr_config), on the DAL; nil without a
	// config store.
	traffic *dal.TrafficConfig
	// eng is the config store's parts the shared engine packages read
	// (attempt caps, split modes, receipt policies, doors, change marks).
	eng    *dal.EngineConfig
	pusher dlrsvc.ClientPusher
	// bill books the money of what the pretend vendor does, as a real one's
	// role does (billing.Moments); nil without Redis or a config store.
	bill *billing.Moments
	// rate paces each vendor at its deployment rate, on the key the other
	// vendor roles use — shared through Redis when Redis is set, so every
	// blackhole process of a deployment draws from one bucket.
	rate     ratelimit.Granter
	saidTPS  map[string]int // the rate last said in the log, per vendor
	rng      *rand.Rand
	vendors  []simVendor
	loadedAt time.Time
	// mt is how often the vendor queues are checked and how many messages a
	// check takes from each (YAKU_QUEUE_MT_*, internal/queuecheck; issue 7).
	mt queuecheck.Check
}

// New constructs the blackhole simulator, wiring the queue, hot and config stores.
func New(cfg config.Config) worker.Worker {
	p := &Process{cfg: cfg, log: logging.New("blackhole"), rate: ratelimit.New(), mt: queuecheck.For(queuecheck.MT),
		rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	if l, err := dal.OpenLimits(cfg); err == nil {
		// A counter, not a queue: the main Redis, whatever YAKU_QUEUE_BACKEND says.
		p.rate = ratelimit.NewShared(l)
	}
	// The queues are on the DAL the settings pick (dal.QueuesOn; Redis with
	// YAKU_QUEUE_BACKEND=redis, D-122).
	if q, on, err := dal.OpenQueues(cfg); err == nil {
		p.queue = q
		// Simulated receipts must reach the client like real ones (smsc leg,
		// D-022) — before 2026-08-10 blackhole stamped terminal status but
		// NEVER created a client_dlr row, so blackhole-routed traffic never
		// notified the submitting client (bind or dlr_url) — found live
		// 2026-07-26, fixed as part of the P5 matrix campaign.
		p.pusher = dlrsvc.NewQueuePusher(q)
	} else {
		p.log.Printf("queues unavailable on %s: %v", on, err)
	}
	// What was queued before this build is moved into today's queues at
	// start (issue 22), here too: a deployment may run only vendor roles.
	runtimequeue.HandOver(cfg, p.log.Printf)
	if h, err := dal.OpenRecordStore(cfg); err == nil {
		p.hot = h
		p.recs, p.recQ = recordq.ForRole(context.Background(), cfg, h, p.log.Printf)
	}
	// The config store's parts the shared engine packages read
	// (dal.EngineConfig, on the same pool as traffic; empty when it cannot
	// be opened, as traffic is then nil).
	p.eng = dal.EngineConfigOrNone(cfg)
	if t, err := dal.OpenTrafficConfig(cfg); err == nil {
		p.traffic = t
	}
	// With Redis on its live balances; with none in the config store (step
	// 3): NewMoments is nil when there is nowhere to book.
	if p.traffic != nil {
		if bs, err := dal.OpenBilling(cfg); err == nil {
			p.bill = billing.NewMoments(context.Background(), bs, p.log.Printf)
		} else {
			p.log.Printf("billing unavailable: %v", err)
		}
	}
	return p
}

// Name returns the process role name.
func (p *Process) Name() string { return "blackhole" }

// StepEvery is how often the vendor queues are checked (worker.Paced).
func (p *Process) StepEvery() time.Duration { return p.mt.Every }

// perCheck is how many messages one check takes from a vendor: the setting,
// else 50 batches (a Process a test builds by hand, as before).
func (p *Process) perCheck(batch int) int {
	if p.mt.PerInterval > 0 {
		return p.mt.PerInterval
	}
	return 50 * batch
}

// rec is where this role's records go (see Process.recs).
func (p *Process) rec() recordq.Recorder {
	if p.recs != nil {
		return p.recs
	}
	if p.hot == nil {
		return nil // not a nil store inside the interface
	}
	return p.hot
}
