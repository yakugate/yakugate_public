// Package dbworker implements the "db-worker" process — Process 14, the DB
// worker: the one role that writes the gateway's records to MySQL.
//
// Saleem's design of 2026-10-13 (D-131, stage 2): "a db worker process that
// does the insertions and updates for MO, MT and DLR … every other process
// that needs to perform those tasks just queues the task." And, the same
// day: "db worker should be a new process, the 14th process of gateway" — a
// role of its own, started and watched by the supervisor like the other
// thirteen, never a part of another role.
//
// The roles that carry traffic put one task per record on a Redis list
// (internal/recordq) and this role writes the lists to MySQL in batches, a
// lane at a time.
//
// It writes:
//   - the record tasks of every role (recordq): sent and refused messages,
//     incoming messages and what became of them, rejects, the records the
//     retry and expiry roles close a message with;
//   - the receipts that were finished in Redis (receiptfacts.Queue), which
//     the SMPP vendor role used to write out itself.
//
// It runs on every gateway, one per deployment (it is NOT gateway-wide like
// billing: each deployment's worker writes that deployment's lists). Several
// may run for one deployment: one writes a lane at a time and the others
// stand by. With YAKU_RECORDS_VIA=worker the other roles queue their records
// and write none; without it they write their own, as before, and this role
// only looks now and then for anything left on the lists (after a switch
// back).
//
// While it runs it says so in Redis every few seconds (recordq aliveKey), and
// takes that back when it stops: a role that queues records says "no DB
// worker" in its log and on its status a few seconds after the worker stops
// (half a minute after the role starts when it has never seen one).
//
// The billing queue (issues 3 and 18; Saleem, 26 Oct 2026): every
// deployment's processes queue a money event at the moment an amount is
// decided, in the main Redis, and this role books them into usage_buckets —
// whatever YAKU_RECORDS_VIA says, so every deployment must run a db-worker.
// The billing role is retired; nothing is charged from the records, and the
// records are written already priced by the roles that write them.
//
// Definition: types + construction. The loop is in workings.go.
package dbworker

import (
	"context"
	"os"
	"strings"
	"sync/atomic"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/billingq"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/platform/worker"
	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/internal/receiptfacts"
	"github.com/yakugate/yakugate/internal/recordq"
)

// Process writes the record queue out.
type Process struct {
	cfg config.Config
	log *logging.Logger
	hot dal.RecordStore
	// queue and writer are the record tasks' lists and their writer; nil
	// without Redis or without a hot store (see inert).
	queue  *recordq.Queue
	writer *recordq.Worker
	// viaWorker: the other roles queue their records (YAKU_RECORDS_VIA).
	viaWorker bool
	// rcptQueue and rcptSink are the receipts finished in Redis; nil when
	// receipts are not kept there.
	rcptQueue *receiptfacts.Queue
	rcptSink  receiptfacts.Sink
	// bill books this deployment's billing queue into usage_buckets; nil
	// without Redis or a config store (see billInert).
	bill      *billingq.Workers
	billInert string
	// billDurable is what is wrong with the Redis the money events wait in.
	billDurable atomic.Value
	// inert says why this role writes nothing, "" when it can.
	inert string
	// durable is what is wrong with the Redis the records wait in ("" =
	// nothing), looked at again every few minutes (workings.go).
	durable atomic.Value
}

// New wires the stores.
func New(cfg config.Config) worker.Worker {
	p := &Process{cfg: cfg, log: logging.New("db-worker"), viaWorker: recordq.ViaWorker(cfg)}
	p.durable.Store("")
	p.billDurable.Store("")
	if !cfg.RedisEnabled() {
		// No Redis (plan of 29 Oct 2026, step 3): every role writes its own
		// records, and the money events wait in the config store
		// (billing_events) — this role books them, as it books the Redis
		// queues.
		p.inert = "no Redis is set: every role writes its own records, and there is no record queue to write out"
		p.openBilling(cfg)
		return p
	}
	p.openBilling(cfg)
	h, err := dal.OpenRecordStore(cfg)
	if err != nil {
		p.inert = "the hot store could not be opened: " + err.Error()
		p.log.Printf("%s", p.inert)
		return p
	}
	p.hot = h
	p.queue = recordq.FromConfig(cfg)
	if p.queue == nil {
		p.inert = "the record queue could not be opened"
		p.log.Printf("%s", p.inert)
		return p
	}
	p.writer = recordq.NewWorker(p.queue, h, p.log.Printf)
	// Each lane is checked every YAKU_QUEUE_RECORDS_EVERY_MS, busy or idle,
	// and one check takes at most YAKU_QUEUE_RECORDS_PER_INTERVAL (issue 7).
	rc := queuecheck.For(queuecheck.Records)
	p.writer.Idle, p.writer.PerCheck = rc.Every, rc.PerInterval
	var rcpts *dal.Receipts
	if receiptsInRedis() || p.viaWorker {
		r, err := dal.OpenReceipts(cfg, deployment.Owner())
		if err != nil {
			p.log.Printf("the receipts' own list could not be opened: %v", err)
		}
		rcpts = r
	}
	if rcpts != nil {
		p.rcptQueue = receiptfacts.NewQueue(rcpts.Writes)
		p.rcptQueue.Check = queuecheck.For(queuecheck.ReceiptWrites) // YAKU_QUEUE_RECEIPT_WRITES_* (issue 7)
		p.rcptSink = receiptfacts.ToRecordStore(h)
		// A vendor's receipt is a task on the record queue (recordq
		// receipts.go, 2026-10-13). One that comes off its lane before its
		// message's record has its row written and its status put on this
		// list, which waits for the record and is written out below.
		p.writer.Later = p.rcptQueue.Later
		if p.viaWorker {
			// A receipt can be ready before its message's record is: both
			// wait in Redis and this role writes both. A status is not given
			// up for "no record" while that record may still be in the queue.
			w := p.writer
			p.rcptQueue.StillComing = func(ctx context.Context, r dlr.ReceiptWrite) bool {
				return w.StillComing(ctx, r.MsgID, r.At)
			}
		}
	}
	return p
}

// Name returns the process role name.
func (p *Process) Name() string { return "db-worker" }

// ReadyForTraffic: the role is ready unless it was meant to write and cannot.
func (p *Process) ReadyForTraffic() bool { return p.inert == "" || !p.viaWorker }

// receiptsInRedis is YAKU_RECEIPTS_IN_REDIS, read as the SMPP vendor role
// reads it: on unless it says 0, false, off or no.
func receiptsInRedis() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_RECEIPTS_IN_REDIS"))) {
	case "0", "false", "off", "no":
		return false
	}
	return true
}

// MonitorSections publishes the record queue on this role's status: tasks
// waiting and how long the oldest has waited (the lag between a message and
// its record), tasks parked (MySQL refused them; tried again every second),
// what was written and dropped, the receipts waiting beside them, whether
// the Redis they wait in keeps an append-only file, and the billing queue.
func (p *Process) MonitorSections() map[string]any {
	sec := map[string]any{"mode": "direct"}
	if p.viaWorker {
		sec["mode"] = "worker"
	}
	if p.inert != "" {
		sec["inert"] = p.inert
	}
	if p.writer != nil {
		for k, v := range p.writer.Section() {
			sec[k] = v
		}
	}
	if p.bill != nil {
		bq := p.bill.Section(context.Background())
		if why, _ := p.billDurable.Load().(string); why != "" {
			bq["redis_warning"] = why
		}
		sec["billing_queue"] = bq
	} else if p.billInert != "" {
		sec["billing_queue"] = map[string]any{"inert": p.billInert}
	}
	if p.rcptQueue != nil {
		written, requeued, dropped := p.rcptQueue.Stats()
		sec["receipts"] = map[string]any{"waiting": p.rcptQueue.Waiting(context.Background()),
			"written": written, "put_back": requeued, "let_go": dropped}
	}
	if p.viaWorker {
		sec["redis_appendonly"] = "yes"
	}
	if why, _ := p.durable.Load().(string); why != "" {
		sec["redis_appendonly"] = "no"
		sec["redis_warning"] = why
	}
	return map[string]any{"db_worker": sec}
}

// openBilling opens this deployment's billing queue for booking: the sell
// and buy queues in the queue Redis (and what an earlier build left in the
// main Redis), or — with no Redis — in the config store (billing_events),
// each side booked by itself into usage_buckets.
func (p *Process) openBilling(cfg config.Config) {
	// The config store on its own DAL (MySQL or SQL Server): a store that
	// cannot be opened is said by OpenBilling.
	bs, err := dal.OpenBilling(cfg)
	if err != nil {
		p.billInert = "the billing stores could not be opened: " + err.Error()
		p.log.Printf("NOT BOOKING THE BILLING QUEUE: %s", p.billInert)
		return
	}
	if bs.Queue == nil {
		p.billInert = "the config store's DAL keeps no billing queue"
		p.log.Printf("NOT BOOKING THE BILLING QUEUE: %s", p.billInert)
		return
	}
	// The selling and the buying queue, each booked by itself (27 Oct),
	// in the deployment's queue Redis (29 Oct); what an earlier build
	// left in the main Redis is booked once and let go.
	p.bill = billingq.NewWorkers(bs.Earlier, bs.Queue, bs.Separate,
		bs.Store, deployment.Owner(), billing.LanesFromEnv(), p.log.Printf)
	// Each lane is checked every YAKU_QUEUE_BILLING_EVERY_MS, busy or idle,
	// and one check books at most YAKU_QUEUE_BILLING_PER_INTERVAL (issue 7).
	bc := queuecheck.For(queuecheck.Billing)
	p.bill.SetPace(bc.Every, bc.PerInterval)
}
