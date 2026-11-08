package dbworker

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	redisstore "github.com/yakugate/yakugate/internal/dal/redis"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/recordq"
)

// durableEvery is how often the Redis the records wait in is asked whether
// it keeps an append-only file.
const durableEvery = 5 * time.Minute

// idleWhenDirect is how long an empty lane is left when the other roles
// write their own records: the lanes are only looked at in case a switch
// back from the worker left something on them.
const idleWhenDirect = 2 * time.Second

// Step is not used: the role is a Runnable.
func (p *Process) Step(context.Context) error { return nil }

// Run writes the record queue and the receipt queue to MySQL until ctx ends.
//
//	INPUT:  recq:<lane>[@deployment] (recordq tasks), rcptw[@deployment]
//	        (receipts finished in Redis)
//	LOGIC:  per lane, one writer at a time: up to 500 tasks, each kind in one
//	        statement, a message's tasks in the order they were queued; what
//	        MySQL will not take is put back and tried again
//	OUTPUT: mtlog_/dlrlog_/mo_log_/rejects_ rows and record_index
func (p *Process) Run(ctx context.Context) error {
	billDone := make(chan struct{})
	if p.bill != nil {
		p.log.Printf("booking the billing queues of %q (selling and buying, each by itself): money events into usage_buckets", billing.QueueScope(deployment.Owner()))
		go func() {
			p.bill.Run(ctx)
			close(billDone)
		}()
		p.checkBillDurable(ctx)
	} else {
		close(billDone)
	}
	if p.writer == nil {
		if p.inert != "" && p.viaWorker {
			p.log.Printf("NOT WRITING RECORDS: %s", p.inert)
		}
		<-ctx.Done()
		<-billDone
		return nil
	}
	if p.viaWorker {
		p.log.Printf("writing the record queue: %d lane(s), up to %d records a statement", p.queue.Lanes(), recordq.Batch)
	} else {
		if p.writer.Idle < idleWhenDirect {
			p.writer.Idle = idleWhenDirect
		}
		p.log.Printf("the roles write their own records (YAKU_RECORDS_VIA is not `worker`): only looking for records left on the queue")
	}
	done := make(chan struct{})
	go func() {
		p.writer.Run(ctx)
		close(done)
	}()
	if p.rcptQueue != nil && p.rcptSink != nil {
		// Safe beside an SMPP vendor role that still writes them itself: one
		// process has the receipts' turn at a time.
		go p.rcptQueue.Drain(ctx, p.rcptSink, p.log.Printf)
	}
	p.checkDurable(ctx)
	t := time.NewTicker(durableEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			<-done // the batch being written is finished, and the turns given up
			<-billDone
			return nil
		case <-t.C:
			p.checkDurable(ctx)
			p.checkBillDurable(ctx)
		}
	}
}

// checkDurable says, when it changes, whether the Redis the records wait in
// would lose them if it were killed.
func (p *Process) checkDurable(ctx context.Context) {
	if !p.viaWorker {
		return
	}
	why := recordq.CheckDurable(ctx, p.queue.Store())
	if was, _ := p.durable.Load().(string); why != was {
		p.durable.Store(why)
		if why != "" {
			p.log.Printf("WARNING: %s", why)
		} else {
			p.log.Printf("the Redis that holds the record queue keeps an append-only file")
		}
	}
}

// checkBillDurable says, when it changes, whether the Redis the money events
// wait in (the main one) would lose them if it were killed.
func (p *Process) checkBillDurable(ctx context.Context) {
	if !p.cfg.RedisEnabled() {
		return // the events wait in MySQL (billing_events): nothing to ask
	}
	// The queue Redis the live sides are in, asked on billing's own client
	// (redisstore.QueueClient), as before.
	why := recordq.CheckDurable(ctx, redisstore.NewRecordLanes(redisstore.QueueClient(p.cfg), ""))
	if why != "" {
		why = "billing queue: " + why
	}
	if was, _ := p.billDurable.Load().(string); why != was {
		p.billDurable.Store(why)
		if why != "" {
			p.log.Printf("WARNING: %s", why)
		}
	}
}
