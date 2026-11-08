package httpvendor

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/internal/vendorhealth"
	"github.com/yakugate/yakugate/internal/vendorquality"
)

// Step runs one egress cycle:
//
//	INPUT   — claim pending rows from each HTTP vendor's queue
//	LOGIC   — deliver each via the vendor's config-driven httpadapter (Send),
//	          then poll status for vendors whose profile uses mode=poll
//	OUTPUT  — MDR record (status=sent + vendor_msg_id), queue Ack; poll -> delivered/failed
func (p *Process) Step(ctx context.Context) error {
	if p.queue == nil || p.hot == nil || p.traffic == nil {
		return nil
	}
	p.startOnce.Do(func() {
		// Routing rules review (PO 2026-09-14): say which vendors are being
		// answered, for routing's failover; count vendors' refusals for
		// quality routing (receipts are counted by the DLR service).
		go vendorhealth.NewPublisher(p.traffic.VendorHealth(), "http-vendor", vendorhealth.Instance(), p.health, p.log.Printf).Run(ctx)
		vendorquality.StartCounting(ctx, p.cfg, p.log.Printf)
	})
	if time.Since(p.loadedAt) > p.reload || p.settingsChanged(ctx) {
		p.loadVendors(ctx)
		// Same cadence for the receipt-code overrides: a rule added in the
		// portal reaches the status-poll leg within one reload, exactly like a
		// vendor config change.
		if p.vstatus != nil {
			if err := p.vstatus.Reload(ctx); err != nil {
				p.log.Printf("vendor status codes: %v", err)
			}
		}
		// And the accounts' receipt policies, on the same cadence: a policy
		// saved in the portal starts answering within one reload.
		if p.policies != nil {
			if err := p.policies.Reload(ctx); err != nil {
				p.log.Printf("%v", err)
			}
		}
	}
	// Sending runs in one worker per vendor (below); Step keeps the workers
	// matched to the active vendors and does the status polling.
	p.syncLanes(ctx)
	p.vmu.Lock()
	vendors := p.vendors
	p.vmu.Unlock()
	for _, v := range vendors {
		p.pollStatus(ctx, v)
	}
	return nil
}

// laneIdle is what a round says it would wait when there was nothing to send
// — the old Step interval — and how often a Process a test builds by hand
// checks (mtCheck). A vendor's worker checks every YAKU_QUEUE_MT_EVERY_MS
// since issue 7, whatever a round says.
const laneIdle = 500 * time.Millisecond

// syncLanes starts a worker for each active vendor and stops the worker of a
// vendor that left the active set.
//
// One loop used to serve every vendor, one call at a time on a 500 ms tick: a
// single-send vendor could never exceed 100 messages a second per process, and
// one vendor timing out held up every other vendor behind it (PO 2026-09-11).
func (p *Process) syncLanes(ctx context.Context) {
	p.vmu.Lock()
	defer p.vmu.Unlock()
	live := make(map[int64]bool, len(p.vendors))
	for _, v := range p.vendors {
		live[v.id] = true
	}
	for id, stop := range p.lanes {
		if !live[id] {
			stop()
			delete(p.lanes, id)
		}
	}
	for id := range live {
		if _, ok := p.lanes[id]; ok {
			continue
		}
		lctx, cancel := context.WithCancel(ctx)
		p.lanes[id] = cancel
		p.lanesWG.Add(1)
		go func() {
			defer p.lanesWG.Done()
			p.runVendor(lctx, id)
		}()
	}
}

// stopFor is how long a stopping role waits for its vendors' calls in flight
// to be answered and filed. Under the supervisor's own 20 s.
const stopFor = 12 * time.Second

// Stop lets the calls in flight finish before the process ends
// (worker.Stopper). A role that was stopped — a deploy, a restart from the
// console — used to end at once: a call the vendor was answering at that
// moment was sent but never recorded, and its message went out a second time
// when its lease ran out. Now every worker stops CLAIMING when the role is
// told to stop, and the role waits here until what was already sent is
// answered and filed (sendLane.finish).
func (p *Process) Stop() {
	done := make(chan struct{})
	go func() { p.lanesWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(stopFor):
		p.log.Printf("stopping with vendor calls still unanswered after %s: what they answer now is not recorded", stopFor)
	}
}

// runVendor sends one vendor's queue until the vendor leaves the active set.
// It re-reads the vendor every round, so a reload's new adapter, batch or
// concurrency takes effect without restarting the worker — and without two
// workers ever sending for the same vendor.
//
// One round each YAKU_QUEUE_MT_EVERY_MS, busy or idle (issue 7; Saleem, 26 Oct
// 2026: "fixed interval always"). A worker used to come straight back after a
// full claim, a moment after the rate cap, and every 500 ms when the queue
// was empty; what a round says (more, wait) is no longer read here.
func (p *Process) runVendor(ctx context.Context, id int64) {
	// The calls in flight and their filer outlive a round (filer.go
	// sendLane); when the worker stops they are finished and filed.
	ln := &sendLane{}
	defer ln.finish()
	t := time.NewTicker(p.mtCheck().Every)
	defer t.Stop()
	for ctx.Err() == nil {
		v, ok := p.vendorByID(id)
		if !ok {
			return
		}
		p.roundOn(ctx, v, ln)
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}
}

// mtCheck is how often a vendor's worker checks its queue and how much one
// check takes: YAKU_QUEUE_MT_*, read in New; a Process a test builds by hand
// checks every laneIdle and takes what the vendor's batch allows.
func (p *Process) mtCheck() queuecheck.Check {
	if p.mt.Every > 0 {
		return p.mt
	}
	return queuecheck.Check{Every: laneIdle}
}

func (p *Process) vendorByID(id int64) (vendorAdapter, bool) {
	p.vmu.Lock()
	defer p.vmu.Unlock()
	for _, v := range p.vendors {
		if v.id == id {
			return v, true
		}
	}
	return vendorAdapter{}, false
}
