package smppvendor

import (
	"context"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/vendorhealth"
	"github.com/yakugate/yakugate/internal/vendorquality"
)

// Run is the smpp-vendor supervisor: it keeps the set of running per-session
// binder goroutines in line with the vendors table, re-scanning every
// reloadEvery (or immediately on a forceReload/"reload now" command) so a
// vendor added, edited, or deactivated is picked up WITHOUT a process
// restart (item 5). On context cancel every session goroutine performs a
// graceful unbind + window drain (item 4) and Run waits for them before
// returning.
func (p *Process) Run(ctx context.Context) error {
	if p.queue == nil || p.hot == nil || p.traffic == nil {
		p.log.Printf("stores unavailable; smpp-vendor idle")
		<-ctx.Done()
		return ctx.Err()
	}
	var wg sync.WaitGroup
	p.reconcile(ctx, &wg)
	p.log.Printf("started (reload every %s, %d receipt/MO workers)", p.reloadEvery, len(p.inbound.lanes))
	// Routing rules review (PO 2026-09-14): say which vendors this process
	// can reach, for routing's failover; and count vendors' refusals for
	// quality routing (receipts are counted by the DLR service).
	go p.watchHealth(ctx)
	if p.moParts != nil && p.mo != nil {
		go p.reapMOParts(ctx) // incoming multi-part messages that never completed
	}
	go vendorhealth.NewPublisher(p.traffic.VendorHealth(), "smpp-vendor", vendorhealth.Instance(), p.health, p.log.Printf).Run(ctx)
	// The receipts' database work, written in batches (receiptfacts.Queue).
	// It waits in Redis, so it survives this process; whichever process of
	// the deployment has the turn writes it.
	// With YAKU_RECORDS_VIA=worker the DB worker writes them out, and
	// this role writes nothing to the record tables for them.
	if p.rcptQueue != nil && p.rcptSink != nil && p.recQ == nil {
		go p.rcptQueue.Drain(ctx, p.rcptSink, p.log.Printf)
		p.log.Printf("receipts are finished in Redis; their records are written in batches")
	}
	vendorquality.StartCounting(ctx, p.cfg, p.log.Printf)
	reload := time.NewTicker(p.reloadEvery)
	defer reload.Stop()
	for {
		select {
		case <-ctx.Done():
			// Parent cancel propagates to every session's child context; each does
			// a graceful unbind + drain. Wait for them all, then for the receipts
			// and MO they handed off — every one already acked to the vendor.
			wg.Wait()
			p.inbound.close()
			return ctx.Err()
		case <-reload.C:
			p.reconcile(ctx, &wg)
		case <-p.reloadNow:
			p.reconcile(ctx, &wg)
		}
	}
}

// reconcile brings the running session-goroutine set in line with the
// vendors table: start newly-added/reactivated vendors' sessions, restart
// ones whose connection config changed, and stop sessions that vanished,
// were deactivated, or whose vendor's session count shrank. [SCOPE P3 item 5]
func (p *Process) reconcile(ctx context.Context, wg *sync.WaitGroup) {
	current, ok := p.loadVendors(ctx)
	if !ok {
		// The vendors could not be read: that says nothing about them. The
		// reload used to go on with an empty list and stop every session it
		// did not see — a 12 s MySQL cut stopped every SMPP bind for 30 s,
		// until the next reload (go-live test 18 Oct, P14 E5, P5 E7, P6 E11:
		// sending, receipts and MO all stopped, receipts then doubled). The
		// sessions keep running as they are until a reload succeeds.
		p.mu.Lock()
		n := len(p.sup)
		p.mu.Unlock()
		p.log.Printf("vendors not read; the %d session(s) running are kept as they are until a reload succeeds", n)
		return
	}
	p.loadStatusCodes(ctx)
	// P6-B3: the MO route cache is built once in New and used by every
	// deliver_sm. Without this it never refreshed, so a new or edited MO route
	// reached the HTTP webhook edge within 30s but needed a smpp-vendor
	// RESTART to reach the SMPP edge — the same routes, two different answers,
	// depending on which way the message came in. Refreshed on the vendor
	// cadence rather than a second ticker: both read the same config store.
	if p.mo != nil {
		if err := p.mo.Reload(ctx); err != nil {
			p.log.Printf("mo route reload: %v", err)
		}
	}

	p.mu.Lock()
	p.current = make(map[int64]vendorConn, len(current))
	for _, v := range current {
		p.current[v.id] = v
	}
	p.mu.Unlock()

	seen := map[sessionKey]bool{}
	for _, v := range current {
		sessions := v.sessions
		if sessions < 1 {
			sessions = 1
		}
		for i := 0; i < sessions; i++ {
			key := sessionKey{VendorID: v.id, Idx: i}
			host := v.hosts[i%len(v.hosts)]
			seen[key] = true
			p.mu.Lock()
			h, running := p.sup[key]
			changed := running && h.fp != v.fingerprint
			p.mu.Unlock()
			if running && !changed {
				// Same connection, maybe new settings: the ones a live bind
				// can take apply now (withLive), and the throttle pause is
				// kept per session, so it is refreshed here.
				p.ensureState(key, v.cid, host, v.throttlePause)
				continue
			}
			if changed {
				p.log.ForVendor(v.cid).Infof("bind_restart", "session %d connection settings changed; restarting bind", i)
				p.stopSession(key)
			}
			cctx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			p.ensureState(key, v.cid, host, v.throttlePause)
			handle := &supHandle{cancel: cancel, done: done, fp: v.fingerprint}
			p.mu.Lock()
			p.sup[key] = handle
			p.mu.Unlock()
			wg.Add(1)
			go func(v vendorConn, key sessionKey, host string) {
				defer wg.Done()
				defer close(done)
				p.runVendor(cctx, v, key, host, handle)
			}(v, key, host)
		}
	}
	// Stop sessions whose vendor disappeared, was deactivated, or whose
	// session count shrank.
	p.mu.Lock()
	var gone []sessionKey
	for key := range p.sup {
		if !seen[key] {
			gone = append(gone, key)
		}
	}
	p.mu.Unlock()
	for _, key := range gone {
		p.log.Printf("vendor=%d session=%d no longer active; stopping bind", key.VendorID, key.Idx)
		p.stopSession(key)
		p.mu.Lock()
		delete(p.state, key)
		p.mu.Unlock()
	}
}

// stopSession cancels one session's goroutine and waits (bounded) for it to
// exit, so a config-change restart never overlaps the old and new binds.
//
// A session that has not stopped by then has its connection CLOSED here
// (2026-10-08). It used to be left alone with a log line: the session was
// forgotten — gone from the supervisor and from monitoring — while its bind
// stayed up for as long as whatever it was stuck in lasted, and the vendor
// went on counting it. A bind this role no longer wants must not outlive
// the decision.
func (p *Process) stopSession(key sessionKey) {
	p.mu.Lock()
	h := p.sup[key]
	delete(p.sup, key)
	p.mu.Unlock()
	if h == nil {
		return
	}
	h.cancel()
	select {
	case <-h.done:
		return
	case <-time.After(p.drainTimeout + stopGrace):
	}
	if h.closeConn() {
		p.log.Printf("vendor=%d session=%d did not stop in time; its connection was closed", key.VendorID, key.Idx)
	} else {
		p.log.Printf("vendor=%d session=%d did not stop in time (it holds no connection)", key.VendorID, key.Idx)
	}
	select {
	case <-h.done:
	case <-time.After(stopGrace):
		p.log.Printf("vendor=%d session=%d is still finishing after its connection was closed", key.VendorID, key.Idx)
	}
}

// stopGrace is how long a stopped session gets beyond its drain time.
var stopGrace = 2 * time.Second

// writerStopFor is how long a session that is ending waits for its record
// writer's last flush (serveVendor). With stopGrace it stays under the time
// stopSession gives a session beyond its drain.
const writerStopFor = 1500 * time.Millisecond

// Step lets the process also satisfy worker.Worker; Run takes precedence.
func (p *Process) Step(ctx context.Context) error { return nil }
