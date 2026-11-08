package smppvendor

import (
	"sync"
	"time"
)

// circuitBreakerConfig is one vendor's error-rate breaker tuning, read from
// protocol_config.circuit_breaker. ThresholdPct<=0 disables the breaker
// entirely — the default, so an unconfigured vendor behaves exactly as
// before this feature existed. [SCOPE P3-BREAKER]
type circuitBreakerConfig struct {
	ThresholdPct int // trip when failed*100 >= ThresholdPct*total, within WindowSec
	WindowSec    int
	CooldownSec  int
	MinSamples   int // never trip on a handful of unlucky sends
}

// breakerState is one vendor's rolling error-rate tracker, aggregated across
// ALL of that vendor's sessions — a vendor's health is a vendor-wide signal
// (a bad route or an upstream outage hits every session equally), not a
// per-TCP-connection one.
type breakerState struct {
	mu          sync.Mutex
	windowStart time.Time
	accepted    int
	failed      int
	openUntil   time.Time
}

func (p *Process) breakerFor(vendorID int64) *breakerState {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.breakers == nil {
		p.breakers = map[int64]*breakerState{}
	}
	b := p.breakers[vendorID]
	if b == nil {
		b = &breakerState{windowStart: time.Now()}
		p.breakers[vendorID] = b
	}
	return b
}

// recordOutcome feeds one submit_sm_resp result into the vendor's breaker,
// tripping it when the error rate crosses cfg.ThresholdPct within the
// rolling window. A no-op whenever the breaker is disabled for this vendor.
func (p *Process) recordOutcome(vendorID int64, cfg circuitBreakerConfig, ok bool) {
	if cfg.ThresholdPct <= 0 {
		return
	}
	b := p.breakerFor(vendorID)
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	win := time.Duration(cfg.WindowSec) * time.Second
	if win <= 0 {
		win = 60 * time.Second
	}
	if now.Sub(b.windowStart) > win {
		b.windowStart, b.accepted, b.failed = now, 0, 0
	}
	if ok {
		b.accepted++
	} else {
		b.failed++
	}
	total := b.accepted + b.failed
	minSamples := cfg.MinSamples
	if minSamples <= 0 {
		minSamples = 20
	}
	if total < minSamples {
		return
	}
	if b.failed*100 >= cfg.ThresholdPct*total {
		cooldown := time.Duration(cfg.CooldownSec) * time.Second
		if cooldown <= 0 {
			cooldown = 30 * time.Second
		}
		if now.After(b.openUntil) {
			// Read straight out of p.current rather than through a helper:
			// this function already holds p.mu, so anything that takes the
			// lock again would deadlock. A breaker trip is the single most
			// important line a vendor produces, so it belongs in that vendor's
			// own file, not only the shared system one.
			p.log.ForVendor(p.current[vendorID].cid).Errorf("breaker",
				"circuit breaker OPEN (%d/%d failed >= %d%%); pausing new sends for %s",
				b.failed, total, cfg.ThresholdPct, cooldown)
		}
		b.openUntil = now.Add(cooldown)
	}
}

// breakerOpen reports whether vendorID's breaker is currently tripped —
// callers stop CLAIMING new work for this vendor but leave any live bind up,
// since the fault is usually upstream routing/balance, not the TCP session.
func (p *Process) breakerOpen(vendorID int64) bool {
	p.mu.Lock()
	b := p.breakers[vendorID]
	p.mu.Unlock()
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Now().Before(b.openUntil)
}

// --- manual resume signal for max_reconnect_attempts (item 1 / [SCOPE P3-COMMAND]) ---

// requestReconnect flags vendorID for a manual resume — set by the
// /command/reconnect admin endpoint, consumed by the next poll of any
// session that gave up dialing after max_reconnect_attempts.
func (p *Process) requestReconnect(vendorID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.resumeRequested == nil {
		p.resumeRequested = map[int64]bool{}
	}
	p.resumeRequested[vendorID] = true
}

func (p *Process) consumeReconnect(vendorID int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.resumeRequested[vendorID] {
		delete(p.resumeRequested, vendorID)
		return true
	}
	return false
}

// forceReload flags the vendor table for an immediate re-scan on the next
// reconcile tick instead of waiting for reloadEvery — the admin-triggered
// "reload now" command.
func (p *Process) forceReload() {
	select {
	case p.reloadNow <- struct{}{}:
	default:
	}
}
