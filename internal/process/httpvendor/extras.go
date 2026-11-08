package httpvendor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
)

// rowExtras reads an endpoint row's Advanced-tab settings onto it (QA
// EP-018): the screen offered TLS (JSON), Retry (JSON) and Circuit Breaker
// (JSON) for a vendor call, and this role loaded none of them.
//
// TLS fails closed: a row asking for a client certificate or its own CA that
// cannot be built is an error, so the vendor is skipped and listed as broken
// rather than called without the settings it asked for. An unreadable retry
// value is ignored, as the push retry policy does.
func rowExtras(ctx context.Context, row *httpadapter.EndpointRow, tlsJSON, retryJSON string) error {
	if t := strings.TrimSpace(tlsJSON); t != "" && t != "null" {
		if err := json.Unmarshal([]byte(t), &row.TLS); err != nil {
			return fmt.Errorf("tls is not valid JSON: %v", err)
		}
		tc, err := httpadapter.TLSConfig(ctx, row.TLS, config.Secrets)
		if err != nil {
			return err
		}
		row.TLSConfig = tc
	}
	if r := strings.TrimSpace(retryJSON); r != "" && r != "null" {
		_ = json.Unmarshal([]byte(r), &row.Retry)
	}
	return nil
}

// breakerConfig is a send row's Circuit Breaker (JSON). Two shapes are read:
// the one P3 uses for SMPP vendors — {"threshold_pct", "window_sec",
// "cooldown_sec", "min_samples"}, trip on an error RATE — and the screen's own
// example — {"error_threshold": 5, "open_sec": 30}, trip on that many failed
// calls IN A ROW. Neither set = no breaker, as before.
type breakerConfig struct {
	thresholdPct int
	minSamples   int
	window       time.Duration
	consecutive  int
	cooldown     time.Duration
}

func (c breakerConfig) on() bool { return c.thresholdPct > 0 || c.consecutive > 0 }

func parseBreaker(raw string) breakerConfig {
	var m map[string]any
	if t := strings.TrimSpace(raw); t == "" || t == "null" || json.Unmarshal([]byte(t), &m) != nil {
		return breakerConfig{}
	}
	num := func(keys ...string) int {
		for _, k := range keys {
			switch v := m[k].(type) {
			case float64:
				return int(v)
			case string:
				if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
					return n
				}
			}
		}
		return 0
	}
	c := breakerConfig{
		thresholdPct: num("threshold_pct"),
		minSamples:   num("min_samples"),
		window:       time.Duration(num("window_sec")) * time.Second,
		consecutive:  num("error_threshold", "consecutive_failures"),
		cooldown:     time.Duration(num("open_sec", "cooldown_sec")) * time.Second,
	}
	if c.thresholdPct > 100 {
		c.thresholdPct = 100
	}
	if c.minSamples <= 0 {
		c.minSamples = 20
	}
	if c.window <= 0 {
		c.window = time.Minute
	}
	if c.cooldown <= 0 {
		c.cooldown = 30 * time.Second
	}
	return c
}

// sendBreaker reads the send row's circuit_breaker column.
func (p *Process) sendBreaker(ctx context.Context, sendID int64) breakerConfig {
	raw, err := p.traffic.VendorCalls().CircuitBreaker(ctx, sendID)
	if err != nil {
		return breakerConfig{}
	}
	return parseBreaker(raw)
}

// breakers holds every vendor's breaker state. A failure is a call that got
// no usable answer — the same thing that marks a vendor unhealthy (a network
// error, a timeout, a retry code, a "not OK" answer) — never a refusal of one
// message, which is the vendor working.
type breakers struct {
	mu sync.Mutex
	m  map[int64]*breakerState
}

type breakerState struct {
	windowStart time.Time
	ok, failed  int
	streak      int
	openUntil   time.Time
}

func newBreakers() *breakers { return &breakers{m: map[int64]*breakerState{}} }

// note feeds one call's outcome in. It returns a description when this call
// tripped the breaker, for the vendor's log.
func (b *breakers) note(vendorID int64, cfg breakerConfig, failed bool, now time.Time) string {
	if b == nil || !cfg.on() {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.m[vendorID]
	if s == nil {
		s = &breakerState{windowStart: now}
		b.m[vendorID] = s
	}
	if now.Sub(s.windowStart) > cfg.window {
		s.windowStart, s.ok, s.failed = now, 0, 0
	}
	if failed {
		s.failed++
		s.streak++
	} else {
		s.ok++
		s.streak = 0
	}
	trip := ""
	if cfg.consecutive > 0 && s.streak >= cfg.consecutive {
		trip = fmt.Sprintf("%d calls failed in a row", s.streak)
	}
	if total := s.ok + s.failed; trip == "" && cfg.thresholdPct > 0 && total >= cfg.minSamples &&
		s.failed*100 >= cfg.thresholdPct*total {
		trip = fmt.Sprintf("%d of %d calls failed in %s (>= %d%%)", s.failed, total, cfg.window, cfg.thresholdPct)
	}
	if trip == "" {
		return ""
	}
	wasOpen := now.Before(s.openUntil)
	s.openUntil = now.Add(cfg.cooldown)
	// Start counting afresh after the pause, so the first call after it is
	// judged on its own and not on the failures that opened the breaker.
	s.windowStart, s.ok, s.failed, s.streak = now, 0, 0, 0
	if wasOpen {
		return ""
	}
	return trip
}

// open reports whether the vendor's breaker is pausing new sends.
func (b *breakers) open(vendorID int64, now time.Time) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.m[vendorID]
	return s != nil && now.Before(s.openUntil)
}

// keep drops the state of vendors no longer served.
func (b *breakers) keep(served map[int64]bool) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for id := range b.m {
		if !served[id] {
			delete(b.m, id)
		}
	}
}
