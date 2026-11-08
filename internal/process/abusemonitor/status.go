package abusemonitor

import (
	"sync"
	"time"
)

// sweepStats is what the fleet status page shows for this role
// (monitor.SectionProvider, [SCOPE P1-STATUS]): when the last sweep ran and
// what it has found/done since process start. Updated by Step/act, read by
// the admin listener and the Redis status push on their own goroutines.
type sweepStats struct {
	mu        sync.Mutex
	lastSweep time.Time
	sweeps    int64
	findings  map[string]int64 // per signal, since start
	actions   map[string]int64 // per action (abuse.warn/throttle/suspend), since start
	// notes are what is wrong with a rule right now, by rule: its params do
	// not parse, it lacks a key it needs, accounts it could not judge. A
	// note goes when the fault does. notedAt is when each was last logged.
	notes   map[string]string
	notedAt map[string]time.Time
	// source is where the last sweep took its figures from: records,
	// counters or compare (counters.go).
	source string
}

func (s *sweepStats) setSource(src string) {
	s.mu.Lock()
	s.source = src
	s.mu.Unlock()
}

// noteEvery is how often an unchanged note is logged again.
const noteEvery = 30 * time.Minute

// setNote records a rule's fault and reports whether to log it: when it is
// new, when its wording changed, or when it has not been said for noteEvery.
func (s *sweepStats) setNote(key, msg string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.notes == nil {
		s.notes, s.notedAt = map[string]string{}, map[string]time.Time{}
	}
	if s.notes[key] == msg && now.Sub(s.notedAt[key]) < noteEvery {
		return false
	}
	s.notes[key], s.notedAt[key] = msg, now
	return true
}

func (s *sweepStats) clearNote(key string) {
	s.mu.Lock()
	delete(s.notes, key)
	delete(s.notedAt, key)
	s.mu.Unlock()
}

func (s *sweepStats) sweep(now time.Time) {
	s.mu.Lock()
	s.lastSweep = now
	s.sweeps++
	s.mu.Unlock()
}

func (s *sweepStats) finding(signal string) {
	s.mu.Lock()
	if s.findings == nil {
		s.findings = map[string]int64{}
	}
	s.findings[signal]++
	s.mu.Unlock()
}

func (s *sweepStats) action(action string) {
	s.mu.Lock()
	if s.actions == nil {
		s.actions = map[string]int64{}
	}
	s.actions[action]++
	s.mu.Unlock()
}

// MonitorSections implements monitor.SectionProvider — the "abuse" section on
// this role's /status (admin port) and in the cluster view.
func (p *Process) MonitorSections() map[string]any {
	p.stats.mu.Lock()
	defer p.stats.mu.Unlock()
	last := ""
	if !p.stats.lastSweep.IsZero() {
		last = p.stats.lastSweep.UTC().Format(time.RFC3339)
	}
	findings := map[string]int64{}
	for k, v := range p.stats.findings {
		findings[k] = v
	}
	actions := map[string]int64{}
	for k, v := range p.stats.actions {
		actions[k] = v
	}
	notes := map[string]string{}
	for k, v := range p.stats.notes {
		notes[k] = v
	}
	return map[string]any{
		"abuse": map[string]any{
			"last_sweep":   last,
			"sweeps":       p.stats.sweeps,
			"interval_sec": int(p.interval / time.Second),
			// Where the figures come from: records | counters | compare.
			"source":   p.stats.source,
			"findings": findings,
			"actions":  actions,
			// A rule that is not running, and why (decodeParams). Empty = all run.
			"rule_notes": notes,
		},
	}
}
