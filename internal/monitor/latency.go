package monitor

// Latency is a fixed-size ring of the most recent per-message pipeline
// durations, feeding the /metrics p95 gauge (P1 scope finalize 2026-08-09 §1:
// p95 < 30 ms measured per segment/pipeline run, not per HTTP hit). A ring of
// the last ringSize samples keeps Add O(1) on the hot path; Quantile copies
// and sorts only on scrape, which is rare. "Recent" rather than
// lifetime-windowed is deliberate — an SLA gauge should reflect current
// behaviour, and 4096 samples at target load spans well under a second, at
// idle it simply holds the latest traffic seen.

import (
	"sort"
	"sync"
	"time"
)

const ringSize = 4096

// Latency records durations and answers quantiles over the most recent
// ringSize samples.
type Latency struct {
	mu      sync.Mutex
	samples [ringSize]int64 // nanoseconds
	head    int
	filled  int
}

// Add records one pipeline duration.
func (l *Latency) Add(d time.Duration) {
	l.mu.Lock()
	l.samples[l.head] = int64(d)
	l.head = (l.head + 1) % ringSize
	if l.filled < ringSize {
		l.filled++
	}
	l.mu.Unlock()
}

// Quantile returns the q-th quantile (0 < q <= 1) of the recent samples in
// seconds, ok=false when nothing has been recorded yet.
// Len is how many samples back the quantiles. A p95 over three requests is
// arithmetic, not a measurement, and a dashboard should be able to say so.
func (l *Latency) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.filled
}

func (l *Latency) Quantile(q float64) (float64, bool) {
	l.mu.Lock()
	n := l.filled
	if n == 0 {
		l.mu.Unlock()
		return 0, false
	}
	buf := make([]int64, n)
	if n < ringSize {
		copy(buf, l.samples[:n])
	} else {
		copy(buf, l.samples[:])
	}
	l.mu.Unlock()
	sort.Slice(buf, func(i, j int) bool { return buf[i] < buf[j] })
	idx := int(q*float64(n)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return float64(buf[idx]) / float64(time.Second), true
}
