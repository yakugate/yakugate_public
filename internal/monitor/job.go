package monitor

import (
	"sort"
	"sync"
	"time"
)

// Job reports a background worker's runs — retry and expiry (continuous
// sweeps), retention and backup (periodic batches). All four used to publish
// nothing: backup wrote 162 tables and the only trace was a log line, and
// retry/expiry had not logged a word since start-up. A job that has silently
// stopped running looks exactly like a job with nothing to do, unless
// something records that it ran.
type Job struct {
	// Done counts the units of work (messages dead-lettered, tables archived);
	// Errors counts failures inside runs.
	Done   Counter
	Errors Counter

	mu         sync.Mutex
	runs       int64
	lastAt     time.Time
	lastTook   time.Duration
	lastResult map[string]any
	lastErr    string
	lastErrAt  time.Time
	every      time.Duration
	byKey      map[string]int64
}

// SetInterval records how often the job is meant to run, so the section can
// say when the next run is due — and a reader can see one is overdue.
func (j *Job) SetInterval(d time.Duration) {
	j.mu.Lock()
	j.every = d
	j.mu.Unlock()
}

// NoteKey adds n to a per-key running total (per vendor, per table family).
func (j *Job) NoteKey(key string, n int64) {
	if n == 0 {
		return
	}
	j.mu.Lock()
	if j.byKey == nil {
		j.byKey = map[string]int64{}
	}
	j.byKey[key] += n
	j.mu.Unlock()
}

// Finish records one run: when it started, what it found, and the error that
// ended it if any. result is copied as given; keep it small.
func (j *Job) Finish(started time.Time, result map[string]any, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.runs++
	j.lastAt = started
	j.lastTook = time.Since(started)
	j.lastResult = result
	if err != nil {
		msg := err.Error()
		if len(msg) > 160 {
			msg = msg[:160] + "…"
		}
		j.lastErr, j.lastErrAt = msg, time.Now()
	}
}

// NoteError records a failure inside a run that did not end it.
func (j *Job) NoteError(err error) {
	if err == nil {
		return
	}
	j.Errors.Add(1)
	msg := err.Error()
	if len(msg) > 160 {
		msg = msg[:160] + "…"
	}
	j.mu.Lock()
	j.lastErr, j.lastErrAt = msg, time.Now()
	j.mu.Unlock()
}

// Section is the job's snapshot block.
func (j *Job) Section() map[string]any {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := map[string]any{
		"runs":   j.runs,
		"done":   j.Done.Snapshot(0),
		"errors": j.Errors.Snapshot(0),
	}
	if !j.lastAt.IsZero() {
		out["last_run_at"] = j.lastAt.UTC().Format(time.RFC3339)
		out["last_run_ms"] = j.lastTook.Milliseconds()
		if j.every > 0 {
			next := j.lastAt.Add(j.every)
			out["next_run_at"] = next.UTC().Format(time.RFC3339)
			// Overdue by more than a full interval means the job is stuck or
			// the process is starved — the state nobody notices otherwise.
			out["overdue"] = time.Since(next) > j.every
		}
	}
	if j.lastResult != nil {
		out["last_result"] = j.lastResult
	}
	if j.lastErr != "" {
		out["last_error"] = j.lastErr
		out["last_error_at"] = j.lastErrAt.UTC().Format(time.RFC3339)
	}
	if len(j.byKey) > 0 {
		keys := make([]string, 0, len(j.byKey))
		for k := range j.byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		m := make(map[string]int64, len(keys))
		for _, k := range keys {
			m[k] = j.byKey[k]
		}
		out["by_key"] = m
	}
	return out
}
