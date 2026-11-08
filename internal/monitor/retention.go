package monitor

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Monitoring history retention (PO 2026-09-11): raw 5-second samples are kept
// for RetentionDays (default 7) and then dropped. No rollups.
//
// Expiry is a partition DROP, not a DELETE. A week of fifteen roles is ~1.8
// million rows; deleting a day of them row by row is minutes of undo log and
// lock pressure on the same database the traffic path writes to, while
// dropping a partition is a metadata change that takes milliseconds.
//
// That needs daily partitions to exist, and 0033 created none — only p_start
// and p_max, so every row landed in p_max. Maintain therefore does both jobs:
// it splits p_max so the next few days each have a partition waiting, and it
// drops the days that have aged out (the DAL's KeepSampleDays, where the
// partitions are worked out). Splitting an EMPTY p_max is instant; the one
// expensive split is the first, which moves whatever history exists into a
// single partition covering everything up to today.
//
// A batched DELETE backs the drop up, for two cases a drop cannot reach: that
// first partition (it covers several days, so it only becomes droppable once
// its NEWEST day expires), and a deployment whose database user may not ALTER.
//
// Every role has a sink, so every role would run this. A MySQL named lock
// makes exactly one of them do it each hour; the rest find the lock taken
// and go back to sampling.

// DefaultRetentionDays is used when the deployment does not say: every
// role's full 5-second reports are kept this long (Saleem 2026-10-05, D-126 —
// was 7; nothing read past one day, and the history the portal moves through
// is now 3 days of detail plus day summaries).
const DefaultRetentionDays = 3

// DefaultSummaryDays is how long day summaries and events are kept
// (YAKU_MONITOR_SUMMARY_DAYS).
const DefaultSummaryDays = 10

// MinPushInterval is the shortest gap between a role's reports
// (YAKU_STATUS_PUSH_SEC is raised to it): the finest moment the monitoring
// screens can show.
const MinPushInterval = 5 * time.Second

const (
	maintainLock     = "yaku_monitor_maintain"
	deleteBatch      = 5000 // rows per DELETE in the fallback
	deleteBatchesMax = 200  // at most 1M rows per run; the rest wait an hour
)

// WithRetention sets how many days of samples to keep. 0 keeps everything
// and disables maintenance.
func (s *SQLSink) WithRetention(days int) *SQLSink {
	if s != nil {
		s.retentionDays = days
	}
	return s
}

// MaintainResult reports what one pass did, for the log line.
type MaintainResult struct {
	Skipped      bool // another instance held the lock
	Created      []string
	Dropped      []string
	Deleted      int64
	PartitionErr error // ALTER refused; the DELETE fallback still ran
	Summarized   int   // day summaries written (days.go)
	SummaryErr   error // summaries not written this pass; nothing is dropped then
	Trimmed      int64 // day summaries and events past their keep
}

// Maintain runs one retention pass. Safe to call from every instance at once:
// a named lock of the hot store's database makes one of them do it.
func (s *SQLSink) Maintain(ctx context.Context, now time.Time) (MaintainResult, error) {
	var res MaintainResult
	if s == nil || s.h == nil || (s.retentionDays <= 0 && s.summaryDays <= 0) {
		res.Skipped = true
		return res, nil
	}
	got, err := s.h.Locked(ctx, maintainLock, func(q dal.MonitorStatements) error {
		return s.maintain(ctx, q, now, &res)
	})
	if err == nil && !got {
		res.Skipped = true
	}
	return res, err
}

// maintain is the pass, on the connection that holds the lock.
func (s *SQLSink) maintain(ctx context.Context, q dal.MonitorStatements, now time.Time, res *MaintainResult) error {
	// Day summaries FIRST: a day is summarized while its reports still exist,
	// and a day whose summary could not be written is not dropped this pass.
	res.Summarized, res.SummaryErr = s.summarizeDays(ctx, q, now)
	if res.SummaryErr == nil {
		res.Trimmed, res.SummaryErr = s.trimSummaries(ctx, q, now)
	}
	if s.retentionDays <= 0 {
		return nil
	}

	today := now.UTC()
	cutoff := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC).
		AddDate(0, 0, -s.retentionDays)

	// Keep every day until its summary is written.
	res.Created, res.Dropped, res.PartitionErr = q.KeepSampleDays(ctx, today, cutoff, res.SummaryErr != nil)
	if res.SummaryErr != nil {
		return nil
	}

	// The fallback. Cheap when there is nothing to do: sampled_at is indexed
	// and partition pruning skips every day that is still in retention.
	for i := 0; i < deleteBatchesMax; i++ {
		n, err := q.DeleteSamplesBefore(ctx, cutoff, deleteBatch)
		if err != nil {
			return err
		}
		res.Deleted += n
		if n < deleteBatch {
			break
		}
	}
	return nil
}

// runMaintenance runs Maintain once a minute after start and hourly after
// that, on its own goroutine so it can never delay a sample.
func (s *SQLSink) runMaintenance(ctx context.Context, role string, logf func(string, ...any)) {
	if s == nil || (s.retentionDays <= 0 && s.summaryDays <= 0) {
		return
	}
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		c, cancel := context.WithTimeout(ctx, 10*time.Minute)
		res, err := s.Maintain(c, time.Now())
		cancel()
		if logf != nil {
			switch {
			case err != nil:
				logf("monitor retention pass failed (history keeps growing until it succeeds): %v", err)
			case res.Skipped:
				// another instance has it this hour
			default:
				if res.PartitionErr != nil {
					logf("monitor retention: partition upkeep refused (%v) — expired rows deleted instead, which is slower", res.PartitionErr)
				}
				if len(res.Created)+len(res.Dropped) > 0 || res.Deleted > 0 {
					logf("monitor retention (%d days, run by %s): partitions added %v, dropped %v, %d old row(s) deleted",
						s.retentionDays, role, res.Created, res.Dropped, res.Deleted)
				}
				if res.SummaryErr != nil {
					logf("monitor day summaries not written (%v) — no day is dropped until they are", res.SummaryErr)
				} else if res.Summarized > 0 || res.Trimmed > 0 {
					logf("monitor day summaries (kept %d days, run by %s): %d written, %d old summary/event row(s) removed",
						s.summaryDays, role, res.Summarized, res.Trimmed)
				}
			}
		}
		timer.Reset(time.Hour)
	}
}
