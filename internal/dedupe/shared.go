package dedupe

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Shared is the duplicate check every gateway process shares, kept by the
// DAL (dal.Dedupe): the main Redis whenever it is set (ONE command a
// message, `SET key <msg id> NX GET EX <ttl>`), else the config store's
// dedupe_keys on MySQL (one statement a new message). Until 29 Oct 2026
// these were dedupe.Redis and dedupe.MySQL; their code moved into
// internal/dal/redis and internal/dal/mysql unchanged.
type Shared struct {
	d        dal.Dedupe
	fallback time.Duration // used when Check's caller passes ttl<=0
}

// NewShared checks on the DAL's keys. ttl is the fallback used when a caller
// passes ttl<=0 to Check.
func NewShared(d dal.Dedupe, ttl time.Duration) *Shared { return &Shared{d: d, fallback: ttl} }

// Check creates key holding msgID for ttl: firstID "" = new, else the id of
// the message it duplicates. The window is whole seconds, at least one.
func (s *Shared) Check(ctx context.Context, key, msgID string, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		ttl = s.fallback
	}
	return s.d.Check(ctx, key, msgID, time.Duration(windowSec(ttl))*time.Second)
}

// Release removes the key of a message that was never queued, only while it
// holds that message's id.
func (s *Shared) Release(ctx context.Context, key, msgID string) error {
	return s.d.Release(ctx, key, msgID)
}

// StartSweep periodically clears keys long expired, for a store whose keys
// do not end by themselves (dal.DedupeSweeper: the MySQL table); Redis keys
// carry their own expiry and it returns at once. Runs until ctx is
// cancelled.
func (s *Shared) StartSweep(ctx context.Context, interval time.Duration, logf func(format string, args ...any)) {
	sw, ok := s.d.(dal.DedupeSweeper)
	if !ok {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := sw.SweepDedupe(ctx)
			if err != nil {
				if logf != nil {
					logf("[dedupe] sweep failed: %v", err)
				}
				continue
			}
			if n > 0 && logf != nil {
				logf("[dedupe] sweep removed %d stale key(s)", n)
			}
		}
	}
}
