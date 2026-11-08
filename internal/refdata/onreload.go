package refdata

import "context"

// OnReload registers fn to run after every successful reload of the
// reference data — whatever caused it: the fingerprint moved on the timer,
// the Admin API's nudge (RefreshForce), or a new Redis snapshot. Caches built
// from the same config store follow the reference data with it instead of
// keeping a timer of their own: the accept path's rate cards (billing.Rater)
// used to reload only every 5 minutes, so a new price was reserved at the old
// one for minutes after the reference data already knew it (QA BILL-001).
// fn runs on the reloading goroutine, after the cache lock is released.
func (m *Mem) OnReload(fn func(context.Context)) {
	if fn == nil {
		return
	}
	m.mu.Lock()
	m.onReload = append(m.onReload, fn)
	m.mu.Unlock()
}

// reload reloads the data, then tells the followers.
func (m *Mem) reload(ctx context.Context) error {
	if err := m.reloadData(ctx); err != nil {
		return err
	}
	m.mu.RLock()
	fns := m.onReload
	m.mu.RUnlock()
	for _, fn := range fns {
		fn(ctx)
	}
	return nil
}
