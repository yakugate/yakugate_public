package hlr

// HLR LIVE DIP — synchronous, guarded, hot-path (P1 scope finalize 2026-08-09,
// PO-ruled "dip in the hot path"). On a cache miss the submit goroutine resolves
// the number's network by calling the external HLR provider INLINE, then fills
// MCC/MNC and proceeds. That is only safe because three guards keep a cold
// campaign or a dead provider from stalling the whole gateway:
//
//	(a) IN-FLIGHT GUARD  — concurrent dips for the SAME number coalesce into one
//	    provider call (singleflight). 100 messages to one new number => 1 dip.
//	(b) PER-DIP TIMEOUT  — a slow/dead provider yields a MISS fast (the message
//	    routes undetected), never hanging the request past the latency budget.
//	(c) MAX-INFLIGHT CEILING — a burst of UNIQUE new numbers beyond the ceiling
//	    does NOT queue and wait; those requests fall back to an async fill so the
//	    provider is never hit with thousands of simultaneous calls.
//
// A successful dip is written to BOTH the DB (persist) and the in-memory cache
// (Put), so the next message to that number is a fast cache hit. When a
// synchronous dip is skipped (ceiling) or fails (timeout/transport error), a
// best-effort ASYNC fill warms the store for next time; this message still went
// out on undetected-network routing, never blocked.

import (
	"context"
	"time"

	"golang.org/x/sync/singleflight"
)

// Provider resolves an MSISDN against the external HLR service. ok=false means
// "not found" (a definitive answer, no retry); a non-nil error means a
// transport/timeout failure (worth an async retry).
type Provider interface {
	Dip(ctx context.Context, msisdn string) (Entry, bool, error)
}

// Writer persists a dipped entry. Implementations: hlr.Cache (in-memory
// update) and the mysql shard persister; MultiWriter fans out to both so a dip
// updates the DB and the cache together.
type Writer interface {
	Put(Entry)
}

// MultiWriter fans a Put out to every writer (skips nils), so the dipper can
// update the cache and the DB with one call.
func MultiWriter(ws ...Writer) Writer {
	var out multiWriter
	for _, w := range ws {
		if w != nil {
			out = append(out, w)
		}
	}
	return out
}

type multiWriter []Writer

func (m multiWriter) Put(e Entry) {
	for _, w := range m {
		w.Put(e)
	}
}

// Dipper performs guarded synchronous dips with an async fallback.
type Dipper struct {
	provider Provider
	writer   Writer
	timeout  time.Duration
	sem      chan struct{}      // hot-path ceiling: buffered to maxInflight (guard c)
	asyncSem chan struct{}      // separate, bounded pool for async fills
	group    singleflight.Group // in-flight guard (guard a), keyed by msisdn
	logf     func(string, ...any)
	// differs (6 Nov 2026): the number's range names another network than the
	// answer's — the number is ported, whatever the provider said. nil = not
	// checked.
	differs func(msisdn, mcc, mnc string) bool
}

// portedByRange marks an answer ported when the number's range is another
// network's (before it is saved, so our HLR data keeps it).
func (d *Dipper) portedByRange(e *Entry) {
	if !e.Ported && d.differs != nil && d.differs(e.MSISDN, e.MCC, e.MNC) {
		e.Ported = true
	}
}

// NewDipper builds a Dipper. timeout is the per-dip ceiling
// (YAKU_HLR_DIP_TIMEOUT_MS); maxInflight is the max concurrent provider calls
// on the hot path (YAKU_HLR_MAX_INFLIGHT). Both are clamped to sane minimums.
func NewDipper(p Provider, w Writer, timeout time.Duration, maxInflight int, logf func(string, ...any)) *Dipper {
	if timeout <= 0 {
		timeout = 300 * time.Millisecond
	}
	if maxInflight < 1 {
		maxInflight = 32
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	// The async pool is deliberately smaller than the hot-path ceiling: async
	// fills are opportunistic warming, not latency-critical, so they must never
	// out-compete live traffic for provider capacity.
	asyncCap := maxInflight / 2
	if asyncCap < 1 {
		asyncCap = 1
	}
	return &Dipper{
		provider: p, writer: w, timeout: timeout,
		sem:      make(chan struct{}, maxInflight),
		asyncSem: make(chan struct{}, asyncCap),
		logf:     logf,
	}
}

// Dip resolves a number on the hot path. Returns (entry, true) on a successful
// dip; (zero, false) on a miss/timeout/ceiling — in which case the caller
// proceeds with undetected-network routing and a background fill has been
// queued when appropriate.
func (d *Dipper) Dip(ctx context.Context, msisdn string) (Entry, bool) {
	e, found, _ := d.Try(ctx, msisdn)
	return e, found
}

// Try is Dip that also says whether the provider was actually asked. attempted
// is false only when the hot-path ceiling was full and no call was made, so a
// caller billing failed lookups (sms_service_config.charge_failed_hlr) never
// charges for a lookup that did not happen. A definitive not-found, a
// timeout and a transport error all count as attempted.
func (d *Dipper) Try(ctx context.Context, msisdn string) (e Entry, found, attempted bool) {
	// Guard (c): try to take a hot-path slot WITHOUT blocking. If the ceiling
	// is full, do not queue behind live traffic — fall back to async.
	select {
	case d.sem <- struct{}{}:
		defer func() { <-d.sem }()
	default:
		d.asyncFill(msisdn)
		return Entry{}, false, false
	}

	// Guard (a): coalesce concurrent dips for the same number into one call.
	v, err, _ := d.group.Do(msisdn, func() (any, error) {
		// Guard (b): per-dip timeout, layered under any shorter request deadline.
		cctx, cancel := context.WithTimeout(ctx, d.timeout)
		defer cancel()
		e, ok, derr := d.provider.Dip(cctx, msisdn)
		// The call goes back with every outcome, for the lookup's log row.
		if derr != nil {
			return dipResult{entry: Entry{Call: e.Call}}, derr
		}
		if !ok {
			return dipResult{entry: Entry{Call: e.Call}}, nil // definitive not-found
		}
		e.CheckedAt = time.Now() // restarts the number's cache time limit
		d.portedByRange(&e)
		d.put(e) // updates DB + cache together (MultiWriter)
		return dipResult{entry: e, found: true}, nil
	})
	res, _ := v.(dipResult)
	if err != nil {
		// Transport/timeout: this message goes undetected; warm the number for
		// next time so the failure is not repeated on every message.
		d.logf("[hlr-dip] %s failed (%v) — undetected-network + async fill", msisdn, err)
		d.asyncFill(msisdn)
		return Entry{Call: res.entry.Call}, false, true
	}
	return res.entry, res.found, true
}

// put saves an answer without its call: the call belongs to the lookup's
// log row, never to the number kept in the cache and the HLR data.
func (d *Dipper) put(e Entry) {
	e.Call = nil
	d.writer.Put(e)
}

type dipResult struct {
	entry Entry
	found bool
}

// asyncFill best-efforts a background dip to warm the store, bounded by its own
// small pool. Coalesced with any in-flight sync dip for the same number via the
// shared singleflight group. If the async pool is full it simply drops — a
// later message to the same number re-triggers it.
func (d *Dipper) asyncFill(msisdn string) {
	select {
	case d.asyncSem <- struct{}{}:
	default:
		return
	}
	go func() {
		defer func() { <-d.asyncSem }()
		_, _, _ = d.group.Do(msisdn, func() (any, error) {
			cctx, cancel := context.WithTimeout(context.Background(), d.timeout)
			defer cancel()
			e, ok, err := d.provider.Dip(cctx, msisdn)
			if err == nil && ok {
				e.CheckedAt = time.Now()
				d.portedByRange(&e)
				d.put(e)
			}
			return nil, err
		})
	}()
}
