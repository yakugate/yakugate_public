package dlr

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
)

// Re-matching filed receipts (2026-09-16).
//
// Apply files a receipt it cannot correlate as an orphan. Most orphans are
// exactly what the name says — foreign, or so stale their message is long
// finished — but one kind is ours: a receipt that beat its own record row into
// the database. The vendor roles write their `sent` records in batches, while
// an incoming receipt is applied at once, so a fast vendor's receipt can ask
// about a message the database has not been told about yet. Apply's own short
// wait covers the usual gap; this sweep covers a slow flush, a stalled batch
// writer, and everything filed before the wait existed.
//
// A re-matched receipt is applied to its message exactly as if it had arrived
// then, and its orphan row is dropped: the receipt is on the message's raw_dlr
// history and its dlrlog row, and a row left here would claim a receipt we
// could not place, which is no longer true.
//
// Order is safe without any extra care. Correlation resolves only messages
// still awaiting delivery truth, so a stale receipt replayed after its message
// reached a terminal status matches nothing and stays an orphan — an ENROUTE
// replayed after its DELIVRD cannot walk the message backwards.

// OrphanReplayer is an optional Store capability: read filed receipts back
// (those not placed yet) and mark one placed once it has been: the orphan row
// itself stays, the logs are written once (issue 14). Stores without it keep
// the old behaviour, where an orphan is final.
type OrphanReplayer interface {
	PendingOrphanDLRs(ctx context.Context, since time.Time, limit int) ([]model.OrphanDLR, error)
	MarkOrphanMatched(ctx context.Context, table string, id int64, msgID string) error
}

// Leaser is an optional Store capability: a named lock that one process of
// the whole gateway holds at a time, given back by release
// (mysql.Store.TryLease: GET_LOCK on the record store's server, named for its
// database). ok=false: another process holds it.
type Leaser interface {
	TryLease(ctx context.Context, name string) (release func(), ok bool, err error)
}

// rematchLease is the lock a re-match pass holds: one pass at a time,
// gateway-wide.
//
// Every dlr role runs the sweep — one on each deployment, and the orphans
// are in the one record store they share. Two passes read the same filed
// receipts in the same moment and each applied them: a receipt got two rows
// and, before the "final already" row was written unpriced, two charges (37
// in a 30-minute run with two deployments, 3,855 when MySQL was saturated;
// 388 second DELIVRD rows after a 20 s MySQL cut, go-live test of 18-19 Oct
// 2026). A pass reads the orphans, applies each and drops its row before it
// gives the lock up, so the next pass — here or on the other deployment —
// does not see what this one applied.
const rematchLease = "yaku.orphan.rematch"

// RematchOrphans tries every receipt filed since `since` against the records
// as they stand now, and reports how many found their message. Receipts that
// still match nothing are left where they are.
//
// The store's read is bounded by `limit`, so one pass is a known amount of
// work however many orphans are filed; the next pass picks up where the cap
// cut this one short.
func (s *Service) RematchOrphans(ctx context.Context, since time.Time, limit int) (int, error) {
	return s.rematch(ctx, since, limit, nil)
}

// orphanBackoff spaces out the tries of a filed receipt that keeps matching
// nothing: after each miss it waits twice as long, from first up to max.
//
// The sweep used to try every receipt of the last hour again on every pass —
// two or more lookups each, every five seconds: with a few hundred filed,
// hundreds of statements a second for receipts that are almost always not
// ours (2026-10-13). A receipt whose record is only late matches on the
// first or second try, which this leaves as quick as it was.
type orphanBackoff struct {
	first, max time.Duration
	wait       map[string]orphanWait
}

type orphanWait struct {
	next time.Time
	step time.Duration
}

func orphanKey(o model.OrphanDLR) string { return o.Table + ":" + strconv.FormatInt(o.ID, 10) }

// due says whether this receipt may be tried now.
func (b *orphanBackoff) due(key string, now time.Time) bool {
	w, ok := b.wait[key]
	return !ok || !now.Before(w.next)
}

// missed notes a try that found nothing: the next one is twice as far off.
func (b *orphanBackoff) missed(key string, now time.Time) {
	step := b.first
	if w, ok := b.wait[key]; ok {
		if step = 2 * w.step; step > b.max {
			step = b.max
		}
	}
	if b.wait == nil {
		b.wait = map[string]orphanWait{}
	}
	b.wait[key] = orphanWait{next: now.Add(step), step: step}
}

// keep forgets every receipt not in seen (matched, or past the window).
func (b *orphanBackoff) keep(seen map[string]bool) {
	for k := range b.wait {
		if !seen[k] {
			delete(b.wait, k)
		}
	}
}

// rematch is RematchOrphans, with the tries of a receipt that keeps missing
// spaced out when back is given (the sweep's own; nil = try every one).
func (s *Service) rematch(ctx context.Context, since time.Time, limit int, back *orphanBackoff) (matched int, err error) {
	r, ok := s.store.(OrphanReplayer)
	if !ok {
		return 0, nil
	}
	// One pass at a time, gateway-wide (rematchLease). Not held: another
	// process is in a pass, and its pass covers these orphans. An error: the
	// store cannot be asked, and nothing could be applied anyway.
	if l, ok := s.store.(Leaser); ok {
		release, held, err := l.TryLease(ctx, rematchLease)
		if err != nil {
			return 0, err
		}
		if !held {
			return 0, nil
		}
		defer release()
	}
	orphans, err := r.PendingOrphanDLRs(ctx, since, limit)
	if err != nil {
		return 0, err
	}
	// matched counts the orphans placed (named: the batch adds its own on the way out).
	now := time.Now()
	seen := map[string]bool{}
	if back != nil {
		// On every way out: what was not read this pass is forgotten.
		defer func() { back.keep(seen) }()
		for _, o := range orphans {
			seen[orphanKey(o)] = true
		}
	}
	// Without Redis the plain finals of a pass are closed together and their
	// orphans marked together (closemany.go): one at a time each was a close,
	// a client receipt and a mark, three commits behind the summary rows
	// every writer updates — about ten orphans a second on the rig's mix,
	// which left the clients' receipts minutes late (30 Oct 2026).
	mc, many := s.manyCloser()
	var coll *closeBatch
	var placed []int // orphans applied whose closes wait in coll
	var errs []error
	if many {
		coll = &closeBatch{}
		errs = make([]error, len(orphans))
		defer func() {
			// Each way out: what was applied is closed and marked.
			matched += s.placeMany(context.WithoutCancel(ctx), r, mc, coll, orphans, placed, errs)
		}()
	}
	for i, o := range orphans {
		if ctx.Err() != nil {
			return matched, ctx.Err()
		}
		if coll != nil {
			coll.at = i
		}
		key := orphanKey(o)
		if s.markQueued(key, now, false) {
			continue // applied already: its mark waits in the record queue
		}
		if back != nil && !back.due(key, now) {
			continue // tried a moment ago and missed: its turn comes later
		}
		// A receipt cannot be about a message that did not exist when it
		// came in (load test 2026-10-06). Vendors hand the same id out again
		// — after a restart, or once a counter wraps — and for an hour this
		// sweep offered every filed receipt to whatever message held its id
		// NOW: an old UNDELIV closed a message accepted minutes after it and
		// the client was told a delivered message had failed.
		if !s.couldBeAbout(ctx, o) {
			if back != nil {
				back.missed(key, now)
			}
			continue // it stays filed: its own message never turned up
		}
		// No grace on the retry: this row has already waited far longer than
		// Apply's wait, so another pause would buy nothing. And no filing: it
		// IS the filed row, so a second copy would be written on every pass.
		// Not cut by a stop: a receipt half applied could close its message
		// and leave the client untold. A stop ends the pass between two
		// orphans (above).
		work := context.WithoutCancel(ctx)
		ok, err := s.applyTo(work, receiptKey{vendorID: o.VendorID, id: o.VendorMsgID}, o.Status, o.ErrorCode, o.Raw, nil, time.Time{}, 0, false, coll)
		if err != nil {
			return matched, err
		}
		if !ok {
			if back != nil {
				back.missed(key, now)
			}
			continue // still nothing of ours — it stays filed
		}
		if coll != nil {
			placed = append(placed, i) // closed and marked with the pass's others
			continue
		}
		// Applied. Mark the orphan placed, but do not let a failed mark undo
		// the match: the receipt is already on the message.
		if s.writes != nil {
			// Worker mode: the mark is a task of the record queue, written a
			// moment later; until then this orphan is still read as pending,
			// so it is remembered here and not applied a second time.
			if err := s.writes.MarkOrphanMatched(work, o.Table, o.ID, ""); err != nil {
				return matched, err
			}
			s.markQueued(key, now, true)
		} else if err := r.MarkOrphanMatched(work, o.Table, o.ID, ""); err != nil {
			return matched, err
		}
		matched++
	}
	return matched, nil
}

// markedFor is how long an orphan applied in worker mode is not offered
// again while its "matched" mark waits in the record queue.
const markedFor = 10 * time.Minute

// markQueued remembers (add) or asks (not add) whether an orphan was applied
// and its mark queued in the last markedFor; old entries go as it is asked.
func (s *Service) markQueued(key string, now time.Time, add bool) bool {
	s.markedMu.Lock()
	defer s.markedMu.Unlock()
	if s.marked == nil {
		s.marked = map[string]time.Time{}
	}
	if add {
		s.marked[key] = now
		return true
	}
	for k, at := range s.marked {
		if now.Sub(at) > markedFor {
			delete(s.marked, k)
		}
	}
	_, ok := s.marked[key]
	return ok
}

// orphanClockSlack is how far apart the clock that stamped a message's id (the
// accepting gateway's) and the one that stamped the filed receipt (the hot
// store's) may be before a message "accepted after the receipt" is believed.
const orphanClockSlack = 3 * time.Second

// couldBeAbout says whether the message a filed receipt would match today
// was already accepted when the receipt came in. The message's id is a ULID
// and carries its accept time; an id nothing can read the time of answers
// yes, as every receipt did before this check.
func (s *Service) couldBeAbout(ctx context.Context, o model.OrphanDLR) bool {
	if o.ReceivedAt.IsZero() {
		return true
	}
	ids, _, _, _, _, _, _, _, _, _, _, err := s.resolve(ctx, receiptKey{vendorID: o.VendorID, id: o.VendorMsgID})
	if err != nil || len(ids) == 0 {
		return true // nothing to judge; apply answers as it always did
	}
	for _, id := range ids {
		if at, ok := monitor.ULIDTime(id); ok && at.After(o.ReceivedAt.Add(orphanClockSlack)) {
			return false
		}
	}
	return true
}

// StartRematch runs RematchOrphans on a ticker until ctx is done; done is
// closed when the sweep has ended, after the orphan it was applying (a stopping
// role waits for it). Every dlr role runs it; with a store that can lease
// (Leaser) one pass at a time runs gateway-wide, because two would apply the
// same rows twice (rematchLease).
//
// YAKU_ORPHAN_REMATCH_SEC is the interval (default 5s, 0 turns the sweep off)
// and YAKU_ORPHAN_REMATCH_MINUTES how far back a pass looks (default 60). A
// receipt older than that window is taken to be genuinely foreign: re-trying
// it every few seconds for ever would be work with no outcome. A receipt
// that misses is tried again after the interval, then after twice that, and
// so on up to YAKU_ORPHAN_REMATCH_MAX_SEC (default 300).
//
// Not with the receipt facts in Redis (29 Oct 2026): a receipt is looked for
// there three times, 5 s apart, before it is filed as an orphan (later.go),
// and the record store matches no receipt — an orphan is evidence only.
func (s *Service) StartRematch(ctx context.Context, logf func(string, ...any)) (done <-chan struct{}) {
	finished := make(chan struct{})
	every := time.Duration(envIntDefault("YAKU_ORPHAN_REMATCH_SEC", 5)) * time.Second
	if every <= 0 || s.Fast() {
		close(finished)
		return finished
	}
	window := time.Duration(envIntDefault("YAKU_ORPHAN_REMATCH_MINUTES", 60)) * time.Minute
	limit := envIntDefault("YAKU_ORPHAN_REMATCH_LIMIT", 200)
	back := &orphanBackoff{first: every, max: time.Duration(envIntDefault("YAKU_ORPHAN_REMATCH_MAX_SEC", 300)) * time.Second}
	if back.max < every {
		back.max = every
	}
	go func() {
		defer close(finished)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, err := s.rematch(ctx, time.Now().UTC().Add(-window), limit, back)
				if err != nil && ctx.Err() == nil && logf != nil {
					logf("orphan re-match: %v", err)
				}
				if n > 0 && logf != nil {
					logf("orphan re-match: %d receipt(s) matched their message after all", n)
				}
			}
		}
	}()
	return finished
}

// envIntDefault reads a whole-number setting, def when unset or unreadable.
func envIntDefault(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
