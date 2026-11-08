// Package billingq books the billing queue (issues 3 and 18; Saleem, 26 Oct
// 2026): each deployment's db-worker takes the money events its processes
// queued (billing.Booker) and adds them up into usage_buckets, the one place
// customer invoices and vendor bills are made from.
//
// A lane is booked by one worker at a time (its turn, as the record lanes'
// are). A batch is MOVED out of the lane into the lane's "doing" list under a
// new id before anything is written, and the config-store transaction that
// books it writes that id as its guard row (usage_batches): a worker that
// stops anywhere leaves the batch in "doing", and the next one books it
// again under the same id — which the guard row turns into "already booked".
// So an event is booked exactly once, whatever stops where.
package billingq

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/monitor"
)

const (
	// Batch is the most events one booking takes.
	Batch = 1000
	// lockFor is how long a lane's turn lasts unless renewed; a killed
	// worker holds its lanes this long.
	lockFor  = 15 * time.Second
	bookFor  = 30 * time.Second
	idleWait = 200 * time.Millisecond
	// delayFirst / delayMost: a batch MySQL refused for one of its rows is
	// tried again event by event; an event refused again waits, from
	// delayFirst doubling to delayMost. It is never dropped: it is money.
	delayFirst = time.Minute
	delayMost  = time.Hour
)

// lane is one of this worker's lanes, as the queue names it. The queue's
// keys and its scripts (the turn, the claim, the end of a batch) are the
// queue's own (dal.BillingQueue; internal/dal/redis billing_queue.go).
func (w *Worker) lane(l int) dal.Lane { return dal.Lane{Scope: w.scope, Side: w.side, Lane: l} }

// Worker books one side of one deployment's billing queue.
type Worker struct {
	rc    dal.BillingQueue
	db    dal.UsageBooks
	scope string
	side  string // billing.SideSell, SideBuy, or SideMixed (the queue of 26 Oct)
	lanes int
	token string
	logf  func(string, ...any)
	// untilEmpty: a lane is let go once nothing is left on it, and Run ends
	// when every lane is (a queue of an earlier build after an upgrade:
	// never written).
	untilEmpty bool
	// label is the side as the status names it ("" = sideName's own).
	label string
	// where says which Redis the queue is in, for the log.
	where string

	// Idle is how often a lane is checked, busy or idle (YAKU_QUEUE_BILLING_
	// EVERY_MS since issue 7; idleWait when zero).
	Idle time.Duration
	// PerCheck is the most events one check books, in one booking
	// (YAKU_QUEUE_BILLING_PER_INTERVAL); Batch when zero.
	PerCheck int

	start  time.Time
	mu     sync.Mutex
	state  map[int]*laneState
	booked *monitor.Counter // events booked (shared by the sides of a Workers)
	batchC *monitor.Counter // batches booked
	repeat int64            // batches found in "doing" (a worker stopped)
	dupes  int64            // batches found booked already
	lastN  int
	lastMS int64
	lastAt time.Time
}

type laneState struct {
	mine    bool
	lastErr string
}

// NewWorker books one side of owner's billing queue ("" = shared) on rc (the
// deployment's queue Redis since 29 Oct 2026; the main Redis before) into
// cfgDB (the usage books). lanes is this deployment's lane count
// (billing.LanesFromEnv).
func NewWorker(rc dal.BillingQueue, cfgDB dal.UsageBooks, owner, side string, lanes int, logf func(string, ...any)) *Worker {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if lanes < 1 {
		lanes = billing.DefaultLanes
	}
	return &Worker{rc: rc, db: cfgDB, scope: billing.QueueScope(owner), side: side, lanes: lanes,
		token: ulid.Make().String(), logf: logf, state: map[int]*laneState{}, start: time.Now(),
		booked: &monitor.Counter{}, batchC: &monitor.Counter{}}
}

// Run books every lane until ctx ends. A lane named later (a process with a
// larger lane count) gets its own booker within ten seconds. A worker made
// to book until empty (the mixed queue of an earlier build) books the lanes
// it finds once and ends when each is empty.
func (w *Worker) Run(ctx context.Context) {
	running := map[int]bool{}
	var wg sync.WaitGroup
	if w.untilEmpty {
		for _, l := range w.usedLanes(ctx) {
			wg.Add(1)
			go func(lane int) {
				defer wg.Done()
				w.drain(ctx, lane)
			}(l)
		}
		wg.Wait()
		return
	}
	start := func() {
		for _, l := range w.usedLanes(ctx) {
			if running[l] {
				continue
			}
			running[l] = true
			wg.Add(1)
			go func(lane int) {
				defer wg.Done()
				w.drain(ctx, lane)
			}(l)
		}
	}
	start()
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-t.C:
			start()
		}
	}
}

// usedLanes is 0..lanes-1 plus every lane anything was put on.
func (w *Worker) usedLanes(ctx context.Context) []int {
	seen := map[int]bool{}
	for i := 0; i < w.lanes; i++ {
		seen[i] = true
	}
	if ms, err := w.rc.Lanes(ctx, w.scope, w.side); err == nil {
		for _, m := range ms {
			if n, err := strconv.Atoi(m); err == nil && n >= 0 && n < 1024 {
				seen[n] = true
			}
		}
	}
	out := make([]int, 0, len(seen))
	for l := range seen {
		out = append(out, l)
	}
	sort.Ints(out)
	return out
}

func (w *Worker) laneState(lane int) *laneState {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.state[lane]
	if s == nil {
		s = &laneState{}
		w.state[lane] = s
	}
	return s
}

func (w *Worker) drain(ctx context.Context, lane int) {
	var lastSaid time.Time
	say := func(format string, a ...any) {
		if time.Since(lastSaid) >= 10*time.Second {
			lastSaid = time.Now()
			w.logf(format, a...)
		}
	}
	wait := func(d time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}
	defer func() {
		rel, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = w.rc.DropTurn(rel, w.lane(lane), w.token)
	}()
	st := w.laneState(lane)
	for ctx.Err() == nil {
		mine, err := w.rc.TakeTurn(ctx, w.lane(lane), w.token, lockFor)
		w.mu.Lock()
		st.mine = err == nil && mine
		w.mu.Unlock()
		if err != nil || !mine {
			if !wait(time.Second) {
				return
			}
			continue
		}
		n, err := w.step(ctx, lane)
		w.mu.Lock()
		if err != nil {
			st.lastErr = err.Error()
		} else {
			st.lastErr = ""
		}
		w.mu.Unlock()
		if err != nil {
			say("billing events waiting to be booked (lane %d): %v — kept in Redis, tried again", lane, err)
			if !wait(time.Second) {
				return
			}
			continue
		}
		if n == 0 && w.untilEmpty && w.laneEmpty(ctx, lane) {
			return
		}
		// The next check comes after the interval, busy or idle (issue 7);
		// a Worker built by hand books again at once after a full batch.
		if n == 0 || w.PerCheck > 0 {
			idle := w.Idle
			if idle <= 0 {
				idle = idleWait
			}
			if !wait(idle) {
				return
			}
		}
	}
}

// laneEmpty says nothing is left on a lane (ready, delayed or doing); the
// lane is then taken off the lanes set (the set goes with its last lane).
func (w *Worker) laneEmpty(ctx context.Context, lane int) bool {
	empty, err := w.rc.DropIfEmpty(ctx, w.lane(lane))
	return err == nil && empty
}

// step books one batch of a lane: claimed, booked, done. n is how many
// events it took (0 = nothing waits).
func (w *Worker) step(ctx context.Context, lane int) (int, error) {
	size := Batch
	if w.PerCheck > 0 {
		size = w.PerCheck
	}
	res, err := w.rc.Claim(ctx, w.lane(lane), w.token, size, ulid.Make().String(), time.Now().UnixMilli())
	if err != nil {
		return 0, err
	}
	if len(res) < 3 {
		return 0, nil
	}
	batch, repeat, members := res[0], res[1] == "1", res[2:]
	if batch == "" {
		// A "doing" list with no id (written by hand, or a lost key): book
		// it under a new one. Its events were never booked under any id —
		// a booked batch's "doing" is cleared in the same step as nothing
		// else, after its commit.
		batch = ulid.Make().String()
		_ = w.rc.SetBatchID(ctx, w.lane(lane), batch)
	}
	if repeat {
		w.mu.Lock()
		w.repeat++
		w.mu.Unlock()
	}
	evs := make([]billing.Event, 0, len(members))
	var unreadable []string
	for _, m := range members {
		if e, ok := billing.ParseEvent(m); ok {
			evs = append(evs, e)
		} else {
			unreadable = append(unreadable, m)
		}
	}
	for _, m := range unreadable {
		// Kept, never dropped: it is money, and someone must read it.
		w.logf("a billing event that cannot be read is kept on the delayed list (lane %d): %.160s", lane, m)
	}
	stop := w.keepTurn(ctx, lane)
	bctx, cancel := context.WithTimeout(ctx, bookFor)
	start := time.Now()
	refused, err := w.book(bctx, batch, evs)
	cancel()
	stop()
	if err != nil {
		return 0, err // MySQL away: the batch stays in "doing", booked again under its id
	}
	var back []dal.Scored
	later := time.Now().Add(delayFirst).UnixMilli()
	for _, e := range refused {
		back = append(back, dal.Scored{Score: float64(later), Member: e.Member()})
	}
	for _, m := range unreadable {
		back = append(back, dal.Scored{Score: float64(time.Now().Add(delayMost).UnixMilli()), Member: m})
	}
	ok, err := w.rc.Done(ctx, w.lane(lane), w.token, back)
	if err != nil {
		return 0, err // booked; "doing" stays and is found booked next time
	}
	if ok != 1 {
		return 0, fmt.Errorf("lane %d: the turn was lost while booking; the next holder finds the batch booked", lane)
	}
	n := len(evs) - len(refused)
	w.booked.Add(int64(n))
	w.batchC.Add(1)
	w.mu.Lock()
	w.lastN, w.lastMS, w.lastAt = len(evs), time.Since(start).Milliseconds(), time.Now()
	w.mu.Unlock()
	return len(members), nil
}

// keepTurn renews the lane's turn while a batch is booked.
func (w *Worker) keepTurn(ctx context.Context, lane int) (stop func()) {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(lockFor / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				_, _ = w.rc.TakeTurn(ctx, w.lane(lane), w.token, lockFor)
			}
		}
	}()
	return func() { close(done) }
}

// book writes a batch in one transaction. refused are events MySQL would
// not take for their own values (found one at a time when the batch was
// refused for a row); err is MySQL being away, and nothing was written.
func (w *Worker) book(ctx context.Context, batch string, evs []billing.Event) (refused []billing.Event, err error) {
	if len(evs) == 0 {
		return nil, nil
	}
	err = w.bookTx(ctx, batch, evs)
	if err == nil || w.db.Away(err) {
		return nil, err
	}
	// One row's values were refused: book the events one at a time, each
	// under its own id (the batch's id plus the event's), to find it.
	for _, e := range evs {
		if e1 := w.bookTx(ctx, batch+":"+e.ID, []billing.Event{e}); e1 != nil {
			if w.db.Away(e1) {
				return nil, e1
			}
			w.logf("billing event %s refused by MySQL, kept to be tried again: %v", e.ID, e1)
			refused = append(refused, e)
		}
	}
	return refused, nil
}

// row is one usage_buckets row of a batch.
type row struct {
	bucket                   time.Time
	side                     string
	company, account, vendor int64
	kind, model, currency    string
	items, parts, units      int64
	// The conversions (config/0163, 27 Oct 2026): part of the key, so a
	// different rate is a row of its own. conv says the events carried them
	// (a row of older events has none: NULL amounts).
	cardCur, ownCur, fx      string
	conv                     bool
	cardUnits, ownUnits      int64
	baseUnits                int64
	convMissing, baseMissing int64
}

func (r row) key() string {
	return fmt.Sprintf("%020d|%s|%020d|%020d|%020d|%s|%s|%s|%s|%s|%s", r.bucket.Unix(), r.side, r.company, r.account, r.vendor,
		r.kind, r.model, r.currency, r.cardCur, r.ownCur, r.fx)
}

// fxKey is a rate as the bucket key keeps it: 6 significant digits, so the
// fetches of a live provider that differ in the 10th place share a row.
func fxKey(fx float64) string {
	if fx <= 0 {
		return "0"
	}
	v, _ := strconv.ParseFloat(strconv.FormatFloat(fx, 'g', 6, 64), 64)
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// convOf is one side's conversion key and amounts in ledger units: card,
// company and base; ok = the side carried its conversions.
//
//	sell: Units are in the customer company's currency; card = Units / fx.
//	buy:  Units are in the buying card's currency; company = Units x fx.
func convOf(side string, s *billing.Side) (cardCur, ownCur, fx string, card, own, base int64, convKnown, baseKnown, ok bool) {
	c := s.Conv
	if c == nil {
		return "", "", "0", 0, 0, 0, false, false, false
	}
	cur := strings.ToUpper(s.Currency)
	rate := c.FX
	if side == "sell" {
		cardCur, ownCur = strings.ToUpper(c.CardCur), cur
		if cardCur == "" {
			cardCur = cur
		}
		if rate <= 0 && cardCur == ownCur {
			rate = 1
		}
		own = s.Units
		if rate > 0 {
			card, convKnown = int64(math.Round(float64(s.Units)/rate)), true
		}
		if c.Base > 0 {
			base, baseKnown = int64(math.Round(float64(own)*c.Base)), true
		}
	} else {
		cardCur, ownCur = cur, strings.ToUpper(c.OwnCur)
		if ownCur == "" {
			ownCur = cur
		}
		if rate <= 0 && cardCur == ownCur {
			rate = 1
		}
		card = s.Units
		if rate > 0 {
			own, convKnown = int64(math.Round(float64(s.Units)*rate)), true
			if c.Base > 0 {
				base, baseKnown = int64(math.Round(float64(own)*c.Base)), true
			}
		}
	}
	return cardCur, ownCur, fxKey(rate), card, own, base, convKnown, baseKnown, true
}

// Rows adds a batch's events up into usage_buckets rows, keys sorted.
func rows(evs []billing.Event) []row {
	sum := map[string]*row{}
	add := func(e billing.Event, side string, s *billing.Side) {
		if s == nil {
			return
		}
		r := row{bucket: billing.BucketOf(e.At), side: side, company: e.Company, account: e.Account, vendor: e.Vendor,
			kind: e.Kind, currency: strings.ToUpper(s.Currency), fx: "0"}
		if side == "sell" {
			r.model = s.Model
		}
		cardCur, ownCur, fx, card, own, base, convKnown, baseKnown, ok := convOf(side, s)
		if ok {
			r.cardCur, r.ownCur, r.fx, r.conv = cardCur, ownCur, fx, true
		}
		k := r.key()
		if got := sum[k]; got != nil {
			r = *got
		}
		r.items += e.Items
		r.parts += e.Parts
		r.units += s.Units
		if ok {
			if convKnown {
				r.cardUnits += card
				r.ownUnits += own
			} else {
				r.convMissing++
				if side == "sell" {
					r.ownUnits += own // the company amount is the side's own
				} else {
					r.cardUnits += card
				}
			}
			if baseKnown {
				r.baseUnits += base
			} else {
				r.baseMissing++
			}
		}
		sum[k] = &r
	}
	for _, e := range evs {
		if e.Company == 0 {
			continue
		}
		add(e, "sell", e.Sell)
		add(e, "buy", e.Buy)
	}
	out := make([]row, 0, len(sum))
	for _, r := range sum {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// bookTx books a batch's rows under its guard row in one transaction of the
// usage books (dal.UsageBooks): a guard already there = booked before.
func (w *Worker) bookTx(ctx context.Context, batch string, evs []billing.Event) error {
	rs := rows(evs)
	guard := batch
	if len(guard) > 26 {
		guard = guardID(guard)
	}
	out := make([]dal.UsageRow, len(rs))
	for i, r := range rs {
		out[i] = dal.UsageRow{Bucket: r.bucket, Side: r.side, Company: r.company, Account: r.account, Vendor: r.vendor,
			Kind: r.kind, Model: r.model, Currency: r.currency, CardCurrency: r.cardCur, CompanyCurrency: r.ownCur, FX: r.fx,
			Items: r.items, Parts: r.parts, Units: r.units, Conv: r.conv, CardUnits: r.cardUnits, CompanyUnits: r.ownUnits,
			BaseUnits: r.baseUnits, ConvMissing: r.convMissing, BaseMissing: r.baseMissing}
	}
	already, err := w.db.Book(ctx, guard, out)
	if err != nil {
		return err
	}
	if already {
		w.mu.Lock()
		w.dupes++
		w.mu.Unlock()
	}
	return nil // booked, or booked already: a worker stopped after its commit
}

// guardID fits a longer id (a batch's id plus an event's) into the guard
// row's CHAR(26): a hash of it, the same for the same id every time.
func guardID(s string) string {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var h1, h2 uint64 = 1469598103934665603, 1099511628211
	for i := 0; i < len(s); i++ {
		h1 ^= uint64(s[i])
		h1 *= 1099511628211
		h2 = h2*31 + uint64(s[i])
	}
	out := make([]byte, 26)
	for i := 0; i < 13; i++ {
		out[i] = alphabet[h1%32]
		h1 /= 32
		out[13+i] = alphabet[h2%32]
		h2 /= 32
	}
	return string(out)
}

// Queue is the queue this worker books.
func (w *Worker) Queue() dal.BillingQueue { return w.rc }

// laneOut is one lane on the status.
type laneOut struct {
	Side     string `json:"side"` // sell | buy | mixed (the queue of an earlier build)
	Lane     int    `json:"lane"`
	Waiting  int64  `json:"waiting"`
	Delayed  int64  `json:"delayed"`
	Doing    int64  `json:"doing"`
	OldestMS int64  `json:"oldest_ms"`
	Mine     bool   `json:"booked_here"`
	LastErr  string `json:"last_error,omitempty"`
}

// sideName is the side as the status says it.
func (w *Worker) sideName() string {
	if w.label != "" {
		return w.label
	}
	if w.side == billing.SideMixed {
		return "mixed"
	}
	return w.side
}

// reading is what waits on this worker's lanes, read now.
type reading struct {
	lanes                           []laneOut
	waiting, delayed, doing, oldest int64
	lastErr                         string
}

func (w *Worker) read(c context.Context) reading {
	var r reading
	now := time.Now().UnixMilli()
	for _, l := range w.usedLanes(c) {
		cnt, err := w.rc.Count(c, w.lane(l))
		if err != nil {
			r.lastErr = err.Error()
			continue
		}
		lo := laneOut{Side: w.sideName(), Lane: l, Waiting: cnt.Ready, Delayed: cnt.Delayed, Doing: cnt.Doing}
		if w.untilEmpty && lo.Waiting+lo.Delayed+lo.Doing == 0 {
			continue // the old queue's empty lanes are not shown
		}
		if cnt.HasOldest {
			lo.OldestMS = now - int64(cnt.Oldest)
		}
		st := w.laneState(l)
		w.mu.Lock()
		lo.Mine, lo.LastErr = st.mine, st.lastErr
		w.mu.Unlock()
		if lo.LastErr != "" {
			r.lastErr = lo.LastErr
		}
		r.waiting += lo.Waiting
		r.delayed += lo.Delayed
		r.doing += lo.Doing
		if lo.OldestMS > r.oldest {
			r.oldest = lo.OldestMS
		}
		r.lanes = append(r.lanes, lo)
	}
	return r
}

// Section is one side's queue as a status section (a Worker made by hand;
// the db-worker reports both sides through Workers.Section).
func (w *Worker) Section(ctx context.Context) map[string]any {
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	r := w.read(c)
	w.mu.Lock()
	defer w.mu.Unlock()
	uptimeSec := time.Since(w.start).Seconds()
	return map[string]any{
		"lanes": r.lanes, "waiting": r.waiting, "delayed": r.delayed, "doing": r.doing, "oldest_ms": r.oldest,
		"booked": w.booked.Snapshot(uptimeSec), "batches": w.batchC.Snapshot(uptimeSec),
		"repeat_batches": w.repeat, "already_booked": w.dupes,
		"last_batch_events": w.lastN, "last_book_ms": w.lastMS, "last_error": r.lastErr,
	}
}
