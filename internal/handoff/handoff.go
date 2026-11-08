// Package handoff moves client receipts and incoming messages (MO) that
// arrive at one gateway for a client another gateway serves (issue 5 of the
// design audit; Saleem, 26 Oct 2026).
//
// A receipt is filed under its message's queue owner (D-121) and an MO under
// the deployment that delivers it (mo.route.deliverer). Each gateway may have
// a queue Redis of its own, and reads only that one: a receipt or MO written
// into the receiving gateway's queue Redis for the other gateway was never
// delivered, never given up and never removed. Now it is PARKED for its owner
// in the main Redis, which every gateway sees, and the owner moves it into its
// own queue:
//
//   - queues:<owner>:handoff:dlr — receipts, as they were pushed: not yet
//     turned into client receipts, so the owner expands a long SMPP message
//     to the part ids IT holds (issue 6), with the hold-back still to wait;
//   - queues:<owner>:handoff:mo — incoming messages, whole (the text stays
//     sealed for a company that encrypts).
//
// Whether to park: each gateway writes a random id into its own queue store
// (qstore:id, SET NX) and names it in the main Redis (hash handoff:stores,
// field = deployment). The owner's id the same as ours: one queue store,
// filed directly. Another or unknown: parked. Queues in MySQL here, with a
// Redis set: never parked (the queue database is the deployments' shared
// one). With no Redis at all (step 3 of the DAL plan, 29 Oct 2026) the lists
// and the names are in the config store and each queue database has its own
// id (dal.OpenHandOffs): gateways on one queue database park nothing, a
// gateway with a queue database of its own is parked for.
//
// The owner moves at most YAKU_QUEUE_HANDOFF_PER_INTERVAL items of each kind
// into its own queue every YAKU_QUEUE_HANDOFF_EVERY_MS (Saleem, 26 Oct 2026: a
// per-interval
// setting, not a cap on the lists). Nothing caps what waits: a gateway that
// is down keeps its items waiting in the main Redis — there is no takeover
// (PO rule). Times come from Redis TIME, so the two hosts' clocks do not
// matter.
package handoff

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/queuecheck"
	"github.com/yakugate/yakugate/internal/receiptfacts"
)

// The two kinds of item.
const (
	KindDLR = "dlr"
	KindMO  = "mo"
)

const (
	// Batch is the most parked items one read of a list takes; an interval
	// reads as many batches as its YAKU_QUEUE_HANDOFF_PER_INTERVAL allows.
	Batch = 200
	// fileChunk is how many are filed before the list is trimmed: a crash
	// between the two files at most this many twice.
	fileChunk = 50
	// lockFor is how long an importer's turn lasts without being renewed.
	lockFor = 15 * time.Second
	// refreshEvery is how often the queue stores' ids are read again.
	refreshEvery = 10 * time.Second
	// heldRetry is how often receipts that could not be parked are tried
	// again; heldMost is how many are kept in memory at most.
	heldRetry = 500 * time.Millisecond
	heldMost  = 200000
	// straysFor is the most a walk for strays may take before another
	// process of the gateway may start one.
	straysFor = 10 * time.Minute
)

// Where the lists live is the DAL's (dal.HandOffs; in the main Redis,
// queues:<owner>:handoff:<kind> with its :lock and :strays, handoff:stores
// naming each deployment's queue store, and qstore:id in each queue store).

// errTurnLost: another process of this gateway took the importer's turn
// while a batch was being filed; nothing was trimmed.
var errTurnLost = errors.New("another process took over the hand-off import")

// HandOff is one gateway's side of the hand-off. Its methods are safe on a
// nil HandOff, which hands nothing off.
type HandOff struct {
	// st is the lists and the stores' ids (main Redis), with this gateway's
	// queue Redis when its queues are there (st.QueuesHere).
	st    dal.HandOffs
	queue bool          // this gateway's queues are in Redis
	me    string        // this deployment
	every time.Duration // YAKU_QUEUE_HANDOFF_EVERY_MS: how often the importer moves items
	// perTick is YAKU_QUEUE_HANDOFF_PER_INTERVAL: the most items of one kind
	// the importer moves into this gateway's queue each interval
	// (internal/queuecheck, issue 7).
	perTick int
	logf    func(string, ...any)

	mu     sync.RWMutex
	mine   string            // our queue store's id; "" = not read yet
	stores map[string]string // deployment -> its queue store's id

	hmu       sync.Mutex
	held      []dlr.Parked // receipts that could not be parked yet, oldest first
	heldSince time.Time
	flushing  bool

	pmu      sync.Mutex
	parkedTo map[string]int64 // receipts and MO parked from here, by owner

	parkedDLR, parkedMO     atomic.Int64
	refusedMO, lost         atomic.Int64
	importedDLR, importedMO atomic.Int64
	progressDropped         atomic.Int64
	unreadable              atomic.Int64
	straysDLR, straysMO     atomic.Int64
}

var (
	processOnce sync.Once
	process     *HandOff
)

// FromConfig is this process's hand-off, made once: nil when there is
// nothing to hand off — no deployment id (the queues are shared). With no
// Redis (plan of 29 Oct 2026, step 3) the lists are the SQL database's
// (dal.OpenHandOffs): gateways on one queue database read one queue store id
// and park nothing; a gateway with a queue database of its own is parked for.
func FromConfig(cfg config.Config, logf func(string, ...any)) *HandOff {
	processOnce.Do(func() {
		me := deployment.Owner()
		if me == "" {
			return
		}
		st, err := dal.OpenHandOffs(cfg)
		if err != nil {
			if logf != nil {
				logf("hand-off: %v", err)
			}
			return
		}
		process = New(context.Background(), st, me, logf)
	})
	return process
}

// Attach makes a role's receipt pusher park the receipts of clients on other
// gateways: p is the role's dlr.QueuePusher (another pusher is left as it
// is). It returns the hand-off, nil when there is none.
func Attach(cfg config.Config, p any, logf func(string, ...any)) *HandOff {
	h := FromConfig(cfg, logf)
	if qp, ok := p.(*dlr.QueuePusher); ok && h != nil {
		qp.SetHandOff(h)
	}
	return h
}

// New is the hand-off of the gateway me, on the DAL's lists (st: the main
// Redis, with its queue Redis when its queues are there). It reads the
// stores' ids once before it returns (a moment at most), then every
// refreshEvery while ctx lasts, and adds a "handoff" section to this
// process's status.
func New(ctx context.Context, st dal.HandOffs, me string, logf func(string, ...any)) *HandOff {
	if st == nil || me == "" {
		return nil
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	h := &HandOff{st: st, queue: st.QueuesHere(), me: me, logf: logf, stores: map[string]string{},
		parkedTo: map[string]int64{}}
	chk := queuecheck.For(queuecheck.Handoff)
	h.every, h.perTick = chk.Every, chk.PerInterval
	first, cancel := context.WithTimeout(ctx, 2*time.Second)
	h.refresh(first)
	cancel()
	go func() {
		t := time.NewTicker(refreshEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c, cancel := context.WithTimeout(ctx, 5*time.Second)
				h.refresh(c)
				cancel()
			}
		}
	}()
	monitor.AddSection("handoff", func() any { return h.Section() })
	return h
}

// refresh names our queue store in the main Redis and reads every
// deployment's. A queue store with no id yet is given one (SET NX: two
// gateways on one queue Redis read the same id). Our queues in MySQL: our
// name is taken out, so a gateway on Redis queues parks for us and our
// importer files into MySQL.
func (h *HandOff) refresh(ctx context.Context) {
	mine := ""
	if h.queue {
		id, err := h.st.QueueStoreID(ctx, newID())
		if err != nil {
			return
		}
		mine = id
		if err := h.st.NameStore(ctx, h.me, mine); err != nil {
			return
		}
	} else if err := h.st.UnnameStore(ctx, h.me); err != nil {
		return
	}
	all, err := h.st.Stores(ctx)
	if err != nil {
		return
	}
	h.mu.Lock()
	h.mine, h.stores = mine, all
	h.mu.Unlock()
}

// Foreign says whether an item for owner must be parked: owner is another
// deployment whose queue store is not ours, or not known. When in doubt it
// parks, which is never wrong (the owner files it), where filing here for a
// gateway that reads another store loses it.
func (h *HandOff) Foreign(owner string) bool {
	if h == nil || !h.queue || owner == "" || owner == h.me {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	theirs := h.stores[owner]
	return h.mine == "" || theirs == "" || theirs != h.mine
}

// knownOther says owner is another deployment KNOWN to read another queue
// store: what is queued here for it is stranded. A deployment not known yet
// may share this store and run an older build, so its items are left alone.
func (h *HandOff) knownOther(owner string) bool {
	if owner == "" || owner == h.me {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	theirs := h.stores[owner]
	return h.mine != "" && theirs != "" && theirs != h.mine
}

// park appends JSON items to owner's list of kind, each stamped with Redis's
// own time ({"p":<ms>,"x":<item>}), all or none (one script, dal.HandOffs).
func (h *HandOff) park(ctx context.Context, owner, kind string, items [][]byte) error {
	if len(items) == 0 {
		return nil
	}
	args := make([]string, 0, len(items))
	for _, b := range items {
		args = append(args, string(b))
	}
	if err := h.st.Park(ctx, owner, kind, args); err != nil {
		return err
	}
	h.pmu.Lock()
	h.parkedTo[owner] += int64(len(items))
	h.pmu.Unlock()
	return nil
}

// ParkReceipts parks receipts for the gateways their clients are on, in
// order (dlr.HandOff). Never an error for the caller: the message's closing
// mark is taken already, so a receipt refused here and sent again by its
// vendor would be recorded only and its client never told. What cannot be
// parked now (the main Redis away) is held in memory, behind which every
// later receipt waits, and parked again every heldRetry. Held receipts are
// lost if the process stops meanwhile.
func (h *HandOff) ParkReceipts(ctx context.Context, rs []dlr.Parked) {
	if h == nil || len(rs) == 0 {
		return
	}
	h.hmu.Lock()
	if len(h.held) > 0 {
		h.holdLocked(rs, nil)
		h.hmu.Unlock()
		return
	}
	h.hmu.Unlock()
	if bad, err := h.parkReceipts(ctx, rs); len(bad) > 0 {
		h.hmu.Lock()
		h.holdLocked(bad, err)
		h.hmu.Unlock()
	}
}

// parkReceipts parks rs, one call an owner, and hands back those of the
// owners whose park failed (in order) with the last error.
func (h *HandOff) parkReceipts(ctx context.Context, rs []dlr.Parked) (bad []dlr.Parked, err error) {
	var owners []string
	of := map[string][]int{}
	for i, r := range rs {
		o := r.Receipt.Owner
		if _, ok := of[o]; !ok {
			owners = append(owners, o)
		}
		of[o] = append(of[o], i)
	}
	failed := map[string]bool{}
	for _, o := range owners {
		items := make([][]byte, 0, len(of[o]))
		for _, i := range of[o] {
			b, _ := json.Marshal(rs[i])
			items = append(items, b)
		}
		if perr := h.park(ctx, o, KindDLR, items); perr != nil {
			failed[o], err = true, perr
			continue
		}
		h.parkedDLR.Add(int64(len(items)))
	}
	if len(failed) == 0 {
		return nil, nil
	}
	for _, r := range rs {
		if failed[r.Receipt.Owner] {
			bad = append(bad, r)
		}
	}
	return bad, err
}

// holdLocked keeps rs to be parked later. hmu is held.
func (h *HandOff) holdLocked(rs []dlr.Parked, cause error) {
	if room := heldMost - len(h.held); len(rs) > room {
		lost := len(rs) - max(room, 0)
		h.lost.Add(int64(lost))
		h.logf("hand-off: %d receipt(s) for other gateways LOST: %d are held already, waiting for the main Redis", lost, len(h.held))
		rs = rs[:max(room, 0)]
	}
	if len(rs) == 0 {
		return
	}
	if len(h.held) == 0 {
		h.heldSince = time.Now()
		why := "behind others held"
		if cause != nil {
			why = cause.Error()
		}
		h.logf("hand-off: receipts for other gateways could not be parked (%s); held in memory and tried again", why)
	}
	h.held = append(h.held, rs...)
	if !h.flushing {
		h.flushing = true
		go h.flushHeld()
	}
}

// flushHeld parks the held receipts again until none is left.
func (h *HandOff) flushHeld() {
	lastSaid := time.Now()
	for {
		time.Sleep(heldRetry)
		h.hmu.Lock()
		batch := append([]dlr.Parked(nil), h.held[:min(len(h.held), 1000)]...)
		h.hmu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		bad, err := h.parkReceipts(ctx, batch)
		cancel()
		h.hmu.Lock()
		// Later receipts were only ever added at the end: the batch is still
		// the head.
		h.held = append(bad, h.held[len(batch):]...)
		if len(h.held) == 0 {
			h.flushing = false
			h.logf("hand-off: the held receipts are parked (%s after the first was held)", time.Since(h.heldSince).Round(time.Second))
			h.hmu.Unlock()
			return
		}
		n := len(h.held)
		h.hmu.Unlock()
		if err != nil && time.Since(lastSaid) >= 30*time.Second {
			lastSaid = time.Now()
			h.logf("hand-off: %d receipt(s) for other gateways still held: %v", n, err)
		}
	}
}

// Held is how many receipts wait in memory to be parked.
func (h *HandOff) Held() int {
	if h == nil {
		return 0
	}
	h.hmu.Lock()
	defer h.hmu.Unlock()
	return len(h.held)
}

// ParkMO parks an incoming message for the gateway that delivers it. An
// error goes back to the caller, which refuses the message so its vendor
// sends it again: an MO has no closing mark, so the resend is safe.
func (h *HandOff) ParkMO(ctx context.Context, m model.Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := h.park(ctx, m.QueueOwner, KindMO, [][]byte{b}); err != nil {
		h.refusedMO.Add(1)
		return fmt.Errorf("park MO for %s: %w", m.QueueOwner, err)
	}
	h.parkedMO.Add(1)
	return nil
}

// MOStager stages an incoming message in a queue (mo.Stager, the queue
// stores).
type MOStager interface {
	StageMO(ctx context.Context, m model.Message) error
}

// stager parks an MO for a client on another gateway and stages the rest.
type stager struct {
	inner MOStager
	h     *HandOff
}

func (s stager) StageMO(ctx context.Context, m model.Message) error {
	if s.h.Foreign(m.QueueOwner) {
		return s.h.ParkMO(ctx, m)
	}
	return s.inner.StageMO(ctx, m)
}

// WrapStager is inner with the incoming messages of clients on other
// gateways parked for them; inner itself when h is nil.
func (h *HandOff) WrapStager(inner MOStager) MOStager {
	if h == nil || inner == nil {
		return inner
	}
	return stager{inner: inner, h: h}
}

// ReceiptFiler files receipts parked for this gateway into its own client
// queues, expanded to the part ids it holds (dlr.QueuePusher.FileNow).
type ReceiptFiler interface {
	FileNow(ctx context.Context, rs []dlr.Parked) error
}

// StrayReceipts / StrayMO are this gateway's queue Redis, walked once at
// start for what was queued there for another gateway (redis.Store).
type StrayReceipts interface {
	HandOverReceiptsOf(ctx context.Context, which func(owner string) bool, fn func(d model.ClientDLR) error) (int, error)
}
type StrayMO interface {
	HandOverMOOf(ctx context.Context, which func(owner string) bool, fn func(m model.Message) error) (int, error)
}

// ImportReceipts moves the receipts parked for this gateway into its own
// queues through f until ctx ends. Safe in every process of the gateway: one
// has the turn at a time. stray, when set, is this gateway's queue Redis:
// once, receipts queued there for another gateway are parked for it.
func (h *HandOff) ImportReceipts(ctx context.Context, f ReceiptFiler, stray StrayReceipts) {
	if h == nil || f == nil {
		return
	}
	var once func(context.Context)
	if stray != nil && h.queue {
		once = func(ctx context.Context) { h.straysOfReceipts(ctx, stray) }
	}
	h.run(ctx, KindDLR, func(ctx context.Context, token string, n int) (int, error) {
		return h.importReceipts(ctx, f, token, n)
	}, once)
}

// ImportMO is ImportReceipts for incoming messages, staged through s (this
// gateway's own queue store, not a wrapped one).
func (h *HandOff) ImportMO(ctx context.Context, s MOStager, stray StrayMO) {
	if h == nil || s == nil {
		return
	}
	var once func(context.Context)
	if stray != nil && h.queue {
		once = func(ctx context.Context) { h.straysOfMO(ctx, stray) }
	}
	h.run(ctx, KindMO, func(ctx context.Context, token string, n int) (int, error) {
		return h.importMO(ctx, s, token, n)
	}, once)
}

// The importer's turn (dal.HandOffs.TakeTurn, the receipt writer's script)
// and the trim of what was filed (Trim: the n items off the head, only while
// the turn is still ours, so an importer whose turn ran out cannot cut items
// another has not filed — parkers only add at the tail) are the DAL's.

// run is the import loop of one kind: every interval, while this process has
// the turn, it moves up to perTick items, Batch at a time, the turn renewed
// before each.
func (h *HandOff) run(ctx context.Context, kind string, step func(ctx context.Context, token string, n int) (int, error), strays func(context.Context)) {
	token := newID()
	defer func() {
		rel, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = h.st.DropTurn(rel, h.me, kind, token)
	}()
	if strays != nil {
		go strays(ctx)
	}
	var lastSaid time.Time
	wait := func(d time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}
	for ctx.Err() == nil {
		for left := h.perTick; left > 0 && ctx.Err() == nil; {
			mine, err := h.st.TakeTurn(ctx, h.me, kind, token, lockFor)
			if err != nil || mine != 1 {
				break
			}
			ask := min(Batch, left)
			n, err := step(ctx, token, ask)
			left -= n
			if err != nil {
				if time.Since(lastSaid) >= 10*time.Second && ctx.Err() == nil {
					lastSaid = time.Now()
					h.logf("hand-off: %s parked for %s not filed yet: %v (left parked, tried again)", kindWords(kind), h.me, err)
				}
				break
			}
			if n < ask {
				break // the list is empty
			}
		}
		if !wait(h.every) {
			return
		}
	}
}

func kindWords(kind string) string {
	if kind == KindMO {
		return "incoming messages"
	}
	return "receipts"
}

// parked is one item as the park script wrote it.
type parked struct {
	P int64           `json:"p"`
	X json.RawMessage `json:"x"`
}

// importReceipts files up to most of the receipts parked for this gateway,
// from the head. It returns how many it took off the list.
func (h *HandOff) importReceipts(ctx context.Context, f ReceiptFiler, token string, most int) (int, error) {
	raw, err := h.st.Head(ctx, h.me, KindDLR, most)
	if err != nil || len(raw) == 0 {
		return 0, err
	}
	now, err := h.st.Now(ctx)
	if err != nil {
		return 0, err
	}
	rs := make([]*dlr.Parked, len(raw)) // nil = not filed (unreadable, or progress for a closed message)
	var progress []string
	var progressAt []int
	for i, s := range raw {
		var e parked
		var p dlr.Parked
		if json.Unmarshal([]byte(s), &e) != nil || json.Unmarshal(e.X, &p) != nil {
			h.unreadable.Add(1) // dropped rather than blocking the head for ever
			continue
		}
		p.Receipt = waited(p.Receipt, time.UnixMilli(e.P), now)
		rs[i] = &p
		if !closing(p.Receipt) && p.Receipt.FactsKey != "" {
			progress = append(progress, p.Receipt.FactsKey)
			progressAt = append(progressAt, i)
		}
	}
	// A progress receipt for a message closed meanwhile is not sent on: the
	// client never gets ENROUTE after DELIVRD. Closed = the message's receipt
	// facts are gone (the receipt that closed it took them, 29 Oct 2026).
	if len(progress) > 0 {
		closed, err := receiptfacts.ClosedOf(ctx, factsOf{h.st}, progress)
		if err != nil {
			return 0, err
		}
		for k, i := range progressAt {
			if closed[progress[k]] {
				rs[i] = nil
				h.progressDropped.Add(1)
			}
		}
	}
	for from := 0; from < len(rs); from += fileChunk {
		to := min(from+fileChunk, len(rs))
		var file []dlr.Parked
		for _, p := range rs[from:to] {
			if p != nil {
				file = append(file, *p)
			}
		}
		if len(file) > 0 {
			if err := f.FileNow(ctx, file); err != nil {
				return from, err
			}
		}
		if err := h.trim(ctx, KindDLR, token, to-from); err != nil {
			return from, err // filed, not trimmed: filed once more next time
		}
		h.importedDLR.Add(int64(len(file)))
	}
	return len(rs), nil
}

// waited is a parked receipt with the time it waited in the list taken off
// its hold-back (Redis's clock both times). A held-back receipt's done date is
// when it was parked plus its hold-back, as it would have been read had it
// been filed where it arrived.
func waited(r dlr.Receipt, parkedAt, now time.Time) dlr.Receipt {
	if r.Delay <= 0 {
		return r
	}
	if r.DoneAt.IsZero() {
		r.DoneAt = parkedAt.Add(r.Delay)
	}
	r.Delay = max(0, r.Delay-now.Sub(parkedAt))
	return r
}

// closing says r is a final receipt, not a progress one.
func closing(r dlr.Receipt) bool {
	return r.Status.IsTerminal() && (r.Stat == "" || r.Stat.Final())
}

// trim takes n filed items off the head of this gateway's list of kind while
// the turn is ours.
func (h *HandOff) trim(ctx context.Context, kind, token string, n int) error {
	ok, err := h.st.Trim(ctx, h.me, kind, token, n)
	if err != nil {
		return err
	}
	if ok != 1 {
		return errTurnLost
	}
	return nil
}

// factsOf asks the hand-off's main Redis which receipt facts are gone
// (receiptfacts.FactsGone).
type factsOf struct{ st dal.HandOffs }

func (f factsOf) Gone(ctx context.Context, keys []string) (map[string]bool, error) {
	return f.st.FactsGone(ctx, keys)
}

// importMO stages up to most of the incoming messages parked for this
// gateway, from the head. It returns how many it took off the list.
func (h *HandOff) importMO(ctx context.Context, s MOStager, token string, most int) (int, error) {
	raw, err := h.st.Head(ctx, h.me, KindMO, most)
	if err != nil || len(raw) == 0 {
		return 0, err
	}
	for from := 0; from < len(raw); from += fileChunk {
		to := min(from+fileChunk, len(raw))
		staged := 0
		var serr error
		for _, item := range raw[from:to] {
			var e parked
			var m model.Message
			if json.Unmarshal([]byte(item), &e) != nil || json.Unmarshal(e.X, &m) != nil {
				h.unreadable.Add(1)
				staged++
				continue
			}
			if err := s.StageMO(ctx, m); err != nil && !duplicate(err) {
				serr = err
				break
			}
			staged++
			h.importedMO.Add(1)
		}
		if staged > 0 {
			if err := h.trim(ctx, KindMO, token, staged); err != nil {
				return from, err
			}
		}
		if serr != nil {
			return from + staged, serr
		}
	}
	return len(raw), nil
}

// duplicate is MySQL's "Duplicate entry" (1062): an MO staged before a crash
// cut the trim is in the queue already. Redis stages one MO id once anyway.
func duplicate(err error) bool {
	s := err.Error()
	return strings.Contains(s, "Error 1062") || strings.Contains(s, "Duplicate entry")
}

// takeStrays is the one-time walk's lock: one process of the gateway at a
// time (main Redis; the walk deletes as it goes, so a second would only find
// less).
func (h *HandOff) takeStrays(ctx context.Context, kind string) (release func(), ok bool) {
	token := newID()
	got, err := h.st.TakeStrays(ctx, h.me, kind, token, straysFor)
	if err != nil || !got {
		return nil, false
	}
	return func() {
		rel, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = h.st.DropStrays(rel, h.me, kind, token)
	}, true
}

// straysOfReceipts parks the client receipts this gateway's queue Redis holds
// for another gateway that reads another store: written before this build,
// where their owner never looks. Each is turned back into a receipt, so its
// owner renders and files it as its own.
func (h *HandOff) straysOfReceipts(ctx context.Context, s StrayReceipts) {
	release, ok := h.takeStrays(ctx, KindDLR)
	if !ok {
		return
	}
	defer release()
	n, err := s.HandOverReceiptsOf(ctx, h.knownOther, func(d model.ClientDLR) error {
		r := dlr.Receipt{MsgID: d.ClientMsgID, Status: model.Status(d.Status), Stat: dlr.Stat(d.Stat),
			ErrCode: dlr.ErrorCodeOf(d.Receipt), Src: d.Src, Dst: d.Dst, DoneAt: d.DoneAt, BindSlot: d.BindSlot,
			Delay: d.Delay, Owner: d.Owner}
		b, _ := json.Marshal(dlr.Parked{AccountID: d.AccountID, Receipt: r})
		if err := h.park(ctx, d.Owner, KindDLR, [][]byte{b}); err != nil {
			return err
		}
		h.straysDLR.Add(1)
		return nil
	})
	switch {
	case err != nil:
		h.logf("hand-off: receipts queued here for other gateways: %d parked for them, then %v (the rest at the next start)", n, err)
	case n > 0:
		h.logf("hand-off: %d receipt(s) queued here for other gateways parked for them", n)
	}
}

// straysOfMO is straysOfReceipts for incoming messages.
func (h *HandOff) straysOfMO(ctx context.Context, s StrayMO) {
	release, ok := h.takeStrays(ctx, KindMO)
	if !ok {
		return
	}
	defer release()
	n, err := s.HandOverMOOf(ctx, h.knownOther, func(m model.Message) error {
		b, err := json.Marshal(m)
		if err != nil {
			return nil
		}
		if err := h.park(ctx, m.QueueOwner, KindMO, [][]byte{b}); err != nil {
			return err
		}
		h.straysMO.Add(1)
		return nil
	})
	switch {
	case err != nil:
		h.logf("hand-off: incoming messages queued here for other gateways: %d parked for them, then %v (the rest at the next start)", n, err)
	case n > 0:
		h.logf("hand-off: %d incoming message(s) queued here for other gateways parked for them", n)
	}
}

// Section is the "handoff" part of this process's status: what it parked for
// other gateways, what it holds, what it imported, and what waits for this
// gateway (read from the main Redis).
func (h *HandOff) Section() map[string]any {
	if h == nil {
		return nil
	}
	h.pmu.Lock()
	to := make(map[string]int64, len(h.parkedTo))
	for o, n := range h.parkedTo {
		to[o] = n
	}
	h.pmu.Unlock()
	h.mu.RLock()
	store := h.mine
	h.mu.RUnlock()
	out := map[string]any{
		"parked_receipts": h.parkedDLR.Load(), "parked_mo": h.parkedMO.Load(), "parked_for": to,
		"held": h.Held(), "lost": h.lost.Load(), "mo_refused": h.refusedMO.Load(),
		"imported_receipts": h.importedDLR.Load(), "imported_mo": h.importedMO.Load(),
		"progress_dropped": h.progressDropped.Load(), "unreadable": h.unreadable.Load(),
		"strays_receipts": h.straysDLR.Load(), "strays_mo": h.straysMO.Load(),
		"queue_store": store, "queues_in_redis": h.queue,
		"every_sec": h.every.Seconds(), "per_interval": h.perTick,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, kind := range []string{KindDLR, KindMO} {
		n, oldest, err := h.Waiting(ctx, kind)
		if err != nil {
			continue
		}
		out["waiting_"+kind], out["oldest_"+kind+"_ms"] = n, oldest
	}
	return out
}

// Waiting is how many items of kind are parked for this gateway, and how
// long the oldest has waited (ms, by Redis's clock).
func (h *HandOff) Waiting(ctx context.Context, kind string) (int64, int64, error) {
	n, s, now, err := h.st.Waiting(ctx, h.me, kind)
	if err != nil {
		return 0, 0, err
	}
	var e parked
	var oldest int64
	if s != "" && json.Unmarshal([]byte(s), &e) == nil && e.P > 0 {
		oldest = max(0, now.UnixMilli()-e.P)
	}
	return n, oldest, nil
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
