package receiptfacts

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/msisdn"
)

// The receipt facts: ONE key a message (Saleem, 29 Oct 2026: "an receipt can
// have only 3 calls at max, 1 set in redis, 2 read from redis, 3 delete from
// redis" — then "even the get and del can go in call").
//
//	rf:<scope>:<vendor msg id>   all of the message's facts, one value
//	                             (SET … EX the vendor's wait, when it is sent)
//
// A final receipt takes them with GETDEL: Redis hands the value to exactly one
// caller, and that caller closes the message. A progress receipt reads them
// with GET. Nothing else is kept per message: no hash beside the pointer, no
// pointer by number, no waiting sets, no told mark.
//
// Two shapes add to that, and only for what needs them:
//
//   - the messages a vendor's id names when it names more than one (a long
//     message an HTTP vendor took as one call; a bulk call the vendor gave one
//     id): one value per line under the id's key, each added with APPEND;
//   - a message sent as several PDUs: each part's own key (SET with its part
//     and the number of parts), and at its final receipt one INCRBY on the
//     message's part counter rfp:<msg id> (MarkPart).
//
// Before 29 Oct 2026 a message cost 29 commands here (a hash, two pointers,
// two waiting sets with their TTL probes and the set of sets, notes of every
// key, a close that rewrote them all): 2 now.

// OpenFor is how long facts are kept when the message's vendor names no wait
// of its own.
const OpenFor = 48 * time.Hour

// On is YAKU_RECEIPTS_IN_REDIS: receipts are finished in Redis unless it says
// 0, false, off or no. (Whether there IS a Redis is the caller's to know.)
func On() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("YAKU_RECEIPTS_IN_REDIS"))) {
	case "0", "false", "off", "no":
		return false
	}
	return true
}

// Store keeps receipt facts in Redis.
//
// Put does not write: it hands the facts to a flusher that files everything
// handed over in the last few milliseconds in ONE round trip. The caller is
// the goroutine that reads a vendor's answers, and a round trip a message on
// it is that bind's ceiling.
type Store struct {
	f dal.ReceiptFacts

	mu      sync.Mutex
	pending []put
	wake    chan struct{}
	once    sync.Once
	// retryAt: a flush failed (Redis away), and what it held waits at the
	// head of pending until then.
	retryAt time.Time
	// lost counts facts let go unfiled: more than unfiledMost waited while
	// Redis was away. errSaid spaces the log line to every ten seconds.
	lost    atomic.Int64
	errSaid atomic.Int64
}

type put struct {
	key string
	f   dlr.Facts
}

// New builds the fact store on the DAL's facts (the main Redis).
func New(f dal.ReceiptFacts) *Store { return &Store{f: f, wake: make(chan struct{}, 1)} }

// How the flusher paces itself: at once when flushAt are waiting, else every
// flushEvery.
const (
	flushEvery = 4 * time.Millisecond
	flushAt    = 256
)

// Lost is how many messages' facts this process let go unfiled (Redis away
// so long that more than unfiledMost waited).
func (s *Store) Lost() int64 { return s.lost.Load() }

// value is a message's facts as they are kept: short names, so a value is a
// line of JSON a little over two hundred bytes. V is the encoding's version;
// X is when the facts run out (Unix seconds), so facts put back keep the life
// they had.
type value struct {
	V  int    `json:"v"`
	M  string `json:"m"`
	A  int64  `json:"a,omitempty"`
	NR bool   `json:"nr,omitempty"`
	FO bool   `json:"fo,omitempty"`
	WI bool   `json:"wi,omitempty"`
	BS int    `json:"bs,omitempty"`
	PV int    `json:"pv,omitempty"`
	MC string `json:"mc,omitempty"`
	MN string `json:"mn,omitempty"`
	S  string `json:"s,omitempty"`
	D  string `json:"d,omitempty"`
	O  string `json:"o,omitempty"`
	T  string `json:"t,omitempty"`
	VI int64  `json:"vi,omitempty"`
	I  string `json:"i,omitempty"`
	G  string `json:"g,omitempty"`
	B  string `json:"b,omitempty"`
	PT int    `json:"pt,omitempty"`
	PP int    `json:"pp,omitempty"`
	TD string `json:"td,omitempty"`
	FC bool   `json:"fc,omitempty"`
	X  int64  `json:"x"`
}

const valueVersion = 1

func lifeOf(f dlr.Facts) time.Duration {
	if f.Wait > 0 {
		return f.Wait
	}
	return OpenFor
}

func encode(f dlr.Facts, now time.Time) string {
	life := lifeOf(f)
	b, _ := json.Marshal(value{V: valueVersion, M: f.MsgID, A: f.AccountID, NR: f.NoReceipt, FO: f.FailureOnly,
		WI: f.WantIntermediate, BS: f.BindSlot, PV: int(f.Privacy), MC: f.MCC, MN: f.MNC, S: f.Src, D: f.Dst,
		O: f.Owner, T: f.Table, VI: f.VendorID, I: f.VendorMsgID, G: f.Group, B: f.Bill, PT: f.Parts, PP: f.Part,
		TD: f.Told, FC: f.FirstCloses, X: now.Add(life).Unix()})
	return string(b)
}

// decode reads a key's value: one message, or a group's (one a line). A
// value it cannot read — a pointer of a build before 29 Oct 2026 held a
// message id, not facts — reads as nothing.
func decode(raw string) []dlr.Facts {
	var out []dlr.Facts
	for _, line := range strings.Split(raw, "\n") {
		if line == "" || line[0] != '{' {
			continue
		}
		var v value
		if json.Unmarshal([]byte(line), &v) != nil || v.M == "" {
			continue
		}
		f := dlr.Facts{MsgID: v.M, AccountID: v.A, NoReceipt: v.NR, FailureOnly: v.FO, WantIntermediate: v.WI,
			BindSlot: v.BS, Privacy: model.Privacy(v.PV), MCC: v.MC, MNC: v.MN, Src: v.S, Dst: v.D, Owner: v.O,
			Table: v.T, VendorID: v.VI, VendorMsgID: v.I, Group: v.G, Bill: v.B, Parts: v.PT, Part: v.PP, Told: v.TD,
			FirstCloses: v.FC}
		if v.X > 0 {
			// What is left of its life: facts put back keep it, and a long
			// message's part counter lives as long.
			f.Wait = time.Second
			if left := time.Until(time.Unix(v.X, 0)); left > time.Second {
				f.Wait = left
			}
		}
		out = append(out, f)
	}
	return out
}

// Put files the facts of a message a vendor has just taken — in the next
// flush, a few milliseconds on. A message alone replaces whatever an earlier
// one left under the same id; a group member is added to the id's group.
func (s *Store) Put(_ context.Context, scope int64, vendorMsgID string, f dlr.Facts) error {
	s.once.Do(func() { go s.flusher() })
	s.mu.Lock()
	s.pending = append(s.pending, put{key: dlr.FactsKey(scope, vendorMsgID), f: f})
	n := len(s.pending)
	s.mu.Unlock()
	if n >= flushAt {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// flusher files what Put was handed, for the life of the process.
func (s *Store) flusher() {
	t := time.NewTicker(flushEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-s.wake:
		}
		s.Flush(context.Background())
	}
}

// How facts that could not be filed are kept: tried again every
// unfiledRetry, at most unfiledMost of them (past that the oldest are let
// go, and counted as lost).
const (
	unfiledRetry = 500 * time.Millisecond
	unfiledMost  = 200000
)

// Flush files everything handed to Put so far, in one round trip: one SET a
// message (APPEND and EXPIRE for a group member).
//
// Facts it cannot file (Redis away) are KEPT and filed with the next flush
// that can (go-live test 18 Oct 2026, P5 E8): with Redis set the facts are the
// only place a message waits for its receipt.
func (s *Store) Flush(ctx context.Context) {
	s.mu.Lock()
	if time.Now().Before(s.retryAt) {
		s.mu.Unlock()
		return
	}
	batch := s.pending
	s.pending = nil
	s.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	now := time.Now()
	puts := make([]dal.FactPut, 0, len(batch))
	for _, e := range batch {
		life := lifeOf(e.f)
		if e.f.Group != "" {
			puts = append(puts, dal.FactPut{Key: e.key, Value: encode(e.f, now) + "\n", Life: life, Add: true})
			continue
		}
		puts = append(puts, dal.FactPut{Key: e.key, Value: encode(e.f, now), Life: life})
	}
	if err := s.f.File(ctx, puts); err != nil {
		// Not filed: kept, ahead of what came since, and tried again.
		s.mu.Lock()
		s.pending = append(batch, s.pending...)
		over := len(s.pending) - unfiledMost
		if over > 0 {
			s.pending = append([]put(nil), s.pending[over:]...)
			s.lost.Add(int64(over))
		}
		waiting := len(s.pending)
		s.retryAt = time.Now().Add(unfiledRetry)
		s.mu.Unlock()
		n := time.Now().UnixNano()
		if last := s.errSaid.Load(); n-last > int64(10*time.Second) && s.errSaid.CompareAndSwap(last, n) {
			log.Printf("[receipts] could not file the facts of %d message(s) in Redis (%v) — kept, and filed when Redis takes them", waiting, err)
		}
	}
}

// Unfiled is how many messages' facts wait to be filed (Redis was away).
func (s *Store) Unfiled() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// pick is the member of a group a receipt is about: the one sent to number
// when it is one of them, else the newest.
func pick(fs []dlr.Facts, number string) int {
	best := -1
	if number != "" {
		for i, f := range fs {
			if msisdn.Same(f.Dst, number) && (best < 0 || f.MsgID > fs[best].MsgID) {
				best = i
			}
		}
		if best >= 0 {
			return best
		}
	}
	best = 0
	for i := range fs {
		if fs[i].MsgID > fs[best].MsgID {
			best = i
		}
	}
	return best
}

// Get reads a message's facts by the vendor's id, for a progress receipt:
// nothing changes. Of a group, the member sent to number, else the newest.
// [dlr.FactStore]
func (s *Store) Get(ctx context.Context, scope int64, vendorMsgID, number string) (dlr.Facts, bool, error) {
	raw, found, err := s.f.Get(ctx, dlr.FactsKey(scope, vendorMsgID))
	if err != nil {
		return dlr.Facts{}, false, err
	}
	if !found {
		return dlr.Facts{}, false, nil
	}
	fs := decode(raw)
	if len(fs) == 0 {
		return dlr.Facts{}, false, nil
	}
	return fs[pick(fs, number)], true, nil
}

// Take reads a message's facts and deletes them in one call (GETDEL): the
// caller closes the message, and nobody else can. [dlr.FactStore]
func (s *Store) Take(ctx context.Context, scope int64, vendorMsgID, number string) ([]dlr.Facts, error) {
	raw, found, err := s.f.Take(ctx, dlr.FactsKey(scope, vendorMsgID))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return s.settle(ctx, scope, vendorMsgID, decode(raw), number)
}

// settle is what a taken value gives a receipt: a message alone; every part
// of a long message sent as one call; of a bulk call's messages, the one the
// receipt is about — the others go back.
func (s *Store) settle(ctx context.Context, scope int64, vendorMsgID string, fs []dlr.Facts, number string) ([]dlr.Facts, error) {
	if len(fs) <= 1 {
		return fs, nil
	}
	i := pick(fs, number)
	if fs[i].Group == dlr.GroupParts {
		var out []dlr.Facts
		for _, f := range fs {
			if f.Group == dlr.GroupParts && f.AccountID == fs[i].AccountID && f.Dst == fs[i].Dst {
				out = append(out, f)
			}
		}
		return out, nil
	}
	rest := make([]dlr.Facts, 0, len(fs)-1)
	rest = append(rest, fs[:i]...)
	rest = append(rest, fs[i+1:]...)
	if err := s.PutBack(ctx, scope, vendorMsgID, rest); err != nil {
		log.Printf("[receipts] %d message(s) of a bulk call under one id could not be filed again: %v", len(rest), err)
	}
	return []dlr.Facts{fs[i]}, nil
}

// PutBack files taken facts again, with what was left of their life.
// [dlr.FactStore]
func (s *Store) PutBack(ctx context.Context, scope int64, vendorMsgID string, fs []dlr.Facts) error {
	if len(fs) == 0 {
		return nil
	}
	now := time.Now()
	var life time.Duration
	lines := make([]string, 0, len(fs))
	for _, f := range fs {
		if f.Wait > life {
			life = f.Wait
		}
		lines = append(lines, encode(f, now))
	}
	val := lines[0]
	if len(fs) > 1 || fs[0].Group != "" {
		val = strings.Join(lines, "\n") + "\n"
	}
	if life <= 0 {
		life = OpenFor
	}
	return s.f.Set(ctx, dlr.FactsKey(scope, vendorMsgID), val, life)
}

// TakeMany is many looks in ONE round trip: GETDEL for an ask that closes, GET
// for one that does not, answered in the order asked. [dlr.ManyFacts]
func (s *Store) TakeMany(ctx context.Context, asks []dlr.FactAsk) ([][]dlr.Facts, error) {
	out := make([][]dlr.Facts, len(asks))
	if len(asks) == 0 {
		return out, nil
	}
	looks := make([]dal.FactLook, len(asks))
	for i, a := range asks {
		looks[i] = dal.FactLook{Key: dlr.FactsKey(a.Scope, a.VendorMsgID), Take: a.Close}
	}
	vals, found, err := s.f.Looks(ctx, looks)
	if err != nil {
		return nil, err
	}
	for i, raw := range vals {
		if !found[i] {
			continue
		}
		fs := decode(raw)
		if len(fs) == 0 {
			continue
		}
		if !asks[i].Close {
			out[i] = []dlr.Facts{fs[pick(fs, asks[i].Number)]}
			continue
		}
		if out[i], err = s.settle(ctx, asks[i].Scope, asks[i].VendorMsgID, fs, asks[i].Number); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// partKey is a message's part counter.
func partKey(msgID string) string { return "rfp:" + msgID }

// partBad is what a failed or expired part adds to the counter: far over any
// number of parts, so one INCRBY says both "a part failed" and "how many".
const partBad = 1_000_000

// MarkFirstPart is MarkPart for a vendor whose first final closes a long
// message (dlr.Facts.FirstCloses): every final adds 1 on the same counter, in
// ONE call; the count of 1 closes the message with that part's status, and
// every part after it is recorded only. [dlr.FirstPartMarker]
func (s *Store) MarkFirstPart(ctx context.Context, msgID string, life time.Duration) (ready, finalAlready bool, err error) {
	if life <= 0 {
		life = OpenFor
	}
	n, err := s.f.CountPart(ctx, partKey(msgID), 1, life)
	if err != nil {
		return false, false, err
	}
	return n == 1, n > 1, nil
}

// MarkPart counts one part's final receipt on its message's counter, in ONE
// call (INCRBY; an EXPIRE as well the first time the counter is made): a
// delivered part adds 1, a failed or expired one partBad. The first failed
// part closes the message failed; the part that brings the delivered ones to
// the number of parts closes it delivered; every part after the close is
// told so. [dlr.PartMarker]
func (s *Store) MarkPart(ctx context.Context, msgID string, parts int, st model.Status, life time.Duration) (agg model.Status, ready, finalAlready bool, err error) {
	add := int64(1)
	if st != model.StatusDelivered {
		add = partBad
	}
	k := partKey(msgID)
	if life <= 0 {
		life = OpenFor
	}
	// One INCRBY; an EXPIRE as well when it made the counter (it lives as
	// long as the message's facts).
	n, err := s.f.CountPart(ctx, k, add, life)
	if err != nil {
		return "", false, false, err
	}
	if add == partBad {
		if n < 2*partBad {
			return model.StatusFailed, true, false, nil // the first failed part
		}
		return model.StatusFailed, false, true, nil
	}
	switch {
	case n >= partBad:
		return model.StatusFailed, false, true, nil // a part failed before
	case n == int64(parts):
		return model.StatusDelivered, true, false, nil
	case n > int64(parts):
		return model.StatusDelivered, false, true, nil // a copy's parts counted on
	}
	return "", false, false, nil
}

// FactsGone is the DAL's look at which facts keys are gone (one EXISTS a
// key, in one round trip): the facts store, or the hand-off's main Redis.
type FactsGone interface {
	Gone(ctx context.Context, keys []string) (map[string]bool, error)
}

// ClosedOf says which of these facts keys are gone: their message was
// closed (or its facts ran out). The hand-off (internal/handoff) asks it
// before a progress receipt parked for this gateway is filed: one for a
// message closed meanwhile is not sent. One EXISTS a key, in one round trip.
func ClosedOf(ctx context.Context, g FactsGone, keys []string) (map[string]bool, error) {
	return g.Gone(ctx, keys)
}

// DropOldKeys lets go, once for the whole gateway, of what the receipt facts
// kept before 29 Oct 2026 that nothing reads any more: the facts hashes
// (rfm:), the waiting sets and their list (wait:), the told marks (told:), the
// groups' sets (rf:…:g) and the pointers by number (rf:…#…). One SCAN of the
// keyspace, by the first dlr role to start on this build; the others see the
// mark (rf:upgraded:2026-10-29) and skip it. The old one-message pointers
// (rf:<scope>:<id>, holding a message id) are left to run out: a receipt that
// takes one reads nothing.
func (s *Store) DropOldKeys(ctx context.Context) (int64, error) { return s.f.DropOldKeys(ctx) }
