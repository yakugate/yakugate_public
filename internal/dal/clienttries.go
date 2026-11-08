package dal

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/yakugate/yakugate/internal/model"
)

// Every try to give a client an incoming message (MO) or a receipt, and what
// the client answered (D-140, Saleem 5 Nov 2026): "same design as we did for
// mt — introduce sub_ for mo and dlr … no updates at all, and all mo, dlr and
// their subs should support bulk/batch inserts".
//
// mo_sub_<company>_<day> and dlr_sub_<company>_<day> (hot/0076): one row per
// try, bind or webhook, written once and never changed, filed by the day of
// the message's id. A webhook try carries the call as it went (pushcall).
// An MO's status is its final row (final_of), worked out when read, as an
// MT's is its closing receipt.

// The kinds of try.
const (
	TryMO  = "mo"
	TryDLR = "dlr"
)

// The channels a try goes by; "" = no try (the gateway gave up).
const (
	TrySMPP = "smpp"
	TryHTTP = "http"
)

// The results of a try.
const (
	TryAccepted = "accepted"  // the client took it
	TryRefused  = "refused"   // the client answered no
	TryNoAnswer = "no_answer" // no answer in time, or the call could not be made
	TryExpired  = "expired"   // the gateway gave up: out of time or out of tries
	TryFailed   = "failed"    // the gateway could not hand it over and will not try
)

// ClientTry is one row of mo_sub_ or dlr_sub_.
type ClientTry struct {
	// TryID is made when the try is made (Note fills it): a write that is
	// repeated leaves out the rows that are there.
	TryID string `json:"y"`
	Kind  string `json:"k"` // TryMO | TryDLR
	// MsgID is the MO, or the message a receipt is about; ClientMsgID the
	// id the client was told when it differs (a part's id).
	MsgID       string `json:"m"`
	ClientMsgID string `json:"cm,omitempty"`
	CompanyID   int64  `json:"c,omitempty"`
	AccountID   int64  `json:"a,omitempty"`
	// Status and Stat: the receipt's words (dlr).
	Status  string `json:"st,omitempty"`
	Stat    string `json:"sw,omitempty"`
	Attempt int    `json:"n,omitempty"`
	Channel string `json:"ch,omitempty"`
	Result  string `json:"r"`
	// Final (mo): this row ends the MO — the try its client took, or the
	// gateway's giving up. The first final row of an MO is its final_of row
	// and is counted in summary_mo; a later one is written as a plain row.
	Final bool `json:"f,omitempty"`
	// CommandStatus is the client's deliver_sm_resp status (smpp; nil = no
	// answer); HTTPStatus its webhook's (0 = no answer).
	CommandStatus *uint32 `json:"cs,omitempty"`
	HTTPStatus    int     `json:"hs,omitempty"`
	ErrorCode     string  `json:"ec,omitempty"`
	Error         string  `json:"er,omitempty"`
	EndpointID    int64   `json:"e,omitempty"`
	Session       string  `json:"ss,omitempty"`
	// The webhook call as it went (pushcall.Fill).
	Method      string `json:"mt,omitempty"`
	URL         string `json:"u,omitempty"`
	ReqHeaders  string `json:"qh,omitempty"`
	ReqBody     string `json:"qb,omitempty"`
	ReqCut      bool   `json:"qc,omitempty"`
	RespHeaders string `json:"rh,omitempty"`
	RespBody    string `json:"rb,omitempty"`
	RespCut     bool   `json:"rc,omitempty"`
	Sealed      bool   `json:"se,omitempty"`
	// Private: the two bodies hold an encrypted company's text and are
	// sealed before the row leaves the role (pushcall). Not written.
	Private bool      `json:"-"`
	TookMS  int64     `json:"t,omitempty"`
	At      time.Time `json:"at"`
	// Message (a final mo row): the MO itself, so a final whose mo_ row is
	// not written yet (the store was away when it was taken in) writes it.
	Message *model.Message `json:"msg,omitempty"`
}

// Status is the MO status a final row gives its message.
func (t ClientTry) MOStatus() model.Status {
	switch t.Result {
	case TryAccepted:
		return model.StatusDelivered
	case TryExpired:
		return model.StatusExpired
	}
	return model.StatusFailed
}

// TryResultOf is the result of a row that gives an MO the status st without
// a try (the gateway gave up, or the MO was written already ended).
func TryResultOf(st model.Status) string {
	switch st {
	case model.StatusDelivered:
		return TryAccepted
	case model.StatusExpired:
		return TryExpired
	}
	return TryFailed
}

// Code is a command_status for a ClientTry.
func Code(s uint32) *uint32 { return &s }

// ClientTries writes tries (the record store, and the record queue for the
// DB worker): many in one go, one multi-row INSERT per table. once: a try
// already there (its TryID) is not written again.
type ClientTries interface {
	WriteClientTries(ctx context.Context, tries []ClientTry, once bool) error
}

// TryLog writes tries off the push path, in batches (D-140): a role notes
// each try and goes on; every TryLogEvery, or every TryLogBatch tries, they
// go to the store in one call. A batch the store refuses is kept and sent
// again with the next; what passes TryLogBuffer waiting is dropped and
// counted (a push is never held up by its own log). An MO's final row is
// not written here: it is written before the MO leaves the queue.
type TryLog struct {
	w    ClientTries
	prep func(ctx context.Context, ts []ClientTry) []ClientTry
	logf func(string, ...any)

	mu      sync.Mutex
	wait    []ClientTry
	kick    chan struct{}
	closed  atomic.Bool
	start   sync.Once
	stop    chan struct{}
	done    chan struct{}
	dropped atomic.Int64
	written atomic.Int64
	refused atomic.Int64
	lastErr atomic.Value
}

const (
	TryLogBuffer = 50000
	TryLogBatch  = 500
	TryLogEvery  = 250 * time.Millisecond
)

// NewTryLog is a log writing to w. prep, when set, works on each batch before
// it is written (pushcall seals an encrypted company's bodies there). w nil:
// a log that keeps nothing.
func NewTryLog(w ClientTries, prep func(context.Context, []ClientTry) []ClientTry, logf func(string, ...any)) *TryLog {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &TryLog{w: w, prep: prep, logf: logf, kick: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
}

// On says whether tries are kept.
func (l *TryLog) On() bool { return l != nil && l.w != nil }

// NewTryID is a try's id.
func NewTryID() string { return ulid.Make().String() }

// Note keeps one try. Never blocks.
func (l *TryLog) Note(t ClientTry) {
	if !l.On() || l.closed.Load() || t.MsgID == "" {
		return
	}
	if t.TryID == "" {
		t.TryID = NewTryID()
	}
	if t.At.IsZero() {
		t.At = time.Now()
	}
	l.start.Do(func() { go l.run() })
	l.mu.Lock()
	if len(l.wait) >= TryLogBuffer {
		l.mu.Unlock()
		if n := l.dropped.Add(1); n == 1 || n%1000 == 0 {
			l.logf("client tries: %d not kept — the record store is not keeping up", n)
		}
		return
	}
	l.wait = append(l.wait, t)
	full := len(l.wait) >= TryLogBatch
	l.mu.Unlock()
	if full {
		select {
		case l.kick <- struct{}{}:
		default:
		}
	}
}

func (l *TryLog) run() {
	defer close(l.done)
	tk := time.NewTicker(TryLogEvery)
	defer tk.Stop()
	for {
		select {
		case <-l.stop:
			deadline := time.Now().Add(5 * time.Second)
			for l.pending() > 0 && time.Now().Before(deadline) {
				if !l.flush() {
					time.Sleep(200 * time.Millisecond)
				}
			}
			return
		case <-tk.C:
		case <-l.kick:
		}
		for l.pending() > 0 && l.flush() {
			if l.pending() < TryLogBatch {
				break
			}
		}
	}
}

func (l *TryLog) pending() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.wait)
}

// flush writes the oldest batch; false when the store refused it (it stays,
// first in line).
func (l *TryLog) flush() bool {
	l.mu.Lock()
	n := min(len(l.wait), TryLogBatch)
	batch := append([]ClientTry(nil), l.wait[:n]...)
	l.mu.Unlock()
	if n == 0 {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows := batch
	if l.prep != nil {
		rows = l.prep(ctx, append([]ClientTry(nil), batch...))
	}
	if err := l.w.WriteClientTries(ctx, rows, false); err != nil {
		l.lastErr.Store(err.Error())
		if c := l.refused.Add(1); c == 1 || c%100 == 0 {
			l.logf("client tries: %d not written yet, kept to try again: %v", n, err)
		}
		return false
	}
	l.mu.Lock()
	l.wait = l.wait[n:]
	if len(l.wait) == 0 {
		l.wait = nil
	}
	l.mu.Unlock()
	l.written.Add(int64(n))
	return true
}

// Close writes what still waits (at most a few seconds) and stops. Safe on a
// nil log or one that never started.
func (l *TryLog) Close() {
	if !l.On() || !l.closed.CompareAndSwap(false, true) {
		return
	}
	l.start.Do(func() { go l.run() })
	close(l.stop)
	select {
	case <-l.done:
	case <-time.After(7 * time.Second):
	}
}

// Stats is what the log did, for the role's status.
func (l *TryLog) Stats() map[string]any {
	if l == nil {
		return nil
	}
	st := map[string]any{"written": l.written.Load(), "dropped": l.dropped.Load(),
		"refused_batches": l.refused.Load(), "waiting": l.pending()}
	if e, ok := l.lastErr.Load().(string); ok {
		st["last_error"] = e
	}
	return st
}
