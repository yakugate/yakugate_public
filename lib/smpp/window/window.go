// Package window enforces the SMPP async send window for the vendor sender: it
// bounds unacknowledged submit_sm PDUs and maps sequence_number -> pending
// message so submit_sm_resp can be correlated. Sequence numbers wrap at
// 0x7FFFFFFF. Timed-out sends are surfaced but NEVER retransmitted (pitfall:
// a timeout is not a delivery failure — the message id may still be live). [SCOPE P3]
package window

import (
	"sync"
	"time"
)

// Pending is one outstanding submit_sm awaiting its response. A long message goes
// out as several PDUs, so WirePart/WireParts say which PDU of the send this is —
// transport bookkeeping that must NOT be confused with the message's own
// concatenation group (model.Message.Concat*), which is business data and gets
// persisted on the record. WireParts<=1 means the message was one PDU. [D-036/D-038]
//
// M is the message the caller keeps with each send (the engine's
// model.Message); the window never looks inside it.
type Pending[M any] struct {
	Seq       uint32
	Msg       M
	WirePart  int // 1-based index of this PDU within the send
	WireParts int // total PDUs this message was sent as
	SentAt    time.Time
	// TimedOut is when the send passed its response timeout (Expired); zero
	// while it is within it. Its answer can still come, late: Ack finds it
	// until DropLate lets it go.
	TimedOut time.Time
}

// IsLead reports whether this PDU is the one that finalizes the message's record
// (the first PDU of the send; a single-PDU message is always the lead).
func (p *Pending[M]) IsLead() bool { return p.WireParts <= 1 || p.WirePart == 1 }

// Window is a single connection's outstanding-send tracker (mutex-guarded).
//
// size is the window the vendor was configured with: the most sends that may
// ever be outstanding. limit is how many may be outstanding NOW — the sender
// lowers it while answers come back late and raises it while they come back
// promptly (Pace), so a size set far above what the path answers a second
// cannot fill with sends nobody will answer in time. A window no larger than
// StartLimit is simply its size, as it always was.
type Window[M any] struct {
	mu      sync.Mutex
	size    int
	limit   int
	seq     uint32
	pending map[uint32]*Pending[M]
	// late holds the sends that timed out, for a while, so that an answer
	// that comes after the timeout still finds its message (2026-10-18).
	// They take no room in the window.
	late map[uint32]*Pending[M]
	// peak is the most sends that were outstanding at one moment since the
	// last Pace: how much of the limit the sender really used (see Pace).
	peak int
}

// StartLimit is how many sends may be outstanding on a new connection before
// its answers have shown what it can carry.
const StartLimit = 64

// New builds a window of the given size (min 1).
func New[M any](size int) *Window[M] {
	if size <= 0 {
		size = 1
	}
	limit := size
	if limit > StartLimit {
		limit = StartLimit
	}
	return &Window[M]{size: size, limit: limit, pending: map[uint32]*Pending[M]{}, late: map[uint32]*Pending[M]{}}
}

// Limit reports how many sends may be outstanding now.
func (w *Window[M]) Limit() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.limit
}

// Pace moves the limit by how the answers are coming back, and returns it.
// Called once a tick by the sender:
//
//   - the oldest unanswered send is older than slow: fewer may be outstanding
//     (three quarters of the limit, never under StartLimit or the size);
//   - it is younger than quick AND the limit was nearly used since the last
//     call: more may be (a quarter more, never over the size);
//   - otherwise it stays.
//
// So the number outstanding settles where sends are answered within about
// `slow`, whatever size was configured — and climbs back to the full size as
// soon as the path answers that fast.
//
// "Nearly used" is the most that was outstanding since the last call (peak),
// not what is outstanding at this moment. Until 2026-10-13 it was this
// moment's count, and a vendor that answers inside one tick has NOTHING
// outstanding when the next tick begins: its limit never left StartLimit, so
// a session sent 64 messages a tick — 427 a second — whatever window and rate
// it was given (load run of that night: 4 sessions, 1,700 a second, every
// session's window_limit 64). The faster the vendor, the surer the cap.
func (w *Window[M]) Pace(now time.Time, quick, slow time.Duration) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	used := w.peak
	if n := len(w.pending); n > used {
		used = n
	}
	w.peak = len(w.pending) // the next tick starts from what is outstanding now
	if w.size <= StartLimit {
		w.limit = w.size
		return w.limit
	}
	var oldest time.Duration
	for _, p := range w.pending {
		if d := now.Sub(p.SentAt); d > oldest {
			oldest = d
		}
	}
	switch {
	case oldest > slow:
		w.limit = w.limit * 3 / 4
		if w.limit < StartLimit {
			w.limit = StartLimit
		}
	case oldest < quick && used*10 >= w.limit*8:
		step := w.limit / 4
		if step < 16 {
			step = 16
		}
		w.limit += step
		if w.limit > w.size {
			w.limit = w.size
		}
	}
	return w.limit
}

// Reserve allocates a sequence number for a single-PDU send.
func (w *Window[M]) Reserve(m M, now time.Time) (seq uint32, ok bool) {
	return w.ReservePart(m, now, 1, 1)
}

// ReservePart allocates a sequence number for PDU `part` of `parts` for one
// message, or ok=false if the window is full. Wraps to 1 at 0x7FFFFFFF.
func (w *Window[M]) ReservePart(m M, now time.Time, part, parts int) (seq uint32, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// The size is the hard bound here; the limit is applied by the sender
	// through Free, so the parts of one long message already begun are never
	// refused half way by a limit that has just been lowered.
	if len(w.pending) >= w.size {
		return 0, false
	}
	w.seq++
	if w.seq >= 0x7FFFFFFF {
		w.seq = 1
	}
	w.pending[w.seq] = &Pending[M]{Seq: w.seq, Msg: m, WirePart: part, WireParts: parts, SentAt: now}
	if n := len(w.pending); n > w.peak {
		w.peak = n
	}
	return w.seq, true
}

// Ack removes and returns the pending send for a response sequence number —
// or a send that timed out and is still kept (TimedOut is set on it then).
func (w *Window[M]) Ack(seq uint32) (*Pending[M], bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.pending[seq]
	if ok {
		delete(w.pending, seq)
		return p, true
	}
	if p, ok = w.late[seq]; ok {
		delete(w.late, seq)
	}
	return p, ok
}

// Expired takes sends older than ttl (timed out) out of the window, and
// returns them. The caller must NOT retransmit — it leaves the queue row for
// the retry/expiry processes. They are kept aside (TimedOut set) so a late
// answer still finds them, until DropLate.
func (w *Window[M]) Expired(ttl time.Duration, now time.Time) []*Pending[M] {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []*Pending[M]
	for seq, p := range w.pending {
		if now.Sub(p.SentAt) > ttl {
			p.TimedOut = now
			out = append(out, p)
			delete(w.pending, seq)
			w.late[seq] = p
		}
	}
	return out
}

// DropLate lets go of the timed-out sends kept longer than keep since their
// timeout, and returns them: an answer that comes now is not matched.
func (w *Window[M]) DropLate(keep time.Duration, now time.Time) []*Pending[M] {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []*Pending[M]
	for seq, p := range w.late {
		if now.Sub(p.TimedOut) > keep {
			out = append(out, p)
			delete(w.late, seq)
		}
	}
	return out
}

// Waiting returns the sends still waiting for an answer — in the window, or
// timed out and kept — that went out at least olderThan ago.
func (w *Window[M]) Waiting(olderThan time.Duration, now time.Time) []*Pending[M] {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []*Pending[M]
	for _, m := range []map[uint32]*Pending[M]{w.pending, w.late} {
		for _, p := range m {
			if now.Sub(p.SentAt) >= olderThan {
				out = append(out, p)
			}
		}
	}
	return out
}

// OldestAge reports how long the longest-outstanding send has been waiting
// for its response (0 when nothing is outstanding). A session uses it to stop
// sending more while its answers are coming back late.
func (w *Window[M]) OldestAge(now time.Time) time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	var oldest time.Duration
	for _, p := range w.pending {
		if d := now.Sub(p.SentAt); d > oldest {
			oldest = d
		}
	}
	return oldest
}

// Free reports how many more sends may go out now: the room under the limit
// (never negative).
func (w *Window[M]) Free() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if n := w.limit - len(w.pending); n > 0 {
		return n
	}
	return 0
}

// Size reports the configured window size (observability).
func (w *Window[M]) Size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// Depth reports how many sends are currently outstanding (observability).
func (w *Window[M]) Depth() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending)
}
