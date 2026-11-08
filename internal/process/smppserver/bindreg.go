package smppserver

// The live binds of each account, and which one a receipt or an incoming
// message goes down.

import (
	"hash/fnv"
	"sort"
	"sync"
)

// bindRegistry tracks live receive-capable (RX/TRX) client sessions per account,
// so the smsc leg can push deliver_sm receipts to bound clients. [SCOPE P5]
type bindRegistry struct {
	mu sync.Mutex
	m  map[int64][]*session
	// rr is the per-account round-robin cursor used whenever a push cannot go
	// down its originating bind (see sessionForSlot) and by MO delivery, which
	// spreads across binds by design.
	rr map[int64]int
}

func newBindRegistry() *bindRegistry { return &bindRegistry{m: map[int64][]*session{}} }

// canReceive reports whether a receipt or an incoming message may be pushed
// down this bind: it is a receiver or transceiver, and the gateway is not in
// the middle of ending it (endBind) — a push to a bind that has been told to
// unbind would only wait out its answer and be sent again.
func (s *session) canReceive() bool {
	return (s.mode == modeRX || s.mode == modeTRX) && !s.ending.Load()
}

// add registers a bind and hands it a SLOT — the lowest number not currently
// in use by that account (PO 2026-09-05). The slot is what a receipt is routed
// back by: it travels with the message (queue + record_index) and comes back on
// the client_dlr job. A number rather than a per-connection id precisely so it
// survives a reconnect: a client that drops bind 3 and comes back takes the
// free slot 3, and its in-flight receipts still land where they started.
func (r *bindRegistry) add(acct int64, s *session) {
	r.mu.Lock()
	used := map[int]bool{}
	for _, x := range r.m[acct] {
		used[x.slot] = true
	}
	slot := 1
	for used[slot] {
		slot++
	}
	s.slot = slot
	r.m[acct] = append(r.m[acct], s)
	r.mu.Unlock()
}

// addIfRoom is add, refused when the account already holds max binds
// (max 0 = no limit). Counting and adding are ONE step here: done apart, two
// binds arriving together both saw room for one and both got in.
func (r *bindRegistry) addIfRoom(acct int64, s *session, max int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if max > 0 && len(r.m[acct]) >= max {
		return false
	}
	used := map[int]bool{}
	for _, x := range r.m[acct] {
		used[x.slot] = true
	}
	slot := 1
	for used[slot] {
		slot++
	}
	s.slot = slot
	r.m[acct] = append(r.m[acct], s)
	return true
}

// bound is every live bind, by account, as of now (a copy the caller owns).
func (r *bindRegistry) bound() map[int64][]*session {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[int64][]*session, len(r.m))
	for a, ss := range r.m {
		out[a] = append([]*session(nil), ss...)
	}
	return out
}

func (r *bindRegistry) remove(acct int64, s *session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur := r.m[acct]
	for i, x := range cur {
		if x == s {
			r.m[acct] = append(cur[:i], cur[i+1:]...)
			break
		}
	}
	if len(r.m[acct]) == 0 {
		delete(r.m, acct)
	}
}

// receiveAccounts lists accounts with at least one RX/TRX bind — the ones the
// delivery legs can actually push to (TX-only accounts are registered for
// max_binds counting but cannot receive deliver_sm).
func (r *bindRegistry) receiveAccounts() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int64, 0, len(r.m))
	for a, ss := range r.m {
		for _, s := range ss {
			if s.canReceive() {
				out = append(out, a)
				break
			}
		}
	}
	return out
}

// count reports live binds across ALL modes (max_binds enforcement, DEC-2).
func (r *bindRegistry) count(acct int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.m[acct])
}

// sessionForSlot picks the bind a client-bound push should go down.
//
// Preference is the ORIGINATING bind — the slot the submit arrived on (PO
// 2026-09-05). Three things send us elsewhere, and all three mean the same
// thing, "use another of this account's binds, taking turns":
//
//   - slot 0: the message never came from a bind (HTTP submit), or predates
//     the column;
//   - the slot is not live here: the client reconnected, or that bind is held
//     by a different gateway instance;
//   - the slot IS live but is send-only (TX): a transmitter bind physically
//     cannot carry a deliver_sm, and a client running TX+RX separately is a
//     normal setup, not a misconfiguration.
//
// Taking turns rather than always answering with the newest bind is the other
// half of the fix: it is what actually spreads receipts across the binds a
// client opened.
func (r *bindRegistry) sessionForSlot(acct int64, slot int) *session {
	r.mu.Lock()
	defer r.mu.Unlock()
	ss := r.m[acct]
	if slot > 0 {
		for _, s := range ss {
			if s.slot == slot && s.canReceive() {
				return s
			}
		}
	}
	return r.nextLocked(acct, ss)
}

// nextLocked returns the account's next receive-capable bind in round-robin
// order. Caller holds r.mu.
func (r *bindRegistry) nextLocked(acct int64, ss []*session) *session {
	rx := make([]*session, 0, len(ss))
	for _, s := range ss {
		if s.canReceive() {
			rx = append(rx, s)
		}
	}
	if len(rx) == 0 {
		return nil
	}
	if r.rr == nil {
		r.rr = map[int64]int{}
	}
	i := r.rr[acct] % len(rx)
	r.rr[acct]++
	return rx[i]
}

// sessionAny returns a receive-capable bind for the account in round-robin
// order, ignoring slots entirely — MO delivery, which the PO deliberately
// spreads across sessions rather than pinning ("mo to be submitted randomly
// across the sessions"): an inbound message has no originating bind to go back
// to, since the subscriber sent it, not the client.
// receivers is acct's receive-capable binds, by slot.
func (r *bindRegistry) receivers(acct int64) []*session {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*session
	for _, s := range r.m[acct] {
		if s.canReceive() {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].slot < out[j].slot })
	return out
}

// sessionForMessage is the bind a message's receipts go down: the bind that
// submitted it when that bind is live and can receive (PO 2026-09-05), else
// the bind its id picks among the account's receive-capable ones — the same
// bind for all of one message's receipts, so one connection keeps their order
// (Saleem, 28 Oct 2026; it was the next bind in turn).
func (r *bindRegistry) sessionForMessage(acct int64, slot int, msgID string) *session {
	rx := r.receivers(acct)
	if len(rx) == 0 {
		return nil
	}
	if slot > 0 {
		for _, s := range rx {
			if s.slot == slot {
				return s
			}
		}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(msgID))
	return rx[int(h.Sum32()%uint32(len(rx)))]
}

func (r *bindRegistry) sessionAny(acct int64) *session {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.nextLocked(acct, r.m[acct])
}
