package httpvendor

import (
	"context"
	"time"

	"github.com/oklog/ulid/v2"

	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
)

// Poll pacing (QA EP-021, SOAP-006, 2026-10-01).
//
// A poll row with no interval used to be polled on every 500 ms Step, for
// every message still 'sent', for ever: one QA message drew 40 calls in 20 s,
// fifteen drew 5,944 in six minutes. And a vendor answering DELIVRD — not in
// the row's status map, which the Builder cannot even set — never finished
// its message, so the flood never ended. Now:
//
//   - each message is polled at once, then 10 s later, then at double the
//     gap each time up to every 10 minutes — or at the row's own interval
//     when it sets one;
//   - a message older than a day is not polled any more. Its age is read
//     from its id, so it counts from ACCEPTANCE: a message scheduled to go
//     out more than a day after it was accepted is never polled. Nothing
//     then moves it out of 'sent' — like a vendor that never sends a
//     receipt; the account's receipt policy (no_receipt) can still answer
//     the client;
//   - the schedule lives in this process's memory: after a restart every
//     waiting message is polled once more at once (at most pollBudget calls
//     per vendor per Step), then backs off from 10 s again;
//   - the vendor's own word is read like a receipt's (dlr.ParseStat), so
//     DELIVRD, UNDELIV, REJECTD, DELIVERED, FAILED … finish a message with no
//     status map at all;
//   - the waiting messages are walked page by page in the order they were
//     accepted, so a vendor with more than one page waiting reaches them all.
const (
	pollFirstGap = 10 * time.Second
	pollMaxGap   = 10 * time.Minute
	// The waiting lists hold only messages still within their vendor's wait
	// (D-136: the receipt facts' life in Redis, open_msgs.wait_until), so the
	// floor is the longest wait a vendor may set (30 days), not a day.
	pollGiveUp = 720 * time.Hour
	pollPage   = 200 // waiting messages read per cycle
	pollBudget = 50  // vendor calls per vendor per cycle
)

// pollGap is the wait after a message's tries-th poll that did not finish it.
// A configured interval is used as it is.
func pollGap(tries int, interval time.Duration) time.Duration {
	if interval > 0 {
		return interval
	}
	d := pollFirstGap
	for i := 1; i < tries && d < pollMaxGap; i++ {
		d *= 2
	}
	return min(d, pollMaxGap)
}

// pollFinal says whether a poll answer ends the message. UNKNOWN does not,
// although SMPP counts it final: a status service saying it does not know
// yet is the commonest answer for a message still on its way, and failing
// the message on it would be guessing.
func pollFinal(status string) bool {
	s := dlrsvc.ParseStat(status, nil)
	return s.Final() && s != dlrsvc.StatUnknown
}

// pollFloor is the smallest message id still polled: the first id of the
// millisecond pollGiveUp ago.
func pollFloor(now time.Time) string {
	var id ulid.ULID
	if err := id.SetTime(ulid.Timestamp(now.Add(-pollGiveUp))); err != nil {
		return ""
	}
	return id.String()
}

type pollEntry struct {
	next  time.Time
	tries int
	pass  int // the pass this message was last seen in
}

// pollSched is the per-message poll plan. Used from Step only, which is
// single-threaded, so it needs no lock (like lastPoll).
type pollSched struct {
	entries map[int64]map[string]*pollEntry // vendor id -> vendor msg id
	cursor  map[int64]string                // vendor id -> last msg id read
	pass    map[int64]int
}

func newPollSched() *pollSched {
	return &pollSched{entries: map[int64]map[string]*pollEntry{}, cursor: map[int64]string{}, pass: map[int64]int{}}
}

// start is where this cycle's page begins: after the last row read, never
// before the oldest message still polled.
func (s *pollSched) start(vendorID int64, now time.Time) string {
	after := s.cursor[vendorID]
	if floor := pollFloor(now); after < floor {
		after = floor
	}
	return after
}

// due says whether a message may be polled now, and marks it seen in this
// pass.
func (s *pollSched) due(vendorID int64, vmid string, now time.Time) bool {
	e := s.entry(vendorID, vmid)
	e.pass = s.pass[vendorID]
	return !now.Before(e.next)
}

// polled records a poll that did not finish the message.
func (s *pollSched) polled(vendorID int64, vmid string, now time.Time, interval time.Duration) {
	e := s.entry(vendorID, vmid)
	e.tries++
	e.next = now.Add(pollGap(e.tries, interval))
}

// applyPolled applies a final poll answer (dlr.Service.Apply: status, raw
// receipt, client push). A variable so a test can stand in for the receipt
// service.
var applyPolled = func(ctx context.Context, p *Process, vendorID int64, vmid, status, errCode string) error {
	_, err := p.svc.Apply(ctx, vendorID, vmid, status, errCode, "", nil)
	return err
}

// read moves the cursor past one row.
func (s *pollSched) read(vendorID int64, msgID string) { s.cursor[vendorID] = msgID }

// wrap ends a pass over the vendor's waiting messages: back to the start, and
// every message not seen in the pass is forgotten (it finished by a receipt,
// expired, or aged out).
func (s *pollSched) wrap(vendorID int64) {
	pass := s.pass[vendorID]
	for id, e := range s.entries[vendorID] {
		if e.pass < pass {
			delete(s.entries[vendorID], id)
		}
	}
	s.pass[vendorID] = pass + 1
	s.cursor[vendorID] = ""
}

// forget drops a vendor no longer served.
func (s *pollSched) forget(vendorID int64) {
	delete(s.entries, vendorID)
	delete(s.cursor, vendorID)
	delete(s.pass, vendorID)
}

func (s *pollSched) entry(vendorID int64, vmid string) *pollEntry {
	m := s.entries[vendorID]
	if m == nil {
		m = map[string]*pollEntry{}
		s.entries[vendorID] = m
	}
	e := m[vmid]
	if e == nil {
		e = &pollEntry{pass: s.pass[vendorID]}
		m[vmid] = e
	}
	return e
}
