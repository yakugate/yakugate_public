package httpvendor

import (
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// An accepted answer with no vendor id (Saleem, 2026-10-12: "if there is
// nothing so, it should be undefined, not failed").
//
// The message is sent, and filed under our own id (vendorRef). When the send
// call reads NO id that is simply how the vendor is, and nothing is said.
// When the call does read one and the answer had nothing there, the operator
// should hear of it — the path is probably wrong (result.messageId for an id
// that is at messageId), and until it is right this vendor's receipts and
// status polls cannot be matched by its id. It is a warning in the vendor's
// log, never a failed message.
//
// One line a vendor a minute, with how many messages it stands for: a busy
// vendor with a wrong path would otherwise write a line per message.

const noIDWarnEvery = time.Minute

type noIDWarn struct {
	mu   sync.Mutex
	last map[int64]time.Time
	n    map[int64]int
}

// due counts one more message with no id for the vendor and reports whether
// a line is due now, and for how many messages.
func (w *noIDWarn) due(id int64, now time.Time) (int, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.last == nil {
		w.last, w.n = map[int64]time.Time{}, map[int64]int{}
	}
	w.n[id]++
	if last, ok := w.last[id]; ok && now.Sub(last) < noIDWarnEvery {
		return 0, false
	}
	n := w.n[id]
	w.last[id], w.n[id] = now, 0
	return n, true
}

// noteNoID says that a message this vendor accepted came back with no vendor
// id — only when the send call reads one, so there is something to fix.
func (p *Process) noteNoID(v vendorAdapter, msgID string) {
	if v.ad == nil {
		return
	}
	src := v.ad.IDSource()
	if src == "" {
		return // the call reads no id: the vendor gives none, by its own settings
	}
	n, ok := p.noID.due(v.id, time.Now())
	if !ok {
		return
	}
	p.log.ForVendor(v.cid).Warnf("send",
		"no vendor id read: the answer has nothing at %q, where the send call reads the vendor's id. %d message(s) were accepted on the success rule and filed under our own id (latest %s); their receipts and status polls cannot be matched by a vendor id until the path is right (HTTP Endpoints, the send call, Response, operator_id)",
		src, n, msgID)
}

// noteBulkAlone says, at most once a minute for a vendor, that its bulk calls
// are carrying one message each (Saleem, 2026-10-12: "i have changed http
// vendor's http endpoint to submit bulk but it is still sending single ones").
//
// Nothing is wrong when it happens: ONE bulk call carries one text from one
// sender to many numbers (httpadapter.SendBulk), so messages that each have
// their own text can only go one to a call. But the log said "bulk send (1
// msgs)" over and over and never why. Said only for a round with several
// messages of which no two could share a call.
func (p *Process) noteBulkAlone(v vendorAdapter, msgs, calls int) {
	if msgs < 2 || calls < msgs || v.ad == nil || !v.ad.BulkMode() {
		return
	}
	n, ok := p.bulkAlone.dueFor(v.id, msgs, time.Now())
	if !ok {
		return
	}
	p.log.ForVendor(v.cid).Infof("send",
		"bulk: %d message(s) went one to a call. One bulk call carries ONE text from one sender to many numbers (up to %d here), and each of these has its own text or sender. Messages with the same text and sender are sent together.",
		n, v.ad.BulkChunkSize())
}

// noteBulkIDs looks at the ids one accepted bulk call came back with (`read`
// = how many the answer held, httpadapter.SendBulkCall) and says, at most once
// a minute a vendor, what they were when they were not one a message. A list
// that cannot be matched to the messages is replaced by our own ids, so each
// message is filed alone and noteNoID does not take it for a wrong path.
//
// Full go-live retest, 19 Oct 2026 (P4 B8): a vendor answered ONE id for a
// call of five numbers and the call was failed and sent again.
func (p *Process) noteBulkIDs(v vendorAdapter, chunk []model.Message, ids []string, read int) {
	if len(chunk) < 2 || len(ids) != len(chunk) || read == 0 || read == len(chunk) {
		return
	}
	now := time.Now()
	if read == 1 {
		if v.ad != nil && v.ad.BulkSingleID() {
			return // the call says so: nothing to tell
		}
		if n, ok := p.bulkOneID.dueFor(v.id, len(chunk), now); ok {
			p.log.ForVendor(v.cid).Infof("send",
				"bulk: the vendor gave ONE id (%s) for a call of %d messages. %d message(s) were filed under the id of their call; a receipt with that id finds its message by the number it names (the receipt webhook call's \"to\" response value), else the newest message of the call still open",
				ids[0], len(chunk), n)
		}
		return
	}
	for i := range ids {
		ids[i] = chunk[i].ID
	}
	if n, ok := p.bulkIDCount.dueFor(v.id, len(chunk), now); ok {
		p.log.ForVendor(v.cid).Warnf("send",
			"bulk: the vendor gave %d id(s) for a call of %d messages, which cannot be matched to them. The call was accepted on the success rule; %d message(s) were filed under our own ids (latest call led by %s), so their receipts and status polls cannot be matched by a vendor id",
			read, len(chunk), n, chunk[0].ID)
	}
}

// dueFor is due for k messages at once.
func (w *noIDWarn) dueFor(id int64, k int, now time.Time) (int, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.last == nil {
		w.last, w.n = map[int64]time.Time{}, map[int64]int{}
	}
	w.n[id] += k
	if last, ok := w.last[id]; ok && now.Sub(last) < noIDWarnEvery {
		return 0, false
	}
	n := w.n[id]
	w.last[id], w.n[id] = now, 0
	return n, true
}
