package dlr

import (
	"context"
	"errors"
	"log"

	"github.com/yakugate/yakugate/internal/model"
)

// ManyMarker is an optional OrphanReplayer capability: every orphan a
// re-match pass placed noted in one call (mysql.Store.MarkOrphansPlaced).
type ManyMarker interface {
	MarkOrphansPlaced(ctx context.Context, orphans []model.OrphanDLR) error
}

// placeMany ends a re-match pass without Redis: the closes its orphans left
// to the end (closeMany), then the marks of the orphans placed — each one
// whose close went or is held — in one call when the store can. It answers
// how many were marked. A mark that fails leaves its orphans pending: the
// next pass finds their messages final and leaves them be, as when one mark
// failed before.
func (s *Service) placeMany(ctx context.Context, r OrphanReplayer, mc ManyCloser, coll *closeBatch, orphans []model.OrphanDLR, placed []int, errs []error) int {
	s.closeMany(ctx, mc, coll, errs)
	var done []model.OrphanDLR
	for _, i := range placed {
		if errs[i] == nil {
			done = append(done, orphans[i])
		}
	}
	if len(done) == 0 {
		return 0
	}
	if mm, ok := r.(ManyMarker); ok {
		if err := mm.MarkOrphansPlaced(ctx, done); err != nil {
			log.Printf("[dlr] orphan re-match: %d receipt(s) applied, their orphans not marked (%v)", len(done), err)
			return 0
		}
		return len(done)
	}
	n := 0
	for _, o := range done {
		if err := r.MarkOrphanMatched(ctx, o.Table, o.ID, ""); err != nil {
			log.Printf("[dlr] orphan re-match: the orphan of %s applied, not marked (%v)", o.VendorMsgID, err)
			break
		}
		n++
	}
	return n
}

// Final receipts closed many at a time on a gateway without Redis (DAL plan,
// step 3, 30 Oct 2026).
//
// Without the receipt facts in Redis every receipt is matched and closed in
// the record store, one at a time: the close (CloseWithReceipt: its row, the
// closing mark and the summaries in one transaction) and then the client's
// receipt (one client_dlr INSERT), two commits a receipt, each waiting for
// the summary rows every writer of the gateway updates. On the rig's mix
// that was about 135 ms a receipt; the SMPP vendor's receipt workers fell
// minutes behind and the clients got their receipts after they had stopped
// listening. ApplyMany now matches each receipt of a batch as before (the
// same reads, in order) and leaves the closes of the plain ones to the end:
// ONE CloseWithReceipts for all of them (one transaction a receipt table)
// and ONE PushBatch for their clients' receipts.
//
// A plain final is one that only closes its message and tells its client:
// no receipt policy answers for it, its client was told nothing before, and
// its record work is not the record queue's. Everything else is done where it
// was, receipt by receipt.

// ManyCloser is an optional Store capability: CloseWithReceipt for many
// final receipts in one call (mysql.Store.CloseWithReceipts), each answered
// as one call after another would be.
type ManyCloser interface {
	CloseWithReceipts(ctx context.Context, rs []model.ReceiptClose) ([]model.ReceiptClosed, error)
}

// errNoRecordYet is a close the store answered "no record": it is held and
// tried again (hold.go), as CloseWithReceipt's sql.ErrNoRows is.
var errNoRecordYet = errors.New("its message's record is not written yet")

// closeBatch is the closes ApplyMany leaves to the end: each job and the
// receipt (its place in the batch) it came from.
type closeBatch struct {
	at   int // the receipt being applied now
	jobs []*closeJob
	of   []int
	// msgs are the messages the jobs close.
	msgs map[string]bool
}

// closing says whether one of msgIDs is closed by a job of the batch.
func (b *closeBatch) closing(msgIDs []string) bool {
	if b == nil {
		return false
	}
	for _, id := range msgIDs {
		if b.msgs[id] {
			return true
		}
	}
	return false
}

// manyCloser is the store's ManyCloser when this service closes in the
// record store itself (not through the record queue) and ApplyMany may close
// a batch at once.
func (s *Service) manyCloser() (ManyCloser, bool) {
	if s.Fast() || s.writes != nil {
		return nil, false
	}
	mc, ok := s.store.(ManyCloser)
	return mc, ok
}

// takes says whether j is left to the batch: a final that closes its message
// and tells only its own client.
func (b *closeBatch) takes(s *Service, j *closeJob) bool {
	if b == nil || !j.ready || !j.st.IsTerminal() || j.told != "" || j.invented != nil || j.closed || j.rowDone {
		return false
	}
	_, ok := s.manyCloser()
	return ok
}

func (b *closeBatch) add(j *closeJob) {
	b.jobs = append(b.jobs, j)
	b.of = append(b.of, b.at)
	if b.msgs == nil {
		b.msgs = map[string]bool{}
	}
	b.msgs[j.ref.MsgID] = true
}

// closeMany closes what ApplyMany left to the end and tells the clients:
// what closeOne does for each, with one store call for the closes and one
// filing for the client receipts. A close the store could not make (an
// error, or no record yet) is held and done by closeOne later, as apply holds
// one; errs[of] is set only when even that could not be.
func (s *Service) closeMany(ctx context.Context, mc ManyCloser, b *closeBatch, errs []error) {
	if b == nil || len(b.jobs) == 0 {
		return
	}
	rs := make([]model.ReceiptClose, len(b.jobs))
	for k, j := range b.jobs {
		s.billJob(ctx, j)
		rs[k] = model.ReceiptClose{Ref: j.ref, Status: j.st, VendorMsgID: j.vendorMsgID, ErrorCode: j.errorCode, Entry: j.entry}
	}
	got, err := mc.CloseWithReceipts(ctx, rs)
	var pushes []Push
	var pushOf []int
	for k, j := range b.jobs {
		r := got[k]
		if !r.Found {
			// Not closed: an error for its receipt table, or no record yet.
			// What the receipt still owes its message waits, and is done
			// when the store takes it (as apply holds a failed closeOne).
			cause := err
			if cause == nil {
				cause = errNoRecordYet
			}
			j := j
			if herr := s.hold("the receipt of "+j.ref.MsgID+" ("+string(j.stat)+")", cause,
				func(ctx context.Context) error { return s.closeOne(ctx, j) }); herr != nil {
				errs[b.of[k]] = herr
			}
			continue
		}
		if !s.closedBy(ctx, j, r.Mine) {
			continue
		}
		if s.pusher != nil && model.WantsTerminalReceipt(j.noReceipt, j.failureOnly, j.st) {
			pushes = append(pushes, Push{AccountID: j.accountID, Receipt: s.clientReceipt(ctx, j)})
			pushOf = append(pushOf, b.of[k])
		}
	}
	if len(pushes) == 0 {
		return
	}
	// The messages are closed: a client receipt the queue refuses is held
	// and filed again (as apply holds a closeOne whose push failed), never
	// handed back — the vendor's copy sent again would find its message
	// final and be recorded only.
	keep := func(n int, cause error) {
		p := pushes[n]
		if herr := s.hold("the receipt of "+p.Receipt.MsgID, cause, func(ctx context.Context) error {
			return s.pusher.PushToClient(ctx, p.AccountID, p.Receipt)
		}); herr != nil && errs[pushOf[n]] == nil {
			errs[pushOf[n]] = herr
		}
	}
	if bp, ok := s.pusher.(BatchPusher); ok {
		if err := bp.PushBatch(ctx, pushes); err != nil {
			for n := range pushes {
				keep(n, err)
			}
		}
		return
	}
	for n, p := range pushes {
		if err := s.pusher.PushToClient(ctx, p.AccountID, p.Receipt); err != nil {
			keep(n, err)
		}
	}
}
