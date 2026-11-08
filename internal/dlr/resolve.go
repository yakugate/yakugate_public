package dlr

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// Matching a receipt to its message(s) on the record store, and keeping a
// receipt whose record is not written yet to be applied again.

// keepForLater keeps a receipt the record store cannot take yet — its
// message's record is not written (cause nil), or the store could not be
// reached (cause) — before anything of it was done. It is filed as an
// orphan, with the reason, and the re-match sweep applies it from the start
// once its record is there (rematch.go). An orphan that cannot be written
// either (MySQL away) waits in memory and is filed when the store is back
// (hold.go). Go-live test 18 Oct 2026: such a receipt was answered and lost.
//
// The orphan carries the word the gateway read (DELIVRD, not a vendor's own
// "2"), so the re-match reads it the same way without the vendor's map; and a
// message stored encrypted has its receipt's text part left out (the orphans
// table is not sealed, and the text is sealed only on the receipt's own row).
//
// A receipt being re-matched (fileOrphan=false) IS the orphan: it stays where
// it is, and the sweep tries it again later.
func (s *Service) keepForLater(ctx context.Context, vendorID int64, vendorMsgID string, stat Stat, rawStatus, errorCode, receiptRaw string,
	privacy model.Privacy, fileOrphan bool, cause error) (bool, error) {
	if !fileOrphan {
		return false, cause
	}
	word := rawStatus
	if stat != "" {
		word = string(stat)
	}
	raw := receiptRaw
	if privacy == model.PrivacyEncrypted {
		raw = withoutText(raw)
	}
	// At most 64 characters: the orphans' reason column (and summary_orphans').
	reason := "record not written yet: kept, applied when it is"
	if cause != nil {
		reason = "record store away: kept, applied when it is back"
	}
	rec, ok := s.orphans()
	if !ok {
		if cause == nil {
			cause = errors.New("its message's record is not written yet")
		}
		return true, cause
	}
	keptCount.Add(1)
	what := "the receipt " + vendorMsgID + " (" + word + ")"
	if err := rec.WriteOrphanDLR(ctx, vendorID, vendorMsgID, word, errorCode, raw, reason); err != nil {
		if herr := s.hold(what, err, func(ctx context.Context) error {
			return rec.WriteOrphanDLR(ctx, vendorID, vendorMsgID, word, errorCode, raw, reason)
		}); herr != nil {
			return false, herr
		}
	}
	noteKept("a receipt was kept to be applied again (%s): %s", reason, what)
	return true, nil
}

// errRecordPending is resolve's answer for a receipt of a long message's part
// whose message is known but not recorded yet (model.ReceiptMatch
// .RecordPending): kept for later, never an orphan "with no message".
var errRecordPending = errors.New("its message's record is not written yet")

// keptCount counts receipts kept to be applied again (keepForLater).
var keptCount atomic.Int64

// KeptForLater is how many receipts this process kept to be applied again
// because their record was not written yet or the record store was away.
func KeptForLater() int64 { return keptCount.Load() }

// keptSaid rate-limits noteKept.
var keptSaid atomic.Int64

func noteKept(format string, a ...any) {
	now := time.Now().UnixNano()
	last := keptSaid.Load()
	if now-last < int64(10*time.Second) || !keptSaid.CompareAndSwap(last, now) {
		return
	}
	log.Printf("[dlr] "+format, a...)
}

// resolve finds the message(s) a receipt correlates to, tolerating a difference
// in id representation between submit_sm_resp and the DLR. It tries each
// candidate form (exact first) and returns the ids for the first that resolves,
// plus the matched form so callers can key downstream lookups on the stored
// value, and the messages' company (their records are in mt_<company>_<month>,
// D-136). Returns empty msgIDs (no error) when nothing correlates.
//
// Only on a gateway without the receipt facts in Redis: with them a receipt
// is matched there and nowhere else (fast.go).
func (s *Service) resolve(ctx context.Context, k receiptKey) (msgIDs []string, accountID int64, noReceipt, failureOnly bool, bindSlot int, privacy model.Privacy, mcc, mnc string, sender, company int64, matched string, err error) {
	// The record store that knows the vendors' companies looks among all of
	// them, by the number when the receipt has one, and by our own id (rule
	// 3, groups.go). It also says which vendor sent the message.
	if mr, ok := s.store.(MatchResolver); ok {
		members := s.members(k.vendorID)
		pending := false
		for _, cand := range k.candidates() {
			m, e := mr.ResolveReceipt(ctx, members, cand, k.number)
			if e != nil {
				return nil, 0, false, false, 0, 0, "", "", 0, 0, "", e
			}
			if len(m.MsgIDs) > 0 {
				return m.MsgIDs, m.AccountID, m.NoReceipt, m.FailureOnly, m.BindSlot, m.Privacy, m.MCC, m.MNC, m.VendorID, m.CompanyID, cand, nil
			}
			pending = pending || m.RecordPending
		}
		if pending {
			return nil, 0, false, false, 0, 0, "", "", 0, 0, k.id, errRecordPending
		}
		return nil, 0, false, false, 0, 0, "", "", 0, 0, k.id, nil
	}
	gr, okGR := s.store.(GroupResolver)
	for _, cand := range k.candidates() {
		if okGR {
			ids, acct, noRcpt, failOnly, slot, mb, mc, mn, e := gr.ResolveVendorMsgIDAll(ctx, k.vendorID, cand)
			if e != nil {
				return nil, 0, false, false, 0, 0, "", "", 0, 0, "", e
			}
			if len(ids) > 0 {
				return ids, acct, noRcpt, failOnly, slot, mb, mc, mn, k.vendorID, 0, cand, nil
			}
			continue
		}
		id, acct, noRcpt, failOnly, slot, mb, mc, mn, ok, e := s.store.ResolveVendorMsgID(ctx, k.vendorID, cand)
		if e != nil {
			return nil, 0, false, false, 0, 0, "", "", 0, 0, "", e
		}
		if ok {
			return []string{id}, acct, noRcpt, failOnly, slot, mb, mc, mn, k.vendorID, 0, cand, nil
		}
	}
	return nil, 0, false, false, 0, 0, "", "", 0, 0, k.id, nil
}
