package dlr

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/mask"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
)

// Receipt intake: a vendor receipt applied to its message (Apply and its
// forms), and a vendor's status word mapped to ours.

// Apply correlates and applies one vendor receipt (the esme leg). rawStatus is
// the vendor's status token; statusMap maps it to a canonical status (delivered|
// failed|expired|sent); receiptRaw is the original receipt text (stored for
// audit). ok=false when the vendor_msg_id is unknown (foreign/stale receipt).
func (s *Service) Apply(ctx context.Context, vendorID int64, vendorMsgID, rawStatus, errorCode, receiptRaw string, statusMap map[string]string) (bool, error) {
	return s.apply(ctx, receiptKey{vendorID: vendorID, id: vendorMsgID}, rawStatus, errorCode, receiptRaw, statusMap, time.Time{}, correlateGrace(), true)
}

// ApplyWaited is Apply for a receipt that has already waited `waited` since
// it arrived — in a receipt worker's queue. The correlation grace is counted
// from ARRIVAL, not from the moment a worker picks the receipt up.
//
// Apply waits the whole grace (250 ms) on every miss. For a receipt taken at
// once that is right: its message's own record may be a batch flush away. For
// one that sat in a queue longer than the grace the record has had its time,
// and the sleep is pure loss — a quarter of a second of a worker for every
// receipt that matches nothing. Load test 2026-10-07: a vendor bind whose
// answers had timed out fed four workers receipts none of which could match,
// and they cleared 26 a second while 180,000 waited behind them and the
// bind's reader — which also reads the submit answers — waited on them.
func (s *Service) ApplyWaited(ctx context.Context, vendorID int64, vendorMsgID, rawStatus, errorCode, receiptRaw string, statusMap map[string]string, waited time.Duration) (bool, error) {
	grace := correlateGrace() - waited
	if grace < 0 {
		grace = 0
	}
	return s.apply(ctx, receiptKey{vendorID: vendorID, id: vendorMsgID}, rawStatus, errorCode, receiptRaw, statusMap, time.Time{}, grace, true)
}

// ApplyAt is Apply for a receipt that says when the message reached the
// phone (2026-09-28): doneAt is kept as the receipt's done_date and becomes
// the done date the client's receipt carries. Zero = the vendor did not say,
// exactly as Apply. A time no receipt could carry is dropped, see doneTime.
func (s *Service) ApplyAt(ctx context.Context, vendorID int64, vendorMsgID, rawStatus, errorCode, receiptRaw string, statusMap map[string]string, doneAt time.Time) (bool, error) {
	return s.apply(ctx, receiptKey{vendorID: vendorID, id: vendorMsgID}, rawStatus, errorCode, receiptRaw, statusMap, doneTime(vendorID, vendorMsgID, doneAt), correlateGrace(), true)
}

// doneTime keeps a vendor's done date only when a receipt could really carry
// it. A vendor clock that is wrong — a time well in the future, or older than
// any message still waiting for a receipt — would put a false delivery time
// on the record and on the client's receipt, so it is dropped and said so;
// the receipt itself is applied as normal.
func doneTime(vendorID int64, vendorMsgID string, t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	now := time.Now()
	if t.After(now.Add(10*time.Minute)) || t.Before(now.AddDate(0, 0, -30)) {
		logging.New("dlr").Warn(logging.SystemScope, "dlr_done_date_ignored",
			"vendor's delivery time is not believable (clock wrong or wrong field mapped); the receipt is applied without it",
			logging.Fields{"vendorId": vendorID, "vendorMsgId": vendorMsgID, "doneAt": t.UTC().Format(time.RFC3339)})
		return time.Time{}
	}
	return t.UTC()
}

// doneText is doneAt as the receipt row stores it.
func doneText(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// apply is Apply with two things the caller decides. A receipt arriving live
// gets the correlation grace and is FILED when it matches nothing; one
// replayed from the orphan table has already waited and is already filed —
// filing it again on every sweep would breed duplicate rows.
func (s *Service) apply(ctx context.Context, k receiptKey, rawStatus, errorCode, receiptRaw string, statusMap map[string]string, doneAt time.Time, grace time.Duration, fileOrphan bool) (bool, error) {
	return s.applyTo(ctx, k, rawStatus, errorCode, receiptRaw, statusMap, doneAt, grace, fileOrphan, nil)
}

// applyTo is apply. With coll set (ApplyMany without Redis, closemany.go) a
// final receipt that only closes its message and tells its client — no
// receipt policy, nothing told before, not the record queue's — is not closed
// here: its close is handed to coll, to be done with the batch's others.
func (s *Service) applyTo(ctx context.Context, k receiptKey, rawStatus, errorCode, receiptRaw string, statusMap map[string]string, doneAt time.Time, grace time.Duration, fileOrphan bool, coll *closeBatch) (bool, error) {
	if k.id == "" {
		k.id, k.alt = k.alt, ""
	}
	if k.id == "" {
		return false, nil
	}
	vendorID, vendorMsgID := k.vendorID, k.id
	// With Redis configured the receipt is finished there and the record
	// store is not asked (fast.go; Saleem, 29 Oct 2026: "no mysql calls").
	// Everything below is a gateway without the receipt facts in Redis.
	if s.Fast() {
		return s.applyRedis(ctx, ReceiptIn{VendorID: k.vendorID, VendorMsgID: k.id, AltID: k.alt, Number: k.number,
			RawStatus: rawStatus, ErrorCode: errorCode, Raw: receiptRaw, StatusMap: statusMap, DoneAt: doneAt,
			Arrived: time.Now()}, 1)
	}
	var err error
	// Usually one record; a reassembled concat group has every part stamped with
	// the same vendor id, so this receipt finalizes the whole group. [D-036]
	//
	// Correlation is representation-tolerant: a vendor that returns the id in a
	// different form in the receipt than in submit_sm_resp (hex vs decimal, zero
	// padding) would otherwise never correlate. We try the incoming id's plausible
	// equivalent forms, exact first, and use the one that resolves. The vendor's
	// exact wire id is preserved in the stored raw receipt (receiptRaw) regardless.
	//
	// A miss is looked up once more after a short wait (2026-09-16): the
	// vendor roles write their `sent` records in batches, so for up to the
	// batch's flush interval a message has no correlatable row, and a vendor
	// that answers fast enough — a mock SMSC above all — sends its receipt
	// inside that window. What is still unmatched after the wait is filed as
	// an orphan, and the re-match sweep gives it its last chances.
	var (
		msgIDs                 []string
		accountID              int64
		noReceipt, failureOnly bool
		bindSlot               int
		privacy                model.Privacy
		mcc, mnc               string
		sender, company        int64
		matched                string
	)
	msgIDs, accountID, noReceipt, failureOnly, bindSlot, privacy, mcc, mnc, sender, company, matched, err = s.resolve(ctx, k)
	if (err == nil || errors.Is(err, errRecordPending)) && len(msgIDs) == 0 && grace > 0 {
		select {
		case <-ctx.Done():
		case <-time.After(grace):
			msgIDs, accountID, noReceipt, failureOnly, bindSlot, privacy, mcc, mnc, sender, company, matched, err = s.resolve(ctx, k)
		}
	}
	// A message an earlier receipt of this batch is closing (its close left to
	// the batch's end, closemany.go) is matched as it would be one receipt
	// after another: it is final, so this receipt finds no open message.
	if err == nil && coll.closing(msgIDs) {
		msgIDs = nil
	}
	// A part of a long message whose record is not written yet: kept, and
	// applied when it is (the re-match sweep), under that reason — not "no
	// message with this vendor_msg_id".
	if errors.Is(err, errRecordPending) {
		return s.keepForLater(ctx, vendorID, vendorMsgID, ParseStat(rawStatus, s.mapFor(statusMap, 0, vendorID)),
			rawStatus, errorCode, receiptRaw, privacy, fileOrphan, nil)
	}
	// Each message's record, by its id and its company (D-136).
	refOf := func(msgID string) model.RecordRef { return model.RecordRef{MsgID: msgID, CompanyID: company} }
	// A vendor may echo the start of the customer's message back in the
	// receipt's `text:` field. That text is stored and forwarded, so for an
	// account on pii_masked it is masked here — and ONLY the part after
	// `text:`: masking the whole receipt would destroy the vendor's message
	// id, the counters and the timestamps that correlation depends on.
	if privacy == model.PrivacyMasked {
		receiptRaw = mask.ReceiptText(receiptRaw, maskMinDigits())
	}
	if err != nil {
		// The record store could not be asked (MySQL away): nothing of the
		// receipt is done, and it is kept to be applied when it can be (go-live
		// test 18 Oct 2026: answered and lost — the record store's match is
		// what a receipt with no facts in Redis meets first).
		return s.keepForLater(ctx, vendorID, vendorMsgID, ParseStat(rawStatus, s.mapFor(statusMap, 0, vendorID)),
			rawStatus, errorCode, receiptRaw, privacy, fileOrphan, err)
	}
	// The vendor that SENT the message (a TX row, when the receipt came in on
	// its RX row): it is the one whose receipts, verdicts, quality and code
	// rules these are. The receipt's own vendor files what matches nothing.
	if sender == 0 {
		sender = vendorID
	}
	// Monitoring: the vendor is talking, whether or not this receipt matches
	// anything (PO 2026-09-11).
	monitor.Verdicts.Receipt(sender, len(msgIDs) > 0)
	if len(msgIDs) == 0 {
		// Unknown vendor_msg_id: foreign, stale, or a receipt that beat its own
		// record row into the database. Kept rather than dropped — before this
		// it vanished, and on two of the three paths without even a log line.
		if rec, ok := s.orphans(); ok && fileOrphan {
			if err := rec.WriteOrphanDLR(ctx, vendorID, vendorMsgID,
				rawStatus, errorCode, receiptRaw, "no message with this vendor_msg_id"); err != nil {
				// Not an error the vendor sees: the orphan's filing waits and
				// is done when the store takes it — the re-match sweep may
				// still find its message (go-live test 18 Oct 2026).
				if herr := s.hold("the receipt "+vendorMsgID, err, func(ctx context.Context) error {
					return rec.WriteOrphanDLR(ctx, vendorID, vendorMsgID, rawStatus, errorCode, receiptRaw, "no message with this vendor_msg_id")
				}); herr != nil {
					log.Printf("[dlr] orphan receipt vendor=%d id=%s not recorded: %v", vendorID, vendorMsgID, herr)
				}
			}
		}
		return false, nil
	}
	// The resolved form is exactly what the record and its concat_parts were
	// stamped with at submit time, so keying the part-tracker / status-stamp on it
	// matches the stored rows.
	vendorMsgID = matched
	// The receipt's own word, one of SMPP's eight (2026-09-23). The vendor's
	// map is consulted first, so a vendor answering "2" or "DLVD" is read
	// correctly; "" means the word means nothing to us.
	stat := ParseStat(rawStatus, s.mapFor(statusMap, sender, vendorID))
	st := stat.Record()
	// [SCOPE P3-3.9.4] A per-vendor rule for this receipt's ERROR code, if the
	// vendor opted in. Applied after the generic mapping, because a rule
	// refines an outcome rather than replacing the whole vocabulary.
	//
	// Why the error code matters: one `stat:` covers very different things.
	// UNDELIV with err 000 is a number that does not exist and will never
	// work; UNDELIV with err 0AB is a handset out of coverage that probably
	// would in ten minutes. Without this they are the same word to the
	// customer.
	//
	// TargetStatus is what makes a retry rule coherent: without it a rule
	// saying "retry" would leave the record reading FAILED while the message
	// is still being retried — telling the customer it failed and then
	// delivering it.
	overrideAction := ""
	if s.overrides != nil && errorCode != "" {
		if r, ok := s.overrides.DLR(sender, errorCode); ok {
			overrideAction = r.Action
			if r.TargetStatus != "" {
				st = model.Status(r.TargetStatus)
			}
		}
	}
	// The ACTION is recorded, not acted on, and that is deliberate.
	//
	// target_status above makes the record correct, which is the half that
	// needed fixing. Making "retry" actually RESEND from a receipt is a
	// different and much larger thing: the message was acked off the vendor
	// queue when it was sent, so there is no requeue path, and building one
	// raises double-send, billing and attempt-counting questions that have not
	// been ruled on. Recording the vendor's classification lets an operator
	// see WHY a code was treated the way it was, without this quietly
	// inventing a resend nobody asked for.

	// An unrecognised status token: leave the message as it is and say so.
	// MapStatus returns "" for anything outside the known vocabulary rather
	// than inventing one from the raw text (see its default branch). The
	// receipt is still stored and still captured — only the status is left
	// alone, so nothing is lost and nothing is fabricated.
	if st == "" {
		logging.New("dlr").Warn(logging.SystemScope, "dlr_unknown_status",
			"vendor sent a delivery status we do not recognise; message left unchanged, the receipt kept",
			logging.Fields{
				"vendorId": vendorID, "vendorMsgId": vendorMsgID,
				"rawStatus": rawStatus, "errorCode": errorCode, "messages": msgIDs,
			})
		// Kept, not dropped (2026-09-23). Until now this returned here and
		// the receipt vanished with it: no row, no evidence, nothing for
		// support to look at when a customer asks what the vendor actually
		// said. It is recorded with its raw text and no status; the message
		// is still left exactly as it was, which was always the right half of
		// this decision.
		s.recordReceipt(ctx, msgIDs, company, entry{Direction: "esme", VendorMsgID: vendorMsgID,
			Status: "", ErrorCode: errorCode, Raw: s.storedReceipt(ctx, accountID, receiptRaw, privacy),
			At: time.Now().UTC().Format(time.RFC3339), DoneAt: doneText(doneAt)})
		return true, nil
	}
	// [SCOPE P13-CAPTURE] The receipt as it arrived, before anything is done
	// with it — the step an operator is usually looking for when they ask why
	// a message shows the status it does. Costs one atomic load when no
	// capture is running.
	if c, on := logging.ActiveForAccountID(accountID); on {
		logging.CaptureTo(c, logging.New("dlr"), "receipt", "receipt from vendor", logging.Fields{
			"vendorId": vendorID, "vendorMsgId": vendorMsgID, "matchedAs": matched,
			"rawStatus": rawStatus, "mappedStatus": string(st), "errorCode": errorCode,
			"vendorRule": overrideAction,
			"raw":        receiptRaw,
			"messages":   msgIDs,
		})
	}
	// An INTERMEDIATE receipt — ENROUTE or ACCEPTD (PO 2026-09-23: "accepted
	// itself is a dlr"). The network is still carrying the message, so
	// nothing about the message moves: no status change, no verdict, no part
	// aggregation, no concat fold, and it keeps its place in retry and in the
	// no-receipt sweep. The receipt is kept, and forwarded when the account
	// and the client both want it.
	if !stat.Final() {
		s.recordReceipt(ctx, msgIDs, company, entry{Direction: "esme", VendorMsgID: vendorMsgID,
			Status: string(st), Stat: string(stat), ErrorCode: errorCode,
			Raw:    s.storedReceipt(ctx, accountID, receiptRaw, privacy),
			At:     time.Now().UTC().Format(time.RFC3339),
			DoneAt: doneText(doneAt)})
		levels := s.levelsFor(accountID)
		// noReceipt is registered_delivery bits 0-1 = 00: this client asked
		// to hear nothing at all, which covers progress notices too.
		if s.pusher == nil || noReceipt || levels == LevelsFinalsOnly {
			return true, nil
		}
		for _, msgID := range msgIDs {
			// A client already given its FINAL answer by the account's
			// receipt policy is not sent progress notices afterwards.
			if s.toldAlready(ctx, refOf(msgID)) != "" {
				continue
			}
			// `registered` asks the client: bit 4 for an SMPP submit, always
			// for an HTTP one. `all` is the operator overriding that.
			if levels != LevelsAll && !s.clientWantsIntermediate(ctx, refOf(msgID)) {
				continue
			}
			src, dst := s.addresses(ctx, refOf(msgID))
			if err := s.pusher.PushToClient(ctx, accountID, Receipt{
				MsgID: msgID, Status: st, Stat: stat, ErrCode: errorCode,
				Src: src, Dst: dst, BindSlot: bindSlot, DoneAt: doneAt,
				Owner: s.ownerOf(ctx, refOf(msgID)), // the deployment its client is on (D-121)
			}); err != nil {
				return true, err
			}
		}
		return true, nil
	}
	// Multi-part send: this receipt covers ONE part. Record that part's status and
	// take the aggregate; while parts are still outstanding the message keeps its
	// current status rather than being finalized on a single part. [D-037]
	ready := true
	partOwn, isPartRow := st, false // this part's own status, before the group's
	if pt, okPT := s.store.(PartTracker); okPT {
		mark := pt.MarkPartAndAggregate
		// A vendor that sends one final per long message: the first closes
		// it (dlr_config.first_receipt_closes, Saleem 4 Nov 2026).
		if ft, okFT := s.store.(FirstPartTracker); okFT && s.firstReceiptCloses(sender) {
			mark = ft.MarkFirstPartFinal
		}
		agg, rdy, isPart, err := mark(ctx, sender, vendorMsgID, st)
		if err != nil {
			// One transaction, rolled back: nothing of this receipt is done.
			return s.keepForLater(ctx, k.vendorID, vendorMsgID, stat, rawStatus, errorCode, receiptRaw, privacy, fileOrphan, err)
		}
		if isPart {
			st, ready, isPartRow = agg, rdy, true
		}
	}
	// For a message stored encrypted, the receipt's text: part (the start of
	// the message, repeated by the vendor) is encrypted too (PO 2026-09-13).
	// Only that part: the vendor's id, status, error and dates stay readable
	// for support. The capture above keeps the plain receipt — captures are
	// for debugging and stay plain (same ruling).
	stored := s.storedReceipt(ctx, accountID, receiptRaw, privacy)
	// The row keeps BOTH: the vendor's own word (one of the eight) and the
	// message status it implies. A per-vendor override that renamed the
	// outcome changes the status only — the vendor still said what it said.
	e := entry{Direction: "esme", VendorMsgID: vendorMsgID, Status: string(stat.Record()),
		Stat: string(stat), ErrorCode: errorCode, Raw: stored, At: time.Now().UTC().Format(time.RFC3339),
		DoneAt: doneText(doneAt)}
	b, _ := json.Marshal(e)
	// The account's own receipt policy, if it has one (config/0119), with the
	// gateway-wide default under it (issue 23). nil for every account on a
	// gateway where nobody set either, and the whole block below then behaves
	// exactly as it did before policies existed.
	pol := s.policyFor(accountID)
	for _, msgID := range msgIDs {
		// What this client has already been told about the message, decided
		// BEFORE we touch anything: the vendor role may have answered for the
		// vendor at submit time, or the no-receipt sweep may have given up
		// waiting. Either way the client has had its one final answer, and
		// this receipt is recorded without being pushed (PO 2026-09-22).
		told := s.toldAlready(ctx, refOf(msgID))
		// A rule that replaces this receipt's status for the client. Claimed
		// here, before the record is touched, so a billing sweep can never
		// read the real status without the client's version beside it.
		var invented *receiptpolicy.Outcome
		if told == "" && ready && pol != nil && st != "" {
			if a, rule := pol.ForReceiptRule(st, rawStatus, errorCode); a != nil {
				o := receiptpolicy.Decide(a, rule, nil)
				if o.Err == "" {
					// The rule said nothing about the code, so the vendor's
					// own goes on: only the status was being replaced, and a
					// client debugging a failure still needs the reason.
					o.Err = errorCode
				}
				if teller, okT := s.teller(); okT && o.Final() {
					mine, err := teller.SetTold(ctx, refOf(msgID), o.Status, o.Err, o.Rule)
					if err != nil {
						log.Printf("[dlr] receipt policy: could not record what %s is told (%v) — the vendor's own receipt goes out instead", msgID, err)
					} else if mine {
						invented = &o
					} else {
						told = "already told by another instance"
					}
				}
			}
		}
		j := &closeJob{ref: refOf(msgID), accountID: accountID, company: company, sender: sender, vendorID: vendorID,
			vendorMsgID: vendorMsgID, errorCode: errorCode, st: st, stat: stat, entry: string(b),
			told: told, invented: invented, noReceipt: noReceipt, failureOnly: failureOnly, bindSlot: bindSlot,
			mcc: mcc, mnc: mnc, doneAt: doneAt, ready: ready}
		// A part that does not close its message, for an account that asked
		// for each part's receipt (part_receipts = 'each', 4 Nov 2026).
		if isPartRow && !ready && s.partToClient(accountID, told, noReceipt, failureOnly, partOwn, rawStatus, errorCode) {
			j.partPush, j.partSt = true, partOwn
		}
		if coll.takes(s, j) {
			coll.add(j)
			continue
		}
		if err := s.closeOne(ctx, j); err != nil {
			// The record store went away part way. What this receipt still
			// owes the message waits, with what is done already, and is
			// finished when the store is back.
			if herr := s.hold("the receipt of "+msgID+" ("+string(stat)+")", err,
				func(ctx context.Context) error { return s.closeOne(ctx, j) }); herr != nil {
				return true, herr
			}
		}
	}
	return true, nil
}

// MapStatus maps a raw vendor status via statusMap, else canonicalises common
// tokens, else lower-cases it.
func MapStatus(raw string, m map[string]string) model.Status {
	if v, ok := m[raw]; ok {
		return model.Status(v)
	}
	switch strings.ToUpper(raw) {
	case "DELIVRD", "DELIVERED":
		return model.StatusDelivered
	case "EXPIRED":
		return model.StatusExpired
	case "UNDELIV", "REJECTD", "DELETED", "FAILED", "UNKNOWN":
		return model.StatusFailed
	default:
		// A token we do not recognise used to become the status VERBATIM, so
		// `stat:ACCEPTD` wrote the status "acceptd" — not one of
		// queued/sent/delivered/failed/expired/rejected, and the column is a
		// varchar so the database accepted it without complaint. Nothing in
		// today's data hit it, but nothing prevented it either.
		//
		// Ruled 2026-09-09: keep whatever the message already is and say so,
		// rather than invent a status. The empty return is that signal —
		// Apply leaves the record alone and logs the token, so someone can add
		// the mapping instead of finding a nonsense status later.
		return ""
	}
}
