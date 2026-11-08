package mysql

// Finding a message from its receipt, in MySQL (D-136).
//
// A vendor's receipt carries the vendor's id for the message and nothing
// about which of our customers it belongs to. On an install with no Redis the
// messages still waiting for their receipt are in open_msgs, across every
// company: vendor + vendor id (+ the handset) → our id and its company. With
// Redis the receipt facts do this and open_msgs is empty (never both). The
// parts of a long message sent as several PDUs are in concat_parts either
// way (the part tracker). Once matched, everything goes by id and company.

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/msisdn"
)

// ResolveVendorMsgID finds our message for a vendor-assigned id, scoped to
// the vendor. ok=false if unknown. [SCOPE P3/P5-DLR]
func (s *Store) ResolveVendorMsgID(ctx context.Context, vendorID int64, vendorMsgID string) (msgID string, accountID int64, noReceipt, failureOnly bool, bindSlot int, privacy model.Privacy, mcc, mnc string, ok bool, err error) {
	m, err := s.resolveReceipt(ctx, []int64{vendorID}, vendorMsgID, "")
	if err != nil || len(m.MsgIDs) == 0 {
		return "", 0, false, false, 0, 0, "", "", false, err
	}
	return m.MsgIDs[0], m.AccountID, m.NoReceipt, m.FailureOnly, m.BindSlot, m.Privacy, m.MCC, m.MNC, true, nil
}

// ResolveVendorMsgIDAll returns every message sharing one vendor id: the
// parts of a long message an HTTP vendor took as one call (D-036).
func (s *Store) ResolveVendorMsgIDAll(ctx context.Context, vendorID int64, vendorMsgID string) (msgIDs []string, accountID int64, noReceipt, failureOnly bool, bindSlot int, privacy model.Privacy, mcc, mnc string, err error) {
	m, err := s.resolveReceipt(ctx, []int64{vendorID}, vendorMsgID, "")
	return m.MsgIDs, m.AccountID, m.NoReceipt, m.FailureOnly, m.BindSlot, m.Privacy, m.MCC, m.MNC, err
}

// ResolveReceipt is ResolveVendorMsgIDAll for every vendor of one company,
// with the receipt's handset number, and by our own message id when the id is
// one of ours and nothing else matched (dlr.MatchResolver). The match order is
// Saleem's (2026-10-14): company + vendor id + number; with no number, company
// + vendor id; with no vendor id we know, our message id.
func (s *Store) ResolveReceipt(ctx context.Context, vendorIDs []int64, vendorMsgID, number string) (model.ReceiptMatch, error) {
	if len(vendorIDs) == 0 || vendorMsgID == "" {
		return model.ReceiptMatch{}, nil
	}
	m, err := s.resolveReceipt(ctx, vendorIDs, vendorMsgID, number)
	if err != nil || len(m.MsgIDs) > 0 || !isULID(vendorMsgID) {
		return m, err
	}
	return s.resolveByOurID(ctx, vendorIDs, vendorMsgID)
}

// maxOpenPerVendorID bounds how many open messages of one vendor id are
// looked at: the parts of one long message (255 at most by the UDH).
const maxOpenPerVendorID = 256

// openRecord is one message still waiting for its delivery truth.
type openRecord struct {
	msgID     string
	accountID int64
	companyID int64
	vendorID  int64 // the vendor that sent it
}

func (o openRecord) ref() model.RecordRef {
	return model.RecordRef{MsgID: o.msgID, CompanyID: o.companyID}
}
func (o openRecord) table() string { return mtOf(o.ref()) }

// resolveReceipt finds the message of a vendor id among the vendors given.
//
// Two collision guards (2026-08-19, p5h TC20): (a) only messages still
// AWAITING delivery truth resolve — vendor ids are unique per vendor SESSION
// and get recycled — and open_msgs holds only those; (b) the whole-message
// arm yields exactly ONE message (the newest), unless the others are parts of
// the same long message an HTTP vendor took as one call (concatSiblings). (c)
// the two arms naming DIFFERENT messages is the id handed out again: the
// receipt is about the newer one, and the dead part row is removed.
func (s *Store) resolveReceipt(ctx context.Context, vendorIDs []int64, vendorMsgID, number string) (model.ReceiptMatch, error) {
	in := "?" + strings.Repeat(",?", len(vendorIDs)-1)
	vargs := make([]any, 0, len(vendorIDs))
	for _, v := range vendorIDs {
		vargs = append(vargs, v)
	}
	var open, parts []openRecord
	if OpenMessagesInMySQL() {
		args := append(append([]any{}, vargs...), vendorMsgID, maxOpenPerVendorID)
		rows, err := s.db.QueryContext(ctx,
			`SELECT msg_id, account_id, company_id, vendor_id FROM open_msgs
			  WHERE vendor_id IN (`+in+`) AND vendor_msg_id=? ORDER BY msg_id DESC LIMIT ?`, args...)
		if err != nil {
			return model.ReceiptMatch{}, err
		}
		for rows.Next() {
			var o openRecord
			if err := rows.Scan(&o.msgID, &o.accountID, &o.companyID, &o.vendorID); err != nil {
				rows.Close()
				return model.ReceiptMatch{}, err
			}
			open = append(open, o)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return model.ReceiptMatch{}, err
		}
	}
	{
		args := append(append([]any{}, vargs...), vendorMsgID)
		rows, err := s.db.QueryContext(ctx,
			`SELECT msg_id, COALESCE(company_id,0), vendor_id FROM concat_parts WHERE vendor_id IN (`+in+`) AND vendor_msg_id=?`, args...)
		if err != nil {
			return model.ReceiptMatch{}, err
		}
		for rows.Next() {
			var o openRecord
			if err := rows.Scan(&o.msgID, &o.companyID, &o.vendorID); err != nil {
				rows.Close()
				return model.ReceiptMatch{}, err
			}
			parts = append(parts, o)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return model.ReceiptMatch{}, err
		}
		sort.SliceStable(parts, func(i, j int) bool { return parts[i].msgID > parts[j].msgID })
	}
	// The receipt's number chooses among the messages the id can be about.
	if number != "" {
		var err error
		if open, err = s.byNumber(ctx, open, number); err != nil {
			return model.ReceiptMatch{}, err
		}
		if parts, err = s.byNumber(ctx, parts, number); err != nil {
			return model.ReceiptMatch{}, err
		}
	}
	var whole, part openRecord
	var group []string
	if len(open) > 0 {
		whole = open[0]
		group = []string{whole.msgID}
		if len(open) > 1 {
			var err error
			if group, err = s.concatSiblings(ctx, open); err != nil {
				return model.ReceiptMatch{}, err
			}
		}
	}
	if len(parts) > 0 {
		part = parts[0]
	}
	var ids []string
	var lead openRecord
	switch {
	case whole.msgID != "" && part.msgID != "" && whole.msgID != part.msgID:
		if whole.msgID > part.msgID { // ULIDs sort by the time they were made
			ids, lead = group, whole
			// Not best effort: left behind, the row would take this very
			// receipt in MarkPartAndAggregate.
			if _, err := s.db.ExecContext(ctx,
				"DELETE FROM concat_parts WHERE vendor_id=? AND vendor_msg_id=? AND msg_id=?",
				part.vendorID, vendorMsgID, part.msgID); err != nil {
				return model.ReceiptMatch{}, fmt.Errorf("old part row of %s for vendor id %s: %w", part.msgID, vendorMsgID, err)
			}
		} else {
			ids, lead = []string{part.msgID}, part
		}
	case whole.msgID != "":
		ids, lead = group, whole
	case part.msgID != "":
		ids, lead = []string{part.msgID}, part
	default:
		return model.ReceiptMatch{}, nil
	}
	m, err := s.matchOf(ctx, ids, lead)
	if err == nil && len(m.MsgIDs) == 0 && lead.msgID == part.msgID {
		// A part row is written when the part is answered, the message's
		// record when the DB worker gets to it: the message is known, its
		// record is on its way (issue 25 follow-up, harness p3h TC-P3-28).
		return model.ReceiptMatch{RecordPending: true}, nil
	}
	if err == nil && len(m.MsgIDs) > 0 && lead.msgID == part.msgID {
		// mt_ is there from acceptance (the three-table design): what a
		// part's receipt waits for now is the vendor's accepted answers in
		// sub_, written when every part has answered. Applied before them,
		// its outcome would be counted with no vendor (the load run of 28 Oct:
		// a long message's outcomes on the account's accepted row).
		sub := recTable(billingstore.KindSub, lead.companyID, lead.msgID)
		taken, terr := acceptedIn(ctx, s.db, sub, []string{lead.msgID})
		if terr != nil && !isNoSuchTable(terr) {
			return model.ReceiptMatch{}, terr
		}
		if len(taken[lead.msgID]) == 0 {
			return model.ReceiptMatch{RecordPending: true}, nil
		}
	}
	return m, err
}

// matchOf is what a receipt's path needs of the messages it matched, read
// from the lead message's record. A part row is written when the part is
// SENT and the message's record a batch later: a receipt in between names a
// message with no record yet, which is "not found yet" — the caller waits and
// looks again (dlr.Service.apply's grace, then the orphan sweep).
func (s *Store) matchOf(ctx context.Context, ids []string, lead openRecord) (model.ReceiptMatch, error) {
	m := model.ReceiptMatch{MsgIDs: ids, VendorID: lead.vendorID, CompanyID: lead.companyID}
	var noRcpt, failOnly, mb int
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(account_id,0), COALESCE(no_receipt,0), COALESCE(failure_receipt_only,0), COALESCE(bind_slot,0),
		        COALESCE(mask_body,0), COALESCE(mcc,''), COALESCE(mnc,'') FROM `+lead.table()+` WHERE msg_id=?`, ids[0]).
		Scan(&m.AccountID, &noRcpt, &failOnly, &m.BindSlot, &mb, &m.MCC, &m.MNC)
	if err == sql.ErrNoRows || isNoSuchTable(err) || (err == nil && m.AccountID == 0) {
		return model.ReceiptMatch{}, nil
	}
	if err != nil {
		return model.ReceiptMatch{}, err
	}
	m.NoReceipt, m.FailureOnly, m.Privacy = noRcpt != 0, failOnly != 0, model.Privacy(mb)
	return m, nil
}

// byNumber keeps, of the messages an id can be about, those sent to number
// (msisdn.Same). None of several is none; one that is not the number is
// still that one — the only message the receipt can be about.
func (s *Store) byNumber(ctx context.Context, recs []openRecord, number string) ([]openRecord, error) {
	if len(recs) == 0 {
		return recs, nil
	}
	dest, err := s.destinations(ctx, recs)
	if err != nil {
		return nil, err
	}
	var keep []openRecord
	for _, o := range recs {
		if msisdn.Same(number, dest[o.msgID]) { // a number not known is not a different one
			keep = append(keep, o)
		}
	}
	if len(keep) == 0 && len(recs) == 1 {
		return recs, nil
	}
	return keep, nil
}

// destinations reads the number each message was sent to, from its record;
// a message whose record cannot be read is left out.
func (s *Store) destinations(ctx context.Context, recs []openRecord) (map[string]string, error) {
	byTable := map[string][]any{}
	var tables []string
	for _, o := range recs {
		t := o.table()
		if _, seen := byTable[t]; !seen {
			tables = append(tables, t)
		}
		byTable[t] = append(byTable[t], o.msgID)
	}
	out := map[string]string{}
	for _, table := range tables {
		ids := byTable[table]
		rows, err := s.db.QueryContext(ctx,
			"SELECT msg_id, COALESCE(destination,'') FROM "+table+" WHERE msg_id IN ("+placeholders(len(ids))+")", ids...)
		if isNoSuchTable(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, d string
			if err := rows.Scan(&id, &d); err != nil {
				rows.Close()
				return nil, err
			}
			out[id] = d
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// resolveByOurID is the third rule: a receipt that names OUR message id (a
// vendor that gave us none, or echoes ours back) is that message, if it is
// still open and was sent through one of the vendors given.
func (s *Store) resolveByOurID(ctx context.Context, vendorIDs []int64, msgID string) (model.ReceiptMatch, error) {
	if !OpenMessagesInMySQL() {
		return model.ReceiptMatch{}, nil
	}
	in := "?" + strings.Repeat(",?", len(vendorIDs)-1)
	args := []any{msgID}
	for _, v := range vendorIDs {
		args = append(args, v)
	}
	var o openRecord
	err := s.db.QueryRowContext(ctx,
		"SELECT msg_id, account_id, company_id, vendor_id FROM open_msgs WHERE msg_id=? AND vendor_id IN ("+in+")", args...).
		Scan(&o.msgID, &o.accountID, &o.companyID, &o.vendorID)
	if err == sql.ErrNoRows {
		return model.ReceiptMatch{}, nil
	}
	if err != nil {
		return model.ReceiptMatch{}, err
	}
	return s.matchOf(ctx, []string{msgID}, o)
}

// isULID reports whether id has the shape of one of our message ids.
func isULID(id string) bool {
	if len(id) != 26 || id[0] > '7' {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= '0' && c <= '9':
		case c >= 'A' && c <= 'Z' && c != 'I' && c != 'L' && c != 'O' && c != 'U':
		default:
			return false
		}
	}
	return true
}

// concatSiblings returns, of the open messages that share one vendor id
// (newest first), the newest and every other PART OF THE SAME LONG MESSAGE:
// an HTTP vendor takes a long message as one call, so each part's record
// carries the one id the vendor gave (D-036). What makes records parts of one
// message is on the records: the same account, sender and destination, and
// the same concat reference and count (udh_ref, udh_total > 1). Anything else
// under the same id is the id handed out twice — only the newest is meant.
func (s *Store) concatSiblings(ctx context.Context, open []openRecord) ([]string, error) {
	newest := open[0]
	ids := []string{newest.msgID}
	var sender, dest string
	var ref, total int
	err := s.db.QueryRowContext(ctx,
		"SELECT COALESCE(sender,''), COALESCE(destination,''), COALESCE(udh_ref,0), COALESCE(udh_total,0) FROM "+newest.table()+" WHERE msg_id=?",
		newest.msgID).Scan(&sender, &dest, &ref, &total)
	if err == sql.ErrNoRows || isNoSuchTable(err) || (err == nil && total <= 1) {
		return ids, nil
	}
	if err != nil {
		return nil, err
	}
	byTable := map[string][]any{}
	var tables []string
	for _, o := range open[1:] {
		if o.accountID != newest.accountID {
			continue
		}
		t := o.table()
		if _, seen := byTable[t]; !seen {
			tables = append(tables, t)
		}
		byTable[t] = append(byTable[t], o.msgID)
	}
	found := map[string]bool{}
	for _, table := range tables {
		cand := byTable[table]
		args := append(append([]any{}, cand...), newest.accountID, sender, dest, ref, total)
		rows, err := s.db.QueryContext(ctx,
			"SELECT msg_id FROM "+table+" WHERE msg_id IN ("+placeholders(len(cand))+
				") AND account_id=? AND sender=? AND destination=? AND udh_ref=? AND udh_total=?", args...)
		if isNoSuchTable(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			found[id] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	for _, o := range open[1:] { // keep the order: newest first
		if found[o.msgID] {
			ids = append(ids, o.msgID)
		}
	}
	return ids, nil
}

// RecordConcatPart registers one part of a multi-part send so its own receipt
// can later be correlated and aggregated, with its message's company.
// Idempotent on (vendor_id, vendor_msg_id). [D-037]
func (s *Store) RecordConcatPart(ctx context.Context, vendorID int64, vendorMsgID string, ref model.RecordRef, seq, total int) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO concat_parts (vendor_id, vendor_msg_id, msg_id, company_id, part_seq, parts_total)
		 VALUES (?,?,?,?,?,?)
		 ON DUPLICATE KEY UPDATE msg_id=VALUES(msg_id), company_id=VALUES(company_id), part_seq=VALUES(part_seq), parts_total=VALUES(parts_total)`,
		vendorID, vendorMsgID, ref.MsgID, ref.CompanyID, seq, total)
	return err
}

// FoldConcatParts lets go of a finished group's working rows (D-040). The
// parts' answers are rows of sub_ and each part's receipt is a row of dlr_
// (issues 12 and 25.4): nothing is folded onto the message's row, which is
// never changed. Safe to call repeatedly.
//
// Not while a part still waits for its receipt: one failed part closes the
// message, and a later part's receipt must still find its row, to be
// recorded (MarkPartAndAggregate: "recorded, not re-finalized") — removed at
// the close, it was filed as an orphan for good and the message's receipts
// were short (BUG-RR-04, 2 Nov 2026). The last part's receipt folds the
// group; parts whose receipt never comes are left for the pruner.
func (s *Store) FoldConcatParts(ctx context.Context, ref model.RecordRef) error {
	var waiting int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM concat_parts WHERE msg_id=? AND (status IS NULL OR status NOT IN ('delivered','failed','expired','rejected'))",
		ref.MsgID).Scan(&waiting); err != nil {
		return err
	}
	if waiting > 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, "DELETE FROM concat_parts WHERE msg_id=?", ref.MsgID)
	return err
}

// PruneConcatParts clears groups still staged after olderThanHours — sends
// whose receipts never all arrived. Returns groups pruned. [SCOPE P8/D-040]
func (s *Store) PruneConcatParts(ctx context.Context, olderThanHours, limit int) (int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT msg_id FROM concat_parts
		 WHERE created_at < DATE_SUB(NOW(), INTERVAL ? HOUR) GROUP BY msg_id LIMIT ?`, olderThanHours, limit)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	n := 0
	for _, id := range ids {
		if _, err := s.db.ExecContext(ctx, "DELETE FROM concat_parts WHERE msg_id=?", id); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// ---- sweeps over the open messages (no Redis) ----

// AwaitingReceipt is one message an account's no_receipt rule may have to
// answer for (dal.AwaitingReceipt).
type AwaitingReceipt = dal.AwaitingReceipt

// AwaitingReceiptOlder lists one account's messages that have been waiting
// for a receipt longer than olderThan, from open_msgs. [receipt policy
// no_receipt, hot/0044] Only this deployment's and the shared ones (D-121).
func (s *Store) AwaitingReceiptOlder(ctx context.Context, accountID int64, olderThan time.Duration, limit int) ([]AwaitingReceipt, error) {
	out, _, err := s.AwaitingReceiptOlderAfter(ctx, accountID, olderThan, AwaitCursor{}, limit)
	return out, err
}

// AwaitCursor is how far the no-receipt sweep has read one account's
// messages (dal.AwaitCursor).
type AwaitCursor = dal.AwaitCursor

// AwaitingReceiptOlderAfter is AwaitingReceiptOlder from after a cursor; it
// returns the cursor of the last message it READ — also of one it left out
// because its client was told already, or it is final.
//
// A message told by the no-receipt rule keeps its row in open_msgs until a
// receipt closes it or its vendor's wait ends (the vendor's real receipt
// still goes on its record), and the rows are read oldest first: read from
// the start every minute, the told ones took every place of the pass, and
// past 500 a customer's messages were never told (go-live test 18 Oct 2026:
// 136 of 600). The sweep reads on from where it was.
func (s *Store) AwaitingReceiptOlderAfter(ctx context.Context, accountID int64, olderThan time.Duration, after AwaitCursor, limit int) ([]AwaitingReceipt, AwaitCursor, error) {
	mins := int(olderThan / time.Minute)
	if mins < 1 {
		mins = 1
	}
	own, ownArgs := s.ownerCond()
	from, fromArgs := "", []any{}
	if after.MsgID != "" {
		from = " AND (sent_at > ? OR (sent_at = ? AND msg_id > ?))"
		fromArgs = []any{after.SentAt.UTC(), after.SentAt.UTC(), after.MsgID}
	}
	args := append(append(append([]any{accountID, mins}, fromArgs...), ownArgs...), limit)
	rows, err := s.db.QueryContext(ctx, `
		SELECT msg_id, company_id, COALESCE(queue_owner,''), sent_at FROM open_msgs
		 WHERE account_id=? AND sent_at < DATE_SUB(NOW(), INTERVAL ? MINUTE)`+from+own+`
		 ORDER BY sent_at, msg_id LIMIT ?`, args...)
	if err != nil {
		return nil, after, err
	}
	var found []AwaitingReceipt
	next := after
	for rows.Next() {
		var r AwaitingReceipt
		var sent time.Time
		if err := rows.Scan(&r.MsgID, &r.CompanyID, &r.Owner, &sent); err != nil {
			rows.Close()
			return nil, after, err
		}
		found = append(found, r)
		next = AwaitCursor{SentAt: sent, MsgID: r.MsgID}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, after, err
	}
	// The rest from each record: no final receipt, nobody told (no answer of
	// the account's own policy filed), and the addresses an SMPP client
	// matches its receipt on.
	var out []AwaitingReceipt
	for _, r := range found {
		var noRcpt, failOnly int
		ref := model.RecordRef{MsgID: r.MsgID, CompanyID: r.CompanyID}
		err := s.db.QueryRowContext(ctx,
			"SELECT COALESCE(m.sender,''), COALESCE(m.destination,''), COALESCE(m.bind_slot,0), COALESCE(m.no_receipt,0), COALESCE(m.failure_receipt_only,0) FROM "+
				mtOf(ref)+" m WHERE m.msg_id=? AND NOT EXISTS (SELECT 1 FROM "+dlrOf(ref)+
				" d WHERE d.msg_id = m.msg_id AND (d.final_of IS NOT NULL OR (d.raw LIKE 'policy:%' AND d.status IN ('delivered','failed','expired'))))",
			r.MsgID).Scan(&r.Sender, &r.Destination, &r.BindSlot, &noRcpt, &failOnly)
		if isNoSuchTable(err) {
			// No receipt table for that day yet: nothing closed it.
			err = s.db.QueryRowContext(ctx,
				"SELECT COALESCE(sender,''), COALESCE(destination,''), COALESCE(bind_slot,0), COALESCE(no_receipt,0), COALESCE(failure_receipt_only,0) FROM "+
					mtOf(ref)+" WHERE msg_id=?", r.MsgID).Scan(&r.Sender, &r.Destination, &r.BindSlot, &noRcpt, &failOnly)
		}
		if err == sql.ErrNoRows || isNoSuchTable(err) {
			continue // final, told, or its record is gone: nothing to answer for
		}
		if err != nil {
			// Not read: the pass ends here, and the cursor stays where it
			// was, so this message is read again next time.
			return nil, after, err
		}
		r.NoReceipt, r.FailureReceiptOnly = noRcpt != 0, failOnly != 0
		out = append(out, r)
	}
	return out, next, nil
}

// PendingDLR is one message awaiting a poll-mode status answer
// (dal.PendingDLR).
type PendingDLR = dal.PendingDLR

// SentAwaitingDLR lists a vendor's messages that are waiting for their
// receipt with a vendor id — the poll-mode HTTP vendor asks their status.
func (s *Store) SentAwaitingDLR(ctx context.Context, vendorID int64, limit int) ([]PendingDLR, error) {
	return s.SentAwaitingDLRPage(ctx, vendorID, "", limit)
}

// SentAwaitingDLRPage is SentAwaitingDLR walked in msg_id order, starting
// after `after` ("" = from the start), so every waiting message is reached in
// turn (QA EP-021).
func (s *Store) SentAwaitingDLRPage(ctx context.Context, vendorID int64, after string, limit int) ([]PendingDLR, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT msg_id, COALESCE(vendor_msg_id,''), account_id, company_id FROM open_msgs
		  WHERE vendor_id=? AND vendor_msg_id IS NOT NULL AND vendor_msg_id<>'' AND msg_id > ?
		  ORDER BY msg_id LIMIT ?`, vendorID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingDLR
	for rows.Next() {
		var p PendingDLR
		if err := rows.Scan(&p.MsgID, &p.VendorMsgID, &p.AccountID, &p.CompanyID); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SweepOpenMessages lets go of messages whose vendor's wait for a receipt
// has run out: they stay `sent` on the log, and a receipt that comes later is
// filed as an orphan. Returns how many were let go.
func (s *Store) SweepOpenMessages(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 5000
	}
	res, err := s.db.ExecContext(ctx, "DELETE FROM open_msgs WHERE wait_until < NOW() ORDER BY wait_until LIMIT ?", limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// recordTableCheck refuses a table name that is not one of ours of the given
// kind: a name that came from outside the database (a receipt's facts in
// Redis) and is put into SQL text.
func recordTableCheck(name, kind string) bool {
	rt, ok := billingstore.ParseRecordTable(name)
	return ok && rt.Kind == kind
}
