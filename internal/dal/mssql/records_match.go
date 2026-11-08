package mssql

// Finding a message from its receipt, on SQL Server (mirrors
// internal/dal/mysql/records_match.go and the part tracker of mysql.go):
// open_msgs (no Redis) by vendor + vendor id (+ the handset), the parts of a
// long message in concat_parts, the same collision guards, the same match
// order. The sweeps over the open messages are here too.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/msisdn"
)

// ResolveVendorMsgID finds our message for a vendor-assigned id, scoped to
// the vendor. ok=false if unknown.
func (s *RecordStore) ResolveVendorMsgID(ctx context.Context, vendorID int64, vendorMsgID string) (msgID string, accountID int64, noReceipt, failureOnly bool, bindSlot int, privacy model.Privacy, mcc, mnc string, ok bool, err error) {
	m, err := s.resolveReceipt(ctx, []int64{vendorID}, vendorMsgID, "")
	if err != nil || len(m.MsgIDs) == 0 {
		return "", 0, false, false, 0, 0, "", "", false, err
	}
	return m.MsgIDs[0], m.AccountID, m.NoReceipt, m.FailureOnly, m.BindSlot, m.Privacy, m.MCC, m.MNC, true, nil
}

// ResolveVendorMsgIDAll returns every message sharing one vendor id: the
// parts of a long message an HTTP vendor took as one call (D-036).
func (s *RecordStore) ResolveVendorMsgIDAll(ctx context.Context, vendorID int64, vendorMsgID string) (msgIDs []string, accountID int64, noReceipt, failureOnly bool, bindSlot int, privacy model.Privacy, mcc, mnc string, err error) {
	m, err := s.resolveReceipt(ctx, []int64{vendorID}, vendorMsgID, "")
	return m.MsgIDs, m.AccountID, m.NoReceipt, m.FailureOnly, m.BindSlot, m.Privacy, m.MCC, m.MNC, err
}

// ResolveReceipt is ResolveVendorMsgIDAll for every vendor of one company,
// with the receipt's handset number, and by our own message id when the id
// is one of ours and nothing else matched (dlr.MatchResolver).
func (s *RecordStore) ResolveReceipt(ctx context.Context, vendorIDs []int64, vendorMsgID, number string) (model.ReceiptMatch, error) {
	if len(vendorIDs) == 0 || vendorMsgID == "" {
		return model.ReceiptMatch{}, nil
	}
	m, err := s.resolveReceipt(ctx, vendorIDs, vendorMsgID, number)
	if err != nil || len(m.MsgIDs) > 0 || !recIsULID(vendorMsgID) {
		return m, err
	}
	return s.resolveByOurID(ctx, vendorIDs, vendorMsgID)
}

// recMaxOpenPerVendorID bounds how many open messages of one vendor id are
// looked at: the parts of one long message.
const recMaxOpenPerVendorID = 256

// recOpen is one message still waiting for its delivery truth.
type recOpen struct {
	msgID     string
	accountID int64
	companyID int64
	vendorID  int64
}

func (o recOpen) ref() model.RecordRef {
	return model.RecordRef{MsgID: o.msgID, CompanyID: o.companyID}
}
func (o recOpen) table() string { return recMT(o.ref()) }

// recVendorList is vendor ids as one JSON parameter.
func recVendorList(ids []int64) any {
	b, _ := json.Marshal(ids)
	return rText(string(b))
}

const recVendorIn = "vendor_id IN (SELECT v FROM OPENJSON(@p1) WITH (v BIGINT '$'))"

var (
	stRecOpenByVendorID = RecordStmts.Add("record.open.by_vendor_id", `SELECT TOP (@p3) msg_id, account_id, company_id, vendor_id FROM open_msgs
		WHERE `+recVendorIn+` AND vendor_msg_id = @p2 ORDER BY msg_id DESC`, true)
	stRecPartsByVendorID = RecordStmts.Add("record.concat.by_vendor_id", `SELECT TOP (@p3) msg_id, COALESCE(company_id, 0), vendor_id FROM concat_parts
		WHERE `+recVendorIn+` AND vendor_msg_id = @p2`, true)
	stRecPartDropOld = RecordStmts.Add("record.concat.drop_old", "DELETE FROM concat_parts WHERE vendor_id = @p1 AND vendor_msg_id = @p2 AND msg_id = @p3", true)
	stRecMatchOf     = RecordStmts.Add("record.mt.match_of", `SELECT COALESCE(account_id, 0), COALESCE(no_receipt, 0), COALESCE(failure_receipt_only, 0),
		COALESCE(bind_slot, 0), COALESCE(mask_body, 0), COALESCE(mcc, N''), COALESCE(mnc, N'') FROM {mt} WHERE msg_id = @p1`, true)
	stRecDestinations = RecordStmts.Add("record.mt.destinations", "SELECT TOP (100000) t.msg_id, COALESCE(t.destination, N'') FROM "+rIDsFrom+"{mt} AS t ON t.msg_id = j.v", true)
	stRecOpenOurID    = RecordStmts.Add("record.open.our_id", "SELECT msg_id, account_id, company_id, vendor_id FROM open_msgs WHERE msg_id = @p2 AND "+recVendorIn, true)
	stRecConcatOf     = RecordStmts.Add("record.mt.concat_of", `SELECT COALESCE(sender, N''), COALESCE(destination, N''), COALESCE(udh_ref, 0), COALESCE(udh_total, 0)
		FROM {mt} WHERE msg_id = @p1`, true)
	stRecSiblings = RecordStmts.Add("record.mt.siblings", "SELECT TOP (100000) t.msg_id FROM "+rIDsFrom+`{mt} AS t ON t.msg_id = j.v
		WHERE t.account_id = @p2 AND t.sender = @p3 AND t.destination = @p4 AND t.udh_ref = @p5 AND t.udh_total = @p6`, true)
)

// scanOpen reads open records off rows; withAccount: the rows carry an
// account.
func scanOpen(rows *sql.Rows, withAccount bool) ([]recOpen, error) {
	defer rows.Close()
	var out []recOpen
	for rows.Next() {
		var o recOpen
		var err error
		if withAccount {
			err = rows.Scan(&o.msgID, &o.accountID, &o.companyID, &o.vendorID)
		} else {
			err = rows.Scan(&o.msgID, &o.companyID, &o.vendorID)
		}
		if err != nil {
			return nil, err
		}
		o.msgID = rTrim(o.msgID)
		out = append(out, o)
	}
	return out, rows.Err()
}

// resolveReceipt finds the message of a vendor id among the vendors given,
// with MySQL's guards: only messages still awaiting their truth resolve; the
// whole-message arm yields ONE message unless the others are parts of the
// same long message; the two arms naming different messages is the id handed
// out again — the newer is meant, and the dead part row is removed.
func (s *RecordStore) resolveReceipt(ctx context.Context, vendorIDs []int64, vendorMsgID, number string) (model.ReceiptMatch, error) {
	vlist := recVendorList(vendorIDs)
	var open, parts []recOpen
	if dal.OpenMessagesInSQL() {
		rows, err := rQuery(ctx, s.db, stRecOpenByVendorID, stRecOpenByVendorID.SQL, vlist, rText(vendorMsgID), recMaxOpenPerVendorID)
		if err != nil {
			return model.ReceiptMatch{}, err
		}
		if open, err = scanOpen(rows, true); err != nil {
			return model.ReceiptMatch{}, err
		}
	}
	{
		rows, err := rQuery(ctx, s.db, stRecPartsByVendorID, stRecPartsByVendorID.SQL, vlist, rText(vendorMsgID), 100000)
		if err != nil {
			return model.ReceiptMatch{}, err
		}
		if parts, err = scanOpen(rows, false); err != nil {
			return model.ReceiptMatch{}, err
		}
		sort.SliceStable(parts, func(i, j int) bool { return parts[i].msgID > parts[j].msgID })
	}
	if number != "" {
		var err error
		if open, err = s.byNumber(ctx, open, number); err != nil {
			return model.ReceiptMatch{}, err
		}
		if parts, err = s.byNumber(ctx, parts, number); err != nil {
			return model.ReceiptMatch{}, err
		}
	}
	var whole, part recOpen
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
	var lead recOpen
	switch {
	case whole.msgID != "" && part.msgID != "" && whole.msgID != part.msgID:
		if whole.msgID > part.msgID { // ULIDs sort by the time they were made
			ids, lead = group, whole
			if _, err := rExec(ctx, s.db, stRecPartDropOld, stRecPartDropOld.SQL, part.vendorID, rText(vendorMsgID), Varchar(part.msgID)); err != nil {
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
		// A part's record is on its way (the DB worker writes it).
		return model.ReceiptMatch{RecordPending: true}, nil
	}
	if err == nil && len(m.MsgIDs) > 0 && lead.msgID == part.msgID {
		// A part's receipt waits for the vendor's accepted answers in sub_.
		sub := recTable(billingstore.KindSub, lead.companyID, lead.msgID)
		taken, terr := recAcceptedIn(ctx, s.db, sub, []string{lead.msgID})
		if terr != nil {
			return model.ReceiptMatch{}, terr
		}
		if len(taken[lead.msgID]) == 0 {
			return model.ReceiptMatch{RecordPending: true}, nil
		}
	}
	return m, err
}

// matchOf is what a receipt's path needs of the messages it matched, read
// from the lead message's record; no record yet = no match.
func (s *RecordStore) matchOf(ctx context.Context, ids []string, lead recOpen) (model.ReceiptMatch, error) {
	m := model.ReceiptMatch{MsgIDs: ids, VendorID: lead.vendorID, CompanyID: lead.companyID}
	var noRcpt, failOnly, mb int
	err := rRow(ctx, s.db, stRecMatchOf, rOn(stRecMatchOf.SQL, "mt", lead.table()), Varchar(ids[0])).
		Scan(&m.AccountID, &noRcpt, &failOnly, &m.BindSlot, &mb, &m.MCC, &m.MNC)
	if err == sql.ErrNoRows || MissingTable(err) || (err == nil && m.AccountID == 0) {
		return model.ReceiptMatch{}, nil
	}
	if err != nil {
		return model.ReceiptMatch{}, err
	}
	m.NoReceipt, m.FailureOnly, m.Privacy = noRcpt != 0, failOnly != 0, model.Privacy(mb)
	return m, nil
}

// byNumber keeps, of the messages an id can be about, those sent to number.
// None of several is none; one that is not the number is still that one.
func (s *RecordStore) byNumber(ctx context.Context, recs []recOpen, number string) ([]recOpen, error) {
	if len(recs) == 0 {
		return recs, nil
	}
	dest, err := s.destinations(ctx, recs)
	if err != nil {
		return nil, err
	}
	var keep []recOpen
	for _, o := range recs {
		if msisdn.Same(number, dest[o.msgID]) {
			keep = append(keep, o)
		}
	}
	if len(keep) == 0 && len(recs) == 1 {
		return recs, nil
	}
	return keep, nil
}

// destinations reads the number each message was sent to, one statement a
// table; a message whose record cannot be read is left out.
func (s *RecordStore) destinations(ctx context.Context, recs []recOpen) (map[string]string, error) {
	byTable := map[string][]string{}
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
		rows, err := rQuery(ctx, s.db, stRecDestinations, rOn(stRecDestinations.SQL, "mt", table), rIDs(byTable[table]))
		if MissingTable(err) {
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
			out[rTrim(id)] = d
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// resolveByOurID is the third rule: a receipt that names OUR message id is
// that message, if it is still open and was sent through one of the vendors.
func (s *RecordStore) resolveByOurID(ctx context.Context, vendorIDs []int64, msgID string) (model.ReceiptMatch, error) {
	if !dal.OpenMessagesInSQL() {
		return model.ReceiptMatch{}, nil
	}
	var o recOpen
	err := rRow(ctx, s.db, stRecOpenOurID, stRecOpenOurID.SQL, recVendorList(vendorIDs), Varchar(msgID)).
		Scan(&o.msgID, &o.accountID, &o.companyID, &o.vendorID)
	if err == sql.ErrNoRows {
		return model.ReceiptMatch{}, nil
	}
	if err != nil {
		return model.ReceiptMatch{}, err
	}
	o.msgID = rTrim(o.msgID)
	return s.matchOf(ctx, []string{msgID}, o)
}

// recIsULID reports whether id has the shape of one of our message ids.
func recIsULID(id string) bool {
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
// the same account, sender and destination, concat reference and count.
func (s *RecordStore) concatSiblings(ctx context.Context, open []recOpen) ([]string, error) {
	newest := open[0]
	ids := []string{newest.msgID}
	var sender, dest string
	var ref, total int
	err := rRow(ctx, s.db, stRecConcatOf, rOn(stRecConcatOf.SQL, "mt", newest.table()), Varchar(newest.msgID)).Scan(&sender, &dest, &ref, &total)
	if err == sql.ErrNoRows || MissingTable(err) || (err == nil && total <= 1) {
		return ids, nil
	}
	if err != nil {
		return nil, err
	}
	byTable := map[string][]string{}
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
		rows, err := rQuery(ctx, s.db, stRecSiblings, rOn(stRecSiblings.SQL, "mt", table), rIDs(byTable[table]),
			newest.accountID, rText(sender), rText(dest), ref, total)
		if MissingTable(err) {
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
			found[rTrim(id)] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	for _, o := range open[1:] {
		if found[o.msgID] {
			ids = append(ids, o.msgID)
		}
	}
	return ids, nil
}

// ---- the parts of a long message (concat_parts) ----

var (
	// One part's row, made or put again (MySQL's ON DUPLICATE KEY UPDATE);
	// updated_at moves only when the row changes, as MySQL's ON UPDATE does.
	stRecPartPut = AddUpsert(RecordStmts, "record.concat.put", UpsertSpec{
		Table: "concat_parts", Keys: []string{"vendor_id", "vendor_msg_id"},
		Set: []string{
			"updated_at = CASE WHEN t.msg_id <> $msg_id OR COALESCE(t.company_id, -1) <> $company_id OR t.part_seq <> $part_seq OR t.parts_total <> $parts_total THEN SYSUTCDATETIME() ELSE t.updated_at END",
			"msg_id = $msg_id", "company_id = $company_id", "part_seq = $part_seq", "parts_total = $parts_total"},
		Insert: []string{"vendor_id", "vendor_msg_id", "msg_id", "company_id", "part_seq", "parts_total"},
	}, true)
	// One part's receipt and its group's verdict before and after, in ONE
	// transaction (MySQL's markPartOnce): the group key read without a lock,
	// then every row of the group locked (UPDLOCK, in index order) so a
	// second receipt of the same message waits here until this one commits.
	stRecPartMark = RecordStmts.Add("record.concat.mark", `SET NOCOUNT ON; SET XACT_ABORT ON;
DECLARE @yk_m CHAR(26), @yk_total INT, @yk_bd INT, @yk_bb INT, @yk_ad INT, @yk_ab INT;
SELECT @yk_m = msg_id, @yk_total = parts_total FROM concat_parts WHERE vendor_id = @p1 AND vendor_msg_id = @p2;
IF @yk_m IS NULL
BEGIN
	SELECT CAST(0 AS INT), 0, 0, 0, 0, 0, CAST(N'' AS NVARCHAR(26));
	RETURN;
END
BEGIN TRAN;
SELECT @yk_bd = COALESCE(SUM(CASE WHEN status = N'delivered' THEN 1 ELSE 0 END), 0),
	@yk_bb = COALESCE(SUM(CASE WHEN status IN (N'failed', N'expired') THEN 1 ELSE 0 END), 0)
	FROM concat_parts WITH (UPDLOCK, ROWLOCK) WHERE msg_id = @yk_m;
UPDATE concat_parts SET status = @p3,
	updated_at = CASE WHEN status IS NULL OR status <> @p3 THEN SYSUTCDATETIME() ELSE updated_at END
	WHERE vendor_id = @p1 AND vendor_msg_id = @p2;
SELECT @yk_ad = COALESCE(SUM(CASE WHEN status = N'delivered' THEN 1 ELSE 0 END), 0),
	@yk_ab = COALESCE(SUM(CASE WHEN status IN (N'failed', N'expired') THEN 1 ELSE 0 END), 0)
	FROM concat_parts WITH (UPDLOCK, ROWLOCK) WHERE msg_id = @yk_m;
COMMIT;
SELECT CAST(1 AS INT), @yk_total, @yk_bd, @yk_bb, @yk_ad, @yk_ab, CAST(@yk_m AS NVARCHAR(26));`, true)
	// Not while a part still waits for its receipt (BUG-RR-04): see FoldConcatParts.
	stRecPartFold = RecordStmts.Add("record.concat.fold", `DELETE FROM concat_parts WHERE msg_id = @p1
		AND NOT EXISTS (SELECT 1 FROM concat_parts AS w WHERE w.msg_id = @p1
			AND (w.status IS NULL OR w.status NOT IN (N'delivered', N'failed', N'expired', N'rejected')))`, true)
	stRecPartPrune = RecordStmts.Add("record.concat.prune", `DELETE FROM concat_parts OUTPUT DELETED.msg_id
		WHERE msg_id IN (SELECT TOP (@p2) msg_id FROM concat_parts WHERE created_at < DATEADD(HOUR, -@p1, SYSUTCDATETIME()) GROUP BY msg_id)`, false)
)

// RecordConcatPart registers one part of a multi-part send so its own
// receipt can later be correlated and aggregated. Idempotent on (vendor_id,
// vendor_msg_id). [D-037]
func (s *RecordStore) RecordConcatPart(ctx context.Context, vendorID int64, vendorMsgID string, ref model.RecordRef, seq, total int) error {
	_, err := stRecPartPut.Exec(ctx, s.db, sql.Named("vendor_id", vendorID), sql.Named("vendor_msg_id", rText(vendorMsgID)),
		sql.Named("msg_id", Varchar(ref.MsgID)), sql.Named("company_id", ref.CompanyID), sql.Named("part_seq", seq), sql.Named("parts_total", total))
	return err
}

// recAggregate is the group verdict from its part counts: one broken part
// breaks the message; every part delivered delivers it; else it is sent.
func recAggregate(delivered, bad, total int) model.Status {
	switch {
	case bad > 0:
		return model.StatusFailed
	case delivered >= total:
		return model.StatusDelivered
	default:
		return model.StatusSent
	}
}

// MarkPartAndAggregate records one part's receipt and returns the group's
// verdict. ready=true exactly once per group: the group's rows are locked
// for the duration, and a group final before this part arrived is not
// finalized again. A deadlock victim goes again (the unit is one
// transaction).
func (s *RecordStore) MarkPartAndAggregate(ctx context.Context, vendorID int64, vendorMsgID string, st model.Status) (agg model.Status, ready, ok bool, err error) {
	return s.markPart(ctx, vendorID, vendorMsgID, st, false)
}

// MarkFirstPartFinal is MarkPartAndAggregate for a vendor whose first final
// closes a long message (dlr_config.first_receipt_closes, Saleem 4 Nov
// 2026): ready for the group's first final part, with that part's status;
// every later part is recorded on its row only. Same statement and locks.
func (s *RecordStore) MarkFirstPartFinal(ctx context.Context, vendorID int64, vendorMsgID string, st model.Status) (agg model.Status, ready, ok bool, err error) {
	return s.markPart(ctx, vendorID, vendorMsgID, st, true)
}

func (s *RecordStore) markPart(ctx context.Context, vendorID int64, vendorMsgID string, st model.Status, first bool) (agg model.Status, ready, ok bool, err error) {
	var found, total, bd, bb, ad, ab int
	var msgID string
	err = Again(ctx, 4, func() error {
		start := time.Now()
		e := s.db.QueryRowContext(ctx, stRecPartMark.SQL, vendorID, rText(vendorMsgID), rText(string(st))).Scan(&found, &total, &bd, &bb, &ad, &ab, &msgID)
		observe(stRecPartMark, start, 1)
		return e
	})
	if err != nil {
		return "", false, false, err
	}
	if found == 0 {
		return "", false, false, nil
	}
	before, after := recAggregate(bd, bb, total), recAggregate(ad, ab, total)
	if first {
		// Closed before = any part already had its final; the message
		// closes with this part's own status.
		before, after = model.StatusSent, st
		if bd+bb > 0 {
			before = model.StatusDelivered
		}
	}
	if before.IsTerminal() {
		// A late or repeated receipt: recorded, not finalized again. The
		// group's rows go once no part waits any more (FoldConcatParts).
		if ferr := s.FoldConcatParts(ctx, model.RecordRef{MsgID: msgID}); ferr != nil {
			log.Printf("[store] fold the parts of %s after its last part's receipt failed (rows stay for the pruner): %v", msgID, ferr)
		}
		return after, false, true, nil
	}
	return after, after.IsTerminal(), true, nil
}

// FoldConcatParts lets go of a finished group's working rows (D-040), once
// no part still waits for its receipt: one failed part closes the message,
// and a later part's receipt must still find its row to be recorded — gone
// at the close, it was an orphan for good (BUG-RR-04, 2 Nov 2026). Safe
// to call repeatedly. Run again when SQL Server makes the DELETE a deadlock
// victim (two parts' receipts of one group closing at once, under load).
func (s *RecordStore) FoldConcatParts(ctx context.Context, ref model.RecordRef) error {
	return Again(ctx, recTries, func() error {
		_, err := rExec(ctx, s.db, stRecPartFold, stRecPartFold.SQL, Varchar(ref.MsgID))
		return err
	})
}

// PruneConcatParts clears groups still staged after olderThanHours, at most
// limit groups, in one statement. Returns the groups pruned.
func (s *RecordStore) PruneConcatParts(ctx context.Context, olderThanHours, limit int) (int, error) {
	rows, err := rQuery(ctx, s.db, stRecPartPrune, stRecPartPrune.SQL, olderThanHours, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	groups := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return len(groups), err
		}
		groups[rTrim(id)] = true
	}
	return len(groups), rows.Err()
}

// ---- sweeps over the open messages (no Redis) ----

var (
	stRecAwaitOpen = RecordStmts.Add("record.open.awaiting", `SELECT TOP (@p3) msg_id, company_id, COALESCE(queue_owner, N''), sent_at FROM open_msgs
		WHERE account_id = @p1 AND sent_at < DATEADD(MINUTE, -@p2, SYSUTCDATETIME()){from}{own} ORDER BY sent_at, msg_id`, false)
	stRecAwaitRecords = RecordStmts.Add("record.mt.awaiting", `SELECT TOP (100000) m.msg_id, COALESCE(m.sender, N''), COALESCE(m.destination, N''), COALESCE(m.bind_slot, 0),
		COALESCE(m.no_receipt, 0), COALESCE(m.failure_receipt_only, 0) FROM `+rIDsFrom+`{mt} AS m ON m.msg_id = j.v
		WHERE NOT EXISTS (SELECT 1 FROM {dlr} AS d WHERE d.msg_id = m.msg_id AND (d.final_of IS NOT NULL
			OR (d.raw LIKE N'policy:%' AND d.status IN (N'delivered', N'failed', N'expired'))))`, false)
	stRecAwaitRecordsAlone = RecordStmts.Add("record.mt.awaiting_alone", `SELECT TOP (100000) m.msg_id, COALESCE(m.sender, N''), COALESCE(m.destination, N''), COALESCE(m.bind_slot, 0),
		COALESCE(m.no_receipt, 0), COALESCE(m.failure_receipt_only, 0) FROM `+rIDsFrom+`{mt} AS m ON m.msg_id = j.v`, false)
	stRecPollPage = RecordStmts.Add("record.open.poll_page", `SELECT TOP (@p3) msg_id, COALESCE(vendor_msg_id, N''), account_id, company_id FROM open_msgs
		WHERE vendor_id = @p1 AND vendor_msg_id IS NOT NULL AND vendor_msg_id <> N'' AND msg_id > @p2 ORDER BY msg_id`, false)
	stRecOpenSweep = RecordStmts.Add("record.open.sweep", `WITH x AS (SELECT TOP (@p1) msg_id FROM open_msgs WHERE wait_until < SYSUTCDATETIME() ORDER BY wait_until)
		DELETE FROM x`, false)
)

// AwaitingReceiptOlderAfter lists one account's messages that have been
// waiting for a receipt longer than olderThan, from open_msgs, read on from
// after; it returns the cursor of the last message it READ. Only this
// deployment's and the shared ones (D-121). The rest of each message is read
// from its record, one statement a day's table (MySQL reads one a message).
func (s *RecordStore) AwaitingReceiptOlderAfter(ctx context.Context, accountID int64, olderThan time.Duration, after dal.AwaitCursor, limit int) ([]dal.AwaitingReceipt, dal.AwaitCursor, error) {
	mins := int(olderThan / time.Minute)
	if mins < 1 {
		mins = 1
	}
	from := ""
	args := []any{accountID, mins, limit}
	if after.MsgID != "" {
		from = " AND (sent_at > @at OR (sent_at = @at AND msg_id > @id))"
		args = append(args, sql.Named("at", Time(after.SentAt)), sql.Named("id", Varchar(after.MsgID)))
	}
	args = append(args, s.ownerArgs()...)
	text := rOnly(stRecAwaitOpen.SQL, "{from}", from, "{own}", s.ownerCond("queue_owner"))
	rows, err := rQuery(ctx, s.db, stRecAwaitOpen, text, args...)
	if err != nil {
		return nil, after, err
	}
	var found []dal.AwaitingReceipt
	next := after
	for rows.Next() {
		var r dal.AwaitingReceipt
		var sent time.Time
		if err := rows.Scan(&r.MsgID, &r.CompanyID, &r.Owner, &sent); err != nil {
			rows.Close()
			return nil, after, err
		}
		r.MsgID = rTrim(r.MsgID)
		found = append(found, r)
		next = dal.AwaitCursor{SentAt: sent.UTC(), MsgID: r.MsgID}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, after, err
	}
	// The rest from each record: no final receipt, nobody told, and the
	// addresses an SMPP client matches its receipt on.
	byTable := map[string][]string{}
	var tables []string
	for _, r := range found {
		t := recMT(model.RecordRef{MsgID: r.MsgID, CompanyID: r.CompanyID})
		if _, ok := byTable[t]; !ok {
			tables = append(tables, t)
		}
		byTable[t] = append(byTable[t], r.MsgID)
	}
	type rest struct {
		sender, dest     string
		slot             int
		noRcpt, failOnly int
	}
	got := map[string]rest{}
	for _, table := range tables {
		dlr := billingstore.SwapKind(table, billingstore.KindDLR)
		rows, err := rQuery(ctx, s.db, stRecAwaitRecords, rOn(stRecAwaitRecords.SQL, "mt", table, "dlr", dlr), rIDs(byTable[table]))
		if MissingTable(err) {
			// No receipt table for that day yet: nothing closed it.
			rows, err = rQuery(ctx, s.db, stRecAwaitRecordsAlone, rOn(stRecAwaitRecordsAlone.SQL, "mt", table), rIDs(byTable[table]))
		}
		if MissingTable(err) {
			continue // its record is gone: nothing to answer for
		}
		if err != nil {
			// Not read: the pass ends here, and the cursor stays where it
			// was, so these messages are read again next time.
			return nil, after, err
		}
		for rows.Next() {
			var id string
			var x rest
			if err := rows.Scan(&id, &x.sender, &x.dest, &x.slot, &x.noRcpt, &x.failOnly); err != nil {
				rows.Close()
				return nil, after, err
			}
			got[rTrim(id)] = x
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, after, err
		}
	}
	var out []dal.AwaitingReceipt
	for _, r := range found {
		x, ok := got[r.MsgID]
		if !ok {
			continue // final, told, or its record is gone
		}
		r.Sender, r.Destination, r.BindSlot = x.sender, x.dest, x.slot
		r.NoReceipt, r.FailureReceiptOnly = x.noRcpt != 0, x.failOnly != 0
		out = append(out, r)
	}
	return out, next, nil
}

// AwaitingReceiptOlder is AwaitingReceiptOlderAfter from the start.
func (s *RecordStore) AwaitingReceiptOlder(ctx context.Context, accountID int64, olderThan time.Duration, limit int) ([]dal.AwaitingReceipt, error) {
	out, _, err := s.AwaitingReceiptOlderAfter(ctx, accountID, olderThan, dal.AwaitCursor{}, limit)
	return out, err
}

// SentAwaitingDLR is SentAwaitingDLRPage from the start.
func (s *RecordStore) SentAwaitingDLR(ctx context.Context, vendorID int64, limit int) ([]dal.PendingDLR, error) {
	return s.SentAwaitingDLRPage(ctx, vendorID, "", limit)
}

// rOnly replaces placeholders that are not tables ({from}, {own}).
func rOnly(text string, pairs ...string) string {
	for i := 0; i+1 < len(pairs); i += 2 {
		text = strings.ReplaceAll(text, pairs[i], pairs[i+1])
	}
	return text
}

// SentAwaitingDLRPage is a vendor's messages waiting for their receipt with
// a vendor id, in msg_id order after `after` (the poll-mode HTTP vendor).
func (s *RecordStore) SentAwaitingDLRPage(ctx context.Context, vendorID int64, after string, limit int) ([]dal.PendingDLR, error) {
	rows, err := rQuery(ctx, s.db, stRecPollPage, stRecPollPage.SQL, vendorID, Varchar(after), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dal.PendingDLR
	for rows.Next() {
		var p dal.PendingDLR
		if err := rows.Scan(&p.MsgID, &p.VendorMsgID, &p.AccountID, &p.CompanyID); err != nil {
			return nil, err
		}
		p.MsgID = rTrim(p.MsgID)
		out = append(out, p)
	}
	return out, rows.Err()
}

// SweepOpenMessages lets go of messages whose vendor's wait for a receipt
// has run out. Returns how many were let go.
func (s *RecordStore) SweepOpenMessages(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 5000
	}
	res, err := rExec(ctx, s.db, stRecOpenSweep, stRecOpenSweep.SQL, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
