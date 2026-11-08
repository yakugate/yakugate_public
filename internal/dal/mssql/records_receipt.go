package mssql

// A message's status and its receipts on SQL Server (mirrors the "a
// message's status", "receipts" and "the status pull" parts of
// internal/dal/mysql/records.go). A message's status is its final receipt —
// the dlr_ row whose final_of is its id, unique (uq_final, filtered to the
// rows that have one, as MySQL's unique key allows many NULLs) — else sent
// when it has a sub_ row, else queued. "Final once": the second receipt to
// close a message meets uq_final and is recorded only.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
)

// recEntry mirrors internal/dlr.entry's wire shape (MySQL's rawDLREntry).
type recEntry struct {
	VendorMsgID  string  `json:"vendor_msg_id"`
	Status       string  `json:"status"`
	Stat         string  `json:"stat"`
	ErrorCode    string  `json:"error_code"`
	Raw          string  `json:"raw"`
	At           string  `json:"at"`
	DoneAt       string  `json:"done_at"`
	Part         int     `json:"part,omitempty"`
	Source       string  `json:"source,omitempty"`
	FinalStatus  string  `json:"final_status,omitempty"`
	RecordedOnly bool    `json:"recorded_only,omitempty"`
	SellRateID   int64   `json:"sell_rate,omitempty"`
	BuyRateID    int64   `json:"buy_rate,omitempty"`
	FX           float64 `json:"fx,omitempty"`
	Charge       string  `json:"charge,omitempty"`
}

// recArrivedAt is when a receipt reached us: the time it carries, else now.
func recArrivedAt(e recEntry) time.Time {
	if t, err := time.Parse(time.RFC3339Nano, e.At); err == nil && !t.IsZero() {
		return t
	}
	return time.Now()
}

// recDoneDate is the vendor's delivery time as a row's done_date; null when
// it did not say.
func recDoneDate(s string) any {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return rTimeText(t)
	}
	return nil
}

// recMessage is what a receipt needs of its message (MySQL's messageOf).
type recMessage struct {
	found, rejected bool
	account, vendor int64
	row             mtRow
}

var (
	stRecRefused = RecordStmts.Add("record.rejects.of", "SELECT COALESCE(account_id, 0), COALESCE(vendor_id, 0) FROM {rej} WHERE msg_id = @p1", true)
	stRecOnce    = RecordStmts.Add("record.dlr.once", `SELECT COUNT(*) FROM {dlr} WHERE msg_id = @p1 AND COALESCE(status, N'') = @p2
		AND COALESCE(error_code, N'') = @p3 AND COALESCE(raw, N'') = @p4`, true)
)

// recReadMessage reads a message's record for a receipt, in x: its MT row,
// else its refusal (MySQL's readMessage).
func recReadMessage(ctx context.Context, x Querier, ref model.RecordRef) (recMessage, error) {
	got, err := recReadMTRows(ctx, x, recMT(ref), []string{ref.MsgID})
	if err != nil && !MissingTable(err) {
		return recMessage{}, err
	}
	if r, ok := got[ref.MsgID]; ok {
		return recMessage{found: true, account: r.key.account, vendor: r.key.vendor, row: r}, nil
	}
	var m recMessage
	err = rRow(ctx, x, stRecRefused, rOn(stRecRefused.SQL, "rej", recRej(ref)), Varchar(ref.MsgID)).Scan(&m.account, &m.vendor)
	if err == sql.ErrNoRows || MissingTable(err) {
		return recMessage{}, nil
	}
	if err != nil {
		return recMessage{}, err
	}
	m.found, m.rejected = true, true
	return m, nil
}

// recThere says the receipt is in table already (same message, status,
// error code and raw text): a task seen twice.
func recThere(ctx context.Context, x Querier, table string, ref model.RecordRef, e recEntry) (bool, error) {
	var n int
	err := rRow(ctx, x, stRecOnce, rOn(stRecOnce.SQL, "dlr", table), Varchar(ref.MsgID), rText(e.Status), rText(e.ErrorCode), rText(e.Raw)).Scan(&n)
	if err != nil && !MissingTable(err) {
		return false, err
	}
	return n > 0, nil
}

// UpdateStatus closes a message with a status the gateway decided; a status
// that is not final changes nothing.
func (s *RecordStore) UpdateStatus(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode string) error {
	if !st.IsTerminal() {
		return nil
	}
	b, _ := json.Marshal(recEntry{VendorMsgID: vendorMsgID, Status: string(st), Stat: recStatFor(string(st)), ErrorCode: errorCode})
	_, err := s.CloseWithReceipt(ctx, ref, st, vendorMsgID, errorCode, string(b), true)
	if err == sql.ErrNoRows {
		return nil // no record yet: nothing to close
	}
	return err
}

// FinalizeOnce gives a message its final status only if it has none yet, and
// says whether THIS call did (dlr.Finalizer).
func (s *RecordStore) FinalizeOnce(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode string) (bool, error) {
	b, _ := json.Marshal(recEntry{VendorMsgID: vendorMsgID, Status: string(st), Stat: recStatFor(string(st)), ErrorCode: errorCode})
	mine, err := s.CloseWithReceipt(ctx, ref, st, vendorMsgID, errorCode, string(b), true)
	if err == sql.ErrNoRows {
		return true, nil // no record yet: nothing was closed before
	}
	return mine, err
}

var (
	stRecWantsMore = RecordStmts.Add("record.mt.wants_intermediate", "SELECT want_intermediate FROM {mt} WHERE msg_id = @p1", true)
	stRecOwnerMT   = RecordStmts.Add("record.mt.queue_owner", "SELECT COALESCE(queue_owner, N'') FROM {mt} WHERE msg_id = @p1", true)
	stRecOwnerRej  = RecordStmts.Add("record.rejects.queue_owner", "SELECT COALESCE(queue_owner, N'') FROM {rej} WHERE msg_id = @p1", true)
	stRecAddrMT    = RecordStmts.Add("record.mt.addresses", "SELECT COALESCE(sender, N''), COALESCE(destination, N'') FROM {mt} WHERE msg_id = @p1", true)
	stRecAddrRej   = RecordStmts.Add("record.rejects.addresses", "SELECT COALESCE(sender_id, N''), COALESCE(msisdn, N'') FROM {rej} WHERE msg_id = @p1", true)
)

// WantsIntermediate answers whether the CLIENT asked to hear about progress
// on this message. A message not found answers yes.
func (s *RecordStore) WantsIntermediate(ctx context.Context, ref model.RecordRef) (bool, error) {
	var want sql.NullInt64
	err := rRow(ctx, s.db, stRecWantsMore, rOn(stRecWantsMore.SQL, "mt", recMT(ref)), Varchar(ref.MsgID)).Scan(&want)
	if err == sql.ErrNoRows || MissingTable(err) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	return !want.Valid || want.Int64 != 0, nil
}

// MessageQueueOwner is whose queue a sent message came from (D-121). "" =
// shared.
func (s *RecordStore) MessageQueueOwner(ctx context.Context, ref model.RecordRef) (string, error) {
	var owner string
	err := rRow(ctx, s.db, stRecOwnerMT, rOn(stRecOwnerMT.SQL, "mt", recMT(ref)), Varchar(ref.MsgID)).Scan(&owner)
	if err == sql.ErrNoRows || MissingTable(err) {
		err = rRow(ctx, s.db, stRecOwnerRej, rOn(stRecOwnerRej.SQL, "rej", recRej(ref)), Varchar(ref.MsgID)).Scan(&owner)
		if err == sql.ErrNoRows || MissingTable(err) {
			return "", nil
		}
	}
	return owner, err
}

// MessageAddresses is a message's sender and destination, for its client's
// receipt.
func (s *RecordStore) MessageAddresses(ctx context.Context, ref model.RecordRef) (string, string, error) {
	var sender, dest string
	err := rRow(ctx, s.db, stRecAddrMT, rOn(stRecAddrMT.SQL, "mt", recMT(ref)), Varchar(ref.MsgID)).Scan(&sender, &dest)
	if err == sql.ErrNoRows || MissingTable(err) {
		err = rRow(ctx, s.db, stRecAddrRej, rOn(stRecAddrRej.SQL, "rej", recRej(ref)), Varchar(ref.MsgID)).Scan(&sender, &dest)
	}
	return sender, dest, err
}

// ---- receipts ----

// AppendRawDLR files one receipt of a message that does not close it.
func (s *RecordStore) AppendRawDLR(ctx context.Context, ref model.RecordRef, entryJSON string) error {
	var e recEntry
	_ = json.Unmarshal([]byte(entryJSON), &e)
	return s.appendReceipt(ctx, ref, e, false)
}

// AppendRawDLROnce is AppendRawDLR for a caller that may write the same
// receipt a second time: a row of it there already leaves everything alone.
func (s *RecordStore) AppendRawDLROnce(ctx context.Context, ref model.RecordRef, entryJSON string) error {
	var e recEntry
	_ = json.Unmarshal([]byte(entryJSON), &e)
	return s.appendReceipt(ctx, ref, e, true)
}

func (s *RecordStore) appendReceipt(ctx context.Context, ref model.RecordRef, e recEntry, once bool) error {
	table := recDLR(ref)
	if err := s.ensureRecords(ctx, table); err != nil {
		return err
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		msg, err := recReadMessage(ctx, tx, ref)
		if err != nil {
			return err
		}
		if !msg.found {
			return sql.ErrNoRows
		}
		if once {
			if there, err := recThere(ctx, tx, table, ref, e); err != nil || there {
				return err
			}
		}
		d := newSums()
		if _, err := recInsertReceiptTx(ctx, tx, ref, table, msg, e, false, d); err != nil {
			return err
		}
		return flushSums(ctx, tx, d)
	})
}

// recReceiptRow is one receipt's row in recDLRCols' order.
func recReceiptRow(msgID string, account int64, e recEntry, vendor int64, status string, final any, dep string, sell, buy int64, fx float64, at time.Time) []any {
	return []any{msgID, account, recReceiptSource(e.Source, e.Raw), rNZ(vendor), rStr(e.VendorMsgID), rNZi(e.Part), nil,
		rStr(status), rStr(e.Stat), rStr(rTrunc(e.ErrorCode, 32)), rStr(rTrunc(e.Raw, 512)), final, rStr(dep), recDoneDate(e.DoneAt),
		rNZ(sell), rNZ(buy), rDec(fx), rTimeText(at)}
}

// recInsertReceiptTx writes one receipt's row in tx, with what it adds to
// the receipts' summary (MySQL's insertReceiptTx). closes: the row is to
// close its message (final_of); a message closed already gets the row as
// recorded only (mine=false), which books nothing and keeps no price.
func recInsertReceiptTx(ctx context.Context, tx *sql.Tx, ref model.RecordRef, table string, msg recMessage, e recEntry, closes bool, d *sums) (mine bool, err error) {
	vendor := msg.vendor
	insert := func(final any, sell, buy int64, fx float64) error {
		status := e.Status
		if final != nil && e.FinalStatus != "" {
			status = e.FinalStatus
		}
		row := recReceiptRow(ref.MsgID, msg.account, e, vendor, status, final, deployment.ID(), sell, buy, fx, recArrivedAt(e))
		return recInsertReceipts(ctx, tx, table, [][]any{row})
	}
	if closes && !msg.rejected {
		err := insert(ref.MsgID, e.SellRateID, e.BuyRateID, e.FX)
		switch {
		case err == nil:
			mine = true
		case IsDuplicate(err):
			// Closed already by another receipt: this one is evidence. Only
			// the statement was undone; the transaction goes on.
			err = insert(nil, 0, 0, 0)
		}
		if err != nil {
			return false, err
		}
	} else {
		sell, buy := e.SellRateID, e.BuyRateID
		if e.RecordedOnly || msg.rejected && closes {
			sell, buy = 0, 0
		}
		if err := insert(nil, sell, buy, e.FX); err != nil {
			return false, err
		}
	}
	d.addReceipt(recArrivedAt(e), msg.account, vendor, e.Stat, e.ErrorCode)
	if mine && msg.row.msgID != "" {
		r := msg.row
		r.status = e.Status
		if fs := e.FinalStatus; fs != "" {
			r.status = fs
		}
		d.addMT(r.key, r.outcome())
		if err := closeOpen(ctx, tx, ref.MsgID); err != nil {
			return mine, err
		}
	}
	return mine, nil
}

// recSubmittedOr: sent once a vendor accepted a submission, else queued (a
// refused or unanswered try waits for its next; Saleem, 2 Nov 2026,
// OBS-RR-01). The closing receipt, when there is one, wins.
const recSubmittedOr = "CASE WHEN EXISTS (SELECT 1 FROM {sub} AS s WHERE s.msg_id = m.msg_id AND s.result = 'accepted') THEN N'sent' ELSE N'queued' END"

var (
	stRecState = RecordStmts.Add("record.state", "SELECT COALESCE(f.status, "+recSubmittedOr+") FROM {mt} AS m LEFT JOIN {dlr} AS f ON f.final_of = m.msg_id WHERE m.msg_id = @p1", true)
	// MySQL's RecordState, with no receipt or submission table for that day:
	// the message alone, sent when it has a submission.
	stRecStateSub   = RecordStmts.Add("record.state.alone_sub", "SELECT "+recSubmittedOr+" FROM {mt} AS m WHERE m.msg_id = @p1", true)
	stRecStateAlone = RecordStmts.Add("record.state.alone", "SELECT N'queued' FROM {mt} AS m WHERE m.msg_id = @p1", true)
	stRecStateRej   = RecordStmts.Add("record.state.refused", "SELECT status FROM {rej} WHERE msg_id = @p1", true)
	stRecExists     = RecordStmts.Add("record.tables_exist", "SELECT COUNT(CASE WHEN OBJECT_ID(@p1, N'U') IS NOT NULL THEN 1 END), COUNT(CASE WHEN OBJECT_ID(@p2, N'U') IS NOT NULL THEN 1 END)", false)
)

// recTablesThere says whether two tables are there, in one round trip.
func recTablesThere(ctx context.Context, x Querier, a, b string) (bool, bool) {
	var ea, eb int
	if err := rRow(ctx, x, stRecExists, stRecExists.SQL, rText(a), rText(b)).Scan(&ea, &eb); err != nil {
		return false, false
	}
	return ea == 1, eb == 1
}

// RecordState says whether a message's record is written, and its status
// worked out (its final receipt, else sent with a vendor answer, else
// queued; a refusal is its own status).
func (s *RecordStore) RecordState(ctx context.Context, ref model.RecordRef) (status string, found bool, err error) {
	var st sql.NullString
	mt, sub, dlr := recMT(ref), recSub(ref), recDLR(ref)
	err = rRow(ctx, s.db, stRecState, rOn(stRecState.SQL, "mt", mt, "sub", sub, "dlr", dlr), Varchar(ref.MsgID)).Scan(&st)
	if MissingTable(err) {
		// No receipt or submission table yet for that day: the message,
		// alone (as on MySQL, a receipt table without a submission table is
		// not read here).
		if subThere, _ := recTablesThere(ctx, s.db, sub, sub); subThere {
			err = rRow(ctx, s.db, stRecStateSub, rOn(stRecStateSub.SQL, "mt", mt, "sub", sub), Varchar(ref.MsgID)).Scan(&st)
		} else {
			err = rRow(ctx, s.db, stRecStateAlone, rOn(stRecStateAlone.SQL, "mt", mt), Varchar(ref.MsgID)).Scan(&st)
		}
	}
	if err == sql.ErrNoRows || MissingTable(err) {
		err = rRow(ctx, s.db, stRecStateRej, rOn(stRecStateRej.SQL, "rej", recRej(ref)), Varchar(ref.MsgID)).Scan(&st)
	}
	if err == sql.ErrNoRows || MissingTable(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return st.String, true, nil
}

// CloseWithReceipt files a final receipt of a message and, when this receipt
// is the one that closes it, marks it so (final_of), in ONE transaction with
// the summaries. mine says this call closed it. A message with no record
// answers sql.ErrNoRows and nothing is written. [dlr.ReceiptCloser]
func (s *RecordStore) CloseWithReceipt(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode, entryJSON string, mayClose bool) (mine bool, err error) {
	return s.closeWithReceipt(ctx, ref, st, vendorMsgID, errorCode, entryJSON, mayClose, false)
}

// CloseWithReceiptOnce is CloseWithReceipt for a task that may be seen
// twice: a row of this receipt there already leaves everything as it is.
func (s *RecordStore) CloseWithReceiptOnce(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode, entryJSON string, mayClose bool) (mine bool, err error) {
	return s.closeWithReceipt(ctx, ref, st, vendorMsgID, errorCode, entryJSON, mayClose, true)
}

// recEntryOf is a receipt's row as the close reads it from its JSON.
func recEntryOf(entryJSON, vendorMsgID string, st model.Status) recEntry {
	var e recEntry
	_ = json.Unmarshal([]byte(entryJSON), &e)
	if e.VendorMsgID == "" {
		e.VendorMsgID = vendorMsgID
	}
	if e.Status == "" {
		e.Status = string(st)
	}
	if string(st) != e.Status && st.IsTerminal() {
		e.FinalStatus = string(st) // a per-vendor rule renamed the outcome
	}
	return e
}

func (s *RecordStore) closeWithReceipt(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode, entryJSON string, mayClose, once bool) (mine bool, err error) {
	e := recEntryOf(entryJSON, vendorMsgID, st)
	table := recDLR(ref)
	if err := s.ensureRecords(ctx, table); err != nil {
		return false, err
	}
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		mine = false
		msg, err := recReadMessage(ctx, tx, ref)
		if err != nil {
			return err
		}
		if !msg.found {
			return sql.ErrNoRows
		}
		if once {
			if there, err := recThere(ctx, tx, table, ref, e); err != nil || there {
				return err
			}
		}
		closes := mayClose && st.IsTerminal()
		e := e
		e.RecordedOnly = !closes
		d := newSums()
		if mine, err = recInsertReceiptTx(ctx, tx, ref, table, msg, e, closes, d); err != nil {
			return err
		}
		return flushSums(ctx, tx, d)
	})
	return mine, s.forgetIfMissing(err, table)
}

var stRecOpenBill = RecordStmts.Add("record.open.bill", "SELECT TOP (1) bill FROM open_msgs WHERE msg_id = @p1", true)

// BillNote is what a message's final receipt books from: its charge mark,
// kept with it in open_msgs while it waits (hot/0074). The logs keep no money
// (issue 16); with Redis the note travels with the receipt facts and
// open_msgs is empty. Without Redis this was "" for every message, so no
// receipt fee was ever booked (BUG-RR-02, 2 Nov 2026).
func (s *RecordStore) BillNote(ctx context.Context, ref model.RecordRef) (string, error) {
	if !dal.OpenMessagesInSQL() || ref.MsgID == "" {
		return "", nil
	}
	var bill sql.NullString
	err := rRow(ctx, s.db, stRecOpenBill, stRecOpenBill.SQL, Varchar(ref.MsgID)).Scan(&bill)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return bill.String, nil
}

// ---- the status pull ----

// recStatusText is a message's status as its client is told it (MySQL's
// statusSelect): the first final answer the account's policy gave in the
// vendor's place, else its final receipt, else sent with a submission, else
// queued. withDLR/withSub: the day has that table.
func recStatusText(withDLR, withSub bool) string {
	submitted, vendorID := "N'queued'", "CAST(NULL AS NVARCHAR(64))"
	if withSub {
		submitted = recSubmittedOr
		vendorID = "(SELECT TOP (1) s.vendor_msg_id FROM {sub} AS s WHERE s.msg_id = m.msg_id AND s.result = 'accepted' ORDER BY s.attempt DESC, s.part)"
	}
	status := submitted
	from := " FROM {mt} AS m"
	if withDLR {
		status = "COALESCE((SELECT TOP (1) p.status FROM {dlr} AS p WHERE p.msg_id = m.msg_id AND p.raw LIKE N'policy:%'" +
			" AND p.status IN (N'delivered', N'failed', N'expired') ORDER BY p.id), f.status, " + submitted + ")"
		from += " LEFT JOIN {dlr} AS f ON f.final_of = m.msg_id"
	}
	return "SELECT m.msg_id AS msg_id, m.request_id AS request_id, " + status + " AS status, " + vendorID +
		" AS vendor_msg_id, m.client_ref AS client_ref, m.account_id AS account_id" + from
}

var (
	stRecStatus         = RecordStmts.Add("record.status", recStatusText(true, true)+" WHERE m.msg_id = @p1", true)
	stRecStatusNoSub    = RecordStmts.Add("record.status.no_sub", recStatusText(true, false)+" WHERE m.msg_id = @p1", true)
	stRecStatusNoDLR    = RecordStmts.Add("record.status.no_dlr", recStatusText(false, true)+" WHERE m.msg_id = @p1", true)
	stRecStatusAlone    = RecordStmts.Add("record.status.alone", recStatusText(false, false)+" WHERE m.msg_id = @p1", true)
	stRecStatusByRef    = RecordStmts.Add("record.status.by_reference", "SELECT TOP (1) t.msg_id, t.request_id, t.status, t.vendor_msg_id, t.client_ref, t.account_id FROM ("+recStatusText(true, true)+" WHERE m.account_id = @a AND m.client_ref = @r) AS t ORDER BY t.msg_id DESC", false)
	stRecCompanyTables  = RecordStmts.Add("record.company_tables", "SELECT TOP (5000) name FROM sys.tables WHERE name LIKE @p1 OR name LIKE @p2 OR name LIKE @p3", false)
	stRecClientRefs     = RecordStmts.Add("record.client_refs", "SELECT TOP (500) t.msg_id, t.client_ref FROM "+rIDsFrom+"{mt} AS t ON t.msg_id = j.v WHERE t.client_ref IS NOT NULL", true)
	recStatusDays       = 90
	recStatusPerStmtMax = 31
)

// recStatusStmt is the status read for a day with the tables it has.
func recStatusStmt(withDLR, withSub bool) (text string, st interface{ Observe(time.Duration, int64) }) {
	switch {
	case withDLR && withSub:
		return stRecStatus.SQL, stRecStatus
	case withDLR:
		return stRecStatusNoSub.SQL, stRecStatusNoSub
	case withSub:
		return stRecStatusNoDLR.SQL, stRecStatusNoDLR
	}
	return stRecStatusAlone.SQL, stRecStatusAlone
}

// StatusByID resolves a message's current status from its record, on the
// read instance when there is one. ok=false when the id is unknown.
func (s *RecordStore) StatusByID(ctx context.Context, ref model.RecordRef) (dal.MsgStatus, bool, error) {
	return s.statusByID(ctx, s.old(), ref)
}

// StatusByIDFresh is StatusByID read where the record is written.
func (s *RecordStore) StatusByIDFresh(ctx context.Context, ref model.RecordRef) (dal.MsgStatus, bool, error) {
	return s.statusByID(ctx, s.db, ref)
}

func (s *RecordStore) statusByID(ctx context.Context, db *sql.DB, ref model.RecordRef) (dal.MsgStatus, bool, error) {
	mt, sub, dlr := recMT(ref), recSub(ref), recDLR(ref)
	m, ok, err := recScanStatus(rRow(ctx, db, stRecStatus, rOn(stRecStatus.SQL, "mt", mt, "sub", sub, "dlr", dlr), Varchar(ref.MsgID)))
	if MissingTable(err) {
		withDLR, withSub := recTablesThere(ctx, db, dlr, sub)
		text, st := recStatusStmt(withDLR, withSub)
		start := time.Now()
		m, ok, err = recScanStatus(db.QueryRowContext(ctx, rOn(text, "mt", mt, "sub", sub, "dlr", dlr), Varchar(ref.MsgID)))
		st.Observe(time.Since(start), 1)
	}
	if err != nil && !MissingTable(err) {
		return dal.MsgStatus{}, false, err
	}
	return m, ok, nil
}

// recScanStatus reads one status row; ok=false when there is none.
func recScanStatus(row interface{ Scan(...any) error }) (dal.MsgStatus, bool, error) {
	var m dal.MsgStatus
	var req, vend, ref sql.NullString
	err := row.Scan(&m.MsgID, &req, &m.Status, &vend, &ref, &m.AccountID)
	if err == sql.ErrNoRows {
		return dal.MsgStatus{}, false, nil
	}
	if err != nil {
		return dal.MsgStatus{}, false, err
	}
	m.MsgID = rTrim(m.MsgID)
	m.RequestID, m.VendorMsgID, m.Reference = req.String, vend.String, ref.String
	return m, true, nil
}

// StatusByReference looks a message up by the id the CLIENT gave it, in its
// company's tables of the last days, newest first, scoped to the account.
// The days that have a table are asked in ONE statement, a branch a day.
func (s *RecordStore) StatusByReference(ctx context.Context, accountID, companyID int64, ref string) (dal.MsgStatus, bool, error) {
	db := s.old()
	have, err := s.companyTables(ctx, db, companyID)
	if err != nil {
		return dal.MsgStatus{}, false, err
	}
	oldest := billingstore.DayStart(time.Now(), -recStatusDays)
	var mts []string
	for name := range have {
		rt, ok := billingstore.ParseRecordTable(name)
		if !ok || rt.Kind != billingstore.KindMT || rt.CompanyID != max(companyID, 0) || rt.Date.Before(oldest) {
			continue
		}
		mts = append(mts, name)
	}
	// The day is the name's end: newest first.
	sort.Sort(sort.Reverse(sort.StringSlice(mts)))
	for from := 0; from < len(mts); from += recStatusPerStmtMax {
		part := mts[from:min(from+recStatusPerStmtMax, len(mts))]
		branches := make([]string, len(part))
		for i, mt := range part {
			dlr := billingstore.SwapKind(mt, billingstore.KindDLR)
			sub := billingstore.SwapKind(mt, billingstore.KindSub)
			branches[i] = rOn(recStatusText(have[dlr], have[sub]), "mt", mt, "sub", sub, "dlr", dlr) +
				" WHERE m.account_id = @a AND m.client_ref = @r"
		}
		text := "SELECT TOP (1) t.msg_id, t.request_id, t.status, t.vendor_msg_id, t.client_ref, t.account_id FROM (" +
			strings.Join(branches, " UNION ALL ") + ") AS t ORDER BY t.msg_id DESC"
		start := time.Now()
		m, ok, err := recScanStatus(db.QueryRowContext(ctx, text, sql.Named("a", accountID), sql.Named("r", rText(ref))))
		stRecStatusByRef.Observe(time.Since(start), 1)
		if err != nil || ok {
			return m, ok, err
		}
	}
	return dal.MsgStatus{}, false, nil
}

// companyTables lists one company's mt_, sub_ and dlr_ tables, as a set.
func (s *RecordStore) companyTables(ctx context.Context, db *sql.DB, companyID int64) (map[string]bool, error) {
	if companyID < 0 {
		companyID = 0
	}
	like := func(kind string) any {
		return rText(strings.ReplaceAll(kind, "_", "[_]") + "[_]" + strconv.FormatInt(companyID, 10) + "[_]%")
	}
	rows, err := rQuery(ctx, db, stRecCompanyTables, stRecCompanyTables.SQL,
		like(billingstore.KindMT), like(billingstore.KindSub), like(billingstore.KindDLR))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}

// ClientRefsFor maps our message ids to the ids their CLIENTS gave them, one
// query a table.
func (s *RecordStore) ClientRefsFor(ctx context.Context, refs []model.RecordRef) (map[string]string, error) {
	byTable := map[string][]string{}
	var order []string
	for _, r := range refs {
		t := recMT(r)
		if _, ok := byTable[t]; !ok {
			order = append(order, t)
		}
		byTable[t] = append(byTable[t], r.MsgID)
	}
	out := map[string]string{}
	for _, table := range order {
		ids := byTable[table]
		for from := 0; from < len(ids); from += rChunk {
			rows, err := rQuery(ctx, s.db, stRecClientRefs, rOn(stRecClientRefs.SQL, "mt", table), rIDs(ids[from:min(from+rChunk, len(ids))]))
			if MissingTable(err) {
				break
			}
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				var id, ref string
				if err := rows.Scan(&id, &ref); err != nil {
					rows.Close()
					return nil, err
				}
				out[rTrim(id)] = ref
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}
