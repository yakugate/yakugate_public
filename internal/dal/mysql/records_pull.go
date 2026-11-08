package mysql

// The status pull: a client asking for a message's status, by our id or
// its own reference.

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/model"
)

// ---- the status pull ----

// statusSelect is a message's status as its client is told it: the first
// final answer the account's receipt policy gave in the vendor's place (a
// `policy:` receipt row), else its final receipt, else submitted (sent) when
// it has a submission (sub_), else enqueued (queued). The three-table design
// (Saleem, 27 Oct 2026): "if submission results are there then submitted, if
// dlr is there then dlr results, else if nothing there, enqueued". The
// vendor's id is its newest accepted submission's.
func statusSelect(mt, dlr, sub string) string {
	return "SELECT m.msg_id, m.request_id, COALESCE(" +
		"(SELECT p.status FROM " + dlr + " p WHERE p.msg_id = m.msg_id AND p.raw LIKE 'policy:%' AND p.status IN ('delivered','failed','expired') ORDER BY p.id LIMIT 1)," +
		" f.status, " + submittedOr(sub) + "), " + vendorIDOf(sub) + ", m.client_ref, m.account_id FROM " + mt + " m LEFT JOIN " + dlr + " f ON f.final_of = m.msg_id"
}

// statusAlone is statusSelect for a day with no receipt table yet.
func statusAlone(mt, sub string) string {
	return "SELECT m.msg_id, m.request_id, " + submittedOr(sub) + ", " + vendorIDOf(sub) + ", m.client_ref, m.account_id FROM " + mt + " m"
}

// submittedOr is "sent" when a vendor accepted a submission of message m in
// sub, else "queued"; always queued when sub is "" (no table). A message the
// vendor refused, or did not answer, is back in its queue for its next try:
// queued until a vendor takes it (Saleem, 2 Nov 2026, OBS-RR-01; it read
// sent from its first refused try). A refusal that ends it has its closing
// receipt, which wins.
func submittedOr(sub string) string {
	if sub == "" {
		return "'queued'"
	}
	return "IF(EXISTS(SELECT 1 FROM " + sub + " s WHERE s.msg_id = m.msg_id AND s.result = 'accepted'), 'sent', 'queued')"
}

// vendorIDOf is the vendor's id for message m: its newest accepted
// submission's first part's.
func vendorIDOf(sub string) string {
	if sub == "" {
		return "NULL"
	}
	return "(SELECT s.vendor_msg_id FROM " + sub + " s WHERE s.msg_id = m.msg_id AND s.result = 'accepted' ORDER BY s.attempt DESC, s.part LIMIT 1)"
}

// subOf is a message's submissions table.
func subOf(ref model.RecordRef) string {
	return billingstore.SwapKind(mtOf(ref), billingstore.KindSub)
}

// subIfThere is a message's submissions table, "" when it does not exist.
func (s *Store) subIfThere(ctx context.Context, db *sql.DB, ref model.RecordRef) string {
	t := subOf(ref)
	if t == "" || !s.tableExists(ctx, db, t) {
		return ""
	}
	return t
}

// StatusByID resolves a message's current status from its record.
// ok=false when the id is unknown. It answers what the client was told when
// the account's receipt policy answered for the vendor (hot/0044): a status
// PULL is the client asking what happened to their message, and it must
// answer the same thing their receipt did.
//
// Read from the read instance when there is one: a status a client pulls may
// be a moment old. A message that is not found there is looked for again on
// the write instance (StatusByIDFresh) before the client is told it does not
// exist.
func (s *Store) StatusByID(ctx context.Context, ref model.RecordRef) (MsgStatus, bool, error) {
	return s.statusByID(ctx, s.old(), ref)
}

// StatusByIDFresh is StatusByID read where the record is written.
func (s *Store) StatusByIDFresh(ctx context.Context, ref model.RecordRef) (MsgStatus, bool, error) {
	return s.statusByID(ctx, s.db, ref)
}

func (s *Store) statusByID(ctx context.Context, db *sql.DB, ref model.RecordRef) (MsgStatus, bool, error) {
	m, ok, err := scanStatus(db.QueryRowContext(ctx, statusSelect(mtOf(ref), dlrOf(ref), subOf(ref))+" WHERE m.msg_id=?", ref.MsgID))
	if isNoSuchTable(err) {
		// No receipt (or submission) table for that day yet.
		sub := s.subIfThere(ctx, db, ref)
		if dlr := dlrOf(ref); s.tableExists(ctx, db, dlr) {
			m, ok, err = scanStatus(db.QueryRowContext(ctx, statusSelect(mtOf(ref), dlr, sub)+" WHERE m.msg_id=?", ref.MsgID))
		} else {
			m, ok, err = scanStatus(db.QueryRowContext(ctx, statusAlone(mtOf(ref), sub)+" WHERE m.msg_id=?", ref.MsgID))
		}
	}
	if err != nil && !isNoSuchTable(err) {
		return MsgStatus{}, false, err
	}
	return m, ok, nil
}

// scanStatus reads one status row; ok=false when there is none.
func scanStatus(row *sql.Row) (MsgStatus, bool, error) {
	var m MsgStatus
	var req, vend, ref sql.NullString
	err := row.Scan(&m.MsgID, &req, &m.Status, &vend, &ref, &m.AccountID)
	if err == sql.ErrNoRows {
		return MsgStatus{}, false, nil
	}
	if err != nil {
		return MsgStatus{}, false, err
	}
	m.RequestID, m.VendorMsgID, m.Reference = req.String, vend.String, ref.String
	return m, true, nil
}

// statusDays is how many days a lookup by the client's own reference reads
// back, newest first: what the hot store keeps by default.
const statusDays = 90

// StatusByReference looks a message up by the id the CLIENT gave it (PO
// 2026-09-06), in its company's tables of the last days. Newest first: a
// reference is deliberately NOT unique. Scoped to the account so one customer
// can never read another's message by guessing a common reference. The days
// that have a table are asked in ONE statement, a branch a day.
func (s *Store) StatusByReference(ctx context.Context, accountID, companyID int64, ref string) (MsgStatus, bool, error) {
	db := s.old()
	tables, err := s.companyTables(ctx, db, companyID, billingstore.KindMT, statusDays)
	if err != nil {
		return MsgStatus{}, false, err
	}
	for from := 0; from < len(tables); from += 31 {
		part := tables[from:min(from+31, len(tables))]
		var q strings.Builder
		args := []any{}
		for i, mt := range part {
			if i > 0 {
				q.WriteString(" UNION ALL ")
			}
			dlr := billingstore.SwapKind(mt, billingstore.KindDLR)
			sub := billingstore.SwapKind(mt, billingstore.KindSub)
			if sub != "" && !s.tableExists(ctx, db, sub) {
				sub = ""
			}
			if dlr != "" && s.tableExists(ctx, db, dlr) {
				q.WriteString("(" + statusSelect(mt, dlr, sub) + " WHERE m.account_id=? AND m.client_ref=?)")
			} else {
				q.WriteString("(" + statusAlone(mt, sub) + " WHERE m.account_id=? AND m.client_ref=?)")
			}
			args = append(args, accountID, ref)
		}
		m, ok, err := scanStatus(db.QueryRowContext(ctx, "SELECT * FROM ("+q.String()+") t ORDER BY 1 DESC LIMIT 1", args...))
		if err != nil || ok {
			return m, ok, err
		}
	}
	return MsgStatus{}, false, nil
}

// companyTables lists one company's log tables of one kind of the last days,
// newest first.
func (s *Store) companyTables(ctx context.Context, db *sql.DB, companyID int64, kind string, days int) ([]string, error) {
	if companyID < 0 {
		companyID = 0
	}
	prefix := fmt.Sprintf("%s\\_%d\\_%%", strings.ReplaceAll(kind, "_", "\\_"), companyID)
	rows, err := db.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE ?", prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	oldest := billingstore.DayStart(time.Now(), -days)
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		rt, ok := billingstore.ParseRecordTable(name)
		if !ok || rt.Kind != kind || rt.CompanyID != companyID || rt.Date.Before(oldest) {
			continue
		}
		out = append(out, name)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(out))) // the day is the name's end: newest first
	return out, rows.Err()
}

// tableExists says a table is there.
func (s *Store) tableExists(ctx context.Context, db *sql.DB, table string) bool {
	var n int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", table).Scan(&n)
	return err == nil && n > 0
}

// ClientRefsFor maps our message ids to the ids their CLIENTS gave them, one
// query a table. Used by the dlr_url poster to put `reference` in a receipt.
func (s *Store) ClientRefsFor(ctx context.Context, refs []model.RecordRef) (map[string]string, error) {
	byTable := map[string][]any{}
	for _, r := range refs {
		t := mtOf(r)
		byTable[t] = append(byTable[t], r.MsgID)
	}
	out := map[string]string{}
	for table, ids := range byTable {
		rows, err := s.db.QueryContext(ctx,
			"SELECT msg_id, client_ref FROM "+table+" WHERE client_ref IS NOT NULL AND msg_id IN ("+placeholders(len(ids))+")", ids...)
		if isNoSuchTable(err) {
			continue
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
			out[id] = ref
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
