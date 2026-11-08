package mysql

// Message logs by company and DAY, written once (issues 12–17 and 25 of the
// design audit; Saleem and the PO, 22–26 Oct 2026). The design:
// products_docs/engineering/data-stores/storage-redesign-2026-10-27.md.
//
// A record is in <kind>_<company>_<YYYYMMDD> (billingstore.RecordTableName):
// mt_ for an outgoing message, written once at ACCEPTANCE with nothing about
// the vendor; sub_ for every answer a vendor gave (per part, per try, with
// the vendor, its id, its price); dlr_ for its receipts, the vendor's
// (source vendor) and the gateway's own (source system: a policy's answer,
// our ACCEPTD, the end of a message refused for good, out of tries or past
// its validity); mo_ for an incoming message and mo_out_ for what became of
// it, hlr_ for a lookup worth logging, rejects_ for a refusal at acceptance,
// orphans_0_ for what matched nothing (the three-table design, Saleem and
// the PO, 27 Oct 2026). The day is the message id's (a ULID); the company rides the
// message from acceptance (model.Message.CompanyID). Whoever asks about a
// message names it by id and company (model.RecordRef).
//
// No row is ever changed. A message's status is its final receipt — the dlr_
// row whose final_of is the message's id — else "sent" (submitted) when it
// has a sub_ row, else "queued" (enqueued); a repeated write
// reads first and inserts only what is not there. The summaries are counted
// by whoever inserts, in the same transaction.
//
// Messages still waiting for a receipt are found across companies through
// ONE place (Saleem 2026-10-14: "Redis if set, else MySQL, never both"): with
// Redis, the receipt facts in the main Redis (internal/receiptfacts); without
// it, open_msgs here. SetOpenMessagesInMySQL says which.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
)

// SetOpenMessagesInMySQL says where the messages waiting for a receipt are
// kept: true = open_msgs (no Redis), false = the receipt facts in Redis. The
// flag is the DAL's (dal.SetOpenMessagesInSQL, 30 Oct 2026): cmd/yakugate
// sets it here for every role.
func SetOpenMessagesInMySQL(on bool) { dal.SetOpenMessagesInSQL(on) }

// OpenMessagesInMySQL reports SetOpenMessagesInMySQL.
func OpenMessagesInMySQL() bool { return dal.OpenMessagesInSQL() }

// recTable is the table of a message's record of one kind.
func recTable(kind string, company int64, msgID string) string {
	return billingstore.TableOf(kind, company, msgID, time.Now())
}

func mtOf(ref model.RecordRef) string { return recTable(billingstore.KindMT, ref.CompanyID, ref.MsgID) }
func moOf(ref model.RecordRef) string { return recTable(billingstore.KindMO, ref.CompanyID, ref.MsgID) }
func dlrOf(ref model.RecordRef) string {
	return recTable(billingstore.KindDLR, ref.CompanyID, ref.MsgID)
}
func rejectsOf(ref model.RecordRef) string {
	return recTable(billingstore.KindRejects, ref.CompanyID, ref.MsgID)
}

// MTTableOf is the table a message's outgoing record is filed in: what the
// vendor roles put in a receipt's facts, so the receipt written later lands
// beside it.
func MTTableOf(m model.Message) string { return mtOf(m.Ref()) }

// ensureRecords makes a log table from its kind's template, once.
func (s *Store) ensureRecords(ctx context.Context, table string) error {
	tpl, ok := billingstore.Templates[billingstore.TableFamily(table)]
	if !ok {
		return fmt.Errorf("not a log table: %q", table)
	}
	return s.ensureCloneTable(ctx, table, tpl)
}

// finalStatus says a status closes a message.
func finalStatus(st string) bool {
	switch st {
	case "delivered", "failed", "expired", "rejected":
		return true
	}
	return false
}

// isDupEntry is MySQL's "Duplicate entry" (1062): a unique key said no.
func isDupEntry(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Duplicate entry")
}

// errRaced is a write that met a row another writer inserted between its read
// and its insert: the whole write is made again, and reads it this time.
var errRaced = errors.New("a row this write meant to make was made meanwhile")

// noteOpen files in open_msgs the messages of a write that wait for a receipt,
// when they are kept in MySQL (no Redis): a message the vendor took whose
// vendor sends receipts waits there for its vendor's time.
func (s *Store) noteOpen(ctx context.Context, x dbtx, made []TerminalRecord) error {
	if !OpenMessagesInMySQL() {
		return nil
	}
	var open []TerminalRecord
	for _, r := range made {
		if r.Msg.Status == model.StatusSent && r.Msg.ReceiptWait() > 0 {
			open = append(open, r)
		}
	}
	if len(open) == 0 {
		return nil
	}
	vals := strings.TrimSuffix(strings.Repeat("(?,?,?,?,?,?,NOW(),DATE_ADD(NOW(), INTERVAL ? SECOND),?,?),", len(open)), ",")
	args := make([]any, 0, len(open)*10)
	for _, r := range open {
		m := r.Msg
		args = append(args, m.ID, m.CompanyID, m.AccountID, m.VendorID, nsStr(r.VendorMsgID), nsStr(m.Destination),
			int64(m.ReceiptWait()/time.Second), nsStr(m.QueueOwner), nsStr(chargeOf(m)))
	}
	_, err := x.ExecContext(ctx, "INSERT IGNORE INTO open_msgs (msg_id,company_id,account_id,vendor_id,vendor_msg_id,destination,sent_at,wait_until,queue_owner,bill) VALUES "+
		vals, args...)
	return err
}

// chargeOf is the charge mark a message's vendor role priced it with ("" =
// none): what its final receipt books from (hot/0074, BUG-RR-02).
func chargeOf(m model.Message) string {
	if m.Priced == nil {
		return ""
	}
	return m.Priced.Charge
}

// closeOpen removes messages from open_msgs: they are final.
func closeOpen(ctx context.Context, x dbtx, msgIDs ...string) error {
	if !OpenMessagesInMySQL() || len(msgIDs) == 0 {
		return nil
	}
	args := make([]any, len(msgIDs))
	for i, id := range msgIDs {
		args[i] = id
	}
	_, err := x.ExecContext(ctx, "DELETE FROM open_msgs WHERE msg_id IN ("+placeholders(len(msgIDs))+")", args...)
	return err
}

// ---- a message's status: its final receipt ----

// messageOf is what a receipt needs of its message: its account and vendor,
// whether it is an MT row (key for its summary row) or a refusal at
// submission (final already), or neither (no record).
type messageOf struct {
	found, rejected bool
	account, vendor int64
	row             mtRow
}
