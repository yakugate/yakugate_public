package mysql

// A message's status from its records: reading the message a receipt is
// about, moving its status, and the facts a receipt is matched with.

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/yakugate/yakugate/internal/model"
)

// readMessage reads a message's record for a receipt, in tx: its MT row,
// else its refusal (a refused duplicate's receipt row hangs on it).
func readMessage(ctx context.Context, x dbtx, ref model.RecordRef) (messageOf, error) {
	got, err := readMTRows(ctx, x, mtOf(ref), []string{ref.MsgID}, false)
	if err != nil && !isNoSuchTable(err) {
		return messageOf{}, err
	}
	if r, ok := got[ref.MsgID]; ok {
		return messageOf{found: true, account: r.key.account, vendor: r.key.vendor, row: r}, nil
	}
	var m messageOf
	err = x.QueryRowContext(ctx, "SELECT COALESCE(account_id,0), COALESCE(vendor_id,0) FROM "+rejectsOf(ref)+" WHERE msg_id=?", ref.MsgID).
		Scan(&m.account, &m.vendor)
	if err == sql.ErrNoRows || isNoSuchTable(err) {
		return messageOf{}, nil
	}
	if err != nil {
		return messageOf{}, err
	}
	m.found, m.rejected = true, true
	return m, nil
}

// UpdateStatus closes a message with a status the gateway decided (a receipt
// with no row of its own: a store's older callers). A status that is not
// final changes nothing — a message on the log is sent. A final one is filed
// as a receipt row that closes the message, if nothing closed it before.
func (s *Store) UpdateStatus(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode string) error {
	if !st.IsTerminal() {
		return nil
	}
	e := rawDLREntry{VendorMsgID: vendorMsgID, Status: string(st), Stat: statForStatus(string(st)), ErrorCode: errorCode}
	b, _ := json.Marshal(e)
	_, err := s.CloseWithReceipt(ctx, ref, st, vendorMsgID, errorCode, string(b), true)
	if err == sql.ErrNoRows {
		return nil // no record yet: nothing to close
	}
	return err
}

// FinalizeOnce gives a message its final status only if it has none yet, and
// says whether THIS call was the one that did (dlr.Finalizer): UpdateStatus
// that answers.
func (s *Store) FinalizeOnce(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode string) (bool, error) {
	e := rawDLREntry{VendorMsgID: vendorMsgID, Status: string(st), Stat: statForStatus(string(st)), ErrorCode: errorCode}
	b, _ := json.Marshal(e)
	mine, err := s.CloseWithReceipt(ctx, ref, st, vendorMsgID, errorCode, string(b), true)
	if err == sql.ErrNoRows {
		return true, nil // no record yet: nothing was closed before
	}
	return mine, err
}

// WantsIntermediate answers whether the CLIENT asked to hear about progress
// on this message — registered_delivery bit 4 for an SMPP submit, always for
// an HTTP one (hot/0045). A message not found answers yes.
func (s *Store) WantsIntermediate(ctx context.Context, ref model.RecordRef) (bool, error) {
	var want sql.NullBool
	err := s.db.QueryRowContext(ctx, "SELECT want_intermediate FROM "+mtOf(ref)+" WHERE msg_id=?", ref.MsgID).Scan(&want)
	if err == sql.ErrNoRows || isNoSuchTable(err) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	return !want.Valid || want.Bool, nil
}

// MessageQueueOwner is whose queue a sent message came from (D-121): the
// deployment its receipts are queued for. "" = shared.
func (s *Store) MessageQueueOwner(ctx context.Context, ref model.RecordRef) (string, error) {
	var owner string
	err := s.db.QueryRowContext(ctx, "SELECT COALESCE(queue_owner,'') FROM "+mtOf(ref)+" WHERE msg_id=?", ref.MsgID).Scan(&owner)
	if err == sql.ErrNoRows || isNoSuchTable(err) {
		err = s.db.QueryRowContext(ctx, "SELECT COALESCE(queue_owner,'') FROM "+rejectsOf(ref)+" WHERE msg_id=?", ref.MsgID).Scan(&owner)
		if err == sql.ErrNoRows || isNoSuchTable(err) {
			return "", nil
		}
	}
	return owner, err
}

// MessageAddresses is a message's sender and destination, for its client's
// receipt (an SMPP client matches receipts on them).
func (s *Store) MessageAddresses(ctx context.Context, ref model.RecordRef) (string, string, error) {
	var sender, dest string
	err := s.db.QueryRowContext(ctx,
		"SELECT COALESCE(sender,''), COALESCE(destination,'') FROM "+mtOf(ref)+" WHERE msg_id=?", ref.MsgID).Scan(&sender, &dest)
	if err == sql.ErrNoRows || isNoSuchTable(err) {
		err = s.db.QueryRowContext(ctx,
			"SELECT COALESCE(sender_id,''), COALESCE(msisdn,'') FROM "+rejectsOf(ref)+" WHERE msg_id=?", ref.MsgID).Scan(&sender, &dest)
	}
	return sender, dest, err
}

// ReceiptFacts reads what a receipt's path needs of a message matched through
// its record: sender and destination, the deployment its client is on, its
// own table, whether the client asked for progress receipts, and whether it
// was sent to the vendor as several parts (each with its own vendor id: such
// a receipt is the part tracker's). [dlr.RecordFacts]
func (s *Store) ReceiptFacts(ctx context.Context, ref model.RecordRef) (src, dst, owner, table string, wantIntermediate, parts bool, err error) {
	table = mtOf(ref)
	var want sql.NullBool
	if err = s.db.QueryRowContext(ctx,
		"SELECT COALESCE(sender,''), COALESCE(destination,''), COALESCE(queue_owner,''), want_intermediate FROM "+
			table+" WHERE msg_id=?", ref.MsgID).Scan(&src, &dst, &owner, &want); err != nil {
		return "", "", "", "", false, false, err
	}
	wantIntermediate = !want.Valid || want.Bool
	// How many parts the vendor got is its submission's (sub_): mt_ holds
	// nothing about the vendor (the three-table design, 27 Oct 2026).
	var sent int
	err = s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(parts),0) FROM "+subOf(ref)+" WHERE msg_id=? AND result='accepted'",
		ref.MsgID).Scan(&sent)
	if err != nil && !isNoSuchTable(err) {
		return "", "", "", "", false, false, err
	}
	err = nil
	if sent > 1 {
		return src, dst, owner, table, wantIntermediate, true, nil
	}
	var n int
	if err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM concat_parts WHERE msg_id=?", ref.MsgID).Scan(&n); err != nil {
		return "", "", "", "", false, false, err
	}
	return src, dst, owner, table, wantIntermediate, n > 0, nil
}
