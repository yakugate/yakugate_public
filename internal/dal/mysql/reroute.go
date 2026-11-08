package mysql

// Moving waiting messages off a vendor that went down (PO 2026-09-14,
// routing rules review): "move immediately if there is any next route, else
// wait and expire". The route is chosen by the caller (pipeline.Rerouter);
// this file only reads candidates and moves one row safely.

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queueflow"
)

// PeekMovable returns up to n messages from one vendor's queue that are ready
// to send now — not held by a sender, not scheduled for later — after the
// internal id `after`, with the last id seen for the next call. Nothing is
// locked or leased: looking must not push a message into retry backoff or
// count an attempt, since most looks end in "no other route, wait".
// MoveQueued re-checks under a lock before anything moves.
//
// Parts of a multi-part group are left out: they belong together and wait
// where they are.
func (s *Store) PeekMovable(ctx context.Context, vendor string, after int64, n int) ([]model.Message, int64, error) {
	t := queueTable(vendor)
	// Only this deployment's messages and the shared ones (D-121): another
	// deployment moves its own, when it is back.
	own, ownArgs := s.ownerCond()
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT %s FROM %s
		WHERE %s AND COALESCE(udh_total,0) <= 1 AND id > ?%s
		ORDER BY id LIMIT ?`, queueSelectCols, t, readyCond, own), append(append([]any{after}, ownArgs...), n)...)
	if err != nil {
		if isNoSuchTable(err) {
			return nil, after, nil
		}
		return nil, after, err
	}
	defer rows.Close()
	var out []model.Message
	last := after
	for rows.Next() {
		m, id, err := scanQueueRow(rows, vendor)
		if err != nil {
			return out, last, err
		}
		out = append(out, m)
		last = id
	}
	return out, last, rows.Err()
}

// moveCols is every column a queue row carries (the Enqueue list). The row is
// copied column by column, so what the caller does not change — validity,
// attempts, receipt wishes, bind slot, template, when it was queued — arrives
// at the new vendor exactly as it was.
const moveCols = `msg_id,trace_id,request_id,account_id,username,vendor_id,route_id,sender,destination,body,encoding,segments,
	udh_ref,udh_total,udh_seq,gsm7,template_id,channel,ap1,ap2,apg1,apg2,transport,ingress,mcc,mnc,status,priority,
	idempotency_key,valid_until,attempts,no_receipt,failure_receipt_only,want_intermediate_receipt,bind_slot,mask_body,
	spam_flag,orig_sender,orig_destination,orig_body,next_attempt_at,created_at,hlr_refreshed,tried_vendors,
	campaign_id,campaign_hash,code_reroute,sender_by,client_sender,send_round,route_level,deployment_id,billing_model,queue_owner,quote,
	company_id,hlr_source,hlr_ported,error_text,ip_address,shortcode,pick_at`

// MoveQueued moves one waiting message from vendor `from` to m.Vendor's queue
// in ONE transaction: the row is taken only while it is still waiting and no
// sender holds it, copied with the new route's fields (vendor, route,
// transport, and the addresses, text and part plan the new route produced),
// and deleted from the old queue. The vendor it leaves is added to the row's
// tried_vendors. moved=false means a sender took it, or it
// was sent, expired or moved meanwhile — nothing was changed.
//
// The queue it moves to may be a clone still made with 50-character ap
// columns: the move is then undone, that table widened, and made once more
// (apwide.go, QA templates API-013).
func (s *Store) MoveQueued(ctx context.Context, from string, m model.Message) (moved bool, err error) {
	moved, err = s.moveQueued(ctx, from, m)
	if isAPTooLong(err) && s.widenAP(ctx, queueTable(m.Vendor), "queue_template") {
		return s.moveQueued(ctx, from, m)
	}
	return moved, err
}

func (s *Store) moveQueued(ctx context.Context, from string, m model.Message) (moved bool, err error) {
	src, dst := queueTable(from), queueTable(m.Vendor)
	if src == dst {
		return false, nil
	}
	if err := s.ensureCloneTable(ctx, dst, "queue_template"); err != nil {
		return false, err
	}
	// READ COMMITTED, as a claim (queuelocks.go), and again when MySQL
	// makes it a lock victim.
	err = s.queueTx(ctx, func(tx *sql.Tx) error {
		moved = false
		var id int64
		err := tx.QueryRowContext(ctx, fmt.Sprintf(`SELECT id FROM %s
		WHERE msg_id=? AND %s
		LIMIT 1 FOR UPDATE SKIP LOCKED`, src, readyCond), m.ID).Scan(&id)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		sel := `msg_id,trace_id,request_id,account_id,username,?,?,?,?,?,?,?,
	udh_ref,udh_total,udh_seq,gsm7,template_id,channel,ap1,ap2,apg1,apg2,?,ingress,mcc,mnc,status,priority,
	idempotency_key,valid_until,attempts,no_receipt,failure_receipt_only,want_intermediate_receipt,bind_slot,mask_body,
	spam_flag,?,?,?,next_attempt_at,created_at,hlr_refreshed,
	JSON_ARRAY_APPEND(COALESCE(tried_vendors, JSON_ARRAY()), '$', ?),
	campaign_id,campaign_hash,code_reroute,sender_by,client_sender,send_round,?,deployment_id,billing_model,queue_owner,quote,
	company_id,hlr_source,hlr_ported,error_text,ip_address,'',NOW(3)`
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s WHERE id=?`, dst, moveCols, sel, src),
			m.VendorID, nz(m.RouteID), m.Sender, m.Destination, m.Body, orStr(m.Encoding, "gsm7"), m.Segments,
			string(m.Transport),
			nsStr(m.OrigSender), nsStr(m.OrigDestination), nsStr(m.OrigBody),
			from,         // tried_vendors (queue/0018): which vendor it was moved off
			m.RouteLevel, // the level of its rule it moves to (queue/0035)
			id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id=?`, src), id); err != nil {
			return err
		}
		moved = true
		return nil
	})
	if err != nil || !moved {
		return false, err
	}
	queueflow.Out(queueflow.MT, from, 1) // it left one vendor's queue for another's (D-127)
	queueflow.In(queueflow.MT, m.Vendor, 1)
	return true, nil
}
