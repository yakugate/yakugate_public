package mysql

// The parts of a long message: the client part ids kept for its receipts,
// and each part's outcome marked together with the whole message's.

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// ClientPartIDsMany returns the part ids of the messages that have any, in
// one read per 500; a message that was not sent in parts is not in the answer.
func (s *Store) ClientPartIDsMany(ctx context.Context, msgIDs []string) (map[string][]string, error) {
	out := map[string][]string{}
	for from := 0; from < len(msgIDs); from += 500 {
		chunk := msgIDs[from:min(from+500, len(msgIDs))]
		args := make([]any, 0, len(chunk))
		for _, id := range chunk {
			args = append(args, id)
		}
		rows, err := s.db.QueryContext(ctx,
			`SELECT msg_id, part_ids FROM client_part_ids WHERE msg_id IN (?`+strings.Repeat(",?", len(chunk)-1)+`)`, args...)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var id, ids string
			if err := rows.Scan(&id, &ids); err == nil && ids != "" {
				out[id] = strings.Split(ids, ",")
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

// SaveClientPartIDs records the ids an SMPP client was given for the parts of
// one long message, under the joined message's id (part 1's), so each part can
// get its receipt (queue/0028, PO 2026-09-11). Written by smpp-server before
// the joined message runs the pipeline, so no receipt can arrive first.
func (s *Store) SaveClientPartIDs(ctx context.Context, msgID string, partIDs []string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO client_part_ids (msg_id, part_ids) VALUES (?, ?)
		 ON DUPLICATE KEY UPDATE part_ids=VALUES(part_ids)`, msgID, strings.Join(partIDs, ","))
	return err
}

// ClientPartIDs returns the part ids saved for msgID, or nil when it was not
// sent in parts (the ordinary case: no row).
func (s *Store) ClientPartIDs(ctx context.Context, msgID string) ([]string, error) {
	var ids string
	err := s.db.QueryRowContext(ctx, `SELECT part_ids FROM client_part_ids WHERE msg_id=?`, msgID).Scan(&ids)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return strings.Split(ids, ","), nil
}

// DropClientPartIDs removes the row once every part has its final receipt.
func (s *Store) DropClientPartIDs(ctx context.Context, msgID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM client_part_ids WHERE msg_id=?`, msgID)
	return err
}

// PruneClientPartIDs deletes rows older than maxAge — messages that never got a
// final receipt must not keep one for ever. Returns rows removed.
func (s *Store) PruneClientPartIDs(ctx context.Context, maxAge time.Duration, limit int) (int, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM client_part_ids WHERE created_at < DATE_SUB(NOW(), INTERVAL ? SECOND) LIMIT ?`,
		int(maxAge.Seconds()), limit)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// MarkPartAndAggregate records one part's receipt and returns the group's
// verdict. ready=true exactly once per group: the group's rows are locked
// for the duration (concurrent receipts for one message serialize here),
// and a group that was already final before this part arrived is not
// finalized again. Without both, three receipts landing in the same second
// each counted three delivered parts, two of them finalized the message,
// and the client got part 1's receipt twice (E2E run 2026-09-16, I-16).
// A repeat is still recorded on its part row (audit), it just does not
// move the message or push again.
func (s *Store) MarkPartAndAggregate(ctx context.Context, vendorID int64, vendorMsgID string,
	st model.Status) (agg model.Status, ready, ok bool, err error) {
	return s.markPart(ctx, vendorID, vendorMsgID, st, false)
}

// MarkFirstPartFinal is MarkPartAndAggregate for a vendor whose first final
// closes a long message (dlr_config.first_receipt_closes, Saleem 4 Nov
// 2026): ready for the group's first final part, with that part's status;
// every later part is recorded on its row only. Same group lock, so two
// parts' receipts at once close the message once.
func (s *Store) MarkFirstPartFinal(ctx context.Context, vendorID int64, vendorMsgID string,
	st model.Status) (agg model.Status, ready, ok bool, err error) {
	return s.markPart(ctx, vendorID, vendorMsgID, st, true)
}

func (s *Store) markPart(ctx context.Context, vendorID int64, vendorMsgID string,
	st model.Status, first bool) (agg model.Status, ready, ok bool, err error) {
	// InnoDB may still pick a victim when a third receipt joins at the wrong
	// moment; a victim simply goes again — the whole unit is one transaction.
	for attempt := 0; ; attempt++ {
		agg, ready, ok, err = s.markPartOnce(ctx, vendorID, vendorMsgID, st, first)
		if err == nil || attempt >= 3 || !isDeadlock(err) {
			return agg, ready, ok, err
		}
	}
}

func isDeadlock(err error) bool { return err != nil && strings.Contains(err.Error(), "Deadlock found") }

// isLockVictim: MySQL rolled this statement back because of other writers —
// a deadlock (1213) or a lock wait that ran out (1205). Nothing is wrong with
// the statement; it only has to be run again.
func isLockVictim(err error) bool {
	if err == nil {
		return false
	}
	m := err.Error()
	return strings.Contains(m, "Error 1213") || strings.Contains(m, "Deadlock found") ||
		strings.Contains(m, "Error 1205") || strings.Contains(m, "Lock wait timeout")
}

// againIfVictim runs one write, and runs it again (up to 8 times, a little
// later each time) when MySQL made it the victim of a deadlock or a lock
// wait. A victim statement is rolled back whole, so running it again is
// exactly the same write.
//
// Why it matters here (2026-10-06): the vendor roles record what they have
// SENT with these writes. A batch that gave up on a deadlock was "left for
// lease retry" — its messages were claimed again when their lease ended and sent to
// the vendor a second time. 391 messages went out twice in one test.
func againIfVictim(ctx context.Context, write func() error) error {
	return againIfVictimN(ctx, 8, write)
}

func (s *Store) markPartOnce(ctx context.Context, vendorID int64, vendorMsgID string,
	st model.Status, first bool) (agg model.Status, ready, ok bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, false, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit
	var msgID string
	var total int
	// The group key is immutable, so it is read without a lock: locking the
	// part's own row first and the group second is how two receipts deadlock
	// (each holding its part, each waiting for the other's). The group lock
	// below is taken in one statement, in index order, by every caller.
	if err = tx.QueryRowContext(ctx,
		"SELECT msg_id, parts_total FROM concat_parts WHERE vendor_id=? AND vendor_msg_id=?",
		vendorID, vendorMsgID).Scan(&msgID, &total); err == sql.ErrNoRows {
		return "", false, false, nil
	} else if err != nil {
		return "", false, false, err
	}
	// Lock every row of the group: a second receipt for the same message
	// waits here until this one has committed. The counts below are locking
	// reads too — under REPEATABLE READ a plain SELECT would keep showing the
	// snapshot taken before the wait and miss the receipt that committed
	// ahead of us, which is exactly the race being closed.
	if _, err = tx.ExecContext(ctx, "SELECT vendor_msg_id FROM concat_parts WHERE msg_id=? FOR UPDATE", msgID); err != nil {
		return "", false, true, err
	}
	before, err := aggregateOf(ctx, tx, msgID, total)
	if err != nil {
		return "", false, true, err
	}
	if first {
		// The first final of any part closed the message: closed before =
		// any part already has one.
		var finals int
		if err = tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM concat_parts WHERE msg_id=? AND status IN ('delivered','failed','expired') FOR UPDATE`,
			msgID).Scan(&finals); err != nil {
			return "", false, true, err
		}
		before = model.StatusSent
		if finals > 0 {
			before = model.StatusDelivered // any final: closed already
		}
	}
	if _, err = tx.ExecContext(ctx,
		"UPDATE concat_parts SET status=? WHERE vendor_id=? AND vendor_msg_id=?",
		string(st), vendorID, vendorMsgID); err != nil {
		return "", false, true, err
	}
	after, err := aggregateOf(ctx, tx, msgID, total)
	if err != nil {
		return "", false, true, err
	}
	if first {
		after = st // the message closes with this part's own status
	}
	if err = tx.Commit(); err != nil {
		return "", false, true, err
	}
	// Final before this part arrived: a late or repeated receipt. Recorded,
	// not re-finalized; the group's rows go once no part waits any more
	// (FoldConcatParts keeps them while one does).
	if before.IsTerminal() {
		if ferr := s.FoldConcatParts(ctx, model.RecordRef{MsgID: msgID}); ferr != nil {
			log.Printf("[store] fold the parts of %s after its last part's receipt failed (rows stay for the pruner): %v", msgID, ferr)
		}
		return after, false, true, nil
	}
	return after, after.IsTerminal(), true, nil
}

// aggregateOf is the group verdict from its part rows: one broken part breaks
// the message; every part delivered delivers it; otherwise it is still sent.
func aggregateOf(ctx context.Context, tx *sql.Tx, msgID string, total int) (model.Status, error) {
	var delivered, bad int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(status='delivered'),0), COALESCE(SUM(status IN ('failed','expired')),0)
		 FROM concat_parts WHERE msg_id=? FOR UPDATE`, msgID).Scan(&delivered, &bad); err != nil {
		return "", err
	}
	switch {
	case bad > 0:
		return model.StatusFailed, nil
	case delivered >= total:
		return model.StatusDelivered, nil
	default:
		return model.StatusSent, nil
	}
}
