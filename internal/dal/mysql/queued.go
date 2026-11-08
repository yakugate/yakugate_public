package mysql

// Finding one waiting message for GET /messages/{id} (QA BUG-YK-EP-016).
//
// A message is recorded only when a vendor takes it, or when it finally fails
// or expires. Until then it lives in queue_<vendor> and nowhere else, so a
// status lookup found nothing and answered 404, although the published
// contract promises QUEUED. Writing a record at acceptance would cost a write
// per message on the 4,000 msg/s path; looking in the queue on demand costs
// nothing until somebody asks.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// FindQueued looks for accountID's message msgID in the named vendors' queues
// (store.QueuedFinder). Each queue is read through its msg_id index, never
// scanned; another account's row is filtered out in the same statement. The
// caller keeps the list short (the vendors the account's own rules name).
//
// Two reads: which of the queue tables exist (a vendor that never had traffic
// has none yet, and one missing table would fail the whole statement — asked
// only for tables this store does not already know), then ONE statement over
// all of them. One statement reads one snapshot, so a
// message moved from one queue to another at that moment (the re-route mover
// copies and deletes in one transaction) is found in exactly one of them —
// separate reads could miss it in both.
func (s *Store) FindQueued(ctx context.Context, msgID string, accountID int64, vendors []string) (model.Message, bool, error) {
	if msgID == "" || len(vendors) == 0 {
		return model.Message{}, false, nil
	}
	cidOf := map[string]string{} // queue table -> the vendor cid it was named by
	var tables []string
	for _, v := range vendors {
		if v == "" {
			continue
		}
		t := queueTable(v)
		if _, dup := cidOf[t]; dup {
			continue
		}
		cidOf[t] = v
		tables = append(tables, t)
	}
	if len(tables) == 0 {
		return model.Message{}, false, nil
	}
	// Which of them exist. A table this store created or has already seen is
	// known to (s.ensured, the memo Enqueue keeps), so only the others are
	// asked of information_schema: once an account's queues are known, a
	// lookup is ONE statement, not two (review of BUG-YK-EP-016 — the
	// information_schema read cost about 1 ms on every miss). A known table
	// dropped since is caught below and forgotten again.
	var unknown []any
	for _, t := range tables {
		if _, ok := s.ensured.Load(t); !ok {
			unknown = append(unknown, t)
		}
	}
	if len(unknown) > 0 {
		rows, err := s.db.QueryContext(ctx,
			"SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name IN ("+placeholders(len(unknown))+")", unknown...)
		if err != nil {
			return model.Message{}, false, err
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return model.Message{}, false, err
			}
			s.ensured.Store(strings.ToLower(name), true)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return model.Message{}, false, err
		}
	}
	var present []string
	for _, t := range tables {
		if _, ok := s.ensured.Load(t); ok {
			present = append(present, t)
		}
	}
	if len(present) == 0 {
		return model.Message{}, false, nil
	}
	m, ok, err := s.findQueuedIn(ctx, msgID, accountID, present, cidOf)
	if err != nil && isNoSuchTable(err) {
		// A table dropped since it was seen (a vendor deleted): read the
		// queues one at a time, pass over the missing one and forget it, so
		// the next lookup asks again and the next Enqueue creates it again.
		for _, t := range present {
			m, ok, err = s.findQueuedIn(ctx, msgID, accountID, []string{t}, cidOf)
			if err != nil && isNoSuchTable(err) {
				s.ensured.Delete(t)
				continue
			}
			if err != nil || ok {
				return m, ok, err
			}
		}
		return model.Message{}, false, nil
	}
	return m, ok, err
}

// findQueuedIn reads msgID from the given queue tables in one statement.
// q_wait is the seconds until the queue offers it again, worked out on the
// SQL clock: pick_at is written NOW(3)-relative, and reading it back as
// a timestamp would mix the session's clock with ours.
func (s *Store) findQueuedIn(ctx context.Context, msgID string, accountID int64, tables []string, cidOf map[string]string) (model.Message, bool, error) {
	parts := make([]string, len(tables))
	args := make([]any, 0, 3*len(tables))
	for i, t := range tables {
		parts[i] = fmt.Sprintf(`(SELECT %s, ? AS q_vendor, COALESCE(TIMESTAMPDIFF(SECOND, NOW(3), pick_at), 0) AS q_wait
        FROM %s WHERE msg_id=? AND account_id=? LIMIT 1)`, queueSelectCols, t)
		args = append(args, cidOf[t], msgID, accountID)
	}
	rows, err := s.db.QueryContext(ctx, strings.Join(parts, " UNION ALL ")+" LIMIT 1", args...)
	if err != nil {
		return model.Message{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return model.Message{}, false, rows.Err()
	}
	var (
		cid  string
		wait int64
	)
	m, _, err := scanQueueRow(rows, "", &cid, &wait)
	if err != nil {
		return model.Message{}, false, err
	}
	m.Vendor = cid
	if wait > 0 {
		m.NextAttemptAt = time.Now().Add(time.Duration(wait) * time.Second)
	}
	return m, true, nil
}
