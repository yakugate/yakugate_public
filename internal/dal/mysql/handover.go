package mysql

// Handing the queues over between MySQL and Redis (Saleem 2026-10-02, D-122).
// YAKU_QUEUE_BACKEND decides where the queues run; an install that switches
// has items waiting in the other place. This file is the MySQL half: the check
// the Redis half asks before moving an outgoing message here, and the move of
// everything waiting in MySQL — outgoing messages, client receipts, incoming
// messages (MO) and SMPP part ids — when the queues now run in Redis. Redis
// keeps no MySQL copy of a queue (Saleem: "if I set redis then no need to
// store in mysql"), so each row is deleted here once Redis has it.

import (
	"context"
	"database/sql"
	"strings"

	"github.com/yakugate/yakugate/internal/model"
)

// HasQueued reports whether a vendor's MySQL queue holds msgID. A queue table
// that does not exist holds nothing.
func (s *Store) HasQueued(ctx context.Context, vendor, msgID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+queueTable(vendor)+" WHERE msg_id=?", msgID).Scan(&n)
	if err != nil {
		if isNoSuchTable(err) {
			return false, nil
		}
		return false, err
	}
	return n > 0, nil
}

// HandInQueued adds an outgoing message handed over from Redis unless the
// vendor's MySQL queue already holds it (false). The table is created when the
// vendor has none yet.
func (s *Store) HandInQueued(ctx context.Context, m model.Message) (bool, error) {
	has, err := s.HasQueued(ctx, m.Vendor, m.ID)
	if err != nil || has {
		return false, err
	}
	return true, s.EnqueueTruth(ctx, m)
}

// RedisSink is where the queued items go when the queues run in Redis: the
// Redis queue store.
type RedisSink interface {
	// HandInQueued adds an outgoing message unless Redis already holds it
	// (queued or in flight); false = it was already there.
	HandInQueued(ctx context.Context, m model.Message) (bool, error)
	EnqueueClientDLR(ctx context.Context, d model.ClientDLR) error
	StageMO(ctx context.Context, m model.Message) error
	SaveClientPartIDs(ctx context.Context, msgID string, partIDs []string) error
}

// HandOverCounts says what a hand-over moved.
type HandOverCounts struct{ MT, Receipts, MO, PartIDs int }

// Any reports whether anything was moved.
func (c HandOverCounts) Any() bool { return c.MT+c.Receipts+c.MO+c.PartIDs > 0 }

const handOverPage = 500

// handOverQueue moves one vendor's queue table into sink — every deployment's
// rows, owners and lease times kept — deleting each row once sink has it.
// Counts the messages sink did not already hold.
func (s *Store) handOverQueue(ctx context.Context, sink RedisSink, vendor string) (int, error) {
	t := queueTable(vendor)
	moved := 0
	for {
		// Always from the start: each page is deleted as it goes.
		msgs, _, err := s.pendingMessages(ctx, vendor, 0, handOverPage, true)
		if err != nil || len(msgs) == 0 {
			return moved, err
		}
		for _, m := range msgs {
			added, err := sink.HandInQueued(ctx, m)
			if err != nil {
				return moved, err
			}
			if _, err := s.db.ExecContext(ctx, "DELETE FROM `"+t+"` WHERE msg_id=?", m.ID); err != nil {
				return moved, err
			}
			if added {
				moved++
			}
		}
	}
}

// HandOverTo moves every queued outgoing message, every waiting client
// receipt, every queued MO and every saved part-id list — every deployment's
// and the shared ones, owners kept — into sink, deleting each row once sink
// has it. Receipts given up and MO abandoned stay: they are the record of what
// a customer never got. An error stops where it is; the rest moves at the next
// start.
func (s *Store) HandOverTo(ctx context.Context, sink RedisSink) (HandOverCounts, error) {
	var c HandOverCounts
	vendors, err := s.QueueVendors(ctx)
	if err != nil {
		return c, err
	}
	for _, v := range vendors {
		n, err := s.handOverQueue(ctx, sink, v)
		c.MT += n
		if err != nil {
			return c, err
		}
	}
	for {
		rows, err := s.db.QueryContext(ctx, `SELECT id,account_id,client_msg_id,status,COALESCE(stat,''),COALESCE(receipt,''),
			COALESCE(bind_slot,0),COALESCE(src,''),COALESCE(dst,''),COALESCE(queue_owner,''),COALESCE(done_at_ms,0),COALESCE(msg_id,'')
			FROM client_dlr WHERE given_up_at IS NULL ORDER BY id LIMIT ?`, handOverPage)
		if err != nil {
			return c, err
		}
		var batch []model.ClientDLR
		for rows.Next() {
			var d model.ClientDLR
			var doneMS int64
			if err := rows.Scan(&d.ID, &d.AccountID, &d.ClientMsgID, &d.Status, &d.Stat, &d.Receipt,
				&d.BindSlot, &d.Src, &d.Dst, &d.Owner, &doneMS, &d.MsgID); err != nil {
				rows.Close()
				return c, err
			}
			d.DoneAt = timeOfMS(doneMS)
			batch = append(batch, d)
		}
		rows.Close()
		if len(batch) == 0 {
			break
		}
		for _, d := range batch {
			id := d.ID
			d.ID = 0
			if err := sink.EnqueueClientDLR(ctx, d); err != nil {
				return c, err
			}
			if _, err := s.db.ExecContext(ctx, "DELETE FROM client_dlr WHERE id=?", id); err != nil {
				return c, err
			}
			c.Receipts++
		}
	}
	for {
		rows, err := s.db.QueryContext(ctx, `SELECT msg_id,COALESCE(trace_id,''),vendor_id,account_id,COALESCE(route_id,0),
			sender,destination,body,encoding,segments,COALESCE(mcc,''),COALESCE(mnc,''),COALESCE(queue_owner,''),
			COALESCE(company_id,0)
			FROM mo_queue WHERE status='queued' ORDER BY id LIMIT ?`, handOverPage)
		if err != nil {
			return c, err
		}
		var batch []model.Message
		for rows.Next() {
			var m model.Message
			if err := rows.Scan(&m.ID, &m.TraceID, &m.VendorID, &m.AccountID, &m.RouteID, &m.Sender, &m.Destination,
				&m.Body, &m.Encoding, &m.Segments, &m.MCC, &m.MNC, &m.QueueOwner, &m.CompanyID); err != nil {
				rows.Close()
				return c, err
			}
			m.Direction = model.DirectionMO
			batch = append(batch, m)
		}
		rows.Close()
		if len(batch) == 0 {
			break
		}
		for _, m := range batch {
			if err := sink.StageMO(ctx, m); err != nil {
				return c, err
			}
			if _, err := s.db.ExecContext(ctx, "DELETE FROM mo_queue WHERE msg_id=?", m.ID); err != nil {
				return c, err
			}
			c.MO++
		}
	}
	for {
		rows, err := s.db.QueryContext(ctx, `SELECT msg_id, part_ids FROM client_part_ids ORDER BY msg_id LIMIT ?`, handOverPage)
		if err != nil {
			if isNoSuchTable(err) {
				break
			}
			return c, err
		}
		type pid struct{ id, parts string }
		var batch []pid
		for rows.Next() {
			var p pid
			var parts sql.NullString
			if err := rows.Scan(&p.id, &parts); err != nil {
				rows.Close()
				return c, err
			}
			p.parts = parts.String
			batch = append(batch, p)
		}
		rows.Close()
		if len(batch) == 0 {
			break
		}
		for _, p := range batch {
			if err := sink.SaveClientPartIDs(ctx, p.id, strings.Split(p.parts, ",")); err != nil {
				return c, err
			}
			if _, err := s.db.ExecContext(ctx, "DELETE FROM client_part_ids WHERE msg_id=?", p.id); err != nil {
				return c, err
			}
			c.PartIDs++
		}
	}
	return c, nil
}
