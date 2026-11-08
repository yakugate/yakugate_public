package mssql

// Final receipts closed many at a time, without Redis, on SQL Server
// (mirrors internal/dal/mysql/receiptclose.go): CloseWithReceipts does what
// CloseWithReceipt does for each receipt of a batch, in ONE transaction a
// receipt table, so a summary row is updated once a batch, not once a
// receipt. The refusals a batch's messages may be are read in one statement
// (MySQL reads one a message).

import (
	"context"
	"database/sql"
	"errors"
	"sort"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
)

var stRecRefusedMany = RecordStmts.Add("record.receipt.close_many.refused", `SELECT TOP (100000) t.msg_id, COALESCE(t.account_id, 0), COALESCE(t.vendor_id, 0)
	FROM `+rIDsFrom+`{rej} AS t ON t.msg_id = j.v`, true)

// CloseWithReceipts is CloseWithReceipt for many final receipts (mayClose
// true for each), in the order given: a message's first receipt in the
// batch closes it when nothing closed it before, a later one is recorded
// only. A message with no record is Found=false. Tables are written one
// after another; err is the first error. [dlr.ManyCloser]
func (s *RecordStore) CloseWithReceipts(ctx context.Context, rs []model.ReceiptClose) (out []model.ReceiptClosed, err error) {
	out = make([]model.ReceiptClosed, len(rs))
	if len(rs) == 0 {
		return out, nil
	}
	byTable := map[string][]int{}
	for i, r := range rs {
		t := recDLR(r.Ref)
		byTable[t] = append(byTable[t], i)
	}
	tables := make([]string, 0, len(byTable))
	for t := range byTable {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	var first error
	for _, table := range tables {
		idx := byTable[table]
		var e error
		for try := 0; try < 3; try++ {
			e = s.closeReceiptTable(ctx, table, rs, idx, out)
			if !errors.Is(e, errRaced) {
				break
			}
		}
		if errors.Is(e, errRaced) {
			// Another writer closed one of these messages between our read
			// and our insert three times running: one at a time.
			e = nil
			for _, i := range idx {
				r := rs[i]
				mine, err := s.CloseWithReceipt(ctx, r.Ref, r.Status, r.VendorMsgID, r.ErrorCode, r.Entry, true)
				switch {
				case err == sql.ErrNoRows:
					out[i] = model.ReceiptClosed{}
				case err != nil:
					if e == nil {
						e = err
					}
				default:
					out[i] = model.ReceiptClosed{Found: true, Mine: mine}
				}
			}
		}
		if e != nil && first == nil {
			first = e
		}
	}
	return out, first
}

// closeReceiptTable writes rs[idx…] — all of one receipt table — in ONE
// transaction, as CloseWithReceipt writes each.
func (s *RecordStore) closeReceiptTable(ctx context.Context, table string, rs []model.ReceiptClose, idx []int, out []model.ReceiptClosed) error {
	if err := s.ensureRecords(ctx, table); err != nil {
		return err
	}
	es := make([]recEntry, len(idx))
	for k, i := range idx {
		r := rs[i]
		es[k] = recEntryOf(r.Entry, r.VendorMsgID, r.Status)
	}
	ref0 := rs[idx[0]].Ref
	mtTable := recMT(ref0)
	rejTable := billingstore.SwapKind(mtTable, billingstore.KindRejects)
	answers := make([]model.ReceiptClosed, len(idx))
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		ids := make([]string, 0, len(idx))
		seen := map[string]bool{}
		for _, i := range idx {
			if id := rs[i].Ref.MsgID; !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		mts, err := recReadMTRows(ctx, tx, mtTable, ids)
		if err != nil && !MissingTable(err) {
			return err
		}
		msgs := make(map[string]recMessage, len(ids))
		var refusedIDs []string
		for _, id := range ids {
			if r, ok := mts[id]; ok {
				msgs[id] = recMessage{found: true, account: r.key.account, vendor: r.key.vendor, row: r}
				continue
			}
			refusedIDs = append(refusedIDs, id)
		}
		// No MT row: a refusal at submission, else no record at all.
		if len(refusedIDs) > 0 {
			rows, err := rQuery(ctx, tx, stRecRefusedMany, rOn(stRecRefusedMany.SQL, "rej", rejTable), rIDs(refusedIDs))
			if err != nil && !MissingTable(err) {
				return err
			}
			if err == nil {
				for rows.Next() {
					var id string
					m := recMessage{found: true, rejected: true}
					if err := rows.Scan(&id, &m.account, &m.vendor); err != nil {
						rows.Close()
						return err
					}
					msgs[rTrim(id)] = m
				}
				err = rows.Err()
				rows.Close()
				if err != nil {
					return err
				}
			}
		}
		var closing []string
		for _, id := range ids {
			if m := msgs[id]; m.found && !m.rejected {
				closing = append(closing, id)
			}
		}
		closed, err := recFinalsIn(ctx, tx, table, closing)
		if err != nil {
			return err
		}
		d := newSums()
		var vals [][]any
		var opened []string
		for k, i := range idx {
			r, e := rs[i], es[k]
			msg := msgs[r.Ref.MsgID]
			answers[k] = model.ReceiptClosed{Found: msg.found}
			if !msg.found {
				continue // nothing is written for a message with no record
			}
			closes := r.Status.IsTerminal()
			e.RecordedOnly = !closes
			var final any
			sell, buy, fx := e.SellRateID, e.BuyRateID, e.FX
			mine := false
			if closes && !msg.rejected {
				if !closed[r.Ref.MsgID] {
					final, mine = r.Ref.MsgID, true
					closed[r.Ref.MsgID] = true
				} else {
					sell, buy, fx = 0, 0, 0 // closed already: this one is evidence
				}
			} else if e.RecordedOnly || msg.rejected && closes {
				sell, buy = 0, 0
			}
			answers[k].Mine = mine
			status := e.Status
			if final != nil && e.FinalStatus != "" {
				status = e.FinalStatus
			}
			vals = append(vals, recReceiptRow(r.Ref.MsgID, msg.account, e, msg.vendor, status, final, deployment.ID(), sell, buy, fx, recArrivedAt(e)))
			d.addReceipt(recArrivedAt(e), msg.account, msg.vendor, e.Stat, e.ErrorCode)
			if mine && msg.row.msgID != "" {
				row := msg.row
				row.status = e.Status
				if fs := e.FinalStatus; fs != "" {
					row.status = fs
				}
				d.addMT(row.key, row.outcome())
				opened = append(opened, r.Ref.MsgID)
			}
		}
		if len(vals) > 0 {
			if err := recInsertReceipts(ctx, tx, table, vals); err != nil {
				if IsDuplicate(err) {
					return errRaced // another writer closed one of them meanwhile: read again
				}
				return err
			}
		}
		if err := closeOpen(ctx, tx, opened...); err != nil {
			return err
		}
		return flushSums(ctx, tx, d)
	})
	if err != nil {
		return s.forgetIfMissing(err, table)
	}
	for k, i := range idx {
		out[i] = answers[k]
	}
	return nil
}

// MarkOrphansPlaced notes every orphan a re-match pass placed, in ONE
// transaction, in table and id order. [dlr.ManyMarker]
func (s *RecordStore) MarkOrphansPlaced(ctx context.Context, orphans []model.OrphanDLR) error {
	ms := make([]dal.OrphanMatch, len(orphans))
	for i, o := range orphans {
		ms[i] = dal.OrphanMatch{Table: o.Table, ID: o.ID}
	}
	sort.Slice(ms, func(i, j int) bool {
		if ms[i].Table != ms[j].Table {
			return ms[i].Table < ms[j].Table
		}
		return ms[i].ID < ms[j].ID
	})
	if len(ms) == 0 {
		return nil
	}
	return s.MarkOrphansMatched(ctx, ms)
}
