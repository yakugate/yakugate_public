package mssql

// Receipts written in batches, on SQL Server (mirrors
// internal/dal/mysql/receiptbatch.go): with Redis a receipt is finished in
// Redis and what it owes the record store arrives here afterwards, five
// hundred at a time — its row in the receipt log, the closing mark of the
// row that closed its message (final_of), the summaries. One transaction per
// receipt table.

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
)

// recMTTableOK says a message table's name is one of ours (it rode the
// receipt's facts through Redis, and is put into SQL text).
func recMTTableOK(name string) bool {
	rt, ok := billingstore.ParseRecordTable(name)
	return ok && rt.Kind == billingstore.KindMT
}

// WriteReceiptBatch does as much of a batch as it can and returns what is
// still owed: the rows of a receipt table that could not be written (err
// set), a closing row whose message has no record yet, a vendor's closing
// row whose accepted answer is not in sub_ yet (Recorded set on those two).
func (s *RecordStore) WriteReceiptBatch(ctx context.Context, rows []dal.ReceiptRow) (owed []dal.ReceiptRow, err error) {
	return s.writeReceiptBatch(ctx, rows, false)
}

// WriteReceiptBatchOnce is WriteReceiptBatch for a caller that may write
// some receipts a second time: a receipt whose row is there already — same
// message, vendor id, word, status and error code — is not written again.
func (s *RecordStore) WriteReceiptBatchOnce(ctx context.Context, rows []dal.ReceiptRow) (owed []dal.ReceiptRow, err error) {
	return s.writeReceiptBatch(ctx, rows, true)
}

func (s *RecordStore) writeReceiptBatch(ctx context.Context, rows []dal.ReceiptRow, once bool) (owed []dal.ReceiptRow, err error) {
	if len(rows) == 0 {
		return nil, nil
	}
	rows = append([]dal.ReceiptRow(nil), rows...)
	byTable := map[string][]int{}
	for i, r := range rows {
		if !recMTTableOK(r.Table) {
			continue // no table of ours: nowhere to put it
		}
		t := billingstore.SwapKind(r.Table, billingstore.KindDLR)
		byTable[t] = append(byTable[t], i)
	}
	tables := make([]string, 0, len(byTable))
	for t := range byTable {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	for _, table := range tables {
		idx := byTable[table]
		for from := 0; from < len(idx); from += rChunk {
			part := idx[from:min(from+rChunk, len(idx))]
			var later []int
			var e error
			for try := 0; try < 3; try++ {
				later, e = s.writeReceiptTable(ctx, table, rows, part, once)
				if !errors.Is(e, errRaced) {
					break
				}
			}
			if e != nil {
				err = e
				for _, i := range part {
					owed = append(owed, rows[i])
				}
				continue
			}
			for _, i := range later {
				r := rows[i]
				r.Recorded = true
				owed = append(owed, r)
			}
		}
	}
	return owed, err
}

var stRecReceiptKeys = RecordStmts.Add("record.dlr.keys", `SELECT TOP (100000) t.msg_id, COALESCE(t.vendor_msg_id, N''), COALESCE(t.stat, N''),
	COALESCE(t.status, N''), COALESCE(t.error_code, N'') FROM `+rIDsFrom+`{dlr} AS t ON t.msg_id = j.v`, true)

// writeReceiptTable writes rows[idx…] — all of one receipt table — in ONE
// transaction and returns the positions owed.
func (s *RecordStore) writeReceiptTable(ctx context.Context, table string, rows []dal.ReceiptRow, idx []int, once bool) (owed []int, err error) {
	if err := s.ensureRecords(ctx, table); err != nil {
		return nil, err
	}
	mtTable := rows[idx[0]].Table
	rejTable := billingstore.SwapKind(mtTable, billingstore.KindRejects)
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		owed = owed[:0]
		todo := append([]int(nil), idx...)
		if once {
			var e error
			if todo, e = recReceiptsNotWritten(ctx, tx, table, rows, todo); e != nil {
				return e
			}
		}
		var ids []string
		for _, i := range todo {
			if rows[i].Closes() {
				ids = append(ids, rows[i].MsgID)
			}
		}
		sort.Strings(ids)
		mts, e := recReadMTRows(ctx, tx, mtTable, ids)
		if e != nil && !MissingTable(e) {
			return e
		}
		var missing []string
		for _, id := range ids {
			if _, ok := mts[id]; !ok {
				missing = append(missing, id)
			}
		}
		rejected, e := recPresentIn(ctx, tx, rejTable, missing)
		if e != nil {
			return e
		}
		closed, e := recFinalsIn(ctx, tx, table, ids)
		if e != nil {
			return e
		}
		d := newSums()
		var vals [][]any
		var closing []string
		for _, i := range todo {
			r := rows[i]
			var final any
			sell, buy, fx := r.SellRateID, r.BuyRateID, r.FX
			if r.Closes() {
				_, onLog := mts[r.MsgID]
				switch {
				case !onLog && !rejected[r.MsgID]:
					owed = append(owed, i) // its record is not written yet
					continue
				case onLog && !closed[r.MsgID] && mts[r.MsgID].key.vendor == 0 &&
					recReceiptSource(r.Source, r.Raw) == recSourceVendor:
					// A vendor's receipt before the vendor's accepted answer
					// is in sub_: it waits for it, so its outcome is counted
					// on the vendor's summary row.
					owed = append(owed, i)
					continue
				case onLog && !closed[r.MsgID]:
					final = r.MsgID
					closed[r.MsgID] = true
					closing = append(closing, r.MsgID)
					mt := mts[r.MsgID]
					mt.status = r.ClosingStatus()
					d.addMT(mt.key, mt.outcome())
				default:
					sell, buy = 0, 0 // closed already: this one is evidence
				}
			}
			if r.RecordedOnly {
				sell, buy = 0, 0
			}
			at := r.At
			if at.IsZero() {
				at = time.Now()
			}
			rowStatus := r.Status
			if final != nil {
				rowStatus = r.ClosingStatus()
			}
			vals = append(vals, []any{r.MsgID, r.AccountID, recReceiptSource(r.Source, r.Raw), rNZ(r.VendorID), rStr(r.VendorMsgID),
				rNZi(r.Part), nil, rStr(rowStatus), rStr(r.Stat), rStr(rTrunc(r.ErrorCode, 32)), rStr(rTrunc(r.Raw, 512)), final,
				rStr(r.Deployment), recDoneDate(r.DoneAt), rNZ(sell), rNZ(buy), rDec(fx), rTimeText(at)})
			d.addReceipt(at, r.AccountID, r.VendorID, r.Stat, r.ErrorCode)
		}
		if len(vals) == 0 {
			return nil
		}
		if e := recInsertReceipts(ctx, tx, table, vals); e != nil {
			if IsDuplicate(e) {
				return errRaced // another writer closed one of them meanwhile: read again
			}
			return e
		}
		if e := closeOpen(ctx, tx, closing...); e != nil {
			return e
		}
		return flushSums(ctx, tx, d)
	})
	return owed, s.forgetIfMissing(err, table)
}

// recReceiptsNotWritten keeps, of rows[idx…], the receipts whose row is not
// in table yet: same message, vendor id, word, status and error code.
func recReceiptsNotWritten(ctx context.Context, x Querier, table string, rows []dal.ReceiptRow, idx []int) ([]int, error) {
	key := func(id, vid, stat, status, code string) string {
		return id + "\x00" + vid + "\x00" + stat + "\x00" + status + "\x00" + code
	}
	ids := make([]string, len(idx))
	for k, i := range idx {
		ids[k] = rows[i].MsgID
	}
	q, err := rQuery(ctx, x, stRecReceiptKeys, rOn(stRecReceiptKeys.SQL, "dlr", table), rIDs(ids))
	if err != nil {
		if MissingTable(err) {
			return idx, nil
		}
		return nil, err
	}
	have := map[string]bool{}
	for q.Next() {
		var id, vid, stat, status, code string
		if err := q.Scan(&id, &vid, &stat, &status, &code); err != nil {
			q.Close()
			return nil, err
		}
		have[key(rTrim(id), vid, stat, status, code)] = true
	}
	err = q.Err()
	q.Close()
	if err != nil {
		return nil, err
	}
	var out []int
	for _, i := range idx {
		r := rows[i]
		if !have[key(r.MsgID, r.VendorMsgID, r.Stat, r.Status, rTrunc(r.ErrorCode, 32))] {
			out = append(out, i)
		}
	}
	return out, nil
}
