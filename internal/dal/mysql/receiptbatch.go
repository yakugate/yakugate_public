package mysql

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
)

// Receipts written in batches (2026-10-07, Saleem: "receipts should not go to
// mysql, if they need any data they may use redis if it is configured").
//
// With Redis configured a receipt is finished in Redis — found, decided,
// pushed to its client (internal/dlr fast.go) — and what it owes the record
// store arrives here afterwards, five hundred at a time: its own row in the
// receipt log. The row of the receipt that closed its message says so
// (dlr_.final_of): that is the message's status, worked out when read (issue
// 14 — no row is ever changed). One transaction per receipt table.

// ReceiptRow is one receipt's database work (dal.ReceiptRow).
type ReceiptRow = dal.ReceiptRow

// MTLogTableFor is the table a message is filed in — the name the vendor
// role's own record write uses, so a receipt filed later lands beside it.
func MTLogTableFor(m model.Message) string { return dal.MTLogTableFor(m) }

// PrivacyOf is what is kept of a message's text (plain, masked, encrypted).
func PrivacyOf(m model.Message) model.Privacy { return dal.PrivacyOf(m) }

// mtTableOK says a message table's name is one of ours. The name comes back
// from outside the database (it rode the receipt's facts through Redis), and
// it is put into SQL text, so anything else is refused.
func mtTableOK(name string) bool { return recordTableCheck(name, billingstore.KindMT) }

// receiptChunk is the most receipt rows in one INSERT.
const receiptChunk = 500

// receiptBatchBetween runs inside a receipt table's transaction, after its
// messages were read and before its rows are written. nil outside tests: it
// is where a test lets a message's record arrive at that moment.
var receiptBatchBetween func()

// WriteReceiptBatch does as much of a batch as it can and returns what is
// still owed:
//
//   - a row of a receipt table that could not be written (with err set):
//     tried again whole;
//   - a row that closes its message while the message has NO record yet —
//     the receipt was faster than the vendor role's own batch write: owed,
//     whole, until the record is there, so the message's outcome is counted
//     in the summary row the message itself is in;
//   - a vendor's row that closes its message while the vendor's accepted
//     answer is not in sub_ yet (mt_ is there from acceptance): owed the same
//     way, so the outcome lands on the vendor's summary row.
//
// Everything else is written and is not returned.
func (s *Store) WriteReceiptBatch(ctx context.Context, rows []ReceiptRow) (owed []ReceiptRow, err error) {
	return s.writeReceiptBatch(ctx, rows, false)
}

// WriteReceiptBatchOnce is WriteReceiptBatch for a caller that may be writing
// some of these receipts a second time (the DB worker, whose batch can be met
// again after a stop): a receipt whose row is in its table already — same
// message, same vendor id, same word, status and error code — is not written
// again. Two receipts a vendor really sent word for word alike are then one
// row, which is the price of never charging one receipt twice (as
// AppendRawDLROnce has it).
func (s *Store) WriteReceiptBatchOnce(ctx context.Context, rows []ReceiptRow) (owed []ReceiptRow, err error) {
	return s.writeReceiptBatch(ctx, rows, true)
}

func (s *Store) writeReceiptBatch(ctx context.Context, rows []ReceiptRow, once bool) (owed []ReceiptRow, err error) {
	if len(rows) == 0 {
		return nil, nil
	}
	rows = append([]ReceiptRow(nil), rows...)
	byTable := map[string][]int{}
	for i, r := range rows {
		if !mtTableOK(r.Table) {
			// No table of ours: the facts of a message sent before the logs
			// were filed by day, whose record is in a table the gateway no
			// longer reads. There is nowhere to put it.
			continue
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
		for from := 0; from < len(idx); from += receiptChunk {
			part := idx[from:min(from+receiptChunk, len(idx))]
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
				// Waiting, whole, for its message's record (Recorded says so).
				r := rows[i]
				r.Recorded = true
				owed = append(owed, r)
			}
		}
	}
	return owed, err
}

// writeReceiptTable writes rows[idx…] — all of one receipt table — in ONE
// transaction: the rows, the closing mark of each message a row closes
// (final_of, unless another row has it already), and the summaries. It
// returns the positions owed (a closing row whose message has no record yet).
func (s *Store) writeReceiptTable(ctx context.Context, table string, rows []ReceiptRow, idx []int, once bool) (owed []int, err error) {
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
			if todo, e = receiptsNotWritten(ctx, tx, table, rows, todo); e != nil {
				return e
			}
		}
		// The messages the closing rows are about: on the log as sent, a
		// refusal at submission (final already), or not written yet.
		var ids []string
		for _, i := range todo {
			if rows[i].Closes() {
				ids = append(ids, rows[i].MsgID)
			}
		}
		sort.Strings(ids)
		mts, e := readMTRows(ctx, tx, mtTable, ids, false)
		if e != nil && !isNoSuchTable(e) {
			return e
		}
		var missing []string
		for _, id := range ids {
			if _, ok := mts[id]; !ok {
				missing = append(missing, id)
			}
		}
		rejected, e := presentIn(ctx, tx, rejTable, missing)
		if e != nil {
			return e
		}
		closed, e := finalsIn(ctx, tx, table, ids)
		if e != nil {
			return e
		}
		if receiptBatchBetween != nil {
			receiptBatchBetween()
		}
		d := newSums()
		var sb []string
		args := make([]any, 0, len(todo)*16)
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
					receiptSource(r.Source, r.Raw) == SourceVendor:
					// A vendor's receipt before the vendor's accepted answer
					// is in sub_ (the three-table design: mt_ is there from
					// acceptance, the answer comes with the DB worker's next
					// write). It waits for it as it waited for the record, so
					// its outcome is counted on the vendor's summary row.
					owed = append(owed, i)
					continue
				case onLog && !closed[r.MsgID]:
					final = r.MsgID
					closed[r.MsgID] = true
					closing = append(closing, r.MsgID)
					mt := mts[r.MsgID]
					mt.status = r.ClosingStatus()
					d.addMT(mt.key, mt.outcome(), 1)
				default:
					// Closed already (another receipt, or a refusal at
					// submission): this one is evidence.
					sell, buy = 0, 0
				}
			}
			if r.RecordedOnly {
				sell, buy = 0, 0
			}
			at := r.At
			if at.IsZero() {
				at = time.Now()
			}
			var done any
			if t, err := time.Parse(time.RFC3339, r.DoneAt); err == nil {
				done = t.UTC()
			}
			// The row that closes its message carries the status the message
			// closes with (a per-vendor rule may have renamed the vendor's
			// outcome; its word stays in stat): the status pull and the
			// summaries read the same status.
			rowStatus := r.Status
			if final != nil {
				rowStatus = r.ClosingStatus()
			}
			sb = append(sb, receiptRowValue)
			args = append(args, r.MsgID, r.AccountID, receiptSource(r.Source, r.Raw), nz(r.VendorID), nsStr(r.VendorMsgID),
				nzInt(r.Part), nil, nsStr(rowStatus),
				nsStr(r.Stat), nsStr(trunc(r.ErrorCode, 32)), nsStr(trunc(r.Raw, 512)), final, nsStr(r.Deployment), done,
				nz(sell), nz(buy), nsFx(fx), ackAtArg(at))
			d.addReceipt(at, r.AccountID, r.VendorID, r.Stat, r.ErrorCode)
		}
		if len(sb) == 0 {
			return nil
		}
		if _, e := tx.ExecContext(ctx, "INSERT INTO "+table+" ("+receiptRowCols+") VALUES "+joinValues(sb), args...); e != nil {
			if isDupEntry(e) {
				return errRaced // another writer closed one of them meanwhile: read again
			}
			return e
		}
		if e := closeOpen(ctx, tx, closing...); e != nil {
			return e
		}
		return s.flushSums(ctx, tx, d)
	})
	return owed, s.forgetIfMissing(err, table)
}

// joinValues joins row placeholders with commas.
func joinValues(vals []string) string {
	n := 0
	for _, v := range vals {
		n += len(v) + 1
	}
	b := make([]byte, 0, n)
	for i, v := range vals {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, v...)
	}
	return string(b)
}

// finalsIn reads which of ids have a closing receipt in table already.
func finalsIn(ctx context.Context, x dbtx, table string, ids []string) (map[string]bool, error) {
	out := map[string]bool{}
	for from := 0; from < len(ids); from += receiptChunk {
		part := ids[from:min(from+receiptChunk, len(ids))]
		args := make([]any, len(part))
		for i, id := range part {
			args[i] = id
		}
		q, err := x.QueryContext(ctx, "SELECT final_of FROM "+table+" WHERE final_of IN ("+placeholders(len(part))+")", args...)
		if err != nil {
			if isNoSuchTable(err) {
				return out, nil
			}
			return nil, err
		}
		for q.Next() {
			var id string
			if q.Scan(&id) == nil {
				out[id] = true
			}
		}
		err = q.Err()
		q.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// receiptsNotWritten keeps, of rows[idx…], the receipts whose row is not in
// table yet: same message, vendor id, word, status and error code.
func receiptsNotWritten(ctx context.Context, x dbtx, table string, rows []ReceiptRow, idx []int) ([]int, error) {
	key := func(id, vid, stat, status, code string) string {
		return id + "\x00" + vid + "\x00" + stat + "\x00" + status + "\x00" + code
	}
	args := make([]any, len(idx))
	for k, i := range idx {
		args[k] = rows[i].MsgID
	}
	q, err := x.QueryContext(ctx, "SELECT msg_id, COALESCE(vendor_msg_id,''), COALESCE(stat,''), COALESCE(status,''), COALESCE(error_code,'') FROM "+table+
		" WHERE msg_id IN ("+placeholders(len(args))+")", args...)
	if err != nil {
		if isNoSuchTable(err) {
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
		have[key(id, vid, stat, status, code)] = true
	}
	err = q.Err()
	q.Close()
	if err != nil {
		return nil, err
	}
	var out []int
	for _, i := range idx {
		r := rows[i]
		if !have[key(r.MsgID, r.VendorMsgID, r.Stat, r.Status, trunc(r.ErrorCode, 32))] {
			out = append(out, i)
		}
	}
	return out, nil
}
