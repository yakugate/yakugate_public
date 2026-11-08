package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
)

// Final receipts closed many at a time, without Redis (DAL plan, step 3,
// 30 Oct 2026).
//
// With no receipt facts in Redis a vendor's receipt is matched and closed in
// the record store: CloseWithReceipt, one transaction a receipt — its row,
// its message's closing mark (final_of) and the summaries. Every transaction
// of the gateway that counts a message (acceptance, the vendor's answer, the
// receipt) updates the same summary_mt row, and holds it until its commit:
// on the rig's mix with no Redis a summary_mt update waited 270 ms on
// average, a receipt cost about 135 ms, and the SMPP vendor's receipt
// workers fell minutes behind (562 + 570 client receipts arrived after the
// rig had stopped listening). CloseWithReceipts does what CloseWithReceipt
// does for each receipt of a batch, in ONE transaction a receipt table, so a
// summary row is updated once a batch, not once a receipt.

// RecordStmts are the record store's statements of this file, by name. The
// receipt table is one a company and day (dlr_<company>_<day>); a row list
// is registered in its one-row form.
var RecordStmts = stmt.New("mysql")

var (
	stReceiptCloseRows = RecordStmts.Add("record.receipt.close_many",
		"INSERT INTO `dlr_<company>_<day>` ("+receiptRowCols+") VALUES "+receiptRowValue, true)
	stReceiptCloseRefused = RecordStmts.Add("record.receipt.close_many.refused",
		"SELECT COALESCE(account_id,0), COALESCE(vendor_id,0) FROM `rejects_<company>_<day>` WHERE msg_id=?", true)
)

// RecordStatements is the registry of this file's statements.
func RecordStatements() *stmt.Registry { return RecordStmts }

// CloseWithReceipts is CloseWithReceipt for many final receipts (mayClose
// true for each), in the order given: a message's first receipt in the batch
// closes it when nothing closed it before, a later one is recorded only —
// exactly what one call after another leaves. A message with no record is
// Found=false and nothing of its receipt is written. An error means nothing
// of the receipt table it names was written; tables are written one after
// another, so the receipts of a table written before stay written — the
// answer for them is in out, and err is the first error. [dlr.ManyCloser]
func (s *Store) CloseWithReceipts(ctx context.Context, rs []model.ReceiptClose) (out []model.ReceiptClosed, err error) {
	out = make([]model.ReceiptClosed, len(rs))
	if len(rs) == 0 {
		return out, nil
	}
	byTable := map[string][]int{}
	for i, r := range rs {
		t := dlrOf(r.Ref)
		byTable[t] = append(byTable[t], i)
	}
	tables := make([]string, 0, len(byTable))
	for t := range byTable {
		tables = append(tables, t)
	}
	sort.Strings(tables) // tables locked in one order by every writer
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
			// and our insert three times running: one at a time, as before.
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
// transaction, as CloseWithReceipt writes each: the message read (its MT row,
// else its refusal), the row (the closing one when the message is not closed
// yet, else recorded only), the closing mark, the summaries.
func (s *Store) closeReceiptTable(ctx context.Context, table string, rs []model.ReceiptClose, idx []int, out []model.ReceiptClosed) error {
	if err := s.ensureRecords(ctx, table); err != nil {
		return err
	}
	// Each receipt's row as closeWithReceipt reads it from its JSON.
	es := make([]rawDLREntry, len(idx))
	for k, i := range idx {
		r := rs[i]
		var e rawDLREntry
		_ = json.Unmarshal([]byte(r.Entry), &e)
		if e.VendorMsgID == "" {
			e.VendorMsgID = r.VendorMsgID
		}
		if e.Status == "" {
			e.Status = string(r.Status)
		}
		if string(r.Status) != e.Status && r.Status.IsTerminal() {
			e.FinalStatus = string(r.Status) // a per-vendor rule renamed the outcome
		}
		es[k] = e
	}
	ref0 := rs[idx[0]].Ref
	mtTable := mtOf(ref0)
	rejTable := rejectsOf(ref0)
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
		mts, err := readMTRows(ctx, tx, mtTable, ids, false)
		if err != nil && !isNoSuchTable(err) {
			return err
		}
		msgs := make(map[string]messageOf, len(ids))
		for _, id := range ids {
			if r, ok := mts[id]; ok {
				msgs[id] = messageOf{found: true, account: r.key.account, vendor: r.key.vendor, row: r}
				continue
			}
			// No MT row: a refusal at submission (a refused duplicate's
			// receipt hangs on it), else no record at all.
			var m messageOf
			start := time.Now()
			err := tx.QueryRowContext(ctx, "SELECT COALESCE(account_id,0), COALESCE(vendor_id,0) FROM "+rejTable+" WHERE msg_id=?", id).
				Scan(&m.account, &m.vendor)
			stReceiptCloseRefused.Observe(time.Since(start), 1)
			if err == sql.ErrNoRows || isNoSuchTable(err) {
				continue
			}
			if err != nil {
				return err
			}
			m.found, m.rejected = true, true
			msgs[id] = m
		}
		var closing []string
		for _, id := range ids {
			if m := msgs[id]; m.found && !m.rejected {
				closing = append(closing, id)
			}
		}
		closed, err := finalsIn(ctx, tx, table, closing)
		if err != nil {
			return err
		}
		d := newSums()
		var vals []string
		args := make([]any, 0, len(idx)*18)
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
			// What insertReceiptTx gives each case: the closing row with its
			// prices; a row recorded only, after a close, with none.
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
				sell, buy = 0, 0 // a receipt recorded only booked nothing
			}
			answers[k].Mine = mine
			status := e.Status
			if final != nil && e.FinalStatus != "" {
				status = e.FinalStatus
			}
			var done any
			if t, err := time.Parse(time.RFC3339, e.DoneAt); err == nil {
				done = t.UTC()
			}
			vals = append(vals, receiptRowValue)
			args = append(args, r.Ref.MsgID, msg.account, receiptSource(e.Source, e.Raw), nz(msg.vendor), nsStr(e.VendorMsgID), nzInt(e.Part), nil,
				nsStr(status), nsStr(e.Stat),
				nsStr(trunc(e.ErrorCode, 32)), nsStr(trunc(e.Raw, 512)), final, nsStr(deployment.ID()), done,
				nz(sell), nz(buy), nsFx(fx), ackAtArg(arrivedAt(e)))
			d.addReceipt(arrivedAt(e), msg.account, msg.vendor, e.Stat, e.ErrorCode)
			if mine && msg.row.msgID != "" {
				row := msg.row
				row.status = e.Status
				if fs := e.FinalStatus; fs != "" {
					row.status = fs
				}
				d.addMT(row.key, row.outcome(), 1)
				opened = append(opened, r.Ref.MsgID)
			}
		}
		for from := 0; from < len(vals); from += receiptChunk {
			to := min(from+receiptChunk, len(vals))
			start := time.Now()
			_, err := tx.ExecContext(ctx, "INSERT INTO "+table+" ("+receiptRowCols+") VALUES "+joinValues(vals[from:to]),
				args[from*receiptRowArgs:to*receiptRowArgs]...)
			stReceiptCloseRows.Observe(time.Since(start), int64(to-from))
			if err != nil {
				if isDupEntry(err) {
					return errRaced // another writer closed one of them meanwhile: read again
				}
				return err
			}
		}
		if err := closeOpen(ctx, tx, opened...); err != nil {
			return err
		}
		return s.flushSums(ctx, tx, d)
	})
	if err != nil {
		return s.forgetIfMissing(err, table)
	}
	for k, i := range idx {
		out[i] = answers[k]
	}
	return nil
}

// receiptRowArgs is how many values one receipt row takes (receiptRowCols).
const receiptRowArgs = 18

// MarkOrphansPlaced notes every orphan a re-match pass placed, in ONE
// transaction (MarkOrphansMatched, the DB worker's: the same statements,
// orphan by orphan, in table and id order). Each mark takes its orphan off
// the summary_orphans row every writer of an orphan updates; one commit a
// pass holds that row once, not once an orphan. [dlr.ManyMarker]
func (s *Store) MarkOrphansPlaced(ctx context.Context, orphans []model.OrphanDLR) error {
	ms := make([]OrphanMatch, len(orphans))
	for i, o := range orphans {
		ms[i] = OrphanMatch{Table: o.Table, ID: o.ID}
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
