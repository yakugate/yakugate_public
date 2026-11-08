package mysql

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
)

// Rows that used to be written by the role that met them, and are now tasks
// of the record queue in worker mode (Saleem, 28 Oct 2026: with the DB worker
// every hot-store log and summary goes through the queue): orphans, orphans
// matched, and lookups with no message. The DB worker writes many at once;
// the one-row methods (WriteOrphanMO, WriteOrphanDLR, MarkOrphanMatched,
// WriteLookup) are these with one row, for a gateway without the worker.
//
// A row carries the moment it happened (At), so a task written late, or
// twice, still says when — and "twice" can be told: once=true (a task that
// may be on a list already) leaves out a row that is there already.

// The rows (dal.OrphanRow, dal.OrphanMatch, dal.LookupRow).
type (
	OrphanRow   = dal.OrphanRow
	OrphanMatch = dal.OrphanMatch
	LookupRow   = dal.LookupRow
)

// WriteOrphans files orphans: one multi-row INSERT per day table, their
// summary_orphans counts in the same transaction.
func (s *Store) WriteOrphans(ctx context.Context, rows []OrphanRow, once bool) error {
	byTable := map[string][]OrphanRow{}
	var order []string
	for _, r := range rows {
		if r.At.IsZero() {
			r.At = time.Now()
		}
		r.At = r.At.UTC()
		if len(r.Raw) > 4096 {
			r.Raw = r.Raw[:4096] // a receipt is a line of text; anything vast is not one
		}
		t := orphansTable(r.At)
		if _, ok := byTable[t]; !ok {
			order = append(order, t)
		}
		byTable[t] = append(byTable[t], r)
	}
	for _, table := range order {
		if err := s.ensureRecords(ctx, table); err != nil {
			return err
		}
		rs := byTable[table]
		err := s.inTx(ctx, func(tx *sql.Tx) error {
			if once {
				var err error
				if rs, err = orphansMissing(ctx, tx, table, rs); err != nil || len(rs) == 0 {
					return err
				}
			}
			vals := strings.TrimSuffix(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?,?,?,FROM_UNIXTIME(CAST(? AS DECIMAL(17,3)))),", len(rs)), ",")
			args := make([]any, 0, len(rs)*14)
			d := newSums()
			for _, r := range rs {
				args = append(args, r.Kind, nz(r.VendorID), nsStr(r.VendorMsgID), nsStr(r.Source), nsStr(r.Destination),
					nsStr(r.Body), nsStr(r.Encoding), nsStr(r.MCC), nsStr(r.MNC), nsStr(r.Status), nsStr(r.ErrorCode),
					nsStr(r.Raw), nsStr(r.Reason), ackAtArg(r.At))
				shortcode := ""
				if r.Kind == "mo" {
					shortcode = r.Destination
				}
				d.addOrphan(r.At, r.Kind, r.VendorID, r.Reason, shortcode)
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO "+table+
				" (kind,vendor_id,vendor_msg_id,source,destination,body,encoding,mcc,mnc,status,error_code,raw,reason,received_at) VALUES "+vals, args...); err != nil {
				return err
			}
			return s.flushSums(ctx, tx, d)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// orphansMissing keeps the rows not in table yet: same kind, vendor, vendor id
// or sender, and the same moment.
func orphansMissing(ctx context.Context, tx *sql.Tx, table string, rs []OrphanRow) ([]OrphanRow, error) {
	var out []OrphanRow
	for _, r := range rs {
		var one int
		err := tx.QueryRowContext(ctx, "SELECT 1 FROM "+table+
			" WHERE kind=? AND received_at=FROM_UNIXTIME(CAST(? AS DECIMAL(17,3))) AND COALESCE(vendor_id,0)=?"+
			" AND COALESCE(vendor_msg_id,'')=? AND COALESCE(source,'')=? LIMIT 1",
			r.Kind, ackAtArg(r.At), r.VendorID, r.VendorMsgID, r.Source).Scan(&one)
		if err == sql.ErrNoRows {
			out = append(out, r)
			continue
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// MarkOrphansMatched notes several filed receipts as applied, in one
// transaction. Safe to repeat: a receipt noted already changes nothing.
func (s *Store) MarkOrphansMatched(ctx context.Context, ms []OrphanMatch) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		for _, m := range ms {
			if err := markOrphanMatchedTx(ctx, tx, m.Table, m.ID, m.MsgID); err != nil {
				return err
			}
		}
		return nil
	})
}

// WriteLookups files lookups with no message: one multi-row INSERT per
// company-day table, their summary_lookup counts in the same transaction.
func (s *Store) WriteLookups(ctx context.Context, rows []LookupRow, once bool) error {
	byTable := map[string][]LookupRow{}
	var order []string
	for _, r := range rows {
		if r.At.IsZero() {
			r.At = time.Now()
		}
		r.At = r.At.UTC()
		t := billingstore.RecordTableName(billingstore.KindHLR, r.CompanyID, r.At)
		if _, ok := byTable[t]; !ok {
			order = append(order, t)
		}
		byTable[t] = append(byTable[t], r)
	}
	for _, table := range order {
		if err := s.ensureRecords(ctx, table); err != nil {
			return err
		}
		rs := byTable[table]
		err := s.inTx(ctx, func(tx *sql.Tx) error {
			if once {
				var keep []LookupRow
				for _, r := range rs {
					var one int
					err := tx.QueryRowContext(ctx, "SELECT 1 FROM "+table+
						" WHERE msg_id IS NULL AND COALESCE(account_id,0)=? AND destination=? AND looked_up_at=FROM_UNIXTIME(CAST(? AS DECIMAL(17,3))) LIMIT 1",
						r.AccountID, r.Destination, ackAtArg(r.At)).Scan(&one)
					if err == sql.ErrNoRows {
						keep = append(keep, r)
						continue
					}
					if err != nil {
						return err
					}
				}
				if rs = keep; len(rs) == 0 {
					return nil
				}
			}
			vals := strings.TrimSuffix(strings.Repeat("(NULL,?,?,?,?,?,?,FROM_UNIXTIME(CAST(? AS DECIMAL(17,3))),?,?,?"+
				strings.Repeat(",?", len(dal.LookupCallCols))+"),", len(rs)), ",")
			args := make([]any, 0, len(rs)*(10+len(dal.LookupCallCols)))
			d := newSums()
			for _, r := range rs {
				args = append(args, nz(r.AccountID), r.Destination, r.Source, nsStr(r.MCC), nsStr(r.MNC), boolInt(r.Ported),
					ackAtArg(r.At), nsStr(deployment.ID()), nsStr(r.OperatorName), nsStr(r.CCC))
				args = append(args, dal.LookupCallArgs(r.Call)...) // the provider's call (hot/0080)
				// Counted in summary_lookup as a message's lookup is (D-136),
				// with no vendor and no charge.
				d.addLookup(mtRow{key: mtKey{bucket: bucketOf(r.At.Unix()), account: r.AccountID, mcc: r.MCC, mnc: r.MNC},
					hlrSource: r.Source, ported: r.Ported})
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO "+table+
				" (msg_id,account_id,destination,source,mcc,mnc,ported,looked_up_at,deployment_id,operator_name,ccc,"+
				strings.Join(dal.LookupCallCols, ",")+") VALUES "+vals, args...); err != nil {
				return err
			}
			return s.flushSums(ctx, tx, d)
		})
		if err != nil {
			return err
		}
	}
	return nil
}
