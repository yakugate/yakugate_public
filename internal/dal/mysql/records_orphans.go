package mysql

// Orphans: receipts and incoming messages that matched nothing (company 0).

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/model"
)

// ---- orphans: what matched nothing (company 0) ----

func orphansTable(t time.Time) string {
	return billingstore.RecordTableName(billingstore.KindOrphans, 0, t)
}

// WriteOrphanMO records an inbound message no MO route owns. RECORD ONLY (PO
// ruling 2026-08-31): it is never re-ingested, even if a matching route appears
// later — delivering a weeks-old inbound message to a customer who has no
// context for it would be worse than the gap it fills.
func (s *Store) WriteOrphanMO(ctx context.Context, vendorID int64, source, destination, body, enc, mcc, mnc, reason string) error {
	return s.WriteOrphans(ctx, []OrphanRow{{Kind: "mo", VendorID: vendorID, Source: source, Destination: destination,
		Body: body, Encoding: enc, MCC: mcc, MNC: mnc, Reason: reason, At: time.Now()}}, false)
}

// WriteOrphanDLR records a receipt that correlates to no message of ours —
// foreign, stale, or one that lost a race with its own record row. RECORD ONLY.
func (s *Store) WriteOrphanDLR(ctx context.Context, vendorID int64, vendorMsgID, status, errorCode, raw, reason string) error {
	return s.WriteOrphans(ctx, []OrphanRow{{Kind: "dlr", VendorID: vendorID, VendorMsgID: vendorMsgID, Status: status,
		ErrorCode: errorCode, Raw: raw, Reason: reason, At: time.Now()}}, false)
}

// PendingOrphanDLRs reads filed receipts that correlated to nothing and were
// not applied since, newest first, for one more try at matching them
// (2026-09-16): a receipt can arrive before the record it belongs to. Only
// rows received since `since` are read; today's table and the day of `since`,
// so a sweep just after midnight still sees the minutes before it.
func (s *Store) PendingOrphanDLRs(ctx context.Context, since time.Time, limit int) ([]model.OrphanDLR, error) {
	if limit <= 0 {
		limit = 200
	}
	now := time.Now().UTC()
	tables := []string{orphansTable(now)}
	for d := billingstore.DayStart(now, -1); !d.Before(billingstore.DayStart(since, 0)); d = d.AddDate(0, 0, -1) {
		tables = append(tables, orphansTable(d))
	}
	var out []model.OrphanDLR
	for _, table := range tables {
		rows, err := s.db.QueryContext(ctx,
			"SELECT o.id, COALESCE(o.vendor_id,0), COALESCE(o.vendor_msg_id,''), COALESCE(o.status,''),"+
				" COALESCE(o.error_code,''), COALESCE(o.raw,''), o.received_at FROM "+table+" o"+
				" LEFT JOIN orphans_matched x ON x.orphan_table = ? AND x.orphan_id = o.id"+
				" WHERE o.kind='dlr' AND o.received_at >= ? AND o.vendor_msg_id IS NOT NULL AND x.orphan_id IS NULL"+
				" ORDER BY o.id DESC LIMIT ?",
			table, since, limit)
		if isNoSuchTable(err) {
			continue // nothing has been filed that day
		}
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			o := model.OrphanDLR{Table: table}
			if err := rows.Scan(&o.ID, &o.VendorID, &o.VendorMsgID, &o.Status, &o.ErrorCode, &o.Raw, &o.ReceivedAt); err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, o)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// MarkOrphanMatched notes that a filed receipt was applied to the message it
// turned out to belong to: it is not offered again, and it comes off the
// orphans' summary, which counts the receipts we could NOT place. The orphan
// row itself stays (logs are never changed); orphans_matched says it is done.
func (s *Store) MarkOrphanMatched(ctx context.Context, table string, id int64, msgID string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error { return markOrphanMatchedTx(ctx, tx, table, id, msgID) })
}

func markOrphanMatchedTx(ctx context.Context, tx *sql.Tx, table string, id int64, msgID string) error {
	rt, ok := billingstore.ParseRecordTable(table)
	if !ok || rt.Kind != billingstore.KindOrphans {
		return fmt.Errorf("not an orphan table: %q", table)
	}
	var at float64
	var vendor int64
	var reason string
	err := tx.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP(received_at), COALESCE(vendor_id,0), COALESCE(reason,'') FROM "+table+
		" WHERE id=? AND kind='dlr'", id).Scan(&at, &vendor, &reason)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, "INSERT IGNORE INTO orphans_matched (orphan_table, orphan_id, msg_id) VALUES (?,?,?)",
		table, id, nsStr(msgID))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil // matched already: taken off the summary then
	}
	const key = " WHERE bucket_start=FROM_UNIXTIME(?) AND kind='dlr' AND vendor_id=? AND reason=? AND shortcode=''"
	if _, err := tx.ExecContext(ctx, "UPDATE summary_orphans SET orphans=IF(orphans>0, orphans-1, 0)"+key,
		bucketOf(int64(at)), vendor, truncRunes(reason, 64)); err != nil {
		return err
	}
	// The last orphan of its row matched after all: the row goes, not
	// a line of 0 on the Orphans summary.
	_, err = tx.ExecContext(ctx, "DELETE FROM summary_orphans"+key+" AND orphans=0", bucketOf(int64(at)), vendor, truncRunes(reason, 64))
	return err
}

// inTx runs fn in a transaction, again when MySQL makes it a lock victim.
func (s *Store) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return againIfVictim(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck
		if err := fn(tx); err != nil {
			return err
		}
		return tx.Commit()
	})
}
