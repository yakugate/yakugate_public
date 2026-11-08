package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
)

// Messages still waiting for a receipt, read from the day tables (29 Oct
// 2026). With Redis set they were the receipt facts' waiting sets in the main
// Redis — four commands a message for two lists only two features read. Those
// two read MySQL now, only for the accounts and vendors that use them:
//
//	the no-receipt rule   an account's messages taken by a vendor longer ago
//	                      than its rule's minutes, with no closing receipt and
//	                      nothing told (AwaitingReceiptInDays);
//	HTTP status polls     a poll-mode vendor's messages it took with an id and
//	                      that have no closing receipt (SentAwaitingDLRInDays).
//
// "Taken" is an accepted answer in sub_, "closed" a dlr_ row with final_of,
// "told" a policy: row with a final status — the same rule as the status
// pull. Both read on the read instance (old()): a message a moment old may be
// missed by one pass and is read by the next.

// awaitTablesFor is how long a list of one kind's tables is kept before it is
// read again (one information_schema read).
const awaitTablesFor = 30 * time.Second

type tablesSeen struct {
	at    time.Time
	names []billingstore.RecordTable
}

var (
	tablesMu    sync.Mutex
	tablesCache = map[string]tablesSeen{}
)

// dayTables is every log table of one kind (one company's, or every
// company's when company < 0) of the last `days` days, oldest first.
func (s *Store) dayTables(ctx context.Context, db *sql.DB, kind string, company int64, days int) ([]billingstore.RecordTable, error) {
	if days < 1 {
		days = 1
	}
	cacheKey := fmt.Sprintf("%p|%s", db, kind)
	tablesMu.Lock()
	seen, ok := tablesCache[cacheKey]
	tablesMu.Unlock()
	if !ok || time.Since(seen.at) > awaitTablesFor {
		rows, err := db.QueryContext(ctx, "SHOW TABLES LIKE '"+strings.ReplaceAll(kind, "_", `\_`)+`\_%'`)
		if err != nil {
			return nil, err
		}
		var names []billingstore.RecordTable
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err != nil {
				rows.Close()
				return nil, err
			}
			if rt, ok := billingstore.ParseRecordTable(t); ok && rt.Kind == kind {
				names = append(names, rt)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		seen = tablesSeen{at: time.Now(), names: names}
		tablesMu.Lock()
		tablesCache[cacheKey] = seen
		tablesMu.Unlock()
	}
	oldest := billingstore.DayStart(time.Now(), -days)
	var out []billingstore.RecordTable
	for _, rt := range seen.names {
		if rt.Date.Before(oldest) || (company >= 0 && rt.CompanyID != company) {
			continue
		}
		out = append(out, rt)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Day != out[j].Day {
			return out[i].Day < out[j].Day
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// closedOrTold is the condition "a receipt closed it, or the client was told
// a final answer in the vendor's place" on a message m, in its day's receipt
// table.
func closedOrTold(dlr string) string {
	return " AND NOT EXISTS (SELECT 1 FROM " + dlr + " d WHERE d.msg_id = m.msg_id AND (d.final_of IS NOT NULL OR (d.raw LIKE 'policy:%' AND d.status IN ('delivered','failed','expired'))))"
}

// AwaitingReceiptInDays lists one account's messages that a vendor took more
// than olderThan ago and that have no closing receipt and nothing told, from
// its company's day tables of the last `days` days, oldest first, read on from
// after. Only this deployment's and the shared ones (D-121). Each carries the
// vendor and the vendor's id it was taken with, which the no-receipt rule
// closes its facts by.
func (s *Store) AwaitingReceiptInDays(ctx context.Context, companyID, accountID int64, olderThan time.Duration, after AwaitCursor, limit, days int) ([]AwaitingReceipt, AwaitCursor, error) {
	mins := int(olderThan / time.Minute)
	if mins < 1 {
		mins = 1
	}
	db := s.old()
	tables, err := s.dayTables(ctx, db, billingstore.KindMT, companyID, days)
	if err != nil {
		return nil, after, err
	}
	own, ownArgs := s.ownerCond()
	own = strings.ReplaceAll(own, "queue_owner", "m.queue_owner")
	var out []AwaitingReceipt
	next := after
	for _, t := range tables {
		if len(out) >= limit {
			break
		}
		if !after.SentAt.IsZero() && t.Date.Add(24*time.Hour).Before(after.SentAt) {
			continue // a day wholly before where the last pass stopped
		}
		sub := billingstore.SwapKind(t.Name, billingstore.KindSub)
		dlr := billingstore.SwapKind(t.Name, billingstore.KindDLR)
		q := func(withDLR bool) (*sql.Rows, error) {
			from, fromArgs := "", []any{}
			if after.MsgID != "" {
				from = " AND (m.accepted_at > ? OR (m.accepted_at = ? AND m.msg_id > ?))"
				fromArgs = []any{after.SentAt.UTC(), after.SentAt.UTC(), after.MsgID}
			}
			cond := ""
			if withDLR {
				cond = closedOrTold(dlr)
			}
			args := append(append(append([]any{accountID, mins}, fromArgs...), ownArgs...), limit-len(out))
			return db.QueryContext(ctx, `
				SELECT m.msg_id, COALESCE(m.sender,''), COALESCE(m.destination,''), COALESCE(m.bind_slot,0),
				       COALESCE(m.no_receipt,0), COALESCE(m.failure_receipt_only,0), COALESCE(m.queue_owner,''), m.accepted_at,
				       (SELECT CONCAT(s.vendor_id, '|', COALESCE(s.vendor_msg_id,'')) FROM `+sub+` s
				         WHERE s.msg_id = m.msg_id AND s.result = 'accepted' ORDER BY s.attempt DESC, s.part LIMIT 1) AS taken
				  FROM `+t.Name+` m
				 WHERE m.account_id = ? AND m.accepted_at < UTC_TIMESTAMP(3) - INTERVAL ? MINUTE`+from+own+cond+`
				HAVING taken IS NOT NULL
				 ORDER BY m.accepted_at, m.msg_id LIMIT ?`, args...)
		}
		rows, err := q(true)
		if isNoSuchTable(err) {
			rows, err = q(false) // no receipt table that day: nothing closed or told
		}
		if isNoSuchTable(err) {
			continue // no answers that day: nothing taken
		}
		if err != nil {
			return nil, after, err
		}
		for rows.Next() {
			var r AwaitingReceipt
			var noRcpt, failOnly int
			var at time.Time
			var taken string
			if err := rows.Scan(&r.MsgID, &r.Sender, &r.Destination, &r.BindSlot, &noRcpt, &failOnly, &r.Owner, &at, &taken); err != nil {
				rows.Close()
				return nil, after, err
			}
			r.CompanyID = t.CompanyID
			r.NoReceipt, r.FailureReceiptOnly = noRcpt != 0, failOnly != 0
			if i := strings.IndexByte(taken, '|'); i > 0 {
				r.VendorID, _ = strconv.ParseInt(taken[:i], 10, 64)
				r.VendorMsgID = taken[i+1:]
			}
			out = append(out, r)
			next = AwaitCursor{SentAt: at, MsgID: r.MsgID}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, after, err
		}
	}
	return out, next, nil
}

// SentAwaitingDLRInDays is a poll-mode vendor's messages it took with an id
// and that no receipt has closed, of every company's day tables of the last
// `days` days, in message-id order after `after`.
func (s *Store) SentAwaitingDLRInDays(ctx context.Context, vendorID int64, after string, limit, days int) ([]PendingDLR, error) {
	db := s.old()
	tables, err := s.dayTables(ctx, db, billingstore.KindSub, -1, days)
	if err != nil {
		return nil, err
	}
	var out []PendingDLR
	for _, t := range tables {
		dlr := billingstore.SwapKind(t.Name, billingstore.KindDLR)
		q := func(withDLR bool) (*sql.Rows, error) {
			cond := ""
			if withDLR {
				cond = " AND NOT EXISTS (SELECT 1 FROM " + dlr + " d WHERE d.msg_id = m.msg_id AND d.final_of IS NOT NULL)"
			}
			return db.QueryContext(ctx, `
				SELECT DISTINCT m.msg_id, m.vendor_msg_id, COALESCE(m.account_id,0) FROM `+t.Name+` m
				 WHERE m.vendor_id = ? AND m.result = 'accepted' AND m.vendor_msg_id IS NOT NULL AND m.vendor_msg_id <> ''
				   AND m.msg_id > ?`+cond+`
				 ORDER BY m.msg_id LIMIT ?`, vendorID, after, limit)
		}
		rows, err := q(true)
		if isNoSuchTable(err) {
			rows, err = q(false)
		}
		if isNoSuchTable(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			p := PendingDLR{CompanyID: t.CompanyID}
			if err := rows.Scan(&p.MsgID, &p.VendorMsgID, &p.AccountID); err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, p)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].MsgID != out[j].MsgID {
			return out[i].MsgID < out[j].MsgID
		}
		return out[i].VendorMsgID < out[j].VendorMsgID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// AwaitDays is how many days of tables a reader of waiting messages looks at
// for a wait of this long: the days it spans, and today (dal.AwaitDays).
func AwaitDays(wait time.Duration) int { return dal.AwaitDays(wait) }
