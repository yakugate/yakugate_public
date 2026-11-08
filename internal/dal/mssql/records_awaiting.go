package mssql

// Messages still waiting for a receipt, read from the day tables, on SQL
// Server (mirrors internal/dal/mysql/awaiting_days.go): the no-receipt rule
// (AwaitingReceiptInDays) and the HTTP status polls (SentAwaitingDLRInDays).
// "Taken" is an accepted answer in sub_, "closed" a dlr_ row with final_of,
// "told" a policy: row with a final status — the status pull's rule. Both
// read on the read instance (old()).

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
)

// recTablesFor is how long a list of one kind's tables is kept before it is
// read again.
const recTablesFor = 30 * time.Second

type recTablesSeen struct {
	at    time.Time
	names []billingstore.RecordTable
}

// recTablesCache is one store's lists, by pool and kind. It is the store's
// own (MySQL keeps one for the process, by the pool's address): a pool closed
// and another made at the same address — tests do — must not read the
// other's list.
type recTablesCache struct {
	mu   sync.Mutex
	seen map[string]recTablesSeen
}

var (
	stRecKindTables = RecordStmts.Add("record.kind_tables", "SELECT TOP (100000) name FROM sys.tables WHERE name LIKE @p1", false)
	stRecAwaitDays  = RecordStmts.Add("record.mt.awaiting_days", `SELECT TOP (@p3) m.msg_id, COALESCE(m.sender, N''), COALESCE(m.destination, N''),
		COALESCE(m.bind_slot, 0), COALESCE(m.no_receipt, 0), COALESCE(m.failure_receipt_only, 0), COALESCE(m.queue_owner, N''), m.accepted_at,
		k.vendor_id, COALESCE(k.vendor_msg_id, N'')
	FROM {mt} AS m
	CROSS APPLY (SELECT TOP (1) s.vendor_id, s.vendor_msg_id FROM {sub} AS s
		WHERE s.msg_id = m.msg_id AND s.result = 'accepted' ORDER BY s.attempt DESC, s.part) AS k
	WHERE m.account_id = @p1 AND m.accepted_at < DATEADD(MINUTE, -@p2, SYSUTCDATETIME()) AND k.vendor_id IS NOT NULL{from}{own}{cond}
	ORDER BY m.accepted_at, m.msg_id`, false)
	stRecPollDays = RecordStmts.Add("record.sub.poll_days", `SELECT DISTINCT TOP (@p3) m.msg_id, m.vendor_msg_id, COALESCE(m.account_id, 0) FROM {sub} AS m
	WHERE m.vendor_id = @p1 AND m.result = 'accepted' AND m.vendor_msg_id IS NOT NULL AND m.vendor_msg_id <> N''
	  AND m.msg_id > @p2{cond}
	ORDER BY m.msg_id`, false)
)

// dayTables is every log table of one kind (one company's, or every
// company's when company < 0) of the last `days` days, oldest first.
func (s *RecordStore) dayTables(ctx context.Context, db *sql.DB, kind string, company int64, days int) ([]billingstore.RecordTable, error) {
	if days < 1 {
		days = 1
	}
	cacheKey := fmt.Sprintf("%p|%s", db, kind)
	c := &s.tables
	c.mu.Lock()
	seen, ok := c.seen[cacheKey]
	c.mu.Unlock()
	if !ok || time.Since(seen.at) > recTablesFor {
		rows, err := rQuery(ctx, db, stRecKindTables, stRecKindTables.SQL, rText(strings.ReplaceAll(kind, "_", "[_]")+"[_]%"))
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
		seen = recTablesSeen{at: time.Now(), names: names}
		c.mu.Lock()
		if c.seen == nil {
			c.seen = map[string]recTablesSeen{}
		}
		c.seen[cacheKey] = seen
		c.mu.Unlock()
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

// recClosedOrTold is "a receipt closed it, or the client was told a final
// answer in the vendor's place" on a message m.
const recClosedOrTold = " AND NOT EXISTS (SELECT 1 FROM {dlr} AS d WHERE d.msg_id = m.msg_id AND (d.final_of IS NOT NULL OR (d.raw LIKE N'policy:%' AND d.status IN (N'delivered', N'failed', N'expired'))))"

// AwaitingReceiptInDays lists one account's messages that a vendor took more
// than olderThan ago and that have no closing receipt and nothing told, from
// its company's day tables of the last `days` days, oldest first, read on
// from after. Only this deployment's and the shared ones (D-121).
func (s *RecordStore) AwaitingReceiptInDays(ctx context.Context, companyID, accountID int64, olderThan time.Duration, after dal.AwaitCursor, limit, days int) ([]dal.AwaitingReceipt, dal.AwaitCursor, error) {
	mins := int(olderThan / time.Minute)
	if mins < 1 {
		mins = 1
	}
	db := s.old()
	tables, err := s.dayTables(ctx, db, billingstore.KindMT, companyID, days)
	if err != nil {
		return nil, after, err
	}
	var out []dal.AwaitingReceipt
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
			from := ""
			args := []any{accountID, mins, limit - len(out)}
			if after.MsgID != "" {
				from = " AND (m.accepted_at > @at OR (m.accepted_at = @at AND m.msg_id > @id))"
				args = append(args, sql.Named("at", Time(after.SentAt)), sql.Named("id", Varchar(after.MsgID)))
			}
			cond := ""
			if withDLR {
				cond = recClosedOrTold
			}
			args = append(args, s.ownerArgs()...)
			text := rOn(rOnly(stRecAwaitDays.SQL, "{from}", from, "{own}", s.ownerCond("m.queue_owner"), "{cond}", cond),
				"mt", t.Name, "sub", sub, "dlr", dlr)
			return rQuery(ctx, db, stRecAwaitDays, text, args...)
		}
		rows, err := q(true)
		if MissingTable(err) {
			rows, err = q(false) // no receipt table that day: nothing closed or told
		}
		if MissingTable(err) {
			continue // no answers that day: nothing taken
		}
		if err != nil {
			return nil, after, err
		}
		for rows.Next() {
			var r dal.AwaitingReceipt
			var noRcpt, failOnly int
			var at time.Time
			if err := rows.Scan(&r.MsgID, &r.Sender, &r.Destination, &r.BindSlot, &noRcpt, &failOnly, &r.Owner, &at,
				&r.VendorID, &r.VendorMsgID); err != nil {
				rows.Close()
				return nil, after, err
			}
			r.MsgID = rTrim(r.MsgID)
			r.CompanyID = t.CompanyID
			r.NoReceipt, r.FailureReceiptOnly = noRcpt != 0, failOnly != 0
			out = append(out, r)
			next = dal.AwaitCursor{SentAt: at.UTC(), MsgID: r.MsgID}
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
func (s *RecordStore) SentAwaitingDLRInDays(ctx context.Context, vendorID int64, after string, limit, days int) ([]dal.PendingDLR, error) {
	db := s.old()
	tables, err := s.dayTables(ctx, db, billingstore.KindSub, -1, days)
	if err != nil {
		return nil, err
	}
	var out []dal.PendingDLR
	for _, t := range tables {
		dlr := billingstore.SwapKind(t.Name, billingstore.KindDLR)
		q := func(withDLR bool) (*sql.Rows, error) {
			cond := ""
			if withDLR {
				cond = " AND NOT EXISTS (SELECT 1 FROM {dlr} AS d WHERE d.msg_id = m.msg_id AND d.final_of IS NOT NULL)"
			}
			text := rOn(rOnly(stRecPollDays.SQL, "{cond}", cond), "sub", t.Name, "dlr", dlr)
			return rQuery(ctx, db, stRecPollDays, text, vendorID, Varchar(after), limit)
		}
		rows, err := q(true)
		if MissingTable(err) {
			rows, err = q(false)
		}
		if MissingTable(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			p := dal.PendingDLR{CompanyID: t.CompanyID}
			if err := rows.Scan(&p.MsgID, &p.VendorMsgID, &p.AccountID); err != nil {
				rows.Close()
				return nil, err
			}
			p.MsgID = rTrim(p.MsgID)
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
