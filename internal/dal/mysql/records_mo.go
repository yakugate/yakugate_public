package mysql

// Incoming messages (mo_ rows) and every try to give them, and receipts, to
// their clients (mo_sub_, dlr_sub_ rows; D-140, 5 Nov 2026). Written once,
// many at a time; nothing is ever changed. mo_out_ (one outcome a message) is
// no longer written: what became of an MO is its final mo_sub_ row.

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
)

var _ dal.MORecords = (*Store)(nil)

// writeMO files one incoming message (Write): WriteMOs of one.
func (s *Store) writeMO(ctx context.Context, m model.Message) error {
	return s.WriteMOs(ctx, []model.Message{m}, false)
}

// WriteMOs files incoming messages, one multi-row INSERT per day table, each
// counted in summary_mo in the same transaction. A message already there is
// left as it is (it is written when it is taken in, and again by a path that
// found no record). A message written already ended (delivered, failed,
// expired: the gateway gave up on it, or it was taken in when the store was
// away and its client has it now) gets its final mo_sub_ row too, its try id
// its own id, so that row is made once however often it is written.
func (s *Store) WriteMOs(ctx context.Context, ms []model.Message, once bool) error {
	byTable := map[string][]model.Message{}
	var order []string
	for _, m := range ms {
		m.Direction = model.DirectionMO
		t := moOf(m.Ref())
		if _, ok := byTable[t]; !ok {
			order = append(order, t)
		}
		byTable[t] = append(byTable[t], m)
	}
	for _, table := range order {
		group := byTable[table]
		if err := s.ensureRecords(ctx, table); err != nil {
			return err
		}
		var ended []dal.ClientTry
		for _, m := range group {
			if finalStatus(string(m.Status)) {
				ended = append(ended, endedTry(m))
			}
		}
		subTable := ""
		if len(ended) > 0 {
			subTable = moSubOf(ended[0])
			if err := s.ensureRecords(ctx, subTable); err != nil {
				return err
			}
		}
		err := s.racedAgain(ctx, func(tx *sql.Tx) error {
			d := newSums()
			if _, err := s.insertMOs(ctx, tx, table, group, d); err != nil {
				return err
			}
			if len(ended) > 0 {
				if err := s.insertMOTries(ctx, tx, subTable, table, ended, true, d); err != nil {
					return err
				}
			}
			return s.flushSums(ctx, tx, d)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// endedTry is the final row of an MO written already ended: no try, the
// gateway's word for what became of it.
func endedTry(m model.Message) dal.ClientTry {
	return dal.ClientTry{TryID: m.ID, Kind: dal.TryMO, MsgID: m.ID, CompanyID: m.CompanyID, AccountID: m.AccountID,
		Result: dal.TryResultOf(m.Status), Final: true, ErrorCode: m.ErrorCode, At: time.Now()}
}

// racedAgain runs fn in a transaction, again when it met a row another
// writer made meanwhile (errRaced, a duplicate key): it reads them this time.
func (s *Store) racedAgain(ctx context.Context, fn func(tx *sql.Tx) error) error {
	var err error
	for range 4 {
		err = s.inTx(ctx, func(tx *sql.Tx) error {
			err := fn(tx)
			if isDupEntry(err) {
				return errRaced
			}
			return err
		})
		if !errors.Is(err, errRaced) {
			return err
		}
	}
	return err
}

// insertMOs writes the messages of ms not in table yet, in one statement,
// and counts them; it returns the summary rows of those it wrote.
func (s *Store) insertMOs(ctx context.Context, tx *sql.Tx, table string, ms []model.Message, d *sums) (map[string]moRow, error) {
	ids := make([]string, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, m.ID)
	}
	have, err := presentIn(ctx, tx, table, ids)
	if err != nil {
		return nil, err
	}
	out := map[string]moRow{}
	const cols = 23
	var args []any
	n := 0
	for _, m := range ms {
		if have[m.ID] {
			continue
		}
		have[m.ID] = true // twice in one batch: once
		// An MO row is written by the gateway that RECEIVED it — the one
		// holding the vendor's bind or serving its callback — so this
		// process's own id is the right answer with nothing to carry.
		// Priced when it was taken in (billing.Moments.MOIn, 26 Oct 2026).
		// received_at is the time in the id — made when the MO came in — as
		// an MT's accepted_at is (28 Oct 2026).
		now := billingstore.IDTime(m.ID, time.Now())
		rates := billing.RatesOf(m)
		args = append(args, m.ID, m.AccountID, m.Sender, m.Destination, nsStr(m.Body), orStr(m.Encoding, "gsm7"), m.Segments,
			nz(m.RouteID), nz(m.VendorID), vendorProtocol(m.Transport), nsStr(m.MCC), nsStr(m.MNC), nsStr(m.ErrorCode),
			int(privacyOf(m)), nsStr(m.QueueOwner), nsStr(deployment.ID()),
			nz(rates.Sell), nz(rates.Buy), nsFx(rates.FX), nsFx(rates.BaseRate), nsFx(rates.BuyFX), nsFx(rates.BuyBase), ackAtArg(now))
		r := moRowOf(m, rates, now)
		d.addMO(r.key, r.whole(), 1)
		out[m.ID] = r
		n++
	}
	if n == 0 {
		return out, nil
	}
	row := "(" + placeholders(cols-1) + ",FROM_UNIXTIME(CAST(? AS DECIMAL(17,3))))"
	_, err = tx.ExecContext(ctx, "INSERT INTO "+table+
		` (msg_id,account_id,sender,destination,body,encoding,segments,route_id,vendor_id,vendor_protocol,mcc,mnc,error_code,
		   mask_body,queue_owner,deployment_id,sell_rate_id,buy_rate_id,fx_rate,base_rate,buy_fx_rate,buy_base_rate,received_at)
		 VALUES `+strings.TrimSuffix(strings.Repeat(row+",", n), ","), args...)
	if err != nil {
		return nil, s.forgetIfMissing(err, table)
	}
	return out, nil
}

// moSubOf and dlrSubOf are the table a try is filed in: its message's
// company and day.
func moSubOf(t dal.ClientTry) string {
	return billingstore.TableOf(billingstore.KindMOSub, t.CompanyID, t.MsgID, t.At)
}

func dlrSubOf(t dal.ClientTry) string {
	return billingstore.TableOf(billingstore.KindDLRSub, t.CompanyID, t.MsgID, t.At)
}

// WriteClientTries files tries (D-140): one multi-row INSERT per day table.
// An MO's first final row is its final_of row, counted in summary_mo (its
// mo_ row written first when it is not there and the try carries the
// message); a final row after it is written as a plain row. once: a try
// already there (its try_id) is left out.
func (s *Store) WriteClientTries(ctx context.Context, tries []dal.ClientTry, once bool) error {
	type group struct {
		table, moTable string
		kind           string
		ts             []dal.ClientTry
	}
	byTable := map[string]*group{}
	var order []string
	for _, t := range tries {
		if t.MsgID == "" {
			continue
		}
		if t.TryID == "" {
			t.TryID = dal.NewTryID()
		}
		if t.At.IsZero() {
			t.At = time.Now()
		}
		t.At = t.At.UTC()
		var table string
		if t.Kind == dal.TryMO {
			table = moSubOf(t)
		} else {
			t.Kind = dal.TryDLR
			table = dlrSubOf(t)
		}
		g := byTable[table]
		if g == nil {
			g = &group{table: table, kind: t.Kind}
			if t.Kind == dal.TryMO {
				g.moTable = recTable(billingstore.KindMO, t.CompanyID, t.MsgID)
			}
			byTable[table] = g
			order = append(order, table)
		}
		g.ts = append(g.ts, t)
	}
	for _, name := range order {
		g := byTable[name]
		if err := s.ensureRecords(ctx, g.table); err != nil {
			return err
		}
		if g.kind == dal.TryMO {
			if err := s.ensureRecords(ctx, g.moTable); err != nil {
				return err
			}
			err := s.racedAgain(ctx, func(tx *sql.Tx) error {
				d := newSums()
				if err := s.insertMOTries(ctx, tx, g.table, g.moTable, g.ts, once, d); err != nil {
					return err
				}
				return s.flushSums(ctx, tx, d)
			})
			if err != nil {
				return err
			}
			continue
		}
		if err := s.racedAgain(ctx, func(tx *sql.Tx) error {
			return s.insertTries(ctx, tx, g.table, g.ts, nil, once)
		}); err != nil {
			return err
		}
	}
	return nil
}

// insertMOTries writes MO tries into table (mo_sub_), moTable being their
// messages' mo_ table: the final ones settle their message once.
func (s *Store) insertMOTries(ctx context.Context, tx *sql.Tx, table, moTable string, ts []dal.ClientTry, once bool, d *sums) error {
	var finals []string
	seen := map[string]bool{}
	for _, t := range ts {
		if t.Final && !seen[t.MsgID] {
			seen[t.MsgID] = true
			finals = append(finals, t.MsgID)
		}
	}
	finalOf := map[string]bool{} // the rows of this batch that settle their message (by try id)
	if len(finals) > 0 {
		done, err := presentBy(ctx, tx, table, "final_of", finals)
		if err != nil {
			return err
		}
		rows, err := readMORows(ctx, tx, moTable, finals, false)
		if err != nil && !isNoSuchTable(err) {
			return err
		}
		if rows == nil {
			rows = map[string]moRow{}
		}
		// A final whose message is not on the log yet writes it, when it
		// carries it (an MO taken in while the store was away).
		var missing []model.Message
		for _, t := range ts {
			if t.Final && t.Message != nil && !done[t.MsgID] {
				if _, ok := rows[t.MsgID]; !ok {
					m := *t.Message
					m.Status = ""
					missing = append(missing, m)
					rows[t.MsgID] = moRow{} // once
				}
			}
		}
		if len(missing) > 0 {
			wrote, err := s.insertMOs(ctx, tx, moTable, missing, d)
			if err != nil {
				return err
			}
			for id, r := range wrote {
				rows[id] = r
			}
		}
		taken := map[string]bool{}
		for _, t := range ts {
			if !t.Final || done[t.MsgID] || taken[t.MsgID] {
				continue
			}
			taken[t.MsgID] = true
			finalOf[t.TryID] = true
			if r, ok := rows[t.MsgID]; ok && r.msgID != "" {
				r.status = string(t.MOStatus())
				d.addMO(r.key, r.outcome(), 1)
			}
		}
		// A giving-up (no try) for an MO that has ended already says nothing:
		// it is left out.
		kept := ts[:0:0]
		for _, t := range ts {
			if t.Final && t.Channel == "" && !finalOf[t.TryID] {
				continue
			}
			kept = append(kept, t)
		}
		ts = kept
	}
	return s.insertTries(ctx, tx, table, ts, finalOf, once)
}

// presentBy reads which of vals column col of table holds.
func presentBy(ctx context.Context, x dbtx, table, col string, vals []string) (map[string]bool, error) {
	have := map[string]bool{}
	for from := 0; from < len(vals); from += 500 {
		part := vals[from:min(from+500, len(vals))]
		args := make([]any, len(part))
		for i, v := range part {
			args[i] = v
		}
		rows, err := x.QueryContext(ctx, "SELECT "+col+" FROM "+table+" WHERE "+col+" IN ("+placeholders(len(part))+")", args...)
		if err != nil {
			if isNoSuchTable(err) {
				return have, nil
			}
			return nil, err
		}
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				rows.Close()
				return nil, err
			}
			have[v] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return have, nil
}

// tryCols are the columns of a try, in the order tryArgs gives them.
const tryCols = "try_id,msg_id,client_msg_id,account_id,status,stat,attempt,channel,result,final_of,command_status,http_status," +
	"error_code,error,endpoint_id,session,method,url,req_headers,req_body,req_cut,resp_headers,resp_body,resp_cut,sealed," +
	"took_ms,deployment_id,sent_at"

// insertTries writes ts into table in one statement; finalOf names (by try
// id) the rows that are their message's final_of (mo_sub_; nil for
// dlr_sub_, which has no such column). once: rows already there are left out.
func (s *Store) insertTries(ctx context.Context, tx *sql.Tx, table string, ts []dal.ClientTry, finalOf map[string]bool, once bool) error {
	if once {
		ids := make([]string, len(ts))
		for i, t := range ts {
			ids[i] = t.TryID
		}
		have, err := presentBy(ctx, tx, table, "try_id", ids)
		if err != nil {
			return err
		}
		var keep []dal.ClientTry
		for _, t := range ts {
			if !have[t.TryID] {
				keep = append(keep, t)
				have[t.TryID] = true
			}
		}
		ts = keep
	}
	if len(ts) == 0 {
		return nil
	}
	cols := tryCols
	isMO := finalOf != nil || strings.HasPrefix(table, billingstore.KindMOSub+"_")
	if isMO {
		cols = strings.Replace(strings.Replace(cols, "client_msg_id,", "", 1), "status,stat,", "", 1)
	} else {
		cols = strings.Replace(cols, "final_of,", "", 1)
	}
	n := strings.Count(cols, ",") + 1
	row := "(" + placeholders(n-1) + ",FROM_UNIXTIME(CAST(? AS DECIMAL(17,3))))"
	args := make([]any, 0, len(ts)*n)
	for _, t := range ts {
		args = append(args, t.TryID, t.MsgID)
		if !isMO {
			args = append(args, nsStr(trunc(clientIDOf(t), 64)))
		}
		args = append(args, nz(t.AccountID))
		if !isMO {
			args = append(args, nsStr(trunc(t.Status, 12)), nsStr(trunc(t.Stat, 8)))
		}
		args = append(args, nz(int64(t.Attempt)), nsStr(t.Channel), t.Result)
		if isMO {
			var fo any
			if finalOf[t.TryID] {
				fo = t.MsgID
			}
			args = append(args, fo)
		}
		var cs any
		if t.CommandStatus != nil {
			cs = int64(*t.CommandStatus)
		}
		args = append(args, cs, nz(int64(t.HTTPStatus)), nsStr(trunc(t.ErrorCode, 32)), nsStr(trunc(t.Error, 512)),
			nz(t.EndpointID), nsStr(trunc(t.Session, 64)), nsStr(trunc(t.Method, 8)), nsStr(trunc(t.URL, 2048)),
			nsStr(t.ReqHeaders), nsStr(t.ReqBody), boolInt(t.ReqCut), nsStr(t.RespHeaders), nsStr(t.RespBody), boolInt(t.RespCut),
			boolInt(t.Sealed), nzTook(t), nsStr(deployment.ID()), ackAtArg(t.At))
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO "+table+" ("+cols+") VALUES "+
		strings.TrimSuffix(strings.Repeat(row+",", len(ts)), ","), args...)
	return s.forgetIfMissing(err, table)
}

// clientIDOf is a receipt try's client_msg_id: the id the client was told,
// only when it is not the message's own.
func clientIDOf(t dal.ClientTry) string {
	if t.ClientMsgID == t.MsgID {
		return ""
	}
	return t.ClientMsgID
}

// nzTook is a try's time in milliseconds; NULL on a row that is no try.
func nzTook(t dal.ClientTry) any {
	if t.Channel == "" || t.TookMS < 0 {
		return nil
	}
	return t.TookMS
}
