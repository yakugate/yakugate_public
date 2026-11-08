package mssql

// Incoming messages, many at a time, and every try to give them — and
// receipts — to their clients (mo_sub_, dlr_sub_; D-140, 5 Nov 2026), on SQL
// Server: what internal/dal/mysql/records_mo.go writes, read from one JSON
// parameter per table. Nothing is ever changed; mo_out_ is not written.

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
)

var _ dal.MORecords = (*RecordStore)(nil)

var recMOSubCols = jcols("try_id", jID, "msg_id", jID, "account_id", jBig, "attempt", jInt, "channel", jTxt, "result", jTxt,
	"final_of", jID, "command_status", jBig, "http_status", jInt, "error_code", jTxt, "error", jTxt, "endpoint_id", jBig,
	"session", jTxt, "method", jTxt, "url", jTxt, "req_headers", jMax, "req_body", jMax, "req_cut", jInt,
	"resp_headers", jMax, "resp_body", jMax, "resp_cut", jInt, "sealed", jInt, "took_ms", jBig, "deployment_id", jTxt,
	"sent_at", jTime)

var recDLRSubCols = jcols("try_id", jID, "msg_id", jID, "client_msg_id", jTxt, "account_id", jBig, "status", jTxt, "stat", jTxt,
	"attempt", jInt, "channel", jTxt, "result", jTxt, "command_status", jBig, "http_status", jInt, "error_code", jTxt,
	"error", jTxt, "endpoint_id", jBig, "session", jTxt, "method", jTxt, "url", jTxt, "req_headers", jMax, "req_body", jMax,
	"req_cut", jInt, "resp_headers", jMax, "resp_body", jMax, "resp_cut", jInt, "sealed", jInt, "took_ms", jBig,
	"deployment_id", jTxt, "sent_at", jTime)

var (
	stRecMOSubInsert = RecordStmts.Add("record.mo_sub.insert", "INSERT INTO {t} ("+jNames(recMOSubCols, "")+") SELECT "+
		jNames(recMOSubCols, "j.")+" FROM "+jRows(recMOSubCols)+jInOrder, false)
	stRecDLRSubInsert = RecordStmts.Add("record.dlr_sub.insert", "INSERT INTO {t} ("+jNames(recDLRSubCols, "")+") SELECT "+
		jNames(recDLRSubCols, "j.")+" FROM "+jRows(recDLRSubCols)+jInOrder, false)
	stRecTryThere = RecordStmts.Add("record.tries.there", `SELECT j.v FROM OPENJSON(@p1)
		WITH (v CHAR(26) COLLATE Latin1_General_100_BIN2 '$') AS j
		WHERE EXISTS (SELECT 1 FROM {t} AS t WHERE t.try_id = j.v)`, false)
	stRecFinalThere = RecordStmts.Add("record.tries.final_there", `SELECT j.v FROM OPENJSON(@p1)
		WITH (v CHAR(26) COLLATE Latin1_General_100_BIN2 '$') AS j
		WHERE EXISTS (SELECT 1 FROM {t} AS t WHERE t.final_of = j.v)`, false)
)

// writeMO files one incoming message (Write): WriteMOs of one.
func (s *RecordStore) writeMO(ctx context.Context, m model.Message) error {
	return s.WriteMOs(ctx, []model.Message{m}, false)
}

// WriteMOs files incoming messages (MySQL's WriteMOs): one INSERT per day
// table, a message already there left as it is, each counted in summary_mo;
// a message written already ended gets its final mo_sub_ row, its try id its
// own id.
func (s *RecordStore) WriteMOs(ctx context.Context, ms []model.Message, once bool) error {
	byTable := map[string][]model.Message{}
	var order []string
	for _, m := range ms {
		m.Direction = model.DirectionMO
		t := recMO(m.Ref())
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
			if recFinal(string(m.Status)) {
				ended = append(ended, recEndedTry(m))
			}
		}
		subTable := ""
		if len(ended) > 0 {
			subTable = recMOSubOf(ended[0])
			// Made before the transaction opens (ensureLookupTables says why).
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
			return flushSums(ctx, tx, d)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func recEndedTry(m model.Message) dal.ClientTry {
	return dal.ClientTry{TryID: m.ID, Kind: dal.TryMO, MsgID: m.ID, CompanyID: m.CompanyID, AccountID: m.AccountID,
		Result: dal.TryResultOf(m.Status), Final: true, ErrorCode: m.ErrorCode, At: time.Now()}
}

// racedAgain runs fn in a transaction, again when it met a row another
// writer made meanwhile (a duplicate key): it reads them this time.
func (s *RecordStore) racedAgain(ctx context.Context, fn func(tx *sql.Tx) error) error {
	var err error
	for range 4 {
		err = s.inTx(ctx, func(tx *sql.Tx) error {
			err := fn(tx)
			if IsDuplicate(err) {
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
// and counts the ones it made; it returns their summary rows.
func (s *RecordStore) insertMOs(ctx context.Context, tx *sql.Tx, table string, ms []model.Message, d *sums) (map[string]moRow, error) {
	byID := map[string]moRow{}
	var rows [][]any
	for _, m := range ms {
		if _, twice := byID[m.ID]; twice {
			continue
		}
		now := billingstore.IDTime(m.ID, time.Now())
		rates := billing.RatesOf(m)
		rows = append(rows, []any{m.ID, m.AccountID, m.Sender, m.Destination, rStr(m.Body), rOr(m.Encoding, "gsm7"), m.Segments,
			rNZ(m.RouteID), rNZ(m.VendorID), recVendorProtocol(m.Transport), rStr(m.MCC), rStr(m.MNC), rStr(m.ErrorCode),
			int(dal.PrivacyOf(m)), rStr(m.QueueOwner), rStr(deployment.ID()),
			rNZ(rates.Sell), rNZ(rates.Buy), rDec(rates.FX), rDec(rates.BaseRate), rDec(rates.BuyFX), rDec(rates.BuyBase), rTimeText(now)})
		byID[m.ID] = recMORowOf(m, rates, now)
	}
	out := map[string]moRow{}
	if len(rows) == 0 {
		return out, nil
	}
	q, err := rQuery(ctx, tx, stRecMOInsert, rOn(stRecMOInsert.SQL, "mo", table), rJSON(rows))
	if err != nil {
		return nil, s.forgetIfMissing(err, table)
	}
	for q.Next() {
		var id string
		if err := q.Scan(&id); err != nil {
			q.Close()
			return nil, err
		}
		id = rTrim(id)
		if r, ok := byID[id]; ok {
			d.addMO(r.key, r.whole())
			out[id] = r
		}
	}
	err = q.Err()
	q.Close()
	if err != nil {
		return nil, err
	}
	return out, nil
}

func recMOSubOf(t dal.ClientTry) string {
	return billingstore.TableOf(billingstore.KindMOSub, t.CompanyID, t.MsgID, t.At)
}

func recDLRSubOf(t dal.ClientTry) string {
	return billingstore.TableOf(billingstore.KindDLRSub, t.CompanyID, t.MsgID, t.At)
}

// WriteClientTries files tries (MySQL's WriteClientTries): one INSERT per
// day table; an MO's first final row is its final_of row, counted in
// summary_mo. once: a try already there is left out.
func (s *RecordStore) WriteClientTries(ctx context.Context, tries []dal.ClientTry, once bool) error {
	type group struct {
		table, moTable, kind string
		ts                   []dal.ClientTry
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
			table = recMOSubOf(t)
		} else {
			t.Kind = dal.TryDLR
			table = recDLRSubOf(t)
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
			if err := s.racedAgain(ctx, func(tx *sql.Tx) error {
				d := newSums()
				if err := s.insertMOTries(ctx, tx, g.table, g.moTable, g.ts, once, d); err != nil {
					return err
				}
				return flushSums(ctx, tx, d)
			}); err != nil {
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

// insertMOTries writes MO tries into table (mo_sub_); the final ones settle
// their message once (MySQL's insertMOTries).
func (s *RecordStore) insertMOTries(ctx context.Context, tx *sql.Tx, table, moTable string, ts []dal.ClientTry, once bool, d *sums) error {
	var finals []string
	seen := map[string]bool{}
	for _, t := range ts {
		if t.Final && !seen[t.MsgID] {
			seen[t.MsgID] = true
			finals = append(finals, t.MsgID)
		}
	}
	finalOf := map[string]bool{}
	if len(finals) > 0 {
		done, err := triesThere(ctx, tx, stRecFinalThere, table, finals)
		if err != nil {
			return err
		}
		rows, err := recReadMORows(ctx, tx, moTable, finals)
		if err != nil && !MissingTable(err) {
			return err
		}
		if rows == nil {
			rows = map[string]moRow{}
		}
		var missing []model.Message
		for _, t := range ts {
			if t.Final && t.Message != nil && !done[t.MsgID] {
				if _, ok := rows[t.MsgID]; !ok {
					m := *t.Message
					m.Status = ""
					missing = append(missing, m)
					rows[t.MsgID] = moRow{}
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
				d.addMO(r.key, r.outcome())
			}
		}
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

// triesThere reads which of ids the statement st finds in table.
func triesThere(ctx context.Context, tx *sql.Tx, st *stmt.Stmt, table string, ids []string) (map[string]bool, error) {
	out := map[string]bool{}
	for from := 0; from < len(ids); from += rChunk {
		part := ids[from:min(from+rChunk, len(ids))]
		q, err := rQuery(ctx, tx, st, rOn(st.SQL, "t", table), rIDs(part))
		if err != nil {
			if MissingTable(err) {
				return out, nil
			}
			return nil, err
		}
		for q.Next() {
			var id string
			if err := q.Scan(&id); err != nil {
				q.Close()
				return nil, err
			}
			out[rTrim(id)] = true
		}
		err = q.Err()
		q.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// insertTries writes ts into table in one statement; finalOf (by try id) the
// rows that are their message's final_of — nil for dlr_sub_.
func (s *RecordStore) insertTries(ctx context.Context, tx *sql.Tx, table string, ts []dal.ClientTry, finalOf map[string]bool, once bool) error {
	if once && len(ts) > 0 {
		ids := make([]string, len(ts))
		for i, t := range ts {
			ids[i] = t.TryID
		}
		have, err := triesThere(ctx, tx, stRecTryThere, table, ids)
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
	isMO := finalOf != nil
	rows := make([][]any, 0, len(ts))
	for _, t := range ts {
		var cs any
		if t.CommandStatus != nil {
			cs = int64(*t.CommandStatus)
		}
		var took any
		if t.Channel != "" && t.TookMS >= 0 {
			took = t.TookMS
		}
		tail := []any{rNZi(t.Attempt), rStr(t.Channel), t.Result}
		if isMO {
			var fo any
			if finalOf[t.TryID] {
				fo = t.MsgID
			}
			tail = append(tail, fo)
		}
		tail = append(tail, cs, rNZi(t.HTTPStatus), rStr(rTrunc(t.ErrorCode, 32)), rStr(rTrunc(t.Error, 512)), rNZ(t.EndpointID),
			rStr(rTrunc(t.Session, 64)), rStr(rTrunc(t.Method, 8)), rStr(rTrunc(t.URL, 2048)), rStr(t.ReqHeaders), rStr(t.ReqBody),
			rBool(t.ReqCut), rStr(t.RespHeaders), rStr(t.RespBody), rBool(t.RespCut), rBool(t.Sealed), took, rStr(deployment.ID()),
			rTimeText(t.At))
		var row []any
		if isMO {
			row = append([]any{t.TryID, t.MsgID, rNZ(t.AccountID)}, tail...)
		} else {
			cm := t.ClientMsgID
			if cm == t.MsgID {
				cm = ""
			}
			row = append([]any{t.TryID, t.MsgID, rStr(rTrunc(cm, 64)), rNZ(t.AccountID), rStr(rTrunc(t.Status, 12)),
				rStr(rTrunc(t.Stat, 8))}, tail...)
		}
		rows = append(rows, row)
	}
	var err error
	if isMO {
		_, err = rExec(ctx, tx, stRecMOSubInsert, rOn(stRecMOSubInsert.SQL, "t", table), rJSON(rows))
	} else {
		_, err = rExec(ctx, tx, stRecDLRSubInsert, rOn(stRecDLRSubInsert.SQL, "t", table), rJSON(rows))
	}
	return s.forgetIfMissing(err, table)
}
