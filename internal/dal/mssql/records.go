package mssql

// The record store on SQL Server (dal.RecordStore; DAL plan, step 4): the
// hot store's message logs by company and DAY, written once — mt_ at
// acceptance, sub_ for every vendor answer, dlr_ for the receipts, mo_ and
// mo_out_ for incoming messages, hlr_ for lookups, rejects_ for refusals,
// orphans_0_ for what matched nothing — and their summaries, counted by
// whoever inserts, in the same transaction. Made like the MySQL record store
// (internal/dal/mysql/records.go and mysql.go's record parts; the design:
// products_docs/engineering/data-stores/storage-redesign-2026-10-27.md):
// the same rows, the same answers, "sent once, final once" by the same
// unique keys (uq_msg, uq_answer, uq_final), the same dal.ErrNoRows.
//
// A day's table is made from its template (tpl_day_*) by CreateLike
// (clone.go: from the catalog, under an application lock, once), never
// CREATE TABLE … LIKE. The statements are records_sql.go's shapes: rows as
// one JSON parameter, "insert if not there" as one statement that says what
// it made, a write that met another writer's new row made again (errRaced),
// a deadlock victim (1205) run again (Again). The database runs
// READ_COMMITTED_SNAPSHOT; a read that must see the latest committed row
// under contention (the parts of a long message) takes UPDLOCK, as MySQL's
// FOR UPDATE did.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The record store, its errors, the config store's data keys and the HLR
// store's vendor sessions, registered without touching mssql.go's init.
var _ = serves(func(d *dal.Driver) {
	d.RecordStore = func(cfg config.Config) (dal.RecordStore, error) {
		if cfg.HotDSN == "" {
			return nil, fmt.Errorf("no hot database (YAKU_HOT_DSN)")
		}
		return NewRecordStore(cfg.HotDSN)
	}
	d.RecordFault = recordFault
	d.DataKeys = func(cfg config.Config) (dal.DataKeys, error) {
		db, err := Get(cfg.ConfigDSN)
		if err != nil {
			return nil, err
		}
		return NewDataKeys(db), nil
	}
	d.VendorSessions = func(dsn string) (dal.VendorSessions, error) {
		db, err := Get(dsn)
		if err != nil {
			return nil, err
		}
		return NewVendorSessions(db), nil
	}
})

var _ dal.RecordStore = (*RecordStore)(nil)

// RecordStore is the record store on one SQL Server hot database.
type RecordStore struct {
	db *sql.DB
	// rd is where the reads that may be a moment old go (a read instance);
	// nil = db.
	rd    *Read
	clone *Cloner
	// ensured: the day tables known to be there.
	ensured sync.Map
	// owner is the deployment whose waiting messages this process answers
	// for (deployment.Owner, D-121); "" = the shared ones.
	owner string
	locks *Locks
	// tables: the day tables' lists the readers of waiting messages keep
	// (records_awaiting.go).
	tables recTablesCache
}

// NewRecordStore is the record store on this process's pool for dsn.
func NewRecordStore(dsn string) (*RecordStore, error) {
	db, err := Get(dsn)
	if err != nil {
		return nil, err
	}
	rd, _ := Reader(dsn)
	return newRecordStoreOn(db, rd), nil
}

// newRecordStoreOn is the record store on a pool of the caller's (tests).
func newRecordStoreOn(db *sql.DB, rd *Read) *RecordStore {
	return &RecordStore{db: db, rd: rd, clone: NewCloner(db), owner: deployment.Owner(), locks: &Locks{db: db}}
}

// Close does nothing: the pool is the process's.
func (s *RecordStore) Close() error { return nil }

// ReadsApart says this store has a read instance of its own.
func (s *RecordStore) ReadsApart() bool { return s != nil && s.rd.Separate() }

// old is the pool for a read that may be a moment old.
func (s *RecordStore) old() *sql.DB {
	if s.rd != nil {
		if d := s.rd.DB(); d != nil {
			return d
		}
	}
	return s.db
}

// SetQueueOwner points the store at another deployment's messages (tests).
func (s *RecordStore) SetQueueOwner(owner string) { s.owner = owner }

// ownerCond is MySQL's ownerCond on SQL Server: my rows and the shared ones,
// only the shared ones when I name no deployment. col is the column; it
// names @own, which ownerArgs gives.
func (s *RecordStore) ownerCond(col string) string {
	if s.owner == "" {
		return " AND " + col + " IS NULL"
	}
	return " AND (" + col + " = @own OR " + col + " IS NULL)"
}

func (s *RecordStore) ownerArgs() []any {
	if s.owner == "" {
		return nil
	}
	return []any{sql.Named("own", rText(s.owner))}
}

// ensureRecords makes a log table from its kind's template, once.
func (s *RecordStore) ensureRecords(ctx context.Context, table string) error {
	if _, ok := s.ensured.Load(table); ok {
		return nil
	}
	tpl, ok := billingstore.Templates[billingstore.TableFamily(table)]
	if !ok {
		return fmt.Errorf("not a log table: %q", table)
	}
	if err := s.clone.CreateLike(ctx, tpl, table); err != nil {
		return err
	}
	s.ensured.Store(table, true)
	return nil
}

// ensureLookupTables makes, before a transaction opens, the hlr_ tables the
// records of msgs will file their lookups in (writeLookups, inside the
// transaction). A table is made on the pool, not the transaction's
// connection: made inside, every pooled connection can be held by a
// transaction waiting for one more (the first records of a day, under load).
func (s *RecordStore) ensureLookupTables(ctx context.Context, msgs []model.Message) error {
	for _, m := range msgs {
		if !recHLRLogged(m.HLRSource, m.Quote) {
			continue
		}
		if err := s.ensureRecords(ctx, recTable(billingstore.KindHLR, m.CompanyID, m.ID)); err != nil {
			return err
		}
	}
	return nil
}

// forgetIfMissing drops tables from the "already made" memo when a write
// found one gone (dropped under a running role): the next try makes it.
func (s *RecordStore) forgetIfMissing(err error, tables ...string) error {
	if err != nil && MissingTable(err) {
		for _, t := range tables {
			s.ensured.Delete(t)
		}
	}
	return err
}

// errRaced is a write that met a row another writer inserted between its
// read and its insert: the whole write is made again, and reads it then.
var errRaced = errors.New("a row this write meant to make was made meanwhile")

// recTries is how many times a write a deadlock victim is run (MySQL's
// againIfVictim: 8).
const recTries = 8

// inTx runs fn in a transaction, again when SQL Server makes it a deadlock
// victim.
func (s *RecordStore) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return Again(ctx, recTries, func() error {
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

// recTable is the table of a message's record of one kind.
func recTable(kind string, company int64, msgID string) string {
	return billingstore.TableOf(kind, company, msgID, time.Now())
}

func recMT(ref model.RecordRef) string {
	return recTable(billingstore.KindMT, ref.CompanyID, ref.MsgID)
}
func recMO(ref model.RecordRef) string {
	return recTable(billingstore.KindMO, ref.CompanyID, ref.MsgID)
}
func recDLR(ref model.RecordRef) string {
	return recTable(billingstore.KindDLR, ref.CompanyID, ref.MsgID)
}
func recRej(ref model.RecordRef) string {
	return recTable(billingstore.KindRejects, ref.CompanyID, ref.MsgID)
}
func recSub(ref model.RecordRef) string {
	return billingstore.SwapKind(recMT(ref), billingstore.KindSub)
}

// recFinal says a status closes a message.
func recFinal(st string) bool {
	switch st {
	case "delivered", "failed", "expired", "rejected":
		return true
	}
	return false
}

// recQuoteLookup is the price of the message's number lookup fixed when it
// was accepted (MySQL's quoteLookup).
func recQuoteLookup(q string) (float64, string) {
	if q == "" {
		return 0, ""
	}
	var v struct {
		HLR *struct {
			Charge   float64 `json:"c"`
			Currency string  `json:"cur"`
		} `json:"hlr"`
	}
	if json.Unmarshal([]byte(q), &v) != nil || v.HLR == nil {
		return 0, ""
	}
	return v.HLR.Charge, v.HLR.Currency
}

// recHLRLogged is the rule for the lookup log (MySQL's hlrLogged): every
// lookup an HLR provider answered, one answered from our own data only when
// it was charged.
func recHLRLogged(source, quote string) bool {
	switch source {
	case "dip", "dip_miss":
		return true
	case "":
		return false
	}
	amt, _ := recQuoteLookup(quote)
	return amt > 0
}

// recClip trims s and cuts it to at most n characters (MySQL's clipReason).
func recClip(s string, n int) string {
	s = strings.TrimSpace(s)
	if r := []rune(s); len(r) > n {
		s = string(r[:n])
	}
	return s
}

// The sizes of a refusal's reason_code and reason_text.
const (
	recReasonCodeMax = 191
	recReasonTextMax = 512
)

var (
	stRecPresent = RecordStmts.Add("record.present", "SELECT TOP (500) t.msg_id FROM "+rIDsFrom+"{tbl} AS t ON t.msg_id = j.v", true)
	stRecTaken   = RecordStmts.Add("record.sub.accepted_in", "SELECT TOP (100000) t.msg_id, t.attempt FROM "+rIDsFrom+
		"{sub} AS t ON t.msg_id = j.v WHERE t.result = 'accepted'", true)
	stRecFinals = RecordStmts.Add("record.dlr.finals_in", "SELECT TOP (500) t.final_of FROM "+
		"OPENJSON(@p1) WITH (v CHAR(26) COLLATE Latin1_General_100_BIN2 '$') AS j INNER LOOP JOIN {dlr} AS t ON t.final_of = j.v", true)
)

// recPresentIn reads which of ids have a row in table (MySQL's presentIn).
// A table that does not exist has none.
func recPresentIn(ctx context.Context, x Querier, table string, ids []string) (map[string]bool, error) {
	have := map[string]bool{}
	for from := 0; from < len(ids); from += rChunk {
		part := ids[from:min(from+rChunk, len(ids))]
		rows, err := rQuery(ctx, x, stRecPresent, rOn(stRecPresent.SQL, "tbl", table), rIDs(part))
		if err != nil {
			if MissingTable(err) {
				return have, nil
			}
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			have[rTrim(id)] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return have, nil
}

// recAcceptedIn reads which (message, try) pairs have an accepted answer in
// sub already (MySQL's acceptedIn).
func recAcceptedIn(ctx context.Context, x Querier, sub string, ids []string) (map[string]map[int]bool, error) {
	out := map[string]map[int]bool{}
	for from := 0; from < len(ids); from += rChunk {
		part := ids[from:min(from+rChunk, len(ids))]
		rows, err := rQuery(ctx, x, stRecTaken, rOn(stRecTaken.SQL, "sub", sub), rIDs(part))
		if err != nil {
			if MissingTable(err) {
				return out, nil
			}
			return nil, err
		}
		for rows.Next() {
			var id string
			var att int
			if err := rows.Scan(&id, &att); err != nil {
				rows.Close()
				return nil, err
			}
			id = rTrim(id)
			if out[id] == nil {
				out[id] = map[int]bool{}
			}
			out[id][att] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// recFinalsIn reads which of ids have a closing receipt in table already
// (MySQL's finalsIn).
func recFinalsIn(ctx context.Context, x Querier, table string, ids []string) (map[string]bool, error) {
	out := map[string]bool{}
	for from := 0; from < len(ids); from += rChunk {
		part := ids[from:min(from+rChunk, len(ids))]
		rows, err := rQuery(ctx, x, stRecFinals, rOn(stRecFinals.SQL, "dlr", table), rIDs(part))
		if err != nil {
			if MissingTable(err) {
				return out, nil
			}
			return nil, err
		}
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil {
				out[rTrim(id)] = true
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ---- outgoing ----

// recMTCols are the columns of an outgoing record (MySQL's mtCols), with the
// types their JSON values are read as.
var recMTCols = jcols(
	"msg_id", jID, "trace_id", jTxt, "request_id", jTxt, "account_id", jBig, "company_id", jBig,
	"sender", jTxt, "destination", jTxt, "body", jMax, "body_enc", jTxt, "campaign_hash", jTxt, "campaign_id", jTxt,
	"encoding", jTxt, "segments", jInt, "udh_ref", jInt, "udh_total", jInt, "udh_seq", jInt, "label", jTxt,
	"ap1", jTxt, "ap2", jTxt, "apg1", jTxt, "apg2", jTxt, "priority", jInt, "client_protocol", jTxt, "ip_address", jTxt,
	"mcc", jTxt, "mnc", jTxt, "hlr_source", jTxt, "hlr_ported", jInt, "route_id", jBig, "template_id", jBig,
	"no_receipt", jInt, "failure_receipt_only", jInt, "want_intermediate", jInt, "bind_slot", jInt, "mask_body", jInt,
	"queue_owner", jTxt, "deployment_id", jTxt, "sender_by", jTxt, "client_sender", jTxt, "client_ref", jTxt,
	"spam_flag", jTxt, "sell_rate_id", jBig, "dlr_sell_rate_id", jBig, "hlr_sell_rate_id", jBig, "fx_rate", jDec,
	"base_rate", jDec, "accepted_at", jTime)

// recInsertOnce is "insert the JSON's rows whose key is not there": the
// statement says the ids it made.
func recInsertOnce(table string, cols []jcol, keyMatch string) string {
	return "INSERT INTO {" + table + "} (" + jNames(cols, "") + ") OUTPUT INSERTED.msg_id SELECT " + jNames(cols, "j.") +
		" FROM " + jRows(cols) + " WHERE NOT EXISTS (SELECT 1 FROM {" + table + "} AS t WHERE " + keyMatch + ")" + jInOrder
}

var stRecMTInsert = RecordStmts.Add("record.mt.insert", recInsertOnce("mt", recMTCols, "t.msg_id = j.msg_id"), true)

// recAcceptRates are a message's rate ids as its acceptance fixed them.
func recAcceptRates(m model.Message) billing.RecordRates {
	return billing.RatesOf(model.Message{Quote: m.Quote})
}

func recMTArgs(m model.Message, rates billing.RecordRates, acc time.Time) []any {
	return []any{
		m.ID, rStr(m.TraceID), rStr(m.RequestID), m.AccountID, rNZ(m.CompanyID), m.Sender, m.Destination,
		rStr(recStoredBody(m)), recBodyEnc(m),
		rStr(m.CampaignHash), rStr(m.CampaignID),
		rOr(m.Encoding, "gsm7"), m.Segments,
		m.ConcatRef, m.ConcatTotal, m.ConcatSeq,
		rStr(m.Channel),
		rStr(m.AP1), rStr(m.AP2), rStr(m.APG1), rStr(m.APG2), rNZi(m.Priority),
		recClientProtocol(m.Ingress), rStr(m.IPAddress),
		rStr(m.MCC), rStr(m.MNC), m.HLRSource, rBool(m.HLRPorted),
		rNZ(m.RouteID), rNZ(m.TemplateID),
		rBool(m.NoReceipt), rBool(m.FailureReceiptOnly), rBool(m.WantsIntermediateReceipt()), m.BindSlot,
		int(dal.PrivacyOf(m)),
		rStr(m.QueueOwner), rStr(m.DeploymentID),
		rStr(m.SenderBy), recSenderByClient(m),
		rStr(m.IdempotencyKey),
		rStr(m.SpamFlag),
		rNZ(rates.Sell), rNZ(rates.DLRSell), rNZ(rates.HLRSell), rDec(rates.FX), rDec(rates.BaseRate),
		rTimeText(acc),
	}
}

// recClientProtocol is how the client sent a message; NULL when not known.
func recClientProtocol(t model.Transport) any {
	switch t {
	case model.TransportHTTP, model.TransportSMPP:
		return string(t)
	}
	return nil
}

// recVendorProtocol is how a message went to its vendor; NULL when it did not.
func recVendorProtocol(t model.Transport) any {
	switch t {
	case model.TransportHTTP, model.TransportSMPP, "soap", "blackhole":
		return string(t)
	}
	return nil
}

// WriteAccepted files outgoing messages' records at ACCEPTANCE: their mt_
// rows, their lookups and the accepted counts, one transaction per table. A
// message that is there already is left as it is and counted nothing.
func (s *RecordStore) WriteAccepted(ctx context.Context, msgs []model.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	msgs = append([]model.Message(nil), msgs...)
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].ID < msgs[j].ID })
	byTable := map[string][]model.Message{}
	var tables []string
	for _, m := range msgs {
		t := dal.MTLogTableFor(m)
		if _, ok := byTable[t]; !ok {
			tables = append(tables, t)
		}
		byTable[t] = append(byTable[t], m)
	}
	sort.Strings(tables)
	for _, table := range tables {
		if err := s.ensureRecords(ctx, table); err != nil {
			return err
		}
		group := byTable[table]
		if err := s.ensureLookupTables(ctx, group); err != nil {
			return err
		}
		var err error
		for try := 0; try < 3; try++ {
			err = s.inTx(ctx, func(tx *sql.Tx) error {
				d := newSums()
				if _, err := s.insertAccepted(ctx, tx, table, group, d, time.Now()); err != nil {
					return err
				}
				return flushSums(ctx, tx, d)
			})
			if !errors.Is(err, errRaced) {
				break
			}
		}
		if err != nil {
			return s.forgetIfMissing(err, table)
		}
	}
	return nil
}

// insertAccepted inserts, in tx, the mt_ rows of msgs that table does not
// have yet, with their lookups and their accepted counts. It answers the
// messages it made.
func (s *RecordStore) insertAccepted(ctx context.Context, tx Querier, table string, msgs []model.Message, d *sums, now time.Time) ([]model.Message, error) {
	byID := map[string]model.Message{}
	var rows [][]any
	var order []string
	for _, m := range msgs {
		if _, seen := byID[m.ID]; seen {
			continue
		}
		byID[m.ID] = m
		order = append(order, m.ID)
		rows = append(rows, recMTArgs(m, recAcceptRates(m), billingstore.IDTime(m.ID, now)))
	}
	if len(rows) == 0 {
		return nil, nil
	}
	made := map[string]bool{}
	for from := 0; from < len(rows); from += rChunk {
		q, err := rQuery(ctx, tx, stRecMTInsert, rOn(stRecMTInsert.SQL, "mt", table), rJSON(rows[from:min(from+rChunk, len(rows))]))
		if err != nil {
			if IsDuplicate(err) {
				return nil, errRaced
			}
			return nil, err
		}
		for q.Next() {
			var id string
			if err := q.Scan(&id); err != nil {
				q.Close()
				return nil, err
			}
			made[rTrim(id)] = true
		}
		err = q.Err()
		q.Close()
		if err != nil {
			if IsDuplicate(err) {
				return nil, errRaced
			}
			return nil, err
		}
	}
	var out []model.Message
	for _, id := range order {
		if !made[id] {
			continue
		}
		m := byID[id]
		out = append(out, m)
		row := recAcceptedRowOf(m, now)
		d.addMT(row.key, row.accepted())
		d.addLookup(row)
	}
	if err := s.writeLookups(ctx, tx, out, false); err != nil {
		return nil, err
	}
	return out, nil
}

// recAcceptedRowOf is a message's summary row as its acceptance counts it:
// no vendor, no buying rate.
func recAcceptedRowOf(m model.Message, now time.Time) mtRow {
	r := recMTRowOf(m, recAcceptRates(m), time.Time{}, "", now)
	r.key.vendor, r.key.buy = 0, 0
	return r
}

// recSubmittedRowOf is a message's summary row as a vendor's taking of it
// counts it: its vendor and that vendor's buying rate.
func recSubmittedRowOf(m model.Message, ackAt time.Time, now time.Time) mtRow {
	r := recMTRowOf(m, recAcceptRates(m), ackAt, "", now)
	r.key.buy = billing.RatesOf(m).Buy
	return r
}

// recAttemptOf is the try a record's write is about.
func recAttemptOf(r dal.TerminalRecord) int {
	for _, a := range r.Answers {
		if a.Result == model.AnswerAccepted || a.Result == "" {
			if a.Attempt > 0 {
				return a.Attempt
			}
		}
	}
	return model.TryOf(r.Msg)
}

// writeSubmitGroup writes what one table's records of a vendor role's batch
// say about their SUBMISSION, in ONE transaction (MySQL's writeSubmitGroup):
// each message's mt_ row if it has none yet, every vendor answer (sub_), the
// submitted counts and open-message rows of a message a vendor took, the
// receipt the gateway makes at submission. Everything is written once: an
// answer by its key, a submission by its accepted answer, a close by
// final_of.
func (s *RecordStore) writeSubmitGroup(ctx context.Context, table string, group []dal.TerminalRecord, receiptRow bool) error {
	now := time.Now()
	sub := billingstore.SwapKind(table, billingstore.KindSub)
	dlr := billingstore.SwapKind(table, billingstore.KindDLR)
	for _, t := range []string{table, sub, dlr} {
		if err := s.ensureRecords(ctx, t); err != nil {
			return err
		}
	}
	msgsOf := make([]model.Message, 0, len(group))
	for _, r := range group {
		msgsOf = append(msgsOf, r.Msg)
	}
	if err := s.ensureLookupTables(ctx, msgsOf); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	d := newSums()
	msgs := make([]model.Message, 0, len(group))
	ids := make([]string, 0, len(group))
	for _, r := range group {
		msgs = append(msgs, r.Msg)
		ids = append(ids, r.Msg.ID)
	}
	if _, err := s.insertAccepted(ctx, tx, table, msgs, d, now); err != nil {
		return err
	}
	seen, err := recAcceptedIn(ctx, tx, sub, ids)
	if err != nil {
		return err
	}
	closed, err := recFinalsIn(ctx, tx, dlr, ids)
	if err != nil {
		return err
	}
	newTaken, firstTaken := map[string]bool{}, map[string]bool{}
	var open []dal.TerminalRecord
	for _, r := range group {
		if r.AckAt.IsZero() {
			continue
		}
		att := recAttemptOf(r)
		if seen[r.Msg.ID][att] {
			continue
		}
		if seen[r.Msg.ID] == nil {
			seen[r.Msg.ID] = map[int]bool{}
			firstTaken[r.Msg.ID] = true
		}
		seen[r.Msg.ID][att] = true
		newTaken[r.Msg.ID] = true
		row := recSubmittedRowOf(r.Msg, r.AckAt, now)
		d.addMT(row.key, row.submitted())
		open = append(open, r)
	}
	var rcpt []dal.TerminalRecord
	for _, r := range group {
		final := recFinal(string(recOrStatus(r.Msg.Status)))
		switch {
		case final && !closed[r.Msg.ID]:
			closed[r.Msg.ID] = true
			rcpt = append(rcpt, r)
		case !final && receiptRow && firstTaken[r.Msg.ID]:
			rcpt = append(rcpt, r)
		}
	}
	if len(rcpt) > 0 {
		if err := s.writeSubmitReceipts(ctx, tx, dlr, rcpt, d, now); err != nil {
			if IsDuplicate(err) {
				return errRaced // another writer closed one meanwhile: read again
			}
			return err
		}
	}
	if err := s.noteOpen(ctx, tx, open); err != nil {
		return err
	}
	if err := s.writeAnswers(ctx, tx, sub, recPricedAnswers(group)); err != nil {
		return err
	}
	if err := flushSums(ctx, tx, d); err != nil {
		return err
	}
	return tx.Commit()
}

// recPricedAnswers is every vendor answer a group's records carry, each
// with what the vendor's try cost and the round it was sent in (MySQL's
// pricedAnswers).
func recPricedAnswers(group []dal.TerminalRecord) []model.SubmitAnswer {
	var out []model.SubmitAnswer
	for _, r := range group {
		m := r.Msg
		rates := billing.RatesOf(m)
		for _, a := range recWithAcceptedAnswer(r) {
			if a.Result == model.AnswerAccepted && a.VendorMsgID == "" && (a.VendorID == 0 || a.VendorID == m.VendorID) {
				a.VendorMsgID = r.VendorMsgID
			}
			if a.VendorID == 0 || a.VendorID == m.VendorID {
				if a.BuyRateID == 0 {
					a.BuyRateID, a.BuyFX, a.BuyBase = rates.Buy, rates.BuyFX, rates.BuyBase
				}
				if a.RouteID == 0 {
					a.RouteID, a.RouteLevel, a.SendRound = m.RouteID, m.RouteLevel, m.SendRound
				}
			}
			out = append(out, a)
		}
	}
	return out
}

// recWithAcceptedAnswer is a record's answers, with the vendor's acceptance
// made from the record itself when the vendor took the message and no
// accepted answer of that try came with it (MySQL's withAcceptedAnswer).
func recWithAcceptedAnswer(r dal.TerminalRecord) []model.SubmitAnswer {
	if r.AckAt.IsZero() {
		return r.Answers
	}
	att := recAttemptOf(r)
	for _, a := range r.Answers {
		if a.Result == model.AnswerAccepted && (a.Attempt == 0 || a.Attempt == att) {
			return r.Answers
		}
	}
	m := r.Msg
	parts := m.SentParts
	if parts <= 0 {
		parts = m.Segments
	}
	if parts <= 0 {
		parts = 1
	}
	return append(append([]model.SubmitAnswer(nil), r.Answers...), model.SubmitAnswer{
		MsgID: m.ID, AccountID: m.AccountID, CompanyID: m.CompanyID,
		Part: 1, Parts: parts, Attempt: att,
		VendorID: m.VendorID, VendorMsgID: r.VendorMsgID, VendorProtocol: string(m.Transport),
		Result: model.AnswerAccepted, SentAt: r.AckAt, AnsweredAt: r.AckAt,
		Deployment: rOr(m.SentDeploymentID, m.DeploymentID),
	})
}

// recSubCols are the columns of a vendor's answer (MySQL's subCols).
var recSubCols = jcols(
	"msg_id", jID, "account_id", jBig, "part", jInt, "parts", jInt, "attempt", jInt, "vendor_id", jBig,
	"vendor_msg_id", jTxt, "vendor_protocol", jTxt, "command_status", jBig, "http_status", jInt,
	"result", jTxt, "code", jTxt, "words", jTxt, "deployment_id", jTxt, "session", jTxt, "buy_rate_id", jBig,
	"buy_fx_rate", jDec, "buy_base_rate", jDec, "route_id", jBig, "route_level", jInt, "send_round", jInt,
	"method", jTxt, "url", jTxt, "req_headers", jMax, "req_body", jMax, "req_cut", jInt, "resp_headers", jMax,
	"resp_body", jMax, "resp_cut", jInt, "sealed", jInt, "took_ms", jBig,
	"sent_at", jTime, "answered_at", jTime)

// The answers' insert leaves out an answer whose key (a message, a part, a
// try, a result: uq_answer) is there: MySQL's INSERT IGNORE.
var stRecSubInsert = RecordStmts.Add("record.sub.insert", "INSERT INTO {sub} ("+jNames(recSubCols, "")+") SELECT "+
	jNames(recSubCols, "j.")+" FROM "+jRows(recSubCols)+" WHERE NOT EXISTS (SELECT 1 FROM {sub} AS t"+
	" WHERE t.msg_id = j.msg_id AND t.part = j.part AND t.attempt = j.attempt AND t.result = j.result)"+jInOrder, true)

// writeAnswers files vendor answers in table (sub_), the ones there already
// left out by their key.
func (s *RecordStore) writeAnswers(ctx context.Context, x Querier, table string, as []model.SubmitAnswer) error {
	if len(as) == 0 || table == "" {
		return nil
	}
	if err := s.ensureRecords(ctx, table); err != nil {
		return err
	}
	// MySQL's INSERT IGNORE keeps the first of two answers with one key.
	seen := map[string]bool{}
	var rows [][]any
	for _, a := range as {
		row := recAnswerArgs(a)
		k := fmt.Sprintf("%v\x00%v\x00%v\x00%v", row[0], row[2], row[4], row[10])
		if seen[k] {
			continue
		}
		seen[k] = true
		rows = append(rows, row)
	}
	for from := 0; from < len(rows); from += rChunk {
		text := rOn(stRecSubInsert.SQL, "sub", table)
		arg := rJSON(rows[from:min(from+rChunk, len(rows))])
		if _, err := recIgnoring(ctx, x, stRecSubInsert, text, arg); err != nil {
			return s.forgetIfMissing(err, table)
		}
	}
	return nil
}

// recIgnoring runs an "insert if not there" and runs it again when another
// writer's row of the same key landed between its read and its write (the
// unique key said no): its rows are then read as there, as MySQL's INSERT
// IGNORE leaves them. In a transaction only the statement is undone.
func recIgnoring(ctx context.Context, x Querier, st interface{ Observe(time.Duration, int64) }, text string, args ...any) (int64, error) {
	var err error
	for try := 0; try < 5; try++ {
		start := time.Now()
		var res sql.Result
		res, err = x.ExecContext(ctx, text, args...)
		if err == nil {
			n, _ := res.RowsAffected()
			st.Observe(time.Since(start), n)
			return n, nil
		}
		st.Observe(time.Since(start), 0)
		if !IsDuplicate(err) {
			return 0, err
		}
	}
	return 0, err
}

func recAnswerArgs(a model.SubmitAnswer) []any {
	part, parts, attempt := a.Part, a.Parts, a.Attempt
	if part < 1 {
		part = 1
	}
	if parts < part {
		parts = part
	}
	if attempt < 1 {
		attempt = 1
	}
	var cs any
	if a.HasStatus {
		cs = a.CommandStatus
	}
	c := recCallArgsOf(a.Call)
	result := a.Result
	if result == "" {
		result = model.AnswerAccepted
	}
	return []any{a.MsgID, rNZ(a.AccountID), part, parts, attempt, rNZ(a.VendorID), rStr(rTrunc(a.VendorMsgID, 64)),
		recVendorProtocol(model.Transport(a.VendorProtocol)), cs, rNZi(a.HTTPStatus), result, rStr(rTrunc(a.Code, 32)),
		rStr(rTrunc(a.Words, 255)), rStr(rOr(a.Deployment, deployment.ID())), rStr(rTrunc(a.Session, 64)),
		rNZ(a.BuyRateID), rDec(a.BuyFX), rDec(a.BuyBase), rNZ(a.RouteID), rNZi(a.RouteLevel), rNZi(a.SendRound),
		c.method, c.url, c.reqHeaders, c.reqBody, c.reqCut, c.respHeaders, c.respBody, c.respCut, c.sealed, c.took,
		rTimeText(a.SentAt), rTimeText(a.AnsweredAt)}
}

// recCallArgs are an answer's call columns (hot/0077; MySQL's callArgsOf).
type recCallArgs struct {
	method, url, reqHeaders, reqBody, respHeaders, respBody, took any
	reqCut, respCut, sealed                                       int
}

func recCallArgsOf(c *model.VendorCall) recCallArgs {
	if c == nil {
		return recCallArgs{}
	}
	var took any
	if c.TookMS > 0 {
		took = c.TookMS
	}
	return recCallArgs{method: rStr(rTrunc(c.Method, 8)), url: rStr(rTrunc(c.URL, 2048)), reqHeaders: rStr(c.ReqHeaders),
		reqBody: rStr(c.ReqBody), reqCut: rBool(c.ReqCut), respHeaders: rStr(c.RespHeaders), respBody: rStr(c.RespBody),
		respCut: rBool(c.RespCut), sealed: rBool(c.Sealed), took: took}
}

// WriteAnswers files vendor answers that end nothing by themselves, each in
// the sub_ table of its message's company and day.
func (s *RecordStore) WriteAnswers(ctx context.Context, as []model.SubmitAnswer) error {
	byTable := map[string][]model.SubmitAnswer{}
	var tables []string
	for _, a := range as {
		t := recTable(billingstore.KindSub, a.CompanyID, a.MsgID)
		if _, ok := byTable[t]; !ok {
			tables = append(tables, t)
		}
		byTable[t] = append(byTable[t], a)
	}
	sort.Strings(tables)
	for _, t := range tables {
		if err := s.writeAnswers(ctx, s.db, t, byTable[t]); err != nil {
			return err
		}
	}
	return nil
}

// The sources of a receipt row (hot/0072).
const (
	recSourceVendor = "vendor"
	recSourceSystem = "system"
)

// recReceiptSource is a receipt row's source: the one it was given, else
// system for a `policy:` row, else vendor (MySQL's receiptSource).
func recReceiptSource(given, raw string) string {
	switch given {
	case recSourceVendor, recSourceSystem:
		return given
	}
	if strings.HasPrefix(raw, "policy:") {
		return recSourceSystem
	}
	return recSourceVendor
}

// recOwnStat is the word of a receipt the gateway makes when a message ends
// at submission (MySQL's ownStat).
func recOwnStat(st, code string) string {
	switch code {
	case "MAX_ATTEMPTS_REACHED", "MAX_ATTEMPTS":
		return "UNDELIV"
	case "VALIDITY_EXPIRED", "EXPIRED":
		return "EXPIRED"
	}
	if st == string(model.StatusFailed) || st == string(model.StatusRejected) {
		if code != "" {
			return "REJECTD"
		}
	}
	return recStatFor(st)
}

// recStatFor is the SMPP stat word for one of the gateway's statuses
// (MySQL's statForStatus).
func recStatFor(status string) string {
	switch status {
	case "delivered":
		return "DELIVRD"
	case "failed":
		return "UNDELIV"
	case "expired":
		return "EXPIRED"
	case "sent":
		return "ACCEPTD"
	case "rejected":
		return "REJECTD"
	case "queued":
		return "ENROUTE"
	}
	return "UNKNOWN"
}

// recOrStatus is s, queued when empty.
func recOrStatus(s model.Status) model.Status {
	if s == "" {
		return model.StatusQueued
	}
	return s
}

// recDLRCols are the columns of a receipt row (MySQL's receiptRowCols).
var recDLRCols = jcols(
	"msg_id", jID, "account_id", jBig, "source", jTxt, "vendor_id", jBig, "vendor_msg_id", jTxt, "part", jInt,
	"attempt", jInt, "status", jTxt, "stat", jTxt, "error_code", jTxt, "raw", jTxt, "final_of", jID,
	"deployment_id", jTxt, "done_date", jSec, "sell_rate_id", jBig, "buy_rate_id", jBig, "fx_rate", jDec, "delivered_at", jTime)

var stRecDLRInsert = RecordStmts.Add("record.dlr.insert", "INSERT INTO {dlr} ("+jNames(recDLRCols, "")+") SELECT "+
	jNames(recDLRCols, "j.")+" FROM "+jRows(recDLRCols)+jInOrder, true)

// recInsertReceipts inserts receipt rows (each in recDLRCols' order) in one
// statement a chunk.
func recInsertReceipts(ctx context.Context, x Querier, table string, rows [][]any) error {
	text := rOn(stRecDLRInsert.SQL, "dlr", table)
	for from := 0; from < len(rows); from += rChunk {
		if _, err := rExec(ctx, x, stRecDLRInsert, text, rJSON(rows[from:min(from+rChunk, len(rows))])); err != nil {
			return err
		}
	}
	return nil
}

// writeSubmitReceipts files the receipts the gateway makes at submission
// (source system): its own ACCEPTD of a message an SMPP vendor took, and the
// receipt that CLOSES a message final at submission (MySQL's
// writeSubmitReceipts).
func (s *RecordStore) writeSubmitReceipts(ctx context.Context, x Querier, dlrTable string, recs []dal.TerminalRecord, d *sums, now time.Time) error {
	rows := make([][]any, 0, len(recs))
	for _, r := range recs {
		m := r.Msg
		st := string(recOrStatus(m.Status))
		if st == string(model.StatusRejected) {
			st = string(model.StatusFailed)
		}
		final := recFinal(st)
		code := recClip(rOr(r.ErrorCode, m.ErrorCode), 32)
		var closes, vendor, raw any
		stat := recStatFor(st)
		if final {
			closes = m.ID
			if r.AckAt.IsZero() {
				stat = recOwnStat(st, code)
				raw = rStr(recClip(m.ErrorText, 512))
			}
		}
		if !r.AckAt.IsZero() {
			vendor = rNZ(m.VendorID)
		}
		rows = append(rows, []any{m.ID, m.AccountID, recSourceSystem, vendor, rStr(r.VendorMsgID), nil, recAttemptOf(r), st,
			stat, rStr(code), raw, closes, rStr(rOr(m.SentDeploymentID, m.DeploymentID)), nil, nil, nil, nil, rTimeText(now)})
		vid := m.VendorID
		if r.AckAt.IsZero() {
			vid = 0
		}
		d.addReceipt(now, m.AccountID, vid, stat, code)
		if final {
			row := recAcceptedRowOf(m, now)
			if !r.AckAt.IsZero() {
				row = recSubmittedRowOf(m, r.AckAt, now)
			}
			row.status = st
			d.addMT(row.key, row.outcome())
		}
	}
	return recInsertReceipts(ctx, x, dlrTable, rows)
}

// recHLRCols are a lookup row's columns.
var recHLRCols = jcols("msg_id", jID, "account_id", jBig, "destination", jTxt, "source", jTxt, "mcc", jTxt, "mnc", jTxt,
	"ported", jInt, "sell_rate_id", jBig, "fx_rate", jDec, "base_rate", jDec, "looked_up_at", jTime, "deployment_id", jTxt,
	"operator_name", jTxt, "ccc", jTxt, "refused_reason", jTxt,
	// the HLR provider's call (hot/0080, 6 Nov 2026), dal.LookupCallCols' order
	"method", jTxt, "url", jTxt, "req_headers", jMax, "req_body", jMax, "req_cut", jInt, "http_status", jInt,
	"resp_headers", jMax, "resp_body", jMax, "resp_cut", jInt, "took_ms", jBig, "call_error", jTxt)

var stRecHLRInsert = RecordStmts.Add("record.hlr.insert", "INSERT INTO {hlr} ("+jNames(recHLRCols, "")+") SELECT "+
	jNames(recHLRCols, "j.")+" FROM "+jRows(recHLRCols)+jInOrder, true)

// writeLookups files the lookups worth logging of the records a write made,
// in hlr_<company>_<day> (MySQL's writeLookups).
// refused (6 Nov 2026): the messages are refusals written now; each live
// lookup (dip, dip_miss) is logged with the refusal's code, never charged.
func (s *RecordStore) writeLookups(ctx context.Context, x Querier, made []model.Message, refused bool) error {
	byTable := map[string][]model.Message{}
	var order []string
	for _, m := range made {
		if refused && m.HLRSource != "dip" && m.HLRSource != "dip_miss" || !refused && !recHLRLogged(m.HLRSource, m.Quote) {
			continue
		}
		t := recTable(billingstore.KindHLR, m.CompanyID, m.ID)
		if _, ok := byTable[t]; !ok {
			order = append(order, t)
		}
		byTable[t] = append(byTable[t], m)
	}
	now := time.Now()
	for _, table := range order {
		if err := s.ensureRecords(ctx, table); err != nil {
			return err
		}
		ms := byTable[table]
		rows := make([][]any, 0, len(ms))
		for _, m := range ms {
			rates := billing.RatesOf(m)
			var rate, reason any
			if amt, _ := recQuoteLookup(m.Quote); amt > 0 && !refused {
				rate = rNZ(rates.HLRSell)
			}
			if refused {
				code, _ := model.SplitReason(m.RejectReason)
				reason = rStr(code)
			}
			at := billingstore.IDTime(m.ID, now)
			rows = append(rows, append([]any{m.ID, rNZ(m.AccountID), m.Destination, m.HLRSource, rStr(m.MCC), rStr(m.MNC),
				rBool(m.HLRPorted), rate, rDec(rates.FX), rDec(rates.BaseRate), rTimeText(at), rStr(m.DeploymentID),
				rStr(m.HLROperator), rStr(m.HLRCCC), reason}, dal.LookupCallArgs(m.HLRCall)...))
		}
		if _, err := rExec(ctx, x, stRecHLRInsert, rOn(stRecHLRInsert.SQL, "hlr", table), rJSON(rows)); err != nil {
			return err
		}
	}
	return nil
}

// recNowSec is the database clock cut to the second, as MySQL's NOW() into a
// DATETIME(0) (SQL Server would round SYSUTCDATETIME() to the column).
const recNowSec = "CAST(CONVERT(VARCHAR(19), SYSUTCDATETIME(), 120) AS DATETIME2(0))"

var (
	stRecOpenNote = RecordStmts.Add("record.open.note", `INSERT INTO open_msgs (msg_id, company_id, account_id, vendor_id, vendor_msg_id, destination, sent_at, wait_until, queue_owner, bill)
		SELECT j.m, j.c, j.a, j.v, j.vm, j.d, `+recNowSec+`, DATEADD(SECOND, j.w, `+recNowSec+`), j.o, j.b
		FROM OPENJSON(@p1) WITH (m CHAR(26) COLLATE Latin1_General_100_BIN2 '$[0]', c BIGINT '$[1]', a BIGINT '$[2]', v BIGINT '$[3]',
			vm NVARCHAR(4000) '$[4]', d NVARCHAR(4000) '$[5]', w BIGINT '$[6]', o NVARCHAR(4000) '$[7]', b NVARCHAR(2000) '$[8]') AS j
		WHERE NOT EXISTS (SELECT 1 FROM open_msgs AS t WHERE t.msg_id = j.m)`, true)
	stRecOpenClose = RecordStmts.Add("record.open.close", "DELETE t FROM "+rIDsFrom+"open_msgs AS t ON t.msg_id = j.v", true)
)

// noteOpen files in open_msgs the messages of a write that wait for a
// receipt, when they are kept in the SQL database (no Redis).
func (s *RecordStore) noteOpen(ctx context.Context, x Querier, made []dal.TerminalRecord) error {
	if !dal.OpenMessagesInSQL() {
		return nil
	}
	seen := map[string]bool{}
	var rows [][]any
	for _, r := range made {
		m := r.Msg
		if m.Status != model.StatusSent || m.ReceiptWait() <= 0 || seen[m.ID] {
			continue
		}
		seen[m.ID] = true
		rows = append(rows, []any{m.ID, m.CompanyID, m.AccountID, m.VendorID, rStr(r.VendorMsgID), rStr(m.Destination),
			int64(m.ReceiptWait() / time.Second), rStr(m.QueueOwner), rStr(chargeOf(m))})
	}
	if len(rows) == 0 {
		return nil
	}
	_, err := recIgnoring(ctx, x, stRecOpenNote, stRecOpenNote.SQL, rJSON(rows))
	return err
}

// chargeOf is the charge mark a message's vendor role priced it with ("" =
// none): what its final receipt books from (hot/0074, BUG-RR-02).
func chargeOf(m model.Message) string {
	if m.Priced == nil {
		return ""
	}
	return m.Priced.Charge
}

// closeOpen removes messages from open_msgs: they are final.
func closeOpen(ctx context.Context, x Querier, msgIDs ...string) error {
	if !dal.OpenMessagesInSQL() || len(msgIDs) == 0 {
		return nil
	}
	_, err := rExec(ctx, x, stRecOpenClose, stRecOpenClose.SQL, rIDs(msgIDs))
	return err
}

// ---- the vendor roles' record writes ----

// Write files one message's record: an incoming message in mo_, an outgoing
// one as a batch of one (writeBatch), with the vendor's answer time now for
// a message a vendor has just taken.
func (s *RecordStore) Write(ctx context.Context, m model.Message) error {
	if m.Direction == model.DirectionMO {
		return s.writeMO(ctx, m)
	}
	var ack time.Time
	if m.Status == model.StatusSent {
		ack = time.Now()
	}
	return s.writeBatch(ctx, []dal.TerminalRecord{{Msg: m, VendorMsgID: m.VendorMsgID, AckAt: ack}}, false, nil)
}

// WriteTerminalBatch writes records already at their terminal status, with
// the receipt row each closes (once).
func (s *RecordStore) WriteTerminalBatch(ctx context.Context, recs []dal.TerminalRecord) error {
	return s.writeBatch(ctx, recs, true, nil)
}

// WriteRated is WriteTerminalBatch (receiptRow) or WriteBatch for records
// that may carry their price; it answers, per message table, what it wrote.
func (s *RecordStore) WriteRated(ctx context.Context, recs []dal.TerminalRecord, receiptRow bool) ([]dal.RatedOutcome, error) {
	outs := []dal.RatedOutcome{}
	err := s.writeBatch(ctx, recs, receiptRow, &outs)
	return outs, err
}

// WriteBatch writes many records in one pass exactly as Write writes each,
// with no receipt row.
func (s *RecordStore) WriteBatch(ctx context.Context, recs []dal.TerminalRecord) error {
	return s.writeBatch(ctx, recs, false, nil)
}

// writeBatch is WriteTerminalBatch, with or without the receipt row (MySQL's
// writeBatch): each table's records in ONE transaction, made again when it
// is a deadlock victim or met another writer's row.
func (s *RecordStore) writeBatch(ctx context.Context, recs []dal.TerminalRecord, receiptRow bool, outs *[]dal.RatedOutcome) error {
	if len(recs) == 0 {
		return nil
	}
	recs = append([]dal.TerminalRecord(nil), recs...)
	sort.Slice(recs, func(i, j int) bool { return recs[i].Msg.ID < recs[j].Msg.ID })
	byTable := map[string][]dal.TerminalRecord{}
	var tables []string
	for _, r := range recs {
		if r.Msg.Priced == nil && r.Rating != nil && r.Rating.Detail != "" {
			r.Msg.Priced = &model.Priced{Detail: r.Rating.Detail}
		}
		t := dal.MTLogTableFor(r.Msg)
		if _, ok := byTable[t]; !ok {
			tables = append(tables, t)
		}
		byTable[t] = append(byTable[t], r)
	}
	sort.Strings(tables)
	for _, table := range tables {
		group := byTable[table]
		if err := s.ensureRecords(ctx, table); err != nil {
			return err
		}
		var err error
		for try := 0; try < 3; try++ {
			err = Again(ctx, recTries, func() error { return s.writeSubmitGroup(ctx, table, group, receiptRow) })
			if !errors.Is(err, errRaced) {
				break
			}
		}
		if err != nil {
			return s.forgetIfMissing(err, table, billingstore.SwapKind(table, billingstore.KindDLR),
				billingstore.SwapKind(table, billingstore.KindSub))
		}
		if outs != nil {
			*outs = append(*outs, dal.RatedOutcome{Table: table, Claimed: map[string]bool{}})
		}
	}
	return nil
}

// TryLease takes a named lock on the record store's database without
// waiting (an application lock, held on a pinned connection; see locks.go).
// An application lock belongs to its database, so the name needs no
// database in it, as MySQL's GET_LOCK name does. ok=false: held elsewhere.
func (s *RecordStore) TryLease(ctx context.Context, name string) (release func(), ok bool, err error) {
	h, ans, err := s.locks.TryLock(ctx, name)
	if err != nil {
		return nil, false, err
	}
	if ans != dal.LockTaken || h == nil {
		return nil, false, nil
	}
	return h.Release, true, nil
}
