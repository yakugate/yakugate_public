package mysql

// Outgoing records at acceptance: a message's mt_ row, its columns and
// values, written once.

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/model"
)

// ---- outgoing ----

// mtCols are the columns of an outgoing record: what is known when it is
// ACCEPTED, and nothing about the vendor (the three-table design, Saleem and
// the PO, 27 Oct 2026: every vendor fact is a sub_ row). The last is a time
// given as Unix seconds with milliseconds.
const mtCols = `msg_id,trace_id,request_id,account_id,company_id,sender,destination,body,body_enc,campaign_hash,campaign_id,
	encoding,segments,udh_ref,udh_total,udh_seq,label,ap1,ap2,apg1,apg2,priority,client_protocol,ip_address,
	mcc,mnc,hlr_source,hlr_ported,route_id,template_id,
	no_receipt,failure_receipt_only,want_intermediate,bind_slot,mask_body,queue_owner,deployment_id,
	sender_by,client_sender,client_ref,spam_flag,sell_rate_id,dlr_sell_rate_id,hlr_sell_rate_id,fx_rate,
	base_rate,accepted_at`

// mtValue is one record's placeholders for mtCols.
var mtValue = valuesFor(mtCols, 1)

// valuesFor is "(?,?,…)" for a column list whose last `times` columns are
// times given as Unix seconds with milliseconds.
func valuesFor(cols string, times int) string {
	n := len(strings.Split(cols, ","))
	return "(" + strings.Repeat("?,", n-times) +
		strings.TrimSuffix(strings.Repeat("FROM_UNIXTIME(CAST(? AS DECIMAL(17,3))),", times), ",") + ")"
}

// acceptRates are a message's rate ids as its acceptance fixed them: the
// quote's (D-130), never a later vendor's pricing (that is its sub_ row's).
func acceptRates(m model.Message) billing.RecordRates {
	return billing.RatesOf(model.Message{Quote: m.Quote})
}

func mtArgs(m model.Message, rates billing.RecordRates, acc time.Time) []any {
	return []any{
		m.ID, nsStr(m.TraceID), nsStr(m.RequestID), m.AccountID, nz(m.CompanyID), m.Sender, m.Destination,
		nsStr(storedBody(m)), bodyEncOf(m),
		// The text's signature and the client's campaign (queue/0030).
		nsStr(m.CampaignHash), nsStr(m.CampaignID),
		orStr(m.Encoding, "gsm7"), m.Segments,
		m.ConcatRef, m.ConcatTotal, m.ConcatSeq,
		// The client's own label (issue 17: was `channel`, not a protocol).
		nsStr(m.Channel),
		nsStr(m.AP1), nsStr(m.AP2), nsStr(m.APG1), nsStr(m.APG2), nzInt(m.Priority),
		// How the client sent it (issue 17) and its address (issue 13).
		clientProtocol(m.Ingress), nsStr(m.IPAddress),
		nsStr(m.MCC), nsStr(m.MNC), m.HLRSource, boolInt(m.HLRPorted),
		// The rule that matched it at acceptance, and its template.
		nz(m.RouteID), nz(m.TemplateID),
		// What the client asked for: no_receipt / failure_receipt_only / the
		// progress notices are registered_delivery (PO 2026-09-03); bind_slot
		// is the bind its receipt goes back down (PO 2026-09-05).
		boolInt(m.NoReceipt), boolInt(m.FailureReceiptOnly), boolInt(m.WantsIntermediateReceipt()), m.BindSlot,
		int(privacyOf(m)),
		// Whose queue it waits in (D-121): its receipts are queued there.
		nsStr(m.QueueOwner), nsStr(m.DeploymentID),
		// Who chose the sender (hot/0049).
		nsStr(m.SenderBy), senderByClient(m),
		nsStr(m.IdempotencyKey),
		// A spam rule in observe mode matched at accept (config/0098).
		nsStr(m.SpamFlag),
		// The selling price as rate version ids, fixed at acceptance (issue
		// 16): worked out when read.
		nz(rates.Sell), nz(rates.DLRSell), nz(rates.HLRSell), nsFx(rates.FX), nsFx(rates.BaseRate),
		// accepted_at is the time in the id.
		ackAtArg(acc),
	}
}

// clientProtocol is how the client sent a message: http or smpp; NULL when
// it is not known.
func clientProtocol(t model.Transport) any {
	switch t {
	case model.TransportHTTP, model.TransportSMPP:
		return string(t)
	}
	return nil
}

// vendorProtocol is how a message went to its vendor; NULL when it did not.
func vendorProtocol(t model.Transport) any {
	switch t {
	case model.TransportHTTP, model.TransportSMPP, "soap", "blackhole":
		return string(t)
	}
	return nil
}

// nsFx is a conversion rate, NULL when there was none.
func nsFx(v float64) any {
	if v <= 0 {
		return nil
	}
	return v
}

// receiptReason is a failure receipt's code: its word, with its error code
// when that says something ("UNDELIV:034"). "000" and the like say nothing.
// A row with no word (the gateway's own verdict) gives its code
// (MAX_ATTEMPTS_REACHED).
func receiptReason(stat, code string) string {
	stat, code = strings.TrimSpace(stat), strings.TrimSpace(code)
	if strings.Trim(code, "0") == "" {
		code = ""
	}
	switch {
	case stat != "" && code != "":
		return stat + ":" + code
	case stat != "":
		return stat
	}
	return code
}

// failedStatus is a final status a reason belongs to.
func failedStatus(st string) bool {
	return st == string(model.StatusFailed) || st == string(model.StatusExpired)
}

// The sizes of a refusal's reason_code and reason_text.
const (
	reasonCodeMax = 191
	reasonTextMax = 512
)

// clipReason trims s and cuts it to at most n characters.
func clipReason(s string, n int) string {
	s = strings.TrimSpace(s)
	if r := []rune(s); len(r) > n {
		s = string(r[:n])
	}
	return s
}

// presentIn reads which of ids have a row in table. A table that does not
// exist has none.
func presentIn(ctx context.Context, x dbtx, table string, ids []string) (map[string]bool, error) {
	have := map[string]bool{}
	for from := 0; from < len(ids); from += 500 {
		part := ids[from:min(from+500, len(ids))]
		args := make([]any, len(part))
		for i, id := range part {
			args[i] = id
		}
		rows, err := x.QueryContext(ctx, "SELECT msg_id FROM "+table+" WHERE msg_id IN ("+placeholders(len(part))+")", args...)
		if err != nil {
			if isNoSuchTable(err) {
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
			have[id] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return have, nil
}

// WriteAccepted files outgoing messages' records at ACCEPTANCE (the
// three-table design, Saleem and the PO, 27 Oct 2026): their mt_ rows —
// nothing about a vendor — their lookups and the accepted counts, one
// transaction per table. A message that is there already is left as it is
// and counted nothing, so a task written twice changes nothing.
func (s *Store) WriteAccepted(ctx context.Context, msgs []model.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	msgs = append([]model.Message(nil), msgs...)
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].ID < msgs[j].ID })
	byTable := map[string][]model.Message{}
	var tables []string
	for _, m := range msgs {
		t := MTTableOf(m)
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
		var err error
		for try := 0; try < 3; try++ {
			err = againIfVictim(ctx, func() error {
				write := func() error {
					return s.inTx(ctx, func(tx *sql.Tx) error {
						d := newSums()
						if _, err := s.insertAccepted(ctx, tx, table, group, d, time.Now()); err != nil {
							return err
						}
						return s.flushSums(ctx, tx, d)
					})
				}
				e := write()
				if isAPTooLong(e) && s.widenAP(ctx, table) {
					e = write()
				}
				return e
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
// have yet, with their lookups and their accepted counts (on the account's
// summary row with no vendor and no buying rate: no vendor is known at
// acceptance). It answers the messages it made.
func (s *Store) insertAccepted(ctx context.Context, tx dbtx, table string, msgs []model.Message, d *sums, now time.Time) ([]model.Message, error) {
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		ids = append(ids, m.ID)
	}
	have, err := presentIn(ctx, tx, table, ids)
	if err != nil {
		return nil, err
	}
	var made []model.Message
	seen := map[string]bool{}
	for _, m := range msgs {
		if !have[m.ID] && !seen[m.ID] {
			seen[m.ID] = true
			made = append(made, m)
		}
	}
	if len(made) == 0 {
		return nil, nil
	}
	var sb strings.Builder
	args := make([]any, 0, len(made)*48)
	for i, m := range made {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(mtValue)
		rates := acceptRates(m)
		args = append(args, mtArgs(m, rates, billingstore.IDTime(m.ID, now))...)
		row := acceptedRowOf(m, now)
		d.addMT(row.key, row.accepted(), 1)
		d.addLookup(row)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+table+" ("+mtCols+") VALUES "+sb.String(), args...); err != nil {
		if isDupEntry(err) {
			return nil, errRaced
		}
		return nil, err
	}
	if err := s.writeLookups(ctx, tx, made, false); err != nil {
		return nil, err
	}
	return made, nil
}

// acceptedRowOf is a message's summary row as its acceptance counts it: no
// vendor, no buying rate.
func acceptedRowOf(m model.Message, now time.Time) mtRow {
	r := mtRowOf(m, acceptRates(m), time.Time{}, "", now)
	r.key.vendor, r.key.buy = 0, 0
	return r
}

// submittedRowOf is a message's summary row as a vendor's taking of it counts
// it: its vendor and that vendor's buying rate, the selling rate fixed at
// acceptance — the row its closing receipt's outcome is counted in
// (readMTRows reads the same back from mt_ and sub_).
func submittedRowOf(m model.Message, ackAt time.Time, now time.Time) mtRow {
	r := mtRowOf(m, acceptRates(m), ackAt, "", now)
	r.key.buy = billing.RatesOf(m).Buy
	return r
}

// attemptOf is the try a record's write is about: its accepted answer's, else
// the message's own count.
func attemptOf(r TerminalRecord) int {
	for _, a := range r.Answers {
		if a.Result == model.AnswerAccepted || a.Result == "" {
			if a.Attempt > 0 {
				return a.Attempt
			}
		}
	}
	return model.TryOf(r.Msg)
}

// acceptedIn reads which (message, try) pairs have an accepted answer in sub
// already: a submission written twice is counted once.
func acceptedIn(ctx context.Context, x dbtx, sub string, ids []string) (map[string]map[int]bool, error) {
	out := map[string]map[int]bool{}
	for from := 0; from < len(ids); from += 500 {
		part := ids[from:min(from+500, len(ids))]
		args := make([]any, len(part))
		for i, id := range part {
			args[i] = id
		}
		rows, err := x.QueryContext(ctx, "SELECT msg_id, attempt FROM "+sub+" WHERE result='accepted' AND msg_id IN ("+placeholders(len(part))+")", args...)
		if err != nil {
			if isNoSuchTable(err) {
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
