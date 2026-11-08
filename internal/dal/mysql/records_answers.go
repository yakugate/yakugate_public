package mysql

// A vendor's answers (sub_ rows): the submit group and the answers it
// files.

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
)

// writeSubmitGroup writes what one table's records of a vendor role's batch
// say about their SUBMISSION (the three-table design, 27 Oct 2026), in ONE
// transaction:
//
//   - each message's mt_ row if it has none yet (one accepted before this
//     build, or its acceptance task still behind it): written as acceptance
//     would have written it, and counted accepted;
//   - every vendor answer, a sub_ row, priced with the vendor's rate;
//   - a message a vendor took (AckAt set): counted submitted on its vendor's
//     summary row, its open-message row (no Redis);
//   - a receipt the gateway makes at submission (system generated): the
//     gateway's ACCEPTD of an SMPP vendor's message (receiptRow), and the one
//     that closes a message final at submission — the blackhole's outcome, or
//     a message no vendor kept (refused with no tries left, out of tries,
//     validity over), whose closing receipt says why. Never a rejects_ row:
//     that is for refusals at acceptance.
//
// Everything is written once: an answer by its key, a submission by its
// accepted answer, a close by final_of.
func (s *Store) writeSubmitGroup(ctx context.Context, table string, group []TerminalRecord, receiptRow bool) error {
	now := time.Now()
	sub := billingstore.SwapKind(table, billingstore.KindSub)
	dlr := billingstore.SwapKind(table, billingstore.KindDLR)
	// The day's tables are made BEFORE the transaction: one made after it
	// has read is refused to it ("Table definition has changed", 1412), so
	// the first write of every company's day failed once.
	for _, t := range []string{table, sub, dlr} {
		if err := s.ensureRecords(ctx, t); err != nil {
			return err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit
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
	seen, err := acceptedIn(ctx, tx, sub, ids)
	if err != nil {
		return err
	}
	closed, err := finalsIn(ctx, tx, dlr, ids)
	if err != nil {
		return err
	}
	// newTaken: a try a vendor took that was not written yet; firstTaken:
	// the message's first such try, the one that gets the at-submit receipt
	// row (a message really sent again has its answer in sub_, and no
	// second notice).
	newTaken, firstTaken := map[string]bool{}, map[string]bool{}
	var open []TerminalRecord
	for _, r := range group {
		if r.AckAt.IsZero() {
			continue
		}
		att := attemptOf(r)
		if seen[r.Msg.ID][att] {
			continue
		}
		if seen[r.Msg.ID] == nil {
			seen[r.Msg.ID] = map[int]bool{}
			firstTaken[r.Msg.ID] = true
		}
		seen[r.Msg.ID][att] = true
		newTaken[r.Msg.ID] = true
		row := submittedRowOf(r.Msg, r.AckAt, now)
		d.addMT(row.key, row.submitted(), 1)
		open = append(open, r)
	}
	var rcpt []TerminalRecord
	for _, r := range group {
		final := finalStatus(string(orStatus(r.Msg.Status)))
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
			if isDupEntry(err) {
				return errRaced // another writer closed one meanwhile: read again
			}
			return err
		}
	}
	if err := s.noteOpen(ctx, tx, open); err != nil {
		return err
	}
	if err := s.writeAnswers(ctx, tx, sub, pricedAnswers(group)); err != nil {
		return err
	}
	if err := s.flushSums(ctx, tx, d); err != nil {
		return err
	}
	return tx.Commit()
}

// pricedAnswers is every vendor answer a group's records carry, each with
// what the vendor's try cost and the round it was sent in (hot/0070): the
// facts of a submission, which mt_ no longer holds.
func pricedAnswers(group []TerminalRecord) []model.SubmitAnswer {
	var out []model.SubmitAnswer
	for _, r := range group {
		m := r.Msg
		rates := billing.RatesOf(m)
		for _, a := range withAcceptedAnswer(r) {
			if a.Result == model.AnswerAccepted && a.VendorMsgID == "" && (a.VendorID == 0 || a.VendorID == m.VendorID) {
				// The id the message is filed under when the vendor gave
				// none of its own for this answer: our own id, or one id for
				// a whole bulk call (httpvendor.vendorRef). A receipt finds
				// its message by the accepted answer's id.
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

// withAcceptedAnswer is a record's answers, with the vendor's acceptance
// made from the record itself when the vendor took the message (AckAt) and
// no accepted answer of that try came with it (a writer of before the
// three-table design, a task queued by one): the sub_ row is where "taken",
// the vendor and its id are read from, so a taken message never reads
// queued.
func withAcceptedAnswer(r TerminalRecord) []model.SubmitAnswer {
	if r.AckAt.IsZero() {
		return r.Answers
	}
	att := attemptOf(r)
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
		Deployment: orStr(m.SentDeploymentID, m.DeploymentID),
	})
}

// subCols are the columns of a vendor's answer; the last two are times.
const subCols = `msg_id,account_id,part,parts,attempt,vendor_id,vendor_msg_id,vendor_protocol,command_status,http_status,
	result,code,words,deployment_id,session,buy_rate_id,buy_fx_rate,buy_base_rate,route_id,route_level,send_round,
	method,url,req_headers,req_body,req_cut,resp_headers,resp_body,resp_cut,sealed,took_ms,
	sent_at,answered_at`

var subValue = valuesFor(subCols, 2)

// writeAnswers files vendor answers in table (sub_), the ones there already
// left out by its key (a message, a part, a try, a result).
func (s *Store) writeAnswers(ctx context.Context, x dbtx, table string, as []model.SubmitAnswer) error {
	if len(as) == 0 || table == "" {
		return nil
	}
	if err := s.ensureRecords(ctx, table); err != nil {
		return err
	}
	for from := 0; from < len(as); from += 500 {
		part := as[from:min(from+500, len(as))]
		var sb strings.Builder
		args := make([]any, 0, len(part)*33)
		for i, a := range part {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(subValue)
			args = append(args, answerArgs(a)...)
		}
		if _, err := x.ExecContext(ctx, "INSERT IGNORE INTO "+table+" ("+subCols+") VALUES "+sb.String(), args...); err != nil {
			return s.forgetIfMissing(err, table)
		}
	}
	return nil
}

func answerArgs(a model.SubmitAnswer) []any {
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
	c := callArgsOf(a.Call)
	result := a.Result
	if result == "" {
		result = model.AnswerAccepted
	}
	return []any{a.MsgID, nz(a.AccountID), part, parts, attempt, nz(a.VendorID), nsStr(trunc(a.VendorMsgID, 64)),
		vendorProtocol(model.Transport(a.VendorProtocol)), cs, nzInt(a.HTTPStatus), result, nsStr(trunc(a.Code, 32)),
		nsStr(trunc(a.Words, 255)), nsStr(orStr(a.Deployment, deployment.ID())), nsStr(trunc(a.Session, 64)),
		nz(a.BuyRateID), nsFx(a.BuyFX), nsFx(a.BuyBase), nz(a.RouteID), nzInt(a.RouteLevel), nzInt(a.SendRound),
		c.method, c.url, c.reqHeaders, c.reqBody, c.reqCut, c.respHeaders, c.respBody, c.respCut, c.sealed, c.took,
		ackAtArg(a.SentAt), ackAtArg(a.AnsweredAt)}
}

// callArgs are an answer's call columns (hot/0077): NULLs and zeros for an
// answer with no call (SMPP).
type callArgs struct {
	method, url, reqHeaders, reqBody, respHeaders, respBody, took any
	reqCut, respCut, sealed                                       int
}

func callArgsOf(c *model.VendorCall) callArgs {
	if c == nil {
		return callArgs{}
	}
	var took any
	if c.TookMS > 0 {
		took = c.TookMS
	}
	return callArgs{method: nsStr(trunc(c.Method, 8)), url: nsStr(trunc(c.URL, 2048)), reqHeaders: nsStr(c.ReqHeaders),
		reqBody: nsStr(c.ReqBody), reqCut: boolInt(c.ReqCut), respHeaders: nsStr(c.RespHeaders), respBody: nsStr(c.RespBody),
		respCut: boolInt(c.RespCut), sealed: boolInt(c.Sealed), took: took}
}

// WriteAnswers files vendor answers that end nothing by themselves — a "not
// now", a part with no answer while its message tries again — each in the
// sub_ table of its message's company and day.
func (s *Store) WriteAnswers(ctx context.Context, as []model.SubmitAnswer) error {
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
