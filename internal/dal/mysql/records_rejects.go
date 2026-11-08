package mysql

// Refusals at acceptance (rejects_ rows).

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
)

// rejectCols are the columns of a refusal, both stages; the last is a time
// given as Unix seconds with milliseconds.
const rejectCols = `msg_id,rejected_stage,status,account_id,client_protocol,client_msg_id,request_id,sender_id,msisdn,parts,sent_parts,label,
	reason_code,reason_text,reason_detail,reason_audit,ip_address,vendor_id,vendor_protocol,route_id,attempts,mcc,mnc,queue_owner,
	deployment_id,body,body_enc,sell_rate_id,buy_rate_id,dlr_sell_rate_id,hlr_sell_rate_id,fx_rate,
	base_rate,buy_fx_rate,buy_base_rate,hlr_source,no_receipt,failure_receipt_only,rejected_at`

var rejectValue = valuesFor(rejectCols, 1)

// StageAcceptance is what every refusal's rejects_.rejected_stage says: a
// submit refused while being accepted. The "submission" stage (Saleem 22 Oct
// 2026) is gone since the three tables: such a message stays on the outgoing
// log (Saleem, 28 Oct 2026: "if not then remove this").
const StageAcceptance = "ACCEPTANCE"

// ---- refusals ----

// rejectsTableOf is where a refusal at acceptance is filed: its account's
// company (0 = refused before the account was known) and the day of its id.
func rejectsTableOf(m model.Message, at time.Time) string {
	return billingstore.RecordTableName(billingstore.KindRejects, m.CompanyID, at)
}

// rejectAt is when a refusal was made: the time in its id, else now.
func rejectAt(m model.Message, now time.Time) time.Time {
	if m.ID == "" {
		return now.UTC()
	}
	return billingstore.IDTime(m.ID, now)
}

// WriteRejected records a submit the accept pipeline refused, in
// rejects_<company>_<day>. Best-effort by contract: a rejection has already
// been answered to the client, so a failure here is logged by the caller and
// never changes the response. [SCOPE P1-REJECTLOG / S-01]
func (s *Store) WriteRejected(ctx context.Context, m model.Message) error {
	return s.WriteRejectedBatch(ctx, []model.Message{m})
}

// WriteRejectedBatch writes many refused submits in one statement a table,
// with their summary. A row's rejected_at is when the refusal was MADE (the
// time in the message's id), not when the row is written: the DB worker
// writes these some time after the refusal (recordq). A refusal with no id is
// given one made now (the reject queue gives one already, so a refusal written
// twice is one row).
func (s *Store) WriteRejectedBatch(ctx context.Context, ms []model.Message) error {
	return s.writeRejected(ctx, ms, false)
}

// WriteRejectedBatchOnce is WriteRejectedBatch for a caller that may be
// writing some of these rows a second time (the DB worker, when its last
// batch did not end cleanly): a refusal whose id is in its table already is
// left out.
func (s *Store) WriteRejectedBatchOnce(ctx context.Context, ms []model.Message) error {
	return s.writeRejected(ctx, ms, true)
}

func (s *Store) writeRejected(ctx context.Context, ms []model.Message, once bool) error {
	if len(ms) == 0 {
		return nil
	}
	now := time.Now().UTC()
	byTable := map[string][]model.Message{}
	var order []string
	for _, m := range ms {
		if m.ID == "" {
			m.ID = ulid.Make().String()
		}
		t := rejectsTableOf(m, rejectAt(m, now))
		if _, ok := byTable[t]; !ok {
			order = append(order, t)
		}
		byTable[t] = append(byTable[t], m)
	}
	sort.Strings(order)
	dep := nsStr(deployment.ID())
	for _, table := range order {
		if err := s.ensureRecords(ctx, table); err != nil {
			return err
		}
		rows := byTable[table]
		// A refusal whose client was sent a receipt (a duplicate) has that
		// receipt filed beside it, in the same transaction (refusalReceipts).
		dlrTable := billingstore.SwapKind(table, billingstore.KindDLR)
		for _, m := range rows {
			if m.RefusalReceipt != "" {
				if err := s.ensureRecords(ctx, dlrTable); err != nil {
					return err
				}
				break
			}
		}
		for from := 0; from < len(rows); from += 500 {
			chunk := rows[from:min(from+500, len(rows))]
			err := againIfVictim(ctx, func() error {
				tx, err := s.db.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer tx.Rollback() //nolint:errcheck
				ids := make([]string, len(chunk))
				for i, m := range chunk {
					ids[i] = m.ID
				}
				// Always read first: an id met twice in one batch, or a
				// refusal the worker wrote before its batch broke off.
				have, err := presentIn(ctx, tx, table, ids)
				if err != nil {
					return err
				}
				var sb strings.Builder
				args := make([]any, 0, len(chunk)*34)
				d := newSums()
				n := 0
				var told, written []model.Message
				for _, m := range chunk {
					if have[m.ID] {
						continue
					}
					have[m.ID] = true
					written = append(written, m)
					if m.RefusalReceipt != "" {
						told = append(told, m)
					}
					if n > 0 {
						sb.WriteByte(',')
					}
					n++
					sb.WriteString(rejectValue)
					parts := m.Segments
					if parts < 1 {
						parts = 1
					}
					// The bare code and the sentence apart (hot/0040):
					// reason_code is what reports group by, reason_text is
					// what the sender was told.
					code, text := model.SplitReason(m.RejectReason)
					at := rejectAt(m, now)
					proto := clientProtocol(m.Ingress)
					if proto == nil {
						proto = "http"
					}
					args = append(args, m.ID, StageAcceptance, string(model.StatusRejected), nz(m.AccountID), proto,
						nsStr(m.IdempotencyKey), nsStr(m.RequestID), nsStr(m.Sender), nsStr(m.Destination), parts, nil,
						nsStr(m.Channel), code, nsStr(trunc(text, reasonTextMax)), nil,
						// Operator-only (hot/0031), kept out of reason_code.
						nsStr(trunc(m.RejectAudit, 190)), nsStr(m.IPAddress), nil, nil, nil, nil, nsStr(m.MCC), nsStr(m.MNC),
						nil, dep, nsStr(rejectBody(m)), "plain_text", nil, nil, nil, nil, nil, // the rate ids and fx_rate
						nil, nil, nil, // base_rate, buy_fx_rate, buy_base_rate: refused before any price
						m.HLRSource, boolInt(m.NoReceipt), boolInt(m.FailureReceiptOnly), ackAtArg(at))
					d.addReject(at, m.AccountID, proto.(string), code, int64(parts))
				}
				if n == 0 {
					return nil
				}
				if _, err := tx.ExecContext(ctx, "INSERT INTO "+table+" ("+rejectCols+") VALUES "+sb.String(), args...); err != nil {
					if isDupEntry(err) {
						return errRaced
					}
					return err
				}
				if err := refusalReceipts(ctx, tx, dlrTable, told, now, dep, d); err != nil {
					if isDupEntry(err) {
						return errRaced
					}
					return err
				}
				// The refused messages' live lookups on the HLR log (6 Nov 2026).
				if err := s.writeLookups(ctx, tx, written, true); err != nil {
					return err
				}
				if err := s.flushSums(ctx, tx, d); err != nil {
					return err
				}
				return tx.Commit()
			})
			if errors.Is(err, errRaced) {
				err = s.writeRejected(ctx, chunk, true) // read again: the other writer's rows are there now
			}
			if err != nil {
				return s.forgetIfMissing(err, table)
			}
		}
	}
	_ = once // every write reads first: a refusal has an id of its own
	return nil
}

// refusalReceipts files the receipts refused messages' clients were sent (a
// duplicate's REJECTD 408, Message.RefusalReceipt) as dlr_ rows of source
// system that close their message, in the refusal's own transaction: the
// refusal and the receipt it was answered with are written together or not
// at all. The row keeps our reason code (DUPLICATE_MESSAGE); raw says what
// the client read.
func refusalReceipts(ctx context.Context, x dbtx, table string, ms []model.Message, now time.Time, dep any, d *sums) error {
	if len(ms) == 0 {
		return nil
	}
	vals := make([]string, 0, len(ms))
	args := make([]any, 0, len(ms)*18)
	for _, m := range ms {
		stat, errCode, _ := strings.Cut(m.RefusalReceipt, " ")
		code, _ := model.SplitReason(m.RejectReason)
		at := rejectAt(m, now)
		vals = append(vals, receiptRowValue)
		args = append(args, m.ID, nz(m.AccountID), SourceSystem, nil, nil, nil, nil,
			string(model.StatusFailed), nsStr(stat), nsStr(trunc(code, 32)),
			nsStr("told the client stat:"+stat+" err:"+errCode), m.ID, dep, nil, nil, nil, nil, ackAtArg(at))
		d.addReceipt(at, m.AccountID, 0, stat, code)
	}
	_, err := x.ExecContext(ctx, "INSERT INTO "+table+" ("+receiptRowCols+") VALUES "+strings.Join(vals, ","), args...)
	return err
}
