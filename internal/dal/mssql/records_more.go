package mssql

// The record store's incoming messages, refusals, orphans and lookups with no
// message, on SQL Server (mirrors internal/dal/mysql/records.go's
// "incoming", "refusals" and "orphans" parts and records_queued.go). Every
// row is written once and counted once, in its summary, in the same
// transaction.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/yakugate/yakugate/internal/billingstore"
	cry "github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/mask"
	"github.com/yakugate/yakugate/internal/model"
)

// ---- what is kept of a text (mysql.go's storedBody, bodyEncOf, rejectBody) ----

// recBodyEnc is the record's body_enc: what was kept of its text.
func recBodyEnc(m model.Message) string {
	switch dal.PrivacyOf(m) {
	case model.PrivacyEncrypted:
		return "all_encrypted"
	case model.PrivacyMasked:
		return "pii_masked"
	}
	return "plain_text"
}

// recStoredBody is the body as it is KEPT: masked when the account asked for
// it, never over ciphertext.
func recStoredBody(m model.Message) string {
	if !m.MaskBody || cry.IsCiphertext(m.Body) {
		return m.Body
	}
	return mask.Digits(m.Body, recMaskMinDigits())
}

// recRejectBody is the text of a refused message as it may be kept.
func recRejectBody(m model.Message) string {
	if !m.KeepRejectBody || m.Body == "" || cry.IsCiphertext(m.Body) {
		return ""
	}
	if m.MaskBody {
		return mask.Digits(m.Body, recMaskMinDigits())
	}
	return m.Body
}

// recMaskMinDigits is the shortest digit run hidden (YAKU_MASK_MIN_DIGITS).
func recMaskMinDigits() int {
	if v := os.Getenv("YAKU_MASK_MIN_DIGITS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return mask.DefaultMinDigits
}

// recSenderByClient is what the client sent, kept only when the gateway
// chose another sender: NULL when the client's own was used.
func recSenderByClient(m model.Message) any {
	if m.SenderBy == "" {
		return nil
	}
	return m.ClientSender
}

// ---- incoming ----

var recMOCols = jcols("msg_id", jID, "account_id", jBig, "sender", jTxt, "destination", jTxt, "body", jMax,
	"encoding", jTxt, "segments", jInt, "route_id", jBig, "vendor_id", jBig, "vendor_protocol", jTxt, "mcc", jTxt, "mnc", jTxt,
	"error_code", jTxt, "mask_body", jInt, "queue_owner", jTxt, "deployment_id", jTxt, "sell_rate_id", jBig, "buy_rate_id", jBig,
	"fx_rate", jDec, "base_rate", jDec, "buy_fx_rate", jDec, "buy_base_rate", jDec, "received_at", jTime)

var stRecMOInsert = RecordStmts.Add("record.mo.insert", recInsertOnce("mo", recMOCols, "t.msg_id = j.msg_id"), true)

// ---- refusals ----

// recStageAcceptance is what every refusal's rejected_stage says.
const recStageAcceptance = "ACCEPTANCE"

var recRejectCols = jcols("msg_id", jID, "rejected_stage", jTxt, "status", jTxt, "account_id", jBig, "client_protocol", jTxt,
	"client_msg_id", jTxt, "request_id", jTxt, "sender_id", jTxt, "msisdn", jTxt, "parts", jInt, "sent_parts", jInt, "label", jTxt,
	"reason_code", jTxt, "reason_text", jTxt, "reason_detail", jTxt, "reason_audit", jTxt, "ip_address", jTxt, "vendor_id", jBig,
	"vendor_protocol", jTxt, "route_id", jBig, "attempts", jInt, "mcc", jTxt, "mnc", jTxt, "queue_owner", jTxt,
	"deployment_id", jTxt, "body", jMax, "body_enc", jTxt, "sell_rate_id", jBig, "buy_rate_id", jBig, "dlr_sell_rate_id", jBig,
	"hlr_sell_rate_id", jBig, "fx_rate", jDec, "base_rate", jDec, "buy_fx_rate", jDec, "buy_base_rate", jDec, "hlr_source", jTxt,
	"no_receipt", jInt, "failure_receipt_only", jInt, "rejected_at", jTime)

var stRecRejectInsert = RecordStmts.Add("record.rejects.insert", recInsertOnce("rej", recRejectCols, "t.msg_id = j.msg_id"), true)

// recRejectAt is when a refusal was made: the time in its id, else now.
func recRejectAt(m model.Message, now time.Time) time.Time {
	if m.ID == "" {
		return now.UTC()
	}
	return billingstore.IDTime(m.ID, now)
}

// WriteRejected records a submit the accept pipeline refused, in
// rejects_<company>_<day>.
func (s *RecordStore) WriteRejected(ctx context.Context, m model.Message) error {
	return s.WriteRejectedBatch(ctx, []model.Message{m})
}

// WriteRejectedBatch writes many refused submits in one statement a table,
// with their summary; a refusal there already is left out.
func (s *RecordStore) WriteRejectedBatch(ctx context.Context, ms []model.Message) error {
	return s.writeRejected(ctx, ms)
}

// WriteRejectedBatchOnce is WriteRejectedBatch for a caller that may write
// some rows a second time: every write leaves out a refusal whose id is
// there, as on MySQL.
func (s *RecordStore) WriteRejectedBatchOnce(ctx context.Context, ms []model.Message) error {
	return s.writeRejected(ctx, ms)
}

func (s *RecordStore) writeRejected(ctx context.Context, ms []model.Message) error {
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
		t := billingstore.RecordTableName(billingstore.KindRejects, m.CompanyID, recRejectAt(m, now))
		if _, ok := byTable[t]; !ok {
			order = append(order, t)
		}
		byTable[t] = append(byTable[t], m)
	}
	sort.Strings(order)
	dep := rStr(deployment.ID())
	for _, table := range order {
		if err := s.ensureRecords(ctx, table); err != nil {
			return err
		}
		rows := byTable[table]
		dlrTable := billingstore.SwapKind(table, billingstore.KindDLR)
		for _, m := range rows {
			if m.RefusalReceipt != "" {
				if err := s.ensureRecords(ctx, dlrTable); err != nil {
					return err
				}
				break
			}
		}
		for from := 0; from < len(rows); from += rChunk {
			chunk := rows[from:min(from+rChunk, len(rows))]
			var err error
			for try := 0; try < 3; try++ {
				err = s.inTx(ctx, func(tx *sql.Tx) error { return s.writeRejectChunk(ctx, tx, table, dlrTable, chunk, now, dep) })
				if !errors.Is(err, errRaced) {
					break
				}
			}
			if err != nil {
				return s.forgetIfMissing(err, table)
			}
		}
	}
	return nil
}

// writeRejectChunk inserts, in tx, the refusals of chunk whose id is not in
// table, the receipts their clients were sent, and their counts.
func (s *RecordStore) writeRejectChunk(ctx context.Context, tx *sql.Tx, table, dlrTable string, chunk []model.Message, now time.Time, dep any) error {
	byID := map[string]model.Message{}
	var ids []string
	var rows [][]any
	for _, m := range chunk {
		if _, seen := byID[m.ID]; seen {
			continue
		}
		byID[m.ID] = m
		ids = append(ids, m.ID)
		parts := m.Segments
		if parts < 1 {
			parts = 1
		}
		code, text := model.SplitReason(m.RejectReason)
		proto := recClientProtocol(m.Ingress)
		if proto == nil {
			proto = "http"
		}
		rows = append(rows, []any{m.ID, recStageAcceptance, string(model.StatusRejected), rNZ(m.AccountID), proto,
			rStr(m.IdempotencyKey), rStr(m.RequestID), rStr(m.Sender), rStr(m.Destination), parts, nil,
			rStr(m.Channel), code, rStr(rTrunc(text, recReasonTextMax)), nil,
			rStr(rTrunc(m.RejectAudit, 190)), rStr(m.IPAddress), nil, nil, nil, nil, rStr(m.MCC), rStr(m.MNC),
			nil, dep, rStr(recRejectBody(m)), "plain_text", nil, nil, nil, nil, nil,
			nil, nil, nil,
			m.HLRSource, rBool(m.NoReceipt), rBool(m.FailureReceiptOnly), rTimeText(recRejectAt(m, now))})
	}
	q, err := rQuery(ctx, tx, stRecRejectInsert, rOn(stRecRejectInsert.SQL, "rej", table), rJSON(rows))
	if err != nil {
		if IsDuplicate(err) {
			return errRaced
		}
		return err
	}
	made := map[string]bool{}
	for q.Next() {
		var id string
		if err := q.Scan(&id); err != nil {
			q.Close()
			return err
		}
		made[rTrim(id)] = true
	}
	err = q.Err()
	q.Close()
	if err != nil {
		if IsDuplicate(err) {
			return errRaced
		}
		return err
	}
	d := newSums()
	var told, written []model.Message
	for _, id := range ids {
		if !made[id] {
			continue
		}
		m := byID[id]
		written = append(written, m)
		if m.RefusalReceipt != "" {
			told = append(told, m)
		}
		parts := m.Segments
		if parts < 1 {
			parts = 1
		}
		code, _ := model.SplitReason(m.RejectReason)
		proto, _ := recClientProtocol(m.Ingress).(string)
		if proto == "" {
			proto = "http"
		}
		d.addReject(recRejectAt(m, now), m.AccountID, proto, code, int64(parts))
	}
	// The refused messages' live lookups on the HLR log (6 Nov 2026).
	if err := s.writeLookups(ctx, tx, written, true); err != nil {
		return err
	}
	if err := recRefusalReceipts(ctx, tx, dlrTable, told, now, dep, d); err != nil {
		if IsDuplicate(err) {
			return errRaced
		}
		return err
	}
	return flushSums(ctx, tx, d)
}

// recRefusalReceipts files the receipts refused messages' clients were sent
// (a duplicate's REJECTD), closing their message, in the refusal's own
// transaction (MySQL's refusalReceipts).
func recRefusalReceipts(ctx context.Context, x Querier, table string, ms []model.Message, now time.Time, dep any, d *sums) error {
	if len(ms) == 0 {
		return nil
	}
	rows := make([][]any, 0, len(ms))
	for _, m := range ms {
		stat, errCode, _ := strings.Cut(m.RefusalReceipt, " ")
		code, _ := model.SplitReason(m.RejectReason)
		at := recRejectAt(m, now)
		rows = append(rows, []any{m.ID, rNZ(m.AccountID), recSourceSystem, nil, nil, nil, nil,
			string(model.StatusFailed), rStr(stat), rStr(rTrunc(code, 32)),
			rStr("told the client stat:" + stat + " err:" + errCode), m.ID, dep, nil, nil, nil, nil, rTimeText(at)})
		d.addReceipt(at, m.AccountID, 0, stat, code)
	}
	return recInsertReceipts(ctx, x, table, rows)
}

// ---- orphans: what matched nothing (company 0) ----

func recOrphansTable(t time.Time) string {
	return billingstore.RecordTableName(billingstore.KindOrphans, 0, t)
}

// WriteOrphanMO records an inbound message no MO route owns. RECORD ONLY.
func (s *RecordStore) WriteOrphanMO(ctx context.Context, vendorID int64, source, destination, body, enc, mcc, mnc, reason string) error {
	return s.WriteOrphans(ctx, []dal.OrphanRow{{Kind: "mo", VendorID: vendorID, Source: source, Destination: destination,
		Body: body, Encoding: enc, MCC: mcc, MNC: mnc, Reason: reason, At: time.Now()}}, false)
}

// WriteOrphanDLR records a receipt that correlates to no message of ours.
func (s *RecordStore) WriteOrphanDLR(ctx context.Context, vendorID int64, vendorMsgID, status, errorCode, raw, reason string) error {
	return s.WriteOrphans(ctx, []dal.OrphanRow{{Kind: "dlr", VendorID: vendorID, VendorMsgID: vendorMsgID, Status: status,
		ErrorCode: errorCode, Raw: raw, Reason: reason, At: time.Now()}}, false)
}

var recOrphanCols = jcols("kind", jTxt, "vendor_id", jBig, "vendor_msg_id", jTxt, "source", jTxt, "destination", jTxt,
	"body", jMax, "encoding", jTxt, "mcc", jTxt, "mnc", jTxt, "status", jTxt, "error_code", jTxt, "raw", jMax, "reason", jTxt,
	"received_at", jTime)

var (
	stRecOrphanInsert = RecordStmts.Add("record.orphans.insert", "INSERT INTO {orph} ("+jNames(recOrphanCols, "")+") SELECT "+
		jNames(recOrphanCols, "j.")+" FROM "+jRows(recOrphanCols)+jInOrder, true)
	// The rows of a batch not there yet: same kind, vendor, vendor id or
	// sender, and the same moment (MySQL's orphansMissing, one row at a time
	// there), answered by their place in the batch.
	stRecOrphanMissing = RecordStmts.Add("record.orphans.missing", `SELECT j.i FROM OPENJSON(@p1)
		WITH (i INT '$[0]', k NVARCHAR(4000) '$[1]', v BIGINT '$[2]', vm NVARCHAR(4000) '$[3]', src NVARCHAR(4000) '$[4]', at DATETIME2(3) '$[5]') AS j
		WHERE NOT EXISTS (SELECT 1 FROM {orph} AS o WHERE o.kind = j.k AND o.received_at = j.at AND COALESCE(o.vendor_id, 0) = j.v
			AND COALESCE(o.vendor_msg_id, N'') = j.vm AND COALESCE(o.source, N'') = j.src)`, false)
)

// WriteOrphans files orphans: one INSERT per day table, their
// summary_orphans counts in the same transaction. once leaves out a row that
// is there already.
func (s *RecordStore) WriteOrphans(ctx context.Context, rows []dal.OrphanRow, once bool) error {
	byTable := map[string][]dal.OrphanRow{}
	var order []string
	for _, r := range rows {
		if r.At.IsZero() {
			r.At = time.Now()
		}
		r.At = r.At.UTC()
		if len(r.Raw) > 4096 {
			r.Raw = r.Raw[:4096]
		}
		t := recOrphansTable(r.At)
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
			rs := rs
			if once {
				var err error
				if rs, err = recOrphansMissing(ctx, tx, table, rs); err != nil || len(rs) == 0 {
					return err
				}
			}
			vals := make([][]any, 0, len(rs))
			d := newSums()
			for _, r := range rs {
				vals = append(vals, []any{r.Kind, rNZ(r.VendorID), rStr(r.VendorMsgID), rStr(r.Source), rStr(r.Destination),
					rStr(r.Body), rStr(r.Encoding), rStr(r.MCC), rStr(r.MNC), rStr(r.Status), rStr(r.ErrorCode),
					rStr(r.Raw), rStr(r.Reason), rTimeText(r.At)})
				shortcode := ""
				if r.Kind == "mo" {
					shortcode = r.Destination
				}
				d.addOrphan(r.At, r.Kind, r.VendorID, r.Reason, shortcode)
			}
			if _, err := rExec(ctx, tx, stRecOrphanInsert, rOn(stRecOrphanInsert.SQL, "orph", table), rJSON(vals)); err != nil {
				return err
			}
			return flushSums(ctx, tx, d)
		})
		if err != nil {
			return s.forgetIfMissing(err, table)
		}
	}
	return nil
}

// recOrphansMissing keeps the rows not in table yet.
func recOrphansMissing(ctx context.Context, tx *sql.Tx, table string, rs []dal.OrphanRow) ([]dal.OrphanRow, error) {
	keys := make([][]any, len(rs))
	for i, r := range rs {
		keys[i] = []any{i, r.Kind, r.VendorID, r.VendorMsgID, r.Source, rTimeText(r.At)}
	}
	q, err := rQuery(ctx, tx, stRecOrphanMissing, rOn(stRecOrphanMissing.SQL, "orph", table), rJSON(keys))
	if err != nil {
		return nil, err
	}
	defer q.Close()
	missing := map[int]bool{}
	for q.Next() {
		var i int
		if err := q.Scan(&i); err != nil {
			return nil, err
		}
		missing[i] = true
	}
	if err := q.Err(); err != nil {
		return nil, err
	}
	var out []dal.OrphanRow
	for i, r := range rs {
		if missing[i] {
			out = append(out, r)
		}
	}
	return out, nil
}

var stRecOrphansPending = RecordStmts.Add("record.orphans.pending", `SELECT TOP (@p3) o.id, COALESCE(o.vendor_id, 0), COALESCE(o.vendor_msg_id, N''),
		COALESCE(o.status, N''), COALESCE(o.error_code, N''), COALESCE(o.raw, N''), o.received_at
	FROM {orph} AS o LEFT JOIN orphans_matched AS x ON x.orphan_table = @p1 AND x.orphan_id = o.id
	WHERE o.kind = 'dlr' AND o.received_at >= @p2 AND o.vendor_msg_id IS NOT NULL AND x.orphan_id IS NULL
	ORDER BY o.id DESC`, false)

// PendingOrphanDLRs reads filed receipts that correlated to nothing and were
// not applied since, newest first, for one more try at matching them:
// today's table and back to the day of since.
func (s *RecordStore) PendingOrphanDLRs(ctx context.Context, since time.Time, limit int) ([]model.OrphanDLR, error) {
	if limit <= 0 {
		limit = 200
	}
	now := time.Now().UTC()
	tables := []string{recOrphansTable(now)}
	for d := billingstore.DayStart(now, -1); !d.Before(billingstore.DayStart(since, 0)); d = d.AddDate(0, 0, -1) {
		tables = append(tables, recOrphansTable(d))
	}
	var out []model.OrphanDLR
	for _, table := range tables {
		rows, err := rQuery(ctx, s.db, stRecOrphansPending, rOn(stRecOrphansPending.SQL, "orph", table), rText(table), Time(since), limit)
		if MissingTable(err) {
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
			o.ReceivedAt = o.ReceivedAt.UTC()
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

// The marks of orphans applied after all, for one orphan table, in one
// batch: the marks not there yet (orphans_matched), and each new mark taken
// off its summary_orphans row — a row its last orphan left goes. MySQL does
// the same orphan by orphan (markOrphanMatchedTx): a count taken down n
// times, never below 0, is the count less n, never below 0.
var stRecOrphansMark = RecordStmts.Add("record.orphans.mark", `SET NOCOUNT ON;
DECLARE @yk_new TABLE (id BIGINT PRIMARY KEY);
INSERT INTO orphans_matched (orphan_table, orphan_id, msg_id) OUTPUT INSERTED.orphan_id INTO @yk_new (id)
	SELECT @p2, j.o, j.m FROM OPENJSON(@p1) WITH (o BIGINT '$[0]', m CHAR(26) COLLATE Latin1_General_100_BIN2 '$[1]') AS j
	INNER LOOP JOIN {orph} AS o ON o.id = j.o
	WHERE o.kind = 'dlr' AND NOT EXISTS (SELECT 1 FROM orphans_matched AS x WITH (UPDLOCK, HOLDLOCK) WHERE x.orphan_table = @p2 AND x.orphan_id = j.o);
WITH g AS (SELECT DATEADD(MINUTE, CAST(DATEDIFF_BIG(SECOND, '19700101', o.received_at) / 900 AS INT) * 15, CAST('19700101' AS DATETIME2(0))) AS b,
		COALESCE(o.vendor_id, 0) AS v, LEFT(COALESCE(o.reason, N''), 64) AS r, COUNT_BIG(*) AS n
	FROM @yk_new AS w INNER JOIN {orph} AS o ON o.id = w.id GROUP BY DATEADD(MINUTE, CAST(DATEDIFF_BIG(SECOND, '19700101', o.received_at) / 900 AS INT) * 15, CAST('19700101' AS DATETIME2(0))),
		COALESCE(o.vendor_id, 0), LEFT(COALESCE(o.reason, N''), 64))
UPDATE t SET orphans = CASE WHEN t.orphans > g.n THEN t.orphans - g.n ELSE 0 END, updated_at = SYSUTCDATETIME()
	FROM g INNER JOIN summary_orphans AS t ON t.bucket_start = g.b AND t.kind = N'dlr' AND t.vendor_id = g.v AND t.reason = g.r AND t.shortcode = N'';
WITH g AS (SELECT DISTINCT DATEADD(MINUTE, CAST(DATEDIFF_BIG(SECOND, '19700101', o.received_at) / 900 AS INT) * 15, CAST('19700101' AS DATETIME2(0))) AS b,
		COALESCE(o.vendor_id, 0) AS v, LEFT(COALESCE(o.reason, N''), 64) AS r
	FROM @yk_new AS w INNER JOIN {orph} AS o ON o.id = w.id)
DELETE t FROM g INNER JOIN summary_orphans AS t ON t.bucket_start = g.b AND t.kind = N'dlr' AND t.vendor_id = g.v AND t.reason = g.r AND t.shortcode = N''
	WHERE t.orphans = 0;`, false)

// MarkOrphanMatched notes that a filed receipt was applied to the message it
// turned out to belong to: not offered again, and off the orphans' summary.
func (s *RecordStore) MarkOrphanMatched(ctx context.Context, table string, id int64, msgID string) error {
	return s.MarkOrphansMatched(ctx, []dal.OrphanMatch{{Table: table, ID: id, MsgID: msgID}})
}

// MarkOrphansMatched notes several filed receipts as applied, in one
// transaction (one batch a day's orphan table). Safe to repeat.
func (s *RecordStore) MarkOrphansMatched(ctx context.Context, ms []dal.OrphanMatch) error {
	byTable := map[string][][]any{}
	seen := map[string]bool{}
	var order []string
	for _, m := range ms {
		rt, ok := billingstore.ParseRecordTable(m.Table)
		if !ok || rt.Kind != billingstore.KindOrphans {
			return fmt.Errorf("not an orphan table: %q", m.Table)
		}
		k := m.Table + "\x00" + strconv.FormatInt(m.ID, 10)
		if seen[k] {
			continue // marked by the first already
		}
		seen[k] = true
		if _, ok := byTable[m.Table]; !ok {
			order = append(order, m.Table)
		}
		byTable[m.Table] = append(byTable[m.Table], []any{m.ID, rStr(m.MsgID)})
	}
	if len(order) == 0 {
		return nil
	}
	sort.Strings(order)
	return s.inTx(ctx, func(tx *sql.Tx) error {
		for _, table := range order {
			rows := byTable[table]
			sort.Slice(rows, func(i, j int) bool { return rows[i][0].(int64) < rows[j][0].(int64) })
			if _, err := rExec(ctx, tx, stRecOrphansMark, rOn(stRecOrphansMark.SQL, "orph", table), rJSON(rows), rText(table)); err != nil {
				return err
			}
		}
		return nil
	})
}

// ---- lookups with no message (records_queued.go on MySQL) ----

var recLookupCols = jcols("account_id", jBig, "destination", jTxt, "source", jTxt, "mcc", jTxt, "mnc", jTxt, "ported", jInt,
	"looked_up_at", jTime, "deployment_id", jTxt, "operator_name", jTxt, "ccc", jTxt,
	// the HLR provider's call (hot/0080, 6 Nov 2026), dal.LookupCallCols' order
	"method", jTxt, "url", jTxt, "req_headers", jMax, "req_body", jMax, "req_cut", jInt, "http_status", jInt,
	"resp_headers", jMax, "resp_body", jMax, "resp_cut", jInt, "took_ms", jBig, "call_error", jTxt)

var (
	stRecLookupInsert = RecordStmts.Add("record.lookups.insert", "INSERT INTO {hlr} (msg_id, "+jNames(recLookupCols, "")+") SELECT NULL, "+
		jNames(recLookupCols, "j.")+" FROM "+jRows(recLookupCols)+jInOrder, false)
	stRecLookupMissing = RecordStmts.Add("record.lookups.missing", `SELECT j.i FROM OPENJSON(@p1)
		WITH (i INT '$[0]', a BIGINT '$[1]', d NVARCHAR(4000) '$[2]', at DATETIME2(3) '$[3]') AS j
		WHERE NOT EXISTS (SELECT 1 FROM {hlr} AS h WHERE h.msg_id IS NULL AND COALESCE(h.account_id, 0) = j.a
			AND h.destination = j.d AND h.looked_up_at = j.at)`, false)
)

// WriteLookup files one lookup that has no message (GET /sms/v4/operator).
func (s *RecordStore) WriteLookup(ctx context.Context, r dal.LookupRow) error {
	if r.At.IsZero() {
		r.At = time.Now()
	}
	return s.WriteLookups(ctx, []dal.LookupRow{r}, false)
}

// WriteLookups files lookups with no message: one INSERT per company-day
// table, their summary_lookup counts in the same transaction.
func (s *RecordStore) WriteLookups(ctx context.Context, rows []dal.LookupRow, once bool) error {
	byTable := map[string][]dal.LookupRow{}
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
			rs := rs
			if once {
				keys := make([][]any, len(rs))
				for i, r := range rs {
					keys[i] = []any{i, r.AccountID, r.Destination, rTimeText(r.At)}
				}
				q, err := rQuery(ctx, tx, stRecLookupMissing, rOn(stRecLookupMissing.SQL, "hlr", table), rJSON(keys))
				if err != nil {
					return err
				}
				missing := map[int]bool{}
				for q.Next() {
					var i int
					if err := q.Scan(&i); err != nil {
						q.Close()
						return err
					}
					missing[i] = true
				}
				err = q.Err()
				q.Close()
				if err != nil {
					return err
				}
				var keep []dal.LookupRow
				for i, r := range rs {
					if missing[i] {
						keep = append(keep, r)
					}
				}
				if rs = keep; len(rs) == 0 {
					return nil
				}
			}
			vals := make([][]any, 0, len(rs))
			d := newSums()
			for _, r := range rs {
				vals = append(vals, append([]any{rNZ(r.AccountID), r.Destination, r.Source, rStr(r.MCC), rStr(r.MNC), rBool(r.Ported),
					rTimeText(r.At), rStr(deployment.ID()), rStr(r.OperatorName), rStr(r.CCC)}, dal.LookupCallArgs(r.Call)...))
				d.addLookup(mtRow{key: mtKey{bucket: recBucket(r.At.Unix()), account: r.AccountID, mcc: r.MCC, mnc: r.MNC},
					hlrSource: r.Source, ported: r.Ported})
			}
			if _, err := rExec(ctx, tx, stRecLookupInsert, rOn(stRecLookupInsert.SQL, "hlr", table), rJSON(vals)); err != nil {
				return err
			}
			return flushSums(ctx, tx, d)
		})
		if err != nil {
			return s.forgetIfMissing(err, table)
		}
	}
	return nil
}
