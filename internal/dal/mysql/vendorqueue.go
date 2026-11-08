package mysql

// A vendor's queue: a message put in (Enqueue), the columns read back
// when one is taken, and how many wait in it.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queueflow"
)

// Enqueue inserts a pending message into the vendor's queue table. [SCOPE P1-17]
func (s *Store) Enqueue(ctx context.Context, m model.Message) error {
	t := queueTable(m.Vendor)
	// GW-01: a vendor onboarded after seeding (portal, bulk import, manual SQL)
	// has no queue table until this clones one — same lazy pattern as the hot
	// store's mtlog_*/dlrlog_* families. Cached, so it is one map load per call
	// after the first.
	if err := s.ensureCloneTable(ctx, t, "queue_template"); err != nil {
		return err
	}
	q := fmt.Sprintf(`INSERT INTO %s
        (msg_id,trace_id,request_id,account_id,username,vendor_id,route_id,sender,destination,body,encoding,segments,
         udh_ref,udh_total,udh_seq,gsm7,
         template_id,channel,ap1,ap2,apg1,apg2,transport,ingress,mcc,mnc,status,priority,idempotency_key,valid_until,attempts,
         no_receipt,failure_receipt_only,want_intermediate_receipt,bind_slot,mask_body,spam_flag,
         orig_sender,orig_destination,orig_body,campaign_id,campaign_hash,code_reroute,deployment_id,sender_by,client_sender,send_round,route_level,tried_vendors,billing_model,queue_owner,quote,company_id,hlr_source,hlr_ported,error_text,ip_address,shortcode,pick_at,next_attempt_at,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,DATE_ADD(NOW(3), INTERVAL ? MICROSECOND),IF(? IS NULL, NULL, DATE_ADD(NOW(), INTERVAL ? SECOND)),NOW())`, t)
	// next_attempt_at: NULL claims immediately; a future value (from a
	// client-requested sendAt, [SCOPE P1-SCHEDULE]) delays visibility to
	// Claim's own "next_attempt_at IS NULL OR <=NOW()" gate — no separate
	// scheduling mechanism needed, it reuses the retry-lease column. Written as
	// a NOW()-relative offset, NOT a driver-side (UTC) timestamp: every reader
	// and every lease writer (Claim, sweep) is on the SQL session clock, and on
	// a server whose local time runs ahead of UTC a driver-written UTC value
	// would look hours in the past — sendAt fired immediately (found by
	// TC-P7-06 automation, 2026-08-10; same clock-mixing class as the
	// valid_until/UTC_TIMESTAMP bug fixed at P7 build time).
	var delaySecs any
	if !m.NextAttemptAt.IsZero() {
		delaySecs = int64(time.Until(m.NextAttemptAt) / time.Second)
	}
	// A clone still made with 50-character ap columns is widened by the
	// write that needs it (apwide.go, QA templates API-013). An insert
	// MySQL refused as a deadlock victim is made again (queuelocks.go): it
	// was the client's QUEUE_UNAVAILABLE, with no retry (retest 19 Oct 2026).
	err := s.withWideAP(ctx, []string{t, "queue_template"}, func() error {
		_, err := s.writeOne(ctx, q, enqueueArgs(m, delaySecs)...)
		return err
	})
	if err == nil {
		queueflow.In(queueflow.MT, m.Vendor, 1) // monitoring: the queue's rate in (D-127)
	}
	// A vendor's queue table dropped under a running role (a vendor deleted
	// and created again with the same cid) must not mean 503 until restart:
	// forget the memo so the next enqueue creates it again (2026-09-17).
	return s.forgetIfMissing(err, t)
}

// queueErrorTextMax is the size of a queue row's error_text (queue/0041).
const queueErrorTextMax = 512

// enqueueArgs are Enqueue's values, in the order of its column list.
func enqueueArgs(m model.Message, delaySecs any) []any {
	return []any{
		m.ID, orStr(m.TraceID, m.ID), orStr(m.RequestID, m.ID), m.AccountID, nsStr(m.Username), m.VendorID, nz(m.RouteID), m.Sender, m.Destination,
		m.Body, orStr(m.Encoding, "gsm7"), m.Segments,
		m.ConcatRef, m.ConcatTotal, m.ConcatSeq, gsm7Flag(m.Gsm7),
		nz(m.TemplateID), nsStr(m.Channel),
		nsStr(m.AP1), nsStr(m.AP2), nsStr(m.APG1), nsStr(m.APG2),
		string(m.Transport), string(orTransport(m.Ingress, model.TransportHTTP)), nsStr(m.MCC), nsStr(m.MNC),
		string(orStatus(m.Status)), m.Priority, nsStr(m.IdempotencyKey), nzTime(m.ValidUntil), m.Attempts,
		boolInt(m.NoReceipt), boolInt(m.FailureReceiptOnly), boolInt(m.WantIntermediateReceipt), m.BindSlot, boolInt(m.MaskBody),
		nsStr(m.SpamFlag),
		nsStr(m.OrigSender), nsStr(m.OrigDestination), nsStr(m.OrigBody),
		nsStr(m.CampaignID), nsStr(m.CampaignHash), nsStr(m.CodeReroute),
		// The gateway that accepted it (queue/0033), so the record written at
		// the other end of the queue can say where the submit landed.
		nsStr(m.DeploymentID),
		// Who chose the sender, and what the client sent then (queue/0034).
		nsStr(m.SenderBy), senderByClient(m),
		// Where it is in its rule's plan (queue/0035).
		m.SendRound, m.RouteLevel, triedJSON(m.TriedVendors),
		// How the company paid at acceptance (queue/0036, QA BUG-024).
		nsStr(m.BillingModel),
		// Whose queue it is (queue/0037, D-121); NULL = shared.
		nsStr(m.QueueOwner),
		// What it costs, fixed at acceptance (queue/0038).
		nsStr(m.Quote),
		// Whose company it is, for the record's table (queue/0039, D-136).
		nz(m.CompanyID),
		// How its network was found, for its record, the HLR log and the
		// HLR summary (queue/0040).
		nsStr(m.HLRSource), boolInt(m.HLRPorted),
		// What its last vendor answered (queue/0041).
		nsStr(clipReason(m.ErrorText, queueErrorTextMax)),
		// The client's address, for its record (queue/0043, issue 13).
		nsStr(m.IPAddress),
		// Its short code's queue ('' = the common one) and when it may be
		// taken (queue/0044, issue 22).
		m.ShortCode, pickDelayMicros(m),
		delaySecs, delaySecs,
	}
}

// pickDelayMicros is how long from now a queued message waits before it may
// be taken: until m.NextAttemptAt (a scheduled send, a retry after its gap),
// else not at all. Written relative to the session's NOW(3), as every reader
// compares (the clock-mixing rule of next_attempt_at, 2026-08-10).
func pickDelayMicros(m model.Message) int64 {
	if m.NextAttemptAt.IsZero() {
		return 0
	}
	if d := time.Until(m.NextAttemptAt); d > 0 {
		return d.Microseconds()
	}
	return 0
}

// triedJSON is tried_vendors as stored: a JSON array, NULL when empty.
func triedJSON(v []string) any {
	if len(v) == 0 {
		return nil
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// queueSelectCols is what a vendor process gets back when it claims a message.
//
// A column missing from HERE is a field the message loses on its way out: the
// vendor process is handed a Message rebuilt from this list and nothing else,
// and whatever it does not carry is written to the record as NULL. request_id
// and trace_id were both absent from the initial commit until 2026-09-08 — the
// enqueue INSERT wrote them faithfully and nothing ever read them back, so
// every record claimed through the MySQL queue had a NULL request_id and the
// portal's outgoing log could not show a batch id.
//
// It was invisible with the Redis runtime queue in front, which carries the
// whole Message as JSON and so never depended on this list, and MySQL is the
// documented Phase-1 default — so the same traffic showed the id or not
// depending only on whether Redis happened to be gated.
const queueSelectCols = `id,msg_id,request_id,trace_id,account_id,username,COALESCE(route_id,0),sender,destination,body,encoding,segments,
        COALESCE(udh_ref,0),COALESCE(udh_total,0),COALESCE(udh_seq,0),transport,ingress,mcc,mnc,attempts,
        channel,ap1,ap2,apg1,apg2,priority,
        COALESCE(no_receipt,0),COALESCE(failure_receipt_only,0),COALESCE(want_intermediate_receipt,0),COALESCE(bind_slot,0),
        COALESCE(idempotency_key,''),COALESCE(mask_body,0),COALESCE(spam_flag,''),
        gsm7,COALESCE(orig_sender,''),COALESCE(orig_destination,''),COALESCE(orig_body,''),valid_until,
        COALESCE(campaign_id,''),COALESCE(campaign_hash,''),COALESCE(code_reroute,''),COALESCE(template_id,0),
        COALESCE(deployment_id,''),COALESCE(sender_by,''),COALESCE(client_sender,''),
        COALESCE(send_round,0),COALESCE(route_level,0),COALESCE(tried_vendors,''),
        COALESCE(billing_model,''),COALESCE(queue_owner,''),COALESCE(quote,''),COALESCE(company_id,0),
        COALESCE(hlr_source,''),COALESCE(hlr_ported,0),COALESCE(error_text,''),COALESCE(ip_address,''),
        COALESCE(shortcode,'')`

// scanQueueRow reads one row shaped like queueSelectCols into a model.Message,
// returning its internal auto-increment id (for the lease UPDATE) alongside it.
//
// `extra` are scan targets for columns a caller appended AFTER queueSelectCols
// in its own SELECT (PendingMessages adds vendor_id, status and the two
// timestamps). They exist so that every reader of queueSelectCols goes through
// THIS function: the Scan is positional, so a second hand-written scan
// elsewhere silently breaks the moment a column is added to the list — which
// is exactly what happened to PendingMessages when request_id/trace_id and
// bind_slot/mask_body arrived, leaving Redis repopulation dead with an
// argument-count error nobody saw until a restart.
func scanQueueRow(rows *sql.Rows, vendor string, extra ...any) (model.Message, int64, error) {
	var (
		m                            model.Message
		id                           int64
		req, trace                   sql.NullString
		uname                        sql.NullString
		enc, transp                  sql.NullString
		ingr                         sql.NullString
		mcc, mnc                     sql.NullString
		seg                          sql.NullInt64
		att                          sql.NullInt64
		chn                          sql.NullString
		ap1, ap2                     sql.NullString
		apg1, apg2                   sql.NullString
		prio                         sql.NullInt64
		templateID                   int64
		noRcpt, failOnly, wantInterm int
		hlrPorted                    int
		bindSlot                     int
		clientRef                    string
		maskBody                     int
		spamFlag                     string
		gsm7                         sql.NullInt64
		validUntil                   sql.NullTime
		tried                        string
	)
	targets := []any{&id, &m.ID, &req, &trace, &m.AccountID, &uname, &m.RouteID, &m.Sender, &m.Destination, &m.Body, &enc, &seg,
		&m.ConcatRef, &m.ConcatTotal, &m.ConcatSeq, &transp, &ingr, &mcc, &mnc, &att,
		&chn, &ap1, &ap2, &apg1, &apg2, &prio,
		&noRcpt, &failOnly, &wantInterm, &bindSlot, &clientRef, &maskBody, &spamFlag,
		&gsm7, &m.OrigSender, &m.OrigDestination, &m.OrigBody, &validUntil,
		&m.CampaignID, &m.CampaignHash, &m.CodeReroute,
		&templateID,
		// Which gateway accepted it (queue/0033). Last, so the `extra`
		// columns a caller appends after queueSelectCols keep their places.
		&m.DeploymentID,
		// Who chose the sender (queue/0034) — after deployment_id for the
		// same reason.
		&m.SenderBy, &m.ClientSender,
		// Where it is in its rule's plan (queue/0035), and the vendors tried
		// in this round (queue/0018).
		&m.SendRound, &m.RouteLevel, &tried,
		// How the company paid at acceptance (queue/0036).
		&m.BillingModel,
		// Whose queue it is (queue/0037, D-121).
		&m.QueueOwner,
		// What it costs, fixed at acceptance (queue/0038).
		&m.Quote,
		// Whose company it is (queue/0039, D-136).
		&m.CompanyID,
		// How its network was found (queue/0040).
		&m.HLRSource, &hlrPorted,
		// What its last vendor answered (queue/0041).
		&m.ErrorText,
		// The client's address (queue/0043).
		&m.IPAddress,
		// Its short code's queue (queue/0044).
		&m.ShortCode,
	}
	targets = append(targets, extra...)
	if err := rows.Scan(targets...); err != nil {
		return model.Message{}, 0, err
	}
	// The client's own id, written at submit and — until 2026-09-06 — never
	// read back, which is why it never reached the record or the reply.
	m.IdempotencyKey = clientRef
	if tried != "" {
		_ = json.Unmarshal([]byte(tried), &m.TriedVendors)
	}
	// The batch id the client was answered with, and the caller's correlation
	// id. Both are written at enqueue and both belong on the record: the
	// portal's outgoing log finds a whole multi-recipient send by request_id.
	m.RequestID, m.TraceID = req.String, trace.String
	m.MaskBody = maskBody != 0
	// Carried from accept (config/0098) so the vendor process that writes the
	// record can put it there — it has no reference cache and cannot re-derive
	// which spam rule matched.
	m.SpamFlag = spamFlag
	// The client's encoding choice and what a route rewrite replaced
	// (queue/0029): a message moved off a down vendor is routed — and its
	// parts planned — again from these.
	if gsm7.Valid {
		m.Gsm7 = strconv.FormatBool(gsm7.Int64 != 0)
	}
	// The expiry travels to the sender (2026-09-14): smpp-vendor writes the
	// time left into submit_sm's validity_period. It used to be dropped here,
	// so a MySQL-queued message reached the vendor with no validity at all.
	if validUntil.Valid {
		m.ValidUntil = validUntil.Time
	}
	m.NoReceipt, m.FailureReceiptOnly, m.WantIntermediateReceipt = noRcpt != 0, failOnly != 0, wantInterm != 0
	m.HLRPorted = hlrPorted != 0
	m.BindSlot = bindSlot
	m.Username = uname.String
	m.Channel = chn.String
	m.AP1, m.AP2 = ap1.String, ap2.String
	m.APG1, m.APG2 = apg1.String, apg2.String
	m.Priority = int(prio.Int64)
	m.TemplateID = templateID // the matched template rides the queue row to the record (I-20)
	m.Ingress = model.Transport(ingr.String)
	m.Vendor = vendor
	m.Encoding = enc.String
	m.Transport = model.Transport(transp.String)
	m.Segments = int(seg.Int64)
	m.MCC, m.MNC = mcc.String, mnc.String
	m.Attempts = int(att.Int64)
	return m, id, nil
}

// senderByClient is what the client sent, stored only when the gateway chose
// another sender (queue/0034, hot/0049): NULL when the client's own was used,
// "" kept as "" when the client sent none — the difference is the point.
func senderByClient(m model.Message) any {
	if m.SenderBy == "" {
		return nil
	}
	return m.ClientSender
}

// QueuedForVendor counts every row in a vendor's queue table, whichever
// deployment owns it (store.VendorQueueCounter) — the same count the console's
// vendor delete takes. A table that does not exist holds nothing.
func (s *Store) QueuedForVendor(ctx context.Context, vendor string) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+queueTable(vendor)+"`").Scan(&n)
	if err != nil && isNoSuchTable(err) {
		return 0, nil
	}
	return n, err
}
