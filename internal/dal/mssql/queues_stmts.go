package mssql

// The queue store's statements on SQL Server, by name (mirrors
// internal/dal/mysql/queuestmts.go). Every statement of the queue family is
// here, so the memory-optimized pass (natively compiled procedures in place
// of these texts) changes one file. A statement built at the call (an IN
// list, the owner's condition, one account's attempt window, the sub-queues
// of one take) is registered in its one-account, one-sub-queue form; the
// text a call sends is that form with more values. A vendor's table is
// written [queue_<cid>] and named at the call (qOn).

import (
	"fmt"
	"strings"

	"github.com/microsoft/go-mssqldb"
)

// QueueStmts is the queue family's statements and the hand-off lists'.
var QueueStmts = newRegistry()

// qTablePlaceholder is where a vendor's table goes in a registered text.
const qTablePlaceholder = "[queue_<cid>]"

// qOn is a registered text for one vendor's table.
func qOn(sqlText, table string) string {
	return strings.ReplaceAll(sqlText, qTablePlaceholder, quoteIdent(table))
}

// qReady is a row's "may be taken now" (MySQL's pick_at <= NOW(3)).
const qReady = "pick_at <= SYSUTCDATETIME()"

// qSecsFromNow is MySQL's DATE_ADD(NOW(), INTERVAL p SECOND) for a
// DATETIME2(0) column. NOW() is cut to the second; SQL Server ROUNDS a time
// to the column's precision, so SYSUTCDATETIME() + p could land up to half a
// second ahead — a receipt queued "now" would not be due for half a second.
// Half a second taken off before the rounding gives the cut second exactly.
func qSecsFromNow(p string) string {
	return "DATEADD(MILLISECOND, -500, DATEADD(SECOND, " + p + ", SYSUTCDATETIME()))"
}

// qNowMS is the database's clock in Unix milliseconds (UTC).
const qNowMS = "DATEDIFF_BIG(MILLISECOND, '19700101', SYSUTCDATETIME())"

// The kinds of value a queue row's column is written with. Text goes as
// NVARCHAR(MAX) (qText) so every call declares its parameters alike and
// shares one plan: go-mssqldb declares a string NVARCHAR(<its length>), and
// SQL Server keeps a plan per declaration — every insert would compile.
const (
	qkID    = iota // CHAR(26) message id: VARCHAR (Varchar), never NVARCHAR
	qkText         // text, NOT NULL
	qkNText        // text, "" = NULL (MySQL's nsStr)
	qkInt          // a number
	qkNInt         // a number, 0 = NULL (MySQL's nz)
	qkFlag         // a nullable 0/1, -1 = NULL (MySQL's gsm7Flag)
	qkTime         // a time, the zero time = NULL (MySQL's nzTime)
)

type qCol struct {
	name string
	kind int
}

// qEnqueueCols is a queue row's columns as Enqueue writes them, in the order
// of MySQL's Enqueue list; pick_at, next_attempt_at and created_at follow.
var qEnqueueCols = []qCol{
	{"msg_id", qkID}, {"trace_id", qkID}, {"request_id", qkText}, {"account_id", qkInt}, {"username", qkNText},
	{"vendor_id", qkInt}, {"route_id", qkNInt}, {"sender", qkText}, {"destination", qkText}, {"body", qkText},
	{"encoding", qkText}, {"segments", qkInt},
	{"udh_ref", qkInt}, {"udh_total", qkInt}, {"udh_seq", qkInt}, {"gsm7", qkFlag},
	{"template_id", qkNInt}, {"channel", qkNText}, {"ap1", qkNText}, {"ap2", qkNText}, {"apg1", qkNText}, {"apg2", qkNText},
	{"transport", qkText}, {"ingress", qkText}, {"mcc", qkNText}, {"mnc", qkNText}, {"status", qkText}, {"priority", qkInt},
	{"idempotency_key", qkNText}, {"valid_until", qkTime}, {"attempts", qkInt},
	{"no_receipt", qkInt}, {"failure_receipt_only", qkInt}, {"want_intermediate_receipt", qkInt}, {"bind_slot", qkInt}, {"mask_body", qkInt},
	{"spam_flag", qkNText}, {"orig_sender", qkNText}, {"orig_destination", qkNText}, {"orig_body", qkNText},
	{"campaign_id", qkNText}, {"campaign_hash", qkNText}, {"code_reroute", qkNText},
	{"deployment_id", qkNText}, {"sender_by", qkNText}, {"client_sender", qkNText},
	{"send_round", qkInt}, {"route_level", qkInt}, {"tried_vendors", qkNText},
	{"billing_model", qkNText}, {"queue_owner", qkNText}, {"quote", qkNText}, {"company_id", qkNInt},
	{"hlr_source", qkNText}, {"hlr_ported", qkInt}, {"error_text", qkNText}, {"ip_address", qkNText}, {"shortcode", qkText},
}

// qValueExpr is how a parameter is written for its column's kind.
func qValueExpr(kind int, p string) string {
	switch kind {
	case qkNText:
		return "NULLIF(" + p + ", N'')"
	case qkNInt:
		return "NULLIF(" + p + ", 0)"
	case qkFlag:
		return "NULLIF(" + p + ", -1)"
	case qkTime:
		return "NULLIF(" + p + ", CAST('00010101' AS DATETIME2))"
	}
	return p
}

// qEnqueueColumnList is the INSERT's column list.
func qEnqueueColumnList() string {
	names := make([]string, 0, len(qEnqueueCols)+3)
	for _, c := range qEnqueueCols {
		names = append(names, c.name)
	}
	return strings.Join(append(names, "pick_at", "next_attempt_at", "created_at"), ",")
}

// qEnqueueValues is one row's values from @p<first> on: the columns, then
// the pick time (seconds and microseconds from now), then next_attempt_at
// (a flag and seconds from now).
func qEnqueueValues(first int) string {
	vals := make([]string, 0, len(qEnqueueCols)+3)
	p := func() string { s := fmt.Sprintf("@p%d", first); first++; return s }
	for _, c := range qEnqueueCols {
		vals = append(vals, qValueExpr(c.kind, p()))
	}
	// pick_at is DATETIME2(3): half a millisecond off before the rounding
	// gives MySQL's cut NOW(3), so a message queued for now is due now.
	pickS, pickUS := p(), p()
	vals = append(vals, "DATEADD(MICROSECOND, "+pickUS+" - 500, DATEADD(SECOND, "+pickS+", SYSUTCDATETIME()))")
	has, secs := p(), p()
	vals = append(vals, "CASE WHEN "+has+" = 1 THEN "+qSecsFromNow(secs)+" END", "SYSUTCDATETIME()")
	return strings.Join(vals, ",")
}

// qEnqueueArgsPerRow is how many parameters one row takes.
var qEnqueueArgsPerRow = len(qEnqueueCols) + 4

// qSelectCols is what a take hands back (MySQL's queueSelectCols), each an
// expression of the row's columns written {p}col: "" for a SELECT, "deleted."
// for a take's OUTPUT. The order is scanQueueRow's.
const qSelectCols = `{p}id,{p}msg_id,{p}request_id,{p}trace_id,{p}account_id,{p}username,COALESCE({p}route_id,0),{p}sender,{p}destination,{p}body,{p}encoding,{p}segments,
	COALESCE({p}udh_ref,0),COALESCE({p}udh_total,0),COALESCE({p}udh_seq,0),{p}transport,{p}ingress,{p}mcc,{p}mnc,{p}attempts,
	{p}channel,{p}ap1,{p}ap2,{p}apg1,{p}apg2,{p}priority,
	COALESCE({p}no_receipt,0),COALESCE({p}failure_receipt_only,0),COALESCE({p}want_intermediate_receipt,0),COALESCE({p}bind_slot,0),
	COALESCE({p}idempotency_key,N''),COALESCE({p}mask_body,0),COALESCE({p}spam_flag,N''),
	{p}gsm7,COALESCE({p}orig_sender,N''),COALESCE({p}orig_destination,N''),COALESCE({p}orig_body,N''),{p}valid_until,
	COALESCE({p}campaign_id,N''),COALESCE({p}campaign_hash,''),COALESCE({p}code_reroute,N''),COALESCE({p}template_id,0),
	COALESCE({p}deployment_id,N''),COALESCE({p}sender_by,N''),COALESCE({p}client_sender,N''),
	COALESCE({p}send_round,0),COALESCE({p}route_level,0),COALESCE({p}tried_vendors,N''),
	COALESCE({p}billing_model,N''),COALESCE({p}queue_owner,N''),COALESCE({p}quote,N''),COALESCE({p}company_id,0),
	COALESCE({p}hlr_source,N''),COALESCE({p}hlr_ported,0),COALESCE({p}error_text,N''),COALESCE({p}ip_address,N''),
	COALESCE({p}shortcode,N'')`

// qRawCols is every column qSelectCols reads, for a take's CTE (OUTPUT can
// only name the CTE's columns).
const qRawCols = `id,msg_id,request_id,trace_id,account_id,username,route_id,sender,destination,body,encoding,segments,
	udh_ref,udh_total,udh_seq,transport,ingress,mcc,mnc,attempts,channel,ap1,ap2,apg1,apg2,priority,
	no_receipt,failure_receipt_only,want_intermediate_receipt,bind_slot,idempotency_key,mask_body,spam_flag,
	gsm7,orig_sender,orig_destination,orig_body,valid_until,campaign_id,campaign_hash,code_reroute,template_id,
	deployment_id,sender_by,client_sender,send_round,route_level,tried_vendors,billing_model,queue_owner,quote,company_id,
	hlr_source,hlr_ported,error_text,ip_address,shortcode,pick_at`

func qCols(prefix string) string { return strings.ReplaceAll(qSelectCols, "{p}", prefix) }

// qTakeOutput is what a take of a sub-queue OUTPUTs: the row and its pick
// time, so its rows are put back in the take's order (OUTPUT keeps none).
var qTakeOutput = qCols("deleted.") + ",deleted.pick_at"

// A take of one sub-queue: its share, in priority order, oldest first, the
// rows a taker holds passed over (READPAST), deleted as they are read and
// handed back by OUTPUT. "{own}" is the owner's condition.
const qTakeOne = `WITH yk_take AS (SELECT TOP ({n}) ` + qRawCols + `
	FROM [queue_<cid>] WITH (ROWLOCK, UPDLOCK, READPAST)
	WHERE {own} AND shortcode = {sc} AND ` + qReady + `
	ORDER BY priority, pick_at, id)
	DELETE FROM yk_take OUTPUT `

// The sub-queues of a vendor's table that hold rows, and how much each has
// ready (up to @n, by priority), in ONE statement: an owner's short codes
// are found one index seek each on idx_pick (a skip scan, a recursive CTE
// taking the next short code after the last — SQL Server has no loose index
// scan, and a DISTINCT would read every queued row, as MySQL's did before
// 30 Oct), then each one's ready rows counted up to @n. @own is the
// deployment ("" = none: it owns no rows). Own rows come first, then the
// shared ones, each owner's short codes in the index's order.
const qSubQueuesReady = `WITH yk_shared AS (
	SELECT TOP (1) shortcode FROM [queue_<cid>] WHERE queue_owner IS NULL ORDER BY shortcode
	UNION ALL
	SELECT r.shortcode FROM (SELECT t.shortcode, ROW_NUMBER() OVER (ORDER BY t.shortcode) AS rn
		FROM yk_shared AS s JOIN [queue_<cid>] AS t ON t.queue_owner IS NULL AND t.shortcode > s.shortcode) AS r WHERE r.rn = 1
), yk_own AS (
	SELECT TOP (1) shortcode FROM [queue_<cid>] WHERE queue_owner = @own ORDER BY shortcode
	UNION ALL
	SELECT r.shortcode FROM (SELECT t.shortcode, ROW_NUMBER() OVER (ORDER BY t.shortcode) AS rn
		FROM yk_own AS s JOIN [queue_<cid>] AS t ON t.queue_owner = @own AND t.shortcode > s.shortcode) AS r WHERE r.rn = 1
)
SELECT CAST(1 AS BIT) AS own, s.shortcode, r.priority, COUNT(*) AS n FROM yk_own AS s
	CROSS APPLY (SELECT TOP (@n) priority FROM [queue_<cid>] WHERE queue_owner = @own AND shortcode = s.shortcode AND ` + qReady + ` ORDER BY priority) AS r
	GROUP BY s.shortcode, r.priority
UNION ALL
SELECT CAST(0 AS BIT), s.shortcode, r.priority, COUNT(*) FROM yk_shared AS s
	CROSS APPLY (SELECT TOP (@n) priority FROM [queue_<cid>] WHERE queue_owner IS NULL AND shortcode = s.shortcode AND ` + qReady + ` ORDER BY priority) AS r
	GROUP BY s.shortcode, r.priority
ORDER BY own DESC, shortcode, priority
OPTION (MAXRECURSION 0)`

// The client receipt claim (MySQL's cdlrClaimHead + its lease, in ONE
// statement): one account's due rows by idx_cdlr_claim (named, as MySQL
// names it: on a small or new table the optimizer scans the whole table in
// id order instead) in id order, the
// ones another taker holds passed over, leased 60 s and counted as they are
// taken. "{scope}", "{age}", "{own}" and "{attempts}" are built at the call.
var qCDLRClaim = `WITH yk_take AS (SELECT TOP (@p1) id,account_id,client_msg_id,status,stat,receipt,attempts,created_at,
		bind_slot,src,dst,queue_owner,done_at_ms,msg_id,next_attempt_at
	FROM client_dlr WITH (ROWLOCK, UPDLOCK, READPAST, INDEX(idx_cdlr_claim))
	WHERE {scope} AND given_up_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= SYSUTCDATETIME()){age}{own}
	ORDER BY id)
UPDATE yk_take SET attempts = {attempts}, next_attempt_at = ` + qSecsFromNow("60") + `
OUTPUT deleted.id,deleted.account_id,deleted.client_msg_id,deleted.status,COALESCE(deleted.stat,N''),COALESCE(deleted.receipt,N''),
	inserted.attempts,deleted.created_at,COALESCE(deleted.bind_slot,0),COALESCE(deleted.src,N''),COALESCE(deleted.dst,N''),
	COALESCE(deleted.queue_owner,N''),COALESCE(deleted.done_at_ms,0),COALESCE(deleted.msg_id,'')`

// The MO claim, the same way (MySQL's moClaimHead + its lease).
var qMOClaim = `WITH yk_take AS (SELECT TOP (@p1) id,msg_id,vendor_id,account_id,route_id,sender,destination,body,
		encoding,segments,mcc,mnc,attempts,received_at,queue_owner,company_id,priced,next_attempt_at
	FROM mo_queue WITH (ROWLOCK, UPDLOCK, READPAST, INDEX(idx_mo_claim))
	WHERE status = 'queued' AND {scope} AND (next_attempt_at IS NULL OR next_attempt_at <= SYSUTCDATETIME()){age}{own}
	ORDER BY id)
UPDATE yk_take SET attempts = {attempts}, next_attempt_at = ` + qSecsFromNow("60") + `
OUTPUT deleted.msg_id,deleted.vendor_id,deleted.account_id,COALESCE(deleted.route_id,0),deleted.sender,deleted.destination,deleted.body,
	deleted.encoding,deleted.segments,COALESCE(deleted.mcc,N''),COALESCE(deleted.mnc,N''),inserted.attempts,deleted.received_at,
	COALESCE(deleted.queue_owner,N''),COALESCE(deleted.company_id,0),COALESCE(deleted.priced,N''),deleted.id`

// qOneClaim is a claim's text in its registered form: one account, the
// owner's condition, no age, a plain count.
func qOneClaim(text string) string {
	r := strings.NewReplacer("{scope}", "(account_id IN (@p2))", "{age}", "",
		"{own}", " AND (queue_owner = @p3 OR queue_owner IS NULL)", "{attempts}", "attempts + 1")
	return r.Replace(text)
}

// A list of ids as one parameter: a JSON array read by OPENJSON, so a list
// of any length is one statement and one plan (SQL Server takes at most
// 2,100 parameters). The list drives the join (INNER LOOP JOIN keeps that
// order): one index seek an id, never a scan of the table, whatever the
// optimizer guesses of OPENJSON. Message ids are read as CHAR(26) in the
// column's collation, so the index is sought.
const (
	qIDsBigint = "OPENJSON(@p1) WITH (v BIGINT '$') AS j INNER LOOP JOIN "
	qIDsMsg    = "OPENJSON(@p1) WITH (v CHAR(26) COLLATE Latin1_General_100_BIN2 '$') AS j INNER LOOP JOIN "
)

// The receipts a batch queues, one JSON row each (EnqueueClientDLRBatch).
var qCDLRBatch = `INSERT INTO client_dlr (account_id,client_msg_id,status,stat,receipt,bind_slot,src,dst,queue_owner,done_at_ms,msg_id,next_attempt_at,created_at)
	SELECT j.a, j.m, j.s, NULLIF(j.t, N''), NULLIF(j.r, N''), j.b, NULLIF(j.src, N''), NULLIF(j.dst, N''), NULLIF(j.o, N''), NULLIF(j.d, 0), NULLIF(j.g, ''),
		` + qSecsFromNow("j.w") + `, ` + qSecsFromNow("j.w") + `
	FROM OPENJSON(@p1) WITH (a BIGINT '$.a', m CHAR(26) '$.m', s NVARCHAR(16) '$.s', t NVARCHAR(8) '$.t', r NVARCHAR(512) '$.r',
		b INT '$.b', src NVARCHAR(32) '$.src', dst NVARCHAR(32) '$.dst', o NVARCHAR(64) '$.o', d BIGINT '$.d', g CHAR(26) '$.g', w INT '$.w', i INT '$.i') AS j
	ORDER BY j.i`

var (
	// The outgoing queues (mtqueue.go, mysql.go, reroute.go, queued.go,
	// handover.go on MySQL).
	stQEnqueue = QueueStmts.Add("queue.mt.enqueue",
		"INSERT INTO [queue_<cid>] ("+qEnqueueColumnList()+") VALUES ("+qEnqueueValues(1)+")", true)
	stQHandIn = QueueStmts.Add("queue.mt.hand_in",
		"INSERT INTO [queue_<cid>] ("+qEnqueueColumnList()+") SELECT "+qEnqueueValues(1)+
			" WHERE NOT EXISTS (SELECT 1 FROM [queue_<cid>] WITH (UPDLOCK, HOLDLOCK) WHERE msg_id = @p1)", false)
	stQReady = QueueStmts.Add("queue.mt.ready", qSubQueuesReady, true)
	stQTake  = QueueStmts.Add("queue.mt.take", "SET NOCOUNT ON; SET XACT_ABORT ON; BEGIN TRAN;\n"+
		strings.NewReplacer("{n}", "@n1", "{own}", "queue_owner = @own", "{sc}", "@sc1").Replace(qTakeOne)+qTakeOutput+";\nCOMMIT;", true)
	stQConcatGroup = QueueStmts.Add("queue.mt.concat_group", `DELETE FROM [queue_<cid>] WITH (ROWLOCK, UPDLOCK, READPAST)
		OUTPUT `+qCols("deleted.")+`
		WHERE account_id = @p1 AND sender = @p2 AND destination = @p3 AND udh_ref = @p4 AND udh_total = @p5{own}`, false)
	stQPeek = QueueStmts.Add("queue.mt.peek_movable", `SELECT TOP (@p1) `+qCols("")+` FROM [queue_<cid>]
		WHERE `+qReady+` AND COALESCE(udh_total,0) <= 1 AND id > @p2{own} ORDER BY id`, false)
	// The move off a down vendor: the row taken while it still waits and no
	// sender holds it, copied with the new route's fields, deleted — one
	// transaction, one round trip. It answers 1 when it moved.
	stQMove = QueueStmts.Add("queue.mt.move", `SET NOCOUNT ON; SET XACT_ABORT ON;
		DECLARE @yk_id BIGINT;
		BEGIN TRAN;
		SELECT TOP (1) @yk_id = id FROM {src} WITH (ROWLOCK, UPDLOCK, READPAST) WHERE msg_id = @p1 AND `+qReady+`;
		IF @yk_id IS NOT NULL
		BEGIN
			INSERT INTO {dst} (`+qMoveCols+`)
			SELECT msg_id,trace_id,request_id,account_id,username,@p2,NULLIF(@p3, 0),@p4,@p5,@p6,@p7,@p8,
				udh_ref,udh_total,udh_seq,gsm7,template_id,channel,ap1,ap2,apg1,apg2,@p9,ingress,mcc,mnc,status,priority,
				idempotency_key,valid_until,attempts,no_receipt,failure_receipt_only,want_intermediate_receipt,bind_slot,mask_body,
				spam_flag,NULLIF(@p10, N''),NULLIF(@p11, N''),NULLIF(@p12, N''),next_attempt_at,created_at,hlr_refreshed,
				JSON_MODIFY(COALESCE(tried_vendors, N'[]'), 'append $', @p13),
				campaign_id,campaign_hash,code_reroute,sender_by,client_sender,send_round,@p14,deployment_id,billing_model,queue_owner,quote,
				company_id,hlr_source,hlr_ported,error_text,ip_address,N'',SYSUTCDATETIME()
			FROM {src} WHERE id = @yk_id;
			DELETE FROM {src} WHERE id = @yk_id;
		END
		COMMIT;
		SELECT CASE WHEN @yk_id IS NULL THEN 0 ELSE 1 END`, false)
	stQTablesIn = QueueStmts.Add("queue.mt.tables_in", "SELECT name FROM sys.tables WHERE name IN (@p1)", false)
	stQFindOne  = QueueStmts.Add("queue.mt.find", `SELECT TOP (1) `+qCols("")+`, @p1 AS q_vendor,
		COALESCE(DATEDIFF(SECOND, SYSUTCDATETIME(), pick_at), 0) AS q_wait FROM [queue_<cid>] WHERE msg_id = @p2 AND account_id = @p3`, false)
	stQTables = QueueStmts.Add("queue.mt.tables",
		"SELECT TOP (10000) name FROM sys.tables WHERE name LIKE N'queue[_]%' AND name <> N'queue_template' ORDER BY name", false)
	// One queue's figures in one round trip: depth and ready from idx_pick,
	// the oldest ready one, and the tried count while the queue is small
	// enough for it (it needs every row).
	stQStat = QueueStmts.Add("queue.mt.stat", `SET NOCOUNT ON;
		DECLARE @yk_depth BIGINT, @yk_ready BIGINT, @yk_age BIGINT, @yk_tried BIGINT = -1;
		SELECT @yk_depth = COUNT_BIG(*), @yk_ready = SUM(CASE WHEN `+qReady+` THEN 1 ELSE 0 END),
			@yk_age = DATEDIFF_BIG(SECOND, MIN(CASE WHEN `+qReady+` THEN pick_at END), SYSUTCDATETIME())
			FROM [queue_<cid>] WHERE 1=1{own};
		IF @yk_depth <= @p1 SELECT @yk_tried = COUNT_BIG(*) FROM [queue_<cid>] WHERE attempts > 0{own};
		SELECT @yk_depth, COALESCE(@yk_ready, 0), @yk_age, @yk_tried`, false)
	stQCount = QueueStmts.Add("queue.mt.count", "SELECT COUNT(*) FROM [queue_<cid>]", false)

	// The client receipts (mysql.go on MySQL).
	stQCDLREnqueue = QueueStmts.Add("queue.client_dlr.enqueue",
		`INSERT INTO client_dlr (account_id,client_msg_id,status,stat,receipt,bind_slot,src,dst,queue_owner,done_at_ms,msg_id,next_attempt_at,created_at)
		 VALUES (@p1,@p2,@p3,NULLIF(@p4, N''),NULLIF(@p5, N''),@p6,NULLIF(@p7, N''),NULLIF(@p8, N''),NULLIF(@p9, N''),NULLIF(@p10, 0),NULLIF(@p12, ''),`+
			qSecsFromNow("@p11")+`,`+qSecsFromNow("@p11")+`)`, true)
	stQCDLRBatch     = QueueStmts.Add("queue.client_dlr.enqueue_batch", qCDLRBatch, true)
	stQCDLRClaim     = QueueStmts.Add("queue.client_dlr.claim", qOneClaim(qCDLRClaim), true)
	stQCDLRAckOne    = QueueStmts.Add("queue.client_dlr.ack", "DELETE FROM client_dlr WHERE id = @p1", true)
	stQCDLRAckMany   = QueueStmts.Add("queue.client_dlr.ack_many", "DELETE t FROM "+qIDsBigint+"client_dlr AS t ON t.id = j.v", true)
	stQCDLRResched   = QueueStmts.Add("queue.client_dlr.reschedule", "UPDATE client_dlr SET next_attempt_at = "+qSecsFromNow("@p1")+" WHERE id = @p2", false)
	stQCDLRGiveUp    = QueueStmts.Add("queue.client_dlr.give_up", "UPDATE client_dlr SET given_up_at = SYSUTCDATETIME() WHERE id = @p1 AND given_up_at IS NULL", false)
	stQCDLRSweep     = QueueStmts.Add("queue.client_dlr.sweep", "UPDATE client_dlr SET given_up_at = SYSUTCDATETIME() WHERE given_up_at IS NULL AND created_at < DATEADD(SECOND, -@p1, SYSUTCDATETIME()){own}", false)
	stQPendingCounts = QueueStmts.Add("queue.pending_counts", `SELECT d.n, d.age, m.n, m.age FROM
		(SELECT COUNT_BIG(*) AS n, DATEDIFF_BIG(SECOND, MIN(created_at), SYSUTCDATETIME()) AS age FROM client_dlr WHERE 1=1{own}) AS d
		CROSS JOIN (SELECT COUNT_BIG(*) AS n, DATEDIFF_BIG(SECOND, MIN(received_at), SYSUTCDATETIME()) AS age FROM mo_queue WHERE status = 'queued'{own2}) AS m`, false)
	stQPendingDLR = QueueStmts.Add("queue.pending_by_account.dlr", `SELECT TOP (@p1) account_id, COUNT_BIG(*),
		SUM(CASE WHEN next_attempt_at IS NULL OR next_attempt_at <= SYSUTCDATETIME() THEN 1 ELSE 0 END),
		SUM(CASE WHEN attempts > 0 THEN 1 ELSE 0 END), DATEDIFF_BIG(SECOND, MIN(created_at), SYSUTCDATETIME())
		FROM client_dlr WHERE 1=1{own} GROUP BY account_id ORDER BY COUNT_BIG(*) DESC`, false)
	stQPendingMO = QueueStmts.Add("queue.pending_by_account.mo", `SELECT TOP (@p1) account_id, COUNT_BIG(*),
		SUM(CASE WHEN next_attempt_at IS NULL OR next_attempt_at <= SYSUTCDATETIME() THEN 1 ELSE 0 END),
		SUM(CASE WHEN attempts > 0 THEN 1 ELSE 0 END), DATEDIFF_BIG(SECOND, MIN(received_at), SYSUTCDATETIME())
		FROM mo_queue WHERE status = 'queued'{own} GROUP BY account_id ORDER BY COUNT_BIG(*) DESC`, false)

	// The incoming messages (mysql.go on MySQL). next_attempt_at is the cut
	// second, as MySQL's NOW(): due at once.
	stQMOStage = QueueStmts.Add("queue.mo.stage", `INSERT INTO mo_queue
		(msg_id,trace_id,vendor_id,account_id,route_id,sender,destination,body,encoding,segments,mcc,mnc,
		 queue_owner,company_id,priced,status,priority,next_attempt_at)
		VALUES (@p1,@p2,@p3,@p4,NULLIF(@p5, 0),@p6,@p7,@p8,@p9,@p10,NULLIF(@p11, N''),NULLIF(@p12, N''),
		 NULLIF(@p13, N''),NULLIF(@p14, 0),NULLIF(@p15, N''),'queued',9,`+qSecsFromNow("0")+`)`, true)
	stQMOClaim   = QueueStmts.Add("queue.mo.claim", qOneClaim(qMOClaim), true)
	stQMOAck     = QueueStmts.Add("queue.mo.ack", "DELETE t FROM "+qIDsMsg+"mo_queue AS t ON t.msg_id = j.v", true)
	stQMOResched = QueueStmts.Add("queue.mo.reschedule", "UPDATE mo_queue SET next_attempt_at = "+qSecsFromNow("@p1")+" WHERE msg_id = @p2", false)
	stQMOGiveUp  = QueueStmts.Add("queue.mo.give_up", "UPDATE mo_queue SET status = 'expired' WHERE msg_id = @p1 AND status = 'queued'", false)
	stQMOSweep   = QueueStmts.Add("queue.mo.sweep", "UPDATE mo_queue SET status = 'expired' WHERE status = 'queued' AND received_at < DATEADD(SECOND, -@p1, SYSUTCDATETIME()){own}", false)
	stQMOExpire  = QueueStmts.Add("queue.mo.expire", "UPDATE mo_queue SET status = 'expired' WHERE status = 'queued' AND valid_until IS NOT NULL AND valid_until <= SYSUTCDATETIME(){own}", false)

	// The SMPP part ids (mysql.go on MySQL).
	stQPartsSave = AddUpsert(QueueStmts, "queue.part_ids.save", UpsertSpec{
		Table: "client_part_ids", Keys: []string{"msg_id"}, Set: []string{"part_ids = $part_ids"},
	}, false)
	stQPartsOne  = QueueStmts.Add("queue.part_ids.read", "SELECT part_ids FROM client_part_ids WHERE msg_id = @p1", false)
	stQPartsMany = QueueStmts.Add("queue.part_ids.read_many", "SELECT TOP (500) t.msg_id, t.part_ids FROM "+qIDsMsg+"client_part_ids AS t ON t.msg_id = j.v", false)
	stQPartsDrop = QueueStmts.Add("queue.part_ids.drop", "DELETE FROM client_part_ids WHERE msg_id = @p1", false)
)

// qMoveCols is every column a queue row carries, as MoveQueued copies them
// (MySQL's moveCols).
const qMoveCols = `msg_id,trace_id,request_id,account_id,username,vendor_id,route_id,sender,destination,body,encoding,segments,
	udh_ref,udh_total,udh_seq,gsm7,template_id,channel,ap1,ap2,apg1,apg2,transport,ingress,mcc,mnc,status,priority,
	idempotency_key,valid_until,attempts,no_receipt,failure_receipt_only,want_intermediate_receipt,bind_slot,mask_body,
	spam_flag,orig_sender,orig_destination,orig_body,next_attempt_at,created_at,hlr_refreshed,tried_vendors,
	campaign_id,campaign_hash,code_reroute,sender_by,client_sender,send_round,route_level,deployment_id,billing_model,queue_owner,quote,
	company_id,hlr_source,hlr_ported,error_text,ip_address,shortcode,pick_at`

// qText is a value for a text column (NVARCHAR(MAX): one declaration for
// every call, see qkText).
func qText(s string) mssql.NVarCharMax { return mssql.NVarCharMax(s) }

// qWithOwn writes an owner's condition into a registered text.
func qWithOwn(text, own string) string { return strings.ReplaceAll(text, "{own}", own) }
