package mssql

// Summaries written with the records, on SQL Server (mirrors
// internal/dal/mysql/summaries.go): whoever writes a record adds what it
// changed to summary_mt, summary_mo, summary_lookup, summary_dlr,
// summary_rejects and summary_orphans, in the SAME transaction as the record.
// What a message contributes is worked out from the message as it is written
// (recMTRowOf), and what a closing receipt adds from the message's row read
// back by its key (recReadMTRows), so the two are the same figures to the
// second — the same rules, the same keys and the same counts as MySQL's.
//
// On SQL Server every summary a write touches is ONE batch, one round trip:
// for each table an UPDATE of the rows that are there (the rows read WITH
// (UPDLOCK, SERIALIZABLE) in the keys' order, as MySQL's upsert locks them)
// and an INSERT of the rest, the rows given as one JSON parameter. Keys are
// sorted the same way by every writer, and the JSON drives the join in that
// order, so two writes that touch the same rows wait for each other and
// never deadlock. updated_at is set by the UPDATE (MySQL's ON UPDATE
// CURRENT_TIMESTAMP).
//
// MySQL's flush also takes counts DOWN (a message moved to its priced row);
// nothing calls it with a negative count since the records are written
// priced (every addMT and addMO is +1), so that half is not written here.

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/dal/stmt"
	"github.com/yakugate/yakugate/internal/model"
)

// recBucket is the start of the 15-minute bucket of a Unix time, UTC.
func recBucket(unix int64) int64 {
	b := unix - unix%900
	if unix < 0 && unix%900 != 0 {
		b -= 900
	}
	return b
}

// ---- outgoing (summary_mt) ----

type mtKey struct {
	bucket    int64
	account   int64
	vendor    int64
	mcc, mnc  string
	sender    string
	sell, buy int64
}

type mtCounts struct {
	submittedParts, deliveredParts, undelivParts, expiredParts int64
	vendorAcceptedParts, sumVendorAckSec, maxVendorAckSec      int64
	submittedMsgs, deliveredMsgs, failedMsgs, expiredMsgs      int64
	acceptedParts, acceptedMsgs                                int64
}

// mtRow is what one message row contributes to summary_mt.
type mtRow struct {
	msgID     string
	key       mtKey
	segs      int64
	status    string
	acked     bool
	ackSec    int64
	hlrSource string
	ported    bool
	quote     string
}

var (
	// What an mt row contributes to its summary row (MySQL's mtRowCols):
	// the vendor and its buying rate are the message's newest ACCEPTED
	// submission's (sub_), none = vendor 0 and buy 0.
	stRecMTRows = RecordStmts.Add("record.summary.mt_rows", `SELECT TOP (500) m.msg_id,
		DATEDIFF_BIG(SECOND, '19700101', COALESCE(m.accepted_at, m.created_at)), COALESCE(m.account_id, 0),
		COALESCE(k.vendor_id, 0), COALESCE(m.mcc, N''), COALESCE(m.mnc, N''), LEFT(COALESCE(m.sender, N''), 20),
		COALESCE(m.sell_rate_id, 0), COALESCE(k.buy_rate_id, 0), COALESCE(m.segments, 1)
		FROM `+rIDsFrom+`{mt} AS m ON m.msg_id = j.v
		OUTER APPLY (SELECT TOP (1) s.vendor_id, s.buy_rate_id FROM {sub} AS s
			WHERE s.msg_id = m.msg_id AND s.result = 'accepted' ORDER BY s.attempt DESC, s.part) AS k`, true)
	stRecMTRowsAlone = RecordStmts.Add("record.summary.mt_rows_alone", `SELECT TOP (500) m.msg_id,
		DATEDIFF_BIG(SECOND, '19700101', COALESCE(m.accepted_at, m.created_at)), COALESCE(m.account_id, 0),
		0, COALESCE(m.mcc, N''), COALESCE(m.mnc, N''), LEFT(COALESCE(m.sender, N''), 20),
		COALESCE(m.sell_rate_id, 0), 0, COALESCE(m.segments, 1)
		FROM `+rIDsFrom+`{mt} AS m ON m.msg_id = j.v`, true)
	// An incoming record for its summary row: summary_mo.sender_id is the
	// short code it arrived on, the record's destination (MySQL's moRowCols).
	stRecMORows = RecordStmts.Add("record.summary.mo_rows", `SELECT TOP (500) m.msg_id,
		DATEDIFF_BIG(SECOND, '19700101', COALESCE(m.received_at, m.created_at)), COALESCE(m.account_id, 0), COALESCE(m.vendor_id, 0),
		COALESCE(m.mcc, N''), COALESCE(m.mnc, N''), LEFT(COALESCE(m.destination, N''), 20),
		COALESCE(m.sell_rate_id, 0), COALESCE(m.buy_rate_id, 0), COALESCE(m.segments, 1)
		FROM `+rIDsFrom+`{mo} AS m ON m.msg_id = j.v`, true)
)

// recReadMTRows reads the summary keys of the rows of ids in table (MySQL's
// readMTRows). A day with no sub_ table yet reads the rows as their
// acceptance counted them.
func recReadMTRows(ctx context.Context, x Querier, table string, ids []string) (map[string]mtRow, error) {
	out := map[string]mtRow{}
	sub := billingstore.SwapKind(table, billingstore.KindSub)
	for from := 0; from < len(ids); from += rChunk {
		part := ids[from:min(from+rChunk, len(ids))]
		rows, err := rQuery(ctx, x, stRecMTRows, rOn(stRecMTRows.SQL, "mt", table, "sub", sub), rIDs(part))
		if err != nil && (MissingTable(err) || recUnknownColumn(err)) {
			rows, err = rQuery(ctx, x, stRecMTRowsAlone, rOn(stRecMTRowsAlone.SQL, "mt", table), rIDs(part))
		}
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var r mtRow
			var at int64
			if err := rows.Scan(&r.msgID, &at, &r.key.account, &r.key.vendor, &r.key.mcc, &r.key.mnc, &r.key.sender,
				&r.key.sell, &r.key.buy, &r.segs); err != nil {
				rows.Close()
				return nil, err
			}
			r.msgID = rTrim(r.msgID)
			r.key.bucket = recBucket(at)
			if r.segs < 1 {
				r.segs = 1
			}
			out[r.msgID] = r
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// recUnknownColumn says err is "Invalid column name" (207): MySQL's 1054.
func recUnknownColumn(err error) bool {
	n, ok := ErrorNumber(err)
	return ok && n == 207
}

// recMTRowOf is what one message contributes to its summary row as it is
// written (MySQL's mtRowOf).
func recMTRowOf(m model.Message, rates billing.RecordRates, ackAt time.Time, status string, now time.Time) mtRow {
	acc := billingstore.IDTime(m.ID, now)
	r := mtRow{msgID: m.ID, segs: int64(m.Segments), status: status,
		hlrSource: m.HLRSource, ported: m.HLRPorted, quote: m.Quote,
		key: mtKey{bucket: recBucket(acc.Unix()), account: m.AccountID, vendor: m.VendorID,
			mcc: m.MCC, mnc: m.MNC, sender: rRunes(m.Sender, 20), sell: rates.Sell, buy: rates.Buy}}
	if r.segs < 1 {
		r.segs = 1
	}
	if !ackAt.IsZero() {
		r.acked = true
		if d := ackAt.Sub(acc); d > 0 {
			r.ackSec = int64(d / time.Second)
		}
	}
	return r
}

func (r mtRow) accepted() mtCounts { return mtCounts{acceptedParts: r.segs, acceptedMsgs: 1} }

func (r mtRow) submitted() mtCounts {
	q := r
	q.status = ""
	return q.whole()
}

func (r mtRow) whole() mtCounts {
	c := r.outcome()
	c.submittedParts, c.submittedMsgs = r.segs, 1
	if r.acked {
		c.vendorAcceptedParts = r.segs
		c.sumVendorAckSec = r.ackSec * r.segs
		c.maxVendorAckSec = r.ackSec
	}
	return c
}

func (r mtRow) outcome() mtCounts {
	var c mtCounts
	switch r.status {
	case "delivered":
		c.deliveredParts, c.deliveredMsgs = r.segs, 1
	case "failed":
		c.undelivParts, c.failedMsgs = r.segs, 1
	case "expired":
		c.expiredParts, c.expiredMsgs = r.segs, 1
	}
	return c
}

func (c *mtCounts) add(o mtCounts) {
	c.submittedParts += o.submittedParts
	c.deliveredParts += o.deliveredParts
	c.undelivParts += o.undelivParts
	c.expiredParts += o.expiredParts
	c.vendorAcceptedParts += o.vendorAcceptedParts
	c.sumVendorAckSec += o.sumVendorAckSec
	if o.maxVendorAckSec > c.maxVendorAckSec {
		c.maxVendorAckSec = o.maxVendorAckSec
	}
	c.submittedMsgs += o.submittedMsgs
	c.deliveredMsgs += o.deliveredMsgs
	c.failedMsgs += o.failedMsgs
	c.expiredMsgs += o.expiredMsgs
	c.acceptedParts += o.acceptedParts
	c.acceptedMsgs += o.acceptedMsgs
}

func (c mtCounts) values() []int64 {
	return []int64{c.submittedParts, c.deliveredParts, c.undelivParts, c.expiredParts,
		c.vendorAcceptedParts, c.sumVendorAckSec, c.submittedMsgs, c.deliveredMsgs, c.failedMsgs, c.expiredMsgs,
		c.acceptedParts, c.acceptedMsgs}
}

// ---- incoming (summary_mo) ----

type moKey struct {
	bucket    int64
	account   int64
	vendor    int64
	mcc, mnc  string
	sender    string
	sell, buy int64
}

type moCounts struct {
	receivedParts, receivedMsgs, clientDeliveredParts, clientFailedParts int64
}

type moRow struct {
	msgID  string
	key    moKey
	segs   int64
	status string
}

// recReadMORows reads the summary keys of the incoming records of ids.
func recReadMORows(ctx context.Context, x Querier, table string, ids []string) (map[string]moRow, error) {
	out := map[string]moRow{}
	for from := 0; from < len(ids); from += rChunk {
		part := ids[from:min(from+rChunk, len(ids))]
		rows, err := rQuery(ctx, x, stRecMORows, rOn(stRecMORows.SQL, "mo", table), rIDs(part))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var r moRow
			var at int64
			if err := rows.Scan(&r.msgID, &at, &r.key.account, &r.key.vendor, &r.key.mcc, &r.key.mnc, &r.key.sender,
				&r.key.sell, &r.key.buy, &r.segs); err != nil {
				rows.Close()
				return nil, err
			}
			r.msgID = rTrim(r.msgID)
			r.key.bucket = recBucket(at)
			if r.segs < 1 {
				r.segs = 1
			}
			out[r.msgID] = r
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// recMORowOf is what an incoming message contributes as it is written.
func recMORowOf(m model.Message, rates billing.RecordRates, now time.Time) moRow {
	r := moRow{msgID: m.ID, segs: int64(m.Segments),
		key: moKey{bucket: recBucket(now.Unix()), account: m.AccountID, vendor: m.VendorID,
			mcc: m.MCC, mnc: m.MNC, sender: rRunes(m.Destination, 20), sell: rates.Sell, buy: rates.Buy}}
	if r.segs < 1 {
		r.segs = 1
	}
	return r
}

func (r moRow) whole() moCounts {
	c := r.outcome()
	c.receivedParts, c.receivedMsgs = r.segs, 1
	return c
}

func (r moRow) outcome() moCounts {
	var c moCounts
	switch r.status {
	case "delivered":
		c.clientDeliveredParts = r.segs
	case "failed", "expired":
		c.clientFailedParts = r.segs
	}
	return c
}

func (c *moCounts) add(o moCounts) {
	c.receivedParts += o.receivedParts
	c.receivedMsgs += o.receivedMsgs
	c.clientDeliveredParts += o.clientDeliveredParts
	c.clientFailedParts += o.clientFailedParts
}

func (c moCounts) values() []int64 {
	return []int64{c.receivedParts, c.receivedMsgs, c.clientDeliveredParts, c.clientFailedParts}
}

// ---- number lookups (summary_lookup) ----

type lookupKey struct {
	bucket   int64
	account  int64
	vendor   int64
	mcc, mnc string
}

type lookupCounts struct {
	requests, cacheHits, found, notFound, ported, charged int64
	chargedAmount                                         float64
}

// recLookupOf is what one new message row adds to summary_lookup (MySQL's
// lookupOf).
func recLookupOf(r mtRow) (lookupKey, lookupCounts, bool) {
	var c lookupCounts
	switch r.hlrSource {
	case "db":
		c.requests, c.cacheHits, c.found = 1, 1, 1
	case "dip":
		c.requests, c.found = 1, 1
	case "miss", "dip_miss":
		c.requests, c.notFound = 1, 1
	}
	if r.ported && c.requests > 0 {
		c.ported = 1
	}
	if amt, _ := recQuoteLookup(r.quote); amt > 0 {
		c.charged, c.chargedAmount = 1, amt
	}
	if c.requests == 0 && c.charged == 0 {
		return lookupKey{}, c, false
	}
	return lookupKey{bucket: r.key.bucket, account: r.key.account, vendor: r.key.vendor,
		mcc: r.key.mcc, mnc: r.key.mnc}, c, true
}

// ---- the rest: added one row at a time ----

type dlrKey struct {
	bucket          int64
	account, vendor int64
	stat, errorCode string
}

type rejectKey struct {
	bucket     int64
	account    int64
	channel    string
	reasonCode string
}

type orphanKey struct {
	bucket            int64
	kind              string
	vendor            int64
	reason, shortcode string
}

// sums is what one write adds to the summaries.
type sums struct {
	mt      map[mtKey]*mtCounts
	mo      map[moKey]*moCounts
	lookup  map[lookupKey]*lookupCounts
	dlr     map[dlrKey]int64
	rejects map[rejectKey]*[2]int64 // msgs, parts
	orphans map[orphanKey]int64
}

func newSums() *sums {
	return &sums{mt: map[mtKey]*mtCounts{}, mo: map[moKey]*moCounts{}, lookup: map[lookupKey]*lookupCounts{},
		dlr: map[dlrKey]int64{}, rejects: map[rejectKey]*[2]int64{}, orphans: map[orphanKey]int64{}}
}

func (d *sums) addMT(k mtKey, c mtCounts) {
	t := d.mt[k]
	if t == nil {
		t = &mtCounts{}
		d.mt[k] = t
	}
	t.add(c)
}

func (d *sums) addMO(k moKey, c moCounts) {
	t := d.mo[k]
	if t == nil {
		t = &moCounts{}
		d.mo[k] = t
	}
	t.add(c)
}

func (d *sums) addLookup(r mtRow) {
	k, c, ok := recLookupOf(r)
	if !ok {
		return
	}
	t := d.lookup[k]
	if t == nil {
		t = &lookupCounts{}
		d.lookup[k] = t
	}
	t.requests += c.requests
	t.cacheHits += c.cacheHits
	t.found += c.found
	t.notFound += c.notFound
	t.ported += c.ported
	t.charged += c.charged
	t.chargedAmount += c.chargedAmount
}

func (d *sums) addReceipt(at time.Time, account, vendor int64, stat, errorCode string) {
	d.dlr[dlrKey{bucket: recBucket(at.Unix()), account: account, vendor: vendor,
		stat: rRunes(stat, 8), errorCode: rRunes(errorCode, 32)}]++
}

func (d *sums) addReject(at time.Time, account int64, channel, reasonCode string, parts int64) {
	k := rejectKey{bucket: recBucket(at.Unix()), account: account, channel: channel, reasonCode: rRunes(reasonCode, 191)}
	t := d.rejects[k]
	if t == nil {
		t = &[2]int64{}
		d.rejects[k] = t
	}
	t[0]++
	t[1] += parts
}

func (d *sums) addOrphan(at time.Time, kind string, vendor int64, reason, shortcode string) {
	d.orphans[orphanKey{bucket: recBucket(at.Unix()), kind: kind, vendor: vendor,
		reason: rRunes(reason, 64), shortcode: rRunes(shortcode, 32)}]++
}

func (d *sums) empty() bool {
	return len(d.mt) == 0 && len(d.mo) == 0 && len(d.lookup) == 0 && len(d.dlr) == 0 &&
		len(d.rejects) == 0 && len(d.orphans) == 0
}

// ---- the flush: one batch ----

var mtCountNames = []string{"submitted_parts", "delivered_parts", "undeliv_parts", "expired_parts",
	"vendor_accepted_parts", "sum_vendor_ack_sec", "submitted_msgs", "delivered_msgs", "failed_msgs", "expired_msgs",
	"accepted_parts", "accepted_msgs"}

var moCountNames = []string{"received_parts", "received_msgs", "client_delivered_parts", "client_failed_parts"}

// sumSpec is one summary table's upsert from a JSON parameter.
type sumSpec struct {
	table string
	param string
	keys  []jcol // the row's key, in the unique key's order
	adds  []jcol // counts added to the row (NULL read as 0)
	maxes []jcol // values only ever raised
	fixed [][2]string
}

// sumUpsertSQL is a summary table's UPDATE of the rows there and INSERT of
// the rest: one part of the flush's batch.
func sumUpsertSQL(s sumSpec) string {
	cols := append(append(append([]jcol(nil), s.keys...), s.adds...), s.maxes...)
	from := "OPENJSON(@" + s.param + ") " + jWith(cols) + " AS j"
	match := make([]string, len(s.keys))
	for i, k := range s.keys {
		match[i] = "t." + k.name + " = j." + k.name
	}
	for _, f := range s.fixed {
		match = append(match, "t."+f[0]+" = "+f[1])
	}
	on := strings.Join(match, " AND ")
	set := make([]string, 0, len(s.adds)+len(s.maxes)+1)
	for _, a := range s.adds {
		set = append(set, a.name+" = COALESCE(t."+a.name+", 0) + j."+a.name)
	}
	for _, m := range s.maxes {
		set = append(set, m.name+" = CASE WHEN j."+m.name+" > t."+m.name+" THEN j."+m.name+" ELSE t."+m.name+" END")
	}
	set = append(set, "updated_at = SYSUTCDATETIME()")
	names := jNames(cols, "")
	vals := jNames(cols, "j.")
	for _, f := range s.fixed {
		names += ", " + f[0]
		vals += ", " + f[1]
	}
	return fmt.Sprintf(`UPDATE t SET %s FROM %s INNER LOOP JOIN %s AS t WITH (UPDLOCK, SERIALIZABLE, FORCESEEK) ON %s;
INSERT INTO %s (%s) SELECT %s FROM %s WHERE NOT EXISTS (SELECT 1 FROM %s AS t WITH (UPDLOCK, SERIALIZABLE, FORCESEEK) WHERE %s);`,
		strings.Join(set, ", "), from, s.table, on, s.table, names, vals, from, s.table, on)
}

func countCols(names []string) []jcol {
	out := make([]jcol, len(names))
	for i, n := range names {
		out[i] = jcol{n, jBig}
	}
	return out
}

var (
	sumMT = sumSpec{table: "summary_mt", param: "mt",
		keys: jcols("bucket_start", jSec, "account_id", jBig, "vendor_id", jBig, "mcc", jTxt, "mnc", jTxt,
			"sender_id", jTxt, "sell_card_id", jBig, "buy_card_id", jBig),
		adds: countCols(mtCountNames), maxes: jcols("max_vendor_ack_sec", jBig)}
	sumMO = sumSpec{table: "summary_mo", param: "mo",
		keys: jcols("bucket_start", jSec, "account_id", jBig, "vendor_id", jBig, "mcc", jTxt, "mnc", jTxt,
			"sender_id", jTxt, "sell_card_id", jBig, "buy_card_id", jBig),
		adds: countCols(moCountNames)}
	sumLookup = sumSpec{table: "summary_lookup", param: "lk",
		keys: jcols("bucket_start", jSec, "account_id", jBig, "vendor_id", jBig, "mcc", jTxt, "mnc", jTxt),
		adds: append(countCols([]string{"requests", "cache_hits", "found", "not_found", "ported_parts", "charged_lookups"}),
			jcol{"charged_amount", "DECIMAL(16,6)"}),
		fixed: [][2]string{{"service", "'hlr'"}, {"sell_card_id", "0"}, {"buy_card_id", "0"}}}
	sumDLR = sumSpec{table: "summary_dlr", param: "dl",
		keys: jcols("bucket_start", jSec, "account_id", jBig, "vendor_id", jBig, "stat", jTxt, "error_code", jTxt),
		adds: countCols([]string{"receipts"})}
	sumRejects = sumSpec{table: "summary_rejects", param: "rj",
		keys: jcols("bucket_start", jSec, "account_id", jBig, "channel", jTxt, "reason_code", jTxt),
		adds: countCols([]string{"rejected_msgs", "rejected_parts"})}
	sumOrphans = sumSpec{table: "summary_orphans", param: "orp",
		keys: jcols("bucket_start", jSec, "kind", jTxt, "vendor_id", jBig, "reason", jTxt, "shortcode", jTxt),
		adds: countCols([]string{"orphans"})}

	stSumMT      = RecordStmts.Add("record.summary.mt", sumUpsertSQL(sumMT), true)
	stSumMO      = RecordStmts.Add("record.summary.mo", sumUpsertSQL(sumMO), true)
	stSumLookup  = RecordStmts.Add("record.summary.lookup", sumUpsertSQL(sumLookup), true)
	stSumDLR     = RecordStmts.Add("record.summary.dlr", sumUpsertSQL(sumDLR), true)
	stSumRejects = RecordStmts.Add("record.summary.rejects", sumUpsertSQL(sumRejects), true)
	stSumOrphans = RecordStmts.Add("record.summary.orphans", sumUpsertSQL(sumOrphans), true)
)

func mtKeyLess(a, b mtKey) bool {
	if a.bucket != b.bucket {
		return a.bucket < b.bucket
	}
	if a.account != b.account {
		return a.account < b.account
	}
	if a.vendor != b.vendor {
		return a.vendor < b.vendor
	}
	if a.mcc != b.mcc {
		return a.mcc < b.mcc
	}
	if a.mnc != b.mnc {
		return a.mnc < b.mnc
	}
	if a.sender != b.sender {
		return a.sender < b.sender
	}
	if a.sell != b.sell {
		return a.sell < b.sell
	}
	return a.buy < b.buy
}

// flushSums writes d in ONE batch (MySQL's flushSums: one upsert a table).
// Rows with nothing to add are left out, as MySQL's are.
func flushSums(ctx context.Context, x Querier, d *sums) error {
	if d == nil || d.empty() {
		return nil
	}
	var parts []string
	var used []*stmt.Stmt
	var args []any
	add := func(st *stmt.Stmt, param string, rows [][]any) {
		if len(rows) == 0 {
			return
		}
		parts = append(parts, st.SQL)
		used = append(used, st)
		args = append(args, sql.Named(param, rJSON(rows)))
	}

	mtKeys := make([]mtKey, 0, len(d.mt))
	for k := range d.mt {
		mtKeys = append(mtKeys, k)
	}
	sort.Slice(mtKeys, func(i, j int) bool { return mtKeyLess(mtKeys[i], mtKeys[j]) })
	var mtRows [][]any
	for _, k := range mtKeys {
		c := d.mt[k]
		has := c.maxVendorAckSec > 0
		for _, v := range c.values() {
			has = has || v > 0
		}
		if !has {
			continue
		}
		row := []any{rSecText(k.bucket), k.account, k.vendor, k.mcc, k.mnc, k.sender, k.sell, k.buy}
		for _, v := range c.values() {
			row = append(row, v)
		}
		mtRows = append(mtRows, append(row, c.maxVendorAckSec))
	}
	add(stSumMT, "mt", mtRows)

	moKeys := make([]moKey, 0, len(d.mo))
	for k := range d.mo {
		moKeys = append(moKeys, k)
	}
	sort.Slice(moKeys, func(i, j int) bool { return mtKeyLess(mtKey(moKeys[i]), mtKey(moKeys[j])) })
	var moRows [][]any
	for _, k := range moKeys {
		c := d.mo[k]
		up := false
		for _, v := range c.values() {
			up = up || v > 0
		}
		if !up {
			continue
		}
		row := []any{rSecText(k.bucket), k.account, k.vendor, k.mcc, k.mnc, k.sender, k.sell, k.buy}
		for _, v := range c.values() {
			row = append(row, v)
		}
		moRows = append(moRows, row)
	}
	add(stSumMO, "mo", moRows)

	lkKeys := make([]lookupKey, 0, len(d.lookup))
	for k := range d.lookup {
		lkKeys = append(lkKeys, k)
	}
	sort.Slice(lkKeys, func(i, j int) bool {
		a, b := lkKeys[i], lkKeys[j]
		return mtKeyLess(mtKey{bucket: a.bucket, account: a.account, vendor: a.vendor, mcc: a.mcc, mnc: a.mnc},
			mtKey{bucket: b.bucket, account: b.account, vendor: b.vendor, mcc: b.mcc, mnc: b.mnc})
	})
	var lkRows [][]any
	for _, k := range lkKeys {
		c := d.lookup[k]
		lkRows = append(lkRows, []any{rSecText(k.bucket), k.account, k.vendor, k.mcc, k.mnc,
			c.requests, c.cacheHits, c.found, c.notFound, c.ported, c.charged, strconv.FormatFloat(c.chargedAmount, 'f', -1, 64)})
	}
	add(stSumLookup, "lk", lkRows)

	dlKeys := make([]dlrKey, 0, len(d.dlr))
	for k := range d.dlr {
		dlKeys = append(dlKeys, k)
	}
	sort.Slice(dlKeys, func(i, j int) bool {
		a, b := dlKeys[i], dlKeys[j]
		if a.bucket != b.bucket {
			return a.bucket < b.bucket
		}
		if a.account != b.account {
			return a.account < b.account
		}
		if a.vendor != b.vendor {
			return a.vendor < b.vendor
		}
		if a.stat != b.stat {
			return a.stat < b.stat
		}
		return a.errorCode < b.errorCode
	})
	var dlRows [][]any
	for _, k := range dlKeys {
		dlRows = append(dlRows, []any{rSecText(k.bucket), k.account, k.vendor, k.stat, k.errorCode, d.dlr[k]})
	}
	add(stSumDLR, "dl", dlRows)

	rjKeys := make([]rejectKey, 0, len(d.rejects))
	for k := range d.rejects {
		rjKeys = append(rjKeys, k)
	}
	sort.Slice(rjKeys, func(i, j int) bool {
		a, b := rjKeys[i], rjKeys[j]
		if a.bucket != b.bucket {
			return a.bucket < b.bucket
		}
		if a.account != b.account {
			return a.account < b.account
		}
		if a.channel != b.channel {
			return a.channel < b.channel
		}
		return a.reasonCode < b.reasonCode
	})
	var rjRows [][]any
	for _, k := range rjKeys {
		rjRows = append(rjRows, []any{rSecText(k.bucket), k.account, k.channel, k.reasonCode, d.rejects[k][0], d.rejects[k][1]})
	}
	add(stSumRejects, "rj", rjRows)

	orKeys := make([]orphanKey, 0, len(d.orphans))
	for k := range d.orphans {
		orKeys = append(orKeys, k)
	}
	sort.Slice(orKeys, func(i, j int) bool {
		a, b := orKeys[i], orKeys[j]
		if a.bucket != b.bucket {
			return a.bucket < b.bucket
		}
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		if a.vendor != b.vendor {
			return a.vendor < b.vendor
		}
		if a.reason != b.reason {
			return a.reason < b.reason
		}
		return a.shortcode < b.shortcode
	})
	var orRows [][]any
	for _, k := range orKeys {
		orRows = append(orRows, []any{rSecText(k.bucket), k.kind, k.vendor, k.reason, k.shortcode, d.orphans[k]})
	}
	add(stSumOrphans, "orp", orRows)

	if len(parts) == 0 {
		return nil
	}
	start := time.Now()
	_, err := x.ExecContext(ctx, "SET NOCOUNT ON;\n"+strings.Join(parts, "\n"), args...)
	for _, st := range used {
		observe(st, start, 0)
	}
	if err != nil {
		return fmt.Errorf("summaries: %w", err)
	}
	return nil
}
