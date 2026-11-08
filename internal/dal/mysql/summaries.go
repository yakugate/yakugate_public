package mysql

// Summaries written with the records (Saleem 2026-10-14, D-136: "all
// summaries, mo, mt, hlr, dlr, rejected, all are written by db worker if redis
// is there, else written by each process"; every dashboard reads them only).
//
// Until now the billing role REBUILT summary_mt, summary_mo and summary_lookup
// on each sweep: it found the buckets whose records had changed (by
// record_index.updated_at) and summed them again from the records. Now
// whoever writes a record adds what it changed, in the SAME transaction as
// the record itself:
//
//   - a new message row adds all of itself to its bucket;
//   - a message that becomes final adds its outcome (delivered, failed,
//     expired) to the bucket it is already in;
//   - a message that is priced moves all of itself from the unpriced row
//     (rate ids 0) to the priced one;
//   - a receipt, a refusal, an orphan adds one.
//
// Counted once (insert-only logs, issue 14): a writer reads which of its
// rows are on the log already and inserts — and counts — only the others; a
// receipt counts its message's outcome only when it is the one that closed it
// (dlr_.final_of). What a message contributes is worked out from the message
// as it is written (mtRowOf), and what a closing receipt adds from the
// message's row read back by its key (readMTRows), so the two are the same
// figures to the second.

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/billingstore"
	"github.com/yakugate/yakugate/internal/model"
)

// dbtx is a pool or a transaction: what a summary write runs on.
type dbtx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// bucketOf is the start of the 15-minute bucket of a Unix time, UTC.
func bucketOf(unix int64) int64 {
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
	// accepted* are counted when the message's mt_ row is written, at
	// acceptance (hot/0071, the three-table design of 27 Oct 2026), on the
	// account's row with no vendor; submitted* when a vendor took it.
	acceptedParts, acceptedMsgs int64
}

// mtRow is what one message row contributes to summary_mt.
type mtRow struct {
	msgID  string
	key    mtKey
	segs   int64
	status string
	acked  bool
	ackSec int64
	// For summary_lookup: how its network was found, ported, and the lookup
	// price fixed at acceptance (quote).
	hlrSource string
	ported    bool
	quote     string
}

// mtRowCols reads what an mt row contributes to its summary row; scanMTRow
// is its pair. The bucket is the message's accept time (its id's), the sender
// its first 20 characters, the selling rate id the row's own (issue 16). Read
// for a receipt that closes the message: its outcome goes to the row the
// message was counted in when a vendor took it.
//
// mt_ holds nothing about the vendor (the three-table design, 27 Oct 2026):
// the vendor and its buying rate are those of the message's newest ACCEPTED
// submission in sub (its sub_ table, the same company and day). No accepted
// submission (refused, out of tries, validity over) = vendor 0 and buy 0, the
// row its acceptance was counted in. sub "" = no sub_ table to ask.
func mtRowCols(sub string) string {
	vendor, buy := "0", "0"
	if sub != "" {
		pick := " FROM " + sub + " s WHERE s.msg_id = m.msg_id AND s.result = 'accepted' ORDER BY s.attempt DESC, s.part LIMIT 1)"
		vendor = "COALESCE((SELECT s.vendor_id" + pick + ",0)"
		buy = "COALESCE((SELECT s.buy_rate_id" + pick + ",0)"
	}
	return `m.msg_id, UNIX_TIMESTAMP(COALESCE(m.accepted_at, m.created_at)), COALESCE(m.account_id,0), ` + vendor + `,
	COALESCE(m.mcc,''), COALESCE(m.mnc,''), LEFT(COALESCE(m.sender,''),20),
	COALESCE(m.sell_rate_id,0), ` + buy + `, COALESCE(m.segments,1)`
}

func scanMTRow(rows *sql.Rows) (mtRow, error) {
	var r mtRow
	var at float64
	err := rows.Scan(&r.msgID, &at, &r.key.account, &r.key.vendor, &r.key.mcc, &r.key.mnc, &r.key.sender,
		&r.key.sell, &r.key.buy, &r.segs)
	r.key.bucket = bucketOf(int64(at))
	if r.segs < 1 {
		r.segs = 1
	}
	return r, err
}

// readMTRows reads the summary keys of the rows of ids in table. lock reads
// them FOR SHARE: the row cannot go (retention) while its outcome is counted.
func readMTRows(ctx context.Context, x dbtx, table string, ids []string, lock bool) (map[string]mtRow, error) {
	out := map[string]mtRow{}
	sub := billingstore.SwapKind(table, billingstore.KindSub)
	for from := 0; from < len(ids); from += 500 {
		part := ids[from:min(from+500, len(ids))]
		args := make([]any, 0, len(part))
		for _, id := range part {
			args = append(args, id)
		}
		query := func(sub string) (*sql.Rows, error) {
			q := "SELECT " + mtRowCols(sub) + " FROM " + table + " m WHERE m.msg_id IN (" + placeholders(len(part)) + ")"
			if lock {
				q += " FOR SHARE OF m"
			}
			return x.QueryContext(ctx, q, args...)
		}
		rows, err := query(sub)
		if err != nil && sub != "" && (isNoSuchTable(err) || isUnknownColumnErr(err)) {
			// No sub_ table for that day yet (nothing was submitted), or one
			// made before hot/0070: the message as its acceptance counted it.
			rows, err = query("")
		}
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			r, err := scanMTRow(rows)
			if err != nil {
				rows.Close()
				return nil, err
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

// mtRowOf is what one message contributes to its summary row as it is
// written: the same figures readMTRows reads back from its row. ackAt is when
// the vendor took it (zero = none did); status is what the message is at
// that moment (sent, or final: delivered by the blackhole, failed or expired
// when it is rejected at submission).
func mtRowOf(m model.Message, rates billing.RecordRates, ackAt time.Time, status string, now time.Time) mtRow {
	acc := billingstore.IDTime(m.ID, now)
	r := mtRow{msgID: m.ID, segs: int64(m.Segments), status: status,
		hlrSource: m.HLRSource, ported: m.HLRPorted, quote: m.Quote,
		key: mtKey{bucket: bucketOf(acc.Unix()), account: m.AccountID, vendor: m.VendorID,
			mcc: m.MCC, mnc: m.MNC, sender: truncRunes(m.Sender, 20), sell: rates.Sell, buy: rates.Buy}}
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

// accepted is what a message adds when its mt_ row is written, at acceptance.
func (r mtRow) accepted() mtCounts {
	return mtCounts{acceptedParts: r.segs, acceptedMsgs: 1}
}

// submitted is what a message adds when a vendor took it: submitted, and the
// vendor's answer time.
func (r mtRow) submitted() mtCounts {
	q := r
	q.status = ""
	return q.whole()
}

// whole is everything a row contributes; outcome only what its final status
// adds (a row that becomes final).
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

func (c *mtCounts) add(o mtCounts, sign int64) {
	c.submittedParts += sign * o.submittedParts
	c.deliveredParts += sign * o.deliveredParts
	c.undelivParts += sign * o.undelivParts
	c.expiredParts += sign * o.expiredParts
	c.vendorAcceptedParts += sign * o.vendorAcceptedParts
	c.sumVendorAckSec += sign * o.sumVendorAckSec
	if sign > 0 && o.maxVendorAckSec > c.maxVendorAckSec {
		c.maxVendorAckSec = o.maxVendorAckSec // a max is only ever raised
	}
	c.submittedMsgs += sign * o.submittedMsgs
	c.deliveredMsgs += sign * o.deliveredMsgs
	c.failedMsgs += sign * o.failedMsgs
	c.expiredMsgs += sign * o.expiredMsgs
	c.acceptedParts += sign * o.acceptedParts
	c.acceptedMsgs += sign * o.acceptedMsgs
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

// moRowCols reads an incoming record for its summary row. summary_mo.sender_id
// is the short code the MO arrived on (hot/0016): the record's DESTINATION.
// It was read from the record's sender — the subscriber's number — so the
// summary had a row for nearly every subscriber (go-live test of 18 Oct 2026,
// P6 F3: 8,843 rows for 9,446 MO).
const moRowCols = `msg_id, UNIX_TIMESTAMP(COALESCE(received_at, created_at)), COALESCE(account_id,0), COALESCE(vendor_id,0),
	COALESCE(mcc,''), COALESCE(mnc,''), LEFT(COALESCE(destination,''),20),
	COALESCE(sell_rate_id,0), COALESCE(buy_rate_id,0), COALESCE(segments,1)`

// readMORows reads the summary keys of the incoming records of ids in table.
func readMORows(ctx context.Context, x dbtx, table string, ids []string, lock bool) (map[string]moRow, error) {
	out := map[string]moRow{}
	for from := 0; from < len(ids); from += 500 {
		part := ids[from:min(from+500, len(ids))]
		args := make([]any, 0, len(part))
		for _, id := range part {
			args = append(args, id)
		}
		q := "SELECT " + moRowCols + " FROM " + table + " WHERE msg_id IN (" + placeholders(len(part)) + ")"
		if lock {
			q += " FOR SHARE"
		}
		rows, err := x.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var r moRow
			var at float64
			if err := rows.Scan(&r.msgID, &at, &r.key.account, &r.key.vendor, &r.key.mcc, &r.key.mnc, &r.key.sender,
				&r.key.sell, &r.key.buy, &r.segs); err != nil {
				rows.Close()
				return nil, err
			}
			r.key.bucket = bucketOf(int64(at))
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

// moRowOf is what an incoming message contributes as it is written.
func moRowOf(m model.Message, rates billing.RecordRates, now time.Time) moRow {
	r := moRow{msgID: m.ID, segs: int64(m.Segments),
		key: moKey{bucket: bucketOf(now.Unix()), account: m.AccountID, vendor: m.VendorID,
			mcc: m.MCC, mnc: m.MNC, sender: truncRunes(m.Destination, 20), sell: rates.Sell, buy: rates.Buy}}
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

func (c *moCounts) add(o moCounts, sign int64) {
	c.receivedParts += sign * o.receivedParts
	c.receivedMsgs += sign * o.receivedMsgs
	c.clientDeliveredParts += sign * o.clientDeliveredParts
	c.clientFailedParts += sign * o.clientFailedParts
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

// lookupOf is what one new message row adds to summary_lookup: the same
// counting the summary was always rebuilt with (a lookup is 'db', 'dip',
// 'miss' or 'dip_miss'; an answer from the client, a range or the country
// is not one), and a charge when the quote priced its lookup.
func lookupOf(r mtRow) (lookupKey, lookupCounts, bool) {
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
	if amt, _ := quoteLookup(r.quote); amt > 0 {
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

func (d *sums) addMT(k mtKey, c mtCounts, sign int64) {
	t := d.mt[k]
	if t == nil {
		t = &mtCounts{}
		d.mt[k] = t
	}
	t.add(c, sign)
}

func (d *sums) addMO(k moKey, c moCounts, sign int64) {
	t := d.mo[k]
	if t == nil {
		t = &moCounts{}
		d.mo[k] = t
	}
	t.add(c, sign)
}

func (d *sums) addLookup(r mtRow) {
	k, c, ok := lookupOf(r)
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
	d.dlr[dlrKey{bucket: bucketOf(at.Unix()), account: account, vendor: vendor,
		stat: truncRunes(stat, 8), errorCode: truncRunes(errorCode, 32)}]++ // hot/0067: 32 (was 16)
}

func (d *sums) addReject(at time.Time, account int64, channel, reasonCode string, parts int64) {
	k := rejectKey{bucket: bucketOf(at.Unix()), account: account, channel: channel, reasonCode: truncRunes(reasonCode, 191)}
	t := d.rejects[k]
	if t == nil {
		t = &[2]int64{}
		d.rejects[k] = t
	}
	t[0]++
	t[1] += parts
}

func (d *sums) addOrphan(at time.Time, kind string, vendor int64, reason, shortcode string) {
	d.orphans[orphanKey{bucket: bucketOf(at.Unix()), kind: kind, vendor: vendor,
		reason: truncRunes(reason, 64), shortcode: truncRunes(shortcode, 32)}]++
}

func (d *sums) empty() bool {
	return len(d.mt) == 0 && len(d.mo) == 0 && len(d.lookup) == 0 && len(d.dlr) == 0 &&
		len(d.rejects) == 0 && len(d.orphans) == 0
}

// truncRunes cuts s to n characters, as LEFT() does.
func truncRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// flush writes d. Keys are written in one order by every writer, so two
// writes that touch the same summary rows wait for each other and never
// deadlock. Counts that go up are one upsert per table; counts that go down
// (a message moved to its priced row) are an UPDATE each, never below 0.
func (s *Store) flushSums(ctx context.Context, x dbtx, d *sums) error {
	if d == nil || d.empty() {
		return nil
	}
	if err := flushMT(ctx, x, d.mt); err != nil {
		return fmt.Errorf("summary_mt: %w", err)
	}
	if err := flushMO(ctx, x, d.mo); err != nil {
		return fmt.Errorf("summary_mo: %w", err)
	}
	if err := flushLookup(ctx, x, d.lookup); err != nil {
		return fmt.Errorf("summary_lookup: %w", err)
	}
	if err := flushDLR(ctx, x, d.dlr); err != nil {
		return fmt.Errorf("summary_dlr: %w", err)
	}
	if err := flushRejects(ctx, x, d.rejects); err != nil {
		return fmt.Errorf("summary_rejects: %w", err)
	}
	if err := flushOrphans(ctx, x, d.orphans); err != nil {
		return fmt.Errorf("summary_orphans: %w", err)
	}
	return nil
}

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

// pos and neg split a signed count.
func pos(v int64) int64 {
	if v > 0 {
		return v
	}
	return 0
}
func neg(v int64) int64 {
	if v < 0 {
		return -v
	}
	return 0
}

var mtCountNames = []string{"submitted_parts", "delivered_parts", "undeliv_parts", "expired_parts",
	"vendor_accepted_parts", "sum_vendor_ack_sec", "submitted_msgs", "delivered_msgs", "failed_msgs", "expired_msgs",
	"accepted_parts", "accepted_msgs"}

func (c mtCounts) values() []int64 {
	return []int64{c.submittedParts, c.deliveredParts, c.undelivParts, c.expiredParts,
		c.vendorAcceptedParts, c.sumVendorAckSec, c.submittedMsgs, c.deliveredMsgs, c.failedMsgs, c.expiredMsgs,
		c.acceptedParts, c.acceptedMsgs}
}

func flushMT(ctx context.Context, x dbtx, m map[mtKey]*mtCounts) error {
	if len(m) == 0 {
		return nil
	}
	keys := make([]mtKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return mtKeyLess(keys[i], keys[j]) })
	var ups []mtKey
	for _, k := range keys {
		c := m[k]
		vals := c.values()
		has := c.maxVendorAckSec > 0
		for _, v := range vals {
			if v > 0 {
				has = true
			}
		}
		if has {
			ups = append(ups, k)
		}
	}
	for from := 0; from < len(ups); from += 200 {
		part := ups[from:min(from+200, len(ups))]
		var sb strings.Builder
		sb.WriteString("INSERT INTO summary_mt (bucket_start, account_id, vendor_id, mcc, mnc, sender_id, sell_card_id, buy_card_id, " +
			strings.Join(mtCountNames, ", ") + ", max_vendor_ack_sec) VALUES ")
		args := make([]any, 0, len(part)*19)
		for i, k := range part {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("(FROM_UNIXTIME(?),?,?,?,?,?,?,?" + strings.Repeat(",?", len(mtCountNames)) + ",?)")
			args = append(args, k.bucket, k.account, k.vendor, k.mcc, k.mnc, k.sender, k.sell, k.buy)
			for _, v := range m[k].values() {
				args = append(args, pos(v))
			}
			args = append(args, m[k].maxVendorAckSec)
		}
		sb.WriteString(" ON DUPLICATE KEY UPDATE ")
		for i, n := range mtCountNames {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(n + "=COALESCE(" + n + ",0)+VALUES(" + n + ")")
		}
		sb.WriteString(", max_vendor_ack_sec=GREATEST(max_vendor_ack_sec, VALUES(max_vendor_ack_sec))")
		if _, err := x.ExecContext(ctx, sb.String(), args...); err != nil {
			return err
		}
	}
	for _, k := range keys {
		vals := m[k].values()
		down := false
		for _, v := range vals {
			if v < 0 {
				down = true
			}
		}
		if !down {
			continue
		}
		set := make([]string, 0, len(mtCountNames))
		args := make([]any, 0, len(mtCountNames)*2+8)
		for i, n := range mtCountNames {
			if v := neg(vals[i]); v > 0 {
				set = append(set, n+"=IF(COALESCE("+n+",0)>?, COALESCE("+n+",0)-?, 0)")
				args = append(args, v, v)
			}
		}
		keyArgs := []any{k.bucket, k.account, k.vendor, k.mcc, k.mnc, k.sender, k.sell, k.buy}
		args = append(args, keyArgs...)
		if _, err := x.ExecContext(ctx, "UPDATE summary_mt SET "+strings.Join(set, ", ")+
			" WHERE "+summaryKeyWhere, args...); err != nil {
			return err
		}
		// A row a move emptied (its messages were priced and counted under
		// their rate ids) goes: kept, it showed on the screens as a second
		// line of zeros. Same key, same transaction — a writer adding to it
		// meanwhile waits for the lock and then makes the row again.
		if _, err := x.ExecContext(ctx, "DELETE FROM summary_mt WHERE "+summaryKeyWhere+" AND "+allZero(mtCountNames), keyArgs...); err != nil {
			return err
		}
	}
	return nil
}

// summaryKeyWhere names one row of summary_mt or summary_mo by its key.
const summaryKeyWhere = "bucket_start=FROM_UNIXTIME(?) AND account_id=? AND vendor_id=? AND mcc=? AND mnc=? AND sender_id=? AND sell_card_id=? AND buy_card_id=?"

// allZero is "every one of these counts is 0" (NULL read as 0).
func allZero(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = "COALESCE(" + c + ",0)=0"
	}
	return strings.Join(parts, " AND ")
}

var moCountNames = []string{"received_parts", "received_msgs", "client_delivered_parts", "client_failed_parts"}

func (c moCounts) values() []int64 {
	return []int64{c.receivedParts, c.receivedMsgs, c.clientDeliveredParts, c.clientFailedParts}
}

func flushMO(ctx context.Context, x dbtx, m map[moKey]*moCounts) error {
	if len(m) == 0 {
		return nil
	}
	keys := make([]moKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return mtKeyLess(mtKey(keys[i]), mtKey(keys[j]))
	})
	for _, k := range keys {
		vals := m[k].values()
		up, down := false, false
		for _, v := range vals {
			up = up || v > 0
			down = down || v < 0
		}
		if up {
			args := []any{k.bucket, k.account, k.vendor, k.mcc, k.mnc, k.sender, k.sell, k.buy}
			for _, v := range vals {
				args = append(args, pos(v))
			}
			upd := make([]string, len(moCountNames))
			for i, n := range moCountNames {
				upd[i] = n + "=COALESCE(" + n + ",0)+VALUES(" + n + ")"
			}
			if _, err := x.ExecContext(ctx, "INSERT INTO summary_mo (bucket_start, account_id, vendor_id, mcc, mnc, sender_id, sell_card_id, buy_card_id, "+
				strings.Join(moCountNames, ", ")+") VALUES (FROM_UNIXTIME(?),?,?,?,?,?,?,?"+strings.Repeat(",?", len(moCountNames))+
				") ON DUPLICATE KEY UPDATE "+strings.Join(upd, ", "), args...); err != nil {
				return err
			}
		}
		if down {
			set := []string{}
			args := []any{}
			for i, n := range moCountNames {
				if v := neg(vals[i]); v > 0 {
					set = append(set, n+"=IF(COALESCE("+n+",0)>?, COALESCE("+n+",0)-?, 0)")
					args = append(args, v, v)
				}
			}
			keyArgs := []any{k.bucket, k.account, k.vendor, k.mcc, k.mnc, k.sender, k.sell, k.buy}
			args = append(args, keyArgs...)
			if _, err := x.ExecContext(ctx, "UPDATE summary_mo SET "+strings.Join(set, ", ")+
				" WHERE "+summaryKeyWhere, args...); err != nil {
				return err
			}
			// A row a move emptied goes, as in summary_mt.
			if _, err := x.ExecContext(ctx, "DELETE FROM summary_mo WHERE "+summaryKeyWhere+" AND "+allZero(moCountNames), keyArgs...); err != nil {
				return err
			}
		}
	}
	return nil
}

func flushLookup(ctx context.Context, x dbtx, m map[lookupKey]*lookupCounts) error {
	if len(m) == 0 {
		return nil
	}
	keys := make([]lookupKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		return mtKeyLess(mtKey{bucket: a.bucket, account: a.account, vendor: a.vendor, mcc: a.mcc, mnc: a.mnc},
			mtKey{bucket: b.bucket, account: b.account, vendor: b.vendor, mcc: b.mcc, mnc: b.mnc})
	})
	var sb strings.Builder
	sb.WriteString(`INSERT INTO summary_lookup (bucket_start, service, account_id, vendor_id, mcc, mnc, sell_card_id, buy_card_id,
		requests, cache_hits, found, not_found, ported_parts, failed, charged_lookups, charged_amount) VALUES `)
	args := make([]any, 0, len(keys)*12)
	for i, k := range keys {
		c := m[k]
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(FROM_UNIXTIME(?),'hlr',?,?,?,?,0,0,?,?,?,?,?,0,?,?)")
		args = append(args, k.bucket, k.account, k.vendor, k.mcc, k.mnc,
			c.requests, c.cacheHits, c.found, c.notFound, c.ported, c.charged, c.chargedAmount)
	}
	sb.WriteString(` ON DUPLICATE KEY UPDATE requests=requests+VALUES(requests), cache_hits=cache_hits+VALUES(cache_hits),
		found=found+VALUES(found), not_found=not_found+VALUES(not_found), ported_parts=ported_parts+VALUES(ported_parts),
		charged_lookups=charged_lookups+VALUES(charged_lookups), charged_amount=charged_amount+VALUES(charged_amount)`)
	_, err := x.ExecContext(ctx, sb.String(), args...)
	return err
}

func flushDLR(ctx context.Context, x dbtx, m map[dlrKey]int64) error {
	if len(m) == 0 {
		return nil
	}
	keys := make([]dlrKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
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
	var sb strings.Builder
	sb.WriteString("INSERT INTO summary_dlr (bucket_start, account_id, vendor_id, stat, error_code, receipts) VALUES ")
	args := make([]any, 0, len(keys)*6)
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(FROM_UNIXTIME(?),?,?,?,?,?)")
		args = append(args, k.bucket, k.account, k.vendor, k.stat, k.errorCode, m[k])
	}
	sb.WriteString(" ON DUPLICATE KEY UPDATE receipts=receipts+VALUES(receipts)")
	_, err := x.ExecContext(ctx, sb.String(), args...)
	return err
}

func flushRejects(ctx context.Context, x dbtx, m map[rejectKey]*[2]int64) error {
	if len(m) == 0 {
		return nil
	}
	keys := make([]rejectKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
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
	var sb strings.Builder
	sb.WriteString("INSERT INTO summary_rejects (bucket_start, account_id, channel, reason_code, rejected_msgs, rejected_parts) VALUES ")
	args := make([]any, 0, len(keys)*6)
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(FROM_UNIXTIME(?),?,?,?,?,?)")
		args = append(args, k.bucket, k.account, k.channel, k.reasonCode, m[k][0], m[k][1])
	}
	sb.WriteString(" ON DUPLICATE KEY UPDATE rejected_msgs=rejected_msgs+VALUES(rejected_msgs), rejected_parts=rejected_parts+VALUES(rejected_parts)")
	_, err := x.ExecContext(ctx, sb.String(), args...)
	return err
}

func flushOrphans(ctx context.Context, x dbtx, m map[orphanKey]int64) error {
	if len(m) == 0 {
		return nil
	}
	keys := make([]orphanKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
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
	var sb strings.Builder
	sb.WriteString("INSERT INTO summary_orphans (bucket_start, kind, vendor_id, reason, shortcode, orphans) VALUES ")
	args := make([]any, 0, len(keys)*6)
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(FROM_UNIXTIME(?),?,?,?,?,?)")
		args = append(args, k.bucket, k.kind, k.vendor, k.reason, k.shortcode, m[k])
	}
	sb.WriteString(" ON DUPLICATE KEY UPDATE orphans=orphans+VALUES(orphans)")
	_, err := x.ExecContext(ctx, sb.String(), args...)
	return err
}
