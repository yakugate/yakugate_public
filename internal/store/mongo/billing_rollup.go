package mongo

// P9's rollup half on MongoDB.
//
// Every SQL GROUP BY here becomes an aggregation, but the GROUPING KEY is
// still computed in Go. That is the same decision the MSSQL store makes and
// for the same reason: MySQL floors a unix timestamp to a 15-minute boundary,
// and expressing "the same boundary" in a $dateTrunc would put a message in a
// different bucket around DST and pre-epoch dates. One definition, in one
// language, or the two stores report the same traffic differently.
//
// So the pipelines below fetch the rows that matter and fold them in Go. That
// is slower than letting the server aggregate, and it is the right trade here:
// P9 is an off-hot-path worker on a timer, and correctness across backends
// matters more than the sweep finishing a few hundred milliseconds sooner.
//
// NOT RUN AGAINST A LIVE SERVER — see billing.go's note.

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/yakugate/yakugate/internal/billingstore"
)

func (s *BillingStore) DirtySummaryTables(ctx context.Context, since string) ([]string, error) {
	t, err := parseTS(since)
	if err != nil {
		return nil, err
	}
	vals, err := s.db.Collection("record_index").Distinct(ctx, "rec_table",
		bson.M{"updated_at": bson.M{"$gte": t}})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, v := range vals {
		if name, ok := v.(string); ok && name != "" {
			out = append(out, name)
		}
	}
	return out, nil
}

// DirtySummaryBuckets narrows a dirty partition to the buckets that moved.
//
// A receipt arriving days late dirties the message's ORIGINAL submit bucket:
// the bucket comes from accepted_at, the dirt marker from the status clock.
func (s *BillingStore) DirtySummaryBuckets(ctx context.Context, table, since string) ([]billingstore.SummaryBucket, error) {
	ts, err := parseTS(since)
	if err != nil {
		return nil, err
	}
	timeField := "accepted_at"
	if strings.HasPrefix(table, "mo_log_") {
		timeField = "received_at"
	}
	cur, err := s.db.Collection("record_index").Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"updated_at": bson.M{"$gte": ts}, "rec_table": table}}},
		{{Key: "$lookup", Value: bson.M{
			"from": table, "localField": "msg_id", "foreignField": "msg_id", "as": "row"}}},
		{{Key: "$unwind", Value: "$row"}},
		{{Key: "$project", Value: bson.M{
			"at":      bson.M{"$ifNull": bson.A{"$row." + timeField, "$row.created_at"}},
			"account": bson.M{"$ifNull": bson.A{"$row.account_id", int64(0)}},
		}}},
	})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	seen := map[billingstore.SummaryBucket]bool{}
	var out []billingstore.SummaryBucket
	for cur.Next(ctx) {
		var d struct {
			At      time.Time `bson:"at"`
			Account int64     `bson:"account"`
		}
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		b := billingstore.SummaryBucket{Bucket: bucketOf(d.At).Format(tsLayout), AccountID: d.Account}
		if !seen[b] {
			seen[b] = true
			out = append(out, b)
		}
	}
	return out, cur.Err()
}

// mtRow is what the MT rollup needs from one message document.
type mtRow struct {
	At        time.Time `bson:"at"`
	Account   int64     `bson:"account_id"`
	Vendor    int64     `bson:"vendor_id"`
	MCC       string    `bson:"mcc"`
	MNC       string    `bson:"mnc"`
	Sender    string    `bson:"sender"`
	Segments  int64     `bson:"segments"`
	Status    string    `bson:"status"`
	AckAt     time.Time `bson:"vendor_ack_at"`
	RateCards string    `bson:"rate_cards"`
}

func (s *BillingStore) ReconcileSummaryMT(ctx context.Context, table string) error {
	return s.rebuildMT(ctx, table, nil, "")
}

func (s *BillingStore) ReconcileSummaryMTBucket(ctx context.Context, table string, accountID int64, bucket string) error {
	return s.rebuildMT(ctx, table, &accountID, bucket)
}

func (s *BillingStore) rebuildMT(ctx context.Context, table string, accountID *int64, bucket string) error {
	match := bson.M{}
	var bucketStart time.Time
	if accountID != nil {
		var err error
		if bucketStart, err = parseTS(bucket); err != nil {
			return err
		}
		match["account_id"] = *accountID
		// The half-open bucket window, matching the SQL BETWEEN the other two
		// backends use.
		match["$or"] = bson.A{
			bson.M{"accepted_at": bson.M{"$gte": bucketStart, "$lt": bucketStart.Add(15 * time.Minute)}},
			bson.M{"accepted_at": bson.M{"$exists": false},
				"created_at": bson.M{"$gte": bucketStart, "$lt": bucketStart.Add(15 * time.Minute)}},
		}
		if _, err := s.db.Collection("summary_mt").DeleteMany(ctx,
			bson.M{"account_id": *accountID, "bucket_start": bucketStart}); err != nil {
			return err
		}
	}

	cur, err := s.db.Collection(table).Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$lookup", Value: bson.M{
			"from": "record_index", "localField": "msg_id", "foreignField": "msg_id", "as": "loc"}}},
		{{Key: "$unwind", Value: bson.M{"path": "$loc", "preserveNullAndEmptyArrays": true}}},
		{{Key: "$project", Value: bson.M{
			"at":            bson.M{"$ifNull": bson.A{"$accepted_at", "$created_at"}},
			"account_id":    bson.M{"$ifNull": bson.A{"$account_id", int64(0)}},
			"vendor_id":     bson.M{"$ifNull": bson.A{"$vendor_id", int64(0)}},
			"mcc":           bson.M{"$ifNull": bson.A{"$mcc", ""}},
			"mnc":           bson.M{"$ifNull": bson.A{"$mnc", ""}},
			"sender":        bson.M{"$ifNull": bson.A{"$sender", ""}},
			"segments":      bson.M{"$ifNull": bson.A{"$segments", int64(1)}},
			"status":        bson.M{"$ifNull": bson.A{"$loc.status", ""}},
			"vendor_ack_at": 1,
			"rate_cards":    bson.M{"$ifNull": bson.A{"$rate_cards", ""}},
		}}},
	})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	type key struct {
		bucket            time.Time
		account, vendor   int64
		mcc, mnc, sender  string
		sellCard, buyCard int64
	}
	type agg struct {
		submitted, delivered, undeliv, expired, vendorAccepted int64
		sumAckSec, maxAckSec                                   int64
	}
	sums := map[key]*agg{}
	for cur.Next(ctx) {
		var r mtRow
		if err := cur.Decode(&r); err != nil {
			return err
		}
		if r.Segments == 0 {
			r.Segments = 1
		}
		sell, buy := rateCardIDs(r.RateCards)
		// Sender is truncated to 20 to match the column width the other two
		// backends store, so the same traffic groups identically everywhere.
		k := key{bucketOf(r.At), r.Account, r.Vendor, r.MCC, r.MNC, truncate(r.Sender, 20), sell, buy}
		a := sums[k]
		if a == nil {
			a = &agg{}
			sums[k] = a
		}
		a.submitted += r.Segments
		switch r.Status {
		case "delivered":
			a.delivered += r.Segments
		case "failed":
			a.undeliv += r.Segments
		case "expired":
			a.expired += r.Segments
		}
		if !r.AckAt.IsZero() {
			a.vendorAccepted += r.Segments
			secs := int64(r.AckAt.Sub(r.At).Seconds())
			if secs < 0 {
				secs = 0 // GREATEST(...,0): a vendor clock ahead of ours is not negative latency
			}
			a.sumAckSec += secs * r.Segments
			if secs > a.maxAckSec {
				a.maxAckSec = secs
			}
		}
	}
	if err := cur.Err(); err != nil {
		return err
	}

	coll := s.db.Collection("summary_mt")
	for k, a := range sums {
		filter := bson.M{
			"bucket_start": k.bucket, "account_id": k.account, "vendor_id": k.vendor,
			"mcc": k.mcc, "mnc": k.mnc, "sender_id": k.sender,
			"sell_card_id": k.sellCard, "buy_card_id": k.buyCard,
		}
		set := bson.M{
			"submitted_parts": a.submitted, "delivered_parts": a.delivered,
			"undeliv_parts": a.undeliv, "expired_parts": a.expired,
			"vendor_accepted_parts": a.vendorAccepted,
			"sum_vendor_ack_sec":    a.sumAckSec, "max_vendor_ack_sec": a.maxAckSec,
			"updated_at": time.Now().UTC(),
		}
		if _, err := coll.UpdateOne(ctx, filter, bson.M{"$set": set}, options.Update().SetUpsert(true)); err != nil {
			return err
		}
	}
	return nil
}

func (s *BillingStore) ReconcileSummaryMOBucket(ctx context.Context, table string, accountID int64, bucket string) error {
	b, err := parseTS(bucket)
	if err != nil {
		return err
	}
	if _, err := s.db.Collection("summary_mo").DeleteMany(ctx,
		bson.M{"account_id": accountID, "bucket_start": b}); err != nil {
		return err
	}
	cur, err := s.db.Collection(table).Find(ctx, bson.M{
		"account_id":  accountID,
		"received_at": bson.M{"$gte": b, "$lt": b.Add(15 * time.Minute)},
	})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	type key struct {
		bucket           time.Time
		vendor           int64
		mcc, mnc, sender string
	}
	type agg struct{ received, delivered, failed, retry int64 }
	sums := map[key]*agg{}
	for cur.Next(ctx) {
		var d struct {
			ReceivedAt   time.Time `bson:"received_at"`
			CreatedAt    time.Time `bson:"created_at"`
			VendorID     int64     `bson:"vendor_id"`
			MCC          string    `bson:"mcc"`
			MNC          string    `bson:"mnc"`
			Sender       string    `bson:"sender"`
			Segments     int64     `bson:"segments"`
			ClientStatus string    `bson:"client_status"`
		}
		if err := cur.Decode(&d); err != nil {
			return err
		}
		at := d.ReceivedAt
		if at.IsZero() {
			at = d.CreatedAt
		}
		if d.Segments == 0 {
			d.Segments = 1
		}
		k := key{bucketOf(at), d.VendorID, d.MCC, d.MNC, truncate(d.Sender, 20)}
		a := sums[k]
		if a == nil {
			a = &agg{}
			sums[k] = a
		}
		a.received += d.Segments
		switch d.ClientStatus {
		case "delivered":
			a.delivered += d.Segments
		case "failed":
			a.failed += d.Segments
		case "retry":
			a.retry += d.Segments
		}
	}
	if err := cur.Err(); err != nil {
		return err
	}
	for k, a := range sums {
		if _, err := s.db.Collection("summary_mo").InsertOne(ctx, bson.M{
			"bucket_start": k.bucket, "account_id": accountID, "vendor_id": k.vendor,
			"mcc": k.mcc, "mnc": k.mnc, "sender_id": k.sender,
			"sell_card_id": int64(0), "buy_card_id": int64(0),
			"received_parts": a.received, "client_delivered_parts": a.delivered,
			"client_failed_parts": a.failed, "client_retry_parts": a.retry,
			"updated_at": time.Now().UTC(),
		}); err != nil {
			return err
		}
	}
	return nil
}

// ReconcileSummaryLookupBucket clears the bucket: HLR/CRP lookup logging has
// no source collection on this backend, and showing stale numbers would be
// worse than showing none while inventing them would be worse still.
func (s *BillingStore) ReconcileSummaryLookupBucket(ctx context.Context, table string, accountID int64, bucket string) error {
	b, err := parseTS(bucket)
	if err != nil {
		return err
	}
	_, err = s.db.Collection("summary_lookup").DeleteMany(ctx,
		bson.M{"account_id": accountID, "bucket_start": b})
	return err
}

func (s *BillingStore) ReconcileSummaryDlrFlow(ctx context.Context, mtTable, since string) error {
	ts, err := parseTS(since)
	if err != nil {
		return err
	}
	dlr := "dlrlog_" + strings.TrimPrefix(mtTable, "mtlog_")
	cur, err := s.db.Collection(dlr).Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"created_at": bson.M{"$gte": ts}}}},
		{{Key: "$lookup", Value: bson.M{
			"from": mtTable, "localField": "msg_id", "foreignField": "msg_id", "as": "mtrow"}}},
		{{Key: "$unwind", Value: bson.M{"path": "$mtrow", "preserveNullAndEmptyArrays": true}}},
	})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	type key struct {
		bucket time.Time
		vendor int64
		sender string
	}
	type agg struct {
		received, delivered, undeliv, expired, rejected int64
		sumLag, maxLag, negLag, noDone                  int64
	}
	sums := map[key]*agg{}
	for cur.Next(ctx) {
		var d struct {
			CreatedAt time.Time `bson:"created_at"`
			DoneDate  time.Time `bson:"done_date"`
			VendorID  int64     `bson:"vendor_id"`
			Status    string    `bson:"status"`
			MTRow     struct {
				Sender string `bson:"sender"`
			} `bson:"mtrow"`
		}
		if err := cur.Decode(&d); err != nil {
			return err
		}
		k := key{bucketOf(d.CreatedAt), d.VendorID, truncate(d.MTRow.Sender, 20)}
		a := sums[k]
		if a == nil {
			a = &agg{}
			sums[k] = a
		}
		a.received++
		switch strings.ToLower(d.Status) {
		case "delivered":
			a.delivered++
		case "failed", "undeliv":
			a.undeliv++
		case "expired":
			a.expired++
		case "rejected":
			a.rejected++
		}
		if d.DoneDate.IsZero() {
			a.noDone++
			continue
		}
		lag := int64(d.CreatedAt.Sub(d.DoneDate).Seconds())
		if lag < 0 {
			// The vendor's clock is ahead of ours. Counted, never averaged in:
			// a negative would drag the mean into nonsense.
			a.negLag++
			continue
		}
		a.sumLag += lag
		if lag > a.maxLag {
			a.maxLag = lag
		}
	}
	if err := cur.Err(); err != nil {
		return err
	}
	coll := s.db.Collection("summary_dlr_flow")
	for k, a := range sums {
		if _, err := coll.UpdateOne(ctx,
			bson.M{"bucket_start": k.bucket, "vendor_id": k.vendor, "sender_id": k.sender},
			bson.M{"$set": bson.M{
				"received_parts": a.received, "delivered_parts": a.delivered,
				"undeliv_parts": a.undeliv, "expired_parts": a.expired, "rejected_parts": a.rejected,
				"sum_forward_lag_sec": a.sumLag, "max_forward_lag_sec": a.maxLag,
				"neg_lag_parts": a.negLag, "no_done_date_parts": a.noDone,
				"updated_at": time.Now().UTC(),
			}}, options.Update().SetUpsert(true)); err != nil {
			return err
		}
	}
	return nil
}

func (s *BillingStore) ReconcileSummaryErrDaily(ctx context.Context, mtTable, since string) error {
	ts, err := parseTS(since)
	if err != nil {
		return err
	}
	dlr := "dlrlog_" + strings.TrimPrefix(mtTable, "mtlog_")
	cur, err := s.db.Collection(dlr).Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"created_at": bson.M{"$gte": ts}}}},
		{{Key: "$lookup", Value: bson.M{
			"from": mtTable, "localField": "msg_id", "foreignField": "msg_id", "as": "mtrow"}}},
		{{Key: "$unwind", Value: bson.M{"path": "$mtrow", "preserveNullAndEmptyArrays": true}}},
	})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	type key struct {
		day      string
		vendor   int64
		mcc, mnc string
		errCode  string
	}
	counts := map[key]int64{}
	for cur.Next(ctx) {
		var d struct {
			CreatedAt time.Time `bson:"created_at"`
			VendorID  int64     `bson:"vendor_id"`
			ErrorCode string    `bson:"error_code"`
			MTRow     struct {
				MCC string `bson:"mcc"`
				MNC string `bson:"mnc"`
			} `bson:"mtrow"`
		}
		if err := cur.Decode(&d); err != nil {
			return err
		}
		counts[key{d.CreatedAt.UTC().Format("2006-01-02"), d.VendorID,
			d.MTRow.MCC, d.MTRow.MNC, d.ErrorCode}]++
	}
	if err := cur.Err(); err != nil {
		return err
	}
	coll := s.db.Collection("summary_err_daily")
	for k, n := range counts {
		day, _ := time.Parse("2006-01-02", k.day)
		if _, err := coll.UpdateOne(ctx,
			bson.M{"day": day, "service": "mt", "vendor_id": k.vendor,
				"mcc": k.mcc, "mnc": k.mnc, "err_code": k.errCode},
			bson.M{"$set": bson.M{"parts": n}},
			options.Update().SetUpsert(true)); err != nil {
			return err
		}
	}
	return nil
}

// ReconcileSummaryLive rebuilds the rolling per-minute view. Deleted and
// rebuilt rather than updated: it is a moving window, so a minute that fell
// out of it has to disappear.
func (s *BillingStore) ReconcileSummaryLive(ctx context.Context, todayTables []string, windowMin int) error {
	if windowMin <= 0 {
		windowMin = 30
	}
	cutoff := time.Now().UTC().Add(-time.Duration(windowMin) * time.Minute)
	if _, err := s.db.Collection("summary_live").DeleteMany(ctx,
		bson.M{"service": "mt", "minute_start": bson.M{"$gte": cutoff}}); err != nil {
		return err
	}
	type key struct {
		minute time.Time
		vendor int64
	}
	type agg struct{ in, ok, failed int64 }
	sums := map[key]*agg{}
	for _, table := range todayTables {
		cur, err := s.db.Collection(table).Aggregate(ctx, mongo.Pipeline{
			{{Key: "$match", Value: bson.M{"accepted_at": bson.M{"$gte": cutoff}}}},
			{{Key: "$lookup", Value: bson.M{
				"from": "record_index", "localField": "msg_id", "foreignField": "msg_id", "as": "loc"}}},
			{{Key: "$unwind", Value: bson.M{"path": "$loc", "preserveNullAndEmptyArrays": true}}},
		})
		if err != nil {
			return err
		}
		for cur.Next(ctx) {
			var d struct {
				AcceptedAt time.Time `bson:"accepted_at"`
				VendorID   int64     `bson:"vendor_id"`
				Segments   int64     `bson:"segments"`
				Loc        struct {
					Status string `bson:"status"`
				} `bson:"loc"`
			}
			if err := cur.Decode(&d); err != nil {
				cur.Close(ctx)
				return err
			}
			if d.Segments == 0 {
				d.Segments = 1
			}
			k := key{d.AcceptedAt.UTC().Truncate(time.Minute), d.VendorID}
			a := sums[k]
			if a == nil {
				a = &agg{}
				sums[k] = a
			}
			a.in += d.Segments
			switch d.Loc.Status {
			case "delivered":
				a.ok += d.Segments
			case "failed", "expired":
				a.failed += d.Segments
			}
		}
		err = cur.Err()
		cur.Close(ctx)
		if err != nil {
			return err
		}
	}
	for k, a := range sums {
		if _, err := s.db.Collection("summary_live").InsertOne(ctx, bson.M{
			"minute_start": k.minute, "service": "mt", "vendor_id": k.vendor,
			"in_parts": a.in, "ok_parts": a.ok, "failed_parts": a.failed,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *BillingStore) ClearAllSummaries(ctx context.Context) error {
	for _, c := range []string{"summary_mt", "summary_mo", "summary_dlr_flow", "summary_err_daily", "summary_lookup"} {
		if _, err := s.db.Collection(c).DeleteMany(ctx, bson.M{}); err != nil {
			return err
		}
	}
	return nil
}

// rateCardIDs pulls the sell/buy rate-card ids out of the stored rate_cards
// JSON. The SQL backends use JSON_EXTRACT / JSON_VALUE on the same document;
// here it is stored as a string for the same reason — the value is written by
// the rating code as JSON and read back by three different stores, so it stays
// one representation.
func rateCardIDs(raw string) (sell, buy int64) {
	if raw == "" {
		return 0, 0
	}
	var doc struct {
		Sell struct {
			RateID int64 `json:"rate_id"`
		} `json:"sell"`
		Buy struct {
			RateID int64 `json:"rate_id"`
		} `json:"buy"`
	}
	if err := jsonUnmarshal([]byte(raw), &doc); err != nil {
		return 0, 0
	}
	return doc.Sell.RateID, doc.Buy.RateID
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// jsonUnmarshal is named rather than used inline so the rate_cards decode
// reads the same as the SQL backends JSON_VALUE calls.
func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
