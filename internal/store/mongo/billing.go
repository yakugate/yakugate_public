package mongo

// Process 9's hot store on MongoDB.
//
// THE DOCUMENT MODEL, and why it is this rather than something more Mongo-ish.
//
// The obvious Mongo design would be one `records` collection with a partition
// field, and rollups computed on demand. This does the opposite: it mirrors
// the relational shape one-for-one — a collection per hot partition, a
// `record_index` locator collection, and a materialised collection per
// summary. Three reasons, in order of weight:
//
//  1. The rollups are READ by the portal and the Admin API, which query
//     summary_mt and friends by name. A backend that computed them on demand
//     would need those readers rewritten too, and then the portal would behave
//     differently depending on which store a deployment ran — the exact
//     failure this parity work exists to remove.
//  2. Retention drops a partition by DROPPING it. A collection per partition
//     keeps that an O(1) operation; a partition field turns it into a mass
//     delete over the largest collection in the system.
//  3. P9's contract is defined in terms of partitions (ListRecordTables,
//     UnratedRecords(table, n)). Honouring it with a different physical shape
//     means translating on every call, and every translation is somewhere the
//     two backends can drift.
//
// So: collection names ARE the table names, documents carry the same field
// names as the columns, and the aggregation pipelines below compute what the
// SQL GROUP BY computes. Where MySQL floors a unix timestamp to a 15-minute
// bucket, this aligns in Go for the same reason MSSQL does — one definition of
// which bucket a message belongs to, or the two stores report differently.
//
// NOT RUN AGAINST A LIVE SERVER. The gateway does not boot on a Mongo config
// store yet, so this is written against the shared contract in
// billingstoretest and reviewed, not exercised. The conformance suite is what
// a live pass should run the day that changes.

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/yakugate/yakugate/internal/billingstore"
)

// BillingStore is P9's view of a Mongo hot store.
type BillingStore struct{ db *mongo.Database }

// NewBilling connects for rating. Unlike the SQL stores this DOES connect —
// the driver's Connect plus a Ping is the only way to know a Mongo URI is
// usable, and a billing worker must know that at start-up rather than on its
// first sweep.
func NewBilling(ctx context.Context, uri string) (*BillingStore, error) {
	s, err := New(uri)
	if err != nil {
		return nil, err
	}
	return &BillingStore{db: s.db}, nil
}

// tsLayout is the one timestamp shape every backend speaks.
const tsLayout = "2006-01-02 15:04:05"

func bucketOf(t time.Time) time.Time { return t.UTC().Truncate(15 * time.Minute) }

func parseTS(s string) (time.Time, error) { return time.Parse(tsLayout, strings.TrimSpace(s)) }

// ─── finding work ───────────────────────────────────────────────────────────

// ListRecordTables enumerates partition COLLECTIONS. Same names, same meaning,
// parsed by the same shared function every backend uses.
func (s *BillingStore) ListRecordTables(ctx context.Context) ([]billingstore.RecordTable, error) {
	names, err := s.db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	var out []billingstore.RecordTable
	for _, n := range names {
		if strings.HasSuffix(n, "_template") {
			continue
		}
		if rt, ok := billingstore.ParseRecordTable(n); ok {
			out = append(out, rt)
		}
	}
	return out, nil
}

// UnratedRecords reads unpriced MT documents.
//
// A missing collection is NOT an error in Mongo — a Find on one returns an
// empty cursor — which is exactly the behaviour the other two backends were
// changed to match, so a partition dropped by retention mid-sweep reads the
// same everywhere.
func (s *BillingStore) UnratedRecords(ctx context.Context, table string, n, maxAttempts int) ([]billingstore.UnratedRecord, error) {
	cur, err := s.db.Collection(table).Find(ctx,
		unratedFilter(maxAttempts, ""),
		// Oldest first, matching the SQL backends' ORDER BY id: without it a
		// steady arrival of new documents can leave older ones unrated
		// indefinitely.
		options.Find().SetLimit(int64(n)).SetSort(bson.D{{Key: "id", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []billingstore.UnratedRecord
	for cur.Next(ctx) {
		var d struct {
			MsgID       string `bson:"msg_id"`
			AccountID   int64  `bson:"account_id"`
			RouteID     int64  `bson:"route_id"`
			Destination string `bson:"destination"`
			MCC         string `bson:"mcc"`
			MNC         string `bson:"mnc"`
			Segments    int    `bson:"segments"`
		}
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		if d.Segments == 0 {
			d.Segments = 1 // the column's COALESCE(...,1), kept identical
		}
		out = append(out, billingstore.UnratedRecord{
			MsgID: d.MsgID, AccountID: d.AccountID, RouteID: d.RouteID,
			Destination: d.Destination, MCC: d.MCC, MNC: d.MNC, Segments: d.Segments,
		})
	}
	return out, cur.Err()
}

// UnratedDLRs reads unpriced receipts, joined to their message and to the
// locator for the client's receipt preference.
//
// $lookup is the join. Two of them, because the SQL does two — a receipt needs
// the MT row for its route and network, and the locator row for whether the
// client was ever shown the receipt at all.
func (s *BillingStore) UnratedDLRs(ctx context.Context, dlrTable string, n, maxAttempts int) ([]billingstore.UnratedDLR, error) {
	mt := "mtlog_" + strings.TrimPrefix(dlrTable, "dlrlog_")
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: unratedFilter(maxAttempts, "")}},
		{{Key: "$sort", Value: bson.D{{Key: "id", Value: 1}}}},
		{{Key: "$limit", Value: int64(n)}},
		{{Key: "$lookup", Value: bson.M{
			"from": mt, "localField": "msg_id", "foreignField": "msg_id", "as": "mtrow"}}},
		{{Key: "$lookup", Value: bson.M{
			"from": "record_index", "localField": "msg_id", "foreignField": "msg_id", "as": "loc"}}},
		{{Key: "$unwind", Value: bson.M{"path": "$mtrow", "preserveNullAndEmptyArrays": false}}},
		{{Key: "$unwind", Value: bson.M{"path": "$loc", "preserveNullAndEmptyArrays": true}}},
	}
	cur, err := s.db.Collection(dlrTable).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []billingstore.UnratedDLR
	for cur.Next(ctx) {
		var d struct {
			ID        int64  `bson:"id"`
			MsgID     string `bson:"msg_id"`
			Status    string `bson:"status"`
			AccountID int64  `bson:"account_id"`
			MTRow     struct {
				RouteID     int64  `bson:"route_id"`
				Destination string `bson:"destination"`
				MCC         string `bson:"mcc"`
				MNC         string `bson:"mnc"`
			} `bson:"mtrow"`
			Loc struct {
				NoReceipt          bool `bson:"no_receipt"`
				FailureReceiptOnly bool `bson:"failure_receipt_only"`
			} `bson:"loc"`
		}
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		out = append(out, billingstore.UnratedDLR{
			ID: d.ID, MsgID: d.MsgID, Status: d.Status, AccountID: d.AccountID,
			RouteID: d.MTRow.RouteID, Destination: d.MTRow.Destination,
			MCC: d.MTRow.MCC, MNC: d.MTRow.MNC,
			NoReceipt: d.Loc.NoReceipt, FailureReceiptOnly: d.Loc.FailureReceiptOnly,
		})
	}
	return out, cur.Err()
}

// ─── pricing ────────────────────────────────────────────────────────────────

func (s *BillingStore) ApplyRating(ctx context.Context, table, msgID string, buy, sell *float64, currency, detailJSON string) error {
	_, err := s.db.Collection(table).UpdateOne(ctx,
		bson.M{"msg_id": msgID}, bson.M{"$set": ratingFields(buy, sell, currency, detailJSON)})
	return err
}

func (s *BillingStore) ApplyRatingDLR(ctx context.Context, table string, id int64, buy, sell *float64, currency, detailJSON string) error {
	_, err := s.db.Collection(table).UpdateOne(ctx,
		bson.M{"id": id}, bson.M{"$set": ratingFields(buy, sell, currency, detailJSON)})
	return err
}

// ratingFields is the price stamp. rated_at is always set, even when there is
// no price: "rated, not chargeable" must still stop the row being examined
// again, or a free message is re-read on every sweep forever.
func ratingFields(buy, sell *float64, currency, detailJSON string) bson.M {
	f := bson.M{"rated_at": time.Now().UTC()}
	if buy != nil {
		f["buy_price"] = *buy
	}
	if sell != nil {
		f["sell_price"] = *sell
	}
	if currency != "" {
		f["currency"] = currency
	}
	if detailJSON != "" {
		f["rate_cards"] = detailJSON
	}
	return f
}

func (s *BillingStore) TouchRecordIndex(ctx context.Context, msgIDs []string) error {
	if len(msgIDs) == 0 {
		return nil
	}
	_, err := s.db.Collection("record_index").UpdateMany(ctx,
		bson.M{"msg_id": bson.M{"$in": msgIDs}},
		bson.M{"$set": bson.M{"updated_at": time.Now().UTC()}})
	return err
}

// ─── coordination ───────────────────────────────────────────────────────────

// DBNow is the SERVER's clock, obtained with hostInfo rather than time.Now():
// every watermark comparison is against this, and a process whose own clock
// differs would skip or re-do work.
func (s *BillingStore) DBNow(ctx context.Context) (string, error) {
	var res struct {
		System struct {
			CurrentTime time.Time `bson:"currentTime"`
		} `bson:"system"`
	}
	if err := s.db.RunCommand(ctx, bson.D{{Key: "hostInfo", Value: 1}}).Decode(&res); err != nil {
		return "", err
	}
	if res.System.CurrentTime.IsZero() {
		return "", errors.New("hostInfo returned no server time")
	}
	return res.System.CurrentTime.UTC().Format(tsLayout), nil
}

func (s *BillingStore) SummaryWatermark(ctx context.Context, name string) (string, bool, error) {
	var d struct {
		Watermark time.Time `bson:"watermark"`
	}
	err := s.db.Collection("summary_state").FindOne(ctx, bson.M{"_id": name}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return d.Watermark.UTC().Format(tsLayout), true, nil
}

func (s *BillingStore) SetSummaryWatermark(ctx context.Context, name, ts string) error {
	t, err := parseTS(ts)
	if err != nil {
		return err
	}
	_, err = s.db.Collection("summary_state").UpdateOne(ctx,
		bson.M{"_id": name},
		bson.M{"$set": bson.M{"watermark": t, "updated_at": time.Now().UTC()}},
		options.Update().SetUpsert(true))
	return err
}

// TryLease is a lock DOCUMENT with a TTL, because Mongo has no session-scoped
// advisory lock like GET_LOCK or sp_getapplock.
//
// The TTL is what makes it safe: a billing process that dies holding the lease
// would otherwise block every other instance forever, with no session for the
// server to notice ending. Renewal is deliberately absent — a sweep is far
// shorter than the TTL, and a lease that outlives its holder by a few minutes
// costs one delayed sweep, where a stuck lease costs all of them.
const leaseTTL = 5 * time.Minute

func (s *BillingStore) TryLease(ctx context.Context, name string) (func(), bool, error) {
	coll := s.db.Collection("billing_leases")
	now := time.Now().UTC()
	// Take it only if nobody holds it, or the holder's lease has expired.
	res, err := coll.UpdateOne(ctx,
		bson.M{"_id": name, "$or": bson.A{
			bson.M{"expires_at": bson.M{"$lt": now}},
			bson.M{"expires_at": bson.M{"$exists": false}},
		}},
		bson.M{"$set": bson.M{"expires_at": now.Add(leaseTTL), "taken_at": now}},
		options.Update().SetUpsert(true))
	if err != nil {
		// A duplicate key here means someone else inserted it between our
		// filter and our upsert — that is "not granted", not a failure.
		if mongo.IsDuplicateKeyError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if res.ModifiedCount == 0 && res.UpsertedCount == 0 {
		return nil, false, nil
	}
	return func() {
		// Release by expiring it, not by deleting: a delete would race with
		// another instance's upsert and could resurrect a stale document.
		_, _ = coll.UpdateOne(context.Background(), bson.M{"_id": name},
			bson.M{"$set": bson.M{"expires_at": time.Now().UTC().Add(-time.Second)}})
	}, true, nil
}

// compile-time proof that this satisfies the contract every backend is held to
var _ billingstore.Store = (*BillingStore)(nil)

// unratedFilter is "no price yet, and not written off".
//
// rating_attempts is absent on a healthy document — nothing writes it until
// something fails — so the cap has to accept missing as well as below-cap, or
// every untouched record would be skipped.
func unratedFilter(maxAttempts int, prefix string) bson.M {
	f := bson.M{prefix + "rated_at": bson.M{"$exists": false}}
	if maxAttempts > 0 {
		f["$or"] = bson.A{
			bson.M{prefix + "rating_attempts": bson.M{"$exists": false}},
			bson.M{prefix + "rating_attempts": bson.M{"$lt": maxAttempts}},
		}
	}
	return f
}

// NoteRatingFailure counts a failure and returns the new total.
// FindOneAndUpdate with ReturnDocument(After) gives the updated value in one
// round trip.
func (s *BillingStore) NoteRatingFailure(ctx context.Context, table, msgID, reason string) (int, error) {
	return s.noteFailure(ctx, table, bson.M{"msg_id": msgID}, reason)
}

func (s *BillingStore) NoteRatingFailureDLR(ctx context.Context, table string, id int64, reason string) (int, error) {
	return s.noteFailure(ctx, table, bson.M{"id": id}, reason)
}

func (s *BillingStore) noteFailure(ctx context.Context, table string, filter bson.M, reason string) (int, error) {
	var doc struct {
		Attempts int `bson:"rating_attempts"`
	}
	err := s.db.Collection(table).FindOneAndUpdate(ctx, filter,
		bson.M{
			"$inc": bson.M{"rating_attempts": 1},
			"$set": bson.M{"rating_error": truncate(reason, 255)},
		},
		options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		// The document went away between the sweep reading it and this — a
		// partition dropped by retention mid-sweep. Nothing to count, and not
		// a failure of its own.
		return 0, nil
	}
	return doc.Attempts, err
}
