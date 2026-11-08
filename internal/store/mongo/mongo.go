package mongo

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"

	"github.com/yakugate/yakugate/internal/model"
)

// Store is the MongoDB backend: per-vendor queue collections + a records collection.
type Store struct{ db *mongo.Database }

// New connects to MongoDB. Database defaults to "yakugate" unless the URI names one.
func New(uri string) (*Store, error) {
	cl, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	if err := cl.Ping(context.Background(), nil); err != nil {
		return nil, err
	}
	// Database name is taken from the URI if present, else defaults to "yakugate".
	name := "yakugate"
	if cs, err := connstring.ParseAndValidate(uri); err == nil && cs.Database != "" {
		name = cs.Database
	}
	return &Store{db: cl.Database(name)}, nil
}

var identRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func (s *Store) queueColl(vendor string) *mongo.Collection {
	return s.db.Collection("queue_" + strings.ToLower(identRe.ReplaceAllString(vendor, "_")))
}

// recordColl is the per-tenant, per-month MDR collection (records_<account>_<YYYYMM>).
func (s *Store) recordColl(accountID int64, ym string) *mongo.Collection {
	return s.db.Collection(fmt.Sprintf("records_%d_%s", accountID, ym))
}
func (s *Store) recordIndex() *mongo.Collection { return s.db.Collection("record_index") }

const leaseSeconds = 60

// Enqueue inserts a pending message document.
func (s *Store) Enqueue(ctx context.Context, m model.Message) error {
	if m.Encoding == "" {
		m.Encoding = "gsm7"
	}
	if m.Status == "" {
		m.Status = model.StatusQueued
	}
	// next_attempt_at nil claims immediately; a future value (client sendAt,
	// [SCOPE P1-SCHEDULE]) delays Claim's own next_attempt_at gate below —
	// same lease field Claim already advances on retry.
	var nextAttempt any
	if !m.NextAttemptAt.IsZero() {
		nextAttempt = m.NextAttemptAt
	}
	_, err := s.queueColl(m.Vendor).InsertOne(ctx, bson.M{
		"msg_id": m.ID, "account_id": m.AccountID, "sender": m.Sender, "destination": m.Destination,
		"body": m.Body, "encoding": m.Encoding, "segments": m.Segments, "template_id": m.TemplateID,
		"transport": string(m.Transport), "mcc": m.MCC, "mnc": m.MNC, "status": string(m.Status),
		"priority": m.Priority, "idempotency_key": m.IdempotencyKey, "request_id": m.RequestID,
		"route_id": m.RouteID, "attempts": 0, "next_attempt_at": nextAttempt, "created_at": time.Now().UTC(),
	})
	return err
}

// Claim leases up to n messages via findOneAndUpdate (atomic per document).
func (s *Store) Claim(ctx context.Context, vendor string, n int) ([]model.Message, error) {
	coll := s.queueColl(vendor)
	now := time.Now().UTC()
	filter := bson.M{
		"status": "queued",
		"$or": bson.A{
			bson.M{"next_attempt_at": nil},
			bson.M{"next_attempt_at": bson.M{"$lte": now}},
		},
	}
	update := bson.M{
		"$set": bson.M{"next_attempt_at": now.Add(leaseSeconds * time.Second)},
		"$inc": bson.M{"attempts": 1},
	}
	opts := options.FindOneAndUpdate().SetSort(bson.D{{Key: "priority", Value: 1}, {Key: "_id", Value: 1}})

	out := make([]model.Message, 0, n)
	for i := 0; i < n; i++ {
		var doc struct {
			MsgID       string `bson:"msg_id"`
			AccountID   int64  `bson:"account_id"`
			Sender      string `bson:"sender"`
			Destination string `bson:"destination"`
			Body        string `bson:"body"`
			Encoding    string `bson:"encoding"`
			Segments    int    `bson:"segments"`
			Transport   string `bson:"transport"`
			MCC         string `bson:"mcc"`
			MNC         string `bson:"mnc"`
			Attempts    int    `bson:"attempts"`
		}
		err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
		if err == mongo.ErrNoDocuments {
			break
		} else if err != nil {
			return out, err
		}
		out = append(out, model.Message{
			ID: doc.MsgID, AccountID: doc.AccountID, Sender: doc.Sender, Destination: doc.Destination,
			Body: doc.Body, Encoding: doc.Encoding, Segments: doc.Segments,
			Transport: model.Transport(doc.Transport), MCC: doc.MCC, MNC: doc.MNC, Attempts: doc.Attempts,
			Vendor: vendor,
		})
	}
	return out, nil
}

// Ack deletes handled messages from the queue.
func (s *Store) Ack(ctx context.Context, vendor string, msgIDs ...string) error {
	if len(msgIDs) == 0 {
		return nil
	}
	_, err := s.queueColl(vendor).DeleteMany(ctx, bson.M{"msg_id": bson.M{"$in": msgIDs}})
	return err
}

// [SCOPE P1-STORE-HOT] Write inserts a record into the per-tenant, per-month collection + record_index.
func (s *Store) Write(ctx context.Context, m model.Message) error {
	if m.Status == "" {
		m.Status = model.StatusQueued
	}
	now := time.Now().UTC()
	ym := now.Format("200601") // UTC month by ruling (PO 2026-09-17), like the MySQL partitions
	if _, err := s.recordColl(m.AccountID, ym).InsertOne(ctx, bson.M{
		"msg_id": m.ID, "account_id": m.AccountID, "username": m.Username, "sender": m.Sender,
		"destination": m.Destination, "mcc": m.MCC, "mnc": m.MNC, "transport": string(m.Transport),
		"segments": m.Segments, "status": string(m.Status), "service_day": now.Format("2006-01-02"),
		"submit_date": now, "created_at": now,
	}); err != nil {
		return err
	}
	_, err := s.recordIndex().UpdateOne(ctx, bson.M{"_id": m.ID}, bson.M{"$set": bson.M{
		"account_id": m.AccountID, "ym": ym, "vendor_id": m.VendorID,
		"status": string(m.Status), "created_at": now,
	}}, options.Update().SetUpsert(true))
	return err
}

// UpdateStatus applies a DLR: find the tenant-month collection via record_index, update it.
func (s *Store) UpdateStatus(ctx context.Context, msgID string, st model.Status, vendorMsgID, errorCode string) error {
	var idx struct {
		AccountID int64  `bson:"account_id"`
		YM        string `bson:"ym"`
	}
	if err := s.recordIndex().FindOne(ctx, bson.M{"_id": msgID}).Decode(&idx); err != nil {
		return err
	}
	if _, err := s.recordColl(idx.AccountID, idx.YM).UpdateOne(ctx, bson.M{"msg_id": msgID}, bson.M{"$set": bson.M{
		"status": string(st), "vendor_msg_id": vendorMsgID, "error_code": errorCode,
		"delivery_date": time.Now().UTC(),
	}}); err != nil {
		return err
	}
	_, err := s.recordIndex().UpdateOne(ctx, bson.M{"_id": msgID}, bson.M{"$set": bson.M{
		"status": string(st), "vendor_msg_id": vendorMsgID,
	}})
	return err
}

// Archive handled by the retention process; no-op here.
func (s *Store) Archive(ctx context.Context, ms []model.Message) error { return nil }
