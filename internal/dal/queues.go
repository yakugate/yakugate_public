package dal

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/queues"
	"github.com/yakugate/yakugate/internal/store"
)

// The queue family. These are the methods the MySQL and the Redis queue
// stores already shared, word for word, on 29 Oct 2026 — the roles reached
// them through small interfaces of their own (smppvendor.vendorQueue,
// mo.moQueue, …), which these satisfy. Every DAL that serves Queues gives
// all of them, so a role can be handed any of them.

// VendorQueue is the outgoing queues, one per vendor: messages wait ready or
// delayed, and a sender takes them (issue 22: taken, not leased).
type VendorQueue interface {
	Enqueue(ctx context.Context, m model.Message) error
	Claim(ctx context.Context, vendor string, n int) ([]model.Message, error)
	ClaimSplit(ctx context.Context, vendor string, n int, mode queues.Mode) ([]model.Message, error)
	Ack(ctx context.Context, vendor string, msgIDs ...string) error
	Return(ctx context.Context, ms ...model.Message) error
	// MoveQueued moves a waiting message to another vendor's queue; false =
	// it had already left (sent, taken, expired) and nothing changed.
	MoveQueued(ctx context.Context, from string, m model.Message) (bool, error)
	// PeekMovable looks at what waits for a vendor without taking it.
	PeekMovable(ctx context.Context, vendor string, after int64, n int) ([]model.Message, int64, error)
	FindQueued(ctx context.Context, msgID string, accountID int64, vendors []string) (model.Message, bool, error)
	QueueDepths(ctx context.Context) (map[string]store.VendorQueueStat, error)
	QueuedForVendor(ctx context.Context, vendor string) (int64, error)
	// HandInQueued adds a message moved in from another store unless it is
	// already held; false = it was there.
	HandInQueued(ctx context.Context, m model.Message) (bool, error)
	MaxAttempts(vendor string) int
	SetAttemptCap(global int, perVendor map[string]int)
	SetQueueOwner(owner string)
}

// ClientReceiptQueue is the receipts waiting to be handed to clients (a
// bind or a webhook).
type ClientReceiptQueue interface {
	EnqueueClientDLR(ctx context.Context, d model.ClientDLR) error
	EnqueueClientDLRBatch(ctx context.Context, ds []model.ClientDLR) error
	ClaimClientDLR(ctx context.Context, claims []model.DLRClaim, n int) ([]model.ClientDLR, error)
	ClaimClientDLRStale(ctx context.Context, claims []model.DLRClaim, minAgeSec, n int) ([]model.ClientDLR, error)
	AckClientDLR(ctx context.Context, ids ...int64) error
	RescheduleClientDLR(ctx context.Context, id int64, delay time.Duration) error
	GiveUpClientDLR(ctx context.Context, id int64) error
	SweepGiveUpClientDLR(ctx context.Context, olderThan time.Duration) (int64, error)
}

// MOQueue is the incoming messages waiting to be handed to clients.
type MOQueue interface {
	StageMO(ctx context.Context, m model.Message) error
	ClaimMO(ctx context.Context, claims []model.MOClaim, n int) ([]model.Message, error)
	ClaimMOStale(ctx context.Context, claims []model.MOClaim, minAgeSec, n int) ([]model.Message, error)
	AckMO(ctx context.Context, msgIDs ...string) error
	RescheduleMO(ctx context.Context, msgID string, delay time.Duration) error
	GiveUpMO(ctx context.Context, msgID string) error
	SweepGiveUpMO(ctx context.Context, olderThan time.Duration) (int64, error)
}

// PartIDs is the ids an SMPP client was given for the parts of a long
// message, kept until its receipt goes out.
type PartIDs interface {
	SaveClientPartIDs(ctx context.Context, msgID string, partIDs []string) error
	ClientPartIDs(ctx context.Context, msgID string) ([]string, error)
	ClientPartIDsMany(ctx context.Context, msgIDs []string) (map[string][]string, error)
	DropClientPartIDs(ctx context.Context, msgID string) error
}

// ConcatGroups is the multi-part buffer: the parts of one long message
// gathered before it is sent as one.
type ConcatGroups interface {
	FetchConcatGroup(ctx context.Context, vendor string, accountID int64,
		sender, dest string, ref, total int) ([]model.Message, error)
}

// QueueStats is what Monitoring reads about the client queues.
type QueueStats interface {
	PendingCounts(ctx context.Context) (dlr, mo store.VendorQueueStat, err error)
	PendingByAccount(ctx context.Context, kind string, limit int) (map[int64]store.VendorQueueStat, error)
}

// Queues is the whole family on one database.
type Queues interface {
	VendorQueue
	ClientReceiptQueue
	MOQueue
	PartIDs
	ConcatGroups
	QueueStats
	Close() error
}
