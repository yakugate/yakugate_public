package dal

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// Receipts finished in the main Redis (plan of 29 Oct 2026): the facts a
// receipt needs about its message, the receipts' database work waiting to be
// written, and what is parked for another gateway. What a receipt means —
// the facts' encoding, which message a receipt is about, when a status is
// given up — stays in internal/receiptfacts and internal/handoff; what is
// kept and read is here. Every method is the one step it was before the
// move: one command, one pipeline, one transaction or one script.

// FactPut is one message's facts as filed: a value under a key for its life,
// or (Add) a line added to a group's value, its life renewed.
type FactPut struct {
	Key   string
	Value string
	Life  time.Duration
	Add   bool
}

// FactLook is one look at a facts key: Take = read and delete in one call.
type FactLook struct {
	Key  string
	Take bool
}

// ReceiptFacts is the receipt facts: one value a key, under the key the
// vendor's id names (dlr.FactsKey).
type ReceiptFacts interface {
	// File files many facts in ONE round trip.
	File(ctx context.Context, puts []FactPut) error
	// Get reads a key's value; found is false when there is none.
	Get(ctx context.Context, key string) (value string, found bool, err error)
	// Take reads a key's value and deletes it in ONE call: only one caller
	// is given it.
	Take(ctx context.Context, key string) (value string, found bool, err error)
	// Set files a value under a key for life.
	Set(ctx context.Context, key, value string, life time.Duration) error
	// Looks is many Get / Take in ONE round trip, answered in the order
	// asked (found[i] false = nothing there).
	Looks(ctx context.Context, looks []FactLook) (values []string, found []bool, err error)
	// CountPart adds n to a message's part counter and answers the count
	// after; a counter made by this call is given life (a second call).
	CountPart(ctx context.Context, key string, n int64, life time.Duration) (int64, error)
	// Gone says which of the keys are not there, in one round trip.
	Gone(ctx context.Context, keys []string) (map[string]bool, error)
	// DropOldKeys lets go, once for the whole gateway, of what the facts of
	// a build before 29 Oct 2026 kept that nothing reads any more.
	DropOldKeys(ctx context.Context) (int64, error)
}

// ReceiptWrites is one deployment's list of receipts' database work waiting
// to be written (a list a deployment; any process of it may write it out,
// one at a time by a turn). Items are opaque values, oldest first.
type ReceiptWrites interface {
	// Owner is the deployment whose list this is ("" = none named).
	Owner() string
	// Push adds values at the tail of this deployment's list, one call.
	Push(ctx context.Context, values []string) error
	// PushTo adds values to the lists of the deployments named, in ONE
	// transaction, in the order of owners.
	PushTo(ctx context.Context, owners []string, values map[string][]string) error
	// Len is how many wait on this deployment's list.
	Len(ctx context.Context) (int64, error)
	// TakeTurn takes the writer's turn when it is free, or renews it when it
	// is token's: 1 = it is token's now.
	TakeTurn(ctx context.Context, token string, ttl time.Duration) (int, error)
	// DropTurn lets the writer's turn go while it is token's.
	DropTurn(ctx context.Context, token string) error
	// Head reads up to most values at the head.
	Head(ctx context.Context, most int) ([]string, error)
	// Done takes n values off the head and adds back at the tail, while the
	// turn is token's: 1 = done, 0 = the turn was lost.
	Done(ctx context.Context, token string, n int, back []string) (int, error)
}

// Receipts is what OpenReceipts built: the facts and this deployment's
// write-behind list, on one connection (the roles kept them on one client).
type Receipts struct {
	Facts  ReceiptFacts
	Writes ReceiptWrites
}

// HandOffs is the lists receipts and incoming messages are parked on for
// the gateway that owns them (internal/handoff), and the names of the
// gateways' queue stores. kind is "dlr" or "mo".
type HandOffs interface {
	// QueuesHere says this gateway's queues are in Redis (it has a queue
	// store id of its own).
	QueuesHere() bool
	// QueueStoreID gives this gateway's queue store an id if it has none
	// (newID; two gateways on one store read the same) and reads it.
	QueueStoreID(ctx context.Context, newID string) (string, error)
	// NameStore files me's queue store id; UnnameStore takes me's name out.
	NameStore(ctx context.Context, me, id string) error
	UnnameStore(ctx context.Context, me string) error
	// Stores is every deployment's queue store id.
	Stores(ctx context.Context) (map[string]string, error)
	// Park adds items to owner's list of kind, each stamped with the
	// database's own time, all or none.
	Park(ctx context.Context, owner, kind string, items []string) error
	// TakeTurn / DropTurn are the importer's turn on owner's list of kind
	// (1 = token's).
	TakeTurn(ctx context.Context, owner, kind, token string, ttl time.Duration) (int, error)
	DropTurn(ctx context.Context, owner, kind, token string) error
	// Head reads up to most items at the head of owner's list of kind.
	Head(ctx context.Context, owner, kind string, most int) ([]string, error)
	// Now is the database's own clock.
	Now(ctx context.Context) (time.Time, error)
	// Trim takes n items off the head while the turn is token's: 1 = done,
	// 0 = the turn was lost.
	Trim(ctx context.Context, owner, kind, token string, n int) (int, error)
	// TakeStrays / DropStrays are the one-time walk's lock for owner and
	// kind.
	TakeStrays(ctx context.Context, owner, kind, token string, ttl time.Duration) (bool, error)
	DropStrays(ctx context.Context, owner, kind, token string) error
	// Waiting reads, in one round trip, how many items wait on owner's list
	// of kind, the one at its head ("" = none) and the database's clock.
	Waiting(ctx context.Context, owner, kind string) (n int64, head string, now time.Time, err error)
	// FactsGone is ReceiptFacts.Gone on the same database (the main one).
	FactsGone(ctx context.Context, keys []string) (map[string]bool, error)
}

// ReceiptsOn says which DAL keeps the receipt facts, the write-behind lists
// and the hand-off lists: the main Redis whenever one is set; "" when none
// is (receipts are then matched in the record store, as before).
func ReceiptsOn(cfg config.Config) config.Backend {
	if cfg.RedisEnabled() {
		return config.BackendRedis
	}
	return ""
}

func receiptsDriver(cfg config.Config, area string) (Driver, config.Backend, error) {
	on := ReceiptsOn(cfg)
	if on == "" {
		return Driver{}, on, fmt.Errorf("dal: %s: no Redis set: %w", area, ErrNotServed)
	}
	d, ok := Lookup(on)
	if !ok {
		return Driver{}, on, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	return d, on, nil
}

// OpenReceipts opens the receipt facts and the write-behind list of the
// deployment owner, on one new connection to the main Redis.
func OpenReceipts(cfg config.Config, owner string) (*Receipts, error) {
	d, on, err := receiptsDriver(cfg, "receipts")
	if err != nil {
		return nil, err
	}
	if d.Receipts == nil {
		return nil, fmt.Errorf("dal: receipts on %s: %w", on, ErrNotServed)
	}
	r, err := d.Receipts(cfg, owner)
	if err != nil {
		return nil, fmt.Errorf("dal: receipts on %s: %w", on, err)
	}
	return r, nil
}

// OpenHandOffs opens the hand-off lists (main Redis) with this gateway's
// queue Redis when its queues are in Redis. With no Redis set (step 3) the
// lists are the SQL database's: the config store, which every gateway of
// the install sees as they all see the main Redis, with the queue store's
// own id in the queue store.
func OpenHandOffs(cfg config.Config) (HandOffs, error) {
	if ReceiptsOn(cfg) == "" {
		on := SQL(cfg)
		d, ok := Lookup(on)
		if !ok {
			return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
		}
		if d.HandOffs == nil {
			return nil, fmt.Errorf("dal: hand-off on %s: %w", on, ErrNotServed)
		}
		h, err := d.HandOffs(cfg)
		if err != nil {
			return nil, fmt.Errorf("dal: hand-off on %s: %w", on, err)
		}
		return h, nil
	}
	d, on, err := receiptsDriver(cfg, "hand-off")
	if err != nil {
		return nil, err
	}
	if d.HandOffs == nil {
		return nil, fmt.Errorf("dal: hand-off on %s: %w", on, ErrNotServed)
	}
	h, err := d.HandOffs(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: hand-off on %s: %w", on, err)
	}
	return h, nil
}
