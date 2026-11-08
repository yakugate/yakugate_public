package dal

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// The record queue (plan of 29 Oct 2026): the lists a deployment's roles put
// their record writes on and its DB worker writes out (internal/recordq, D-131).
// The tasks, their lanes and the rules of the worker stay in recordq; what is
// kept and read is here. A lane is a number; the database names its lists
// from it and the deployment (the owner) the lanes were opened for.
//
// Every method is the one step it was before the move: one command, one
// pipeline, one transaction or one script.

// RecordPut is one put of record tasks, as one transaction: the lanes to name
// on the list of lanes (ones this process has not named yet), the tasks by
// lane, and by lane the (message id, what a status pull may be told) pairs.
// Owner is the deployment whose lists they go on. A queue store that takes
// messages off a vendor queue (or puts one on, or files a client's receipt)
// carries a RecordPut in the same transaction (AckWithRecords and its
// siblings on the Redis queue store).
type RecordPut struct {
	Owner   string
	Name    []int
	Tasks   map[int][]string
	Pending map[int][]string
}

// Empty says there is nothing to put.
func (p RecordPut) Empty() bool { return len(p.Tasks) == 0 && len(p.Pending) == 0 }

// RecordLanes is one deployment's record queue.
type RecordLanes interface {
	// Owner is the deployment whose lists these are ("" = none named).
	Owner() string
	// Put queues tasks in ONE transaction: all of them, or none.
	Put(ctx context.Context, p RecordPut) error
	// NamedLanes is every lane anything was ever put on, as written.
	NamedLanes(ctx context.Context) ([]string, error)
	// Look counts, in one round trip, what waits in the lanes given (their
	// tasks and their parked ones) and says whether a DB worker has said it
	// is running.
	Look(ctx context.Context, lanes []int) (waiting int64, alive bool, err error)
	// Heads is the task at the head of each lane given ("" = empty), in one
	// round trip.
	Heads(ctx context.Context, lanes []int) ([]string, error)
	// PendingOf is what each lane given holds for msgID for a status pull
	// ("" = nothing), in one round trip.
	PendingOf(ctx context.Context, lanes []int, msgID string) ([]string, error)
	// HeldIn says whether msgID has a task parked on any of the lanes given,
	// in one round trip.
	HeldIn(ctx context.Context, lanes []int, msgID string) (bool, error)

	// The DB worker's side.

	// Beat says, for ttl, that the worker token runs.
	Beat(ctx context.Context, token string, ttl time.Duration) error
	// Leave takes the sign of life back while it is still token's.
	Leave(ctx context.Context, token string) error
	// TakeTurn takes a lane's turn when it is free, or renews it when it is
	// token's: 1 = it is token's now.
	TakeTurn(ctx context.Context, lane int, token string, ttl time.Duration) (int, error)
	// DropTurn lets a lane's turn go while it is token's.
	DropTurn(ctx context.Context, lane int, token string) error
	// Batch reads, in one round trip, up to most tasks at the lane's head,
	// the messages that have tasks parked (id -> count) and whether the
	// lane's "doing" mark is set.
	Batch(ctx context.Context, lane, most int) (raw []string, held map[string]string, doing bool, err error)
	// MarkDoing sets the lane's "doing" mark.
	MarkDoing(ctx context.Context, lane int, token string) error
	// Done takes n tasks off the lane's head while the turn is token's,
	// parks the (message id, task) pairs given, clears the "doing" mark and
	// the pending entries of the messages written: 1 = done, 0 = the turn was
	// lost.
	Done(ctx context.Context, lane int, token string, n int, park [][2]string, written []string) (int, error)
	// Len is how many tasks wait on the lane.
	Len(ctx context.Context, lane int) (int64, error)
	// Parked reads up to most parked tasks of the lane, from the head.
	Parked(ctx context.Context, lane, most int) ([]string, error)
	// ParkDone takes n parked tasks off the head while the turn is token's,
	// puts back the ones given at the head in their order and counts down
	// the messages gone: 1 = done, 0 = the turn was lost.
	ParkDone(ctx context.Context, lane int, token string, n int, back, gone []string) (int, error)
	// ParkedLen is how many tasks are parked on the lane.
	ParkedLen(ctx context.Context, lane int) (int64, error)

	// AppendOnly is what the database says of its append-only file ("yes",
	// "no"); known is false when it will not say.
	AppendOnly(ctx context.Context) (value string, known bool)
	// Unsent says the database certainly did not run a call that failed
	// with err: no task of it can be queued.
	Unsent(err error) bool
}

// RecordsOn says which DAL keeps the record queue: Redis, the queues' own
// (YAKU_QUEUE_REDIS_*) else the main one, whenever a Redis is set; "" when
// none is (every role then writes its own records).
func RecordsOn(cfg config.Config) config.Backend {
	if cfg.RedisEnabled() {
		return config.BackendRedis
	}
	return ""
}

// OpenRecordLanes opens the record queue of the deployment owner on the DAL
// RecordsOn picks. ErrNotServed when no Redis is set.
func OpenRecordLanes(cfg config.Config, owner string) (RecordLanes, error) {
	on := RecordsOn(cfg)
	if on == "" {
		return nil, fmt.Errorf("dal: record queue: no Redis set: %w", ErrNotServed)
	}
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.RecordLanes == nil {
		return nil, fmt.Errorf("dal: record queue on %s: %w", on, ErrNotServed)
	}
	l, err := d.RecordLanes(cfg, owner)
	if err != nil {
		return nil, fmt.Errorf("dal: record queue on %s: %w", on, err)
	}
	return l, nil
}
