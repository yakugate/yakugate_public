package recordq

import (
	"context"
	"errors"
	"fmt"

	redisstore "github.com/yakugate/yakugate/internal/dal/redis"
	"github.com/yakugate/yakugate/internal/model"
)

// RejectQueue is what queues a refused submit's row (*Client).
type RejectQueue interface {
	WriteRejected(ctx context.Context, m model.Message) error
}

// DirectRejects writes refused submits' rows itself (the record store).
type DirectRejects interface {
	WriteRejectedBatch(ctx context.Context, ms []model.Message) error
}

// RejectsOrDirect files a refused submit's row through the record queue, and
// straight to the record store when the queue certainly did not take it
// (Saleem 2026-10-14: "write to MySQL directly").
//
// With the DB worker on, a refusal's row is a task in Redis. In the drill of
// 2026-10-13 the queue store was stopped for 6 s: about 3 in 10 of the
// refusals made in those seconds had no row, only the role's log line
// "reject log write failed". Now such a row is written as a gateway without
// the worker writes it, by WriteRejectedBatch: the same row, stamped with the
// time in the message id, as the worker's.
//
// Only when the task CANNOT be in Redis (NotQueued): the queue was full, or no
// connection was made. After an unclear error — a timeout once the commands
// may have reached Redis — the worker may still write the row, and a second
// one would count the refusal twice; the task is put again, marked to be
// written once, until Redis takes it (RejectKeeper, rejects_kept.go). It used
// to be left to the first put, and the error said: when that put had not
// reached Redis the refusal had no row (go-live test of 18 Oct 2026, P14 E6:
// 4 of 600).
type RejectsOrDirect struct {
	Queue  RejectQueue
	Direct DirectRejects
}

// WriteRejected implements the accept roles' RejectWriter.
//
// Since 28 Oct 2026 (Saleem: with the DB worker every hot-store row goes
// through the queue; when Redis is away the role "holds it in memory, queues
// it later") a refusal the queue did not take is HELD and put again — also
// when it certainly did not reach Redis. The row is no longer written to
// MySQL by the role (D-135's direct write is retired in worker mode). Direct
// is used only with a queue that cannot hold (none of the gateway's).
func (r RejectsOrDirect) WriteRejected(ctx context.Context, m model.Message) error {
	err := r.Queue.WriteRejected(ctx, m)
	if err == nil {
		return nil
	}
	if k, ok := r.Queue.(RejectKeeper); ok {
		return k.KeepRejected(m, err)
	}
	if !NotQueued(err) || r.Direct == nil {
		return err
	}
	if derr := r.Direct.WriteRejectedBatch(ctx, []model.Message{m}); derr != nil {
		return fmt.Errorf("%w; and straight to the record store: %v", err, derr)
	}
	return nil
}

// RejectBatchQueue is an optional RejectQueue capability: many refusals'
// rows in one Redis transaction (Client.WriteRejectedBatch).
type RejectBatchQueue interface {
	WriteRejectedBatch(ctx context.Context, ms []model.Message) error
}

// RejectBatchKeeper is an optional RejectQueue capability: a batch whose put
// failed or did not say is held and put again, marked Once
// (Client.KeepRejectedBatch).
type RejectBatchKeeper interface {
	KeepRejectedBatch(ms []model.Message, cause error) error
}

// WriteRejectedBatch is WriteRejected for many refusals: the reject log's
// batches (rejectlog.BatchWriter). With a queue that takes them at once they
// go to Redis in ONE transaction — all of them or none — and a batch it did
// not take is held whole, as a single refusal is. Before 30 Oct 2026 the
// reject log wrote one row a round trip here: two writers kept up with a few
// thousand a second, and the rest of a heavy run's refusals were dropped
// (load test of that day, finding 3).
func (r RejectsOrDirect) WriteRejectedBatch(ctx context.Context, ms []model.Message) error {
	if len(ms) == 0 {
		return nil
	}
	bq, ok := r.Queue.(RejectBatchQueue)
	if !ok {
		for _, m := range ms {
			if err := r.WriteRejected(ctx, m); err != nil {
				return err
			}
		}
		return nil
	}
	err := bq.WriteRejectedBatch(ctx, ms)
	if err == nil {
		return nil
	}
	if k, ok := r.Queue.(RejectBatchKeeper); ok {
		return k.KeepRejectedBatch(ms, err)
	}
	if !NotQueued(err) || r.Direct == nil {
		return err
	}
	if derr := r.Direct.WriteRejectedBatch(ctx, ms); derr != nil {
		return fmt.Errorf("%w; and straight to the record store: %v", err, derr)
	}
	return nil
}

// NotQueued reports whether err leaves no task in Redis: the queue refused
// before trying (ErrFull), Redis could not be reached at all (no connection
// made, a closed client, no free connection in the pool), or Redis answered
// and refused the transaction whole — EXECABORT (the transaction was
// discarded), LOADING (a Redis started again, still reading its file: the
// drill of 2026-10-14 met 108 of these in the second after the restart),
// READONLY (a replica).
//
// What Redis's errors mean is the Redis DAL's to say (redisstore.NotQueued).
func NotQueued(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrFull) {
		return true
	}
	return redisstore.NotQueued(err)
}
