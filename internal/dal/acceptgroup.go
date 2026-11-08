package dal

import (
	"context"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// Records at acceptance committed together, without Redis (DAL plan, step 3,
// 30 Oct 2026).
//
// With no DB worker the accept roles write each accepted message's record
// (WriteAccepted: its mt_ row and its accepted counts) while the client
// waits, one transaction a message. Every one of them updates the same
// summary_mt row and holds it until its commit, on one of the role's 16
// pooled connections: on the rig's mix at 150/s the HTTP submits waited 2 s
// on average and one waited past the client's 15 s (answered, recorded,
// never heard). AcceptGroup commits the records of a process together, as
// the balance moves are (BillingEvents.Move): a call is handed to one of
// acceptWriters writers, which writes it with every call waiting behind it
// (at most acceptBatch) in ONE WriteAccepted; a call that comes alone is
// written at once — nobody waits to fill a batch. Every record is still
// written before its caller is answered, and the rows and counts are the
// ones each call alone would write (WriteAccepted leaves a message that is
// there already, so a batch written again changes nothing).

// AcceptGroup is a WriteAccepted that commits a process's calls together.
// Moved from internal/dal/mysql on 30 Oct 2026, unchanged: it needs only the
// store's WriteAccepted, so every SQL DAL's records get it.
type AcceptGroup struct {
	s    AcceptedWriter
	once sync.Once
	in   chan *acceptReq
}

// AcceptedWriter is what AcceptGroup groups: a record store's WriteAccepted.
type AcceptedWriter interface {
	WriteAccepted(ctx context.Context, msgs []model.Message) error
}

// NewAcceptGroup is the group writer over s's WriteAccepted.
func NewAcceptGroup(s AcceptedWriter) *AcceptGroup { return &AcceptGroup{s: s} }

const (
	acceptWriters = 2                // transactions a process has in flight
	acceptBatch   = 256              // calls one write carries at most
	acceptFor     = 10 * time.Second // the most one batch may take
)

type acceptReq struct {
	msgs []model.Message
	done chan error
}

// WriteAccepted writes msgs' records with the calls waiting beside it and
// answers when they are written. The caller waits for its answer even when
// its own context ends: the batch may be writing its record.
func (g *AcceptGroup) WriteAccepted(_ context.Context, msgs []model.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	g.once.Do(func() {
		g.in = make(chan *acceptReq, 4096)
		for i := 0; i < acceptWriters; i++ {
			go g.write()
		}
	})
	r := &acceptReq{msgs: msgs, done: make(chan error, 1)}
	g.in <- r
	return <-r.done
}

// write writes what waits, a batch at a time, until the process ends.
func (g *AcceptGroup) write() {
	for first := range g.in {
		batch := []*acceptReq{first}
	more:
		for len(batch) < acceptBatch {
			select {
			case r := <-g.in:
				batch = append(batch, r)
			default:
				break more
			}
		}
		var all []model.Message
		for _, r := range batch {
			all = append(all, r.msgs...)
		}
		ctx, cancel := context.WithTimeout(context.Background(), acceptFor)
		err := g.s.WriteAccepted(ctx, all)
		if err != nil && len(batch) > 1 {
			// One call's record may be what the store refused: each alone,
			// so only that one fails.
			for _, r := range batch {
				r.done <- g.s.WriteAccepted(ctx, r.msgs)
			}
			cancel()
			continue
		}
		cancel()
		for _, r := range batch {
			r.done <- err
		}
	}
}
