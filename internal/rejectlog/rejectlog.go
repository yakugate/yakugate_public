// Package rejectlog writes refused-submit records without letting a storm of
// refusals take the database from the traffic path.
//
// Each refusal used to start its own goroutine and its own INSERT. An account
// held to 10/s and still sending 2000/s (production, 2026-10-06) therefore
// asked for two thousand hot-store connections a second, for records nobody
// was waiting on — it kept a 50-connection pool full on its own. Here the
// records go through a bounded queue to a FEW fixed writers: at most that many
// connections are ever used for it, and when the queue is full the record is
// dropped and counted. The client already has its answer; losing a reject row
// under pressure is strictly better than slowing an accept.
//
// Every writer of the gateway takes many records at once (BatchWriter): the
// record store one statement a table, the record queue one Redis transaction
// a batch. Until 30 Oct 2026 the record queue took one row a round trip, and
// two writers kept up with a few thousand a second: the heavy load test runs
// of that day kept 53,130 of 241,236 refusals ("Load test refusals",
// finding 3), the rest dropped here and not shown on smpp-server's status.
package rejectlog

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/yakugate/yakugate/internal/model"
)

// Writer is what stores one refused submit (the hot store's WriteRejected).
type Writer interface {
	WriteRejected(ctx context.Context, m model.Message) error
}

const (
	// Workers is how many writes run at once — and so how many
	// connections reject logging can hold.
	Workers = 2
	// Depth is how many records may wait for a writer: 50,000 refusals in
	// 10 s fit while the writers are held up for seconds. It was 4,096. A
	// record waits as a pointer, so the room itself costs half a megabyte.
	Depth = 65536
	// WriteTimeout bounds one write, so a sick database cannot hold a writer.
	WriteTimeout = 3 * time.Second
)

// RefusalNoter is told of every refusal handed to the queue (the abuse
// monitor's counters, abusecount.Counter): the account, the bare reason
// code, the message's parts. It must never block.
type RefusalNoter interface {
	NoteRefusal(accountID int64, code string, parts int, at time.Time)
}

// Queue is a bounded queue of reject records in front of a Writer.
type Queue struct {
	w       Writer
	noter   RefusalNoter
	ch      chan *model.Message
	onErr   func(m model.Message, err error)
	written atomic.Int64
	dropped atomic.Int64
	failed  atomic.Int64
	// retrying: records in a batch the store refused, being tried again.
	retrying atomic.Int64
	once     sync.Once
}

// New makes a queue; its writers start with the first record. onErr (may be
// nil) is told of a write that failed.
func New(w Writer, onErr func(m model.Message, err error)) *Queue {
	return &Queue{w: w, ch: make(chan *model.Message, Depth), onErr: onErr}
}

// Count has every refusal put on the queue told to n as well. Set it before
// the first Put.
func (q *Queue) Count(n RefusalNoter) *Queue {
	if q != nil {
		q.noter = n
	}
	return q
}

// Put hands a record to the writers, or drops it when the queue is full.
// It never blocks. A nil queue or writer takes nothing.
func (q *Queue) Put(m model.Message) {
	if q == nil {
		return
	}
	// A refusal that never became a message (a request we could not read, a
	// login that failed, a full SMPP window) gets an id made now: the row is
	// dated by it, not by when a writer gets to it, and a writer that writes
	// it a second time can tell it is there (WriteRejectedBatchOnce).
	if m.ID == "" {
		m.ID = ulid.Make().String()
	}
	if q.noter != nil && m.AccountID > 0 {
		// Counted whether or not the row is then written: the refusal happened.
		code, _ := model.SplitReason(m.RejectReason)
		q.noter.NoteRefusal(m.AccountID, code, m.Segments, time.Now())
	}
	if q.w == nil {
		return
	}
	q.once.Do(func() {
		for i := 0; i < Workers; i++ {
			go q.run()
		}
	})
	select {
	case q.ch <- &m:
	default:
		q.dropped.Add(1)
	}
}

func (q *Queue) run() {
	if bw, ok := q.w.(BatchWriter); ok {
		q.runBatches(bw)
		return
	}
	for m := range q.ch {
		ctx, cancel := context.WithTimeout(context.Background(), WriteTimeout)
		err := q.w.WriteRejected(ctx, *m)
		cancel()
		if err != nil {
			q.failed.Add(1)
			if q.onErr != nil {
				q.onErr(*m, err)
			}
			continue
		}
		q.written.Add(1)
	}
}

// BatchWriter is an optional Writer capability: many refusals in one write
// (mysql.Store.WriteRejectedBatch — one statement a table, and ONE update of
// each summary row, in one transaction; the SQL Server store the same;
// recordq.RejectsOrDirect — one Redis transaction). The store must leave out a refusal
// whose id is in its table already, so a batch written again after a write
// that did land (its answer lost to the time limit) adds nothing twice.
type BatchWriter interface {
	WriteRejectedBatch(ctx context.Context, ms []model.Message) error
}

const (
	// batchMost is the most refusals one batch write carries.
	batchMost = 500
	// batchTimeout bounds one batch write.
	batchTimeout = 10 * time.Second
	// keepFor is how long a batch the store refused is tried again before
	// its refusals are given up (each said to onErr, counted as failed).
	keepFor = 2 * time.Minute
	// againEvery is the pause between two tries of a refused batch.
	againEvery = 500 * time.Millisecond
)

// runBatches is run for a writer that takes many at once (DAL plan, step 3,
// 30 Oct 2026). Without Redis every refusal was its own transaction on a hot
// store whose summary rows every writer of the gateway waits on; under load a
// write ran past WriteTimeout and its row was lost — 140 of 2,200 WINDOW_FULL
// refusals a run of the rig's mix ("every refusal is logged in rejected",
// Saleem 2026-10-14). Now a writer takes what is waiting (up to batchMost)
// and writes it in one call, and a batch the store refuses is KEPT and tried
// again until keepFor has passed; meanwhile new refusals wait in the queue
// (and only past Depth are dropped and counted, as before).
func (q *Queue) runBatches(bw BatchWriter) {
	for first := range q.ch {
		// A new slice each batch: a writer may keep what it was given (the
		// record queue's client holds a batch Redis did not take).
		batch := make([]model.Message, 1, min(batchMost, 1+len(q.ch)))
		batch[0] = *first
	more:
		for len(batch) < batchMost {
			select {
			case m, ok := <-q.ch:
				if !ok {
					break more
				}
				batch = append(batch, *m)
			default:
				break more
			}
		}
		since := time.Now()
		for tries := 0; ; tries++ {
			ctx, cancel := context.WithTimeout(context.Background(), batchTimeout)
			err := bw.WriteRejectedBatch(ctx, batch)
			cancel()
			if err == nil {
				if tries > 0 {
					q.retrying.Add(-int64(len(batch)))
				}
				q.written.Add(int64(len(batch)))
				break
			}
			if tries == 0 {
				q.retrying.Add(int64(len(batch)))
			}
			if time.Since(since) < keepFor {
				time.Sleep(againEvery)
				continue
			}
			q.retrying.Add(-int64(len(batch)))
			q.failed.Add(int64(len(batch)))
			if q.onErr != nil {
				for _, m := range batch {
					q.onErr(m, err)
				}
			}
			break
		}
	}
}

// Stats is the queue for monitoring: how many records wait, how many are in
// a batch the store refused that is being tried again, and since the process
// started how many were written (with the DB worker: queued in Redis, or held
// by the record queue's client to be put again), dropped for want of room, or
// failed (given up after keepFor). Dropped or failed above 0 is a loss.
type Stats struct {
	Waiting  int   `json:"waiting"`
	Retrying int64 `json:"retrying"`
	Written  int64 `json:"written"`
	Dropped  int64 `json:"dropped"`
	Failed   int64 `json:"failed"`
}

// Stats reads the counters; a nil queue reads as zeros.
func (q *Queue) Stats() Stats {
	if q == nil {
		return Stats{}
	}
	return Stats{Waiting: len(q.ch), Retrying: q.retrying.Load(), Written: q.written.Load(),
		Dropped: q.dropped.Load(), Failed: q.failed.Load()}
}

// AddTo puts the queue's counters on a role's status sections as
// "reject_log". A nil queue (no refusal yet, or reject logging off) adds
// nothing.
func (q *Queue) AddTo(sections map[string]any) map[string]any {
	if q == nil {
		return sections
	}
	if sections == nil {
		sections = map[string]any{}
	}
	sections["reject_log"] = q.Stats()
	return sections
}
