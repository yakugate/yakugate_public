// Package recordq is the queue between the roles that carry traffic and the
// ONE role that writes their records to MySQL.
//
// Saleem's design of 2026-10-13 (D-131, stage 2): "a db worker process that
// does the insertions and updates for MO, MT and DLR … every other process
// that needs to perform those tasks just queues the task." Until then each
// role wrote its own records: the SMPP vendor role in batches, the HTTP
// vendor role three statements a message, incoming messages, rejects, retry
// and expiry one row at a time — and a slow MySQL slowed sending itself.
//
// With Redis set (and YAKU_RECORDS_VIA not direct) a role hands its record writes to a Client,
// which puts one Task per record on a Redis list; the DB worker (Run)
// reads the lists and writes to MySQL in batches. With the setting unset, or
// without Redis, a role calls the record store itself, exactly as before.
//
// Lanes. There are YAKU_RECORD_LANES lists (4). A message's tasks always go
// to the same one (a hash of its id), so they are written in the order they
// were queued; the lanes are written side by side. One process at a time
// writes a lane (a turn, as receiptfacts.Queue has): a second DB worker
// stands by and takes over when the first stops.
//
// What a role may rely on: Put returns only when Redis has the tasks. From
// then the record is as safe as the queued message was — both are in the
// queue Redis, which must run with `appendonly yes` (CheckDurable says when
// it does not). A role acks its message off the vendor queue AFTER the Put,
// the order it always had with the record write: a stop between the two
// sends the message again, never loses its record.
//
// A vendor's receipt is a task too (receipts.go, Saleem 2026-10-13: "once
// matched, checked, send to customer and db insertion queue, in both queues
// at the same time"): its own row and its message's final status go on the
// message's lane, queued in the same Redis transaction as the client's
// receipt. Until then receipts kept a list of their own
// (receiptfacts.Queue), which is still what a gateway without the DB worker
// uses, and where a receipt that beat its message's record waits for it.
//
// What it does not do: summaries (the billing role, from the records).
package recordq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/queueflow"
)

// The kinds of task: which write of the record store each one is.
const (
	KindWrite     = "w" // Store.Write: one record, an existing one left as it is (MO, retry, expiry)
	KindBatch     = "b" // Store.WriteBatch: a sent or refused message of the HTTP vendor role
	KindTerminal  = "t" // Store.WriteTerminalBatch: the same with its receipt row (SMPP vendor, blackhole)
	KindStatus    = "s" // Store.UpdateStatus
	KindReject    = "r" // Store.WriteRejected
	KindMOOutcome = "o" // what became of an MO, as builds before D-140 queued it: read as its final try (mo_sub_)
	KindRawDLR    = "d" // Store.AppendRawDLR: a receipt row the gateway makes itself (the retry role's "out of tries")
	KindReceipt   = "c" // Store.WriteReceiptBatch: a vendor's receipt, matched and decided — its row, its message's final status
	// The three-table design (Saleem and the PO, 27 Oct 2026): an outgoing
	// message's mt_ row is queued at acceptance, every vendor answer is a
	// sub_ row, every receipt a dlr_ row — all three written by the DB worker.
	KindAccepted = "m" // Store.WriteAccepted: the message's mt_ row, at acceptance
	KindAnswers  = "a" // Store.WriteAnswers: a vendor's answers that end nothing by themselves
	// Every hot-store row through the queue in worker mode (Saleem, 28 Oct
	// 2026): what the roles wrote to MySQL themselves until then.
	KindOrphan        = "u" // Store.WriteOrphans: an MO no route owns, a receipt that matched nothing
	KindOrphanMatched = "v" // Store.MarkOrphansMatched: a filed receipt applied after all
	KindLookup        = "l" // Store.WriteLookups: a lookup with no message (GET /sms/v4/operator)
	KindClose         = "k" // Store.CloseWithReceipt: a receipt matched in the record store, its row and its message's close
	// D-140 (Saleem 5 Nov 2026): incoming messages and every try to give a
	// client an MO or a receipt, written many at a time, never changed.
	KindMO  = "i" // Store.WriteMOs: an incoming message's mo_ row
	KindTry = "y" // Store.WriteClientTries: a try to give a client an MO or a receipt (mo_sub_, dlr_sub_)
)

// Task is one record write, as it waits in Redis.
type Task struct {
	Kind string `json:"k"`
	ID   string `json:"i"` // the message it is about: its lane, and its order
	// Co is the message's customer company, for a task that names a message
	// by its id alone (a status, a receipt row): its record is in
	// mt_<company>_<day> (D-136). A task that carries the message has it there.
	Co int64 `json:"c,omitempty"`
	At int64 `json:"t"` // when it was queued, unix milliseconds
	// Tries is how many times a write of this task alone failed and it was
	// put back.
	Tries int `json:"n,omitempty"`
	// Failed is when MySQL first refused it (unix milliseconds): it is kept
	// and tried again for KeepFor from then.
	Failed int64           `json:"f,omitempty"`
	D      json.RawMessage `json:"d"`
	// Once says this task may be on a list already — it is put again after
	// an answer that did not say whether the first put reached Redis
	// (KeepRejected) — and is written the careful way: nothing that is there
	// already. Read for refusals' rows.
	Once bool `json:"o,omitempty"`
	// pend is what a status pull may be told of the message while this task
	// waits (Pending); set for the tasks that carry an outgoing message's
	// status, and never queued itself.
	pend string
}

// PendingRecord is what is known of an outgoing message whose record is
// still in the queue: enough for GET /messages/{id} to answer as the record
// will, once it is written.
type PendingRecord struct {
	Status      string `json:"s"`
	AccountID   int64  `json:"a"`
	RequestID   string `json:"r,omitempty"`
	VendorMsgID string `json:"v,omitempty"`
	Reference   string `json:"c,omitempty"`
}

// pendOf is a message's PendingRecord as it is kept in Redis. The status is
// the one the client was told when the account's receipt policy answered for
// the vendor, as the record's own status pull reads it.
func pendOf(m model.Message, vendorMsgID string) string {
	if m.Direction == model.DirectionMO || m.ID == "" {
		return ""
	}
	st := string(m.Status)
	if m.ToldStatus != "" {
		st = string(m.ToldStatus)
	}
	if vendorMsgID == "" {
		vendorMsgID = m.VendorMsgID
	}
	b, err := json.Marshal(PendingRecord{Status: st, AccountID: m.AccountID, RequestID: m.RequestID,
		VendorMsgID: vendorMsgID, Reference: m.IdempotencyKey})
	if err != nil {
		return ""
	}
	return string(b)
}

type statusChange struct {
	Status      model.Status `json:"s"`
	VendorMsgID string       `json:"v,omitempty"`
	ErrorCode   string       `json:"e,omitempty"`
}

const (
	// DefaultLanes is how many lists the tasks are spread over.
	DefaultLanes = 4
	// Batch is how many tasks one write to MySQL takes.
	Batch = 500
	// DefaultMax is how many tasks may wait before a role is told the queue
	// is full (YAKU_RECORD_QUEUE_MAX): at about 1 KB a task, 2 GB of Redis.
	DefaultMax = 2_000_000
	// KeepFor is how long a task MySQL refuses is kept and tried again
	// before it is let go, loudly. (A MySQL that is away is waited for
	// without limit: see isAway.)
	KeepFor = 24 * time.Hour
	// NoWorkerAfter is how long a role, once started, waits for a DB
	// worker's first sign of life before it says none is running (the roles
	// of a gateway start in any order).
	NoWorkerAfter = 30 * time.Second
	// WorkerGoneAfter is how long a role waits, once it has seen a DB worker,
	// after that worker's sign of life is gone: a second worker of the
	// deployment writes it again within aliveEvery when the first stops.
	WorkerGoneAfter = 6 * time.Second
)

// Queue is the lists of one deployment (owner "" = an install that names no
// deployment), for the roles that add to them and the DB worker that writes
// them out. The lists themselves are the DAL's (dal.RecordLanes: in Redis,
// recq:<lane>[@owner] and the keys beside it, internal/dal/redis/records.go).
//
// What is kept beside a lane: its parked tasks (MySQL refused them, in the
// order they were queued) and per message how many it has parked — a message
// with anything parked has its later tasks parked behind it, so its records
// are written in order (Worker.step); per message what a status pull may be
// told while its record waits (PendingRecord, written with the task, removed
// when the task is written to MySQL); the lane's "doing" mark (Worker.step);
// a running DB worker's sign of life, which runs out by itself; and the set
// of lanes anything was ever put on — the DB worker writes out every lane in
// it, so a role started with another YAKU_RECORD_LANES cannot leave tasks on
// a list nobody reads.
type Queue struct {
	l     dal.RecordLanes
	owner string
	lanes int
	token string
}

// New names the lists. lanes < 1 = DefaultLanes.
func New(l dal.RecordLanes, lanes int) *Queue {
	if lanes < 1 {
		lanes = DefaultLanes
	}
	tok := make([]byte, 12)
	_, _ = rand.Read(tok)
	return &Queue{l: l, owner: l.Owner(), lanes: lanes, token: hex.EncodeToString(tok)}
}

// Pending says what is known of a message whose record is still in the
// queue. found is false when it has none there: the record is written (ask
// the record store), or there never was one.
func (q *Queue) Pending(ctx context.Context, msgID string) (PendingRecord, bool, error) {
	lanes, err := q.usedLanes(ctx)
	if err != nil {
		return PendingRecord{}, false, err
	}
	vals, err := q.l.PendingOf(ctx, lanes, msgID)
	if err != nil {
		return PendingRecord{}, false, err
	}
	for _, v := range vals {
		var pr PendingRecord
		if v != "" && json.Unmarshal([]byte(v), &pr) == nil && pr.Status != "" {
			return pr, true, nil
		}
	}
	return PendingRecord{}, false, nil
}

func (q *Queue) laneOf(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(q.lanes))
}

// ErrFull says the record queue holds as many tasks as it may: the DB worker
// is not keeping up, or is not running. A role treats it as a record write
// that failed — it keeps the message and tries again.
var ErrFull = errors.New("the record queue is full: the DB worker is not keeping up, or is not running")

// Client is what a role writes its records through in place of the record
// store. It has the store's own method names, so the code around a record
// write — the retries, the held leases — is the same either way.
type Client struct {
	q    *Queue
	max  int64
	logf func(string, ...any)

	full    atomic.Bool
	waiting atomic.Int64
	put     atomic.Int64
	// noWorker: no DB worker has said it is running for NoWorkerAfter.
	noWorker atomic.Bool
	mu       sync.Mutex
	known    map[int]bool // lanes this process has named in lanesKey

	// ctx is the client's life (NewClient): the refusals kept to be put
	// again (rejects_kept.go) are tried until it ends.
	ctx context.Context
	// tasks held to be put again, marked Once (kept.go).
	keepMu      sync.Mutex
	kept        []keptTask
	keepRunning bool
}

// NewClient makes a role's writer and starts the watch that says when the
// queue is full; it stops with ctx. max < 1 = DefaultMax.
func NewClient(ctx context.Context, q *Queue, max int64, logf func(string, ...any)) *Client {
	if max < 1 {
		max = DefaultMax
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	c := &Client{q: q, max: max, logf: logf, known: map[int]bool{}, ctx: ctx}
	go c.watch(ctx)
	return c
}

// watch reads, once a second, how many tasks wait and whether a DB worker is
// running: a Put asks nothing.
func (c *Client) watch(ctx context.Context) {
	t := time.NewTicker(watchEvery)
	defer t.Stop()
	var saidFull, saidNone time.Time
	seen := time.Now() // a gateway's roles start in any order: the worker is given noWorkerAfter
	grace := noWorkerAfter
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, alive, err := c.q.look(ctx)
		if err != nil {
			continue // keep the last answer
		}
		c.waiting.Store(n)
		full := n >= c.max
		if full && time.Since(saidFull) > 30*time.Second {
			saidFull = time.Now()
			c.logf("the record queue holds %d tasks (the most allowed is %d): records are held back until the DB worker has written some", n, c.max)
		}
		c.full.Store(full)
		if alive {
			seen = time.Now()
			// Seen once, a worker that stops is missed within seconds: it
			// takes its sign of life back (Worker.leave), or that runs out.
			grace = goneAfter
			if c.noWorker.Swap(false) {
				c.logf("a DB worker is running again: %d record(s) were waiting for it", n)
			}
			continue
		}
		if time.Since(seen) < grace {
			continue
		}
		c.noWorker.Store(true)
		if time.Since(saidNone) > time.Minute {
			saidNone = time.Now()
			c.logf("NO DB WORKER IS RUNNING for this deployment (records go through the DB worker; YAKU_RECORDS_VIA=direct writes them directly): %d record(s) wait in Redis and are written when the db-worker role starts", n)
		}
	}
}

// How often a role looks at the queue, and how long it waits for a worker's
// sign of life; variables so a test need not wait half a minute.
var (
	watchEvery    = time.Second
	noWorkerAfter = NoWorkerAfter
	goneAfter     = WorkerGoneAfter
)

// WorkerRunning says whether a DB worker has said it is running (true until
// one has been missed for NoWorkerAfter after the role started, or for
// WorkerGoneAfter once one had been seen).
func (c *Client) WorkerRunning() bool { return !c.noWorker.Load() }

// Full says the queue holds as many tasks as it may. A role that takes
// messages off a vendor queue asks before it takes more: what it sent it
// could not record.
// A nil client (the role writes its own records) is never full.
func (c *Client) Full() bool { return c != nil && c.full.Load() }

// Waiting is how many tasks wait in all lanes, the parked ones with them.
func (q *Queue) Waiting(ctx context.Context) (int64, error) {
	n, _, err := q.look(ctx)
	return n, err
}

// look reads, in one round trip, how many tasks wait and whether a DB worker
// has said it is running.
func (q *Queue) look(ctx context.Context) (waiting int64, alive bool, err error) {
	lanes, err := q.usedLanes(ctx)
	if err != nil {
		return 0, false, err
	}
	return q.l.Look(ctx, lanes)
}

// oldestHead is when the oldest task at the head of any lane was queued (unix
// milliseconds), 0 when every lane is empty. ok is false when Redis could
// not be read.
func (q *Queue) oldestHead(ctx context.Context) (at int64, ok bool) {
	lanes, err := q.usedLanes(ctx)
	if err != nil {
		return 0, false
	}
	heads, err := q.l.Heads(ctx, lanes)
	if err != nil {
		return 0, false
	}
	for _, v := range heads {
		var t Task
		if v != "" && json.Unmarshal([]byte(v), &t) == nil && t.At > 0 && (at == 0 || t.At < at) {
			at = t.At
		}
	}
	return at, true
}

// usedLanes is every lane anything was put on, and this queue's own.
func (q *Queue) usedLanes(ctx context.Context) ([]int, error) {
	seen := map[int]bool{}
	for l := 0; l < q.lanes; l++ {
		seen[l] = true
	}
	members, err := q.l.NamedLanes(ctx)
	if err != nil {
		return nil, err
	}
	for _, m := range members {
		if l, perr := strconv.Atoi(m); perr == nil && l >= 0 && l < 1024 {
			seen[l] = true
		}
	}
	out := make([]int, 0, len(seen))
	for l := range seen {
		out = append(out, l)
	}
	return out, nil
}

// Put queues tasks. It returns nil only when Redis has every one of them.
func (c *Client) Put(ctx context.Context, tasks ...Task) error {
	if c.full.Load() {
		return ErrFull
	}
	return c.putNow(ctx, tasks)
}

// putNow is Put without the limit.
func (c *Client) putNow(ctx context.Context, tasks []Task) error {
	if len(tasks) == 0 {
		return nil
	}
	put, done, err := c.stage(ctx, tasks)
	if err != nil {
		return err
	}
	// One transaction: all of them are queued, or none is.
	if err := c.q.l.Put(ctx, put); err != nil {
		return fmt.Errorf("queue %d record task(s): %w", len(tasks), err)
	}
	done()
	return nil
}

// stage prepares tasks to be queued: put is what one transaction carries
// (the lanes this process has not named yet, the tasks by lane, the pending
// entries by lane), done is called once that transaction has run.
func (c *Client) stage(_ context.Context, tasks []Task) (put dal.RecordPut, done func(), err error) {
	now := time.Now().UnixMilli()
	byLane := map[int][]string{}
	pend := map[int][]string{} // per lane: message id, what a status pull may be told
	for _, t := range tasks {
		if t.At == 0 {
			t.At = now
		}
		b, err := json.Marshal(t)
		if err != nil {
			return dal.RecordPut{}, nil, err
		}
		l := c.q.laneOf(t.ID)
		byLane[l] = append(byLane[l], string(b))
		if t.pend != "" {
			pend[l] = append(pend[l], t.ID, t.pend)
		}
	}
	put = dal.RecordPut{Owner: c.q.owner, Tasks: byLane, Pending: pend}
	c.mu.Lock()
	for l := range byLane {
		if !c.known[l] {
			put.Name = append(put.Name, l)
		}
	}
	c.mu.Unlock()
	done = func() {
		c.mu.Lock()
		for l := range byLane {
			c.known[l] = true
		}
		c.mu.Unlock()
		c.put.Add(int64(len(tasks)))
		// What went INTO each list, for Monitoring's Queues tab.
		for l, vals := range byLane {
			queueflow.In(queueflow.REC, strconv.Itoa(l), int64(len(vals)))
		}
	}
	return put, done, nil
}

// TxAcker is a queue store that takes messages off a vendor queue in a
// transaction which also queues record tasks (the Redis store's
// AckWithRecords).
type TxAcker interface {
	AckWithRecords(ctx context.Context, vendor string, msgIDs []string, p dal.RecordPut) error
}

// RecordAndAck queues the records of messages a vendor role has dealt with
// AND takes those messages off their vendor queue, in ONE Redis transaction:
// both happen or neither does. receiptRow says the records carry their
// at-submit receipt row (the SMPP vendor role, the blackhole), as
// WriteTerminalBatch does against WriteBatch.
//
// Why: done as two steps — the record, then the ack, the order the roles
// always had — a role stopped between them leaves a message that is recorded
// as sent and still queued, and it is sent a second time when its lease
// ends. Here there is no between.
//
// It needs the queues in the same Redis as the record queue, which they are
// whenever the queues are in Redis (both use the queue Redis). An error means
// nothing happened: the caller treats it as a record write that failed, and
// tries again.
func (c *Client) RecordAndAck(ctx context.Context, q TxAcker, vendor string, msgIDs []string, recs []dal.TerminalRecord, receiptRow bool) error {
	if c.full.Load() {
		return ErrFull
	}
	kind := KindBatch
	if receiptRow {
		kind = KindTerminal
	}
	tasks := make([]Task, 0, len(recs))
	for _, r := range recs {
		t, err := task(kind, r.Msg.ID, r)
		if err != nil {
			return err
		}
		t.pend = pendOf(r.Msg, r.VendorMsgID)
		tasks = append(tasks, t)
	}
	put, done, err := c.stage(ctx, tasks)
	if err != nil {
		return err
	}
	if err := q.AckWithRecords(ctx, vendor, msgIDs, put); err != nil {
		return fmt.Errorf("queue %d record task(s) with their ack: %w", len(tasks), err)
	}
	done()
	return nil
}

func task(kind, id string, v any) (Task, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return Task{}, err
	}
	return Task{Kind: kind, ID: id, D: b}, nil
}

// Write queues Store.Write for one record; an incoming message is a KindMO
// task, written with the others of its batch (WriteMOs).
func (c *Client) Write(ctx context.Context, m model.Message) error {
	kind := KindWrite
	if m.Direction == model.DirectionMO {
		kind = KindMO
	}
	t, err := task(kind, m.ID, m)
	if err != nil {
		return err
	}
	t.pend = pendOf(m, "")
	return c.Put(ctx, t)
}

func (c *Client) records(ctx context.Context, kind string, recs []dal.TerminalRecord) error {
	tasks := make([]Task, 0, len(recs))
	for _, r := range recs {
		t, err := task(kind, r.Msg.ID, r)
		if err != nil {
			return err
		}
		t.pend = pendOf(r.Msg, r.VendorMsgID)
		tasks = append(tasks, t)
	}
	return c.Put(ctx, tasks...)
}

// WriteBatch queues Store.WriteBatch for each record.
func (c *Client) WriteBatch(ctx context.Context, recs []dal.TerminalRecord) error {
	return c.records(ctx, KindBatch, recs)
}

// WriteTerminalBatch queues Store.WriteTerminalBatch for each record.
func (c *Client) WriteTerminalBatch(ctx context.Context, recs []dal.TerminalRecord) error {
	return c.records(ctx, KindTerminal, recs)
}

// WriteAccepted queues the mt_ rows of messages just accepted (Store
// WriteAccepted): one task a message, on its lane, ahead of everything its
// vendor will say.
func (c *Client) WriteAccepted(ctx context.Context, msgs []model.Message) error {
	tasks, err := acceptedTasks(msgs)
	if err != nil {
		return err
	}
	return c.Put(ctx, tasks...)
}

func acceptedTasks(msgs []model.Message) ([]Task, error) {
	tasks := make([]Task, 0, len(msgs))
	for _, m := range msgs {
		t, err := task(KindAccepted, m.ID, m)
		if err != nil {
			return nil, err
		}
		t.Co = m.CompanyID
		t.pend = pendOf(m, "")
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// TxEnqueuer is a vendor queue that can enqueue a message with record tasks
// in the same transaction (redis.Store.EnqueueWithRecords).
type TxEnqueuer interface {
	EnqueueWithRecords(ctx context.Context, m model.Message, p dal.RecordPut) error
}

// EnqueueAccepted puts an accepted message on its vendor queue AND its mt_
// task on the record queue in ONE Redis transaction (Saleem, 27 Oct 2026:
// "mt_ will be inserted on acceptance or queued to redis, simultaneously it
// will be sent to vendor queue for submission"). handled is false when q
// cannot (the queues are in MySQL): the caller then enqueues and records in
// two steps.
func (c *Client) EnqueueAccepted(ctx context.Context, q any, m model.Message) (handled bool, err error) {
	te, ok := q.(TxEnqueuer)
	if !ok {
		return false, nil
	}
	if c.full.Load() {
		return true, ErrFull
	}
	tasks, err := acceptedTasks([]model.Message{m})
	if err != nil {
		return true, err
	}
	put, done, err := c.stage(ctx, tasks)
	if err != nil {
		return true, err
	}
	if err := te.EnqueueWithRecords(ctx, model.ForQueue(m), put); err != nil {
		return true, err
	}
	done()
	return true, nil
}

// WriteAnswers queues vendor answers that end nothing by themselves (a "not
// now", a part with no answer while its message tries again): one task a
// message, on its lane.
func (c *Client) WriteAnswers(ctx context.Context, as []model.SubmitAnswer) error {
	byMsg := map[string][]model.SubmitAnswer{}
	var order []string
	for _, a := range as {
		if _, ok := byMsg[a.MsgID]; !ok {
			order = append(order, a.MsgID)
		}
		byMsg[a.MsgID] = append(byMsg[a.MsgID], a)
	}
	tasks := make([]Task, 0, len(order))
	for _, id := range order {
		t, err := task(KindAnswers, id, byMsg[id])
		if err != nil {
			return err
		}
		t.Co = byMsg[id][0].CompanyID
		tasks = append(tasks, t)
	}
	// Not held back by the queue's limit: the vendor has answered already.
	return c.putNow(ctx, tasks)
}

// UpdateStatus queues Store.UpdateStatus.
func (c *Client) UpdateStatus(ctx context.Context, ref model.RecordRef, s model.Status, vendorMsgID, errorCode string) error {
	t, err := task(KindStatus, ref.MsgID, statusChange{Status: s, VendorMsgID: vendorMsgID, ErrorCode: errorCode})
	if err != nil {
		return err
	}
	t.Co = ref.CompanyID
	return c.Put(ctx, t)
}

// WriteRejected queues Store.WriteRejected.
func (c *Client) WriteRejected(ctx context.Context, m model.Message) error {
	t, err := task(KindReject, m.ID, m)
	if err != nil {
		return err
	}
	return c.Put(ctx, t)
}

// WriteRejectedBatch queues Store.WriteRejected for each refusal, all in ONE
// Redis transaction (the reject log's batches, rejectlog.BatchWriter): one
// round trip for up to 500 rows, where one a row kept up with only a few
// thousand a second (load test of 30 Oct 2026, finding 3).
func (c *Client) WriteRejectedBatch(ctx context.Context, ms []model.Message) error {
	tasks, err := rejectTasks(ms)
	if err != nil {
		return err
	}
	return c.Put(ctx, tasks...)
}

func rejectTasks(ms []model.Message) ([]Task, error) {
	tasks := make([]Task, 0, len(ms))
	for _, m := range ms {
		t, err := task(KindReject, m.ID, m)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// AppendRawDLR queues Store.AppendRawDLR.
func (c *Client) AppendRawDLR(ctx context.Context, ref model.RecordRef, entry string) error {
	t, err := task(KindRawDLR, ref.MsgID, entry)
	if err != nil {
		return err
	}
	t.Co = ref.CompanyID
	// A receipt row: held and put again when the queue does not take it, never
	// written to MySQL by the role (28 Oct 2026).
	return c.putOrKeep(ctx, t)
}

// closeTask is a receipt the receipt service matched in the record store:
// its row, and its message's close when this receipt is the one that closes
// it — decided BEFORE it is queued, by the closing mark in Redis (May).
type closeTask struct {
	Status      model.Status `json:"s"`
	VendorMsgID string       `json:"v,omitempty"`
	ErrorCode   string       `json:"e,omitempty"`
	Entry       string       `json:"r"`
	May         bool         `json:"m,omitempty"`
}

// CloseWithReceipt queues a receipt's row and its message's close (kind k).
// mine is mayClose: in worker mode the closing mark in Redis has decided who
// closes the message before this is called (dlr.Service.closeRecord); the
// record store's own key (final_of) stays the guard when the worker writes it.
// A task the queue does not take is held and put again.
func (c *Client) CloseWithReceipt(ctx context.Context, ref model.RecordRef, st model.Status, vendorMsgID, errorCode, entryJSON string, mayClose bool) (bool, error) {
	t, err := task(KindClose, ref.MsgID, closeTask{Status: st, VendorMsgID: vendorMsgID, ErrorCode: errorCode, Entry: entryJSON, May: mayClose})
	if err != nil {
		return false, err
	}
	t.Co = ref.CompanyID
	if err := c.putOrKeep(ctx, t); err != nil {
		return false, err
	}
	return mayClose && st.IsTerminal(), nil
}

// WriteMOs queues incoming messages (dal.MORecords), a KindMO task each.
func (c *Client) WriteMOs(ctx context.Context, ms []model.Message, _ bool) error {
	ts := make([]Task, 0, len(ms))
	for _, m := range ms {
		m.Direction = model.DirectionMO
		t, err := task(KindMO, m.ID, m)
		if err != nil {
			return err
		}
		t.pend = pendOf(m, "")
		ts = append(ts, t)
	}
	return c.Put(ctx, ts...)
}

// WriteClientTries queues tries (dal.ClientTries), a KindTry task each, on
// its message's lane: an MO's final try is written after its mo_ row. Held
// in memory when Redis does not take them (putOrKeep).
func (c *Client) WriteClientTries(ctx context.Context, tries []dal.ClientTry, _ bool) error {
	ts := make([]Task, 0, len(tries))
	for _, tr := range tries {
		if tr.MsgID == "" {
			continue
		}
		if tr.TryID == "" {
			tr.TryID = dal.NewTryID()
		}
		t, err := task(KindTry, tr.MsgID, tr)
		if err != nil {
			return err
		}
		t.Co = tr.CompanyID
		ts = append(ts, t)
	}
	if len(ts) == 0 {
		return nil
	}
	return c.putOrKeep(ctx, ts...)
}

// Stats is a role's side of the queue for its status page: tasks it queued
// since it started, tasks waiting in all (every role's), tasks it holds in
// memory to be put again (kept.go), whether it is being told the queue is
// full, and whether a DB worker is running.
func (c *Client) Stats() map[string]any {
	return map[string]any{"queued": c.put.Load(), "waiting": c.waiting.Load(), "kept": c.Kept(), "full": c.full.Load(), "most": c.max,
		"db_worker_running": !c.noWorker.Load()}
}

// AddTo puts the role's side of the record queue on its status sections as
// "record_queue". A nil client (the role writes its own records) adds
// nothing.
func (c *Client) AddTo(sections map[string]any) map[string]any {
	if c == nil {
		return sections
	}
	if sections == nil {
		sections = map[string]any{}
	}
	sections["record_queue"] = c.Stats()
	return sections
}

// Durability is a database that can say whether it keeps an append-only
// file (dal.RecordLanes).
type Durability interface {
	AppendOnly(ctx context.Context) (value string, known bool)
}

// CheckDurable says what is wrong with the Redis the records wait in, or ""
// when nothing is: without the append-only file, a Redis that is killed
// loses what was queued since its last save — records, here, of messages
// that were sent. A Redis that will not say (a hosted one often does not
// allow CONFIG) is not complained about.
func CheckDurable(ctx context.Context, d Durability) string {
	c2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if got, ok := d.AppendOnly(c2); ok && got != "yes" {
		return "the Redis that holds the record queue runs with appendonly " + got +
			": if it is killed, the records queued since its last save are lost. Set `appendonly yes`"
	}
	return ""
}

// Recorder is the record writes a traffic role makes. The record store
// itself is one (a role that writes its own records, as before) and so is a
// Client (the records go to the DB worker).
type Recorder interface {
	Write(ctx context.Context, m model.Message) error
	WriteBatch(ctx context.Context, recs []dal.TerminalRecord) error
	WriteTerminalBatch(ctx context.Context, recs []dal.TerminalRecord) error
	UpdateStatus(ctx context.Context, ref model.RecordRef, s model.Status, vendorMsgID, errorCode string) error
	WriteRejected(ctx context.Context, m model.Message) error
	AppendRawDLR(ctx context.Context, ref model.RecordRef, entry string) error
	WriteAccepted(ctx context.Context, msgs []model.Message) error
	WriteAnswers(ctx context.Context, as []model.SubmitAnswer) error
}

// ViaWorker says whether this gateway's roles hand their record writes to
// the DB worker: whenever there is a Redis to queue them in, unless
// YAKU_RECORDS_VIA=direct. Since the three-table design (Saleem, 27 Oct
// 2026: "all dlrs and subs are also queued just like mt") the worker is the
// default; direct is kept as a way out, and is what a store with no Redis
// (a harness) always gets.
func ViaWorker(cfg config.Config) bool {
	return cfg.RedisEnabled() && !RecordsDirect()
}

// RecordsDirect says the settings ask every role to write its own records
// (YAKU_RECORDS_VIA=direct).
func RecordsDirect() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("YAKU_RECORDS_VIA")), "direct")
}

// FromConfig names the lists of this deployment, in the Redis the queues
// are in (YAKU_QUEUE_REDIS_*, else the main one: dal.OpenRecordLanes), with
// YAKU_RECORD_LANES. nil when no Redis is set (callers ask ViaWorker, or
// whether a Redis is set, first).
func FromConfig(cfg config.Config) *Queue {
	lanes := DefaultLanes
	if n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("YAKU_RECORD_LANES"))); err == nil && n >= 1 && n <= 64 {
		lanes = n
	}
	l, err := dal.OpenRecordLanes(cfg, deployment.Owner())
	if err != nil {
		return nil
	}
	return New(l, lanes)
}

// ForRole is the writer a role uses for its records: store itself, or — with
// ViaWorker — a Client on this deployment's lists. client is nil when the
// role writes its own. A Redis that keeps no append-only file is said once.
func ForRole(ctx context.Context, cfg config.Config, store Recorder, logf func(string, ...any)) (rec Recorder, client *Client) {
	if !ViaWorker(cfg) {
		return store, nil
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	q := FromConfig(cfg)
	max := int64(DefaultMax)
	if n, err := strconv.ParseInt(strings.TrimSpace(os.Getenv("YAKU_RECORD_QUEUE_MAX")), 10, 64); err == nil && n >= 1 {
		max = n
	}
	if why := CheckDurable(ctx, q.l); why != "" {
		logf("WARNING: %s", why)
	}
	logf("records go to the DB worker (the default with Redis; YAKU_RECORDS_VIA=direct writes them directly): %d lane(s), at most %d waiting", q.lanes, max)
	client = NewClient(ctx, q, max, logf)
	return client, client
}

// Store is the lists themselves (the DB worker's durability check).
func (q *Queue) Store() dal.RecordLanes { return q.l }

// Lanes is how many lists this queue spreads its tasks over.
func (q *Queue) Lanes() int { return q.lanes }
