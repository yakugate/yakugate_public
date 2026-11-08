// Package queueflow counts the messages going into and out of each queue, in
// this process, for monitoring (Saleem 2026-10-05, D-127: "we need queues as
// well and their entire visibility").
//
// A queue's depth says how much is waiting; only its two rates say whether it
// is filling or draining, and how long it will take to empty. The stores call
// In and Out where a message really enters or leaves a queue — an enqueue, an
// ack, a move to another vendor's queue — so the counts are the queue's own,
// whichever role caused them. Every process counts only what it did itself;
// adding up the reports of a deployment's processes gives the deployment's
// rates, with nothing counted twice.
//
// No dependencies: the stores import it, and so does the monitor that
// publishes it.
package queueflow

import (
	"sort"
	"sync"
	"time"
)

// The three kinds of queue.
const (
	MT  = "mt"  // messages waiting for a vendor (one queue per vendor)
	DLR = "dlr" // receipts waiting for a customer (one per account)
	MO  = "mo"  // incoming messages waiting for a customer (one per account)
	// REC is the records waiting for the DB worker (one list per lane,
	// internal/recordq): in = a role queued a record, out = the worker wrote
	// it. 2026-10-13, Saleem: "i cant see queues and their counts that are
	// consumed by db worker".
	REC = "rec"
)

const window = 60 // seconds the rates are taken over

type counter struct {
	total   int64
	sec     int64 // unix second buckets[head] belongs to
	head    int
	buckets [window]int64
}

func (c *counter) advance(now int64) {
	if c.sec == 0 {
		c.sec = now
		return
	}
	gap := now - c.sec
	if gap <= 0 {
		return
	}
	if gap >= window {
		c.buckets = [window]int64{}
		c.head, c.sec = 0, now
		return
	}
	for i := int64(0); i < gap; i++ {
		c.head = (c.head + 1) % window
		c.buckets[c.head] = 0
	}
	c.sec = now
}

func (c *counter) add(now, n int64) {
	c.advance(now)
	c.buckets[c.head] += n
	c.total += n
}

// rate is the average a second over the last minute.
func (c *counter) rate(now int64) float64 {
	c.advance(now)
	var sum int64
	for _, b := range c.buckets {
		sum += b
	}
	return float64(sum) / window
}

type flow struct{ in, out counter }

var (
	mu    sync.Mutex
	flows = map[string]*flow{} // kind + "\x00" + name
	clock = defaultClock
)

func defaultClock() int64 { return time.Now().Unix() }

func get(kind, name string) *flow {
	k := kind + "\x00" + name
	f := flows[k]
	if f == nil {
		f = &flow{}
		flows[k] = f
	}
	return f
}

// In records n messages added to a queue.
func In(kind, name string, n int64) {
	if n <= 0 || name == "" {
		return
	}
	mu.Lock()
	get(kind, name).in.add(clock(), n)
	mu.Unlock()
}

// Out records n messages that left a queue (sent, ended or moved away).
func Out(kind, name string, n int64) {
	if n <= 0 || name == "" {
		return
	}
	mu.Lock()
	get(kind, name).out.add(clock(), n)
	mu.Unlock()
}

// Stat is one queue's flow in this process since it started.
type Stat struct {
	Kind     string  `json:"kind"`
	Queue    string  `json:"queue"`
	InTotal  int64   `json:"in_total"`
	OutTotal int64   `json:"out_total"`
	InTPS    float64 `json:"in_tps_60s"`
	OutTPS   float64 `json:"out_tps_60s"`
}

// Snapshot is every queue this process has moved a message in or out of.
func Snapshot() []Stat {
	now := clock()
	mu.Lock()
	out := make([]Stat, 0, len(flows))
	for k, f := range flows {
		kind, name := split(k)
		out = append(out, Stat{Kind: kind, Queue: name, InTotal: f.in.total, OutTotal: f.out.total,
			InTPS: round2(f.in.rate(now)), OutTPS: round2(f.out.rate(now))})
	}
	mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Queue < out[j].Queue
	})
	return out
}

func split(k string) (string, string) {
	for i := 0; i < len(k); i++ {
		if k[i] == 0 {
			return k[:i], k[i+1:]
		}
	}
	return k, ""
}

func round2(f float64) float64 { return float64(int64(f*100+0.5)) / 100 }

// reset clears every count (tests).
func reset() {
	mu.Lock()
	flows = map[string]*flow{}
	mu.Unlock()
}
