// Package queuecheck says how often each queue is checked and how much one
// check may take (issue 7 of the design audit; Saleem, 26 Oct 2026: "make
// checks configurable, for all the queues introduce settings for interval" —
// a fixed interval always, "just configurable quantity per configurable
// interval").
//
// Every queue the gateway polls has two settings:
//
//	YAKU_QUEUE_<KIND>_EVERY_MS      how often it is checked, busy or idle
//	YAKU_QUEUE_<KIND>_PER_INTERVAL  the most items one check takes
//
// A check takes up to that many and waits for the next interval; nothing else
// decides when it runs. What still limits a check is the vendor's and the
// gateway's own protection (the vendor's rate, the SMPP window, a full record
// queue, an account's throughput), not the pace of checks.
//
// The queue Redis was asked about 190 times a second with no traffic: each
// smpp-vendor session claimed every 150 ms and smpp-server looked for
// receipts and MO every 200 ms per bound client. The defaults are 1000 ms
// (Saleem's choice: lower idle load by default).
package queuecheck

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/platform/logging"
)

// Kind is one kind of queue.
type Kind string

// The kinds of queue. HLR has none: lookups run on their own and their log
// rows wait in the record queue (Records).
const (
	MT            Kind = "MT"             // vendor queues: smpp-vendor, http-vendor, blackhole
	DLR           Kind = "DLR"            // receipts waiting for a client: smpp-server, dlr webhooks
	MO            Kind = "MO"             // MO waiting for a client: smpp-server, mo webhooks
	Records       Kind = "RECORDS"        // the DB worker's record queue
	Billing       Kind = "BILLING"        // the billing queue
	ReceiptWrites Kind = "RECEIPT_WRITES" // rcptw: receipt rows waiting for MySQL
	Handoff       Kind = "HANDOFF"        // receipts and MO another gateway took in
	Reroute       Kind = "REROUTE"        // the re-route inbox and down vendors' queues
	Retry         Kind = "RETRY"          // vendor queues: messages out of tries
	Expiry        Kind = "EXPIRY"         // vendor queues: past their validity
)

// Check is how one kind of queue is checked.
type Check struct {
	Every       time.Duration // how often
	PerInterval int           // the most items one check takes
}

// Bounds of a setting: outside them the default is used, and said once.
const (
	leastEvery = 10 * time.Millisecond
	mostEvery  = 60 * time.Second
)

// spec is a kind's two settings and their defaults. Every name is written out
// whole, so the list of known settings finds it.
type spec struct {
	every, per     string
	everyMS, perN  int
	oldEvery, unit string // an older setting read while every is unset, and what it was
}

var specs = map[Kind]spec{
	MT:            {every: "YAKU_QUEUE_MT_EVERY_MS", per: "YAKU_QUEUE_MT_PER_INTERVAL", everyMS: 1000, perN: 2000},
	DLR:           {every: "YAKU_QUEUE_DLR_EVERY_MS", per: "YAKU_QUEUE_DLR_PER_INTERVAL", everyMS: 1000, perN: 5000, oldEvery: "YAKU_SMPP_DELIVER_MS"},
	MO:            {every: "YAKU_QUEUE_MO_EVERY_MS", per: "YAKU_QUEUE_MO_PER_INTERVAL", everyMS: 1000, perN: 5000, oldEvery: "YAKU_SMPP_DELIVER_MS"},
	Records:       {every: "YAKU_QUEUE_RECORDS_EVERY_MS", per: "YAKU_QUEUE_RECORDS_PER_INTERVAL", everyMS: 1000, perN: 5000},
	Billing:       {every: "YAKU_QUEUE_BILLING_EVERY_MS", per: "YAKU_QUEUE_BILLING_PER_INTERVAL", everyMS: 1000, perN: 5000},
	ReceiptWrites: {every: "YAKU_QUEUE_RECEIPT_WRITES_EVERY_MS", per: "YAKU_QUEUE_RECEIPT_WRITES_PER_INTERVAL", everyMS: 1000, perN: 5000},
	Handoff:       {every: "YAKU_QUEUE_HANDOFF_EVERY_MS", per: "YAKU_QUEUE_HANDOFF_PER_INTERVAL", everyMS: 1000, perN: 5000},
	Reroute:       {every: "YAKU_QUEUE_REROUTE_EVERY_MS", per: "YAKU_QUEUE_REROUTE_PER_INTERVAL", everyMS: 2000, perN: 200},
	Retry:         {every: "YAKU_QUEUE_RETRY_EVERY_MS", per: "YAKU_QUEUE_RETRY_PER_INTERVAL", everyMS: 1000, perN: 500},
	Expiry:        {every: "YAKU_QUEUE_EXPIRY_EVERY_MS", per: "YAKU_QUEUE_EXPIRY_PER_INTERVAL", everyMS: 1000, perN: 500},
}

// Default is a kind's check with nothing set.
func Default(k Kind) Check {
	s := specs[k]
	return Check{Every: time.Duration(s.everyMS) * time.Millisecond, PerInterval: s.perN}
}

var (
	mu    sync.Mutex
	read  = map[Kind]Check{} // what this process read, for its status
	said  = map[string]bool{}
	logf  = logging.New("queue-checks").Printf
	added bool
)

// For reads a kind's two settings, as this process uses them. A role reads
// each kind it checks once, when it is built; the kind then shows on its
// status (queue_checks).
func For(k Kind) Check {
	c := Read(k)
	mu.Lock()
	read[k] = c
	if !added {
		added = true
		monitor.AddSection("queue_checks", Section)
	}
	mu.Unlock()
	return c
}

// Read is For without the status: the settings as they are now.
func Read(k Kind) Check {
	s, ok := specs[k]
	if !ok {
		return Check{Every: time.Second, PerInterval: 1}
	}
	c := Default(k)
	if v, ok := lookup(s.every); ok {
		if ms, err := strconv.Atoi(v); err == nil && inEvery(time.Duration(ms)*time.Millisecond) {
			c.Every = time.Duration(ms) * time.Millisecond
		} else {
			sayOnce(s.every, fmt.Sprintf("%s=%q is not a whole number of milliseconds from %d to %d: %s is used",
				s.every, v, leastEvery.Milliseconds(), mostEvery.Milliseconds(), c.Every))
		}
	} else if s.oldEvery != "" {
		// A settings file written before 26 Oct 2026 may still set the old
		// one: it is kept while the new one is not set.
		if v, ok := lookup(s.oldEvery); ok {
			if ms, err := strconv.Atoi(v); err == nil && inEvery(time.Duration(ms)*time.Millisecond) {
				c.Every = time.Duration(ms) * time.Millisecond
				sayOnce(s.oldEvery+"/"+string(k), fmt.Sprintf("%s is retired: it is read for %s because %s is not set (%s)",
					s.oldEvery, strings.ToLower(string(k)), s.every, c.Every))
			}
		}
	}
	if v, ok := lookup(s.per); ok {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			c.PerInterval = n
		} else {
			sayOnce(s.per, fmt.Sprintf("%s=%q is not a whole number of 1 or more: %d is used", s.per, v, c.PerInterval))
		}
	}
	return c
}

func lookup(name string) (string, bool) {
	v, ok := os.LookupEnv(name)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func inEvery(d time.Duration) bool { return d >= leastEvery && d <= mostEvery }

func sayOnce(key, text string) {
	mu.Lock()
	seen := said[key]
	said[key] = true
	mu.Unlock()
	if !seen {
		logf("%s", text)
	}
}

// Section is the queue_checks part of this process's status: each kind it
// checks, how often and how much.
func Section() any {
	mu.Lock()
	defer mu.Unlock()
	if len(read) == 0 {
		return nil
	}
	out := map[string]any{}
	for k, c := range read {
		out[strings.ToLower(string(k))] = map[string]any{"every_ms": c.Every.Milliseconds(), "per_interval": c.PerInterval}
	}
	return out
}

// Names lists every setting of this package, the interval then the quantity
// of each kind, sorted by kind.
func Names() []string {
	kinds := make([]string, 0, len(specs))
	for k := range specs {
		kinds = append(kinds, string(k))
	}
	sort.Strings(kinds)
	var out []string
	for _, k := range kinds {
		s := specs[Kind(k)]
		out = append(out, s.every, s.per)
	}
	return out
}
