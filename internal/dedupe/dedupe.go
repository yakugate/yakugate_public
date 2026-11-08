// Package dedupe provides the duplicate check used by the submit pipeline
// (pipeline.Deduper).
//
// One call a message (Saleem, 29 Oct 2026: "just go and check the hash
// created by text, sender, receiver and user provided message reference id,
// against that account id, i.e account_id:hash, try to create, if exists
// means duplicate else let it be created, simple, with ttl provided in dedup
// ttl, in account or default (30 min)").
//
// The caller builds the key (pipeline.DedupKey: dedup:<account>:<hash>);
// Check tries to create it holding the new message's id for ttl. Created =
// a new message. Already there = a duplicate of the id it holds, which the
// pipeline refuses (409 DUPLICATE_MESSAGE naming the first id, 28 Oct 2026).
//
// What went with it (it cost four commands more a message): the pending
// claim, its marker key and the Hold/Commit of 19 Oct 2026, and the separate
// "same messageId, different text" conflict — the hash covers the text, so a
// different text is a new message. A key whose message is not queued is
// released by the enqueue stage (Release, on the failure path only); a
// process that dies between the check and the queue write leaves its key for
// the window, and the client's resend is refused as a duplicate (accepted,
// as ruled, for one call a message).
package dedupe

import (
	"context"
	"sync"
	"time"
)

// DefaultTTL is how long a message is remembered when neither the account
// (dedupe_ttl_sec) nor YAKU_DEDUPE_TTL_SEC says (Saleem, 29 Oct 2026; it
// was 24 hours).
const DefaultTTL = 30 * time.Minute

// windowSec is a key's window in whole seconds, at least one: EX 0 is
// refused by Redis.
func windowSec(ttl time.Duration) int {
	if s := int(ttl / time.Second); s > 0 {
		return s
	}
	return 1
}

type record struct {
	msgID string
	exp   time.Time
}

// Mem is an in-memory duplicate check: one process only. A deployment with
// several processes uses Redis or MySQL (same interface).
type Mem struct {
	mu        sync.Mutex
	seen      map[string]record
	fallback  time.Duration // used when Check's caller passes ttl<=0
	lastSweep time.Time
}

// NewMem's ttl is the fallback used when a caller passes ttl<=0 to Check.
func NewMem(ttl time.Duration) *Mem {
	return &Mem{seen: map[string]record{}, fallback: ttl}
}

// Check creates key holding msgID for ttl. firstID is "" when it was created
// (a new message), else the id it already holds (a duplicate).
func (d *Mem) Check(_ context.Context, key, msgID string, ttl time.Duration) (firstID string, err error) {
	if ttl <= 0 {
		ttl = d.fallback
	}
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	if r, ok := d.seen[key]; ok && r.exp.After(now) {
		return r.msgID, nil
	}
	d.seen[key] = record{msgID: msgID, exp: now.Add(ttl)}
	// Expired records swept at most once a minute (chunk-8 ruling 8.3).
	if now.Sub(d.lastSweep) >= time.Minute {
		d.lastSweep = now
		for k, r := range d.seen {
			if r.exp.Before(now) {
				delete(d.seen, k)
			}
		}
	}
	return "", nil
}

// Release removes the key when it still holds msgID: a message that was
// never queued must not make the client's resend a duplicate.
func (d *Mem) Release(_ context.Context, key, msgID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if r, ok := d.seen[key]; ok && r.msgID == msgID {
		delete(d.seen, key)
	}
	return nil
}
