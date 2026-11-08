package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Allowance is what this process has taken from each key's per-second bucket
// and not handed out yet. The bucket is asked a few times a second at most —
// a quarter of the limit at a time, or what is needed if more — and the
// rest is handed out from memory. What is left when the second ends is let
// go; the bucket's own key ends with its second.
//
// Built 28 Oct 2026 for smpp-server's receipt and MO legs (each claim asked
// the main Redis, behind thousands of submits), and used by the accept path
// since 29 Oct (Saleem: the rate limit cost 4 Redis calls a message — two
// buckets asked per message). The limit is never exceeded: every unit handed
// out was granted by the shared bucket. Near the cap a process may hold
// units it does not use, so the others get up to a quarter less in that
// second.
type Allowance struct {
	mu        sync.Mutex
	held      map[string]*allowed
	lastSweep int64
}

type allowed struct {
	sec  int64
	left int
	dry  bool // the bucket gave less than asked: it is empty for this second
	// asking is closed when the one request out for this second comes back;
	// nil = none out. One request a key at a time (load test of 30 Oct
	// 2026): when a chunk ran out, every submit asked at once, the first
	// four got 2,500 and the rest 0, and a 0 that came back before the units
	// marked the bucket empty while up to 10,000 were on their way.
	asking chan struct{}
}

// waitFor bounds how long a Take waits for another's request to come back
// when its caller set no end (Shared's own call ends after callFor).
const waitFor = callFor + time.Second

// Take hands out up to n units of key for this second, asking g for more
// when what is held does not cover n. Once the bucket has given less than
// it was asked for, it is not asked again in that second.
func (a *Allowance) Take(g Granter, key string, limit, n int) int {
	return a.TakeContext(context.Background(), g, key, limit, n)
}

// TakeContext is Take for a caller with a context: while another request
// for key is out, it waits for that answer, no longer than ctx (and
// waitFor) allows, and then takes from it or asks again.
func (a *Allowance) TakeContext(ctx context.Context, g Granter, key string, limit, n int) int {
	if n < 1 {
		return 0
	}
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	a.mu.Lock()
	defer a.mu.Unlock()
	for {
		h := a.entry(key)
		if h.left >= n || h.dry {
			return h.give(n)
		}
		if h.asking == nil {
			a.ask(h, g, key, limit, max(n-h.left, limit/4))
			return h.give(n)
		}
		out := h.asking
		a.mu.Unlock()
		if timer == nil {
			timer = time.NewTimer(waitFor)
		}
		select {
		case <-out:
			a.mu.Lock()
		case <-ctx.Done():
			a.mu.Lock()
			return h.give(n)
		case <-timer.C:
			a.mu.Lock()
			return h.give(n)
		}
	}
}

// entry is key's units for this second; a.mu is held.
func (a *Allowance) entry(key string) *allowed {
	now := time.Now().Unix()
	if a.held == nil {
		a.held = map[string]*allowed{}
	}
	if now-a.lastSweep >= 60 {
		a.lastSweep = now
		for k, h := range a.held {
			if h.sec < now && h.asking == nil {
				delete(a.held, k)
			}
		}
	}
	h := a.held[key]
	if h == nil || h.sec != now {
		h = &allowed{sec: now}
		a.held[key] = h
	}
	return h
}

// ask is the one request out for h: a.mu is let go while the bucket answers,
// and held again when ask returns. A short answer (with no other request
// out, so nothing more is coming) says the bucket is empty.
func (a *Allowance) ask(h *allowed, g Granter, key string, limit, want int) {
	want = min(limit, want)
	done := make(chan struct{})
	h.asking = done
	got := 0
	defer func() {
		h.asking = nil
		close(done)
		h.left += got
		if got < want {
			h.dry = true
		}
	}()
	a.mu.Unlock()
	defer a.mu.Lock()
	got = g.Grant(key, limit, want)
}

// give hands out up to n of what h holds; a.mu is held.
func (h *allowed) give(n int) int {
	give := min(n, h.left)
	h.left -= give
	return give
}

// Back keeps units taken but not used, for the rest of their second.
func (a *Allowance) Back(key string, n int) {
	if n < 1 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if h := a.held[key]; h != nil && h.sec == time.Now().Unix() {
		h.left += n
	}
}

// Release hands back to the shared bucket the units of key this process
// still holds for the current second. A drainer calls it when its queue
// came back short: units held with nothing to deliver starve the other legs
// on the same bucket (4 Nov 2026, the MO held on tw-2). The drainer itself
// skips that account for the rest of its check, so an idle account costs
// one ask and one return a check. It does nothing while a request for key
// is out, or once the second has passed.
func (a *Allowance) Release(g Granter, key string) {
	a.mu.Lock()
	h := a.held[key]
	if h == nil || h.sec != time.Now().Unix() || h.asking != nil {
		a.mu.Unlock()
		return
	}
	n := h.left
	h.left = 0
	a.mu.Unlock()
	if n > 0 && g != nil {
		g.Return(key, n)
	}
}

// Chunked is a rate limiter for the accept path: Allow draws from what this
// process holds of the shared bucket (Allowance), so a message costs no
// Redis call unless the chunk has run out. It is a Granter too, passing
// straight through, for code that asks the bucket itself.
type Chunked struct {
	g Granter
	a Allowance
}

// NewChunked wraps a shared Granter (Shared, the buckets of the DAL).
func NewChunked(g Granter) *Chunked { return &Chunked{g: g} }

// Allow reports whether n units of key fit in its per-second limit. A
// refused request takes nothing: what it was given back stays for a smaller
// one in the same second.
func (c *Chunked) Allow(key string, limit, n int) bool {
	return c.AllowContext(context.Background(), key, limit, n)
}

// AllowContext is Allow for a caller with a context: a wait for the chunk
// another message asked for ends with ctx.
func (c *Chunked) AllowContext(ctx context.Context, key string, limit, n int) bool {
	if limit <= 0 {
		return true
	}
	if n < 1 {
		n = 1
	}
	got := c.a.TakeContext(ctx, c.g, key, limit, n)
	if got == n {
		return true
	}
	c.a.Back(key, got)
	return false
}

// Grant and Return pass through to the shared bucket.
func (c *Chunked) Grant(key string, limit, want int) int { return c.g.Grant(key, limit, want) }
func (c *Chunked) Return(key string, n int)              { c.g.Return(key, n) }
