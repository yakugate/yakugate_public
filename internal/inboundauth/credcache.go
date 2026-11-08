package inboundauth

import (
	"crypto/sha256"
	"crypto/subtle"
	"sync"
	"time"
)

// verifiedCache remembers passwords that have already passed bcrypt.
//
// The same reasoning as the submit edge's (internal/auth, chunk-2 ruling 2.1):
// bcrypt at its default cost is tens of milliseconds of pure CPU, and a vendor
// pushing receipts pays it on every single callback. An entry binds to the
// exact bcrypt hash it was verified against, so changing the stored password
// invalidates it by itself — there is no invalidation hook to forget.
//
// What is stored is a SHA-256 of the presented password, never the password,
// and the compare is constant time.
type verifiedCache struct {
	mu  sync.RWMutex
	ttl time.Duration
	m   map[string]entry
}

type entry struct {
	sum  [32]byte
	seen time.Time
}

// credTTL bounds how long a verified password is remembered. Short enough
// that a rotated credential stops working promptly even if the hash it was
// bound to somehow survives, long enough to cover a burst of callbacks.
const credTTL = 5 * time.Minute

func newVerifiedCache() *verifiedCache {
	return &verifiedCache{ttl: credTTL, m: map[string]entry{}}
}

// hit reports whether this exact (hash, password) pair has already passed
// bcrypt recently.
func (c *verifiedCache) hit(bcryptHash, presented string) bool {
	c.mu.RLock()
	e, ok := c.m[bcryptHash]
	c.mu.RUnlock()
	if !ok || time.Since(e.seen) > c.ttl {
		return false
	}
	sum := sha256.Sum256([]byte(presented))
	return subtle.ConstantTimeCompare(e.sum[:], sum[:]) == 1
}

// store remembers a pair that has just passed bcrypt, and drops anything
// stale while it holds the lock — the map only ever has one entry per
// endpoint password, so there is nothing heavier to do here.
func (c *verifiedCache) store(bcryptHash, presented string) {
	sum := sha256.Sum256([]byte(presented))
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.m {
		if now.Sub(e.seen) > c.ttl {
			delete(c.m, k)
		}
	}
	c.m[bcryptHash] = entry{sum: sum, seen: now}
}
