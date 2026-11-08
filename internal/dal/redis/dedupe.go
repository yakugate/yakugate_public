package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Dedupe is the duplicate check in the main Redis (dal.Dedupe), moved from
// internal/dedupe (redis.go) on 29 Oct 2026: ONE command a message,
// `SET key <msg id> NX GET EX <window>` (Redis 7 and later). It creates the
// key and answers nothing, or leaves it and answers the id it holds.
type Dedupe struct{ c *redis.Client }

// NewDedupe keeps the duplicate check on a Redis client.
func NewDedupe(c *redis.Client) *Dedupe { return &Dedupe{c: c} }

// Check creates key holding msgID for window: firstID "" = new, else the id
// of the message it duplicates. FAILS OPEN on a Redis error (a new message):
// a blip risks accepting a duplicate, which is recoverable, where refusing
// live traffic is not.
func (d *Dedupe) Check(ctx context.Context, key, msgID string, window time.Duration) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	first, err := d.c.SetArgs(cctx, key, msgID, redis.SetArgs{
		Mode: "NX", Get: true, TTL: window,
	}).Result()
	if err != nil {
		return "", nil // redis.Nil = created; anything else = fail open
	}
	return first, nil
}

// releaseScript deletes the key only while it holds THIS message's id, so a
// newer, legitimate key is never removed.
var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

// Release removes the key of a message that was never queued (the failure
// path only). Best effort, like Check.
func (d *Dedupe) Release(ctx context.Context, key, msgID string) error {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = releaseScript.Run(cctx, d.c, []string{key}, msgID).Err()
	return nil
}
