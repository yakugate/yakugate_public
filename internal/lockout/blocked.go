package lockout

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Who is locked right now (2026-10-09, Saleem: an operator could lift a block
// with POST /admin/unblock but had no way to SEE one — he had to be told the
// login name or the address by the customer it was hurting).
//
// A ladder in memory just reads its own entries. The shared ladders read the
// list the DAL keeps of who is locked (dal.Blocks.Blocked; in Redis the index
// lkidx, written by the script that sets a lock — so only when a lock is
// imposed, never per request). The lock stays the truth: an id whose lock
// has gone (ended, or lifted by Clear) is not reported.
//
// Every shared ladder of a gateway writes that ONE list, as they share one
// store already: the identity ladder, the per-address ladder and the
// guessing guard are kept apart by the form of the id ("u:alice",
// "ip:203.0.113.7", a bare address), not by where it is filed.
//
// A lock imposed by a process older than the index is not in it. It is still
// enforced, and still lifted by Clear; it is only not listed. The longest
// lock is an hour, so that gap closes by itself.

// Block is one login or address a ladder holds locked right now: ID as the
// ladder keys it, Level the rung it is on (1 to len(rungs)), Remaining until
// it lets go by itself.
type Block = dal.Block

// Lister is a ladder that can say who it holds. more reports that it holds
// more than limit; the ones locked longest are the ones returned.
type Lister interface {
	Blocked(limit int) (blocks []Block, more bool, err error)
}

// Ref names a block without showing its id. A token ladder's id IS the token
// that was tried, which no screen should carry; the reference is what a caller
// hands back to say "this one".
func Ref(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:12])
}

// Rungs are the lock lengths, shortest first — for a screen that explains them.
func Rungs() []time.Duration { return append([]time.Duration(nil), rungs...) }

// Window is how long failures are counted together.
func Window() time.Duration { return window }

// longestFirst orders blocks by time left, then by id so equal ones keep one
// order, and cuts the list at limit.
func longestFirst(blocks []Block, limit int) ([]Block, bool) {
	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].Remaining != blocks[j].Remaining {
			return blocks[i].Remaining > blocks[j].Remaining
		}
		return blocks[i].ID < blocks[j].ID
	})
	if limit > 0 && len(blocks) > limit {
		return blocks[:limit], true
	}
	return blocks, false
}

// Blocked lists the ids this ladder holds locked now. One pass over the
// entries under the mutex — the same cost as the eviction sweep.
func (l *Ladder) Blocked(limit int) ([]Block, bool, error) {
	l.mu.Lock()
	now := l.now()
	var out []Block
	for id, e := range l.entries {
		if e.until.After(now) {
			out = append(out, Block{ID: id, Level: e.level, Remaining: e.until.Sub(now)})
		}
	}
	l.mu.Unlock()
	out, more := longestFirst(out, limit)
	return out, more, nil
}

// Blocked lists the addresses the guard holds blocked now.
func (g *SprayGuard) Blocked(limit int) ([]Block, bool, error) { return g.ladder.Blocked(limit) }

// Blocked lists what the shared ladders hold locked now — every ladder that
// shares this store, see the note at the top of the file.
func (g *SharedSprayGuard) Blocked(limit int) ([]Block, bool, error) { return g.ladder.Blocked(limit) }

// Blocked lists what the shared ladders hold locked now. Unlike Locked it
// does not fail open: a list that could not be read is an error, never
// "nobody".
func (r *Shared) Blocked(limit int) ([]Block, bool, error) {
	if limit <= 0 {
		limit = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, cut, err := r.b.Blocked(ctx, limit)
	if err != nil {
		return nil, false, err
	}
	out, more := longestFirst(out, limit)
	return out, more || cut, nil
}
