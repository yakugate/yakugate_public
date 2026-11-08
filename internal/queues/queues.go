// Package queues is what the gateway's queues share in both stores (issue 22
// of the design audit, with issue 25's names; Saleem and the PO, 23–26 Oct
// 2026): the names, the form of a waiting item, and how a vendor's rate is
// split between its queues.
//
// Every queue is two parts:
//   - ready: what may be taken now, in priority order (1 first), first in
//     first out within one priority;
//   - delayed: what waits for its pick time (a retry after its gap, a
//     scheduled send).
//
// A take removes what it takes (a crash between the take and the send loses
// that take: accepted by the PO, 26 Oct). Nothing is leased and nothing is
// acknowledged.
//
// In Redis a queue is two sorted sets, queues:<dep>:<what>:<who>:ready and
// …:delayed, holding the whole item; in MySQL it is the rows of the queue
// table with that short code, ready once pick_at has come.
package queues

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Common is the sub-queue of a vendor that every message goes to unless its
// sender is one of the vendor's short codes.
const Common = "common"

// Scope is the deployment part of a queue's name: the deployment id when it
// is set, else "shared" (an install that never named its deployments).
func Scope(owner string) string {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return "shared"
	}
	return owner
}

// --- names (issue 25 point 5) ----------------------------------------------

// The vendor part of an MT queue's name is the vendor's cid, the name its
// MySQL queue table has (queue_<cid>); issue 25 calls it the vendor id.

// MTBase is the start of every key of one vendor's queues in a scope:
// queues:<dep>:mt:<cid>.
func MTBase(scope, vendor string) string { return "queues:" + scope + ":mt:" + vendor }

// MTReady is one of a vendor's queues: queues:<dep>:mt:<cid>:<sub>:ready.
func MTReady(scope, vendor, sub string) string { return MTBase(scope, vendor) + ":" + sub + ":ready" }

// MTDelayed is the delayed part of MTReady's queue.
func MTDelayed(scope, vendor, sub string) string { return MTBase(scope, vendor) + ":" + sub + ":delayed" }

// MTList is the set of a vendor's sub-queues that hold something.
func MTList(scope, vendor string) string { return MTBase(scope, vendor) + ":list" }

// MTGroup holds the items of one long message whose parts were queued one by
// one (an SMPP client's parts the gateway could not join first), so the take
// of one part can take its siblings.
func MTGroup(scope, vendor, group string) string { return MTBase(scope, vendor) + ":group:" + group }

// MTVendors is the set of vendors that have queues in this scope.
func MTVendors(scope string) string { return "queues:" + scope + ":mt:vendors" }

// Sub is the sub-queue a message waits in: its sender when that is one of the
// vendor's short codes (shortCodes, as the vendor lists them), else Common.
// A sender that cannot be part of a key is Common.
func Sub(sender string, shortCodes []string) string {
	for _, sc := range shortCodes {
		if sc == sender && keySafe(sender) {
			return sender
		}
	}
	return Common
}

// keySafe: a short code may be part of a key (and of a MySQL value) as it is.
func keySafe(s string) bool {
	if s == "" || len(s) > 16 || s == Common {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '+', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}

// --- items -----------------------------------------------------------------

// Item is one waiting item as it is kept: <ULID>|<attempt>|<payload>. The id
// first makes items of one priority come out oldest first; the attempt is
// the times it was sent already (a retry puts it back with one more).
func Item(id string, attempt int, payload string) string {
	return id + "|" + strconv.Itoa(attempt) + "|" + payload
}

// ParseItem is Item read back. ok is false for anything not made by Item.
func ParseItem(s string) (id string, attempt int, payload string, ok bool) {
	i := strings.IndexByte(s, '|')
	if i <= 0 {
		return "", 0, "", false
	}
	j := strings.IndexByte(s[i+1:], '|')
	if j < 0 {
		return "", 0, "", false
	}
	n, err := strconv.Atoi(s[i+1 : i+1+j])
	if err != nil || n < 0 {
		return "", 0, "", false
	}
	return s[:i], n, s[i+2+j:], true
}

// --- scores ----------------------------------------------------------------

// Priority is a message's priority as a queue orders it: 1 (first) to 9; an
// unset or unusable one is 9.
func Priority(p int) int {
	if p < 1 || p > 9 {
		return 9
	}
	return p
}

// DelayedScore is an item's score in a delayed set: its pick time in
// milliseconds, times ten, plus its priority, so the move to ready knows the
// priority without reading the item. Exact in a float64 for any date before
// the year 30,000.
func DelayedScore(at time.Time, priority int) float64 {
	return float64(at.UnixMilli()*10 + int64(Priority(priority)))
}

// DueScore is the highest delayed score that is due at now.
func DueScore(now time.Time) float64 { return float64(now.UnixMilli()*10 + 9) }

// --- the split of a rate (issue 22 points 5 and 6) -------------------------

// Mode is how a vendor's rate is split between its queues.
type Mode string

const (
	// Fair: each queue with work gets an equal share; a queue that needs less
	// takes only that, and what it leaves goes to the queues with more.
	// Priority orders only inside each queue.
	Fair Mode = "fair"
	// ByPriority: priority 1 is taken first from all the vendor's queues, then
	// 2, and so on, until the rate is used; a level that holds more than is
	// left is split fairly.
	ByPriority Mode = "priority"
)

// ParseMode reads a vendor's setting; anything else is Fair.
func ParseMode(s string) Mode {
	if Mode(strings.ToLower(strings.TrimSpace(s))) == ByPriority {
		return ByPriority
	}
	return Fair
}

// Waiting is how much one queue has ready: Ready in all, and by priority
// (index 1–9) for the priority split. A store may count up to the amount
// being split and no further.
type Waiting struct {
	Ready  int64
	ByPrio [10]int64
}

// Split shares n between queues: out[i] is how many to take from queue i,
// never more than it has ready. turn rotates the units left over after an
// even split between the queues (pass a counter that grows each tick), so no
// queue is always the one left short.
func Split(n int, qs []Waiting, mode Mode, turn int) []int {
	out := make([]int, len(qs))
	if n <= 0 || len(qs) == 0 {
		return out
	}
	if mode != ByPriority {
		need := make([]int64, len(qs))
		for i, q := range qs {
			need[i] = q.Ready
		}
		fill(out, need, int64(n), turn)
		return out
	}
	left := int64(n)
	for p := 1; p <= 9 && left > 0; p++ {
		need := make([]int64, len(qs))
		var sum int64
		for i, q := range qs {
			// What the levels above took from this queue is not here twice.
			need[i] = q.ByPrio[p]
			sum += need[i]
		}
		if sum == 0 {
			continue
		}
		if sum <= left {
			for i := range qs {
				out[i] += int(need[i])
			}
			left -= sum
			continue
		}
		fill(out, need, left, turn)
		left = 0
	}
	return out
}

// fill adds to out an even share of n between the queues with need, each
// taking at most its need: water filling, then the units left by rounding
// one each, starting at turn.
func fill(out []int, need []int64, n int64, turn int) {
	need = append([]int64(nil), need...)
	for n > 0 {
		var active []int
		for i, v := range need {
			if v > 0 {
				active = append(active, i)
			}
		}
		if len(active) == 0 {
			return
		}
		share := n / int64(len(active))
		if share == 0 {
			// Fewer units than queues: one each, from turn on.
			start := turn % len(active)
			if start < 0 {
				start += len(active)
			}
			for k := 0; k < len(active) && n > 0; k++ {
				i := active[(start+k)%len(active)]
				out[i]++
				need[i]--
				n--
			}
			return
		}
		for _, i := range active {
			give := share
			if need[i] < give {
				give = need[i]
			}
			out[i] += int(give)
			need[i] -= give
			n -= give
		}
	}
}

// turns is each vendor's tick counter, for Split's turn.
var turns sync.Map // vendor -> *atomic.Uint64

// Turn is the next turn of a vendor's split: it grows by one each tick, so
// the units an even split leaves over go round its queues.
func Turn(vendor string) int {
	v, _ := turns.LoadOrStore(vendor, new(atomic.Uint64))
	return int(v.(*atomic.Uint64).Add(1) % 1_000_000)
}
