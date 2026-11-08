package redis

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/store"
)

// Queue depth and age for monitoring — store.QueueDepthStats and
// store.PendingCountStats, which until 2026-09-11 only the MySQL store
// implemented.
//
// That gap blanked the queue panel exactly when Redis is gated, which is the
// recommended hot-path configuration: httpserver type-asserts its queue to
// QueueDepthStats, the assertion failed silently, and every snapshot went out
// with no `queues` section. The dashboard read zero depth through a load run
// that had tens of thousands of messages waiting.
//
// Key DISCOVERY is cached. Finding the q:* keys means SCANning the whole
// keyspace — 115k keys on the dev box, most of them per-message payloads — and
// doing that every 5 seconds from every role that publishes would be real
// load for a monitoring feature. Names are rediscovered once a minute; the
// per-key ZCARD and head read every sample are O(1) and O(log n).
//
// AGE is read from the zset score, which is visible-at. A message leased to a
// vendor process has a future score and is deliberately not "waiting"; a
// scheduled message (sendAt in the future) is not waiting either. So age is
// "the longest any claimable message has been claimable" — the stall signal —
// rather than time since submit.

const discoverEvery = time.Minute

type keyDiscovery struct {
	mu   sync.Mutex
	keys map[string][]string // pattern -> keys
	at   map[string]time.Time
	// seen keeps every key a scan has found, so a queue that drained to
	// empty (Redis deletes the zset) and filled again before the next scan
	// is still read — not missing, and so shown as empty, for up to a minute.
	seen map[string]map[string]bool
}

// seenKeysMax bounds the keys remembered per pattern.
const seenKeysMax = 10000

var discovery = &keyDiscovery{keys: map[string][]string{}, at: map[string]time.Time{}, seen: map[string]map[string]bool{}}

// depthLast is the last good QueueDepths answer per queue owner: a scan or
// read that fails reports it marked stale rather than no queues at all,
// which every screen showed as empty (production load test, 2026-10-05).
var depthLast = struct {
	sync.Mutex
	m map[string]map[string]store.VendorQueueStat
}{m: map[string]map[string]store.VendorQueueStat{}}

// discover returns the keys matching pattern, rescanning at most once a minute.
// (No caller is left on the hot or the monitoring path; kept for tools.)
func (s *Store) discover(ctx context.Context, pattern string) ([]string, error) {
	discovery.mu.Lock()
	if at, ok := discovery.at[pattern]; ok && time.Since(at) < discoverEvery {
		k := discovery.keys[pattern]
		discovery.mu.Unlock()
		return k, nil
	}
	discovery.mu.Unlock()
	var keys []string
	iter := s.c.Scan(ctx, 0, pattern, 1000).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	discovery.mu.Lock()
	seen := discovery.seen[pattern]
	if seen == nil {
		seen = map[string]bool{}
		discovery.seen[pattern] = seen
	}
	found := map[string]bool{}
	for _, k := range keys {
		found[k] = true
		if len(seen) < seenKeysMax {
			seen[k] = true
		}
	}
	for k := range seen {
		if !found[k] {
			keys = append(keys, k) // ZCARD of a missing key is 0: harmless
		}
	}
	discovery.keys[pattern], discovery.at[pattern] = keys, time.Now()
	discovery.mu.Unlock()
	return keys, nil
}

// zsetStats reads depth, ready count and oldest-claimable age for each key in
// one pipeline — and, for a vendor queue (q:…), how many of its messages were
// taken before (the attempts hash keeps one field per such message).
func (s *Store) zsetStats(ctx context.Context, keys []string) ([]store.VendorQueueStat, error) {
	now := time.Now()
	nowStr := strconv.FormatInt(now.Unix(), 10)
	// A score carries priority/1000 as a fraction, so "due by now" is every
	// score below the next whole second.
	dueStr := strconv.FormatFloat(float64(now.Unix())+0.9999, 'f', 4, 64)
	pipe := s.c.Pipeline()
	cards := make([]*redis.IntCmd, len(keys))
	readys := make([]*redis.IntCmd, len(keys))
	tried := make([]*redis.IntCmd, len(keys))
	heads := make([]*redis.ZSliceCmd, len(keys))
	for i, k := range keys {
		cards[i] = pipe.ZCard(ctx, k)
		readys[i] = pipe.ZCount(ctx, k, "-inf", dueStr)
		heads[i] = pipe.ZRangeByScoreWithScores(ctx, k, &redis.ZRangeBy{Min: "-inf", Max: nowStr, Count: 1})
		if qn, ok := strings.CutPrefix(k, "q:"); ok {
			tried[i] = pipe.HLen(ctx, attKey(qn))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	out := make([]store.VendorQueueStat, len(keys))
	for i := range keys {
		st := store.VendorQueueStat{Depth: cards[i].Val(), Ready: readys[i].Val()}
		st.Held = st.Depth - st.Ready
		if tried[i] != nil {
			st.Tried = tried[i].Val()
		}
		if h := heads[i].Val(); len(h) > 0 {
			// The score carries priority/1000 as a fraction; the whole
			// seconds are the visible-at time.
			if age := now.Unix() - int64(math.Floor(h[0].Score)); age > 0 {
				st.OldestAgeSec = age
			}
		}
		out[i] = st
	}
	return out, nil
}

// QueueDepths reports every vendor queue's depth and oldest claimable age.
func (s *Store) QueueDepths(ctx context.Context) (map[string]store.VendorQueueStat, error) {
	out, err := s.queueDepths(ctx)
	depthLast.Lock()
	defer depthLast.Unlock()
	if err != nil {
		last, ok := depthLast.m[s.owner]
		if !ok {
			return nil, err
		}
		stale := make(map[string]store.VendorQueueStat, len(last))
		for v, st := range last {
			st.Stale = true
			stale[v] = st
		}
		return stale, nil
	}
	depthLast.m[s.owner] = out
	return out, nil
}

// customerQueues are the receipt (or MO) queues this process drains — its own
// and the shared ones (D-121) — with the account each belongs to. They are
// found through the account sets every enqueue adds to (cdlr:accts, mo:accts),
// never by scanning key names: with four million keys in Redis that scan
// overran the monitoring budget, the read failed, and the busiest queue on the
// box — 2.7 million receipts waiting for an account with no bind — was shown
// as empty and "flowing" (2026-10-07).
func (s *Store) customerQueues(ctx context.Context, kind string) (keys []string, accts []int64, err error) {
	setKey, queueKey := cdlrAcctsKey, cdlrAcctKey
	if kind == "mo" {
		setKey, queueKey = moAcctsKey, moAcctKey
	}
	for _, owner := range s.myOwners() {
		members, err := s.c.SMembers(ctx, setKey(owner)).Result()
		if err != nil {
			return nil, nil, err
		}
		for _, m := range members {
			a, err := strconv.ParseInt(m, 10, 64)
			if err != nil {
				continue
			}
			keys = append(keys, queueKey(a, owner))
			accts = append(accts, a)
		}
	}
	return keys, accts, nil
}

// PendingCounts reports the client-push backlogs: receipts (cdlr:acct:*) and
// MO (mo:acct:*) waiting for a customer's bind or URL.
//
// An entry whose job payload has gone is still counted until the give-up
// sweep takes it (it removes such entries since 2026-10-07): a count of live
// payloads would mean a read per member.
func (s *Store) PendingCounts(ctx context.Context) (dlr, mo store.VendorQueueStat, err error) {
	sum := func(kind string) (store.VendorQueueStat, error) {
		keys, _, err := s.customerQueues(ctx, kind)
		if err != nil {
			return store.VendorQueueStat{}, err
		}
		stats, err := s.zsetStats(ctx, keys)
		if err != nil {
			return store.VendorQueueStat{}, err
		}
		var total store.VendorQueueStat
		for _, st := range stats {
			total.Depth += st.Depth
			if st.OldestAgeSec > total.OldestAgeSec {
				total.OldestAgeSec = st.OldestAgeSec
			}
		}
		return total, nil
	}
	if dlr, err = sum("dlr"); err != nil {
		return
	}
	mo, err = sum("mo")
	return
}

// PendingByAccount is PendingCounts per account (store.PendingByAccountStats,
// D-127), largest first, at most limit of each.
func (s *Store) PendingByAccount(ctx context.Context, kind string, limit int) (map[int64]store.VendorQueueStat, error) {
	if limit <= 0 {
		limit = 200
	}
	keys, accts, err := s.customerQueues(ctx, kind)
	if err != nil {
		return nil, err
	}
	stats, err := s.zsetStats(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := map[int64]store.VendorQueueStat{}
	for i, acct := range accts {
		if stats[i].Depth == 0 {
			continue
		}
		st := out[acct]
		st.Depth += stats[i].Depth
		st.Ready += stats[i].Ready
		st.Held += stats[i].Held
		if stats[i].OldestAgeSec > st.OldestAgeSec {
			st.OldestAgeSec = stats[i].OldestAgeSec
		}
		out[acct] = st
	}
	if len(out) > limit { // keep the largest
		ids := make([]int64, 0, len(out))
		for id := range out {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(a, b int) bool { return out[ids[a]].Depth > out[ids[b]].Depth })
		for _, id := range ids[limit:] {
			delete(out, id)
		}
	}
	return out, nil
}

// globEscape makes a name safe inside a SCAN pattern.
func globEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '*', '?', '[', ']', '\\':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}
