package hlr

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// RedisStore is the HLR backend under the YAKU_REDIS_ADDR hot-path gate
// (2026-07-24, PO decision): HLR lives ONLY in Redis — one hlr:<msisdn> key
// per record, maintained by refdata.Syncer's 30s delta sync from MySQL.
//
// Saleem 2026-10-14, again: with Redis set the lookup is Redis only, without
// it the database only — never both, no fallback from one to the other.
//
// Kept in process memory for memoryFor (Saleem, 29 Oct 2026: "loaded once,
// kept 30 s"): a number looked up again within 30 s, found or not, costs no
// Redis call. It was a GET on the main Redis for every message. What it
// costs: a change to a number reaches the accept path up to 30 s later
// (the MySQL->Redis sync already lags up to 30 s).
//
// The GET and SET are the DAL's (dal.HLRCopies, internal/dal/redis/hlr.go,
// moved there on 29 Oct 2026): one command each, as before.
//
// A number Redis does not hold is read from our HLR data in the database
// before anything else (SetDatabase; Saleem, 6 Nov 2026: "yes, now lookup is
// either new number or expiry"): a copy lost from Redis — one key evicted or
// deleted, a Redis rebuilt — used to send the number to a paid live lookup
// although the database had it. Found there, it is put back in Redis. This
// replaces "Redis only, never both" (2026-10-14) for the miss only: a number
// Redis holds is never read from the database.
type RedisStore struct {
	c  dal.HLRCopies
	db Store // our HLR data in the database (hlr_<first4>); nil = Redis only

	mu        sync.Mutex
	mem       map[string]memEntry
	lastSweep time.Time
}

type memEntry struct {
	e     Entry
	found bool
	until time.Time
}

const (
	// memoryFor is how long a lookup is kept in memory.
	memoryFor = 30 * time.Second
	// memoryMost bounds the numbers kept: past it, the expired ones go, and
	// if that is not enough nothing more is kept until they do.
	memoryMost = 200_000
)

// NewRedisStore wraps the numbers' copies in the main Redis (a role builds
// them on its shared client: redis.NewHLRCopies) as an hlr.Store.
func NewRedisStore(c dal.HLRCopies) *RedisStore {
	return &RedisStore{c: c, mem: map[string]memEntry{}}
}

const lookupTimeout = 2 * time.Second

// dbTimeout bounds the database read of a number Redis lacks: past it the
// number counts as not found (the next step of the account's order runs), so
// a slow database never holds a message longer than this.
const dbTimeout = 500 * time.Millisecond

// SetDatabase sets our HLR data in the database as the place a number Redis
// does not hold is read from (6 Nov 2026). nil = Redis only.
func (s *RedisStore) SetDatabase(db Store) { s.db = db }

// fromDatabase reads msisdn from the database within dbTimeout.
func (s *RedisStore) fromDatabase(msisdn string) (Entry, bool) {
	if s.db == nil {
		return Entry{}, false
	}
	type got struct {
		e  Entry
		ok bool
	}
	ch := make(chan got, 1)
	go func() {
		e, ok := s.db.Lookup(msisdn)
		ch <- got{e, ok}
	}()
	select {
	case g := <-ch:
		if g.ok && !g.e.Deleted {
			g.e.MSISDN = msisdn
			return g.e, true
		}
		return Entry{}, false
	case <-time.After(dbTimeout):
		return Entry{}, false
	}
}

// Lookup fetches one entry: from memory when it was looked up within
// memoryFor, else from Redis, else from the database (SetDatabase). A miss in
// all of them, or an error, reads as "not found" —
// the HLR stage's both-miss contract is proceed-without-network-info, so a
// transient Redis blip degrades routing precision, never availability. An
// error is not kept; a miss is.
func (s *RedisStore) Lookup(msisdn string) (Entry, bool) {
	now := time.Now()
	s.mu.Lock()
	if m, ok := s.mem[msisdn]; ok && now.Before(m.until) {
		s.mu.Unlock()
		return m.e, m.found
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()
	raw, found, err := s.c.GetHLR(ctx, msisdn)
	if err != nil || !found {
		// Not in Redis (or Redis did not answer): our HLR data in the
		// database, before any paid lookup (6 Nov 2026). Found there, the
		// copy goes back in Redis (when Redis answered). Kept in memory
		// either way, so a number is read from the database at most once
		// every memoryFor.
		if e, ok := s.fromDatabase(msisdn); ok {
			s.keep(msisdn, e, true, now)
			if err == nil {
				if b, merr := json.Marshal(e); merr == nil {
					s.c.SetHLR(ctx, msisdn, b)
				}
			}
			return e, true
		}
		if err != nil && s.db == nil {
			return Entry{}, false // an error is not kept
		}
		s.keep(msisdn, Entry{}, false, now)
		return Entry{}, false
	}
	var e Entry
	if json.Unmarshal(raw, &e) != nil || e.Deleted {
		s.keep(msisdn, Entry{}, false, now)
		return Entry{}, false
	}
	s.keep(msisdn, e, true, now)
	return e, true
}

// keep remembers a lookup for memoryFor.
func (s *RedisStore) keep(msisdn string, e Entry, found bool, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mem == nil {
		s.mem = map[string]memEntry{}
	}
	if len(s.mem) >= memoryMost && now.Sub(s.lastSweep) >= time.Second {
		s.lastSweep = now
		for k, m := range s.mem {
			if !now.Before(m.until) {
				delete(s.mem, k)
			}
		}
	}
	if _, had := s.mem[msisdn]; !had && len(s.mem) >= memoryMost {
		return
	}
	s.mem[msisdn] = memEntry{e: e, found: found, until: now.Add(memoryFor)}
}

// Put writes a dipped entry so the very next Lookup is an immediate hit. On the
// Redis hot-path backend the HLR stage reads Redis directly, so without this a
// live dip (which persists to MySQL hlr_<shard>) would not be visible until the
// 30s MySQL->Redis sync — meaning a burst of MT to a new number re-dips the
// provider until the sync catches up. This keeps the hot copy current NOW; the
// syncer reconciles it thereafter. No TTL (the synced records carry none).
// Satisfies hlr.Writer, so the dipper fans out to it alongside the persister.
// The memory copy is updated too, so this process does not read its own
// miss back.
func (s *RedisStore) Put(e Entry) {
	if e.MSISDN == "" {
		return
	}
	s.keep(e.MSISDN, e, !e.Deleted, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()
	if b, err := json.Marshal(e); err == nil {
		s.c.SetHLR(ctx, e.MSISDN, b)
	}
}
