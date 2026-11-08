// Redis reference snapshot (2026-07-24, PO decision) — the distribution layer
// between MySQL and every gateway instance's in-memory cache when the
// YAKU_REDIS_ADDR hot-path gate is on:
//
//	MySQL --(elected syncer, every 30s)--> ref:snapshot in Redis
//	Redis --(every instance, ~3s version watch)--> refdata.Mem (in-memory)
//
// Hot-path reads STAY in-process (the same Mem maps as ungated mode — no
// per-request Redis round trips for reference data); Redis is where the data
// LIVES between MySQL and the fleet. One instance at a time holds the sync
// lease, so MySQL sees one reference reader regardless of fleet size, and
// POST /admin/cache/refresh on ANY instance force-syncs then bumps
// ref:version — every other instance converges within the watch interval.
//
// HLR is deliberately NOT part of this snapshot: per the same PO decision it
// lives ONLY in Redis (hlr:<msisdn> keys, maintained by SyncHLR below) and is
// read per-lookup by internal/hlr.RedisStore, never held in process memory.
package refdata

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/hlr"
)

// The keys (ref:snapshot, ref:version, ref:synced_at, ref:sync:lock, hlr:<msisdn>,
// hlr:synced_at) and the commands on them are the DAL's
// (internal/dal/redis/reference_snapshots.go, since 29 Oct 2026).

// Snapshot is the full reference dataset as one JSON document — written
// atomically (single SET), versioned, so a reader can never observe half a
// sync.
type Snapshot struct {
	Accounts     []Account                        `json:"accounts"`
	Routes       []Route                          `json:"routes"`
	AutoReplace  map[int64][]AutoReplaceRule      `json:"auto_replace"`
	Spam         SpamLists                        `json:"spam"`
	Templates    map[int64][]TemplateRegex        `json:"templates"`
	Blocklist    []Block                          `json:"blocklist"`
	CountryRules map[int64]map[string]CountryRule `json:"country_rules"`
	Revocations  map[string]bool                  `json:"revocations"`
	Rates        map[int64][]RateRow              `json:"rates"`
}

// Syncer owns the MySQL -> Redis leg: reference snapshot + HLR delta.
type Syncer struct {
	src  Loader
	hlr  hlr.DeltaSource // nil = HLR sync disabled
	rc   dal.ReferenceSnapshots
	id   string // lease holder identity (host+pid)
	logf func(string, ...any)
}

// NewSyncer builds the MySQL->Redis syncer. src is the MYSQL loader (never
// the Redis one — that would sync Redis to itself).
func NewSyncer(src Loader, hlrSrc hlr.DeltaSource, rc dal.ReferenceSnapshots, logf func(string, ...any)) *Syncer {
	host, _ := os.Hostname()
	return &Syncer{src: src, hlr: hlrSrc, rc: rc,
		id: fmt.Sprintf("%s:%d", host, os.Getpid()), logf: logf}
}

// Start runs the sync loop: every interval, try to take (or renew) the fleet
// lease; the holder syncs, everyone else stays idle. Lease TTL is 1.5x the
// interval so a crashed holder is replaced within one cycle.
func (s *Syncer) Start(ctx context.Context, every time.Duration) {
	ttl := every + every/2
	t := time.NewTicker(every)
	defer t.Stop()
	// immediate first attempt so a fresh deployment has a snapshot right away
	s.tick(ctx, ttl)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tick(ctx, ttl)
		}
	}
}

func (s *Syncer) tick(ctx context.Context, ttl time.Duration) {
	if !s.acquireLease(ctx, ttl) {
		return
	}
	if err := s.SyncRef(ctx); err != nil && s.logf != nil {
		s.logf("redis ref sync: %v", err)
	}
	if err := s.SyncHLR(ctx); err != nil && s.logf != nil {
		s.logf("redis hlr sync: %v", err)
	}
}

// acquireLease takes the fleet-wide sync lease, or renews it when this
// instance already holds it.
func (s *Syncer) acquireLease(ctx context.Context, ttl time.Duration) bool {
	return s.rc.TakeLease(ctx, s.id, ttl)
}

// SyncRef performs one MySQL -> Redis reference sync and bumps ref:version.
// Exported (lease NOT checked) for the refresh URL: an operator's explicit
// refresh must sync now, whichever instance received it.
func (s *Syncer) SyncRef(ctx context.Context) error {
	snap := Snapshot{}
	var err error
	if snap.Accounts, err = s.src.LoadAccounts(ctx); err != nil {
		return fmt.Errorf("accounts: %w", err)
	}
	if snap.Routes, err = s.src.LoadRoutes(ctx); err != nil {
		return fmt.Errorf("routes: %w", err)
	}
	if snap.AutoReplace, err = s.src.LoadAutoReplace(ctx); err != nil {
		return fmt.Errorf("auto_replace: %w", err)
	}
	if snap.Spam, err = s.src.LoadSpam(ctx); err != nil {
		return fmt.Errorf("spam: %w", err)
	}
	if snap.Templates, err = s.src.LoadTemplates(ctx); err != nil {
		return fmt.Errorf("templates: %w", err)
	}
	if snap.Blocklist, err = s.src.LoadBlocklist(ctx); err != nil {
		return fmt.Errorf("blocklist: %w", err)
	}
	if snap.CountryRules, err = s.src.LoadCountryRules(ctx); err != nil {
		return fmt.Errorf("country_rules: %w", err)
	}
	if snap.Revocations, err = s.src.LoadRevocations(ctx); err != nil {
		return fmt.Errorf("revocations: %w", err)
	}
	if snap.Rates, err = s.src.LoadRates(ctx); err != nil {
		return fmt.Errorf("rates: %w", err)
	}
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return s.rc.Publish(ctx, b)
}

// SyncHLR copies changed HLR entries into per-msisdn Redis keys — full load
// on the first run (empty high-water mark), delta on last_check after that,
// mirroring hlr.Cache's own sync strategy. Deleted rows are removed. No-op
// when no HLR source was wired.
func (s *Syncer) SyncHLR(ctx context.Context) error {
	if s.hlr == nil {
		return nil
	}
	var since time.Time
	if raw, err := s.rc.HLRMark(ctx); err == nil && raw != "" {
		since, _ = time.Parse(time.RFC3339Nano, raw)
	}
	if since.IsZero() {
		if err := s.hlr.Prepare(ctx); err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
	}
	entries, maxSeen, err := s.hlr.LoadSince(ctx, since)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	puts := make([]dal.HLRPut, 0, len(entries))
	for _, e := range entries {
		if e.Deleted {
			puts = append(puts, dal.HLRPut{MSISDN: e.MSISDN, Delete: true})
			continue
		}
		b, err := json.Marshal(e)
		if err != nil {
			continue
		}
		puts = append(puts, dal.HLRPut{MSISDN: e.MSISDN, JSON: b})
	}
	if err := s.rc.PutHLR(ctx, puts, maxSeen.UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if s.logf != nil {
		s.logf("hlr -> redis: %d entr(ies) synced (since %s)", len(entries), since.Format(time.RFC3339))
	}
	return nil
}

// RedisLoader implements Loader against the Redis snapshot — what Mem
// hydrates from when the gate is on. The blob is fetched at most once per
// version: Mem.Refresh calls all nine LoadX methods back-to-back, and each
// serves from the same fetched Snapshot.
type RedisLoader struct {
	rc dal.ReferenceSnapshots

	mu      sync.Mutex
	snap    *Snapshot
	version int64 // ref:version the cached snap was fetched at
}

// NewRedisLoader builds a snapshot-backed Loader.
func NewRedisLoader(rc dal.ReferenceSnapshots) *RedisLoader { return &RedisLoader{rc: rc} }

// Version returns the current fleet-wide snapshot version (0 = no snapshot
// published yet).
func (l *RedisLoader) Version(ctx context.Context) (int64, error) {
	return l.rc.Version(ctx)
}

// current returns the snapshot for the CURRENT ref:version, refetching only
// when the version moved since the last fetch.
func (l *RedisLoader) current(ctx context.Context) (*Snapshot, error) {
	v, err := l.Version(ctx)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.snap != nil && l.version == v {
		return l.snap, nil
	}
	raw, err := l.rc.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return nil, err
	}
	l.snap, l.version = &snap, v
	return l.snap, nil
}

func (l *RedisLoader) LoadAccounts(ctx context.Context) ([]Account, error) {
	s, err := l.current(ctx)
	if err != nil {
		return nil, err
	}
	return s.Accounts, nil
}

func (l *RedisLoader) LoadRoutes(ctx context.Context) ([]Route, error) {
	s, err := l.current(ctx)
	if err != nil {
		return nil, err
	}
	return s.Routes, nil
}

func (l *RedisLoader) LoadAutoReplace(ctx context.Context) (map[int64][]AutoReplaceRule, error) {
	s, err := l.current(ctx)
	if err != nil {
		return nil, err
	}
	return s.AutoReplace, nil
}

func (l *RedisLoader) LoadSpam(ctx context.Context) (SpamLists, error) {
	s, err := l.current(ctx)
	if err != nil {
		return SpamLists{}, err
	}
	return s.Spam, nil
}

func (l *RedisLoader) LoadTemplates(ctx context.Context) (map[int64][]TemplateRegex, error) {
	s, err := l.current(ctx)
	if err != nil {
		return nil, err
	}
	return s.Templates, nil
}

func (l *RedisLoader) LoadBlocklist(ctx context.Context) ([]Block, error) {
	s, err := l.current(ctx)
	if err != nil {
		return nil, err
	}
	return s.Blocklist, nil
}

func (l *RedisLoader) LoadCountryRules(ctx context.Context) (map[int64]map[string]CountryRule, error) {
	s, err := l.current(ctx)
	if err != nil {
		return nil, err
	}
	return s.CountryRules, nil
}

func (l *RedisLoader) LoadRevocations(ctx context.Context) (map[string]bool, error) {
	s, err := l.current(ctx)
	if err != nil {
		return nil, err
	}
	return s.Revocations, nil
}

func (l *RedisLoader) LoadRates(ctx context.Context) (map[int64][]RateRow, error) {
	s, err := l.current(ctx)
	if err != nil {
		return nil, err
	}
	return s.Rates, nil
}

// StartRedisWatch keeps Mem in step with the Redis snapshot: every interval
// (the "memory refreshes from redis every 3 seconds" leg) it checks
// ref:version and reloads ONLY when the version moved — so a refresh forced
// anywhere in the fleet lands here within one interval, while quiet cycles
// cost a single GET. Replaces StartRefresh when the gate is on.
func (m *Mem) StartRedisWatch(ctx context.Context, l *RedisLoader, every time.Duration, logf func(string, ...any)) {
	var lastSeen int64 = -1 // force the first reload
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			v, err := l.Version(ctx)
			if err != nil {
				if logf != nil {
					logf("redis version check: %v", err)
				}
				continue
			}
			if v == lastSeen || v == 0 {
				continue
			}
			if err := m.Refresh(ctx); err != nil {
				if logf != nil {
					logf("reference reload from redis: %v", err)
				}
				continue
			}
			lastSeen = v
		}
	}
}

// SnapshotVersion is a helper for the refresh URL's response payload.
func SnapshotVersion(ctx context.Context, rc dal.ReferenceSnapshots) string {
	return rc.VersionText(ctx)
}
