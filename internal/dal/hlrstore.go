package dal

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// The HLR numbers as internal/hlr keeps them beside the reference cache's
// reads (HLRNumbers, reference.go): a live lookup's answer written to its
// number's shard table, the main Redis's copy of each number read on the
// accept path, and the shard tables of an HLR store on SQL Server.

// HLRShardWriter writes a live lookup's answer into its number's shard
// table, hlr_<shard> (the HLR store: YAKU_HLR_DSN, else the queue database).
type HLRShardWriter interface {
	// PutShard upserts one number into hlr_<shard>; shard is digits only
	// (hlr.Shard), never input. A missing shard table is an error the
	// caller logs. The row's OperatorName and CCC are written when given;
	// "" keeps what the row already holds (6 Nov 2026).
	PutShard(ctx context.Context, shard string, r HLRRow) error
}

// HLRCopies is the main Redis's copy of each number (hlr:<msisdn>), the
// entry as JSON.
type HLRCopies interface {
	// GetHLR reads one number's copy. found=false with a nil error: none.
	GetHLR(ctx context.Context, msisdn string) (raw []byte, found bool, err error)
	// SetHLR writes one number's copy, with no expiry. Best effort.
	SetHLR(ctx context.Context, msisdn string, raw []byte)
}

// HLRShards is an HLR store's shard tables read whole (hlr.DeltaSource): one
// full load at start, then the numbers changed since a moment.
type HLRShards interface {
	// Lookup reads one number from its shard; deleted rows are misses.
	Lookup(msisdn string) (HLRRow, bool)
	// Prepare brings every shard up to the cache's shape. Idempotent.
	Prepare(ctx context.Context) error
	// Count totals the numbers across every shard.
	Count(ctx context.Context) (int64, error)
	// LoadSince reads the numbers changed at or after since (zero = a full
	// load, deleted ones left out) and the newest change seen.
	LoadSince(ctx context.Context, since time.Time) ([]HLRRow, time.Time, error)
	Close() error
}

// hlrDriver is the DAL the HLR store's setting names (config.HLRStore), or
// an error when there is no HLR store.
func hlrDriver(cfg config.Config) (Driver, string, config.Backend, error) {
	b, dsn := cfg.HLRStore()
	if dsn == "" {
		return Driver{}, "", b, fmt.Errorf("dal: no HLR store: %w", ErrNotServed)
	}
	d, ok := Lookup(b)
	if !ok {
		return Driver{}, "", b, fmt.Errorf("dal: no %s DAL in this binary (have %v)", b, Registered())
	}
	return d, dsn, b, nil
}

// OpenHLRShardWriter opens the shard writer on the HLR store.
func OpenHLRShardWriter(cfg config.Config) (HLRShardWriter, error) {
	d, dsn, b, err := hlrDriver(cfg)
	if err != nil {
		return nil, err
	}
	if d.HLRShardWriter == nil {
		return nil, fmt.Errorf("dal: HLR shard writes on %s: %w", b, ErrNotServed)
	}
	w, err := d.HLRShardWriter(dsn)
	if err != nil {
		return nil, fmt.Errorf("dal: HLR shard writes on %s: %w", b, err)
	}
	return w, nil
}

// OpenHLRShards opens the HLR store's shard tables for the cache.
func OpenHLRShards(cfg config.Config) (HLRShards, error) {
	d, dsn, b, err := hlrDriver(cfg)
	if err != nil {
		return nil, err
	}
	if d.HLRShards == nil {
		return nil, fmt.Errorf("dal: HLR shards on %s: %w", b, ErrNotServed)
	}
	s, err := d.HLRShards(dsn)
	if err != nil {
		return nil, fmt.Errorf("dal: HLR shards on %s: %w", b, err)
	}
	return s, nil
}

// OpenHLRCopies opens the numbers' copies on the main Redis (only when
// Redis is set: without it the lookup is the database's alone).
func OpenHLRCopies(cfg config.Config) (HLRCopies, error) {
	if !cfg.RedisEnabled() {
		return nil, fmt.Errorf("dal: HLR copies: %w", ErrNotServed)
	}
	d, ok := Lookup(config.BackendRedis)
	if !ok {
		return nil, fmt.Errorf("dal: no redis DAL in this binary (have %v)", Registered())
	}
	if d.HLRCopies == nil {
		return nil, fmt.Errorf("dal: HLR copies on redis: %w", ErrNotServed)
	}
	c, err := d.HLRCopies(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: HLR copies on redis: %w", err)
	}
	return c, nil
}
