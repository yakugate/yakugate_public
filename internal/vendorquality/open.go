package vendorquality

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// Store is both halves: counting processes add to it, routing processes read.
type Store interface {
	Sink
	Source
}

// Open returns the store every process of one deployment agrees on: Redis
// when Redis is configured, else the hot store's vendor_quality_minute when
// the hot store is MySQL, else nil (quality routing then has no counts, and a
// quality level behaves as list order). Where the counts are kept is the
// DAL's (dal.OpenVendorQuality; internal/dal/redis/quality.go, the vq:<minute>
// hashes, and internal/dal/mysql/traffic.go).
func Open(cfg config.Config) Store {
	q, err := dal.OpenVendorQuality(cfg)
	if err != nil || q == nil {
		return nil
	}
	return OnDAL(q)
}

// OnDAL is the store kept by a DAL. nil = nil store.
func OnDAL(q dal.VendorQuality) Store {
	if q == nil {
		return nil
	}
	return dalStore{q}
}

// dalStore turns this package's buckets and counts into the DAL's.
type dalStore struct{ q dal.VendorQuality }

// Add implements Sink.
func (s dalStore) Add(ctx context.Context, bs []Bucket) error {
	out := make([]dal.QualityBucket, len(bs))
	for i, b := range bs {
		out[i] = dal.QualityBucket{Minute: b.Minute, Key: dal.QualityKey(b.Key), Counts: dal.QualityCounts(b.Counts)}
	}
	return s.q.Add(ctx, out)
}

// Load implements Source.
func (s dalStore) Load(ctx context.Context, window time.Duration) (map[Key]Counts, error) {
	got, err := s.q.Load(ctx, window)
	if err != nil {
		return nil, err
	}
	out := make(map[Key]Counts, len(got))
	for k, c := range got {
		out[Key(k)] = Counts(c)
	}
	return out, nil
}

// StartCounting turns this process's Default counter on against the
// deployment's store. Called once by every role that sees verdicts.
func StartCounting(ctx context.Context, cfg config.Config, logf func(string, ...any)) {
	if s := Open(cfg); s != nil {
		Default.Start(ctx, s, logf)
	}
}
