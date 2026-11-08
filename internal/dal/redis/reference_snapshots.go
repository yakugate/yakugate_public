package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
)

// The reference snapshot in Redis (dal.ReferenceSnapshots), moved here from
// internal/refdata/snapshot.go on 29 Oct 2026 with the same keys and the same
// commands. What goes into the snapshot, and when, stayed in refdata.

const (
	snapshotKey = "ref:snapshot"
	versionKey  = "ref:version"
	syncedAtKey = "ref:synced_at"
	syncLockKey = "ref:sync:lock"

	hlrKeyPrefix   = "hlr:"
	hlrSyncedAtKey = "hlr:synced_at" // RFC3339Nano high-water mark of last_check
)

// ReferenceSnapshots is the reference snapshot on one Redis client.
type ReferenceSnapshots struct {
	rc *redis.Client
}

// NewReferenceSnapshots works on rc (the main Redis).
func NewReferenceSnapshots(rc *redis.Client) *ReferenceSnapshots {
	return &ReferenceSnapshots{rc: rc}
}

var _ dal.ReferenceSnapshots = (*ReferenceSnapshots)(nil)

// TakeLease takes the fleet-wide sync lease, or renews it when holder
// already holds it.
func (s *ReferenceSnapshots) TakeLease(ctx context.Context, holder string, ttl time.Duration) bool {
	ok, err := s.rc.SetNX(ctx, syncLockKey, holder, ttl).Result()
	if err != nil {
		return false
	}
	if ok {
		return true
	}
	h, err := s.rc.Get(ctx, syncLockKey).Result()
	if err == nil && h == holder {
		s.rc.Expire(ctx, syncLockKey, ttl) // renew own lease
		return true
	}
	return false
}

// Publish writes the snapshot, bumps ref:version and stamps ref:synced_at in
// one transaction.
func (s *ReferenceSnapshots) Publish(ctx context.Context, snapshot []byte) error {
	pipe := s.rc.TxPipeline()
	pipe.Set(ctx, snapshotKey, snapshot, 0)
	pipe.Incr(ctx, versionKey)
	pipe.Set(ctx, syncedAtKey, time.Now().UTC().Format(time.RFC3339), 0)
	_, err := pipe.Exec(ctx)
	return err
}

// Version returns the current fleet-wide snapshot version (0 = no snapshot
// published yet).
func (s *ReferenceSnapshots) Version(ctx context.Context) (int64, error) {
	v, err := s.rc.Get(ctx, versionKey).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return v, err
}

// VersionText is the version for the refresh URL's response payload.
func (s *ReferenceSnapshots) VersionText(ctx context.Context) string {
	v, err := s.rc.Get(ctx, versionKey).Result()
	if err != nil {
		return "0"
	}
	if _, convErr := strconv.ParseInt(v, 10, 64); convErr != nil {
		return "0"
	}
	return v
}

// Snapshot is the published snapshot.
func (s *ReferenceSnapshots) Snapshot(ctx context.Context) ([]byte, error) {
	raw, err := s.rc.Get(ctx, snapshotKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("no reference snapshot in redis yet (syncer not run?)")
		}
		return nil, err
	}
	return raw, nil
}

// HLRMark is the high-water mark of the last HLR copy ("" = none yet).
func (s *ReferenceSnapshots) HLRMark(ctx context.Context) (string, error) {
	return s.rc.Get(ctx, hlrSyncedAtKey).Result()
}

// PutHLR copies numbers into their hlr:<msisdn> keys (a deleted one is
// removed) and moves the mark, in one pipeline.
func (s *ReferenceSnapshots) PutHLR(ctx context.Context, puts []dal.HLRPut, mark string) error {
	pipe := s.rc.Pipeline()
	for _, p := range puts {
		key := hlrKeyPrefix + p.MSISDN
		if p.Delete {
			pipe.Del(ctx, key)
			continue
		}
		pipe.Set(ctx, key, p.JSON, 0)
	}
	pipe.Set(ctx, hlrSyncedAtKey, mark, 0)
	_, err := pipe.Exec(ctx)
	return err
}
