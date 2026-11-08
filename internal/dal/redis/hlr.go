package redis

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// The numbers' copies in the main Redis, one hlr:<msisdn> key per number,
// kept by refdata.Syncer's 30 s delta sync from MySQL and by every live dip
// — moved here unchanged on 29 Oct 2026 (the DAL plan) from
// internal/hlr/redis.go. The 30 s memory in front of it stays in internal/hlr.

// openHLRCopies is the copies' opener: the main Redis (FromConfig).
func openHLRCopies(cfg config.Config) (dal.HLRCopies, error) {
	return NewHLRCopies(FromConfig(cfg).Client()), nil
}

// HLRCopies is dal.HLRCopies on one Redis client.
type HLRCopies struct{ c *redis.Client }

var _ dal.HLRCopies = (*HLRCopies)(nil)

// NewHLRCopies wraps a shared Redis client a role already has.
func NewHLRCopies(c *redis.Client) *HLRCopies { return &HLRCopies{c: c} }

// GetHLR reads one number's copy: one GET.
func (h *HLRCopies) GetHLR(ctx context.Context, msisdn string) ([]byte, bool, error) {
	raw, err := h.c.Get(ctx, "hlr:"+msisdn).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

// SetHLR writes one number's copy with no TTL (the synced records carry
// none): one SET, its answer not waited on beyond the call.
func (h *HLRCopies) SetHLR(ctx context.Context, msisdn string, raw []byte) {
	h.c.Set(ctx, "hlr:"+msisdn, raw, 0)
}
