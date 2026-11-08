package redis

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
)

// CampaignWindow is the live campaign window in the main Redis
// (dal.CampaignWindow), moved from internal/campaignwin on 29 Oct 2026:
// cw:<hash>:<bucket>, a hash of account id to messages.
type CampaignWindow struct{ c *redis.Client }

// NewCampaignWindow keeps the window on a Redis client.
func NewCampaignWindow(c *redis.Client) *CampaignWindow { return &CampaignWindow{c: c} }

func campaignKey(hash string, b int64) string { return "cw:" + hash + ":" + strconv.FormatInt(b, 10) }

// AddCampaignCounts adds a flush in one pipeline: the counts, then an
// expiry on each bucket written.
func (w *CampaignWindow) AddCampaignCounts(ctx context.Context, counts []dal.CampaignCount, keep time.Duration) error {
	pipe := w.c.Pipeline()
	keys := map[string]bool{}
	for _, k := range counts {
		rk := campaignKey(k.Hash, k.Bucket)
		pipe.HIncrBy(ctx, rk, strconv.FormatInt(k.Account, 10), int64(k.N))
		keys[rk] = true
	}
	for rk := range keys {
		pipe.Expire(ctx, rk, keep)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// CampaignBuckets reads one signature's buckets in one pipeline.
func (w *CampaignWindow) CampaignBuckets(ctx context.Context, hash string, buckets []int64) ([]map[int64]int, error) {
	pipe := w.c.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, 0, len(buckets))
	for _, b := range buckets {
		cmds = append(cmds, pipe.HGetAll(ctx, campaignKey(hash, b)))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	out := make([]map[int64]int, len(cmds))
	for i, cmd := range cmds {
		m := map[int64]int{}
		for f, v := range cmd.Val() {
			id, err1 := strconv.ParseInt(f, 10, 64)
			cnt, err2 := strconv.Atoi(v)
			if err1 != nil || err2 != nil {
				continue
			}
			m[id] += cnt
		}
		out[i] = m
	}
	return out, nil
}
