package redis

import (
	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// OpenCounters keeps every shared counter and block on ONE client of the
// main Redis (never the queues' own: a counter belongs with the other
// counters), as the accept roles did with their one `rc`.
func OpenCounters(cfg config.Config) *dal.Counters {
	return CountersOn(FromConfig(cfg).Client())
}

// CountersOn keeps them on a client the caller already has (tests).
func CountersOn(c *redis.Client) *dal.Counters {
	return &dal.Counters{
		Limits:    NewLimits(c),
		Usage:     NewUsage(c),
		Blocks:    NewBlocks(c),
		Dedupe:    NewDedupe(c),
		Campaigns: NewCampaignWindow(c),
		Abuse:     NewAbuseCounts(c),
		Conn:      c,
	}
}

var (
	_ dal.Limits         = (*Limits)(nil)
	_ dal.Usage          = (*Usage)(nil)
	_ dal.Blocks         = (*Blocks)(nil)
	_ dal.Dedupe         = (*Dedupe)(nil)
	_ dal.CampaignWindow = (*CampaignWindow)(nil)
	_ dal.AbuseCounts    = (*AbuseCounts)(nil)
)
