package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// What a Redis says about itself, for monitoring (internal/storewatch, D-129)
// — moved here unchanged on 29 Oct 2026 (the DAL plan): PING, INFO, and what
// its Sentinels know of the master.

// ServerProbe is one Redis the gateway uses, on a client of its own, and the
// Sentinels the settings name.
type ServerProbe struct {
	client    *redis.Client
	sentinels []*redis.SentinelClient
}

var _ dal.RedisServerProbe = (*ServerProbe)(nil)

// openServerProbe opens the Redis c names (FromConfig) and, under Sentinel,
// a client for each Sentinel.
func openServerProbe(c config.Config) (dal.RedisServerProbe, error) {
	p := &ServerProbe{client: FromConfig(c).Client()}
	if c.RedisSentinel() {
		for _, a := range c.RedisSentinelAddrs {
			p.sentinels = append(p.sentinels, redis.NewSentinelClient(&redis.Options{Addr: a, Password: c.RedisSentinelPassword,
				DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second}))
		}
	}
	return p, nil
}

func (p *ServerProbe) Ping(ctx context.Context) error           { return p.client.Ping(ctx).Err() }
func (p *ServerProbe) Info(ctx context.Context) (string, error) { return p.client.Info(ctx).Result() }
func (p *ServerProbe) Addr() string                             { return p.client.Options().Addr }

// Sentinels are the Sentinel clients, in the settings' order.
func (p *ServerProbe) Sentinels() []dal.RedisSentinelProbe {
	out := make([]dal.RedisSentinelProbe, len(p.sentinels))
	for i, s := range p.sentinels {
		out[i] = sentinelProbe{s}
	}
	return out
}

// Close closes the client and the Sentinels'.
func (p *ServerProbe) Close() error {
	if p.client != nil {
		_ = p.client.Close()
	}
	for _, s := range p.sentinels {
		_ = s.Close()
	}
	return nil
}

type sentinelProbe struct{ c *redis.SentinelClient }

func (s sentinelProbe) Master(ctx context.Context, name string) (map[string]string, error) {
	return s.c.Master(ctx, name).Result()
}
