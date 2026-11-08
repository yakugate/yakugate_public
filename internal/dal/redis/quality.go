package redis

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// openVendorQuality is the counts' opener: the main Redis.
func openVendorQuality(cfg config.Config) (dal.VendorQuality, error) {
	return NewVendorQuality(FromConfig(cfg).Client()), nil
}

// VendorQuality keeps one hash per minute, vq:<unix minute>, with two fields
// per vendor and network: "<vendor>|<mcc>|<mnc>|d" (delivered) and "...|j"
// (judged). Every writer adds with HINCRBY, so any number of processes merge
// by themselves; each hash expires two hours after its last write.
// (internal/vendorquality's RedisStore, moved here unchanged on 29 Oct 2026.)
type VendorQuality struct{ c *redis.Client }

var _ dal.VendorQuality = (*VendorQuality)(nil)

// NewVendorQuality wraps a client. nil client = nil store.
func NewVendorQuality(c *redis.Client) *VendorQuality {
	if c == nil {
		return nil
	}
	return &VendorQuality{c: c}
}

func qualityMinuteHash(minute int64) string { return "vq:" + strconv.FormatInt(minute, 10) }

func qualityField(k dal.QualityKey, kind string) string {
	return strconv.FormatInt(k.VendorID, 10) + "|" + k.MCC + "|" + k.MNC + "|" + kind
}

// Add adds the buckets in one pipeline.
func (s *VendorQuality) Add(ctx context.Context, bs []dal.QualityBucket) error {
	pipe := s.c.Pipeline()
	touched := map[string]bool{}
	for _, b := range bs {
		h := qualityMinuteHash(b.Minute.Unix())
		if b.Counts.Delivered > 0 {
			pipe.HIncrBy(ctx, h, qualityField(b.Key, "d"), b.Counts.Delivered)
		}
		pipe.HIncrBy(ctx, h, qualityField(b.Key, "j"), b.Counts.Judged)
		touched[h] = true
	}
	for h := range touched {
		pipe.Expire(ctx, h, 2*time.Hour)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Load is one HGETALL per minute of the window, pipelined.
func (s *VendorQuality) Load(ctx context.Context, window time.Duration) (map[dal.QualityKey]dal.QualityCounts, error) {
	now := time.Now().UTC().Unix()
	first := now - int64(window.Seconds())
	first -= first % 60
	pipe := s.c.Pipeline()
	var cmds []*redis.MapStringStringCmd
	for m := first; m <= now; m += 60 {
		cmds = append(cmds, pipe.HGetAll(ctx, qualityMinuteHash(m)))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	out := map[dal.QualityKey]dal.QualityCounts{}
	for _, cmd := range cmds {
		for f, v := range cmd.Val() {
			k, kind, ok := qualityParseField(f)
			if !ok {
				continue
			}
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				continue
			}
			c := out[k]
			if kind == "d" {
				c.Delivered += n
			} else {
				c.Judged += n
			}
			out[k] = c
		}
	}
	return out, nil
}

func qualityParseField(f string) (dal.QualityKey, string, bool) {
	p := strings.Split(f, "|")
	if len(p) != 4 || (p[3] != "d" && p[3] != "j") {
		return dal.QualityKey{}, "", false
	}
	id, err := strconv.ParseInt(p[0], 10, 64)
	if err != nil {
		return dal.QualityKey{}, "", false
	}
	return dal.QualityKey{VendorID: id, MCC: p[1], MNC: p[2]}, p[3], true
}
