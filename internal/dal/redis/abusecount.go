package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yakugate/yakugate/internal/dal"
)

// AbuseCounts is the abuse monitor's counters in the main Redis
// (dal.AbuseCounts), moved from internal/abusecount on 29 Oct 2026, the same
// commands:
//
//	ab:m:<minute>   hash   minute counts (dal.AbuseMinute)
//	ab:h:<hour>     hash   hour counts (dal.AbuseHour)
//	ab:p:<minute>   hash   prefix counts (dal.AbusePrefix)
//	ab:d:<yyyymmdd> hash   day counts (dal.AbuseDay)
//	ab:u:<acct>:<minute>  HyperLogLog of the numbers sent to
//	ab:t:<acct>     list   the newest texts, JSON, newest at the head
//	ab:epoch        since when the counters are whole (set once)
//	ab:cfg          what the monitor asks for: prefix_len, since
//
// The fields of a hash are the package's ("<acct>|<name>").
type AbuseCounts struct{ c *redis.Client }

// NewAbuseCounts keeps the counters on a Redis client.
func NewAbuseCounts(c *redis.Client) *AbuseCounts { return &AbuseCounts{c: c} }

const (
	abuseEpochKey = "ab:epoch"
	abuseCfgKey   = "ab:cfg"
)

// abuseKey names a hash bucket of a kind.
func abuseKey(kind byte, b int64) string {
	switch kind {
	case dal.AbuseHour:
		return "ab:h:" + strconv.FormatInt(b, 10)
	case dal.AbusePrefix:
		return "ab:p:" + strconv.FormatInt(b, 10)
	case dal.AbuseDay:
		// The day's date, UTC: a unix day starts at a UTC midnight.
		return "ab:d:" + time.Unix(b*86400, 0).UTC().Format("20060102")
	default:
		return "ab:m:" + strconv.FormatInt(b, 10)
	}
}

func abuseNumbersKey(acct, m int64) string {
	return "ab:u:" + strconv.FormatInt(acct, 10) + ":" + strconv.FormatInt(m, 10)
}
func abuseTextKey(acct int64) string { return "ab:t:" + strconv.FormatInt(acct, 10) }

// FlushAbuse writes a flush in ONE pipeline.
func (a *AbuseCounts) FlushAbuse(ctx context.Context, f dal.AbuseFlush) error {
	pipe := a.c.Pipeline()
	// Since when the counters are whole: set by the first flush of the first
	// accept process, and set again by the first flush after Redis lost
	// everything — which is what makes the monitor wait for a new baseline.
	pipe.SetNX(ctx, abuseEpochKey, f.Since.UTC().Format(time.RFC3339), 0)
	expire := map[string]time.Duration{}
	for _, ck := range f.Counts {
		key := abuseKey(ck.Kind, ck.Bucket)
		expire[key] = f.Keep[ck.Kind]
		pipe.HIncrBy(ctx, key, ck.Field, ck.N)
	}
	for _, set := range f.Numbers {
		key := abuseNumbersKey(set.Account, set.Minute)
		members := make([]any, len(set.Numbers))
		for i, d := range set.Numbers {
			members[i] = d
		}
		pipe.PFAdd(ctx, key, members...)
		expire[key] = f.Keep[dal.AbuseNumbers]
	}
	for _, set := range f.Texts {
		key := abuseTextKey(set.Account)
		vals := make([]any, 0, len(set.Samples))
		for _, s := range set.Samples {
			if b, err := json.Marshal(s); err == nil {
				vals = append(vals, b)
			}
		}
		if len(vals) == 0 {
			continue
		}
		pipe.LPush(ctx, key, vals...) // the last pushed is the newest, at the head
		pipe.LTrim(ctx, key, 0, int64(f.SampleKeep-1))
		expire[key] = f.Keep[dal.AbuseTexts]
	}
	for key, d := range expire {
		pipe.Expire(ctx, key, d)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// AbusePrefixLen reads the prefix length the monitor asked for.
func (a *AbuseCounts) AbusePrefixLen(ctx context.Context) (int, error) {
	return a.c.HGet(ctx, abuseCfgKey, "prefix_len").Int()
}

// AskAbusePrefixLen sets the prefix length to count, unless it is already
// asked for, and answers since when it has been.
func (a *AbuseCounts) AskAbusePrefixLen(ctx context.Context, n int) (time.Time, error) {
	cur, _ := a.c.HGet(ctx, abuseCfgKey, "prefix_len").Int()
	since, _ := a.c.HGet(ctx, abuseCfgKey, "since").Result()
	if cur == n && since != "" {
		if t, err := time.Parse(time.RFC3339, since); err == nil {
			return t, nil
		}
	}
	now := time.Now().UTC()
	err := a.c.HSet(ctx, abuseCfgKey, "prefix_len", n, "since", now.Format(time.RFC3339)).Err()
	return now, err
}

// AbuseEpoch reads since when the counters are whole.
func (a *AbuseCounts) AbuseEpoch(ctx context.Context) (time.Time, bool, error) {
	v, err := a.c.Get(ctx, abuseEpochKey).Result()
	if err == redis.Nil {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	t, perr := time.Parse(time.RFC3339, v)
	if perr != nil {
		return time.Time{}, false, nil
	}
	return t, true, nil
}

// AbuseBuckets reads buckets of one kind in one pipeline.
func (a *AbuseCounts) AbuseBuckets(ctx context.Context, kind byte, buckets []int64) ([]map[string]int64, error) {
	pipe := a.c.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(buckets))
	for i, b := range buckets {
		cmds[i] = pipe.HGetAll(ctx, abuseKey(kind, b))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	out := make([]map[string]int64, len(cmds))
	for i, cmd := range cmds {
		raw, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		m := make(map[string]int64, len(raw))
		for f, v := range raw {
			if n, perr := strconv.ParseInt(v, 10, 64); perr == nil {
				m[f] = n
			}
		}
		out[i] = m
	}
	return out, nil
}

// AbuseDistinct counts the numbers of an account over minutes (PFCOUNT
// over their HyperLogLogs).
func (a *AbuseCounts) AbuseDistinct(ctx context.Context, account int64, minutes []int64) (int64, error) {
	keys := make([]string, 0, len(minutes))
	for _, m := range minutes {
		keys = append(keys, abuseNumbersKey(account, m))
	}
	return a.c.PFCount(ctx, keys...).Result()
}

// AbuseSamples reads an account's kept texts, newest first.
func (a *AbuseCounts) AbuseSamples(ctx context.Context, account int64, keep int) ([]dal.AbuseSample, error) {
	raw, err := a.c.LRange(ctx, abuseTextKey(account), 0, int64(keep-1)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]dal.AbuseSample, 0, len(raw))
	for _, v := range raw {
		var s dal.AbuseSample
		if json.Unmarshal([]byte(v), &s) != nil {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}
