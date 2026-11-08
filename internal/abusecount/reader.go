package abusecount

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/abuse"
	"github.com/yakugate/yakugate/internal/dal"
)

// Totals is one account's counts over a span.
type Totals struct {
	N            int64            // messages
	Enc          map[string]int64 // messages by encoding
	SpendMicro   int64            // cost to the customer, in 1/SpendScale of its currency
	HLRConsulted int64
	HLRMiss      int64
	// Sums of abuse.Measure over the messages that had text.
	FmtN, FmtPunct, FmtEmoji, FmtPad, FmtCased, FmtCaps int64
}

// Spend is the cost in whole units of the company's currency.
func (t Totals) Spend() float64 { return float64(t.SpendMicro) / SpendScale }

// Shape is the average shape of the texts counted, and how many there were —
// what abuse.AverageShape gives for the same texts.
func (t Totals) Shape() (abuse.Shape, int) {
	if t.FmtN == 0 {
		return abuse.Shape{}, 0
	}
	n := float64(t.FmtN)
	out := abuse.Shape{
		Punct:   float64(t.FmtPunct) / shapeScale / n,
		Emoji:   float64(t.FmtEmoji) / shapeScale / n,
		Padding: float64(t.FmtPad) / shapeScale / n,
	}
	// Half the texts must be cased before a capitals ratio is quoted at all.
	if t.FmtCased*2 >= t.FmtN && t.FmtCased > 0 {
		out.Cased = true
		out.Caps = float64(t.FmtCaps) / shapeScale / float64(t.FmtCased)
	}
	return out, int(t.FmtN)
}

func (t *Totals) add(name string, n int64) {
	switch name {
	case "n":
		t.N += n
	case "sp":
		t.SpendMicro += n
	case "hc":
		t.HLRConsulted += n
	case "hm":
		t.HLRMiss += n
	case "fn":
		t.FmtN += n
	case "fp":
		t.FmtPunct += n
	case "fe":
		t.FmtEmoji += n
	case "fx":
		t.FmtPad += n
	case "fc":
		t.FmtCased += n
	case "fu":
		t.FmtCaps += n
	default:
		if strings.HasPrefix(name, "e:") {
			if t.Enc == nil {
				t.Enc = map[string]int64{}
			}
			t.Enc[name[2:]] += n
		}
	}
}

// Reader reads the counters. One per monitor; it remembers buckets that can
// no longer change, so a sweep reads from Redis only the last minutes.
type Reader struct {
	a dal.AbuseCounts

	mu      sync.Mutex
	minutes map[int64]map[string]int64 // closed minute buckets, parsed
	hours   map[int64]map[string]int64 // closed hour buckets
}

// NewReader reads the DAL's counters. No store gives nil.
func NewReader(a dal.AbuseCounts) *Reader {
	if a == nil {
		return nil
	}
	return &Reader{a: a, minutes: map[int64]map[string]int64{}, hours: map[int64]map[string]int64{}}
}

// Epoch is since when the counters are whole. ok=false: nothing has counted
// yet (or Redis lost it) — nothing may be judged from them.
func (r *Reader) Epoch(ctx context.Context) (time.Time, bool, error) {
	return r.a.AbuseEpoch(ctx)
}

// AskPrefixLen tells the accept roles which prefix length to count (the AIT
// rule's). It answers since when that length has been asked for: prefixes of
// an earlier length are not comparable, so AIT waits one window after a
// change.
func (r *Reader) AskPrefixLen(ctx context.Context, n int) (time.Time, error) {
	return r.a.AskAbusePrefixLen(ctx, n)
}

// bucket reads hash buckets [from, to) of one kind, from memory when closed.
func (r *Reader) buckets(ctx context.Context, kind byte, from, to int64, nowBucket int64) ([]map[string]int64, error) {
	cache := r.minutes
	if kind == 'h' {
		cache = r.hours
	}
	out := make([]map[string]int64, 0, to-from)
	var ask []int64
	r.mu.Lock()
	for b := from; b < to; b++ {
		if m, ok := cache[b]; ok {
			out = append(out, m)
		} else {
			ask = append(ask, b)
		}
	}
	r.mu.Unlock()
	if len(ask) == 0 {
		return out, nil
	}
	got, err := r.a.AbuseBuckets(ctx, kind, ask)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, b := range ask {
		m := got[i]
		out = append(out, m)
		// A bucket two behind the current one no longer changes (a flush is
		// at most a second late; a message's bucket is its accept time).
		if b < nowBucket-1 {
			cache[b] = m
		}
	}
	// Forget what is past its keep time.
	for b := range r.minutes {
		if b < nowBucket-int64(minuteKeep/time.Minute) && kind == 'm' {
			delete(r.minutes, b)
		}
	}
	for b := range r.hours {
		if b < nowBucket-int64(hourKeep/time.Hour) && kind == 'h' {
			delete(r.hours, b)
		}
	}
	return out, nil
}

func addInto(sum map[int64]*Totals, buckets []map[string]int64) {
	for _, m := range buckets {
		for field, n := range m {
			i := strings.IndexByte(field, '|')
			if i <= 0 {
				continue
			}
			acct, err := strconv.ParseInt(field[:i], 10, 64)
			if err != nil {
				continue
			}
			t := sum[acct]
			if t == nil {
				t = &Totals{}
				sum[acct] = t
			}
			t.add(field[i+1:], n)
		}
	}
}

// exactMinutes is the longest span read minute by minute; a longer one, or
// one that starts before the minutes kept, is read by the hour.
const exactMinutes = 220

// Totals sums every account's counters over [from, to), and says how much of
// that span the answer really covers.
//
// A span of up to exactMinutes that lies inside the minutes kept is exact to
// the minute. Anything longer (a baseline, a day's window) is read by whole
// hours with the minutes of its last, part hour; the part hour at its far
// end is left out, and covered is shorter by it — the caller divides by what
// was covered, never by what it asked for.
func (r *Reader) Totals(ctx context.Context, from, to time.Time) (sum map[int64]*Totals, covered time.Duration, err error) {
	sum = map[int64]*Totals{}
	if !to.After(from) {
		return sum, 0, nil
	}
	now := time.Now()
	nowMin, nowHour := now.Unix()/60, now.Unix()/3600
	fromMin, toMin := from.Unix()/60, (to.Unix()+59)/60
	if to.Unix()%60 == 0 {
		toMin = to.Unix() / 60
	}
	oldestMin := nowMin - int64(minuteKeep/time.Minute) + 10
	if toMin-fromMin <= exactMinutes && fromMin >= oldestMin {
		bs, err := r.buckets(ctx, 'm', fromMin, toMin, nowMin)
		if err != nil {
			return nil, 0, err
		}
		addInto(sum, bs)
		return sum, time.Duration(toMin-fromMin) * time.Minute, nil
	}
	// By the hour: whole hours from the first full one, then the minutes of
	// the last, part hour.
	fromHour := (from.Unix() + 3599) / 3600
	toHour := to.Unix() / 3600
	if toHour < fromHour {
		toHour = fromHour
	}
	hs, err := r.buckets(ctx, 'h', fromHour, toHour, nowHour)
	if err != nil {
		return nil, 0, err
	}
	addInto(sum, hs)
	covered = time.Duration(toHour-fromHour) * time.Hour
	if edgeFrom := toHour * 60; toMin > edgeFrom && edgeFrom >= oldestMin {
		ms, err := r.buckets(ctx, 'm', edgeFrom, toMin, nowMin)
		if err != nil {
			return nil, 0, err
		}
		addInto(sum, ms)
		covered += time.Duration(toMin-edgeFrom) * time.Minute
	}
	return sum, covered, nil
}

// Prefixes is, per account, the messages to each number prefix of length n
// over [from, to) (minute-exact; from must be inside the minutes kept).
// Counts under "*" are prefixes past the cap: part of the total, never the
// top prefix.
func (r *Reader) Prefixes(ctx context.Context, from, to time.Time, n int) (map[int64]map[string]int64, error) {
	out := map[int64]map[string]int64{}
	fromMin, toMin := from.Unix()/60, (to.Unix()+59)/60
	var minutes []int64
	for m := fromMin; m < toMin; m++ {
		minutes = append(minutes, m)
	}
	got, err := r.a.AbuseBuckets(ctx, dal.AbusePrefix, minutes)
	if err != nil {
		return nil, err
	}
	for _, raw := range got {
		for field, cnt := range raw {
			i := strings.IndexByte(field, '|')
			if i <= 0 {
				continue
			}
			acct, err1 := strconv.ParseInt(field[:i], 10, 64)
			p := field[i+1:]
			if err1 != nil || (p != otherPrefix && len(p) != n) {
				continue // a prefix counted at another length: not comparable
			}
			if out[acct] == nil {
				out[acct] = map[string]int64{}
			}
			out[acct][p] += cnt
		}
	}
	return out, nil
}

// OtherPrefix is the name prefixes past the cap are counted under.
const OtherPrefix = otherPrefix

// Distinct is how many different numbers an account sent to over [from, to):
// an estimate (HyperLogLog, standard error under 1%), a little low when a
// flush carried more numbers than it may.
func (r *Reader) Distinct(ctx context.Context, acct int64, from, to time.Time) (int64, error) {
	fromMin, toMin := from.Unix()/60, (to.Unix()+59)/60
	minutes := make([]int64, 0, toMin-fromMin)
	for m := fromMin; m < toMin; m++ {
		minutes = append(minutes, m)
	}
	if len(minutes) == 0 {
		return 0, nil
	}
	return r.a.AbuseDistinct(ctx, acct, minutes)
}

// Samples is an account's kept texts accepted at or after since, newest
// first, at most limit (0 = all kept).
func (r *Reader) Samples(ctx context.Context, acct int64, since time.Time, limit int) ([]Sample, error) {
	kept, err := r.a.AbuseSamples(ctx, acct, SampleKeep)
	if err != nil {
		return nil, err
	}
	out := make([]Sample, 0, len(kept))
	for _, s := range kept {
		if s.At < since.Unix() {
			continue
		}
		out = append(out, s)
	}
	// By accept time, not by the order the accept processes happened to
	// flush in.
	sort.SliceStable(out, func(i, j int) bool { return out[i].At > out[j].At })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Refusals is each account's refused messages over the last `days` whole
// UTC days and today, and how many of those days have a bucket at all (the
// caller decides whether that is enough to stand for the whole span).
func (r *Reader) Refusals(ctx context.Context, days int) (map[int64]int64, int, error) {
	out := map[int64]int64{}
	now := time.Now().UTC()
	buckets := make([]int64, 0, days+1)
	for d := 0; d <= days; d++ {
		// The UTC day as a day number: the day bucket NoteRefusal counts in.
		buckets = append(buckets, now.AddDate(0, 0, -d).Unix()/86400)
	}
	got, err := r.a.AbuseBuckets(ctx, dal.AbuseDay, buckets)
	if err != nil {
		return nil, 0, err
	}
	have := 0
	for _, raw := range got {
		if len(raw) > 0 {
			have++
		}
		for field, n := range raw {
			i := strings.IndexByte(field, '|')
			if i <= 0 || field[i+1:] != "rj" {
				continue
			}
			acct, err1 := strconv.ParseInt(field[:i], 10, 64)
			if err1 == nil {
				out[acct] += n
			}
		}
	}
	return out, have, nil
}
