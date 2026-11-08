package daltest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// The contracts of the shared counters and blocks (dal/counters.go). Each
// takes TWO handles, a and b, opened as two processes would open them (two
// connections): what one writes the other must see, and two taking from one
// limit together never take more than it. Step 3's MySQL versions must pass
// the same suites the Redis ones pass today.

// stamp keeps the keys of one run apart from every other run's.
func stamp(t *testing.T) string {
	return fmt.Sprintf("dalt%d", time.Now().UnixNano()%1e12)
}

// RunLimits is the per-second buckets' contract (dal.Limits).
func RunLimits(t *testing.T, a, b dal.Limits) {
	ctx := context.Background()
	st := stamp(t)
	// A fixed second for each case: the buckets of the test are not tied
	// to the clock, and a case never straddles two seconds.
	sec := time.Now().Unix()

	t.Run("spend counts and answers the count", func(t *testing.T) {
		key := st + ":spend"
		if n, err := a.Spend(ctx, key, sec, 5); err != nil || n != 5 {
			t.Fatalf("first spend: %d %v, want 5", n, err)
		}
		if n, err := b.Spend(ctx, key, sec, 3); err != nil || n != 8 {
			t.Fatalf("the other process's spend: %d %v, want 8 (one bucket)", n, err)
		}
		if n, _ := a.Spend(ctx, key, sec+1, 2); n != 2 {
			t.Fatalf("the next second is a new bucket: %d, want 2", n)
		}
	})

	t.Run("take gives what fits and no more", func(t *testing.T) {
		key := st + ":take"
		for i, want := range []int{60, 40, 0} {
			if got, err := a.Take(ctx, key, sec, 100, 60); err != nil || got != want {
				t.Fatalf("take %d: %d %v, want %d", i+1, got, err, want)
			}
		}
		// Spend draws on the same bucket: a full bucket counts past its limit.
		if n, _ := b.Spend(ctx, key, sec, 1); n != 101 {
			t.Fatalf("a spend on a full bucket answers %d, want 101", n)
		}
		if got, _ := b.Take(ctx, key, sec+1, 100, 250); got != 100 {
			t.Fatalf("the next second gives the whole limit and no more: %d", got)
		}
	})

	t.Run("grant and return", func(t *testing.T) {
		key := st + ":grant"
		if got, err := a.Grant(ctx, key, sec, 100, 60); err != nil || got != 60 {
			t.Fatalf("grant: %d %v, want 60", got, err)
		}
		if err := b.Return(ctx, key, sec, 25); err != nil {
			t.Fatal(err)
		}
		if got, _ := a.Grant(ctx, key, sec, 100, 500); got != 65 {
			t.Fatalf("after 25 of 60 came back the second has 65 left, got %d", got)
		}
		// More handed back than was taken: the bucket stops at zero.
		if err := a.Return(ctx, key, sec, 500); err != nil {
			t.Fatal(err)
		}
		if got, _ := a.Grant(ctx, key, sec, 100, 500); got != 100 {
			t.Fatalf("a bucket never goes below zero: granted %d of a limit of 100", got)
		}
		if got, _ := b.Grant(ctx, key, sec, 100, 1); got != 0 {
			t.Fatalf("an empty bucket grants nothing, got %d", got)
		}
	})

	t.Run("a return to a bucket that is not there makes none", func(t *testing.T) {
		key := st + ":late"
		if err := a.Return(ctx, key, sec, 40); err != nil {
			t.Fatal(err)
		}
		if got, _ := b.Grant(ctx, key, sec, 10, 50); got != 10 {
			t.Fatalf("the second gives its limit, not the limit plus a late return: %d", got)
		}
	})

	race := func(t *testing.T, key string, take func(l dal.Limits) (int, error)) {
		var got atomic.Int64
		var wg sync.WaitGroup
		for _, l := range []dal.Limits{a, b, a, b, a, b, a, b} {
			wg.Add(1)
			go func(l dal.Limits) {
				defer wg.Done()
				for i := 0; i < 400; i++ {
					n, err := take(l)
					if err != nil {
						t.Error(err)
						return
					}
					if n == 0 {
						return
					}
					got.Add(int64(n))
				}
			}(l)
		}
		wg.Wait()
		if got.Load() != 1000 {
			t.Fatalf("two processes took %d from a limit of 1000 (want exactly 1000: never more, and all of it)", got.Load())
		}
	}
	t.Run("two takers never take more than the limit", func(t *testing.T) {
		key := st + ":race-take"
		race(t, key, func(l dal.Limits) (int, error) { return l.Take(ctx, key, sec, 1000, 7) })
	})
	t.Run("two granters never take more than the limit", func(t *testing.T) {
		key := st + ":race-grant"
		race(t, key, func(l dal.Limits) (int, error) { return l.Grant(ctx, key, sec, 1000, 3) })
	})
	t.Run("concurrent spends are all counted", func(t *testing.T) {
		key := st + ":race-spend"
		var wg sync.WaitGroup
		for _, l := range []dal.Limits{a, b, a, b} {
			wg.Add(1)
			go func(l dal.Limits) {
				defer wg.Done()
				for i := 0; i < 250; i++ {
					if _, err := l.Spend(ctx, key, sec, 1); err != nil {
						t.Error(err)
						return
					}
				}
			}(l)
		}
		wg.Wait()
		if n, _ := a.Spend(ctx, key, sec, 0); n != 1000 {
			t.Fatalf("1000 spends counted as %d", n)
		}
	})
}

// RunUsage is the period counts' contract (dal.Usage).
func RunUsage(t *testing.T, a, b dal.Usage) {
	ctx := context.Background()
	key := stamp(t) + ":usage:20261029"
	if n, err := a.GetUsage(ctx, key); err != nil || n != 0 {
		t.Fatalf("a key never counted: %d %v, want 0 and no error", n, err)
	}
	var wg sync.WaitGroup
	for _, u := range []dal.Usage{a, b, a, b} {
		wg.Add(1)
		go func(u dal.Usage) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := u.AddUsage(ctx, key, 2, time.Hour); err != nil {
					t.Error(err)
					return
				}
			}
		}(u)
	}
	wg.Wait()
	if n, err := b.GetUsage(ctx, key); err != nil || n != 400 {
		t.Fatalf("both processes' adds: %d %v, want 400", n, err)
	}
}

// RunBlocks is the login ladders' and the guessing guard's contract
// (dal.Blocks). The rules: 3 failures trip; rungs of 1, 2, 3 and 4 minutes.
func RunBlocks(t *testing.T, a, b dal.Blocks) {
	ctx := context.Background()
	st := stamp(t)
	rules := dal.LadderRules{
		Threshold:      3,
		Rungs:          [4]time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute, 4 * time.Minute},
		DecayAfter:     time.Hour,
		EscalateWithin: 15 * time.Minute,
	}
	var used []string
	id := func(name string) string {
		v := st + ":" + name
		used = append(used, v)
		return v
	}
	t.Cleanup(func() {
		for _, v := range used {
			a.ClearBlock(ctx, v)
		}
	})

	t.Run("the threshold trips the first rung, seen by every process", func(t *testing.T) {
		u := id("u")
		if left, err := a.LockedFor(ctx, u); err != nil || left != 0 {
			t.Fatalf("a fresh id is not locked: %v %v", left, err)
		}
		for i := 0; i < 2; i++ {
			locked, _, level, err := a.Fail(ctx, u, rules, time.Now())
			if err != nil || locked || level != 0 {
				t.Fatalf("failure %d under the threshold: locked=%v level=%d %v", i+1, locked, level, err)
			}
		}
		locked, retry, level, err := b.Fail(ctx, u, rules, time.Now()) // the other process's failure counts too
		if err != nil || !locked || level != 1 || retry != time.Minute {
			t.Fatalf("the third failure: locked=%v retry=%v level=%d %v, want a lock of 1 min on rung 1", locked, retry, level, err)
		}
		left, err := a.LockedFor(ctx, u)
		if err != nil || left <= 0 || left > time.Minute {
			t.Fatalf("the other process sees the lock: %v %v", left, err)
		}
		// A reset clears the count, never the lock.
		if err := a.ResetFails(ctx, u); err != nil {
			t.Fatal(err)
		}
		if left, _ := b.LockedFor(ctx, u); left <= 0 {
			t.Fatal("a reset lifted a lock")
		}
	})

	t.Run("a trip soon after the last climbs a rung, up to the last", func(t *testing.T) {
		u := id("climb")
		for want := 1; want <= 5; want++ {
			var locked bool
			var level int
			var retry time.Duration
			for i := 0; i < 3; i++ {
				locked, retry, level, _ = a.Fail(ctx, u, rules, time.Now())
			}
			rung := want
			if rung > 4 {
				rung = 4
			}
			if !locked || level != rung || retry != rules.Rungs[rung-1] {
				t.Fatalf("trip %d: locked=%v level=%d retry=%v, want rung %d of %v", want, locked, level, retry, rung, rules.Rungs[rung-1])
			}
		}
	})

	t.Run("a cleared id starts again from nothing", func(t *testing.T) {
		u := id("clear")
		for i := 0; i < 6; i++ {
			a.Fail(ctx, u, rules, time.Now())
		}
		if err := b.ClearBlock(ctx, u); err != nil {
			t.Fatal(err)
		}
		if left, _ := a.LockedFor(ctx, u); left != 0 {
			t.Fatalf("still locked after a clear: %v", left)
		}
		var level int
		for i := 0; i < 3; i++ {
			_, _, level, _ = a.Fail(ctx, u, rules, time.Now())
		}
		if level != 1 {
			t.Fatalf("after a clear the next trip is rung 1, got %d", level)
		}
	})

	t.Run("who is locked is listed, by every process", func(t *testing.T) {
		u := id("listed")
		for i := 0; i < 3; i++ {
			a.Fail(ctx, u, rules, time.Now())
		}
		blocks, _, err := b.Blocked(ctx, 1000)
		if err != nil {
			t.Fatal(err)
		}
		var found *dal.Block
		for i := range blocks {
			if blocks[i].ID == u {
				found = &blocks[i]
			}
		}
		if found == nil || found.Level != 1 || found.Remaining <= 0 || found.Remaining > time.Minute {
			t.Fatalf("the lock is not listed as it is: %+v", found)
		}
		a.ClearBlock(ctx, u)
		blocks, _, err = b.Blocked(ctx, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, bl := range blocks {
			if bl.ID == u {
				t.Fatal("a lifted lock is still listed")
			}
		}
	})

	t.Run("two processes failing one id trip once per threshold", func(t *testing.T) {
		u := id("race")
		r := rules
		r.Threshold = 50
		var trips atomic.Int64
		var wg sync.WaitGroup
		for _, bl := range []dal.Blocks{a, b, a, b} {
			wg.Add(1)
			go func(bl dal.Blocks) {
				defer wg.Done()
				for i := 0; i < 25; i++ {
					locked, _, _, err := bl.Fail(ctx, u, r, time.Now())
					if err != nil {
						t.Error(err)
						return
					}
					if locked {
						trips.Add(1)
					}
				}
			}(bl)
		}
		wg.Wait()
		if trips.Load() != 2 {
			t.Fatalf("100 failures at a threshold of 50 tripped %d times, want exactly 2", trips.Load())
		}
	})

	t.Run("identities seen from an address", func(t *testing.T) {
		addr := st + ":203.0.113.7"
		t.Cleanup(func() { a.ClearIdentities(ctx, addr) })
		steps := []struct {
			h        dal.Blocks
			identity string
			added    bool
			distinct int
		}{
			{a, "alice", true, 1},
			{b, "alice", false, 1}, // a repeat is not new, from any process
			{b, "bob", true, 2},
			{a, "carol", true, 3},
		}
		for i, s := range steps {
			added, n, err := s.h.SeeIdentity(ctx, addr, s.identity, time.Minute)
			if err != nil || added != s.added || n != s.distinct {
				t.Fatalf("step %d (%s): added=%v distinct=%d %v, want %v %d", i+1, s.identity, added, n, err, s.added, s.distinct)
			}
		}
		if err := b.ClearIdentities(ctx, addr); err != nil {
			t.Fatal(err)
		}
		if added, n, _ := a.SeeIdentity(ctx, addr, "alice", time.Minute); !added || n != 1 {
			t.Fatalf("after a clear the address starts again: added=%v distinct=%d", added, n)
		}
	})
}

// RunDedupe is the duplicate check's contract (dal.Dedupe).
func RunDedupe(t *testing.T, a, b dal.Dedupe) {
	ctx := context.Background()
	st := stamp(t)
	var used []string
	key := func(name string) string {
		k := "dedup:" + st + ":" + name
		used = append(used, k)
		return k
	}
	t.Cleanup(func() {
		for _, k := range used {
			for _, id := range []string{"M1", "M2", "M3"} {
				a.Release(ctx, k, id)
			}
		}
	})

	t.Run("created, then a duplicate of the first, from any process", func(t *testing.T) {
		k := key("first")
		if first, err := a.Check(ctx, k, "M1", time.Hour); err != nil || first != "" {
			t.Fatalf("first sight: %q %v, want created", first, err)
		}
		if first, err := b.Check(ctx, k, "M2", time.Hour); err != nil || first != "M1" {
			t.Fatalf("the other process's check: %q %v, want a duplicate of M1", first, err)
		}
		// The same message asked twice is NOT settled: MySQL answers
		// "created" (dalmysql.Dedupe: "the same request asked twice"), Redis
		// answers the message's own id (a duplicate of itself). The pipeline
		// asks once a message, so neither is reached today; found when the
		// two moved into the DAL (29 Oct 2026), left for a ruling.
		if first, err := a.Check(ctx, k, "M1", time.Hour); err != nil || (first != "" && first != "M1") {
			t.Fatalf("the same message asked twice: %q %v, want created or itself", first, err)
		}
	})

	t.Run("a release removes only its own key", func(t *testing.T) {
		k := key("release")
		a.Check(ctx, k, "M1", time.Hour)
		if err := b.Release(ctx, k, "M2"); err != nil {
			t.Fatal(err)
		}
		if first, _ := a.Check(ctx, k, "M3", time.Hour); first != "M1" {
			t.Fatalf("a release by another message removed the key: %q", first)
		}
		if err := b.Release(ctx, k, "M1"); err != nil {
			t.Fatal(err)
		}
		if first, _ := a.Check(ctx, k, "M2", time.Hour); first != "" {
			t.Fatalf("after its own release the key is new again: %q", first)
		}
	})

	t.Run("a key ends with its window", func(t *testing.T) {
		k := key("window")
		a.Check(ctx, k, "M1", time.Second)
		time.Sleep(2100 * time.Millisecond)
		if first, err := b.Check(ctx, k, "M2", time.Hour); err != nil || first != "" {
			t.Fatalf("after the window: %q %v, want created", first, err)
		}
		if first, _ := a.Check(ctx, k, "M3", time.Hour); first != "M2" {
			t.Fatalf("the key now holds the new message: %q", first)
		}
	})

	t.Run("two processes checking one key: one is created", func(t *testing.T) {
		k := key("race")
		var mu sync.Mutex
		created, firsts := []string{}, map[string]int{}
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			h := a
			if i%2 == 1 {
				h = b
			}
			wg.Add(1)
			go func(h dal.Dedupe, id string) {
				defer wg.Done()
				first, err := h.Check(ctx, k, id, time.Hour)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				if first == "" {
					created = append(created, id)
				} else {
					firsts[first]++
				}
			}(h, fmt.Sprintf("R%02d", i))
		}
		wg.Wait()
		if len(created) != 1 || len(firsts) != 1 || firsts[created[0]] != 19 {
			t.Fatalf("created %v, duplicates of %v: want one created and 19 naming it", created, firsts)
		}
		a.Release(ctx, k, created[0])
	})

	if sw, ok := a.(dal.DedupeSweeper); ok {
		t.Run("the sweep runs", func(t *testing.T) {
			if _, err := sw.SweepDedupe(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// RunCampaignWindow is the live campaign window's contract
// (dal.CampaignWindow).
func RunCampaignWindow(t *testing.T, a, b dal.CampaignWindow) {
	ctx := context.Background()
	hash := fmt.Sprintf("%064x", time.Now().UnixNano())
	bucket := time.Now().Unix() / 600
	if err := a.AddCampaignCounts(ctx, []dal.CampaignCount{
		{Hash: hash, Bucket: bucket, Account: 7, N: 5},
		{Hash: hash, Bucket: bucket - 1, Account: 9, N: 2},
	}, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := b.AddCampaignCounts(ctx, []dal.CampaignCount{{Hash: hash, Bucket: bucket, Account: 7, N: 3}}, time.Hour); err != nil {
		t.Fatal(err)
	}
	got, err := b.CampaignBuckets(ctx, hash, []int64{bucket, bucket - 1, bucket - 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0][7] != 8 || len(got[0]) != 1 || got[1][9] != 2 || len(got[2]) != 0 {
		t.Fatalf("buckets read back %v, want [{7:8} {9:2} {}] (in the order asked; both processes' counts added)", got)
	}
}

// RunAbuseCounts is the abuse counters' contract (dal.AbuseCounts). reset
// empties the store's abuse counters (the epoch and the prefix length are
// one per store).
func RunAbuseCounts(t *testing.T, a, b dal.AbuseCounts, reset func()) {
	ctx := context.Background()
	reset()
	t.Cleanup(reset)
	keep := map[byte]time.Duration{
		dal.AbuseMinute: time.Hour, dal.AbuseHour: time.Hour, dal.AbusePrefix: time.Hour,
		dal.AbuseDay: time.Hour, dal.AbuseNumbers: time.Hour, dal.AbuseTexts: time.Hour,
	}

	if _, ok, err := a.AbuseEpoch(ctx); err != nil || ok {
		t.Fatalf("an empty store has no epoch: ok=%v %v", ok, err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	minute, hour, day := now.Unix()/60, now.Unix()/3600, now.Unix()/86400
	f1 := dal.AbuseFlush{Since: now.Add(-time.Minute), Keep: keep, SampleKeep: 3,
		Counts: []dal.AbuseCount{
			{Kind: dal.AbuseMinute, Bucket: minute, Field: "7|n", N: 2},
			{Kind: dal.AbuseHour, Bucket: hour, Field: "7|n", N: 2},
			{Kind: dal.AbusePrefix, Bucket: minute, Field: "7|44770", N: 2},
			{Kind: dal.AbuseDay, Bucket: day, Field: "7|rj", N: 4},
		},
		Numbers: []dal.AbuseNumberSet{{Account: 7, Minute: minute, Numbers: []string{"447700000001", "447700000002"}}},
		Texts: []dal.AbuseTextSet{{Account: 7, Samples: []dal.AbuseSample{
			{At: now.Unix() - 3, Body: "one"}, {At: now.Unix() - 2, Body: "two"},
		}}},
	}
	f2 := dal.AbuseFlush{Since: now, Keep: keep, SampleKeep: 3,
		Counts: []dal.AbuseCount{
			{Kind: dal.AbuseMinute, Bucket: minute, Field: "7|n", N: 3},
			{Kind: dal.AbuseMinute, Bucket: minute, Field: "8|n", N: 1},
		},
		Numbers: []dal.AbuseNumberSet{{Account: 7, Minute: minute + 1, Numbers: []string{"447700000002", "447700000003"}}},
		Texts: []dal.AbuseTextSet{{Account: 7, Samples: []dal.AbuseSample{
			{At: now.Unix() - 1, Dest: "447700000003", Sender: "BRAND", Body: "three"}, {At: now.Unix(), Body: "four"},
		}}},
	}
	if err := a.FlushAbuse(ctx, f1); err != nil {
		t.Fatal(err)
	}
	if err := b.FlushAbuse(ctx, f2); err != nil {
		t.Fatal(err)
	}

	if since, ok, err := b.AbuseEpoch(ctx); err != nil || !ok || !since.Equal(f1.Since) {
		t.Fatalf("the epoch is the first flush's and is set once: %v %v %v, want %v", since, ok, err, f1.Since)
	}
	ms, err := b.AbuseBuckets(ctx, dal.AbuseMinute, []int64{minute, minute + 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 || ms[0]["7|n"] != 5 || ms[0]["8|n"] != 1 || len(ms[1]) != 0 {
		t.Fatalf("minute buckets %v, want [{7|n:5 8|n:1} {}]", ms)
	}
	for _, c := range []struct {
		kind  byte
		b     int64
		field string
		n     int64
	}{{dal.AbuseHour, hour, "7|n", 2}, {dal.AbusePrefix, minute, "7|44770", 2}, {dal.AbuseDay, day, "7|rj", 4}} {
		got, err := a.AbuseBuckets(ctx, c.kind, []int64{c.b})
		if err != nil || len(got) != 1 || got[0][c.field] != c.n {
			t.Fatalf("kind %c: %v %v, want %s=%d", c.kind, got, err, c.field, c.n)
		}
	}
	if n, err := a.AbuseDistinct(ctx, 7, []int64{minute, minute + 1}); err != nil || n != 3 {
		t.Fatalf("different numbers over two minutes: %d %v, want 3", n, err)
	}
	if n, _ := a.AbuseDistinct(ctx, 7, []int64{minute}); n != 2 {
		t.Fatalf("different numbers in one minute: %d, want 2", n)
	}
	samples, err := a.AbuseSamples(ctx, 7, 3)
	if err != nil {
		t.Fatal(err)
	}
	var bodies []string
	for _, s := range samples {
		bodies = append(bodies, s.Body)
	}
	if fmt.Sprint(bodies) != "[four three two]" || samples[1].Dest != "447700000003" || samples[1].Sender != "BRAND" {
		t.Fatalf("kept texts %+v, want the newest 3, newest first, as written", samples)
	}

	t1, err := a.AskAbusePrefixLen(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := b.AbusePrefixLen(ctx); err != nil || n != 5 {
		t.Fatalf("the prefix length asked for: %d %v", n, err)
	}
	// Kept to the second (the first answer carries the moment it was asked;
	// later ones the time as stored).
	if t2, _ := b.AskAbusePrefixLen(ctx, 5); !t2.Equal(t1.Truncate(time.Second)) {
		t.Fatalf("asking again for the same length keeps its time: %v, was %v", t2, t1)
	}
	time.Sleep(1100 * time.Millisecond)
	if t3, _ := b.AskAbusePrefixLen(ctx, 6); !t3.After(t1) {
		t.Fatalf("a new length starts a new time: %v, was %v", t3, t1)
	}
}
