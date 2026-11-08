package daltest

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
)

// The contracts of the record queue, the receipts' keeping and the hand-off
// (dal/records.go, dal/receipts.go), the multi-part buffer (dal/concat.go)
// and vendor quality and the config store's small parts (dal/traffic.go).
// Each takes the handles two processes would have: what one writes the other
// sees, and what only one may have, one has. Step 3's MySQL versions must
// pass the suites the Redis ones pass today.

// RunRecordLanes is the record queue's contract. a and b are one
// deployment's lists opened twice; its owner is a's (a test's own).
func RunRecordLanes(t *testing.T, a, b dal.RecordLanes) {
	ctx := context.Background()
	if a.Owner() != b.Owner() {
		t.Fatalf("a and b must be one deployment's lists: %q, %q", a.Owner(), b.Owner())
	}
	owner := a.Owner()
	put := dal.RecordPut{Owner: owner, Name: []int{0, 1},
		Tasks:   map[int][]string{0: {`{"k":"w","i":"M1","t":1}`, `{"k":"w","i":"M2","t":2}`}, 1: {`{"k":"w","i":"M3","t":3}`}},
		Pending: map[int][]string{0: {"M1", `{"s":"sent","a":7}`}}}
	if err := a.Put(ctx, put); err != nil {
		t.Fatal(err)
	}

	t.Run("a put is seen by the other process", func(t *testing.T) {
		named, err := b.NamedLanes(ctx)
		sort.Strings(named)
		if err != nil || len(named) != 2 || named[0] != "0" || named[1] != "1" {
			t.Fatalf("named lanes %v %v", named, err)
		}
		n, alive, err := b.Look(ctx, []int{0, 1})
		if err != nil || n != 3 || alive {
			t.Fatalf("look: %d alive=%v %v, want 3 and no worker", n, alive, err)
		}
		heads, err := b.Heads(ctx, []int{0, 1, 2})
		if err != nil || heads[0] != `{"k":"w","i":"M1","t":1}` || heads[1] != `{"k":"w","i":"M3","t":3}` || heads[2] != "" {
			t.Fatalf("heads %q %v", heads, err)
		}
		pend, err := b.PendingOf(ctx, []int{0, 1}, "M1")
		if err != nil || pend[0] != `{"s":"sent","a":7}` || pend[1] != "" {
			t.Fatalf("pending %q %v", pend, err)
		}
	})

	t.Run("one turn a lane", func(t *testing.T) {
		if n, err := a.TakeTurn(ctx, 0, "ta", time.Minute); err != nil || n != 1 {
			t.Fatalf("a takes: %d %v", n, err)
		}
		if n, _ := b.TakeTurn(ctx, 0, "tb", time.Minute); n != 0 {
			t.Fatal("b took a turn a holds")
		}
		if n, _ := a.TakeTurn(ctx, 0, "ta", time.Minute); n != 1 {
			t.Fatal("a could not renew its own turn")
		}
		_ = b.DropTurn(ctx, 0, "tb") // not b's: nothing happens
		if n, _ := b.TakeTurn(ctx, 0, "tb", time.Minute); n != 0 {
			t.Fatal("b's drop let a's turn go")
		}
	})

	t.Run("a batch: taken, parked, its mark and pending cleared", func(t *testing.T) {
		raw, held, doing, err := a.Batch(ctx, 0, 10)
		if err != nil || len(raw) != 2 || len(held) != 0 || doing {
			t.Fatalf("batch %v held=%v doing=%v %v", raw, held, doing, err)
		}
		if err := a.MarkDoing(ctx, 0, "ta"); err != nil {
			t.Fatal(err)
		}
		if _, _, doing, _ := b.Batch(ctx, 0, 10); !doing {
			t.Fatal("the doing mark is not seen")
		}
		if n, _ := b.Done(ctx, 0, "tb", 2, nil, nil); n != 0 {
			t.Fatal("a process without the turn ended the batch")
		}
		n, err := a.Done(ctx, 0, "ta", 2, [][2]string{{"M2", `{"k":"w","i":"M2","t":2,"n":1}`}}, []string{"M1"})
		if err != nil || n != 1 {
			t.Fatalf("done %d %v", n, err)
		}
		if l, _ := b.Len(ctx, 0); l != 0 {
			t.Fatalf("%d left on the lane, want 0", l)
		}
		if p, _ := b.ParkedLen(ctx, 0); p != 1 {
			t.Fatalf("%d parked, want 1", p)
		}
		if h, _ := b.HeldIn(ctx, []int{0, 1}, "M2"); !h {
			t.Fatal("the parked message is not held")
		}
		if _, held, doing, _ := b.Batch(ctx, 0, 10); doing || held["M2"] != "1" {
			t.Fatalf("after done: doing=%v held=%v", doing, held)
		}
		if pend, _ := b.PendingOf(ctx, []int{0}, "M1"); pend[0] != "" {
			t.Fatalf("the written message's pending entry stays: %q", pend[0])
		}
	})

	t.Run("the park list: put back at the head, then gone", func(t *testing.T) {
		raw, err := a.Parked(ctx, 0, 10)
		if err != nil || len(raw) != 1 {
			t.Fatalf("parked %v %v", raw, err)
		}
		if n, _ := a.ParkDone(ctx, 0, "ta", 1, []string{`{"k":"w","i":"M2","n":2}`}, nil); n != 1 {
			t.Fatal("park done")
		}
		if raw, _ := b.Parked(ctx, 0, 10); len(raw) != 1 || raw[0] != `{"k":"w","i":"M2","n":2}` {
			t.Fatalf("put back: %v", raw)
		}
		if n, _ := a.ParkDone(ctx, 0, "ta", 1, nil, []string{"M2"}); n != 1 {
			t.Fatal("park done")
		}
		if h, _ := b.HeldIn(ctx, []int{0}, "M2"); h {
			t.Fatal("a message with nothing parked is still held")
		}
	})

	t.Run("a worker's sign of life", func(t *testing.T) {
		if err := a.Beat(ctx, "ta", time.Minute); err != nil {
			t.Fatal(err)
		}
		if _, alive, _ := b.Look(ctx, []int{0}); !alive {
			t.Fatal("the beat is not seen")
		}
		_ = b.Leave(ctx, "tb") // not b's
		if _, alive, _ := a.Look(ctx, []int{0}); !alive {
			t.Fatal("another's leave took the sign away")
		}
		_ = a.Leave(ctx, "ta")
		if _, alive, _ := b.Look(ctx, []int{0}); alive {
			t.Fatal("the sign stays after its worker left")
		}
		_ = a.DropTurn(ctx, 0, "ta")
	})

	t.Run("an error that sent nothing", func(t *testing.T) {
		if a.Unsent(nil) {
			t.Fatal("nil is no error")
		}
	})
}

// RunReceiptFacts is the receipt facts' contract: a and b are two processes'.
func RunReceiptFacts(t *testing.T, a, b dal.ReceiptFacts) {
	ctx := context.Background()
	st := stamp(t)
	k1, k2, kg, kp := "rf:"+st+":1", "rf:"+st+":2", "rf:"+st+":g", "rfp:"+st
	if err := a.File(ctx, []dal.FactPut{
		{Key: k1, Value: `{"m":"A"}`, Life: time.Minute},
		{Key: k2, Value: `{"m":"B"}`, Life: time.Minute},
		{Key: kg, Value: "x\n", Life: time.Minute, Add: true},
		{Key: kg, Value: "y\n", Life: time.Minute, Add: true},
	}); err != nil {
		t.Fatal(err)
	}
	t.Run("filed facts are read by the other process", func(t *testing.T) {
		if v, ok, err := b.Get(ctx, k1); err != nil || !ok || v != `{"m":"A"}` {
			t.Fatalf("get %q %v %v", v, ok, err)
		}
		if v, ok, _ := b.Get(ctx, kg); !ok || v != "x\ny\n" {
			t.Fatalf("a group's lines: %q", v)
		}
		if _, ok, _ := b.Get(ctx, "rf:"+st+":none"); ok {
			t.Fatal("found what was never filed")
		}
	})
	t.Run("a take is given to one process only", func(t *testing.T) {
		if v, ok, err := a.Take(ctx, k1); err != nil || !ok || v != `{"m":"A"}` {
			t.Fatalf("take %q %v %v", v, ok, err)
		}
		if _, ok, _ := b.Take(ctx, k1); ok {
			t.Fatal("taken twice")
		}
		if err := b.Set(ctx, k1, `{"m":"A2"}`, time.Minute); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("many looks at once, in the order asked", func(t *testing.T) {
		vals, found, err := b.Looks(ctx, []dal.FactLook{{Key: k1}, {Key: "rf:" + st + ":none"}, {Key: k2, Take: true}})
		if err != nil || !found[0] || vals[0] != `{"m":"A2"}` || found[1] || !found[2] || vals[2] != `{"m":"B"}` {
			t.Fatalf("looks %q %v %v", vals, found, err)
		}
		gone, err := a.Gone(ctx, []string{k1, k2})
		if err != nil || gone[k1] || !gone[k2] {
			t.Fatalf("gone %v %v", gone, err)
		}
	})
	t.Run("a part counter counts across processes", func(t *testing.T) {
		if n, err := a.CountPart(ctx, kp, 1, time.Minute); err != nil || n != 1 {
			t.Fatalf("first part %d %v", n, err)
		}
		if n, _ := b.CountPart(ctx, kp, 1_000_000, time.Minute); n != 1_000_001 {
			t.Fatalf("second part %d", n)
		}
	})
}

// RunReceiptWrites is the write-behind list's contract: a and b are one
// deployment's list opened by two processes; other is another deployment's.
func RunReceiptWrites(t *testing.T, a, b, other dal.ReceiptWrites) {
	ctx := context.Background()
	if err := a.Push(ctx, []string{"r1", "r2"}); err != nil {
		t.Fatal(err)
	}
	if err := b.PushTo(ctx, []string{b.Owner(), other.Owner()}, map[string][]string{b.Owner(): {"r3"}, other.Owner(): {"o1"}}); err != nil {
		t.Fatal(err)
	}
	if n, _ := a.Len(ctx); n != 3 {
		t.Fatalf("%d on the list, want 3", n)
	}
	if n, _ := other.Len(ctx); n != 1 {
		t.Fatalf("%d on the other deployment's list, want 1", n)
	}
	if n, _ := a.TakeTurn(ctx, "ta", time.Minute); n != 1 {
		t.Fatal("a could not take the turn")
	}
	if n, _ := b.TakeTurn(ctx, "tb", time.Minute); n != 0 {
		t.Fatal("two writers at once")
	}
	head, err := b.Head(ctx, 2)
	if err != nil || len(head) != 2 || head[0] != "r1" || head[1] != "r2" {
		t.Fatalf("head %v %v", head, err)
	}
	if n, _ := b.Done(ctx, "tb", 2, nil); n != 0 {
		t.Fatal("a process without the turn cut the list")
	}
	if n, err := a.Done(ctx, "ta", 2, []string{"r2-again"}); err != nil || n != 1 {
		t.Fatalf("done %d %v", n, err)
	}
	if head, _ := b.Head(ctx, 10); len(head) != 2 || head[0] != "r3" || head[1] != "r2-again" {
		t.Fatalf("after done: %v", head)
	}
	_ = a.DropTurn(ctx, "ta")
	if n, _ := b.TakeTurn(ctx, "tb", time.Minute); n != 1 {
		t.Fatal("the turn was not let go")
	}
	_ = b.DropTurn(ctx, "tb")
}

// RunHandOffs is the hand-off lists' contract: a and b are two gateways'
// handles on one main Redis and one queue store.
func RunHandOffs(t *testing.T, a, b dal.HandOffs) {
	ctx := context.Background()
	st := stamp(t)
	owner := "gw" + st
	t.Run("one id for one queue store", func(t *testing.T) {
		ia, err := a.QueueStoreID(ctx, "id-a")
		if err != nil {
			t.Fatal(err)
		}
		ib, _ := b.QueueStoreID(ctx, "id-b")
		if ia == "" || ia != ib {
			t.Fatalf("two ids for one store: %q %q", ia, ib)
		}
		if err := a.NameStore(ctx, owner, ia); err != nil {
			t.Fatal(err)
		}
		if all, _ := b.Stores(ctx); all[owner] != ia {
			t.Fatalf("stores %v", all)
		}
		_ = b.UnnameStore(ctx, owner)
		if all, _ := a.Stores(ctx); all[owner] != "" {
			t.Fatal("unnamed store still named")
		}
	})
	t.Run("parked items are stamped, read and trimmed under a turn", func(t *testing.T) {
		if err := a.Park(ctx, owner, "dlr", []string{`{"n":1}`, `{"n":2}`}); err != nil {
			t.Fatal(err)
		}
		n, head, now, err := b.Waiting(ctx, owner, "dlr")
		if err != nil || n != 2 || head == "" || now.IsZero() {
			t.Fatalf("waiting %d %q %v %v", n, head, now, err)
		}
		items, _ := b.Head(ctx, owner, "dlr", 10)
		if len(items) != 2 || items[0] != head {
			t.Fatalf("head %v", items)
		}
		if k, _ := b.TakeTurn(ctx, owner, "dlr", "tb", time.Minute); k != 1 {
			t.Fatal("turn")
		}
		if k, _ := a.Trim(ctx, owner, "dlr", "ta", 1); k != 0 {
			t.Fatal("trimmed without the turn")
		}
		if k, _ := b.Trim(ctx, owner, "dlr", "tb", 1); k != 1 {
			t.Fatal("trim")
		}
		if n, _, _, _ := a.Waiting(ctx, owner, "dlr"); n != 1 {
			t.Fatalf("%d left, want 1", n)
		}
		_, _ = b.Trim(ctx, owner, "dlr", "tb", 1)
		_ = b.DropTurn(ctx, owner, "dlr", "tb")
		if _, err := a.Now(ctx); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("one stray walk at a time", func(t *testing.T) {
		if ok, _ := a.TakeStrays(ctx, owner, "mo", "ta", time.Minute); !ok {
			t.Fatal("first walk refused")
		}
		if ok, _ := b.TakeStrays(ctx, owner, "mo", "tb", time.Minute); ok {
			t.Fatal("two walks at once")
		}
		_ = a.DropStrays(ctx, owner, "mo", "ta")
		if ok, _ := b.TakeStrays(ctx, owner, "mo", "tb", time.Minute); !ok {
			t.Fatal("the walk's lock was not let go")
		}
		_ = b.DropStrays(ctx, owner, "mo", "tb")
	})
}

// RunConcatBuffer is the multi-part buffer's contract: mtA and mtB are two
// gateways' MT buffers, mo an MO buffer on the same database.
func RunConcatBuffer(t *testing.T, mtA, mtB, mo dal.ConcatBuffer) {
	ctx := context.Background()
	acct := time.Now().UnixNano()%1_000_000_000 + 1
	key := strconv.FormatInt(acct, 10) + "|S|923001234567|9|3"
	part := func(seq int, body string) (string, string) {
		return "p" + strconv.Itoa(seq), `{"ID":"p` + strconv.Itoa(seq) + `","Body":"` + body + `","ConcatSeq":` + strconv.Itoa(seq) + `,"ConcatTotal":3}`
	}
	t.Run("the gateway with the last part takes the group, once", func(t *testing.T) {
		_, p1 := part(1, "a")
		_, p2 := part(2, "b")
		_, p3 := part(3, "c")
		if got, err := mtA.AddPart(ctx, key, acct, 1, 3, p1, time.Minute); err != nil || got != nil {
			t.Fatalf("one part: %v %v", got, err)
		}
		if got, _ := mtB.AddPart(ctx, key, acct, 2, 3, p2, time.Minute); got != nil {
			t.Fatal("two parts made a group of three")
		}
		if got, _ := mtB.AddPart(ctx, key, acct, 2, 3, p2, time.Minute); got != nil {
			t.Fatal("a part sent again counted twice")
		}
		got, err := mtA.AddPart(ctx, key, acct, 3, 3, p3, time.Minute)
		if err != nil || len(got) != 3 {
			t.Fatalf("the last part: %v %v", got, err)
		}
		bodies := map[string]bool{}
		for _, m := range got {
			bodies[m.Body] = true
		}
		if !bodies["a"] || !bodies["b"] || !bodies["c"] {
			t.Fatalf("parts %v", bodies)
		}
	})
	t.Run("an expired group is claimed once, by its own side", func(t *testing.T) {
		k := strconv.FormatInt(acct, 10) + "|S|923001234567|10|2"
		_, p1 := part(1, "x")
		if _, err := mtA.AddPart(ctx, k, acct, 1, 2, p1, time.Minute); err != nil {
			t.Fatal(err)
		}
		moKey := "mo|" + strconv.FormatInt(acct, 10) + "|S|D|10|2"
		if _, err := mo.AddPart(ctx, moKey, 0, 1, 2, p1, time.Minute); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1100 * time.Millisecond)
		var mine, theirs []string
		claim := func(b dal.ConcatBuffer, into *[]string) {
			if err := b.ClaimExpired(ctx, time.Millisecond, func(key string, parts []*model.Message, bad error) {
				if bad == nil && (key == k || key == moKey) {
					*into = append(*into, key)
				}
			}); err != nil {
				t.Fatal(err)
			}
		}
		claim(mtA, &mine)
		claim(mtB, &theirs)
		if len(mine)+len(theirs) != 1 || append(mine, theirs...)[0] != k {
			t.Fatalf("MT claims %v and %v, want the MT group once", mine, theirs)
		}
		var moGot []string
		claim(mo, &moGot)
		if len(moGot) != 1 || moGot[0] != moKey {
			t.Fatalf("MO claims %v", moGot)
		}
	})
}

// RunVendorQuality is vendor quality's contract: a and b are two processes'.
func RunVendorQuality(t *testing.T, a, b dal.VendorQuality) {
	ctx := context.Background()
	vendor := time.Now().UnixNano()%1_000_000_000 + 1
	k := dal.QualityKey{VendorID: vendor, MCC: "410", MNC: "03"}
	min := time.Now().UTC().Truncate(time.Minute)
	if err := a.Add(ctx, []dal.QualityBucket{{Minute: min, Key: k, Counts: dal.QualityCounts{Delivered: 3, Judged: 4}}}); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(ctx, []dal.QualityBucket{{Minute: min, Key: k, Counts: dal.QualityCounts{Delivered: 1, Judged: 2}}}); err != nil {
		t.Fatal(err)
	}
	got, err := a.Load(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if c := got[k]; c.Delivered != 4 || c.Judged != 6 {
		t.Fatalf("counts %+v, want 4 of 6 (both processes' added)", c)
	}
}
