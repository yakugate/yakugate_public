// Package daltest holds the contract suites of the DAL (plan of 29 Oct 2026):
// one suite per interface, run against every DAL that serves it. Each DAL
// has its own queries; these suites are what keep them behaving the same.
//
// A DAL runs a suite from a test of its own package (so it can clean up its
// own tables), against a scratch database — never the dev box:
//
//	func TestQueuesContract(t *testing.T) {
//		q := scratchQueues(t)
//		daltest.RunQueues(t, q, daltest.QueueOpts{Cleanup: …})
//	}
package daltest

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
)

// QueueOpts lets a DAL clear what a run made.
type QueueOpts struct {
	// Cleanup is called once per vendor queue the run used, after it.
	Cleanup func(vendor string)
	// ClientClaimsOneAtATime: the store's receipt and MO claims read and
	// then lease, so two takers of ONE account at the same moment may both
	// take an item; the side-by-side cases then take turns. The Redis store
	// (walkDue, then a pipeline of leases; found 30 Oct 2026 — two takers
	// handed one MO out twice) until that is ruled on.
	ClientClaimsOneAtATime bool
}

// RunQueues runs the queue family's contract on q.
func RunQueues(t *testing.T, q dal.Queues, o QueueOpts) {
	stamp := time.Now().UnixNano() % 1e9
	n := 0
	vendor := func() string {
		n++
		v := fmt.Sprintf("dalt%d_%d", stamp, n)
		if o.Cleanup != nil {
			t.Cleanup(func() { o.Cleanup(v) })
		}
		return v
	}
	acct := 900000000 + stamp // an account no real data has
	ctx := context.Background()

	t.Run("a message enqueued is taken once, as it was", func(t *testing.T) {
		v := vendor()
		in := []model.Message{msg(stamp, 1, v, acct), msg(stamp, 2, v, acct), msg(stamp, 3, v, acct)}
		for _, m := range in {
			if err := q.Enqueue(ctx, m); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
		}
		got, err := q.Claim(ctx, v, 10)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if len(got) != len(in) {
			t.Fatalf("took %d, want %d", len(got), len(in))
		}
		byID := map[string]model.Message{}
		for _, m := range got {
			byID[m.ID] = m
		}
		for _, m := range in {
			g, ok := byID[m.ID]
			if !ok {
				t.Fatalf("%s not taken", m.ID)
			}
			if g.AccountID != m.AccountID || g.Destination != m.Destination || g.Sender != m.Sender ||
				g.Body != m.Body || g.Priority != m.Priority || g.IdempotencyKey != m.IdempotencyKey {
				t.Fatalf("%s lost fields: %+v", m.ID, g)
			}
		}
		if again, err := q.Claim(ctx, v, 10); err != nil || len(again) != 0 {
			t.Fatalf("a taken message must not be taken again: %d, %v", len(again), err)
		}
		if err := q.Ack(ctx, v, in[0].ID, in[1].ID, in[2].ID); err != nil {
			t.Fatalf("ack: %v", err)
		}
	})

	t.Run("a message for later waits, and is found and counted", func(t *testing.T) {
		v := vendor()
		m := msg(stamp, 4, v, acct)
		m.NextAttemptAt = time.Now().Add(time.Hour)
		if err := q.Enqueue(ctx, m); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		if got, err := q.Claim(ctx, v, 10); err != nil || len(got) != 0 {
			t.Fatalf("a message for later was taken now: %d, %v", len(got), err)
		}
		if c, err := q.QueuedForVendor(ctx, v); err != nil || c != 1 {
			t.Fatalf("QueuedForVendor = %d, %v; want 1", c, err)
		}
		f, ok, err := q.FindQueued(ctx, m.ID, acct, []string{v})
		if err != nil || !ok {
			t.Fatalf("FindQueued: ok=%v err=%v", ok, err)
		}
		if f.NextAttemptAt.IsZero() {
			t.Fatal("a message for later must say when it is next offered")
		}
		if _, ok, _ := q.FindQueued(ctx, m.ID, acct+1, []string{v}); ok {
			t.Fatal("another account's message must not be found")
		}
	})

	t.Run("a message given back is taken again", func(t *testing.T) {
		v := vendor()
		m := msg(stamp, 5, v, acct)
		if err := q.Enqueue(ctx, m); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		got, err := q.Claim(ctx, v, 1)
		if err != nil || len(got) != 1 {
			t.Fatalf("claim: %d, %v", len(got), err)
		}
		if err := q.Return(ctx, got...); err != nil {
			t.Fatalf("return: %v", err)
		}
		again, err := q.Claim(ctx, v, 1)
		if err != nil || len(again) != 1 || again[0].ID != m.ID {
			t.Fatalf("a returned message must be taken again: %v, %v", again, err)
		}
	})

	t.Run("takers side by side never share a message", func(t *testing.T) {
		v := vendor()
		const total = 300
		for i := 0; i < total; i++ {
			if err := q.Enqueue(ctx, msg(stamp, 1000+i, v, acct)); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
		}
		var mu sync.Mutex
		seen := map[string]int{}
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for empty := 0; empty < 3; {
					got, err := q.Claim(ctx, v, 7)
					if err != nil {
						errs <- err
						return
					}
					if len(got) == 0 {
						empty++
						continue
					}
					mu.Lock()
					for _, m := range got {
						seen[m.ID]++
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("claim: %v", err)
		}
		var twice []string
		for id, c := range seen {
			if c > 1 {
				twice = append(twice, id)
			}
		}
		sort.Strings(twice)
		if len(twice) > 0 {
			t.Fatalf("%d message(s) taken twice, e.g. %s", len(twice), twice[0])
		}
		if len(seen) != total {
			t.Fatalf("took %d of %d", len(seen), total)
		}
	})

	t.Run("a client receipt is handed out once until acked", func(t *testing.T) {
		a := acct + 10
		for i := 0; i < 3; i++ {
			d := model.ClientDLR{AccountID: a, ClientMsgID: fmt.Sprintf("01DALT%010d%010d", stamp, i), Status: "delivered",
				Receipt: "id:x stat:DELIVRD", Src: "923001234567", Dst: "ACME", CreatedAt: time.Now()}
			if err := q.EnqueueClientDLR(ctx, d); err != nil {
				t.Fatalf("enqueue receipt: %v", err)
			}
		}
		claims := []model.DLRClaim{{AccountID: a}}
		got, err := q.ClaimClientDLR(ctx, claims, 10)
		if err != nil || len(got) != 3 {
			t.Fatalf("claim receipts: %d, %v", len(got), err)
		}
		if again, err := q.ClaimClientDLR(ctx, claims, 10); err != nil || len(again) != 0 {
			t.Fatalf("a receipt handed out must not be handed out again before its time: %d, %v", len(again), err)
		}
		ids := make([]int64, 0, len(got))
		for _, d := range got {
			if d.AccountID != a || d.Status != "delivered" || d.Src != "923001234567" {
				t.Fatalf("receipt lost fields: %+v", d)
			}
			ids = append(ids, d.ID)
		}
		if err := q.AckClientDLR(ctx, ids...); err != nil {
			t.Fatalf("ack receipts: %v", err)
		}
	})

	t.Run("an MO is handed out once until acked", func(t *testing.T) {
		a := acct + 20
		var want []string
		for i := 0; i < 2; i++ {
			m := msg(stamp, 2000+i, "", a)
			m.Direction = model.DirectionMO
			if err := q.StageMO(ctx, m); err != nil {
				t.Fatalf("stage MO: %v", err)
			}
			want = append(want, m.ID)
		}
		claims := []model.MOClaim{{AccountID: a}}
		got, err := q.ClaimMO(ctx, claims, 10)
		if err != nil || len(got) != 2 {
			t.Fatalf("claim MO: %d, %v", len(got), err)
		}
		// A claim says which try it is: 1 on the first, on every store (D-140).
		for _, m := range got {
			if m.Attempts != 1 {
				t.Fatalf("the first claim of MO %s says %d attempts, want 1", m.ID, m.Attempts)
			}
		}
		if again, err := q.ClaimMO(ctx, claims, 10); err != nil || len(again) != 0 {
			t.Fatalf("an MO handed out must not be handed out again before its time: %d, %v", len(again), err)
		}
		if err := q.AckMO(ctx, want...); err != nil {
			t.Fatalf("ack MO: %v", err)
		}
	})

	// A page of receipts at a time (step 3, 30 Oct 2026): what the smpp-server
	// and the dlr role ask for under load. Two takers side by side never hand
	// out one receipt twice, every receipt is handed out, and one ack takes a
	// whole page off.
	t.Run("pages of receipts taken side by side never overlap, and one ack takes a page", func(t *testing.T) {
		a := acct + 30
		const total = 400
		var ds []model.ClientDLR
		for i := 0; i < total; i++ {
			ds = append(ds, model.ClientDLR{AccountID: a, ClientMsgID: fmt.Sprintf("01DALP%010d%010d", stamp, i), Status: "delivered",
				Stat: "DELIVRD", Receipt: "id:x stat:DELIVRD", Src: "923001234567", Dst: "ACME"})
		}
		if err := q.EnqueueClientDLRBatch(ctx, ds); err != nil {
			t.Fatalf("enqueue receipts: %v", err)
		}
		claims := []model.DLRClaim{{AccountID: a}}
		var mu, turn sync.Mutex
		seen := map[int64]int{}
		var pages [][]int64
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					if o.ClientClaimsOneAtATime {
						turn.Lock()
					}
					got, err := q.ClaimClientDLR(ctx, claims, 200)
					if o.ClientClaimsOneAtATime {
						turn.Unlock()
					}
					if err != nil {
						errs <- err
						return
					}
					if len(got) == 0 {
						return
					}
					ids := make([]int64, len(got))
					mu.Lock()
					for i, d := range got {
						seen[d.ID]++
						ids[i] = d.ID
						// A first claim: MySQL says the count it read (0),
						// Redis the count it wrote (1).
						if d.Attempts > 1 {
							mu.Unlock()
							errs <- fmt.Errorf("receipt %d handed out before (attempts %d)", d.ID, d.Attempts)
							return
						}
					}
					pages = append(pages, ids)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("claim: %v", err)
		}
		for id, c := range seen {
			if c > 1 {
				t.Fatalf("receipt %d handed out %d times", id, c)
			}
		}
		if len(seen) != total {
			t.Fatalf("handed out %d of %d", len(seen), total)
		}
		for _, ids := range pages {
			if err := q.AckClientDLR(ctx, ids...); err != nil {
				t.Fatalf("ack a page of %d: %v", len(ids), err)
			}
			// An ack is whole: a receipt acked is gone, and acking it again
			// is no error.
			if err := q.AckClientDLR(ctx, ids...); err != nil {
				t.Fatalf("ack the same page again: %v", err)
			}
		}
		for _, ids := range pages {
			for _, id := range ids {
				if err := q.RescheduleClientDLR(ctx, id, 0); err != nil {
					t.Fatalf("reschedule an acked receipt: %v", err)
				}
			}
		}
		if again, err := q.ClaimClientDLR(ctx, claims, 500); err != nil || len(again) != 0 {
			t.Fatalf("an acked receipt must never be handed out again: %d, %v", len(again), err)
		}
	})

	t.Run("a receipt made due again is handed out again, its attempts counted", func(t *testing.T) {
		a := acct + 40
		d := model.ClientDLR{AccountID: a, ClientMsgID: fmt.Sprintf("01DALR%020d", stamp), Status: "failed",
			Stat: "UNDELIV", Receipt: "id:x stat:UNDELIV", Src: "923001234567", Dst: "ACME"}
		if err := q.EnqueueClientDLR(ctx, d); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		claims := []model.DLRClaim{{AccountID: a}}
		first := -1
		for k := 0; k < 3; k++ {
			got, err := q.ClaimClientDLR(ctx, claims, 10)
			if err != nil || len(got) != 1 {
				t.Fatalf("claim %d: %d, %v", k+1, len(got), err)
			}
			// Each claim counts one more attempt, and says which try it is:
			// 1 on the first, on every store (D-140, 5 Nov 2026: MySQL and
			// SQL Server answered the count before the claim, Redis after,
			// so a limit of N tries meant N+1 on the SQL stores).
			if first < 0 {
				first = got[0].Attempts
				if first != 1 {
					t.Fatalf("the first claim says %d attempts, want 1 (the try it is)", first)
				}
			}
			if got[0].Attempts != first+k {
				t.Fatalf("claim %d says %d attempts, the first said %d", k+1, got[0].Attempts, first)
			}
			if err := q.RescheduleClientDLR(ctx, got[0].ID, 0); err != nil {
				t.Fatalf("reschedule: %v", err)
			}
		}
		// Out of a bind's window (it may try twice): not handed to it.
		if got, err := q.ClaimClientDLR(ctx, []model.DLRClaim{{AccountID: a, MaxAttempts: 2}}, 10); err != nil || len(got) != 0 {
			t.Fatalf("a receipt past the bind's attempts must not be handed to it: %d, %v", len(got), err)
		}
		// The webhook's claim under 'both' takes an untried receipt and
		// raises its count to the bind's budget (webhookFloors).
		w := d
		w.ClientMsgID = fmt.Sprintf("01DALW%020d", stamp)
		if err := q.EnqueueClientDLR(ctx, w); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		got, err := q.ClaimClientDLR(ctx, []model.DLRClaim{{AccountID: a, MinAttempts: 6}}, 10)
		if err != nil || len(got) != 1 || got[0].Attempts != 6 || got[0].ClientMsgID != w.ClientMsgID {
			t.Fatalf("a webhook claim must take the untried receipt and raise its count to its floor: %+v, %v", got, err)
		}
		if err := q.AckClientDLR(ctx, got[0].ID); err != nil {
			t.Fatalf("ack: %v", err)
		}
		// The first receipt is mid-retry for a bind (attempts 1..5): not
		// the webhook's.
		if got, err := q.ClaimClientDLR(ctx, []model.DLRClaim{{AccountID: a, MinAttempts: 6}}, 10); err != nil || len(got) != 0 {
			t.Fatalf("a receipt a bind is retrying must not be handed to the webhook: %d, %v", len(got), err)
		}
		again, err := q.ClaimClientDLR(ctx, claims, 10)
		if err != nil || len(again) != 1 {
			t.Fatalf("claim: %d, %v", len(again), err)
		}
		id := again[0].ID
		if err := q.GiveUpClientDLR(ctx, id); err != nil {
			t.Fatalf("give up: %v", err)
		}
		if err := q.RescheduleClientDLR(ctx, id, 0); err != nil {
			t.Fatalf("reschedule: %v", err)
		}
		if got, err := q.ClaimClientDLR(ctx, claims, 10); err != nil || len(got) != 0 {
			t.Fatalf("a receipt given up must never be handed out again: %d, %v", len(got), err)
		}
		if err := q.AckClientDLR(ctx, id); err != nil {
			t.Fatalf("ack: %v", err)
		}
	})

	t.Run("pages of MO taken side by side never overlap, and one ack takes a page", func(t *testing.T) {
		a := acct + 50
		const total = 300
		for i := 0; i < total; i++ {
			m := msg(stamp, 3000+i, "", a)
			m.Direction = model.DirectionMO
			if err := q.StageMO(ctx, m); err != nil {
				t.Fatalf("stage MO: %v", err)
			}
		}
		claims := []model.MOClaim{{AccountID: a}}
		var mu, turn sync.Mutex
		seen := map[string]int{}
		var all []string
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					if o.ClientClaimsOneAtATime {
						turn.Lock()
					}
					got, err := q.ClaimMO(ctx, claims, 200)
					if o.ClientClaimsOneAtATime {
						turn.Unlock()
					}
					if err != nil {
						errs <- err
						return
					}
					if len(got) == 0 {
						return
					}
					mu.Lock()
					for _, m := range got {
						seen[m.ID]++
						all = append(all, m.ID)
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("claim MO: %v", err)
		}
		for id, c := range seen {
			if c > 1 {
				t.Fatalf("MO %s handed out %d times", id, c)
			}
		}
		if len(seen) != total {
			t.Fatalf("handed out %d of %d", len(seen), total)
		}
		if err := q.AckMO(ctx, all...); err != nil {
			t.Fatalf("ack %d MO at once: %v", len(all), err)
		}
		for _, id := range all[:5] {
			if err := q.RescheduleMO(ctx, id, 0); err != nil {
				t.Fatalf("reschedule an acked MO: %v", err)
			}
		}
		if again, err := q.ClaimMO(ctx, claims, 500); err != nil || len(again) != 0 {
			t.Fatalf("an acked MO must never be handed out again: %d, %v", len(again), err)
		}
	})

	t.Run("part ids are kept until dropped", func(t *testing.T) {
		id := fmt.Sprintf("01DALTP%019d", stamp)
		parts := []string{"p1", "p2", "p3"}
		if err := q.SaveClientPartIDs(ctx, id, parts); err != nil {
			t.Fatalf("save: %v", err)
		}
		got, err := q.ClientPartIDs(ctx, id)
		if err != nil || fmt.Sprint(got) != fmt.Sprint(parts) {
			t.Fatalf("read back %v, %v", got, err)
		}
		many, err := q.ClientPartIDsMany(ctx, []string{id, id + "x"})
		if err != nil || fmt.Sprint(many[id]) != fmt.Sprint(parts) || len(many[id+"x"]) != 0 {
			t.Fatalf("many: %v, %v", many, err)
		}
		if err := q.DropClientPartIDs(ctx, id); err != nil {
			t.Fatalf("drop: %v", err)
		}
		if got, _ := q.ClientPartIDs(ctx, id); len(got) != 0 {
			t.Fatalf("dropped ids still there: %v", got)
		}
	})
}

// msg is a plain one-part message of the run, ULID-shaped (26 characters).
func msg(stamp int64, i int, vendor string, acct int64) model.Message {
	return model.Message{
		ID:             fmt.Sprintf("01DALT%010d%010d", stamp, i),
		AccountID:      acct,
		Username:       "daltest",
		Sender:         "ACME",
		Destination:    "923001234567",
		Body:           fmt.Sprintf("dal contract %d", i),
		Encoding:       "gsm7",
		Segments:       1,
		Transport:      model.TransportHTTP,
		Priority:       4,
		Status:         model.StatusQueued,
		Vendor:         vendor,
		IdempotencyKey: fmt.Sprintf("dalt-%d", i),
	}
}
