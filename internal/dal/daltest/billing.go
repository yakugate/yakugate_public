package daltest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// The billing family's contracts (dal/billing.go). Today Redis serves the
// live balances and the billing queues and MySQL the billing store; step 3
// gives MySQL the first two, step 4 SQL Server all of them, and each runs the
// same suites.

// LiveBalanceOpts lets a DAL run the live-balance suite on companies no real
// data has.
type LiveBalanceOpts struct {
	// FirstCompany is the first of 20 company ids the run may use; their
	// counters are removed afterwards through the interface (Drop).
	FirstCompany int64
	// DropAll runs the DropAll case, which removes EVERY counter of the store:
	// only on a store of the test's own.
	DropAll bool
}

// RunLiveBalances runs the live balance counters' contract on l.
func RunLiveBalances(t *testing.T, l dal.LiveBalances, o LiveBalanceOpts) {
	ctx := context.Background()
	next := o.FirstCompany
	company := func() int64 {
		next++
		co := next
		_ = l.Drop(ctx, 0, co)
		t.Cleanup(func() { _ = l.Drop(context.Background(), 0, co) })
		return co
	}

	t.Run("a deduction is one move that answers the balance after", func(t *testing.T) {
		co := company()
		if made, err := l.SeedMissing(ctx, []dal.CounterSeed{{Company: co, Units: 300}}); err != nil || made != 1 {
			t.Fatalf("seed: %d %v", made, err)
		}
		if left, err := l.Deduct(ctx, co, 230); err != nil || left != 70 {
			t.Fatalf("deduct 230 of 300: %d %v", left, err)
		}
		if left, err := l.Deduct(ctx, co, 100); err != nil || left != -30 {
			t.Fatalf("a deduction may go below zero: %d %v", left, err)
		}
		if left, err := l.Deduct(ctx, co, -30); err != nil || left != 0 {
			t.Fatalf("a give-back is the same move the other way: %d %v", left, err)
		}
		if seeded, err := l.IsSeeded(ctx, co); err != nil || !seeded {
			t.Fatalf("a seeded counter is on the index: %v %v", seeded, err)
		}
	})

	t.Run("a counter made from nothing is seeded on top, once", func(t *testing.T) {
		co := company()
		if left, err := l.Deduct(ctx, co, 50); err != nil || left != -50 {
			t.Fatalf("deduct from nothing: %d %v", left, err)
		}
		if seeded, _ := l.IsSeeded(ctx, co); seeded {
			t.Fatal("a counter made by a deduction is not seeded")
		}
		if v, err := l.SeedOnTop(ctx, co, 1000); err != nil || v != 950 {
			t.Fatalf("seed on top: %d %v, want 950", v, err)
		}
		if v, err := l.SeedOnTop(ctx, co, 1000); err != nil || v != 950 {
			t.Fatalf("a second seed on top changes nothing: %d %v", v, err)
		}
	})

	t.Run("many deductions answer in order", func(t *testing.T) {
		a, b := company(), company()
		if _, err := l.SeedMissing(ctx, []dal.CounterSeed{{Company: a, Units: 100}, {Company: b, Units: 10}}); err != nil {
			t.Fatal(err)
		}
		got := l.DeductMany(ctx, []dal.Deduction{{Company: a, Units: 40}, {Company: b, Units: 15}, {Company: a, Units: 1}})
		if len(got) != 3 || got[0].Left != 60 || got[1].Left != -5 || got[2].Left != 59 {
			t.Fatalf("deduct many: %+v", got)
		}
		for _, d := range got {
			if d.Err != nil {
				t.Fatal(d.Err)
			}
		}
		if out := l.DeductMany(ctx, nil); len(out) != 0 {
			t.Fatalf("nothing asked, nothing answered: %v", out)
		}
	})

	t.Run("seeding never overwrites a live counter", func(t *testing.T) {
		a, b := company(), company()
		if err := l.Seed(ctx, 0, a, 500); err != nil {
			t.Fatal(err)
		}
		l.Deduct(ctx, a, 100)
		made, err := l.SeedMissing(ctx, []dal.CounterSeed{{Company: a, Units: 999}, {Company: b, Units: 7}})
		if err != nil || made != 1 {
			t.Fatalf("seed missing: %d %v", made, err)
		}
		if v, ok, _ := l.Current(ctx, 0, a); !ok || v != 400 {
			t.Fatalf("a live counter is the truth: %d %v", v, ok)
		}
		if err := l.Seed(ctx, 0, b, 1); err != nil {
			t.Fatal(err)
		}
		if v, _, _ := l.Current(ctx, 0, b); v != 7 {
			t.Fatalf("a second seed changes nothing: %d", v)
		}
	})

	t.Run("a hold takes only what is covered", func(t *testing.T) {
		co := company()
		if r, err := l.Hold(ctx, 0, co, 10); err != nil || r != -2 {
			t.Fatalf("no counter: %d %v, want -2", r, err)
		}
		if r, err := l.HoldLeft(ctx, 0, co, 10); err != nil || len(r) != 1 || r[0] != -2 {
			t.Fatalf("no counter (left): %v %v", r, err)
		}
		l.Seed(ctx, 0, co, 100)
		if r, err := l.Hold(ctx, 0, co, 60); err != nil || r != 1 {
			t.Fatalf("hold 60 of 100: %d %v", r, err)
		}
		if r, err := l.HoldLeft(ctx, 0, co, 30); err != nil || len(r) != 2 || r[0] != 1 || r[1] != 10 {
			t.Fatalf("hold 30 of 40: %v %v", r, err)
		}
		if r, err := l.HoldLeft(ctx, 0, co, 11); err != nil || len(r) != 1 || r[0] != 0 {
			t.Fatalf("hold 11 of 10: %v %v", r, err)
		}
		if r, err := l.Hold(ctx, 0, co, 11); err != nil || r != 0 {
			t.Fatalf("hold 11 of 10: %d %v", r, err)
		}
	})

	t.Run("holds at once never take more than there is", func(t *testing.T) {
		co := company()
		l.Seed(ctx, 0, co, 20)
		var wg sync.WaitGroup
		var mu sync.Mutex
		won := 0
		for i := 0; i < 60; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if r, err := l.HoldLeft(ctx, 0, co, 1); err == nil && len(r) == 2 && r[0] == 1 {
					mu.Lock()
					won++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if v, _, _ := l.Current(ctx, 0, co); won != 20 || v != 0 {
			t.Fatalf("%d holds of 1 won on 20, counter %d", won, v)
		}
	})

	t.Run("a give-back adds only to a counter that is there", func(t *testing.T) {
		co := company()
		if ok, err := l.AddIfThere(ctx, 0, co, 5); err != nil || ok {
			t.Fatalf("no counter: %v %v", ok, err)
		}
		if _, ok, _ := l.Current(ctx, 0, co); ok {
			t.Fatal("a give-back to nothing must not make a counter")
		}
		l.Seed(ctx, 0, co, 10)
		if ok, err := l.AddIfThere(ctx, 0, co, 5); err != nil || !ok {
			t.Fatalf("give back: %v %v", ok, err)
		}
		if err := l.Add(ctx, 0, co, 1); err != nil {
			t.Fatal(err)
		}
		if v, _, _ := l.Current(ctx, 0, co); v != 16 {
			t.Fatalf("counter %d, want 16", v)
		}
	})

	t.Run("an unlinked account has a counter of its own", func(t *testing.T) {
		acct := o.FirstCompany + 9000
		_ = l.Drop(ctx, acct, 0)
		t.Cleanup(func() { _ = l.Drop(context.Background(), acct, 0) })
		if err := l.Seed(ctx, acct, 0, 40); err != nil {
			t.Fatal(err)
		}
		if r, err := l.Hold(ctx, acct, 0, 15); err != nil || r != 1 {
			t.Fatalf("hold on an account: %d %v", r, err)
		}
		if v, ok, _ := l.Current(ctx, acct, 0); !ok || v != 25 {
			t.Fatalf("account counter %d %v", v, ok)
		}
	})

	t.Run("the index lists what was seeded; a gone counter is forgotten", func(t *testing.T) {
		co := company()
		l.Seed(ctx, 0, co, 3)
		find := func() (dal.Counter, bool) {
			cs, err := l.Seeded(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range cs {
				if c.Company == co {
					return c, true
				}
			}
			return dal.Counter{}, false
		}
		c, ok := find()
		if !ok || c.Key == "" || c.Account != 0 {
			t.Fatalf("seeded counter not listed: %+v %v", c, ok)
		}
		if v, found, err := l.Read(ctx, c); err != nil || !found || v != 3 {
			t.Fatalf("read: %d %v %v", v, found, err)
		}
		if err := l.Drop(ctx, 0, co); err != nil {
			t.Fatal(err)
		}
		if _, ok := find(); ok {
			t.Fatal("a dropped counter leaves the index")
		}
		if _, found, err := l.Read(ctx, c); err != nil || found {
			t.Fatalf("a dropped counter reads as gone: %v %v", found, err)
		}
		if err := l.Forget(ctx, c); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a manual change adds to a live counter, else sets it", func(t *testing.T) {
		co := company()
		if err := l.Credit(ctx, co, 500, 2000); err != nil {
			t.Fatal(err)
		}
		if v, _, _ := l.Current(ctx, 0, co); v != 2000 {
			t.Fatalf("no counter: made at the total, got %d", v)
		}
		l.Deduct(ctx, co, 300)
		if err := l.Credit(ctx, co, 500, 2500); err != nil {
			t.Fatal(err)
		}
		if v, _, _ := l.Current(ctx, 0, co); v != 2200 {
			t.Fatalf("a live counter is added to, got %d", v)
		}
		if err := l.Undo(ctx, co, 500); err != nil {
			t.Fatal(err)
		}
		if v, _, _ := l.Current(ctx, 0, co); v != 1700 {
			t.Fatalf("undone: %d", v)
		}
		if seeded, _ := l.IsSeeded(ctx, co); !seeded {
			t.Fatal("a credited counter is on the index")
		}
		if err := l.Credit(ctx, 0, 1, 1); err == nil {
			t.Fatal("a manual change needs a company")
		}
	})

	t.Run("an error that never reached the store is told apart", func(t *testing.T) {
		if l.Unsent(nil) {
			t.Fatal("no error is not an unsent one")
		}
		if l.Unsent(errors.New("i/o timeout")) {
			t.Fatal("a timeout may have run")
		}
	})

	if o.DropAll {
		t.Run("drop all removes every counter", func(t *testing.T) {
			a, b := company(), company()
			l.Seed(ctx, 0, a, 1)
			l.Seed(ctx, 0, b, 1)
			if n, err := l.DropAll(ctx); err != nil || n < 2 {
				t.Fatalf("drop all: %d %v", n, err)
			}
			if _, ok, _ := l.Current(ctx, 0, a); ok {
				t.Fatal("a counter is left")
			}
		})
	}
}

// RunBillingQueue runs the billing queues' contract on q, under scope (a
// deployment name no real data has).
func RunBillingQueue(t *testing.T, q dal.BillingQueue, scope string) {
	ctx := context.Background()
	lane := func(side string, n int) dal.Lane { return dal.Lane{Scope: scope, Side: side, Lane: n} }
	ev := func(at int64, m string) dal.Scored { return dal.Scored{Score: float64(at), Member: m} }
	now := time.Now().UnixMilli()

	t.Run("events put on a lane are counted and the lane named", func(t *testing.T) {
		err := q.Put(ctx, []dal.LaneAdd{
			{Lane: lane("sell", 0), Events: []dal.Scored{ev(now-5000, "s1"), ev(now-1000, "s2")}, Name: true},
			{Lane: lane("buy", 3), Events: []dal.Scored{ev(now-2000, "b1")}, Name: true},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := q.Put(ctx, []dal.LaneAdd{{Lane: lane("sell", 0), Events: []dal.Scored{ev(now-5000, "s1")}}}); err != nil {
			t.Fatal(err)
		}
		if err := q.Put(ctx, nil); err != nil {
			t.Fatalf("nothing to put: %v", err)
		}
		c, err := q.Count(ctx, lane("sell", 0))
		if err != nil || c.Ready != 2 || c.Delayed != 0 || c.Doing != 0 || !c.HasOldest || int64(c.Oldest) != now-5000 {
			t.Fatalf("sell lane 0: %+v %v (an event put twice is there once)", c, err)
		}
		names, err := q.Lanes(ctx, scope, "buy")
		if err != nil || len(names) != 1 || names[0] != "3" {
			t.Fatalf("buy lanes: %v %v", names, err)
		}
		n, oldest, err := q.Waiting(ctx, scope, []string{"sell", "buy"}, 2)
		if err != nil || n != 3 || oldest < 5000 {
			t.Fatalf("waiting: %d %d %v", n, oldest, err)
		}
	})

	t.Run("a lane is booked by one turn at a time", func(t *testing.T) {
		l := lane("sell", 0)
		if ok, err := q.TakeTurn(ctx, l, "A", 5*time.Second); err != nil || !ok {
			t.Fatalf("A takes: %v %v", ok, err)
		}
		if ok, _ := q.TakeTurn(ctx, l, "B", 5*time.Second); ok {
			t.Fatal("B must wait while A holds the turn")
		}
		if ok, _ := q.TakeTurn(ctx, l, "A", 5*time.Second); !ok {
			t.Fatal("A renews its own turn")
		}
		if got, err := q.Claim(ctx, l, "B", 10, "IDB", now); err != nil || len(got) != 0 {
			t.Fatalf("B claims nothing without the turn: %v %v", got, err)
		}
		q.DropTurn(ctx, l, "B")
		if ok, _ := q.TakeTurn(ctx, l, "B", 5*time.Second); ok {
			t.Fatal("B cannot drop A's turn")
		}
		q.DropTurn(ctx, l, "A")
		if ok, _ := q.TakeTurn(ctx, l, "B", 5*time.Second); !ok {
			t.Fatal("after A lets go B takes the turn")
		}
		q.DropTurn(ctx, l, "B")
	})

	t.Run("a batch is claimed, repeated until done, and the refused wait", func(t *testing.T) {
		l := lane("sell", 0)
		if ok, _ := q.TakeTurn(ctx, l, "T", 5*time.Second); !ok {
			t.Fatal("turn")
		}
		got, err := q.Claim(ctx, l, "T", 10, "ID1", now)
		if err != nil || len(got) != 4 || got[0] != "ID1" || got[1] != "0" || got[2] != "s1" || got[3] != "s2" {
			t.Fatalf("claim: %v %v (oldest first)", got, err)
		}
		again, err := q.Claim(ctx, l, "T", 10, "ID2", now)
		if err != nil || len(again) != 4 || again[0] != "ID1" || again[1] != "1" {
			t.Fatalf("a batch left in doing is handed out again under its id: %v %v", again, err)
		}
		if c, _ := q.Count(ctx, l); c.Ready != 0 || c.Doing != 2 {
			t.Fatalf("claimed events are doing: %+v", c)
		}
		if err := q.SetBatchID(ctx, l, "ID3"); err != nil {
			t.Fatal(err)
		}
		if again, _ := q.Claim(ctx, l, "T", 10, "ID4", now); len(again) < 1 || again[0] != "ID3" {
			t.Fatalf("a named batch keeps its name: %v", again)
		}
		if ok, err := q.Done(ctx, l, "X", nil); err != nil || ok != 0 {
			t.Fatalf("done without the turn: %d %v", ok, err)
		}
		if ok, err := q.Done(ctx, l, "T", []dal.Scored{ev(now+60000, "s2")}); err != nil || ok != 1 {
			t.Fatalf("done: %d %v", ok, err)
		}
		c, _ := q.Count(ctx, l)
		if c.Doing != 0 || c.Delayed != 1 || c.Ready != 0 {
			t.Fatalf("after done: %+v", c)
		}
		if got, _ := q.Claim(ctx, l, "T", 10, "ID5", now); len(got) != 0 {
			t.Fatalf("a delayed event waits for its time: %v", got)
		}
		q.Put(ctx, []dal.LaneAdd{{Lane: l, Events: []dal.Scored{ev(now, "s3")}}})
		got, _ = q.Claim(ctx, l, "T", 10, "ID6", now+61000)
		if len(got) != 4 || got[2] != "s2" || got[3] != "s3" {
			t.Fatalf("the due delayed first, then ready: %v", got)
		}
		if got2, _ := q.Claim(ctx, l, "T", 1, "ID7", now+61000); len(got2) != 4 {
			t.Fatalf("doing is handed whole, whatever the max: %v", got2)
		}
		q.Done(ctx, l, "T", nil)
		q.DropTurn(ctx, l, "T")
	})

	t.Run("a claim takes at most its max", func(t *testing.T) {
		l := lane("buy", 1)
		var evs []dal.Scored
		for i := 0; i < 5; i++ {
			evs = append(evs, ev(now+int64(i), fmt.Sprintf("m%d", i)))
		}
		q.Put(ctx, []dal.LaneAdd{{Lane: l, Events: evs}})
		q.TakeTurn(ctx, l, "T", 5*time.Second)
		got, _ := q.Claim(ctx, l, "T", 3, "IDM", now)
		if len(got) != 5 {
			t.Fatalf("three of five: %v", got)
		}
		members := append([]string(nil), got[2:]...)
		if !sort.StringsAreSorted(members) || members[0] != "m0" {
			t.Fatalf("the oldest three: %v", members)
		}
		q.Done(ctx, l, "T", nil)
		q.DropTurn(ctx, l, "T")
		if c, _ := q.Count(ctx, l); c.Ready != 2 {
			t.Fatalf("two left: %+v", c)
		}
	})

	t.Run("an empty lane is let go", func(t *testing.T) {
		l := lane("buy", 3)
		if empty, err := q.DropIfEmpty(ctx, l); err != nil || empty {
			t.Fatalf("a lane with an event is not empty: %v %v", empty, err)
		}
		q.TakeTurn(ctx, l, "T", 5*time.Second)
		q.Claim(ctx, l, "T", 10, "IDE", now)
		q.Done(ctx, l, "T", nil)
		q.DropTurn(ctx, l, "T")
		if empty, err := q.DropIfEmpty(ctx, l); err != nil || !empty {
			t.Fatalf("an empty lane: %v %v", empty, err)
		}
		// Only the emptied lane must leave: a store may also list a lane that
		// holds events and was never named (MySQL lists the lanes that hold
		// rows; buy lane 1 still holds two here), which is never wrong — a
		// lane listed is a lane booked.
		names, _ := q.Lanes(ctx, scope, "buy")
		for _, n := range names {
			if n == "3" {
				t.Fatalf("an empty lane leaves the list: %v", names)
			}
		}
	})

	t.Run("the mixed queue of 26 Oct is a side of its own", func(t *testing.T) {
		l := lane("", 0)
		q.Put(ctx, []dal.LaneAdd{{Lane: l, Events: []dal.Scored{ev(now, "x")}, Name: true}})
		if c, _ := q.Count(ctx, l); c.Ready != 1 {
			t.Fatalf("mixed: %+v", c)
		}
		if c, _ := q.Count(ctx, lane("sell", 0)); c.Ready != 0 {
			t.Fatalf("not on the sell side: %+v", c)
		}
		q.TakeTurn(ctx, l, "T", 5*time.Second)
		q.Claim(ctx, l, "T", 10, "IDX", now)
		q.Done(ctx, l, "T", nil)
		q.DropTurn(ctx, l, "T")
		q.DropIfEmpty(ctx, l)
	})

	if q.Unsent(nil) {
		t.Fatal("no error is not an unsent one")
	}
}

// UsageOpts lets the usage books' suite read what a booking wrote.
type UsageOpts struct {
	// Units reads the amount and items booked for a company, side and kind
	// (summed over its rows).
	Units func(company int64, side, kind string) (units, items int64)
	// Company is a company id no real data has; its rows are removed by
	// Cleanup.
	Company int64
	Cleanup func()
}

// RunUsageBooks runs the usage books' contract on u.
func RunUsageBooks(t *testing.T, u dal.UsageBooks, o UsageOpts) {
	ctx := context.Background()
	if o.Cleanup != nil {
		t.Cleanup(o.Cleanup)
	}
	bucket := time.Date(2026, 10, 29, 10, 15, 0, 0, time.UTC)
	row := func(units int64) dal.UsageRow {
		return dal.UsageRow{Bucket: bucket, Side: "sell", Company: o.Company, Account: 7, Vendor: 3, Kind: "mt",
			Model: "prepaid", Currency: "USD", CardCurrency: "USD", CompanyCurrency: "USD", FX: "1",
			Items: 1, Parts: 2, Units: units, Conv: true, CardUnits: units, CompanyUnits: units, BaseUnits: units}
	}
	guard := fmt.Sprintf("DALT%022d", time.Now().UnixNano()%1e18)

	t.Run("a batch is booked once under its guard", func(t *testing.T) {
		already, err := u.Book(ctx, guard, []dal.UsageRow{row(100), row(23)})
		if err != nil || already {
			t.Fatalf("book: %v %v", already, err)
		}
		if units, items := o.Units(o.Company, "sell", "mt"); units != 123 || items != 2 {
			t.Fatalf("booked %d units, %d items; want 123, 2", units, items)
		}
		already, err = u.Book(ctx, guard, []dal.UsageRow{row(100)})
		if err != nil || !already {
			t.Fatalf("the same guard again: %v %v", already, err)
		}
		if units, _ := o.Units(o.Company, "sell", "mt"); units != 123 {
			t.Fatalf("a repeat writes nothing: %d", units)
		}
	})

	t.Run("a batch with no rows still takes its guard", func(t *testing.T) {
		g := guard[:len(guard)-1] + "Z"
		if already, err := u.Book(ctx, g, nil); err != nil || already {
			t.Fatalf("%v %v", already, err)
		}
		if already, _ := u.Book(ctx, g, nil); !already {
			t.Fatal("the guard of an empty batch is kept")
		}
	})

	t.Run("an error that is not about a row is the store away", func(t *testing.T) {
		if u.Away(nil) {
			t.Fatal("no error is not away")
		}
		if !u.Away(errors.New("connection refused")) {
			t.Fatal("an unknown error counts as away")
		}
	})
}

// BalanceOpts gives the balance-of-record suite companies of its own.
type BalanceOpts struct {
	// NewCompany makes a company with a balance (prepaid or not) and returns
	// its id; Account is an account id to name on ledger rows.
	NewCompany func(t *testing.T, balance float64, prepaid bool) int64
	Account    int64
}

// RunCompanyBalances runs the balance of record's contract on c.
func RunCompanyBalances(t *testing.T, c dal.CompanyBalances, o BalanceOpts) {
	ctx := context.Background()
	ref := func(s string) string { return fmt.Sprintf("DALT-%s-%d", s, time.Now().UnixNano()%1e12) }

	t.Run("a hold takes whole units only when covered", func(t *testing.T) {
		co := o.NewCompany(t, 10, true)
		ok, left, err := c.HoldUnits(ctx, dal.Hold{Account: o.Account, Company: co, Units: 1, Reference: ref("h1"), Reason: "test"}, false)
		if err != nil || !ok || left != 99999 {
			t.Fatalf("hold 0.0001 of 10: %v %d %v", ok, left, err)
		}
		if b, err := c.Balance(ctx, co); err != nil || b != 9.9999 {
			t.Fatalf("balance %v %v", b, err)
		}
		ok, _, err = c.HoldUnits(ctx, dal.Hold{Account: o.Account, Company: co, Units: 100000, Reference: ref("h2")}, false)
		if err != nil || ok {
			t.Fatalf("a hold over the balance is refused: %v %v", ok, err)
		}
		r := ref("h3")
		if n, _ := c.HoldRows(ctx, r); n != 0 {
			t.Fatalf("no row yet: %d", n)
		}
		if ok, _, err := c.HoldUnits(ctx, dal.Hold{Account: o.Account, Company: co, Units: 500, Reference: r, Reason: "test"}, true); err != nil || !ok {
			t.Fatalf("hold with its row: %v %v", ok, err)
		}
		if n, err := c.HoldRows(ctx, r); err != nil || n != 1 {
			t.Fatalf("the hold's row: %d %v", n, err)
		}
		if err := c.GiveBackUnits(ctx, dal.Hold{Account: o.Account, Company: co, Units: 500, Reference: "refund:" + r}, true); err != nil {
			t.Fatal(err)
		}
		if b, _ := c.Balance(ctx, co); b != 9.9999 {
			t.Fatalf("given back: %v", b)
		}
		if err := c.GiveBackUnits(ctx, dal.Hold{Company: co, Units: 0}, false); err != nil {
			t.Fatalf("nothing to give back: %v", err)
		}
	})

	t.Run("the one-statement hold and give-back", func(t *testing.T) {
		co := o.NewCompany(t, 1, true)
		if ok, err := c.HoldIfCovered(ctx, co, 0.6); err != nil || !ok {
			t.Fatalf("%v %v", ok, err)
		}
		if ok, err := c.HoldIfCovered(ctx, co, 0.6); err != nil || ok {
			t.Fatalf("0.6 of 0.4: %v %v", ok, err)
		}
		if err := c.GiveBack(ctx, co, 0.6); err != nil {
			t.Fatal(err)
		}
		if b, _ := c.Balance(ctx, co); b != 1 {
			t.Fatalf("balance %v", b)
		}
	})

	t.Run("prepaid companies are listed with their balance; a write-back sets it", func(t *testing.T) {
		pre := o.NewCompany(t, 12.5, true)
		post := o.NewCompany(t, 3, false)
		cs, err := c.Prepaid(ctx)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[int64]float64{}
		for _, x := range cs {
			seen[x.Company] = x.Balance
		}
		if b, ok := seen[pre]; !ok || b != 12.5 {
			t.Fatalf("prepaid company listed: %v %v", b, ok)
		}
		if _, ok := seen[post]; ok {
			t.Fatal("a postpaid company is not listed")
		}
		if err := c.SetBalance(ctx, pre, 7.25); err != nil {
			t.Fatal(err)
		}
		if b, _ := c.Balance(ctx, pre); b != 7.25 {
			t.Fatalf("set: %v", b)
		}
		if err := c.AddHoldRow(ctx, dal.LedgerRow{Account: o.Account, Company: pre, Units: -1, Prev: 1, After: 0, Reference: ref("r"), Reason: "test"}); err != nil {
			t.Fatal(err)
		}
		if err := c.AddGiveBackRow(ctx, dal.LedgerRow{Account: o.Account, Company: pre, Units: 1, Prev: 0, After: 1, Reference: ref("g")}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a company that is not there is an error", func(t *testing.T) {
		if _, err := c.Balance(ctx, 1<<50); err == nil {
			t.Fatal("no such company")
		}
		if _, _, err := c.HoldUnits(ctx, dal.Hold{Company: 1 << 50, Units: 1}, false); err == nil {
			t.Fatal("no such company to hold on")
		}
	})
}

// RunBillingConfig runs what billing reads from the config store on b: every
// read answers on the real schema, and an exchange rate saved is read back.
// endpointID is an id no real provider has.
func RunBillingConfig(t *testing.T, b dal.BillingConfig, endpointID int64) {
	ctx := context.Background()
	t.Run("the reads answer on the real schema", func(t *testing.T) {
		if _, err := b.AccountTerms(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := b.VendorPoints(ctx); err != nil {
			t.Fatal(err)
		}
		m, err := b.RateMark(ctx)
		if err != nil || !m.NowOK || m.Now == "" {
			t.Fatalf("rate mark: %+v %v", m, err)
		}
		if _, err := b.ReadRates(ctx, time.Time{}); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ReadRates(ctx, time.Now().Add(-72*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if rows, err := b.RatePrices(ctx, []int64{1 << 50}); err != nil || len(rows) != 0 {
			t.Fatalf("no such rate: %v %v", rows, err)
		}
		if _, err := b.FXProviders(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := b.RateRequest(ctx, 1<<50); err == nil {
			t.Fatal("no such rate request")
		}
	})
	t.Run("a rate saved is read back fresh, then stale", func(t *testing.T) {
		if err := b.SaveRate(ctx, endpointID, "AAA", "BBB", 2.5, 60); err != nil {
			t.Fatal(err)
		}
		r, fresh, err := b.StoredRate(ctx, endpointID, "AAA", "BBB", 600)
		if err != nil || r != 2.5 || !fresh {
			t.Fatalf("stored: %v %v %v", r, fresh, err)
		}
		if err := b.SaveRate(ctx, endpointID, "AAA", "BBB", 2.75, -10); err != nil {
			t.Fatal(err)
		}
		if r, fresh, err := b.StoredRate(ctx, endpointID, "AAA", "BBB", 600); err != nil || r != 2.75 || fresh {
			t.Fatalf("a rate that ran out is stale: %v %v %v", r, fresh, err)
		}
		recent, err := b.RecentRates(ctx, 3600)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, x := range recent {
			if x.From == "AAA" && x.To == "BBB" && x.Rate == 2.75 && x.Stale {
				found = true
			}
		}
		if !found {
			t.Fatalf("the rate is in the book: %v", recent)
		}
	})
}

// BookOpts gives the balance book's suite companies of its own and a way to
// read and clear what it wrote.
type BookOpts struct {
	// NewCompany makes a prepaid company with a balance and returns its id.
	NewCompany func(t *testing.T, balance float64) int64
	// Balance reads a company's balance of record in ledger units.
	Balance func(company int64) int64
	// Scope is a deployment name no real data has; Cleanup removes its
	// events.
	Scope   string
	Cleanup func()
}

// RunBalanceBook runs the balance book's contract (dal.BalanceBook, a
// gateway with no Redis) on b, whose events go to q: a balance move and its
// events are one transaction, a hold is refused when short, later moments go
// below zero.
func RunBalanceBook(t *testing.T, b dal.BalanceBook, q dal.BillingQueue, o BookOpts) {
	ctx := context.Background()
	if o.Cleanup != nil {
		t.Cleanup(o.Cleanup)
	}
	now := time.Now().UnixMilli()
	n := 0
	adds := func(lane int) []dal.LaneAdd {
		n++
		s := dal.Scored{Score: float64(now + int64(n)), Member: fmt.Sprintf(`{"id":"ev%d","side":"sell"}`, n)}
		bh := dal.Scored{Score: float64(now + int64(n)), Member: fmt.Sprintf(`{"id":"ev%d","side":"buy"}`, n)}
		return []dal.LaneAdd{
			{Lane: dal.Lane{Scope: o.Scope, Side: "sell", Lane: lane}, Events: []dal.Scored{s}, Name: true},
			{Lane: dal.Lane{Scope: o.Scope, Side: "buy", Lane: lane}, Events: []dal.Scored{bh}, Name: true},
		}
	}
	ready := func(side string, lane int) int64 {
		c, err := q.Count(ctx, dal.Lane{Scope: o.Scope, Side: side, Lane: lane})
		if err != nil {
			t.Fatal(err)
		}
		return c.Ready
	}

	t.Run("a hold that is covered moves the balance and writes its events", func(t *testing.T) {
		co := o.NewCompany(t, 1)
		ok, left, err := b.Move(ctx, co, 6000, true, adds(0))
		if err != nil || !ok || left != 4000 {
			t.Fatalf("hold 0.6 of 1: %v %d %v", ok, left, err)
		}
		if o.Balance(co) != 4000 {
			t.Fatalf("balance %d, want 4000", o.Balance(co))
		}
		if ready("sell", 0) != 1 || ready("buy", 0) != 1 {
			t.Fatal("each side's half is on its lane")
		}
		ok, _, err = b.Move(ctx, co, 5000, true, adds(0))
		if err != nil || ok {
			t.Fatalf("a hold over the balance is refused: %v %v", ok, err)
		}
		if o.Balance(co) != 4000 || ready("sell", 0) != 1 {
			t.Fatal("a refused hold moves nothing and writes nothing")
		}
		ok, left, err = b.Move(ctx, co, 5000, false, adds(0))
		if err != nil || !ok || left != -1000 {
			t.Fatalf("a later moment may go below zero: %v %d %v", ok, left, err)
		}
		if _, left, err = b.Move(ctx, co, -1000, true, adds(0)); err != nil || left != 0 {
			t.Fatalf("a give-back is never refused: %d %v", left, err)
		}
		if ok, _, err := b.Move(ctx, co, 0, true, adds(0)); err != nil || !ok {
			t.Fatalf("a booking that moves nothing (postpaid) only writes: %v %v", ok, err)
		}
		if ready("sell", 0) != 4 || o.Balance(co) != 0 {
			t.Fatalf("four bookings written, balance 0: %d %d", ready("sell", 0), o.Balance(co))
		}
	})

	t.Run("an event put twice is there once", func(t *testing.T) {
		co := o.NewCompany(t, 1)
		a := adds(1)
		b.Move(ctx, co, 1, false, a)
		b.Move(ctx, co, 1, false, a)
		if ready("sell", 1) != 1 {
			t.Fatalf("the same event twice: %d", ready("sell", 1))
		}
		if o.Balance(co) != 9998 {
			t.Fatalf("each move is its own: %d", o.Balance(co))
		}
	})

	t.Run("many moves are one transaction and answer in order", func(t *testing.T) {
		a, c := o.NewCompany(t, 0.1), o.NewCompany(t, 0.001)
		var all []dal.LaneAdd
		all = append(all, adds(2)...)
		all = append(all, adds(2)...)
		got, err := b.MoveMany(ctx, []dal.Deduction{{Company: a, Units: 100}, {Company: c, Units: 15}, {Company: a, Units: 1}}, all)
		if err != nil || len(got) != 3 || got[0].Left != 900 || got[1].Left != -5 || got[2].Left != 899 {
			t.Fatalf("move many: %+v %v", got, err)
		}
		if o.Balance(a) != 899 || o.Balance(c) != -5 || ready("sell", 2) != 2 {
			t.Fatalf("balances %d %d, events %d", o.Balance(a), o.Balance(c), ready("sell", 2))
		}
		if out, err := b.MoveMany(ctx, nil, adds(2)); err != nil || len(out) != 0 || ready("buy", 2) != 3 {
			t.Fatalf("events only: %v %v", out, err)
		}
	})

	t.Run("holds at once never take more than there is", func(t *testing.T) {
		co := o.NewCompany(t, 0.002)
		var wg sync.WaitGroup
		var mu sync.Mutex
		won := 0
		for i := 0; i < 40; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if ok, _, err := b.Move(ctx, co, 1, true, nil); err == nil && ok {
					mu.Lock()
					won++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if won != 20 || o.Balance(co) != 0 {
			t.Fatalf("%d holds of 1 won on 20, balance %d", won, o.Balance(co))
		}
	})

	t.Run("a booking that fails part way changes nothing", func(t *testing.T) {
		co := o.NewCompany(t, 1)
		bad := []dal.LaneAdd{{Lane: dal.Lane{Scope: o.Scope, Side: "side-too-long-for-its-column", Lane: 3},
			Events: []dal.Scored{{Score: float64(now), Member: `{"id":"bad"}`}}}}
		ok, _, err := b.Move(ctx, co, 10, true, bad)
		if err == nil || ok {
			t.Fatalf("an event the store refuses: %v %v", ok, err)
		}
		if o.Balance(co) != 10000 {
			t.Fatalf("its balance move was rolled back with it: %d", o.Balance(co))
		}
		if !b.Unsent(err) {
			t.Fatalf("an error before the commit changed nothing: %v", err)
		}
	})

	t.Run("a company that is not there", func(t *testing.T) {
		if _, _, err := b.Move(ctx, 1<<50, 10, true, nil); err == nil {
			t.Fatal("a hold on no company is an error")
		}
		if ok, _, err := b.Move(ctx, 1<<50, 10, false, adds(4)); err != nil || !ok || ready("sell", 4) != 1 {
			t.Fatalf("a later moment's events are still written: %v %v", ok, err)
		}
	})

	if b.Unsent(nil) {
		t.Fatal("no error is not an unsent one")
	}
	if b.Unsent(errors.New("i/o timeout")) {
		t.Fatal("a timeout may have run")
	}
}
