package daltest

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// RunMonitorHistory is the monitoring history's contract (dal.MonitorHistory),
// on two handles as two processes would have. The store is a SCRATCH hot
// store with monitor_samples (partitioned p_start … p_max), monitor_events,
// monitor_days and monitor_state: rows of a deployment of the test's own are
// written and removed, and old day summaries are deleted.
func RunMonitorHistory(t *testing.T, a, b dal.MonitorHistory) {
	ctx := context.Background()
	dep := stamp(t)
	day := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
	base := day.Add(10 * time.Hour)
	q, st := `{"d":["mt"]}`, `{"mysql_x":{"ok":1}}`

	t.Run("a report is written and read back", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			s := dal.MonitorSample{At: base.Add(time.Duration(i*5) * time.Second), Role: "http-server", Instance: "i1",
				Deployment: dep, State: "running", Accepted: int64(100 + i*10), PushSec: 5, TPS: 2,
				Queues: &q, Stores: &st, Body: []byte{1, 2, 3}}
			if err := a.AddSample(ctx, s); err != nil {
				t.Fatal(err)
			}
		}
		rows, err := b.Samples(ctx, base.Add(-time.Second), base.Add(20*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		var mine []dal.MonitorSampleRow
		for _, r := range rows {
			if r.Deployment == dep {
				mine = append(mine, r)
			}
		}
		if len(mine) != 3 || mine[0].Accepted != 100 || mine[2].Accepted != 120 || !sameJSON(mine[0].Queues, q) || mine[0].Push != 5 {
			t.Fatalf("samples read back: %+v", mine)
		}
		refs, err := b.Near(ctx, base.Add(-time.Minute), base.Add(time.Minute), dep)
		if err != nil || len(refs) != 3 {
			t.Fatalf("near: %d %v", len(refs), err)
		}
		bodies, err := b.Bodies(ctx, []int64{refs[0].ID, refs[1].ID})
		if err != nil || len(bodies) != 2 || string(bodies[0].Body) != "\x01\x02\x03" {
			t.Fatalf("bodies: %+v %v", bodies, err)
		}
		m, err := b.Moment(ctx, dep, base, base.Add(5*time.Second))
		if err != nil || len(m) != 2 || m[0].At.IsZero() {
			t.Fatalf("moment: %+v %v", m, err)
		}
		of, answered, err := b.Answered(ctx, base.Add(7*time.Second), base.Add(-time.Minute), dep)
		if err != nil || of != 1 || answered != 1 {
			t.Fatalf("answered %d of %d (%v), want 1 of 1", answered, of, err)
		}
	})

	t.Run("a day's figures", func(t *testing.T) {
		end := day.AddDate(0, 0, 1)
		deps, err := a.DeploymentsOn(ctx, day, end)
		if err != nil || !contains(deps, dep) {
			t.Fatalf("deployments %v %v", deps, err)
		}
		if p, err := a.DayPush(ctx, dep, day, end); err != nil || !p.Valid || p.Int64 != 5 {
			t.Fatalf("push %+v %v", p, err)
		}
		cs, err := a.DayCounts(ctx, dep, day, end)
		if err != nil || len(cs) != 1 || cs[0].N != 3 || cs[0].Accepted != 20 {
			t.Fatalf("counts %+v %v", cs, err)
		}
		ivs, err := a.DayIntervals(ctx, dep, day, end, 5)
		if err != nil || len(ivs) != 3 || ivs[0].In != 2 {
			t.Fatalf("intervals %+v %v", ivs, err)
		}
		rs, err := a.DayRoles(ctx, dep, day, end, 5)
		if err != nil || len(rs) != 1 || rs[0].N != 3 {
			t.Fatalf("roles %+v %v", rs, err)
		}
		n := 0
		if err := a.DayQueues(ctx, dep, day, end, func(inst string, _ time.Time, raw []byte) error {
			if inst != "http-server|i1" || !sameJSON(raw, q) {
				t.Errorf("queue row %s %s", inst, raw)
			}
			n++
			return nil
		}); err != nil || n != 3 {
			t.Fatalf("queues %d %v", n, err)
		}
		n = 0
		if err := a.DayStores(ctx, dep, day, end, func(time.Time, []byte) error { n++; return nil }); err != nil || n != 3 {
			t.Fatalf("stores %d %v", n, err)
		}
		if err := a.AddDay(ctx, dal.MonitorDay{Deployment: dep, Day: day.Format("2006-01-02"), IntervalSec: 5, Samples: 3,
			PeakTPSIn: 2, PeakTPSInAt: &base, Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		// Written once: a second write is turned away.
		if err := b.AddDay(ctx, dal.MonitorDay{Deployment: dep, Day: day.Format("2006-01-02"), Body: []byte("y")}); err != nil {
			t.Fatal(err)
		}
		if k, err := b.DayKept(ctx, dep, day.Format("2006-01-02")); err != nil || k != 1 {
			t.Fatalf("kept %d %v", k, err)
		}
		sums, err := b.DaySummaries(ctx, day.Format("2006-01-02"), day.Format("2006-01-02"), dep)
		if err != nil || len(sums) != 1 || string(sums[0]) != "x" {
			t.Fatalf("summaries %q %v", sums, err)
		}
	})

	t.Run("an event is written once, with its context", func(t *testing.T) {
		e := dal.MonitorEventRow{Deployment: dep, At: base, Kind: "role_down", Phase: "start", Severity: "warning",
			Subject: "http-server", Detail: "d", IntervalSec: 5}
		id, added, err := a.AddEvent(ctx, e)
		if err != nil || !added || id == 0 {
			t.Fatalf("first write: %d %v %v", id, added, err)
		}
		if _, added, err := b.AddEvent(ctx, e); err != nil || added {
			t.Fatalf("a repeat was written again (%v %v)", added, err)
		}
		if err := a.SetEventContext(ctx, id, []byte("ctx")); err != nil {
			t.Fatal(err)
		}
		got, blob, err := b.Event(ctx, id)
		if err != nil || got.Deployment != dep || got.Kind != "role_down" || string(blob) != "ctx" {
			t.Fatalf("event %+v %q %v", got, blob, err)
		}
		if _, _, err := b.Event(ctx, -1); !errors.Is(err, dal.ErrNoRows) {
			t.Fatalf("no such event: %v, want ErrNoRows", err)
		}
		evs, err := b.Events(ctx, base.Add(-time.Minute), base.Add(time.Minute), dep, 10)
		if err != nil || len(evs) != 1 || !evs[0].HasContext {
			t.Fatalf("events %+v %v", evs, err)
		}
		des, err := b.DayEvents(ctx, dep, day, day.AddDate(0, 0, 1))
		if err != nil || len(des) != 1 || des[0].ID != id {
			t.Fatalf("day events %+v %v", des, err)
		}
	})

	t.Run("state", func(t *testing.T) {
		name := "dalt_" + dep
		if _, err := a.State(ctx, name); !errors.Is(err, dal.ErrNoRows) {
			t.Fatalf("missing state: %v, want ErrNoRows", err)
		}
		if err := a.SetState(ctx, name, "1"); err != nil {
			t.Fatal(err)
		}
		if err := b.SetState(ctx, name, "2"); err != nil {
			t.Fatal(err)
		}
		if v, err := a.State(ctx, name); err != nil || v != "2" {
			t.Fatalf("state %q %v", v, err)
		}
	})

	t.Run("a named lock has one holder", func(t *testing.T) {
		name := "dalt_lock_" + dep
		ran := false
		got, err := a.Locked(ctx, name, func(q dal.MonitorStatements) error {
			ran = true
			other, err := b.Locked(ctx, name, func(dal.MonitorStatements) error {
				t.Error("two holders of one lock")
				return nil
			})
			if err != nil || other {
				t.Errorf("the second asker: got %v %v, want refused", other, err)
			}
			// The statements run on the holder's connection.
			_, err = q.State(ctx, "dalt_none_"+dep)
			if !errors.Is(err, dal.ErrNoRows) {
				t.Errorf("a statement under the lock: %v", err)
			}
			return nil
		})
		if err != nil || !got || !ran {
			t.Fatalf("the first asker: %v %v %v", got, ran, err)
		}
		// Let go: the other takes it now.
		if got, err := b.Locked(ctx, name, func(dal.MonitorStatements) error { return nil }); err != nil || !got {
			t.Fatalf("after the release: %v %v", got, err)
		}
		want := errors.New("fn failed")
		if _, err := a.Locked(ctx, name, func(dal.MonitorStatements) error { return want }); !errors.Is(err, want) {
			t.Fatalf("fn's error: %v", err)
		}
	})

	t.Run("partitions are kept ahead and a second pass adds none", func(t *testing.T) {
		today := time.Now().UTC()
		cutoff := today.AddDate(0, 0, -30)
		if _, err := a.Locked(ctx, "dalt_parts", func(q dal.MonitorStatements) error {
			if _, _, err := q.KeepSampleDays(ctx, today, cutoff, true); err != nil {
				return err
			}
			created, dropped, err := q.KeepSampleDays(ctx, today, cutoff, true)
			if err != nil || len(created) != 0 || len(dropped) != 0 {
				t.Errorf("second pass: created %v dropped %v (%v)", created, dropped, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("old rows go", func(t *testing.T) {
		if n, err := a.DeleteSamplesBefore(ctx, day.AddDate(0, 0, 1), 5000); err != nil || n < 3 {
			t.Fatalf("samples deleted %d %v", n, err)
		}
		if n, err := a.DeleteEventsBefore(ctx, day.AddDate(0, 0, 1)); err != nil || n < 1 {
			t.Fatalf("events deleted %d %v", n, err)
		}
		if n, err := a.DeleteDaysBefore(ctx, day.AddDate(0, 0, 1).Format("2006-01-02")); err != nil || n < 1 {
			t.Fatalf("days deleted %d %v", n, err)
		}
	})
}

// RunLiveStatus is the live view's contract (dal.LiveStatus), on two
// handles as two processes would have, in a throwaway Redis database.
func RunLiveStatus(t *testing.T, a, b dal.LiveStatus) {
	ctx := context.Background()
	st := stamp(t)

	t.Run("a pushed report is read back by key", func(t *testing.T) {
		k := "status:test:" + st
		if err := a.Publish(ctx, k, []byte(`{"x":1}`), time.Second); err != nil {
			t.Fatal(err)
		}
		all, err := b.Statuses(ctx)
		if err != nil || string(all[k]) != `{"x":1}` {
			t.Fatalf("statuses %q %v", all[k], err)
		}
	})

	t.Run("one store-reading turn per scope, taken over once it lapses", func(t *testing.T) {
		hold := 400 * time.Millisecond
		scope := "dalt-" + st
		if ok, err := a.TakeStoreTurn(ctx, scope, "a", hold); err != nil || !ok {
			t.Fatalf("first taker: %v %v", ok, err)
		}
		if ok, err := b.TakeStoreTurn(ctx, scope, "b", hold); err != nil || ok {
			t.Fatalf("two holders (%v %v)", ok, err)
		}
		if ok, _ := a.TakeStoreTurn(ctx, scope, "a", hold); !ok {
			t.Fatal("the holder could not renew")
		}
		time.Sleep(hold + 200*time.Millisecond)
		if ok, err := b.TakeStoreTurn(ctx, scope, "b", hold); err != nil || !ok {
			t.Fatalf("a holder that stopped renewing kept the turn (%v %v)", ok, err)
		}
		if ok, _ := a.TakeStoreTurn(ctx, scope, "a", hold); ok {
			t.Fatal("the old holder took it back from a live one")
		}
	})

	t.Run("the ask to report now", func(t *testing.T) {
		if err := a.SetAsk(ctx, "ask-"+st, time.Minute); err != nil {
			t.Fatal(err)
		}
		if v, err := b.Ask(ctx); err != nil || v != "ask-"+st {
			t.Fatalf("ask %q %v", v, err)
		}
	})

	t.Run("dropping the old waiting sets twice", func(t *testing.T) {
		if _, err := a.DropAwaitSets(ctx); err != nil {
			t.Fatal(err)
		}
		if n, err := b.DropAwaitSets(ctx); err != nil || n != 0 {
			t.Fatalf("a second drop removed %d (%v)", n, err)
		}
	})
}

// sameJSON compares JSON as a JSON column gives it back (spaced its own way).
func sameJSON(got []byte, want string) bool {
	var g, w any
	return json.Unmarshal(got, &g) == nil && json.Unmarshal([]byte(want), &w) == nil && reflect.DeepEqual(g, w)
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
