package daltest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// The contracts of the shared engine packages' storage (29 Oct 2026): the
// named leases and the register beside them (locks.go), the config store's
// parts (engineconfig.go), the audit trail (auditlog.go), the log settings
// (logsettings.go) and the HLR stores (hlrstore.go).

// RunLocks is the named lease's contract. a and b are two holders that do not
// share a session (two processes); kill ends a holder's session the way a
// process that dies or loses its connection does.
func RunLocks(t *testing.T, a, b dal.Locks, kill func(dal.HeldLock)) {
	ctx := context.Background()
	name := "dalt_lease_" + stamp(t)

	t.Run("one holder at a time", func(t *testing.T) {
		la, got, err := a.TryLock(ctx, name)
		if err != nil || got != dal.LockTaken || la == nil {
			t.Fatalf("first take: %v %v", got, err)
		}
		defer la.Release()
		if lb, got, err := b.TryLock(ctx, name); err != nil || got != dal.LockHeld || lb != nil {
			t.Fatalf("second take while held: %v %v", got, err)
		}
		if s, held, err := b.LockHolder(ctx, name); err != nil || !held || s != la.Session() {
			t.Fatalf("holder %d %v %v, want %d", s, held, err, la.Session())
		}
		if err := la.Alive(ctx); err != nil {
			t.Fatalf("holder not alive: %v", err)
		}
		la.Release()
		lb, got, err := b.TryLock(ctx, name)
		if err != nil || got != dal.LockTaken {
			t.Fatalf("take after release: %v %v", got, err)
		}
		lb.Release()
		if _, held, err := a.LockHolder(ctx, name); err != nil || held {
			t.Fatalf("held after both let go: %v %v", held, err)
		}
	})

	t.Run("two holders never hold it at once", func(t *testing.T) {
		race := name + "_race"
		var mu sync.Mutex
		holders, max := 0, 0
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			l := a
			if i%2 == 1 {
				l = b
			}
			wg.Add(1)
			go func(l dal.Locks) {
				defer wg.Done()
				for j := 0; j < 20; j++ {
					h, got, err := l.TryLock(ctx, race)
					if err != nil {
						t.Error(err)
						return
					}
					if got != dal.LockTaken {
						continue
					}
					mu.Lock()
					holders++
					if holders > max {
						max = holders
					}
					mu.Unlock()
					time.Sleep(time.Millisecond)
					mu.Lock()
					holders--
					mu.Unlock()
					h.Release()
				}
			}(l)
		}
		wg.Wait()
		if max != 1 {
			t.Fatalf("%d held it at once", max)
		}
	})

	t.Run("a holder that stops answering loses it", func(t *testing.T) {
		gone := name + "_gone"
		la, got, err := a.TryLock(ctx, gone)
		if err != nil || got != dal.LockTaken {
			t.Fatalf("take: %v %v", got, err)
		}
		kill(la)
		deadline := time.Now().Add(10 * time.Second)
		for {
			if la.Alive(ctx) != nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the dead holder still answers")
			}
			time.Sleep(50 * time.Millisecond)
		}
		la.Release() // safe on a session that is gone
		for {
			lb, got, err := b.TryLock(ctx, gone)
			if err == nil && got == dal.LockTaken {
				lb.Release()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the lease never came free: %v %v", got, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
}

// RoleHomesOpts lets the contract set up the register: List puts a role on a
// deployment (is_singleton), Cleanup removes a role's rows everywhere.
type RoleHomesOpts struct {
	List    func(role, deployment string)
	Cleanup func(role string)
}

// RunRoleHomes is the register's contract (dal.RoleHomes).
func RunRoleHomes(t *testing.T, h dal.RoleHomes, o RoleHomesOpts) {
	ctx := context.Background()
	role := "dalt-role-" + stamp(t)
	t.Cleanup(func() { o.Cleanup(role) })

	if home, known := h.RoleHome(ctx, role); !known || home != "" {
		t.Fatalf("a role listed nowhere: %q %v", home, known)
	}
	o.List(role, "dep-a")
	if home, known := h.RoleHome(ctx, role); !known || home != "dep-a" {
		t.Fatalf("listed on dep-a: %q %v", home, known)
	}
	o.List(role, "dep-b")
	if _, known := h.RoleHome(ctx, role); known {
		t.Fatal("a role listed twice is known")
	}

	since := time.Now().UTC().Truncate(time.Second)
	rh := dal.RoleHolder{Deployment: "dep-a", Host: "h1", PID: 42, Session: 7}
	if err := h.RecordHolder(ctx, role, rh, since); err != nil {
		t.Fatal(err)
	}
	if got, err := h.RoleHolder(ctx, role); err != nil || got != rh {
		t.Fatalf("holder %+v %v, want %+v", got, err, rh)
	}
	rh.Session = 8
	if err := h.RecordHolder(ctx, role, rh, since); err != nil {
		t.Fatal(err)
	}
	if err := h.ForgetHolder(ctx, role, 7); err != nil {
		t.Fatal(err)
	}
	if got, err := h.RoleHolder(ctx, role); err != nil || got.Session != 8 {
		t.Fatalf("another session's row was forgotten: %+v %v", got, err)
	}
	if err := h.ForgetHolder(ctx, role, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RoleHolder(ctx, role); !errors.Is(err, dal.ErrNoRows) {
		t.Fatalf("forgotten holder read: %v", err)
	}

	if got := h.Claimant(ctx, role, time.Minute); got != "" {
		t.Fatalf("claimant before an ask: %q", got)
	}
	h.AskBack(ctx, role, "dep-a", "h1", 42)
	if got := h.Claimant(ctx, role, time.Minute); got != "dep-a" {
		t.Fatalf("claimant %q, want dep-a", got)
	}
	h.DropAsk(ctx, role)
	if got := h.Claimant(ctx, role, time.Minute); got != "" {
		t.Fatalf("claimant after the drop: %q", got)
	}
}

// RunAuditLog is the audit trail's contract: a row written is read back, AS
// CHAR, in the chain walk, and the chain's head follows the newest row.
func RunAuditLog(t *testing.T, a dal.AuditLog, cleanup func(action string)) {
	ctx := context.Background()
	action := "dalt." + stamp(t)
	t.Cleanup(func() { cleanup(action) })
	before, err := a.UnprotectedCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	row := dal.AdminAuditRow{Actor: "engine-admin", Action: action, Entity: "test", EntityID: "1",
		Method: "POST", Route: "/admin/test", ResponseCode: 200, IP: "127.0.0.1",
		Summary: "a test row", Outcome: "success", Detail: `{"k":"v"}`}
	if err := a.AppendAudit(ctx, row); err != nil {
		t.Fatal(err)
	}
	if after, err := a.UnprotectedCount(ctx); err != nil || after != before {
		t.Fatalf("unprotected %d -> %d %v (a new row is chained)", before, after, err)
	}
	var found bool
	var last dal.NullString
	err = a.WalkChain(ctx, []string{"actor", "action", "account_id", "response_code"}, func(id int64, prev, stored dal.NullString, vals []dal.NullString) error {
		if len(vals) != 4 {
			return fmt.Errorf("%d values", len(vals))
		}
		if vals[1].String == action {
			found = true
			if vals[0].String != "engine-admin" || vals[2].Valid || vals[3].String != "200" {
				return fmt.Errorf("row read back: %+v", vals)
			}
		}
		last = stored
		return nil
	})
	if err != nil || !found {
		t.Fatalf("walk: found=%v %v", found, err)
	}
	if head, err := a.ChainHead(ctx); err != nil || head != last.String {
		t.Fatalf("head %q %v, want the newest row's %q", head, err, last.String)
	}
	stop := errors.New("stop")
	if err := a.WalkChain(ctx, []string{"actor"}, func(int64, dal.NullString, dal.NullString, []dal.NullString) error { return stop }); err != stop {
		t.Fatalf("a walk ended by its caller: %v", err)
	}
}

// LogSettingsOpts gives the contract a capture to read: NewCapture makes a
// flow capture (running here for a minute, or due to be purged) and answers
// its id; Cleanup removes it and its records.
type LogSettingsOpts struct {
	NewCapture func(running bool) int64
	Cleanup    func(id int64)
}

// RunLogSettings is the log settings' contract.
func RunLogSettings(t *testing.T, l dal.LogSettings, o LogSettingsOpts) {
	ctx := context.Background()
	if _, err := l.AccountLogConfigs(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := l.VendorLogConfigs(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AccountMasters(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := l.SystemLogConfig(ctx); err != nil && !errors.Is(err, dal.ErrNoRows) {
		t.Fatal(err)
	}

	running, due := o.NewCapture(true), o.NewCapture(false)
	t.Cleanup(func() { o.Cleanup(running); o.Cleanup(due) })
	rows, err := l.Captures(ctx, "dalt-dep")
	if err != nil {
		t.Fatal(err)
	}
	var sawRunning, sawDue bool
	for _, r := range rows {
		switch r.ID {
		case running:
			sawRunning = r.Running && r.SecsLeft > 0 && !r.Due
		case due:
			sawDue = r.Due && !r.Running && !r.Marked
		}
	}
	if !sawRunning || !sawDue {
		t.Fatalf("captures read: running %v, due %v (%+v)", sawRunning, sawDue, rows)
	}
	if err := l.MarkCapturePurged(ctx, due); err != nil {
		t.Fatal(err)
	}
	rows, _ = l.Captures(ctx, "dalt-dep")
	for _, r := range rows {
		if r.ID == due && !r.Marked {
			t.Fatal("a purged capture is not marked")
		}
	}

	var recs []dal.CaptureRecord
	for i := 0; i < 12; i++ {
		msg := ""
		if i%2 == 0 {
			msg = fmt.Sprintf("dalt%022d", i)
		}
		recs = append(recs, dal.CaptureRecord{CaptureID: running, Role: "test", At: time.Now(), Step: "s", MessageID: msg, Line: `{"line":"x"}`})
	}
	if err := l.AddCaptureRecords(ctx, recs); err != nil {
		t.Fatal(err)
	}
	if err := l.TrimCaptureRecords(ctx, []int64{running}, 5); err != nil {
		t.Fatal(err)
	}
	if err := l.TrimCaptureRecords(ctx, []int64{running}, 5); err != nil {
		t.Fatalf("a second trim, under the cap: %v", err)
	}
	if err := l.TrimCaptureRecords(ctx, nil, 0); err != nil {
		t.Fatal(err)
	}
}

// RunChangeMarks is the change marks' contract: the fingerprint moves when a
// row of the table changes, and says so as fresh. touch changes a row.
func RunChangeMarks(t *testing.T, m dal.ChangeMarks, table string, touch func()) {
	ctx := context.Background()
	sig := m.Fingerprint(table)
	a, _, err := sig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	touch()
	b, fresh, err := sig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if a == b || !fresh {
		t.Fatalf("a change did not move the fingerprint: %q -> %q fresh=%v", a, b, fresh)
	}
}

// RunEngineReads reads every part of the config store the shared packages
// read, on a store migrated by the gateway: each answers without an error.
func RunEngineReads(t *testing.T, e *dal.EngineConfig) {
	ctx := context.Background()
	if _, err := e.VendorSends.MaxAttempts(ctx); err != nil {
		t.Fatal(err)
	}
	_ = e.VendorSends.SplitModes(ctx)
	if _, err := e.Policies.ReceiptPolicyRows(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Policies.AccountIDs(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Policies.GatewaySettingText(ctx, "dalt-none"); !errors.Is(err, dal.ErrNoRows) {
		t.Fatalf("a setting that is not there: %v", err)
	}
	if _, shape, err := e.Doors.Doors(ctx); err != nil || shape != dal.DoorShapePath {
		t.Fatalf("doors: shape %d %v (a migrated store has the newest)", shape, err)
	}
	n, _, err := e.HLR.NumberRangesMark(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := e.HLR.NumberRanges(ctx, n); err != nil || int64(len(rows)) != n {
		t.Fatalf("ranges %d of %d: %v", len(rows), n, err)
	}
	if _, err := e.HLR.HLRProviders(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := e.Doors.MoveLegacy(ctx, func(m dal.LegacyDoorMove) error {
		_, err := m.Vendors(ctx)
		return err
	}); err != nil || !got {
		t.Fatalf("the move's lock: %v %v", got, err)
	}
}

// RunHLRCopies is the numbers' copies' contract.
func RunHLRCopies(t *testing.T, c dal.HLRCopies, cleanup func(msisdn string)) {
	ctx := context.Background()
	num := fmt.Sprintf("9299%08d", time.Now().UnixNano()%1e8)
	t.Cleanup(func() { cleanup(num) })
	if _, found, err := c.GetHLR(ctx, num); err != nil || found {
		t.Fatalf("a number not there: %v %v", found, err)
	}
	c.SetHLR(ctx, num, []byte(`{"msisdn":"`+num+`"}`))
	if raw, found, err := c.GetHLR(ctx, num); err != nil || !found || string(raw) != `{"msisdn":"`+num+`"}` {
		t.Fatalf("read back %q %v %v", raw, found, err)
	}
}

// RunHLRShardWriter is the shard writer's contract: a number is written into
// its shard and written again (the upsert), and read reads it back.
func RunHLRShardWriter(t *testing.T, w dal.HLRShardWriter, shard string, read func(msisdn string) (mcc, mnc string, ported bool)) {
	ctx := context.Background()
	num := fmt.Sprintf("%s%08d", shard, time.Now().UnixNano()%1e8)
	if err := w.PutShard(ctx, shard, dal.HLRRow{MSISDN: num, MCC: "410", MNC: "01", OperatorName: "Jazz", CCC: "92"}); err != nil {
		t.Fatal(err)
	}
	// No name this time: the stored one is kept (6 Nov 2026).
	if err := w.PutShard(ctx, shard, dal.HLRRow{MSISDN: num, MCC: "410", MNC: "03", Ported: true}); err != nil {
		t.Fatal(err)
	}
	if mcc, mnc, ported := read(num); mcc != "410" || mnc != "03" || !ported {
		t.Fatalf("read back %s %s %v", mcc, mnc, ported)
	}
	if err := w.PutShard(ctx, "0000x", dal.HLRRow{MSISDN: "0000x1", MCC: "1", MNC: "1"}); err == nil {
		t.Fatal("a shard table that is not there wrote")
	}
}
