package daltest

import (
	"context"
	"testing"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// RunTrafficConfig is the config store's small parts' contract (read and
// written through a scratch config store that has at least one vendor).
func RunTrafficConfig(t *testing.T, tc *dal.TrafficConfig) {
	ctx := context.Background()
	companies, err := tc.VendorCompanies().VendorCompanies(ctx)
	if err != nil || len(companies) == 0 {
		t.Fatalf("vendor companies %v %v (the store needs a vendor)", companies, err)
	}
	var vendor int64
	for v := range companies {
		vendor = v
		break
	}
	if _, err := tc.StatusCodes().StatusCodes(ctx); err != nil {
		t.Fatal(err)
	}
	if sw, err := tc.StatusCodes().Switches(ctx); err != nil || len(sw) != len(companies) {
		t.Fatalf("switches %d %v, want one a vendor", len(sw), err)
	}
	if _, err := tc.MORules().MORoutes(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("a health report is read back and forgotten", func(t *testing.T) {
		h := tc.VendorHealth()
		inst := "dalt-" + stamp(t)
		since := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
		at := time.Now().UTC().Truncate(time.Millisecond)
		if err := h.Report(ctx, inst, "test", at, []dal.VendorHealthReport{{VendorID: vendor, FailingSince: since, Detail: "x"}}); err != nil {
			t.Fatal(err)
		}
		find := func() (dal.VendorHealthRow, bool) {
			rows, err := h.Rows(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rows {
				if r.ID == vendor && r.Instance == inst {
					return r, true
				}
			}
			return dal.VendorHealthRow{}, false
		}
		r, ok := find()
		if !ok || !r.FailingSince.Valid || !r.ReportedAt.Valid || r.Detail != "x" {
			t.Fatalf("report read back: %+v %v", r, ok)
		}
		if err := h.Report(ctx, inst, "test", at, []dal.VendorHealthReport{{VendorID: vendor}}); err != nil {
			t.Fatal(err)
		}
		if r, _ := find(); r.FailingSince.Valid || r.Detail != "" {
			t.Fatalf("a report that is not failing: %+v", r)
		}
		if err := h.Forget(ctx, inst); err != nil {
			t.Fatal(err)
		}
		if _, ok := find(); ok {
			t.Fatal("forgotten report still there")
		}
		if err := h.Prune(ctx, time.Now().UTC().Add(-48*time.Hour)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("an endpoint's alert row is written and cleared", func(t *testing.T) {
		a := tc.WebhookAlerts()
		ep := time.Now().UnixNano()%1_000_000_000 + 900_000_000_000
		if err := a.Failed(ctx, dal.WebhookAlert{EndpointID: ep, AccountID: 1, Kind: "mo", URL: "http://x", LastError: "e", Failed: 2}); err != nil {
			t.Fatal(err)
		}
		if err := a.Failed(ctx, dal.WebhookAlert{EndpointID: ep, AccountID: 1, Kind: "mo", URL: "http://x", LastError: "e", Failed: 1, GaveUp: 1}); err != nil {
			t.Fatal(err)
		}
		if err := a.Clear(ctx, ep, "mo"); err != nil {
			t.Fatal(err)
		}
	})
}
