// Package billingstoretest is the shared statement of what a backend must do
// to be a billing store, expressed as a suite any implementation can run.
//
// A separate package (the fstest idiom) because it has to be importable from
// each backend's own tests. It exists so MySQL, MSSQL and Mongo are held to
// ONE description: two of the three cannot be exercised end to end yet — the
// gateway does not boot on those config stores — so this being written down
// in one place is the only thing keeping them from drifting into three
// different meanings of "rated".
package billingstoretest

import (
	"context"
	"testing"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
)

// RunConformance exercises a billing store against the contract every backend
// must meet. A backend's own package calls this with a live handle; it is
// exported from a _test file so it cannot be linked into the shipping binary.
//
// Deliberately behavioural, not SQL-shaped: the MySQL implementation does
// these with GROUP BY and the Mongo one with an aggregation pipeline, and the
// whole point is that a caller cannot tell which it is talking to.
func RunConformance(t *testing.T, name string, s billingstore.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run(name+"/clock is the STORE's, not ours", func(t *testing.T) {
		now, err := s.DBNow(ctx)
		if err != nil {
			t.Fatalf("DBNow: %v", err)
		}
		if now == "" {
			t.Fatal("DBNow returned an empty timestamp — every watermark and bucket boundary is compared against this")
		}
		if _, err := time.Parse("2006-01-02 15:04:05", now); err != nil {
			t.Errorf("DBNow returned %q, which is not the shared timestamp shape: %v", now, err)
		}
	})

	t.Run(name+"/a lease excludes a second holder", func(t *testing.T) {
		release, ok, err := s.TryLease(ctx, "conformance-lease")
		if err != nil {
			t.Fatalf("TryLease: %v", err)
		}
		if !ok {
			t.Skip("lease already held elsewhere; cannot test exclusion")
		}
		defer release()
		// The contract that matters: while one holder has it, nobody else
		// gets it. Two billing instances rating the same rows is how a
		// customer gets charged twice.
		_, ok2, err := s.TryLease(ctx, "conformance-lease")
		if err != nil {
			t.Fatalf("second TryLease: %v", err)
		}
		if ok2 {
			t.Error("two holders got the same lease — two billing instances would rate the same rows")
		}
	})

	t.Run(name+"/listing partitions never errors on an empty store", func(t *testing.T) {
		if _, err := s.ListRecordTables(ctx); err != nil {
			t.Fatalf("ListRecordTables: %v", err)
		}
	})

	t.Run(name+"/unrated lookups tolerate a missing partition", func(t *testing.T) {
		// A partition named by the locator can be gone — dropped by
		// retention between one sweep and the next. That must read as "no
		// work", never as an error that stops the whole sweep.
		if _, err := s.UnratedRecords(ctx, "mtlog_conformance_absent_01_01_1970", 10, 0); err != nil {
			t.Errorf("a missing partition should read as no work, got %v", err)
		}
		if _, err := s.UnratedDLRs(ctx, "dlrlog_conformance_absent_01_01_1970", 10, 0); err != nil {
			t.Errorf("a missing partition should read as no work, got %v", err)
		}
	})

	t.Run(name+"/a failure on a record that is gone is not itself a failure", func(t *testing.T) {
		// Retention can drop a partition between a sweep reading it and this
		// recording a failure against it. That must read as "nothing to
		// count", never as an error that stops the sweep.
		if _, err := s.NoteRatingFailure(ctx, "mtlog_conformance_absent_01_01_1970", "no-such-id", "conformance"); err != nil {
			t.Errorf("noting a failure on a missing partition should be harmless, got %v", err)
		}
	})
}
