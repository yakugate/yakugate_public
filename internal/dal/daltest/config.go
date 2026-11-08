package daltest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// The contracts of the config family (dal/config.go, dal/reference.go). Run
// against a scratch config store built by `yakugate migrate`: the suite
// makes companies, a blocked number and audit rows of its own (names and
// numbers carrying the run's stamp) and removes them through Opts.Cleanup.

// ConfigOpts is what the config suite needs from the DAL's own test.
type ConfigOpts struct {
	// AuditRows counts the audit_log rows of an action (read past the DAL).
	AuditRows func(action string) int
	// Cleanup removes what the run made: the companies whose name starts
	// with prefix, the blocked numbers starting with prefix's digits, and
	// the audit rows of the actions starting with prefix.
	Cleanup func(prefix, digits string)
}

// RunConfigStore is the config store's contract: transactions that commit
// or leave nothing, the "no row" answer, a company and a blocked number
// written and read back, the audit trail, the menu's counts.
func RunConfigStore(t *testing.T, s dal.ConfigStore, o ConfigOpts) {
	ctx := context.Background()
	n := time.Now().UnixNano() % 1e9
	prefix := fmt.Sprintf("dalt%d", n)
	digits := fmt.Sprintf("9%09d", n)
	if o.Cleanup != nil {
		t.Cleanup(func() { o.Cleanup(prefix, digits) })
	}
	company := func(name string) dal.CompanyInsert {
		return dal.CompanyInsert{UUID: fmt.Sprintf("%s-%s", prefix, name), Name: prefix + " " + name,
			Currency: "USD", BaseRate: "1", CompanyEmail: "a@example.com", SupportEmail: "a@example.com",
			BillingEmail: "a@example.com", RatesEmail: "a@example.com"}
	}

	if err := s.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	t.Run("a rolled-back transaction leaves nothing", func(t *testing.T) {
		tx, err := s.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		id, err := tx.InsertCompany(ctx, company("gone"))
		if err != nil {
			tx.Rollback()
			t.Fatalf("insert in a transaction: %v", err)
		}
		// The transaction sees its own row.
		if ok, err := tx.CompanyExists(ctx, id); err != nil || !ok {
			t.Fatalf("the transaction does not see its own row: %v %v", ok, err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if ok, err := s.CompanyExists(ctx, id); err != nil || ok {
			t.Fatalf("a rolled-back company is there: %v %v", ok, err)
		}
	})

	var id int64
	t.Run("a committed transaction is kept", func(t *testing.T) {
		err := dal.WithTx(ctx, s, func(tx dal.ConfigTx) error {
			var err error
			id, err = tx.InsertCompany(ctx, company("kept"))
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := s.CompanyExists(ctx, id); err != nil || !ok {
			t.Fatalf("a committed company is not there: %v %v", ok, err)
		}
	})

	t.Run("a company reads back as written", func(t *testing.T) {
		if id == 0 {
			t.Skip("no company")
		}
		d, err := s.CompanyDetail(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if d.Name != prefix+" kept" || d.Currency != "USD" || d.Prepaid {
			t.Fatalf("read back %+v", d)
		}
		if taken, err := s.CompanyNameTaken(ctx, prefix+" kept"); err != nil || !taken {
			t.Fatalf("its name is not taken: %v %v", taken, err)
		}
		if taken, err := s.CompanyNameTakenByOther(ctx, prefix+" kept", id); err != nil || taken {
			t.Fatalf("its own name counts against it: %v %v", taken, err)
		}
		if err := s.SetCompanyPrepaid(ctx, id, 1); err != nil {
			t.Fatal(err)
		}
		if d, _ := s.CompanyDetail(ctx, id); !d.Prepaid {
			t.Fatal("prepaid did not stick")
		}
	})

	t.Run("no row is ErrNoRows", func(t *testing.T) {
		if _, err := s.CompanyName(ctx, -1); !errors.Is(err, dal.ErrNoRows) {
			t.Fatalf("a missing company: %v, want ErrNoRows", err)
		}
		if _, err := s.BlockByID(ctx, -1); !errors.Is(err, dal.ErrNoRows) {
			t.Fatalf("a missing blocked number: %v, want ErrNoRows", err)
		}
	})

	t.Run("a blocked number reads back, and leaves", func(t *testing.T) {
		bid, err := s.InsertBlock(ctx, dal.BlockInsert{Number: digits, Source: "manual", Reason: prefix})
		if err != nil {
			t.Fatal(err)
		}
		b, err := s.BlockByID(ctx, bid)
		if err != nil {
			t.Fatal(err)
		}
		if b.Number != digits || b.AccountID != 0 || b.CompanyID != 0 || b.Expires.Valid {
			t.Fatalf("read back %+v", b)
		}
		if n, err := s.BlockCount(ctx, digits+"%"); err != nil || n != 1 {
			t.Fatalf("count: %d %v, want 1", n, err)
		}
		if err := s.DeleteBlockRow(ctx, bid); err != nil {
			t.Fatal(err)
		}
		if _, err := s.BlockByID(ctx, bid); !errors.Is(err, dal.ErrNoRows) {
			t.Fatalf("a deleted row: %v", err)
		}
	})

	t.Run("the audit trail takes a row", func(t *testing.T) {
		action := prefix + ".update"
		err := s.WriteAudit(ctx, dal.AuditRow{CompanyID: id, Action: action, Summary: "contract", Entity: "company",
			EntityID: fmt.Sprint(id), Feature: "companies", Detail: "{}", After: `{"name":"x"}`})
		if err != nil {
			t.Fatal(err)
		}
		if o.AuditRows != nil {
			if n := o.AuditRows(action); n != 1 {
				t.Fatalf("%d audit rows, want 1", n)
			}
		}
	})

	t.Run("the menu's counts", func(t *testing.T) {
		for _, what := range []string{"accounts", "companies", "vendors", "calls", "plans", "dialects", "cards", "spam", "blocked", "operators"} {
			if _, err := s.MenuCount(ctx, what); err != nil {
				t.Errorf("%s: %v", what, err)
			}
		}
		if _, err := s.MenuCount(ctx, "nothing"); err == nil {
			t.Error("an unknown count answered")
		}
	})
}

// RunReferenceData is the reference reads' contract: every read answers on
// a migrated store, a company written through the config store is in the
// accounts' join and the fingerprint, and a table that is not there is an
// error.
func RunReferenceData(t *testing.T, r dal.ReferenceData) {
	ctx := context.Background()
	if _, err := r.Accounts(ctx); err != nil {
		t.Errorf("accounts: %v", err)
	}
	if _, err := r.ApprovedSenders(ctx); err != nil && !r.MissingTable(err) {
		t.Errorf("approved senders: %v", err)
	}
	if _, err := r.HLRDefaultSettings(ctx); err != nil {
		t.Errorf("hlr defaults: %v", err)
	}
	sc := r.HasVendorShortCodes(ctx)
	if _, err := r.RouteVendors(ctx, sc); err != nil {
		t.Errorf("route vendors: %v", err)
	}
	if _, err := r.RouteVendors(ctx, false); err != nil {
		t.Errorf("route vendors without short codes: %v", err)
	}
	for name, read := range map[string]func() error{
		"routes":        func() error { _, err := r.Routes(ctx); return err },
		"rates":         func() error { _, err := r.Rates(ctx); return err },
		"auto replace":  func() error { _, err := r.AutoReplace(ctx); return err },
		"spam":          func() error { _, err := r.Spam(ctx); return err },
		"templates":     func() error { _, err := r.Templates(ctx); return err },
		"blocks":        func() error { _, err := r.Blocks(ctx); return err },
		"country rules": func() error { _, err := r.CountryRules(ctx); return err },
	} {
		if err := read(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	now, err := r.Now(ctx)
	if err != nil || !now.Valid {
		t.Errorf("now: %v %v", now, err)
	}
	if n, _, extra, err := r.TableMark(ctx, "sms_service_config"); err != nil || n < 0 {
		t.Errorf("mark of the accounts: %d %v", n, err)
	} else if n > 0 && !extra.Valid {
		t.Error("the accounts' mark has no checksum")
	}
	if _, _, extra, err := r.TableMark(ctx, "companies"); err != nil || extra.Valid {
		t.Errorf("mark of the companies: %v, checksum %v (none expected)", err, extra.Valid)
	}
	if _, _, _, err := r.TableMark(ctx, "dalt_no_such_table"); err == nil {
		t.Error("a table that is not there has a mark")
	}
}

// RunHLRNumbers is the HLR store's contract on a migrated store: every read
// answers, Prepare can run twice, an unknown number misses.
func RunHLRNumbers(t *testing.T, h dal.HLRNumbers) {
	ctx := context.Background()
	if err := h.Prepare(ctx); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := h.Prepare(ctx); err != nil {
		t.Fatalf("prepare again: %v", err)
	}
	if _, err := h.Count(ctx); err != nil {
		t.Errorf("count: %v", err)
	}
	if _, _, err := h.LoadSince(ctx, time.Time{}); err != nil {
		t.Errorf("full load: %v", err)
	}
	since := time.Now().Add(-time.Hour)
	if _, max, err := h.LoadSince(ctx, since); err != nil || max.Before(since) {
		t.Errorf("delta load: %v, mark %v went back", err, max)
	}
	if _, err := h.Revocations(ctx); err != nil {
		t.Errorf("revocations: %v", err)
	}
	if _, ok := h.Lookup("0000", "0000000000000"); ok {
		t.Error("a number nobody wrote was found")
	}
	tables, err := h.QueueTables(ctx)
	if err != nil {
		t.Fatalf("queue tables: %v", err)
	}
	for _, tb := range tables {
		if tb == "queue_template" {
			t.Error("the template is listed as a queue")
		}
	}
}

// RunVendorQueueTables is the vendor queue tables' contract: made empty,
// counted, dropped, and making or dropping twice is no error.
func RunVendorQueueTables(t *testing.T, v dal.VendorQueueTables) {
	ctx := context.Background()
	cid := fmt.Sprintf("dalt%d", time.Now().UnixNano()%1e9)
	t.Cleanup(func() { _ = v.Drop(context.Background(), cid) })
	for i := 0; i < 2; i++ {
		if err := v.Make(ctx, cid); err != nil {
			t.Fatalf("make %d: %v", i+1, err)
		}
	}
	if n, err := v.Count(ctx, cid); err != nil || n != 0 {
		t.Fatalf("a new queue holds %d (%v)", n, err)
	}
	for i := 0; i < 2; i++ {
		if err := v.Drop(ctx, cid); err != nil {
			t.Fatalf("drop %d: %v", i+1, err)
		}
	}
	if _, err := v.Count(ctx, cid); err == nil {
		t.Fatal("a dropped queue still counts")
	}
}
