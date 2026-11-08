package retention

// GW-23 lifecycle (PO ruled 2026-08-28): the retention role's first
// CONFIG-store duty — balance_history's partition calendar and the
// post-invoice rollup. Once a company's invoice for a period is SENT and
// PAID, that period's per-message commit rows collapse into daily totals
// (balance_history_rollup) and the raw rows delete: the invoice PDF/CSV are
// the per-message evidence from then on. topup/adjust/refund rows are never
// touched — they are the balance audit trail.

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/billingstore"
)

// maxRollupDaysPerStep bounds one sweep's rollup work so a large backlog
// drains over several intervals instead of one long-running Step.
const maxRollupDaysPerStep = 62

func (p *Process) ledgerUpkeep(ctx context.Context, now time.Time) {
	if p.upkeep == nil {
		return
	}
	// Partition calendar: keep this month + next split out of the catch-all
	// (same pattern as record_index; REORGANIZE is a no-op-cheap rebuild of
	// the near-empty pmax partition).
	for _, m := range []time.Time{now, billingstore.MonthStart(now, 1)} { // not now.AddDate(0, 1, 0): from 31 January that is 3 March
		p.ensureLedgerPartition(ctx, m)
	}
	p.rollupPaidPeriods(ctx)
	p.pruneUsageBatches(ctx)
}

// usageBatchKeepDays is how long a billing batch's guard row is kept
// (usage_batches, config/0144). A batch is only ever applied again within
// moments of a stop, or at the next start; ninety days is far past any
// record that could still be waiting in one.
const usageBatchKeepDays = 90

// pruneUsageBatches removes the guard rows of old billing batches, a bounded
// number a sweep. The day's totals (usage_daily) are never removed here: they
// are what invoices and the billing reports read.
func (p *Process) pruneUsageBatches(ctx context.Context) {
	n, err := p.upkeep.PruneUsageBatches(ctx, usageBatchKeepDays)
	if err != nil {
		// A store from before config/0144 has no such table: nothing to do.
		return
	}
	if n > 0 {
		p.log.Printf("billing batches: %d guard row(s) older than %d days removed", n, usageBatchKeepDays)
	}
}

func (p *Process) ensureLedgerPartition(ctx context.Context, month time.Time) {
	name := "p" + month.Format("200601")
	if n, err := p.upkeep.LedgerPartitions(ctx, name); err != nil || n > 0 {
		return
	}
	next := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
	if err := p.upkeep.AddLedgerPartition(ctx, name, next.Format("2006-01-02")); err != nil {
		p.log.Printf("ledger partition %s: %v", name, err)
	} else {
		p.log.Printf("ledger partition %s created", name)
	}
}

// rollupPaidPeriods finds SENT+PAID invoices whose period still holds raw
// commit rows, and rolls those days up. Each day is one transaction — daily
// totals INSERTed and the day's commit rows DELETEd together, so a crash can
// never leave a day half-counted.
func (p *Process) rollupPaidPeriods(ctx context.Context) {
	eligible, err := p.upkeep.PaidPeriods(ctx)
	if err != nil {
		p.log.Printf("ledger rollup scan: %v", err)
		return
	}

	budget := maxRollupDaysPerStep
	for _, pd := range eligible {
		if budget <= 0 {
			return
		}
		days, err := p.upkeep.CommitDays(ctx, pd.Company, pd.From, pd.To)
		if err != nil {
			p.log.Printf("ledger rollup days company=%d: %v", pd.Company, err)
			continue
		}
		for _, day := range days {
			if budget <= 0 {
				return
			}
			if err := p.rollupDay(ctx, pd.Company, day); err != nil {
				p.log.Printf("ledger rollup company=%d day=%s: %v", pd.Company, day, err)
				break // leave the rest of this period for the next sweep
			}
			budget--
		}
	}
}

func (p *Process) rollupDay(ctx context.Context, company int64, day string) error {
	// INSERT IGNORE: a re-run of an already-rolled day (possible only if the
	// day gained NEW commits after rolling, e.g. very late rating) keeps the
	// original totals and the new rows roll on a later pass — never overwrite
	// a rollup with a partial recount.
	// Rolled up: BOTH kinds of message charge, so the day's totals mean the
	// same thing they did before the postpaid rows got their own type
	// (config/0091). Leaving invoice_item out would have quietly emptied every
	// rollup for a postpaid company.
	// Deleted: ONLY the prepaid commits (PO ruling 2026-09-09). An
	// invoice_item is evidence for a bill that may not have been raised yet,
	// and deleting it on the ordinary retention window would destroy the line
	// items behind an invoice nobody had sent.
	//
	// The consequence, stated rather than hidden: invoice_item rows are never
	// removed, so on a busy postpaid deployment this table grows without
	// bound. The rollup still keeps REPORTING bounded; it is the raw rows that
	// accumulate. Worth an expiry rule once someone decides when an invoiced
	// line stops being needed.
	//
	// Both statements are one transaction in the DAL (RollupDay), so a crash
	// can never leave a day half-counted.
	n, err := p.upkeep.RollupDay(ctx, company, day)
	if err != nil {
		return err
	}
	if n > 0 {
		p.log.Printf("ledger rollup: company %d day %s — %d commit row(s) -> daily totals", company, day, n)
	}
	return nil
}
