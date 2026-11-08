package dlr

import (
	"context"
	"time"

	dlrsvc "github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/store"
)

// unknownVendor is the single bucket every unrecognised webhook cid lands in.
const unknownVendor = "(unknown vendor)"

// MonitorSections publishes P5's picture: what vendors are sending us, what we
// are pushing to customers, and what is still waiting.
func (p *Process) MonitorSections() map[string]any {
	sec := map[string]any{
		// Per vendor: applied / unmatched / no_message_id / refused / error.
		// Receipts that arrive over SMPP are applied by smpp-vendor and
		// appear in ITS section — this is the HTTP webhook leg only.
		"webhooks":    p.inbound.Section(),
		"client_push": p.push.Section(),
	}
	// Posted receipts whose ack the queue store refused, still to be taken
	// off (dlr.AckKeeper). Shown only while there are some.
	if n := p.acks.Kept(); n > 0 {
		sec["client_push_acks_kept"] = n
	}
	// Receipts finished in Redis against receipts that took the record
	// store's path (dlr fast.go), and those kept to be applied again: their
	// record was not written yet, or the record store was away (kept_later,
	// as orphans the re-match sweep applies; held, in memory until the store
	// takes them — lost if this role is stopped meanwhile).
	if p.svc != nil {
		rc := map[string]any{"in_redis": p.svc.Fast(), "kept_later": dlrsvc.KeptForLater(), "held": dlrsvc.HeldReceipts()}
		if p.svc.Fast() {
			fast, full := dlrsvc.FastStats()
			rc["fast"], rc["record_store"] = fast, full
		}
		if p.rcptQueue != nil {
			// The receipts' own list, written out by this role with the
			// records written directly.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			written, putBack, letGo := p.rcptQueue.Stats()
			rc["waiting"], rc["written"], rc["put_back"], rc["let_go"] = p.rcptQueue.Waiting(ctx), written, putBack, letGo
			cancel()
		}
		sec["receipts"] = rc
	}
	// Per customer, so a single account's failing dlr_url is visible instead
	// of being summed into the totals above (2026-09-16).
	if rows := p.byAcct.Section(); rows != nil {
		sec["client_push_accounts"] = rows
	}
	// The receipt backlog: waiting for a customer's bind or dlr_url. Bounded
	// in time so a slow store cannot hold up the sample.
	if ps, ok := p.queue.(store.PendingCountStats); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if dlr, _, err := ps.PendingCounts(ctx); err == nil {
			sec["backlog"] = dlr
		}
	}
	// The same backlog per account: whose receipts are waiting, and since
	// when (D-127).
	if pa, ok := p.queue.(store.PendingByAccountStats); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if m, err := pa.PendingByAccount(ctx, "dlr", monitor.BacklogAccountsLimit); err == nil {
			sec["backlog_accounts"] = monitor.BacklogAccounts(m)
		}
	}
	// Receipts owed by vendors ("awaiting a receipt") are not published any
	// more (29 Oct 2026): they were read from the receipt facts' waiting sets
	// in the main Redis, which are gone. Every count on Monitoring comes from
	// the live length of a queue (Saleem: "all countings must be brought from
	// live queues counts from local redis").
	// Receipts this process keeps to look for again (their facts were not in
	// Redis yet), said while there are some.
	if n := dlrsvc.ReceiptsLookedForAgain(); n > 0 {
		sec["receipts_looked_for_again"] = n
	}
	// Per account, the checks its receipt rate on this gateway gave nothing
	// (4 Nov 2026): a queue held by its rate, not one nothing takes out.
	if held := p.rateHeld.Section(); held != nil {
		sec["rate_held"] = held
	}
	out := map[string]any{"dlr": sec}
	// Status-code rules that matched in the last minute (vendorstatus/hits.go).
	if codes := p.vstatus.Section(); codes != nil {
		out["vendor_codes"] = codes
	}
	// Client receipts the queue store refused, kept in memory until it takes
	// them (dlr/keptpush.go). Said only while there are some.
	if n := dlrsvc.KeptClientReceipts(); n > 0 {
		out["client_receipts_kept"] = n
	}
	// The tries to give customers their receipts (dlr_sub_, D-140).
	if st := p.tries.Stats(); st != nil {
		out["client_tries"] = st
	}
	// What this role has put on the record queue: with the DB worker, the
	// database work of the receipts it finished in Redis.
	return p.recQ.AddTo(out)
}
