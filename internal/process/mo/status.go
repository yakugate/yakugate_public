package mo

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/store"
)

// unknownVendor is the single bucket every unrecognised webhook cid lands in.
const unknownVendor = "(unknown vendor)"

// MonitorSections publishes P6's picture: MO vendors are sending us, MO we
// are pushing to customers, and MO still waiting.
func (p *Process) MonitorSections() map[string]any {
	sec := map[string]any{
		// Per vendor: staged / unrouted / unreadable / refused / error. MO
		// that arrives over SMPP is taken in by smpp-vendor and appears in
		// its section; this is the HTTP webhook leg.
		"webhooks":    p.hooks.Section(),
		"client_push": p.push.Section(),
	}
	// Per customer, so "is my MO reaching me" is answerable for one account
	// rather than only for the gateway (2026-09-16). Left out entirely when
	// nothing has been pushed, rather than published as an empty list.
	if rows := p.byAcct.Section(); rows != nil {
		sec["client_push_accounts"] = rows
	}
	if ps, ok := p.queue.(store.PendingCountStats); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, mo, err := ps.PendingCounts(ctx); err == nil {
			sec["backlog"] = mo
		}
	}
	// The same backlog per account: whose MO is waiting, and since when
	// (D-127). Its own short timeout, so it can never cost the totals above.
	if pa, ok := p.queue.(store.PendingByAccountStats); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if m, err := pa.PendingByAccount(ctx, "mo", monitor.BacklogAccountsLimit); err == nil {
			sec["backlog_accounts"] = monitor.BacklogAccounts(m)
		}
	}
	// Per account, the checks its MO rate on this gateway gave nothing
	// (4 Nov 2026): a queue held by its rate, not one nothing takes out.
	if held := p.rateHeld.Section(); held != nil {
		sec["rate_held"] = held
	}
	// The tries to give customers their MO (mo_sub_, D-140).
	if st := p.tries.Stats(); st != nil {
		sec["client_tries"] = st
	}
	return p.recQ.AddTo(map[string]any{"mo": sec})
}
