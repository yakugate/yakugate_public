package console

import (
	"bufio"
	"context"
	"fmt"

	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/provision"
)

// Company balances. [SCOPE P14-BALANCE]
//
// The one screen in this console that moves money, so it is the most
// deliberate one: the amount and a reason are asked, the before and after are
// shown, and the amount has to be typed again before anything is written. The
// rules — one transaction for the balance, its ledger row and its audit row;
// never below zero; the live Redis counter changed too — are in
// provision.AdjustBalance.

func (p *Process) listCompanies(ctx context.Context, sc *bufio.Scanner) {
	all, err := p.store.ListCompanies(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(all) == 0 {
		p.printf("\n  No companies yet — and nothing else can be added until there is one.\n")
		return
	}
	p.printf("\n   %-6s %-32s %-9s %s\n", "id", "name", "billing", "balance")
	list := []opt{}
	for _, c := range all {
		list = append(list, row(c.ID, fmt.Sprintf("   %-6d %-32s %-9s %s", c.ID, trim(c.Name, 32), provision.BillingWord(c.Prepaid), c.Balance), ""))
	}
	id, ok := p.chooseID(sc, "Open a company by id (0 to go back)", list)
	if !ok || id == 0 {
		return
	}
	rows, err := p.store.ListCompanies(ctx, 500)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	for _, c := range rows {
		if c.ID == id {
			p.companyMenu(ctx, sc, c)
			return
		}
	}
	p.errf("  There is no company #%d in this list.\n", id)
}

func (p *Process) companyMenu(ctx context.Context, sc *bufio.Scanner, c provision.CompanyRow) {
	for {
		if c.Prepaid {
			p.printf("\n  Company #%d %q — prepaid, balance %s\n", c.ID, c.Name, c.Balance)
		} else {
			p.printf("\n  Company #%d %q — postpaid: its charges are invoiced\n", c.ID, c.Name)
		}
		// No rate into the base currency = its amounts cannot be compared
		// with a vendor's, and the loss check refuses its messages.
		if d, err := p.store.GetCompany(ctx, c.ID); err == nil && !provision.BaseRateSet(d) {
			p.warnf("  It has no Base Conversion Rate (%s for one %s): what it pays cannot be compared with\n", config.BaseCurrency(), d.Currency)
			p.warnf("  what a vendor costs. Set it under 3 Change a setting.\n")
		}
		opts := []opt{}
		if c.Prepaid {
			opts = append(opts, item("1", "Top up, or correct the balance"))
		} else {
			opts = append(opts, item("1", "Top up — only for a prepaid company"))
		}
		opts = append(opts, item("2", "Recent balance movements"))
		opts = append(opts, item("3", "Change a setting — name, billing, emails, rate to "+config.BaseCurrency()+", template approval"))
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			if !c.Prepaid {
				// The portal refuses it too (409): a postpaid balance is never
				// drawn down, so money put there would sit unused.
				p.errf("  %s is postpaid: its charges are invoiced and it has no balance to top up\n", c.Name)
				p.errf("  or adjust. Make it prepaid first (3 Change a setting → Billing).\n")
				continue
			}
			p.adjustBalance(ctx, sc, c)
			c = p.refreshCompany(ctx, c)
		case "2":
			p.ledger(ctx, c.ID)
		case "3":
			p.editCompany(ctx, sc, c.ID)
			c = p.refreshCompany(ctx, c)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

func (p *Process) refreshCompany(ctx context.Context, c provision.CompanyRow) provision.CompanyRow {
	rows, err := p.store.ListCompanies(ctx, 500)
	if err != nil {
		return c
	}
	for _, r := range rows {
		if r.ID == c.ID {
			return r
		}
	}
	return c
}

func (p *Process) adjustBalance(ctx context.Context, sc *bufio.Scanner, c provision.CompanyRow) {
	p.printf("\n  Balance of %s: %s\n", c.Name, c.Balance)
	p.printf("  A positive amount tops it up; a negative one (e.g. -250) takes money away.\n")
	p.printf("  Every change is written to the ledger with its reason and cannot be edited\n")
	p.printf("  afterwards — a mistake is put right with a second change the other way.\n\n")
	amount, ok := p.ask(sc, "Amount")
	if !ok || amount == "" || amount == "0" {
		return
	}
	reason, ok := p.askName(sc, "Reason (e.g. bank transfer ref 4471)")
	if !ok || reason == "" {
		p.errf("  Left alone: a reason is required.\n")
		return
	}
	if err := provision.ValidateBalanceChange(amount, reason); err != nil {
		p.refusal(err)
		return
	}
	v, _ := provision.ParseMicro(amount)
	p.printf("\n  %s %s for %s — %q\n", verb(v), provision.FormatMicro(abs(v)), c.Name, reason)
	if !p.confirm(sc, amount) {
		p.printf("  Left alone.\n")
		return
	}
	ch, err := p.store.AdjustBalance(ctx, c.ID, amount, reason)
	if err != nil {
		p.refusal(err)
		return
	}
	p.okf("\n  Done: %s → %s %s (%s, ledger reference %s).\n",
		ch.Previous, ch.New, ch.Currency, ch.Type, ch.Reference)
	p.printf("  %s.\n", ch.Live)
}

func (p *Process) ledger(ctx context.Context, companyID int64) {
	lines, err := p.store.DB.RecentLedger(ctx, companyID)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	p.printf("\n  Recent top-ups and corrections (charges for messages are not listed). UTC.\n\n")
	p.printf("   %-16s %-7s %14s %14s %-10s %s\n", "when", "type", "amount", "balance after", "by", "reason")
	n := 0
	for _, l := range lines {
		n++
		p.printf("   %-16s %-7s %14s %14s %-10s %s\n", l.When, l.Type, l.Amount, l.After, trim(l.By, 10), trim(l.Reason, 40))
	}
	if n == 0 {
		p.printf("   none yet\n")
	}
}

func verb(v int64) string {
	if v < 0 {
		return "Take away"
	}
	return "Add"
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
