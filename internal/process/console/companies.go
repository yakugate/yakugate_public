package console

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/provision"
)

// The Companies menu: adding and listing companies.

func (p *Process) companiesMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		p.printf("\n  Companies\n")
		p.printf("  Every account and vendor belongs to one; balance and limits live here.\n\n")
		opts := []opt{}
		opts = append(opts, item("1", "Add a company"))
		opts = append(opts, item("2", "List companies"))
		opts = append(opts, item("0", "Back"))
		choice, ok := p.choose(sc, "Choose", "", opts)
		if !ok {
			return
		}
		switch choice {
		case "1":
			p.addCompany(ctx, sc)
		case "2":
			p.listCompanies(ctx, sc)
		case "0", "":
			return
		default:
			p.errf("  %q is not one of the choices.\n", choice)
		}
	}
}

// addCompany asks only what cannot move money. [SCOPE P14-COMPANY]
func (p *Process) addCompany(ctx context.Context, sc *bufio.Scanner) {
	p.printf("\n  Add a company. 0 to abandon.\n\n")
	var c provision.Company
	var ok bool
	if c.Name, ok = p.askName(sc, "Name"); !ok || c.Name == "" {
		return
	}
	// The operator's own number for this customer (config/0141): optional.
	for {
		m, ok := p.ask(sc, "Master number — your own number for this customer, shown on its invoices and logs [none]")
		if !ok || m == "0" {
			return
		}
		clean, err := provision.CleanMasterNumber(m)
		if err != nil {
			p.refusal(err)
			continue
		}
		c.MasterNumber = clean
		break
	}
	cur, ok := p.ask(sc, "Currency its balance is kept in (three letters) [USD]")
	if !ok || cur == "0" {
		return
	}
	if cur == "" {
		cur = "USD" // the column's own default, which is what the portal leaves too
	}
	c.Currency = cur

	// Base Conversion Rate (Saleem 2026-10-07): required for a currency that
	// is not the system base currency — what the loss check and the
	// dashboards turn this company's amounts into the base currency by. A
	// currency that is not one is left to ValidateCompany below to refuse.
	if base := config.BaseCurrency(); !provision.KeepsBaseCurrency(cur) && len(strings.TrimSpace(cur)) == 3 {
		code := strings.ToUpper(strings.TrimSpace(cur))
		p.printf("  The system base currency is %s. What a customer pays and what a vendor costs are\n", base)
		p.printf("  compared in it, so this company needs its rate: how much %s ONE %s is worth.\n", base, code)
		for {
			r, ok := p.ask(sc, fmt.Sprintf("Base Conversion Rate — %s for one %s (like 0.0036)", base, code))
			if !ok || r == "0" {
				return
			}
			clean, err := provision.CleanBaseRate(code, r)
			if err != nil {
				p.refusal(err)
				continue
			}
			c.BaseRate = clean
			break
		}
	}

	// Postpaid is the Admin API's default: a company created there with no
	// billing_config is read as postpaid by the engine.
	billing, ok := p.askChoice(sc, "Prepaid or postpaid", []string{provision.BillingPrepaid, provision.BillingPostpaid},
		provision.BillingPostpaid, "Prepaid: every charge is taken from its balance, and its accounts are refused once it\n  runs out. Postpaid: charges are invoiced, and the balance is not used.")
	if !ok {
		return
	}
	c.Prepaid = billing == provision.BillingPrepaid

	// The four contact lists the Admin API requires (QA API-007). Invoices go
	// to billing, rate notices to rates; one address may serve them all.
	p.printf("  Email addresses: one or more, separated by commas.\n")
	for {
		e, ok := p.ask(sc, "Company email")
		if !ok || e == "0" {
			return
		}
		if _, err := provision.EmailList("company", e); err != nil {
			p.refusal(err)
			continue
		}
		c.CompanyEmail = e
		break
	}
	for _, f := range []struct {
		label string
		v     *string
	}{{"support", &c.SupportEmail}, {"billing", &c.BillingEmail}, {"rates", &c.RatesEmail}} {
		for {
			e, ok := p.ask(sc, fmt.Sprintf("%s email [same as company email]", strings.ToUpper(f.label[:1])+f.label[1:]))
			if !ok || e == "0" {
				return
			}
			if e == "" {
				e = c.CompanyEmail
			}
			if _, err := provision.EmailList(f.label, e); err != nil {
				p.refusal(err)
				continue
			}
			*f.v = e
			break
		}
	}

	if err := provision.ValidateCompany(c); err != nil {
		p.refusal(err)
		return
	}
	id, err := p.store.CreateCompany(ctx, c)
	if err != nil && id == 0 {
		p.refusal(err)
		return
	}
	p.okf("\n  Created company #%d %q, %s, balance 0.\n", id, c.Name, provision.BillingWord(c.Prepaid))
	if err != nil {
		p.warnf("  %v\n", err) // made, but its audit row was not
	}
	p.printf("  Its balance is not set here: it moves only with a ledger entry beside it,\n")
	p.printf("  so every change can be accounted for. Accounts and vendors can now be\n")
	p.printf("  added under it.\n")
	if c.Prepaid {
		p.warnf("  Prepaid with nothing in it: its accounts cannot send until it is topped up\n")
		p.warnf("  (Companies → List companies → open it → 1).\n")
	}
}

func (p *Process) companies(ctx context.Context) {
	rows, err := p.store.ListCompanies(ctx, 200)
	if err != nil {
		p.errf("  %v\n", err)
		return
	}
	if len(rows) == 0 {
		p.printf("\n  No companies yet — and nothing else can be added until there is one.\n")
		return
	}
	p.printf("\n   %-6s %-32s %s\n", "id", "name", "balance")
	for _, c := range rows {
		p.printf("   %-6d %-32s %s\n", c.ID, trim(c.Name, 32), c.Balance)
	}
	p.printf("\n")
}

// companyRows is the company picker's list, in the format every screen that
// asks "which company?" has always drawn.
func companyRows(companies []provision.CompanyRow) []opt {
	out := make([]opt, 0, len(companies))
	for _, c := range companies {
		out = append(out, row(c.ID, fmt.Sprintf("   %4d  %-28s %s", c.ID, trim(c.Name, 28), c.Balance), ""))
	}
	return out
}
