package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
)

// Changing a company. [SCOPE P14-COMPANY]
//
// One setting at a time, each shown with what it is now, and Enter keeps it.
// What may change and what a good value is are provision.UpdateCompany's
// rules — the Admin API's rules for the same row (companyRoutes.js) — so the
// currency is shown but cannot be changed, and an email list cannot be
// emptied. A refused answer is asked again; 0 goes back, except on the
// days of records kept, where 0 means the gateway's own (YAKU_HOT_DAYS).

func (p *Process) editCompany(ctx context.Context, sc *bufio.Scanner, id int64) {
	d, err := p.store.GetCompany(ctx, id)
	if err != nil {
		p.refusal(err)
		return
	}
	fields := provision.CompanyFields()
	p.printf("\n  Change a setting of %s. Enter keeps a value; 0 goes back.\n", d.Name)
	opts := []opt{}
	for i, f := range fields {
		opts = append(opts, opt{Key: strconv.Itoa(i + 1),
			Text: fmt.Sprintf("   %2d  %-16s %s", i+1, f.Label, trim(companyNow(d, f.Key), 56))})
	}
	for _, f := range provision.CompanyFixed() {
		opts = append(opts, opt{Skip: true,
			Text: fmt.Sprintf("    –  %-16s %s — cannot be changed", f.Label, provision.CompanyValue(d, f.Key)),
			Note: "          " + f.Why})
	}
	opts = append(opts, opt{Key: "0", Text: "    0  Back"})
	pick, ok := p.choose(sc, "Which setting", "0", opts)
	n, _ := strconv.Atoi(pick)
	if !ok || n == 0 {
		return
	}
	if n < 1 || n > len(fields) {
		p.errf("  There is no setting %d.\n", n)
		return
	}
	f := fields[n-1]
	if f.Hint != "" {
		p.printf("  %s\n", f.Hint)
	}
	// On the days kept, 0 is a value — the gateway's own days, which the
	// Admin API writes as no value (null) — so it does not go back; Enter
	// still keeps what is there (go-live test P8 D3, 18 Oct 2026).
	zeroIsValue := f.Key == "hot_days"
	if zeroIsValue {
		p.printf("  Here 0 is a value, not Back: Enter keeps what is set.\n")
	}
	cur := provision.CompanyValue(d, f.Key)
	for {
		var val string
		if f.Kind == "choice" {
			val, ok = p.askChoice(sc, f.Label, f.Choices, cur, "")
		} else {
			val, ok = p.ask(sc, fmt.Sprintf("%s [%s]", f.Label, cur))
			if ok && val == "" {
				val = cur
			}
		}
		if !ok || (val == "0" && !zeroIsValue) {
			return
		}
		if strings.TrimSpace(val) == cur {
			p.printf("  Nothing changed, so nothing was saved.\n")
			return
		}
		if f.Key == "billing" && !p.confirmBilling(sc, d, val) {
			p.printf("  Left alone.\n")
			return
		}
		changed, err := p.store.UpdateCompany(ctx, id, f.Key, val)
		switch {
		case err != nil && changed:
			// Saved; only its audit row is missing, and the operator is told.
			p.okf("  Saved.\n")
			p.warnf("  %v\n", err)
			return
		case err != nil:
			p.refusal(err)
			if provision.Refused(err) && f.Kind != "choice" {
				continue // asked again: nothing typed is lost but the bad answer
			}
			return
		case !changed:
			p.printf("  Nothing changed, so nothing was saved.\n")
			return
		}
		p.okf("  Saved. A running gateway picks it up within 30 seconds.\n")
		return
	}
}

// confirmBilling says what switching prepaid/postpaid does to this company,
// and asks.
func (p *Process) confirmBilling(sc *bufio.Scanner, d provision.CompanyDetail, to string) bool {
	bal, _ := provision.ParseMicro(d.Balance)
	if to == provision.BillingPrepaid {
		p.warnf("\n  From now on every charge for %s's messages is taken from its balance,\n", d.Name)
		p.warnf("  which is %s %s now.\n", provision.FormatMicro(bal), d.Currency)
		if bal <= 0 {
			p.warnf("  With nothing in it, its accounts are refused (402) until it is topped up.\n")
		}
	} else {
		p.warnf("\n  From now on %s's charges are invoiced and its balance is not used.\n", d.Name)
		if bal != 0 {
			p.warnf("  The %s %s in it stays, untouched; it cannot be topped up or corrected\n", provision.FormatMicro(bal), d.Currency)
			p.warnf("  while the company is postpaid.\n")
		}
	}
	yes, ok := p.askYesOK(sc, fmt.Sprintf("Make %s %s?", d.Name, to), false)
	return ok && yes
}

// companyNow is a setting's current value as the menu shows it.
func companyNow(d provision.CompanyDetail, key string) string {
	if key == "billing" {
		if d.Prepaid {
			return fmt.Sprintf("prepaid — balance %s %s", d.Balance, d.Currency)
		}
		return "postpaid — charges are invoiced"
	}
	if key == "base_rate" {
		switch {
		case provision.KeepsBaseCurrency(d.Currency):
			return "1 — it keeps the base currency, nothing to convert"
		case !provision.BaseRateSet(d):
			return "NOT SET — its amounts cannot be compared; set it"
		case d.BaseProvider != "":
			return fmt.Sprintf("%s (one %s) — its currency provider's live rate wins while it answers", d.BaseRate, d.Currency)
		}
		return fmt.Sprintf("%s (one %s)", d.BaseRate, d.Currency)
	}
	return orDash(provision.CompanyValue(d, key))
}
