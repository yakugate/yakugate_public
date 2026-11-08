package httpserver

import (
	"context"
	"strings"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/refdata"
)

// priceResult computes the optional accept-time cost/balance disclosure for
// one accepted recipient (PO 2026-07-27, S-05) — nil/empty whenever the
// account has the corresponding sms_service_config flag off, the rater isn't
// available (config store unreachable), or nothing priced the message (no
// card / no matching row: rated but unpriced, same as P9's own fallback).
// [SCOPE P1-COST]
//
// Both figures are in the COMPANY's balance currency (QA API-005): cost is
// what the balance is charged for this message — the amount the enqueue stage
// took at acceptance when it took one, else the same price the billing role
// will take (the route's card, the account's default card, converted as
// billing converts) — and remainingBalance is the balance after it. The reply
// used to quote the card's own price in the card's currency (0.05 EUR) beside
// a balance in the company's (USD), with the balance moved by the EUR figure.
//
// remainingBalance is the real balance (QA API-004, BILL-005), not the
// reference cache's copy of it, which is minutes old: the figure the
// reservation left when one was taken, else the balance read now — less this
// message's price for a prepaid company that will be charged later (on
// submit or delivery), as it stands for a postpaid one (API-017).
func (p *Process) priceResult(ctx context.Context, m *model.Message, acct *refdata.Account, e *pipeline.Envelope) (cost *float64, currency string, remaining *float64, breakdown priceBreakdown) {
	if p.rater == nil || (!acct.ShowRequestCost && !acct.ShowRemainingBalance) {
		return nil, "", nil, breakdown
	}
	rated := p.replyRating(ctx, m, acct)
	var charged float64
	if e != nil {
		charged = e.Charged
	}
	q, ok := quoteOf(rated, charged, acct.CompanyCurrency)
	if !ok {
		return nil, "", nil, breakdown
	}
	if acct.ShowRequestCost {
		total := q.total
		cost, currency = &total, q.currency
		// The breakdown behind that price (PO 2026-09-03). The total is
		// tax-INCLUSIVE, so `total` is the same number as `cost` by design —
		// it is there because a client reading net and tax expects them to
		// add up to something named total.
		breakdown = priceBreakdown{Net: q.net, Tax: q.tax, TaxPercentage: q.pct, Total: &total}
	}
	if acct.ShowRemainingBalance {
		var live float64
		liveOK := false
		if e == nil || e.BalanceAfter == nil { // the reservation's own figure needs no read
			live, liveOK = p.liveBalance(ctx, acct)
		}
		remaining = replyBalance(acct, e, live, liveOK, q.total)
	}
	return cost, currency, remaining, breakdown
}

// replyRating is the selling price the reply states: the one fixed when the
// message was accepted (its quote, D-130) — what billing charges — and only
// for a message with no quote (none priced here, or an older path) the cards
// as they stand. The reply used to price the message a second time, with its
// own currency lookup: when a provider's rate moved between the two the
// customer was told one cost and charged another (go-live test 18 Oct, P9 B7:
// told 2.8, charged 2.815). A quote with no price says nothing was priced:
// no cost is stated, as nothing will be charged.
func (p *Process) replyRating(ctx context.Context, m *model.Message, acct *refdata.Account) billing.Rated {
	if q, ok := billing.ParseQuote(m.Quote); ok {
		return q.MT.OnRating(billing.Rated{}, m.Segments, q.Parts)
	}
	return p.rater.RateFull(ctx, m.RouteID, m.Destination, m.MCC, m.MNC, m.Segments, billing.RateOpts{
		CompanyCurrency: acct.CompanyCurrency,
		DefaultSellCard: acct.DefaultMTSellCard,
		FX:              p.fx,
		VendorID:        m.VendorID,
	})
}

// replyQuote is the price an accepted reply states, in one currency.
type replyQuote struct {
	total    float64 // what the balance is charged, tax included
	currency string
	net, tax *float64 // the total's split; nil when the rating has none
	pct      *float64
}

// quoteOf turns a rating into the reply's price. charged > 0 is what the
// acceptance reservation took, and wins: the reply states what was really
// taken. Otherwise the rating's SellCharge — the sell total converted into
// the company's currency, exactly what billing charges. The net/tax split is
// scaled with it, so it stays in the same currency and still adds up (tax is
// the total less the rounded net). Rounded to the ledger's four places.
func quoteOf(r billing.Rated, charged float64, companyCurrency string) (replyQuote, bool) {
	total := charged
	if total <= 0 {
		if r.SellCharge == nil {
			return replyQuote{}, false
		}
		total = *r.SellCharge
	}
	q := replyQuote{total: roundMoney(total), currency: r.Currency, pct: r.SellTaxPct}
	if companyCurrency != "" {
		q.currency = companyCurrency // SellCharge (and a reservation) are in the balance's currency
	}
	if r.Sell != nil && *r.Sell > 0 && r.SellNet != nil {
		k := total / *r.Sell
		net := roundMoney(*r.SellNet * k)
		q.net = &net
		if r.SellTax != nil {
			tax := roundMoney(q.total - net)
			q.tax = &tax
		}
	}
	return q, true
}

// replyBalance is the balance an accepted reply shows. Taken at acceptance:
// the balance that reservation left. Otherwise the balance now (live; the
// cached copy only when it cannot be read), less this message's price when a
// prepaid company will be charged for it later — never for a postpaid company,
// whose balance an MT never moves, nor for a message already taken from the
// balance.
func replyBalance(acct *refdata.Account, e *pipeline.Envelope, live float64, liveOK bool, cost float64) *float64 {
	if e != nil && e.BalanceAfter != nil {
		v := roundMoney(*e.BalanceAfter)
		return &v
	}
	rb := acct.EffectiveBalance()
	if liveOK {
		rb = live
	}
	pending := e == nil || e.Charged <= 0
	if acct.Prepaid() && pending {
		rb -= cost
	}
	rb = roundMoney(rb)
	return &rb
}

// remainingAfter is replyBalance against the cached balance, for a message
// with no reservation behind it (QA companies API-017: only a PREPAID
// company has the price taken from its balance).
func remainingAfter(acct *refdata.Account, sell float64) *float64 {
	return replyBalance(acct, nil, 0, false, sell)
}

// liveBalance reads the company's balance as it stands now: the live Redis
// counter when Redis holds it, else companies.balance. ok=false when there is
// no company or it cannot be read (the caller falls back to the cache).
func (p *Process) liveBalance(ctx context.Context, acct *refdata.Account) (float64, bool) {
	if acct.CompanyID == 0 {
		return 0, false
	}
	var (
		b   float64
		err error
	)
	switch {
	case p.balAdmin != nil:
		b, err = p.balAdmin.Balance(ctx, acct.CompanyID)
	case p.balances != nil:
		// No Redis: read at most once per company per TTL (billing.BalanceCache).
		// The balance of record is read through the DAL's statements on this
		// process's config pool (dal.OpenBilling's store).
		p.shownOnce.Do(func() { p.shown = billing.NewBalanceCache(p.balances) })
		b, err = p.shown.Balance(ctx, acct.CompanyID)
	default:
		return 0, false
	}
	if err != nil {
		p.log.Warn(logging.SystemScope, "balance_read", "live balance unreadable — the reply shows the cached one",
			logging.Fields{"company": acct.CompanyID, "error": err.Error()})
		return 0, false
	}
	return b, true
}

// chargedLater is when an account is charged, if that is later than
// acceptance: "submit" or "delivery" (its charge point), else "". The reply
// carries it beside the cost, because for such an account the cost is the
// price as it stands now, not an amount taken — the charge is made later, at
// the price in force then (QA BUG-YK-RC-012).
func chargedLater(acct *refdata.Account) string {
	switch p := strings.ToLower(strings.TrimSpace(acct.ChargePoint)); p {
	case "submit", "delivery":
		return p
	}
	return ""
}
