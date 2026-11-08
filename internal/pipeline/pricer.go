package pipeline

import (
	"context"

	"github.com/yakugate/yakugate/internal/billing"
)

// RaterPricer prices a message at accept exactly as the billing role does
// (billing.Rater.RateFull with the route's sell card, the account's default
// MT card and the company's currency), so the amount reserved is the amount
// that will be charged (PO 2026-09-17).
type RaterPricer struct {
	R  *billing.Rater
	FX *billing.FXResolver
}

func (p RaterPricer) Price(ctx context.Context, e *Envelope) float64 {
	if p.R == nil || e == nil || e.Msg == nil || e.Account == nil {
		return 0
	}
	rated := p.R.RateFull(ctx, e.Msg.RouteID, e.Msg.Destination, e.Msg.MCC, e.Msg.MNC, e.Msg.Segments,
		billing.RateOpts{CompanyCurrency: e.Account.CompanyCurrency, CompanyID: e.Account.CompanyID, DefaultSellCard: e.Account.DefaultMTSellCard, FX: p.FX,
			VendorID: e.Msg.VendorID})
	if rated.SellCharge == nil || *rated.SellCharge <= 0 {
		return 0
	}
	return *rated.SellCharge
}

// Quote is what the message costs, fixed now (billing.Quote, 2026-10-13): the
// message itself for the parts counted here, one delivery receipt and one
// number lookup, each by the cards as they stand at this moment. A side with
// no card or no rate is left out, and is then never charged — whatever card
// is set later. It rides the queue row onto the record, where the billing
// role charges by it.
func (p RaterPricer) Quote(ctx context.Context, e *Envelope) (billing.Quote, bool) {
	if p.R == nil || e == nil || e.Msg == nil || e.Account == nil {
		return billing.Quote{}, false
	}
	parts := e.Msg.Segments
	if parts < 1 {
		parts = 1
	}
	base := billing.RateOpts{CompanyCurrency: e.Account.CompanyCurrency, CompanyID: e.Account.CompanyID, FX: p.FX, VendorID: e.Msg.VendorID}
	q := billing.Quote{V: billing.QuoteVersion, Parts: parts}
	mt := base
	mt.DefaultSellCard = e.Account.DefaultMTSellCard
	q.MT = billing.PriceOf(p.R.RateFull(ctx, e.Msg.RouteID, e.Msg.Destination, e.Msg.MCC, e.Msg.MNC, parts, mt))
	if q.MT == nil {
		// No price, no charge — for its receipt and its lookup too (D-130).
		// They are left out of the quote, so nothing that reads it
		// (summary_lookup's charged count, the lookup log) counts them.
		return q, true
	}
	// One receipt: the rule's receipt card, else the account's default one.
	dlr := base
	dlr.DefaultSellCard, dlr.DLR = e.Account.DefaultDLRSellCard, true
	q.DLR = billing.PriceOf(p.R.RateFull(ctx, e.Msg.RouteID, e.Msg.Destination, e.Msg.MCC, e.Msg.MNC, 1, dlr))
	// One lookup: the account's lookup card, no rule (as the billing role
	// priced it) — and only a lookup the billing role charges: our own data
	// or a live answer, a live miss when the account says so
	// (billing.LookupCharged). The HLR stage has run, so the source is known.
	// A range, the country code or the client's own code is free, and was
	// still quoted: summary_lookup counted it as charged (it reads the
	// quote), so its "charged" figures were higher than what billing took
	// (go-live test 18 Oct, P9 D7).
	if e.Account.DefaultHLRSellCard > 0 && billing.LookupCharged(e.Msg.HLRSource, e.Account.ChargeFailedHLR) {
		q.HLR = billing.PriceOf(p.R.RateFull(ctx, 0, e.Msg.Destination, e.Msg.MCC, e.Msg.MNC, 1,
			billing.RateOpts{CompanyCurrency: e.Account.CompanyCurrency, CompanyID: e.Account.CompanyID, FX: p.FX, DefaultSellCard: e.Account.DefaultHLRSellCard}))
	}
	return q, true
}

// Ready reports whether a message can be priced at all: the rate cards have
// been read at least once.
//
// Go-live test P1 E4 (2026-10-12): a role restarted while the config store
// was down came up from the copy of the reference data in Redis and took
// sends — with no prices. Price answered 0, nothing was reserved, and a
// prepaid account with a balance for four messages had seven accepted and
// none charged. A gateway that cannot say what a message costs must not take
// it: the enqueue stage refuses with 503 until this is true.
func (p RaterPricer) Ready() bool { return p.R.Loaded() }

// Both billing reservers say the balance a hold leaves, so the send reply can
// show the real figure (QA API-004).
var (
	_ BalanceReserver = billing.DBReserver{}
	_ BalanceReserver = (*billing.RedisReserver)(nil)
)
