package billing

import (
	"context"
	"strings"
)

// ToBase is what one unit of a company's currency is worth in the base
// currency: 1 for a company that keeps the base currency; the company's base
// currency provider's rate while it has one ("in real time"); else its Base
// Conversion Rate. A stored 0 is "not set" (config/0171, 3 Nov 2026: the
// column's default was 1, so a real rate of 1 could not be kept): the
// gateway's rate for the pair (Pair) is used, and with none the answer is
// "not known". A stored 1 is a rate like any other.
//
// cur, rate and crp are the company's currency, base_conversion_rate and
// base_thirdparty_crp_id. It is one rule read in two places: the route stage
// compares prices in the base currency with it, and the abuse monitor turns
// its spend floor, an amount in the base currency, into the company's own
// (QA 2026-10-08, B-03). fx may be nil: the stored rate still answers.
func ToBase(ctx context.Context, fx *FXResolver, base, cur string, rate float64, crp string) (float64, bool) {
	if strings.EqualFold(cur, base) {
		return 1, true
	}
	if crp != "" && fx != nil {
		if live, ok := fx.Resolve(ctx, crp, cur, base); ok && live.Rate > 0 {
			return live.Rate, true
		}
	}
	if rate > 0 {
		return rate, true
	}
	if r, ok := fx.Pair(ctx, cur, base); ok {
		return r, true
	}
	return 0, false
}
