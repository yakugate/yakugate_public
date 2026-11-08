package abuse

import (
	"fmt"
	"math"
)

// Reputation — P10 register row "Sender/account reputation score" (ruled
// 2026-09-09: shown only, never feeds an action; built 2026-09-15, PO: "yes,
// as proposed").
//
// One 0-100 number per account with the reasons beside it, recalculated once
// a day, for operators deciding how much room a customer deserves (the
// per-account abuse overrides are where that decision is acted on — by a
// person). Nothing reads this score to act: it is not an input to the
// weighted score, and must never become one (ruling).
//
// It starts at 100 and loses points for what the evidence says, each loss
// capped so no single input can sink an account on its own. An input with too
// little traffic behind it says so and costs nothing: a new customer's first
// hundred messages are not a delivery rate.

// ReputationInputs are one account's facts for the day's score.
type ReputationInputs struct {
	AgeDays                    int // since the account was created
	Warns, Throttles, Suspends int // abuse-monitor actions, last 90 days
	Final, Delivered           int // parts with a final outcome / delivered, last 30 days
	LookedUp, Unknown          int // HLR answers / not found, last 30 days
	Sent, OptOuts              int // parts sent / STOP opt-outs, last 30 days
	Submitted, Refused         int // parts submitted / refused for spam or a blocked number, last 30 days

	// An operator reset the reputation (config/0143): the counts above are
	// then only what happened SINCE, and the lines say so instead of naming
	// a window the evidence no longer covers. Age is not evidence: it stays.
	Reset        bool `json:",omitempty"`
	ResetDaysAgo int  `json:",omitempty"`
}

// ReputationReason is one line of the explanation.
type ReputationReason struct {
	Text   string  `json:"text"`
	Points float64 `json:"points"` // 0 or negative
}

// Minimum evidence before an input counts.
const (
	repMinFinal     = 100
	repMinLookedUp  = 100
	repMinSent      = 500
	repMinSubmitted = 100
)

// Reputation scores one account.
func Reputation(in ReputationInputs) (int, []ReputationReason) {
	score := 100.0
	var why []ReputationReason
	add := func(pts float64, format string, a ...any) {
		pts = math.Round(pts*10) / 10
		score += pts
		why = append(why, ReputationReason{Text: fmt.Sprintf(format, a...), Points: pts})
	}

	// "in 90 days", or "since the reset" when a reset cut that window short.
	span := func(days int) string {
		if in.Reset && in.ResetDaysAgo < days {
			return "since the reset"
		}
		return fmt.Sprintf("in %d days", days)
	}

	switch months := in.AgeDays / 30; {
	case in.AgeDays < 30:
		add(-15, "new account: %d day(s) old", in.AgeDays)
	case in.AgeDays < 90:
		add(-8, "%d month(s) old", months)
	case in.AgeDays < 180:
		add(-3, "%d months old", months)
	default:
		add(0, "%d months old", months)
	}

	if a := in.Warns + in.Throttles + in.Suspends; a == 0 {
		add(0, "no abuse actions %s", span(90))
	} else {
		add(-math.Min(50, float64(5*in.Warns+12*in.Throttles+25*in.Suspends)),
			"abuse actions %s: %d warning(s), %d throttle(s), %d suspension(s)", span(90), in.Warns, in.Throttles, in.Suspends)
	}

	if in.Final < repMinFinal {
		add(0, "too few final results %s to judge delivery (%d)", span(30), in.Final)
	} else {
		rate := 100 * float64(in.Delivered) / float64(in.Final)
		add(-math.Min(30, math.Max(0, 90-rate)), "%.0f%% delivered %s", rate, span(30))
	}

	if in.LookedUp < repMinLookedUp {
		add(0, "too few number lookups %s to judge the list (%d)", span(30), in.LookedUp)
	} else {
		share := 100 * float64(in.Unknown) / float64(in.LookedUp)
		add(-math.Min(20, math.Max(0, (share-5)*1.5)), "%.1f%% of looked-up numbers do not exist", share)
	}

	if in.Sent < repMinSent {
		add(0, "too few messages %s to judge opt-outs (%d)", span(30), in.Sent)
	} else {
		per := 1000 * float64(in.OptOuts) / float64(in.Sent)
		add(-math.Min(20, math.Max(0, per-5)), "%.1f opt-outs per 1,000 messages", per)
	}

	if in.Submitted < repMinSubmitted {
		add(0, "too few submits %s to judge refusals (%d)", span(30), in.Submitted)
	} else {
		share := 100 * float64(in.Refused) / float64(in.Submitted)
		add(-math.Min(20, math.Max(0, (share-2)*2)), "%.1f%% of submits refused for spam or a blocked number", share)
	}

	s := int(math.Round(score))
	if s < 0 {
		s = 0
	}
	if s > 100 {
		s = 100
	}
	return s, why
}
