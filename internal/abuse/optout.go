package abuse

// Opt-out rate — P10 register row "Opt-out rate signal".
//
// The most honest abuse signal available, because it is the RECIPIENTS saying
// so rather than a heuristic guessing. An account whose recipients keep
// replying STOP is sending to people who did not ask for it.
//
// This row was Blocked until 2026-09-09 for a simple reason: a rate had no
// numerator. Nothing recognised STOP and nothing ever wrote an opt-out, so
// there was nothing to count. internal/mo now reads STOP/START, records the
// opt-out against the sending account and writes the blocklist row, so the feed
// is real.
//
// SCOPE NARROWED BY RULING (PO 2026-09-09). The register row said "opt-out /
// complaint-rate". Complaint rate means recipients reporting to the OPERATOR or
// a short-code complaints service; that data arrives from outside and there is
// no channel for it, so it is deferred rather than faked. This is opt-out rate
// and the row was retitled to say so.
//
// SCORE INPUT ONLY, never its own warn/throttle/suspend rule. A high opt-out
// rate is not proof of abuse — a legitimate campaign with a poor offer collects
// STOPs too. What it reliably says is that people do not want this list, which
// is worth an operator looking at and weak grounds for automatically cutting
// off a paying customer.

// OptOutParams tune the signal.
type OptOutParams struct {
	Common
	// MinMessages is the denominator floor. A rate computed over a handful of
	// messages is noise: two STOPs out of three messages is 667 per thousand
	// and means nothing at all.
	MinMessages int `json:"min_messages"`
	// PerThousandWarn is where the signal starts contributing, and
	// PerThousandFull where it contributes fully. Expressed per thousand
	// because that is how the industry and the regulators talk about it, and a
	// number an operator can compare against what a carrier quotes them.
	PerThousandWarn float64 `json:"per_thousand_warn"`
	PerThousandFull float64 `json:"per_thousand_full"`
}

// Validate names the first key this rule cannot run without.
func (p OptOutParams) Validate() error { return needWindow(p.Common) }

// EvalOptOutRate returns 0..1 strength for the score engine.
//
// sent is messages this account sent in the window; optOuts is STOPs received
// against it in the same window. The two are deliberately measured over the
// SAME period even though a STOP always lags the message that provoked it —
// over a window of hours that lag averages out, and pairing each STOP to its
// own message would need correlation the MO path does not carry.
func EvalOptOutRate(optOuts, sent int, p OptOutParams) float64 {
	if sent < p.MinMessages || sent <= 0 || optOuts <= 0 {
		return 0
	}
	if p.PerThousandFull <= p.PerThousandWarn || p.PerThousandWarn <= 0 {
		return 0 // unconfigured: contribute nothing rather than everything
	}
	rate := float64(optOuts) * 1000 / float64(sent)
	if rate < p.PerThousandWarn {
		return 0
	}
	s := (rate - p.PerThousandWarn) / (p.PerThousandFull - p.PerThousandWarn)
	if s > 1 {
		s = 1
	}
	return s
}
