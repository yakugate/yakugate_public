package abuse

import (
	"errors"
	"sort"
)

// The weighted score engine — P10 register row "Weighted spam-score engine".
//
// Every other evaluator in this package answers "how bad is this ONE thing",
// and the monitor keeps the worst answer. That works for signals strong enough
// to act on alone: a spend anomaly of 10x, or an AIT prefix concentration, each
// justify a throttle by themselves.
//
// The CONTENT signals are not like that. Formatting anomaly and opt-out rate
// were both ruled "score input only, never its own rule" (PO 2026-09-09),
// precisely because either alone is weak evidence — a real sale announcement
// looks like spam, and a badly targeted campaign collects STOPs. Their value is
// corroborative: loud formatting AND a high opt-out rate AND a velocity spike
// together mean something none of them means apart.
//
// So this engine sums their weighted contributions and acts on the total.
//
// CAPPED AT THROTTLE BY RULING. A combined score may warn and it may throttle;
// it may never suspend. The trap that avoids is real — if signals judged too
// weak to act on individually can add up to cutting off a paying customer,
// nothing has actually been made safer, the decision has just been laundered
// through arithmetic. Slowing a bad campaign within minutes is the protection
// worth having; ending a commercial relationship stays a human decision.
type ScoreParams struct {
	Common
	// Weights are points per contributing signal, keyed by signal name. A
	// signal absent from the map contributes NOTHING — that is deliberate, so
	// adding a new evaluator cannot start affecting accounts before an
	// operator has decided what it is worth.
	Weights map[string]float64 `json:"weights"`
	// PointsWarn / PointsThrottle are the totals those actions need. A zero or
	// negative threshold disables that level rather than firing on everything,
	// which is the safe reading of an unconfigured value.
	PointsWarn     float64 `json:"points_warn"`
	PointsThrottle float64 `json:"points_throttle"`
	// MinSignals is how many DISTINCT signals must contribute before the score
	// counts at all. The whole argument for this engine is corroboration, so
	// one signal loud enough to cross the threshold on its own is exactly what
	// it must not act on — that would re-create the per-signal rule the PO
	// declined. Below 2 it is treated as 2.
	MinSignals int `json:"min_signals"`
}

// Validate names the first key this rule cannot run without.
func (p ScoreParams) Validate() error {
	if len(p.Weights) == 0 {
		return errors.New("weights is missing or empty")
	}
	return nil
}

// Contribution is one signal's input to an account's score.
type Contribution struct {
	Signal string
	// Strength is 0..1 — how strongly this signal fired, NOT its importance.
	// Importance is the operator's weight. Keeping them separate means an
	// evaluator can say "this is mildly unusual" without deciding how much
	// that ought to matter to this particular customer.
	Strength float64
}

// Score is the outcome for one account in one window.
type Score struct {
	Severity Severity
	Points   float64
	// Signals is the contributing signal names, sorted, so an audit line and a
	// portal row read the same way every time.
	Signals []string
}

// EvalScore sums weighted contributions and returns the action they justify.
func EvalScore(contribs []Contribution, p ScoreParams) Score {
	min := p.MinSignals
	if min < 2 {
		min = 2
	}
	var total float64
	names := make([]string, 0, len(contribs))
	seen := map[string]bool{}
	for _, c := range contribs {
		w, ok := p.Weights[c.Signal]
		if !ok || w <= 0 {
			continue // not configured, or configured to nothing
		}
		s := c.Strength
		if s <= 0 {
			continue
		}
		if s > 1 {
			s = 1 // a strength above 1 is a bug in the caller, not licence to score higher
		}
		if !seen[c.Signal] {
			seen[c.Signal] = true
			names = append(names, c.Signal)
		}
		total += w * s
	}
	sort.Strings(names)
	out := Score{Points: total, Signals: names}
	if len(names) < min {
		return out // not corroborated: no action, whatever the total
	}
	switch {
	case p.PointsThrottle > 0 && total >= p.PointsThrottle:
		out.Severity = Throttle
	case p.PointsWarn > 0 && total >= p.PointsWarn:
		out.Severity = Warn
	}
	return out
}
