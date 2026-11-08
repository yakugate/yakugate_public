package abuse

import "errors"

// Severity is the action level a signal assigns to an account for one window.
type Severity int

const (
	None Severity = iota
	Warn
	Throttle
	Suspend
)

func (s Severity) String() string {
	switch s {
	case Warn:
		return "warn"
	case Throttle:
		return "throttle"
	case Suspend:
		return "suspend"
	}
	return "none"
}

// Common carries the params every abuse_rules row shares.
type Common struct {
	WindowMin   int     `json:"window_min"`
	CooldownMin int     `json:"cooldown_min"`
	ThrottlePct float64 `json:"throttle_pct"`
	// ThrottleUnlimitedTPS is the absolute cap applied when throttling an
	// account with unlimited throughput (accounts.throughput=0) — a percentage
	// of unlimited is meaningless. 0/omitted = the default 10 TPS. Data, not
	// code, like every other P10 tunable (B-P10-01, 2026-07-26).
	ThrottleUnlimitedTPS int `json:"throttle_unlimited_tps"`
}

// DefaultThrottleUnlimitedTPS applies when a rule omits throttle_unlimited_tps.
const DefaultThrottleUnlimitedTPS = 10

// DefaultThrottlePct applies when a rule that throttles omits throttle_pct.
// Without it a missing value is 0%, and 0% of any rate is cut to 1 TPS: the
// seeded charset rule has no throttle_pct, so adding a throttle level to it
// took an account from 200/s straight to 1/s (QA 2026-10-08, B-12).
const DefaultThrottlePct = 10

// A rule that lacks a key it needs is not run (QA 2026-10-08, B-04). The
// other way round was the bug: a missing min_msgs let AIT and template drift
// judge an account on one message, and a missing prefix_share let AIT call
// any spread traffic pumping. Validate says which key is missing, so the
// monitor can log it instead of skipping the rule in silence. The Admin API
// refuses the same rows at the door (services/abuseParams.js).

func needWindow(c Common) error {
	if c.WindowMin <= 0 {
		return errors.New("window_min is missing or 0")
	}
	return nil
}

func needBaseline(hours int) error {
	if hours <= 0 {
		return errors.New("baseline_hours is missing or 0")
	}
	return nil
}

// AITParams tunes the destination-spread / SMS-pumping signal.
type AITParams struct {
	Common
	MinMsgs      int     `json:"min_msgs"`
	PrefixLen    int     `json:"prefix_len"`
	UniqWarn     float64 `json:"uniq_ratio_warn"`
	UniqThrottle float64 `json:"uniq_ratio_throttle"`
	UniqSuspend  float64 `json:"uniq_ratio_suspend"`
	PrefixShare  float64 `json:"prefix_share"`
}

// EvalAIT scores one account's window: n messages, uniq distinct destinations,
// topPrefix = count of messages to the single most-hit prefix (PrefixLen digits).
// AIT pumping looks like a burst of near-unique destinations concentrated in one
// number range — both conditions must hold.
//
// A level whose threshold is 0 or missing is off, as in every other signal:
// without that guard a rule saved without uniq_ratio_suspend suspended an
// account that repeated 20 numbers (ratio 0.33 >= 0). And with no min_msgs or
// no prefix_share there is nothing to judge by, so the answer is None.
func EvalAIT(n, uniq, topPrefix int, p AITParams) Severity {
	if p.MinMsgs <= 0 || p.PrefixShare <= 0 {
		return None
	}
	if n < p.MinMsgs || n == 0 {
		return None
	}
	ratio := float64(uniq) / float64(n)
	share := float64(topPrefix) / float64(n)
	if share < p.PrefixShare {
		return None // spread across ranges — normal marketing traffic shape
	}
	switch {
	case p.UniqSuspend > 0 && ratio >= p.UniqSuspend:
		return Suspend
	case p.UniqThrottle > 0 && ratio >= p.UniqThrottle:
		return Throttle
	case p.UniqWarn > 0 && ratio >= p.UniqWarn:
		return Warn
	}
	return None
}

// Validate names the first key this rule cannot run without.
func (p AITParams) Validate() error {
	if err := needWindow(p.Common); err != nil {
		return err
	}
	switch {
	case p.MinMsgs <= 0:
		return errors.New("min_msgs is missing or 0")
	case p.PrefixLen <= 0:
		return errors.New("prefix_len is missing or 0")
	case p.PrefixShare <= 0:
		return errors.New("prefix_share is missing or 0")
	}
	return nil
}

// VelocityParams tunes the burst-velocity signal (P1 register 1.2.4, ruled to
// P10 2026-07-17, built 2026-09-09).
//
// It compares an account's message COUNT this window against its own recent
// average — the same shape as spend, but counting messages rather than money.
//
// Why it is not redundant with the two signals that already exist. EvalAIT
// requires the traffic to be concentrated in one number range and returns None
// the moment it is spread, by design — that concentration is what makes AIT
// pumping recognisable. EvalSpend notices a spike only through cost, so cheap
// traffic, or an account whose baseline is already generous, can climb a long
// way before it registers. Between them sits the case this catches: a leaked
// API key or a compromised account pushing somebody else's traffic at normal
// prices, spread across networks.
type VelocityParams struct {
	Common
	// BaselineHours is how far back the account's normal rate is measured.
	BaselineHours int `json:"baseline_hours"`
	// MinMsgs is the floor below which a window is too quiet to judge. Without
	// it, an account that normally sends three messages an hour and sends
	// thirty would look like a tenfold burst.
	MinMsgs int `json:"min_msgs"`
	// MinBaseline is the least history this signal will judge on — not a floor
	// applied to the divisor.
	//
	// Flooring was tried first and is wrong: set high enough to protect a new
	// account it swallows real baselines too, so an established customer
	// normally sending 100 an hour gets measured against the floor instead of
	// against itself, and its genuine fivefold burst scores as two. The
	// question "is this account sending far more than usual" simply has no
	// answer when there is no usual, so an account below this is not scored at
	// all until it has some history.
	MinBaseline  float64 `json:"min_baseline"`
	MultWarn     float64 `json:"mult_warn"`
	MultThrottle float64 `json:"mult_throttle"`
	MultSuspend  float64 `json:"mult_suspend"`
}

// EvalVelocity scores one account's window: n messages sent now against
// baselinePerWindow, its own trailing average for a window of the same length.
func EvalVelocity(n int, baselinePerWindow float64, p VelocityParams) Severity {
	// Too quiet to judge. A tenfold jump from two messages to twenty is noise,
	// not a burst, and firing on it would train an operator to ignore this
	// signal entirely.
	if p.MinMsgs > 0 && n < p.MinMsgs {
		return None
	}
	// Too little history to judge. A brand-new account has a baseline near
	// zero, against which any volume is an infinite multiple — scoring it
	// would suspend a customer for their first campaign.
	if baselinePerWindow < p.MinBaseline || baselinePerWindow <= 0 {
		return None
	}
	mult := float64(n) / baselinePerWindow
	switch {
	case p.MultSuspend > 0 && mult >= p.MultSuspend:
		return Suspend
	case p.MultThrottle > 0 && mult >= p.MultThrottle:
		return Throttle
	case p.MultWarn > 0 && mult >= p.MultWarn:
		return Warn
	}
	return None
}

// Validate names the first key this rule cannot run without.
func (p VelocityParams) Validate() error {
	if err := needWindow(p.Common); err != nil {
		return err
	}
	return needBaseline(p.BaselineHours)
}

// BadNumberParams tunes the invalid-number-ratio signal (P1 register 1.2.4,
// ruled to P10 2026-07-17, built 2026-09-09).
//
// It scores the share of an account's destinations that HLR consulted and
// could not find. A high share is the signature of a scraped or purchased
// list: real customer databases do not contain many numbers that no network
// claims. It is a fraud signal and a route-quality problem at once — the
// carrier sees the same undeliverable traffic and eventually says so.
type BadNumberParams struct {
	Common
	// MinMsgs is the floor below which the ratio is meaningless. Two misses
	// out of three messages is 67% and tells you nothing.
	MinMsgs       int     `json:"min_msgs"`
	RatioWarn     float64 `json:"ratio_warn"`
	RatioThrottle float64 `json:"ratio_throttle"`
	RatioSuspend  float64 `json:"ratio_suspend"`
}

// EvalBadNumbers scores one account's window: misses out of consulted, where
// "consulted" excludes messages HLR never looked at.
//
// The divisor is CONSULTED, not total. An account that supplies the operator
// itself, or whose traffic skips HLR, would otherwise show a low ratio purely
// because most of its messages were never checked — flattering exactly the
// account most able to hide.
func EvalBadNumbers(misses, consulted int, p BadNumberParams) Severity {
	if consulted <= 0 || (p.MinMsgs > 0 && consulted < p.MinMsgs) {
		return None
	}
	ratio := float64(misses) / float64(consulted)
	switch {
	case p.RatioSuspend > 0 && ratio >= p.RatioSuspend:
		return Suspend
	case p.RatioThrottle > 0 && ratio >= p.RatioThrottle:
		return Throttle
	case p.RatioWarn > 0 && ratio >= p.RatioWarn:
		return Warn
	}
	return None
}

// Validate names the first key this rule cannot run without.
func (p BadNumberParams) Validate() error { return needWindow(p.Common) }

// CharsetParams tunes the charset-change signal (P1 register 1.2.4, ruled to
// P10 2026-07-17, built 2026-09-09 on the PO's call).
//
// It notices an account sending in an encoding it has essentially never used —
// a GSM-7-only sender suddenly emitting UCS-2, which is what a compromised
// account pushing somebody else's content looks like.
//
// ITS LIMIT, STATED PLAINLY: the record stores the ENCODING, not the script.
// UCS-2 is Urdu, Arabic, Chinese and emoji alike, so this cannot tell a
// hijacked account from a customer launching a legitimate Urdu campaign. That
// is why the seeded thresholds warn and do not throttle or suspend: a warning
// asks a human to look, which is the most this evidence can honestly support.
// Scoring it properly needs the dominant script recorded per message.
type CharsetParams struct {
	Common
	BaselineHours int `json:"baseline_hours"`
	MinMsgs       int `json:"min_msgs"`
	// MinBaseline is the least history needed to know what is normal for this
	// account — same meaning as VelocityParams.MinBaseline, and for the same
	// reason: a new account has no norm to depart from.
	MinBaseline   int     `json:"min_baseline"`
	ShareWarn     float64 `json:"share_warn"`
	ShareThrottle float64 `json:"share_throttle"`
	ShareSuspend  float64 `json:"share_suspend"`
}

// EvalCharsetChange scores what share of this window used encodings the
// account's baseline barely used. newShare is that share, 0..1.
func EvalCharsetChange(windowMsgs, baselineMsgs int, newShare float64, p CharsetParams) Severity {
	if p.MinMsgs > 0 && windowMsgs < p.MinMsgs {
		return None
	}
	// No history, no norm to depart from — the same rule velocity follows.
	if p.MinBaseline > 0 && baselineMsgs < p.MinBaseline {
		return None
	}
	switch {
	case p.ShareSuspend > 0 && newShare >= p.ShareSuspend:
		return Suspend
	case p.ShareThrottle > 0 && newShare >= p.ShareThrottle:
		return Throttle
	case p.ShareWarn > 0 && newShare >= p.ShareWarn:
		return Warn
	}
	return None
}

// Validate names the first key this rule cannot run without.
func (p CharsetParams) Validate() error {
	if err := needWindow(p.Common); err != nil {
		return err
	}
	return needBaseline(p.BaselineHours)
}

// SpendParams tunes the spend-anomaly signal (uses P9 sell_price totals).
type SpendParams struct {
	Common
	BaselineHours int `json:"baseline_hours"`
	// MinBaseline is an amount in the gateway's BASE currency
	// (YAKU_BASE_CURRENCY), in the rule and in an account's override alike.
	// The monitor turns it into the company's own currency before it calls
	// EvalSpend, because the spend it is compared with is in that currency
	// (QA 2026-10-08, B-03: read as a bare number, the seeded 1.0 was one
	// rupee for a PKR company, and a new account was suspended on its third
	// message).
	MinBaseline  float64 `json:"min_baseline"`
	MultWarn     float64 `json:"mult_warn"`
	MultThrottle float64 `json:"mult_throttle"`
	MultSuspend  float64 `json:"mult_suspend"`
}

// EvalSpend compares the current window's spend to the account's per-window
// baseline (trailing average). MinBaseline floors the divisor so a brand-new
// account isn't flagged by its first message. A level whose multiplier is 0 or
// missing is off: without that guard mult_suspend 0 suspended an account for
// one message of 0.01 (any multiple is >= 0).
func EvalSpend(current, baselinePerWindow float64, p SpendParams) Severity {
	base := baselinePerWindow
	if base < p.MinBaseline {
		base = p.MinBaseline
	}
	if base <= 0 || current <= 0 {
		return None
	}
	mult := current / base
	switch {
	case p.MultSuspend > 0 && mult >= p.MultSuspend:
		return Suspend
	case p.MultThrottle > 0 && mult >= p.MultThrottle:
		return Throttle
	case p.MultWarn > 0 && mult >= p.MultWarn:
		return Warn
	}
	return None
}

// Validate names the first key this rule cannot run without.
func (p SpendParams) Validate() error {
	if err := needWindow(p.Common); err != nil {
		return err
	}
	return needBaseline(p.BaselineHours)
}

// SpamParams tunes the anti-evasion content signal.
type SpamParams struct {
	Common
	// SampleMsgs is how many of ONE account's messages are read per sweep;
	// ScanLimit is the ceiling for the whole sweep. With only the ceiling,
	// one busy account used it up and the accounts after it were never read
	// (QA 2026-10-08, B-09). 0 = 200.
	SampleMsgs   int `json:"sample_msgs"`
	ScanLimit    int `json:"scan_limit"`
	HitsWarn     int `json:"hits_warn"`
	HitsThrottle int `json:"hits_throttle"`
	HitsSuspend  int `json:"hits_suspend"`
}

// EvalSpam scores by the count of window messages whose normalized body hit a
// normalized spam keyword.
func EvalSpam(hits int, p SpamParams) Severity {
	switch {
	case p.HitsSuspend > 0 && hits >= p.HitsSuspend:
		return Suspend
	case p.HitsThrottle > 0 && hits >= p.HitsThrottle:
		return Throttle
	case p.HitsWarn > 0 && hits >= p.HitsWarn:
		return Warn
	}
	return None
}

// Validate names the first key this rule cannot run without.
func (p SpamParams) Validate() error { return needWindow(p.Common) }

// TemplateParams tunes the template-drift signal.
type TemplateParams struct {
	Common
	SampleMsgs    int     `json:"sample_msgs"` // one account's sample per sweep; 0 = 200 (see SpamParams)
	ScanLimit     int     `json:"scan_limit"`
	MinMsgs       int     `json:"min_msgs"`
	RatioWarn     float64 `json:"ratio_warn"`
	RatioThrottle float64 `json:"ratio_throttle"`
	RatioSuspend  float64 `json:"ratio_suspend"`
}

// EvalTemplate scores the share of scanned bodies matching NO approved template
// (only meaningful for accounts that have templates but are not accept-time
// enforced — enforced accounts can't drift past P1). A level whose ratio is 0
// or missing is off, and with no min_msgs nothing is judged: a rule saved
// with only window_min and ratio_warn used to suspend on one message.
func EvalTemplate(mismatched, total int, p TemplateParams) Severity {
	if p.MinMsgs <= 0 {
		return None
	}
	if total < p.MinMsgs || total == 0 {
		return None
	}
	ratio := float64(mismatched) / float64(total)
	switch {
	case p.RatioSuspend > 0 && ratio >= p.RatioSuspend:
		return Suspend
	case p.RatioThrottle > 0 && ratio >= p.RatioThrottle:
		return Throttle
	case p.RatioWarn > 0 && ratio >= p.RatioWarn:
		return Warn
	}
	return None
}

// Validate names the first key this rule cannot run without.
func (p TemplateParams) Validate() error {
	if err := needWindow(p.Common); err != nil {
		return err
	}
	if p.MinMsgs <= 0 {
		return errors.New("min_msgs is missing or 0")
	}
	return nil
}
