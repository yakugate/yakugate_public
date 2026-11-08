// Package pushretry is the retry policy for CLIENT-facing deliveries — the
// delivery receipts P5 posts to a customer's dlr_url and the inbound messages
// P6 posts to their mo_url.
//
// Before this, both queues carried `attempts` and `next_attempt_at` columns
// that no policy ever read: every claim set a flat NOW()+60s and incremented
// the count, for ever. An endpoint that was down for good was retried about
// once a minute indefinitely, and nothing ever recorded that a customer had
// lost anything. (P5-B02, P6-B1.)
//
// CONFIGURABLE, NOT HARDCODED (PO ruling 2026-08-31). Every knob has a global
// default from the environment and a per-endpoint override read from the
// `http_endpoints.retry` JSON column — a column that already existed and was
// read by nothing, which is half of P5-B08/P6-B15 closed on the way past.
//
//	{"max_attempts":30,"base_delay_sec":60,"multiplier":2,
//	 "max_delay_sec":3600,"give_up_after_sec":86400}
//
// Any subset may be given; the rest fall back to the global default. 0 on
// either give-up dimension disables THAT dimension, so a policy can be
// "unlimited attempts but stop after an hour", or the reverse. Both disabled
// is retry-for-ever — allowed, because a deployment may genuinely want it, but
// it is then an explicit choice rather than an accident.
package pushretry

import (
	"encoding/json"
	"math"
	"os"
	"strconv"
	"time"
)

// Policy is a fully resolved retry policy: defaults with any per-endpoint
// override already applied.
type Policy struct {
	MaxAttempts int           // 0 = no attempt limit
	BaseDelay   time.Duration // wait before attempt 2
	Multiplier  float64       // growth per attempt (1 = flat)
	MaxDelay    time.Duration // ceiling on the wait
	GiveUpAfter time.Duration // 0 = no time limit
}

// Defaults reads the global policy from the environment, falling back to
// values chosen to ride out an ordinary customer outage (a deploy, an
// overnight failure) without retrying a dead endpoint for ever.
func Defaults() Policy {
	return Policy{
		MaxAttempts: envInt("YAKU_PUSH_MAX_ATTEMPTS", 30),
		BaseDelay:   time.Duration(envInt("YAKU_PUSH_BASE_DELAY_SEC", 60)) * time.Second,
		Multiplier:  envFloat("YAKU_PUSH_MULTIPLIER", 2),
		MaxDelay:    time.Duration(envInt("YAKU_PUSH_MAX_DELAY_SEC", 3600)) * time.Second,
		GiveUpAfter: time.Duration(envInt("YAKU_PUSH_GIVE_UP_AFTER_SEC", 86400)) * time.Second,
	}
}

// override is the per-endpoint http_endpoints.retry document. Pointers so an
// absent key is distinguishable from an explicit 0 — "0 attempts" and "not
// specified" mean very different things.
type override struct {
	MaxAttempts    *int     `json:"max_attempts"`
	BaseDelaySec   *int     `json:"base_delay_sec"`
	Multiplier     *float64 `json:"multiplier"`
	MaxDelaySec    *int     `json:"max_delay_sec"`
	GiveUpAfterSec *int     `json:"give_up_after_sec"`
}

// Resolve applies one endpoint's stored retry JSON on top of a base policy.
// Unreadable JSON leaves the base untouched — a malformed override must not
// silently become "retry for ever" or "give up at once".
func Resolve(base Policy, retryJSON string) Policy {
	if retryJSON == "" || retryJSON == "null" {
		return base
	}
	var o override
	if json.Unmarshal([]byte(retryJSON), &o) != nil {
		return base
	}
	p := base
	if o.MaxAttempts != nil && *o.MaxAttempts >= 0 {
		p.MaxAttempts = *o.MaxAttempts
	}
	if o.BaseDelaySec != nil && *o.BaseDelaySec > 0 {
		p.BaseDelay = time.Duration(*o.BaseDelaySec) * time.Second
	}
	if o.Multiplier != nil && *o.Multiplier >= 1 {
		p.Multiplier = *o.Multiplier
	}
	if o.MaxDelaySec != nil && *o.MaxDelaySec > 0 {
		p.MaxDelay = time.Duration(*o.MaxDelaySec) * time.Second
	}
	if o.GiveUpAfterSec != nil && *o.GiveUpAfterSec >= 0 {
		p.GiveUpAfter = time.Duration(*o.GiveUpAfterSec) * time.Second
	}
	return p
}

// Delay is how long to wait before the attempt AFTER the given attempt count.
// attempts is how many have already been made (1 after the first).
func (p Policy) Delay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	base := p.BaseDelay
	if base <= 0 {
		base = time.Minute
	}
	mult := p.Multiplier
	if mult < 1 {
		mult = 1
	}
	// pow, not repeated multiplication: a stuck row with a large attempt count
	// must not spin, and float overflow is clamped below anyway.
	d := float64(base) * math.Pow(mult, float64(attempts-1))
	if p.MaxDelay > 0 && (d > float64(p.MaxDelay) || math.IsInf(d, 1)) {
		return p.MaxDelay
	}
	return time.Duration(d)
}

// GiveUp reports whether this job has exhausted the policy, and why — the
// reason is logged, so an operator can tell "too many tries" from "too old"
// without reconstructing the arithmetic.
func (p Policy) GiveUp(attempts int, firstTry time.Time, now time.Time) (bool, string) {
	if p.MaxAttempts > 0 && attempts >= p.MaxAttempts {
		return true, "reached the attempt limit of " + strconv.Itoa(p.MaxAttempts)
	}
	if p.GiveUpAfter > 0 && !firstTry.IsZero() && now.Sub(firstTry) >= p.GiveUpAfter {
		return true, "still undelivered after " + p.GiveUpAfter.String()
	}
	return false, ""
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 1 {
			return f
		}
	}
	return def
}
