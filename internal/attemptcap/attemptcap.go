// Package attemptcap gives a vendor role's queue store the most times a
// message may be sent, so a claim passes over one that has had its tries.
//
// The retry role writes a message off once it has been claimed
// YAKU_MAX_ATTEMPTS times (a vendor's own max_attempts wins) — but it can
// only take a message that is DUE, and a bound vendor claims a due message
// within 150 ms. So while its vendor was up, a message the vendor kept
// throttling or never answered was not written off at all: it was sent again
// at 30, 60, 120 … 1800 s until its validity ended (load test E6,
// 2026-10-06 — a vendor set to 2 tries was given 4 and counting). With the
// cap known to the claim, such a message is left due and unclaimed, and the
// retry role takes it on its next sweep.
package attemptcap

import (
	"context"
	"os"
	"strconv"

	"github.com/yakugate/yakugate/internal/dal"
)

// DefaultMax is the send tries per message when YAKU_MAX_ATTEMPTS is not set
// — the retry role's own default.
const DefaultMax = 5

// Setter is a queue store whose Claim can pass over a message already claimed
// its maximum number of times.
type Setter interface {
	SetAttemptCap(global int, perVendor map[string]int)
}

// Global is YAKU_MAX_ATTEMPTS, read as the retry role reads it.
func Global() int {
	if n, err := strconv.Atoi(os.Getenv("YAKU_MAX_ATTEMPTS")); err == nil && n > 0 {
		return n
	}
	return DefaultMax
}

// Apply reads the caps — YAKU_MAX_ATTEMPTS and each vendor's max_attempts
// (cfg: the config store's vendors, nil = none) — and gives them to the queue
// store. A store that cannot take them, or a config store that cannot be
// read, leaves everything as it was: the cap is then the gateway-wide one
// from the last good read, or none.
func Apply(ctx context.Context, cfg dal.VendorSendSettings, queue any, logf func(string, ...any)) {
	st, ok := queue.(Setter)
	if !ok {
		return
	}
	perVendor := map[string]int{}
	if cfg != nil {
		got, err := cfg.MaxAttempts(ctx)
		if err != nil {
			if logf != nil {
				logf("per-vendor max_attempts not read (%v) — the gateway-wide %d applies to every vendor", err, Global())
			}
		} else {
			perVendor = got
		}
	}
	st.SetAttemptCap(Global(), perVendor)
}
