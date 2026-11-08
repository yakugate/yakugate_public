package pipeline

import (
	"context"
	"strconv"

	"github.com/yakugate/yakugate/internal/deployment"
)

// countryRater resolves the destination's per-country segments/sec cap —
// consumer-side interface (satisfied by *refdata.Mem) so tests can stub it.
// owned reports whether the governing rule is the account's OWN (ownership-
// absolute resolution, D-094 addendum): the bucket is then scoped to that
// account (chunk-6 ruling 6.4) instead of shared country-wide.
type countryRater interface {
	CountryRateLimit(accountID int64, msisdn string) (cc string, limit int, owned, ok bool)
}

// --- stage 10/18: rate limit --- [SCOPE P1-03]
// Runs AFTER encoding (PO 2026-07-26) because the unit is SEGMENTS per
// second, and the real segment count only exists once auto-replace has
// rewritten the body and the encoding stage has planned it.
//
// Two buckets, both in segments/second, first breach wins:
//
//  1. Company (shared): every account under one company draws from ONE
//     bucket capped by companies.max_throughput (0 = unlimited). This is the
//     contract ceiling and it spans every deployment.
//  2. This deployment's share of the account (config/0123, PO 2026-09-24:
//     "throughputs are always deployment based"). The account-level
//     mt_throughput column is retired — an account assigned to three
//     gateways is capped on each separately, and the total it was quoted is
//     the sum. An account on the All row has no per-gateway rate and is
//     governed by the company ceiling alone.
//  3. The abuse monitor's penalty (abuse_throttle_tps). NOT configuration —
//     it is what P10's throttle action writes, it applies gateway-wide
//     across every deployment, and only an operator clears it. It had to
//     become its own field when mt_throughput stopped being read, or the
//     throttle would have gone on auditing an action that did nothing.
//  4. Destination country: quota_by_countries.rate_limit (0/absent =
//     unlimited), longest-prefix match, one bucket per matched prefix shared
//     by ALL traffic to that country.
type rateLimitStage struct {
	rl  RateAllower
	ref countryRater
	// globalTPS is the gateway-wide segments/sec ceiling (0 = disabled),
	// checked FIRST — an operator load-shed valve above every tenant bucket
	// (chunk-1 ruling 1.5, PO 2026-07-31). Cluster-wide under the Redis gate
	// (one shared bucket), per-instance in memory mode. [SCOPE P1-03G]
	globalTPS int
	// depID is this process's deployment (config/0122). It selects which of
	// the account's per-deployment rates applies here — and since config/0123
	// that is the ONLY account rate there is.
	depID string
}

// rlRetryAfterSec is the Retry-After every rate-limit refusal carries. Buckets
// are one second wide whatever the cap, so the next second is always the
// earliest a retry can succeed — there is no cleverer number to give.
// [PO 2026-09-03]
const rlRetryAfterSec = 1

func (rateLimitStage) Name() string { return "rate-limit" }

// ctxAllower is a limiter whose wait for its bucket ends with the request
// (ratelimit.Chunked: a message waits for the chunk another one asked for).
type ctxAllower interface {
	AllowContext(ctx context.Context, key string, limit, n int) bool
}

func (s rateLimitStage) allow(ctx context.Context, key string, limit, n int) bool {
	if c, ok := s.rl.(ctxAllower); ok {
		return c.AllowContext(ctx, key, limit, n)
	}
	return s.rl.Allow(key, limit, n)
}
func (s rateLimitStage) Run(ctx context.Context, e *Envelope) *Reject {
	if s.rl == nil {
		return nil // no limiter wired: unlimited (matches nil-able dep convention)
	}
	n := e.Msg.Segments
	if n < 1 {
		n = 1
	}

	// Narrowest bucket first. A bucket counts a message the moment it is
	// asked, whether or not a later bucket then refuses it — so with the
	// widest first, one account held to 10/s by the abuse penalty and still
	// sending 2000/s used up its company's shared allowance (and the
	// gateway's) with messages that were never accepted (2026-10-06).

	// 1. The abuse penalty, gateway-wide. Its own key, so lifting it restores
	// the account without touching what anybody configured.
	if e.Account.AbuseThrottleTPS > 0 {
		if !s.allow(ctx, "throttle:"+strconv.FormatInt(e.Account.ID, 10), e.Account.AbuseThrottleTPS, n) {
			return rejectAfter(429, "RATE_LIMIT_EXCEEDED: account throttled", rlRetryAfterSec)
		}
	}

	// 2. This deployment's share. Keyed by deployment AND account, so each
	// gateway counts its own — which is the whole point of a per-deployment
	// rate, and why the id must be in the key even when Redis is shared.
	if s.depID != "" {
		if cap := e.Account.Deployments.Rate(s.depID, deployment.MT); cap > 0 {
			key := "dep:" + s.depID + "|acct:" + strconv.FormatInt(e.Account.ID, 10)
			if !s.allow(ctx, key, cap, n) {
				return rejectAfter(429, "RATE_LIMIT_EXCEEDED: deployment throughput cap", rlRetryAfterSec)
			}
		}
	}

	// 3. The company's contracted throughput, shared by all its accounts.
	if e.Account.CompanyID != 0 {
		coKey := "co:" + strconv.FormatInt(e.Account.CompanyID, 10)
		if !s.allow(ctx, coKey, e.Account.CompanyThroughput, n) {
			return rejectAfter(429, "RATE_LIMIT_EXCEEDED: company throughput cap", rlRetryAfterSec)
		}
	}

	// 4. The whole gateway.
	if s.globalTPS > 0 && !s.allow(ctx, "global", s.globalTPS, n) {
		return rejectAfter(429, "RATE_LIMIT_EXCEEDED: gateway global cap", rlRetryAfterSec)
	}

	if s.ref != nil {
		if cc, limit, owned, ok := s.ref.CountryRateLimit(e.Account.ID, e.Msg.Destination); ok && limit > 0 {
			// An ACCOUNT-OWNED rule gets its own bucket (chunk-6 ruling 6.4):
			// before this, account A's private "10/sec to 92" cap shared one
			// bucket with every other account's Pakistan traffic — strangers'
			// volume consumed A's allowance and vice versa. A global rule
			// keeps the deliberate country-wide shared bucket.
			key := "cc:" + cc
			if owned {
				key += "|acct:" + strconv.FormatInt(e.Account.ID, 10)
			}
			if !s.allow(ctx, key, limit, n) {
				return rejectAfter(429, "RATE_LIMIT_EXCEEDED: destination country cap", rlRetryAfterSec)
			}
		}
	}
	return nil
}
