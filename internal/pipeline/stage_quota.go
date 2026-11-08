package pipeline

import (
	"context"
	"strconv"
	"time"

	"github.com/yakugate/yakugate/internal/refdata"
)

// --- stage 14/18: quota (company daily/monthly + per-country daily/monthly) ---
// [SCOPE P1-13, rebuilt under chunk-6 rulings 6.1/6.2, 2026-08-04]
//
// Before this rebuild the stage read ONLY the DB-stored daily_used/
// monthly_used JSON — which nothing ever incremented, so no quota could ever
// trip on live traffic — and the company-level quota (companies.quota) was
// loaded but never enforced at all. Now: exhaustion = DB BASE counters (an
// operator-set starting point, e.g. an import) + LIVE usage from the
// UsageCounter, checked here and incremented by the enqueue stage only on
// durable accept. Units are SEGMENTS (D-095; consistent with rate-limit and
// billing). Period-stamped keys make day/month rollover automatic.
//
// Quotas count the parts the VENDOR receives (PO 2026-09-21), the same count
// the client is billed — known only once the route stage has chosen the
// vendor. So the stage runs twice:
//   - early, before HLR (precheck): refuses when the quota cannot take even
//     ONE part, so a customer at the limit never costs a live HLR dip;
//   - after the route stage: the real check, with the vendor's parts, and the
//     one that records the keys the enqueue stage then counts usage on.
type quotaStage struct {
	ref      *refdata.Mem
	usage    UsageCounter     // nil = base-only check (pre-6.1 behaviour)
	now      func() time.Time // injectable for tests
	precheck bool             // the early one-part check: no keys recorded
}

func (s quotaStage) Name() string {
	if s.precheck {
		return "quota-precheck"
	}
	return "quota"
}

func (s quotaStage) Run(_ context.Context, e *Envelope) *Reject {
	n := e.Msg.Segments
	if n < 1 || s.precheck {
		n = 1
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	// Period stamps are UTC by deliberate choice: quota windows must roll at
	// one consistent instant regardless of which tenant's timezone a message
	// belongs to (quiet-hours is the per-tenant-local-time feature; quotas
	// are accounting windows).
	now := nowFn().UTC()
	day, month := now.Format("20060102"), now.Format("200601")

	// Company quota (chunk-6 ruling 6.2 — was loaded, never enforced).
	if e.Account.CompanyID != 0 {
		q := e.Account.CompanyQuota
		if q.DailyQuota != nil || q.MonthlyQuota != nil {
			scope := "co:" + strconv.FormatInt(e.Account.CompanyID, 10)
			keyD, keyM := "qd:"+scope+":"+day, "qm:"+scope+":"+month
			if w := s.quotaExceeded(q, keyD, keyM, n); w != windowNone {
				return rejectAfter(429, "QUOTA_EXHAUSTED: company "+string(w)+" quota", retryAfterSec(w, now))
			}
			if !s.precheck {
				e.QuotaKeys = append(e.QuotaKeys, keyD, keyM)
			}
		}
	}

	// The company's own per-country quota (companies.country_quota, QA
	// API-020): one bucket per company and prefix, shared by all its
	// accounts. Applies beside the country rules below, not instead of them.
	if cc, q, ok := e.Account.CompanyCountryQuotaFor(e.Msg.Destination); ok {
		scope := "co:" + strconv.FormatInt(e.Account.CompanyID, 10) + "|cc:" + cc
		keyD, keyM := "qd:"+scope+":"+day, "qm:"+scope+":"+month
		if w := s.quotaExceeded(q, keyD, keyM, n); w != windowNone {
			return rejectAfter(429, "QUOTA_EXHAUSTED: company destination country "+string(w)+" quota", retryAfterSec(w, now))
		}
		if !s.precheck {
			e.QuotaKeys = append(e.QuotaKeys, keyD, keyM)
		}
	}

	// Country quota — resolved by the one ownership-absolute rule lookup; an
	// account-owned rule counts in its own bucket (chunk-6 ruling 6.4), a
	// global rule in the country-wide shared bucket.
	if cc, q, owned, ok := s.ref.CountryQuotaRule(e.Account.ID, e.Msg.Destination); ok &&
		(q.DailyQuota != nil || q.MonthlyQuota != nil) {
		scope := "cc:" + cc
		if owned {
			scope += "|acct:" + strconv.FormatInt(e.Account.ID, 10)
		}
		keyD, keyM := "qd:"+scope+":"+day, "qm:"+scope+":"+month
		if w := s.quotaExceeded(q, keyD, keyM, n); w != windowNone {
			return rejectAfter(429, "QUOTA_EXHAUSTED: destination country "+string(w)+" quota", retryAfterSec(w, now))
		}
		if !s.precheck {
			e.QuotaKeys = append(e.QuotaKeys, keyD, keyM)
		}
	}
	return nil
}

// quotaWindow names the accounting window a quota check broke on. It exists so
// the reject can carry an honest Retry-After: the two windows roll at
// different instants, so "when can I send again" has no single answer.
type quotaWindow string

const (
	windowNone    quotaWindow = ""
	windowDaily   quotaWindow = "daily"
	windowMonthly quotaWindow = "monthly"
)

// quotaExceeded reports WHICH window admitting n more segments would breach:
// DB base + live count + n > limit. With no UsageCounter wired the live term is
// 0 (base-only, the old behaviour).
//
// Monthly is reported ahead of daily when both are full, because it is the one
// that actually governs — waiting for midnight does not help a caller whose
// month is spent. [PO 2026-09-03]
func (s quotaStage) quotaExceeded(q refdata.Quota, keyD, keyM string, n int) quotaWindow {
	live := func(key string) int {
		if s.usage == nil {
			return 0
		}
		return s.usage.Get(key)
	}
	if q.MonthlyQuota != nil && q.MonthlyUsed+live(keyM)+n > *q.MonthlyQuota {
		return windowMonthly
	}
	if q.DailyQuota != nil && q.DailyUsed+live(keyD)+n > *q.DailyQuota {
		return windowDaily
	}
	return windowNone
}

// retryAfterSec is the whole seconds until w's window rolls over, counted from
// now. Periods are stamped UTC (see Run), so the boundaries are next UTC
// midnight and 00:00 UTC on the 1st. Always at least 1 — a caller told to
// retry in 0 seconds would simply retry into the same refusal.
func retryAfterSec(w quotaWindow, now time.Time) int {
	var next time.Time
	switch w {
	case windowDaily:
		next = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	case windowMonthly:
		next = time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	default:
		return 0
	}
	// Round UP: the window has not rolled until the boundary has passed, so
	// truncating would send the caller back a fraction of a second early.
	secs := int((next.Sub(now) + time.Second - 1) / time.Second)
	if secs < 1 {
		secs = 1
	}
	return secs
}
