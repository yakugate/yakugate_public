package smppserver

// What the two delivery legs (receipts and incoming messages out to bound
// clients) share: their claims for the bound accounts, and their stores.

import (
	"sync/atomic"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/logging"
)

// bindDLRClaims turns the currently-bound accounts into the attempt windows
// this leg owns (P5-B03). An account set to `url` is dropped — its receipts
// belong to the HTTP poster even while it holds a live bind — and an account
// set to `both` is bounded, so once the bind has used its budget the row falls
// to the poster instead of the two legs racing for it.
func (p *Process) bindDLRClaims(accts []int64) []model.DLRClaim {
	out := make([]model.DLRClaim, 0, len(accts))
	for _, id := range accts {
		c := model.DLRClaim{AccountID: id}
		if a, ok := p.ref.AccountByID(id); ok {
			switch a.DLRChannel {
			case "url":
				continue // not this leg's traffic at all
			case "both":
				// 0 would mean "unbounded" — an account configured for both
				// with no bind budget is really a `url` account, so treat it
				// as one rather than silently giving the bind every attempt.
				if a.DLRBindAttempts <= 0 {
					continue
				}
				c.MaxAttempts = a.DLRBindAttempts
			}
			// "bind" (and an empty value from a pre-0077 row) stays unbounded.
		}
		out = append(out, c)
	}
	return out
}

// bindMOClaims is bindDLRClaims for incoming messages, read from the
// account's mo_channel / mo_bind_attempts (config/0146): an account set to
// `url` is dropped — its MO belongs to the webhook even while it holds a live
// bind — and one set to `both` is bounded, so the webhook takes over once the
// bind has used its budget.
func (p *Process) bindMOClaims(accts []int64) []model.MOClaim {
	out := make([]model.MOClaim, 0, len(accts))
	for _, id := range accts {
		c := model.MOClaim{AccountID: id}
		if a, ok := p.ref.AccountByID(id); ok {
			switch a.MOChannel {
			case "url":
				continue // the webhook's traffic, not this leg's
			case "both":
				// 0 would mean "unbounded"; with no bind budget the webhook
				// owns it, as for receipts.
				if a.MOBindAttempts <= 0 {
					continue
				}
				c.MaxAttempts = a.MOBindAttempts
			}
			// "bind" (and an empty value) stays unbounded.
		}
		out = append(out, c)
	}
	return out
}

// claimFailed says in the role's log that a claim for a client failed — the
// leg tries again at its next check — at most every claimSayEvery for each
// leg. Until 28 Oct 2026 nothing was said, and a leg that could not claim
// looked like a client with nothing waiting.
func (p *Process) claimFailed(log *logging.Logger, last *atomic.Int64, what string, acct int64, err error) {
	now := time.Now().UnixNano()
	if prev := last.Load(); now-prev < int64(claimSayEvery) || !last.CompareAndSwap(prev, now) {
		return
	}
	log.Warn(logging.SystemScope, "claim_failed", what+" for a client's binds could not be claimed: tried again at the next check",
		logging.Fields{"account": acct, "error": err.Error()})
}

var claimSayEvery = 10 * time.Second

// initFreed makes the channels a bind's pump wakes the legs on.
func (p *Process) initFreed() {
	p.freedOnce.Do(func() {
		p.dlrFreed = make(chan struct{}, 1)
		p.moFreed = make(chan struct{}, 1)
		// The receipts and the MO number their deliver_sm in bands of their
		// own (seqDLR, seqMO), as they did before the pumps (28 Oct 2026):
		// both counters had started at 1, so a receipt and an MO out on one
		// bind at once could carry the same number. The bind keeps one
		// waiter a number, the second overwrote the first, and that one
		// timed out and was sent again when its lease ended — an MO handed
		// to its client twice (DAL plan, step 3, the rig's mix without
		// Redis, 30 Oct 2026: 1,801 MO handed for 1,799 taken in).
		p.dlrSeq.band(seqDLR)
		p.moSeq.band(seqMO)
	})
}

// rebuildStores retries the store handles that failed at construction —
// [SCOPE P2-SELFHEAL] (2026-07-26): one transient DB outage at boot must not
// permanently disable the delivery legs for the life of the process.
func (p *Process) rebuildStores() {
	p.storeMu.Lock()
	defer p.storeMu.Unlock()
	if p.queue == nil {
		if q, _, err := dal.OpenQueues(p.cfg); err == nil {
			p.queue = q
			logging.New(p.Name()).Info(logging.SystemScope, "store_recovered", "queue store recovered", logging.Fields{"store": "queue"})
		}
	}
	if p.hot == nil {
		if h, err := dal.OpenRecordStore(p.cfg); err == nil {
			p.hot = h
			logging.New(p.Name()).Info(logging.SystemScope, "store_recovered", "hot store recovered", logging.Fields{"store": "hot"})
		}
	}
}
