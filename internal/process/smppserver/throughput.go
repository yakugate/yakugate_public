package smppserver

import (
	"context"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/ratelimit"
)

// Per-account MO and DLR throughput (2026-09-14): an account's
// mo_throughput / dlr_throughput caps how many incoming messages / receipts
// per second are pushed to it ON THIS GATEWAY, on its binds and its webhooks
// together (the mo and dlr roles of the same gateway draw from the same
// buckets, ratelimit.MOKey / DLRKey with the gateway in the key). Units this
// leg holds while its queue is short go back to the bucket at once
// (Allowance.Release), so the webhook leg is not starved by an idle bind
// (4 Nov 2026: tw-2 held yaku_test's MO; the bucket was the account's alone
// and the first gateway to ask each second took all of it). The cap is applied BEFORE a
// claim — only as many items as the account may have this second are taken
// off the queue — so an item over the cap simply waits there. Claiming
// counts an attempt, and a held item must never look like a failed delivery
// (under dlr_channel 'both' the attempt count also decides which leg owns a
// receipt).

// claimMOPaced claims incoming messages for the bound accounts: capped
// accounts one by one, up to what their bucket allows, the rest together as
// before.
func (p *Process) claimMOPaced(ctx context.Context, claims []model.MOClaim, n int) ([]model.Message, error) {
	var free []model.MOClaim
	var jobs []model.Message
	for _, c := range claims {
		limit := p.moLimit(c.AccountID)
		if limit <= 0 || p.grant == nil {
			free = append(free, c)
			continue
		}
		key := ratelimit.MOKey(c.AccountID, p.gateway)
		g := p.allow.Take(p.grant, key, limit, n)
		if g == 0 {
			p.noteHeld(&p.moHeld, "mo_rate_held", c.AccountID, limit)
			continue // over the cap this second; they wait in the queue
		}
		js, err := p.queue.ClaimMO(ctx, []model.MOClaim{c}, g)
		if err != nil {
			p.allow.Back(key, g)
			p.allow.Release(p.grant, key)
			return jobs, err
		}
		p.allow.Back(key, g-len(js))
		if len(js) < g {
			p.allow.Release(p.grant, key) // its queue is short: the units go back to the bucket
		}
		jobs = append(jobs, js...)
	}
	if len(free) > 0 {
		js, err := p.queue.ClaimMO(ctx, free, n)
		if err != nil {
			return jobs, err
		}
		jobs = append(jobs, js...)
	}
	return jobs, nil
}

// claimDLRPaced is claimMOPaced for client receipts.
func (p *Process) claimDLRPaced(ctx context.Context, claims []model.DLRClaim, n int) ([]model.ClientDLR, error) {
	var free []model.DLRClaim
	var jobs []model.ClientDLR
	for _, c := range claims {
		limit := p.dlrLimit(c.AccountID)
		if limit <= 0 || p.grant == nil {
			free = append(free, c)
			continue
		}
		key := ratelimit.DLRKey(c.AccountID, p.gateway)
		g := p.allow.Take(p.grant, key, limit, n)
		if g == 0 {
			p.noteHeld(&p.dlrHeld, "dlr_rate_held", c.AccountID, limit)
			continue
		}
		js, err := p.queue.ClaimClientDLR(ctx, []model.DLRClaim{c}, g)
		if err != nil {
			p.allow.Back(key, g)
			p.allow.Release(p.grant, key)
			return jobs, err
		}
		p.allow.Back(key, g-len(js))
		if len(js) < g {
			p.allow.Release(p.grant, key)
		}
		jobs = append(jobs, js...)
	}
	if len(free) > 0 {
		js, err := p.queue.ClaimClientDLR(ctx, free, n)
		if err != nil {
			return jobs, err
		}
		jobs = append(jobs, js...)
	}
	return jobs, nil
}

// moLimit / dlrLimit are THIS deployment's share of the account's incoming
// rate (config/0123, PO 2026-09-24: throughput is always per deployment).
//
// The account-level mo_throughput/dlr_throughput columns are retired: an
// account assigned to three gateways is capped on each of them separately,
// and the total it was quoted is the sum. An account on the All row carries
// no per-gateway rate, so it is uncapped here and governed only by its
// company's contracted throughput.
func (p *Process) moLimit(id int64) int { return p.depRate(id, deployment.MO) }

func (p *Process) dlrLimit(id int64) int { return p.depRate(id, deployment.DLR) }

func (p *Process) depRate(id int64, k deployment.Kind) int {
	if p.ref == nil {
		return 0
	}
	a, ok := p.ref.AccountByID(id)
	if !ok {
		return 0
	}
	return a.Deployments.Rate(p.gateway, k)
}

// noteHeld counts a check where the account's rate gave nothing, and logs it
// at most once a minute an account (rate_held in the status).
func (p *Process) noteHeld(h *ratelimit.Holds, typ string, acct int64, limit int) {
	if h.Note(acct) {
		holdLog.Info(logging.SystemScope, typ, "held by its rate on this gateway: waits in its queue for the next second",
			logging.Fields{"account": acct, "gateway": p.gateway, "limit": limit})
	}
}

var holdLog = logging.New("smpp-server")
