package pipeline

import (
	"context"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/netcode"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/sendplan"
	"github.com/yakugate/yakugate/lib/encoding"
)

// Rerouter chooses a new route for a message waiting in the queue of a vendor
// that went down (PO 2026-09-14, routing rules review: "move immediately if
// there is any next route, else wait and expire").
//
// It asks the route stage itself — the same matching, the same tiers, the
// same per-type choice and loss check a new message gets now — with one
// difference: the vendor the message is leaving is never chosen, whatever its
// health says this second.
type Rerouter struct {
	stage routeStage
	hlr   hlrStage // for hlr_retry: the number's network looked up again
}

// NewRerouter builds from the same dependencies as the submit pipeline.
func NewRerouter(d SubmitDeps) *Rerouter {
	// dryRun: deciding is not traffic. A message that cannot move is looked
	// at again every few seconds; counting each look as a NO_ROUTE, or
	// writing it to a flow capture, would bury the real figures.
	return &Rerouter{stage: routeStage{ref: d.Ref, rr: newRR(), fx: d.FX,
		qualityMinSample: d.QualityMinSample, health: d.VendorHealth, dryRun: true, noWait: true,
		lossAcceptNoRate: lossAcceptsNoRate(), baseCur: BaseCurrency()},
		hlr: newHLRStage(d, true)}
}

// LookUpAgain finds the number's network again for a vendor's hlr_retry code
// (PO 2026-09-15): the account's own HLR order, minus our stored answer and
// the operator code the client sent — the vendor has just said the network
// they gave is wrong — so a ported number is found by the number ranges or a
// live lookup. changed is false when the answer is the same network (or none),
// and then the message fails like a permanent code.
func (r *Rerouter) LookUpAgain(ctx context.Context, m model.Message) (out model.Message, changed bool) {
	out = m
	if r == nil || r.stage.ref == nil {
		return out, false
	}
	acct, found := r.stage.ref.AccountByID(m.AccountID)
	if !found {
		return out, false
	}
	a := *acct
	order := a.HLROrder
	if order == nil {
		order = refdata.DefaultHLROrder
	}
	a.HLROrder = nil
	for _, step := range order {
		if step != refdata.HLRStepData && step != refdata.HLRStepClient {
			a.HLROrder = append(a.HLROrder, step)
		}
	}
	a.HLRDBCheck = true
	a.HLRMissPolicy = "proceed"
	probe := model.Message{Destination: m.Destination}
	r.hlr.Run(ctx, &Envelope{Msg: &probe, Account: &a})
	if probe.MCC == "" || probe.MNC == "" {
		return out, false
	}
	if probe.MCC == m.MCC && netcode.SameMNC(probe.MNC, m.MNC) {
		return out, false
	}
	out.MCC, out.MNC, out.HLRSource, out.HLRPorted = probe.MCC, probe.MNC, probe.HLRSource, probe.HLRPorted
	return out, true
}

// Reroute returns m on the route it would get now, never on the vendor named
// by m.Vendor.
//
// plainBody and plainOrigBody are m.Body and m.OrigBody in the clear (the
// caller opens sealed ones). A rewrite by the old route is undone first, so
// the new route starts from what the client sent. The returned message's Body
// and OrigBody are in the clear; the caller seals them again when the
// company's messages are encrypted.
//
// ok=false means no other route takes the message now — it stays where it is
// and waits — and why says what stopped it.
func (r *Rerouter) Reroute(ctx context.Context, m model.Message, plainBody, plainOrigBody string) (out model.Message, why string, ok bool) {
	return r.reroute(ctx, m, plainBody, plainOrigBody, false)
}

// NextRound puts a message whose round failed at the start of its next one:
// routed afresh from Main, no vendor excluded, and — when no vendor can send
// right now — on its first rule's Main vendor to wait, as a new message is.
func (r *Rerouter) NextRound(ctx context.Context, m model.Message, plainBody, plainOrigBody string) (out model.Message, why string, ok bool) {
	m.TriedVendors = nil
	return r.reroute(ctx, m, plainBody, plainOrigBody, true)
}

// RetryFor is the retries of the rule a message was routed by (the gateway's
// default when the rule is gone).
func (r *Rerouter) RetryFor(m model.Message) sendplan.Retry {
	if r != nil && r.stage.ref != nil {
		if rt := r.stage.ref.RouteByID(m.RouteID); rt != nil && rt.Retry.FirstSec > 0 {
			return rt.Retry
		}
	}
	return sendplan.DefaultRetry
}

func (r *Rerouter) reroute(ctx context.Context, m model.Message, plainBody, plainOrigBody string, fresh bool) (out model.Message, why string, ok bool) {
	if r == nil || r.stage.ref == nil {
		return m, "no routing data", false
	}
	acct, found := r.stage.ref.AccountByID(m.AccountID)
	if !found {
		return m, "account not found", false
	}
	from := m.Vendor
	body := plainBody
	if m.OrigSender != "" {
		m.Sender = m.OrigSender
	}
	if m.OrigDestination != "" {
		m.Destination = m.OrigDestination
	}
	if plainOrigBody != "" {
		body = plainOrigBody
		if m.ConcatTotal <= 1 {
			if enc, segs, err := encoding.Plan(body, m.Gsm7); err == nil {
				m.Encoding, m.Segments = enc, segs
			}
		}
	}
	m.Body = body
	m.OrigSender, m.OrigDestination, m.OrigBody = "", "", ""

	st := r.stage
	if fresh {
		st.noWait = false
	} else {
		st.leaving = from
	}
	e := &Envelope{Msg: &m, Account: acct}
	if rej := st.Run(ctx, e); rej != nil {
		if rej.Reason == "NO_ROUTE" {
			return m, "no other route with a working vendor", false
		}
		return m, rej.Reason, false
	}
	if !fresh && (m.Vendor == "" || m.Vendor == from) {
		return m, "no other route with a working vendor", false
	}
	return m, "", true
}
