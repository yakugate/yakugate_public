package smppserver

import (
	"context"

	"github.com/yakugate/yakugate/internal/dlr"
	"github.com/yakugate/yakugate/internal/handoff"
)

// importHandedOff moves what other gateways parked for this one's clients
// into its own queues (issue 5): receipts, expanded to the part ids this
// gateway saved when it joined the parts (issue 6), and MO. One process of
// the deployment at a time, beside the dlr and mo roles'. With the queues in
// Redis, what an older build queued in this gateway's queue Redis for
// another gateway is parked for it once.
func (p *Process) importHandedOff(ctx context.Context) {
	if p.handoff == nil || p.queue == nil {
		return
	}
	var strays handoff.StrayReceipts
	if s, ok := p.queue.(handoff.StrayReceipts); ok {
		strays = s
	}
	go p.handoff.ImportReceipts(ctx, dlr.NewQueuePusher(p.queue), strays)
	if s, ok := p.queue.(handoff.MOStager); ok {
		var moStrays handoff.StrayMO
		if m, ok := p.queue.(handoff.StrayMO); ok {
			moStrays = m
		}
		go p.handoff.ImportMO(ctx, s, moStrays)
	}
}
