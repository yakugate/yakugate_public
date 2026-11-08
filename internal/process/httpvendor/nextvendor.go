package httpvendor

import (
	"context"
	"time"

	"github.com/yakugate/yakugate/internal/model"
)

// handTimeout bounds handing one message to the re-route inbox.
const handTimeout = 5 * time.Second

// nextVendor hands a message this vendor could not take now (an answer that
// is not a permanent refusal, a call that failed) to the re-route inbox, so
// the next vendor of its round tries it at once — the rest of its level, then
// the backups — instead of the same vendor after a lease (Saleem 2026-09-30:
// retries are rounds of Main and backups). The routing process decides where
// it goes and when a round is over.
//
// false = the caller tries it again on this vendor after its gap (tryAgain):
// a message sent as one call for several parts, a queue store that cannot
// take it, or a failed hand-off.
func (p *Process) nextVendor(v vendorAdapter, m model.Message, cover []model.Message, code string) bool {
	if len(cover) != 1 || m.ConcatTotal > 1 {
		return false
	}
	enq, ok := p.queue.(interface {
		Enqueue(context.Context, model.Message) error
	})
	if !ok {
		return false
	}
	lg := p.log.ForVendor(v.cid)
	ctx, cancel := context.WithTimeout(context.Background(), handTimeout)
	defer cancel()
	in := m
	in.Vendor = model.RerouteInbox
	in.Status = model.StatusQueued
	in.CodeReroute = model.RerouteAsk(model.ActionNext, code, v.cid)
	if err := enq.Enqueue(ctx, in); err != nil {
		lg.Warnf("send", "message %s: could not hand it to the next vendor of its round: %v (tried again here)", m.ID, err)
		return false
	}
	if err := p.ack(ctx, v.cid, m.ID); err != nil {
		lg.Warnf("send", "message %s: handed to the next vendor of its round but could not be taken off %s's queue: %v — it may be sent twice",
			m.ID, v.cid, err)
	}
	lg.Infof("send", "message %s: %s could not take it now (%s); handed to the next vendor of its round", m.ID, v.cid, code)
	return true
}
