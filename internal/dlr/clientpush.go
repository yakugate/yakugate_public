package dlr

import (
	"context"
	"log"
	"time"

	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
)

// Receipts the service itself queues for a client — submit notices, terminal
// states, many at once — and which deployment's queue a receipt goes to.

// addresses returns a message's sender and destination for its client
// receipt, or empty strings when the store cannot say (the push then falls
// back to "0"). A failed read never fails the receipt.
// QueueOwnerLookup says whose queue a sent message came from (D-121).
type QueueOwnerLookup interface {
	MessageQueueOwner(ctx context.Context, ref model.RecordRef) (string, error)
}

// ownerOf is the deployment a receipt for msgID is queued for: the original
// message's queue owner, read from its record. A receipt can arrive on any
// gateway the vendor is placed on, but the client is connected where the
// message was accepted, and only that deployment pushes it. "" (shared) when
// this gateway names no deployment — no record of such an install carries an
// owner, so it skips the read — or when the record cannot say: then any
// gateway delivers it, as before, rather than none.
func (s *Service) ownerOf(ctx context.Context, ref model.RecordRef) string {
	if deployment.Owner() == "" {
		return ""
	}
	ol, ok := s.store.(QueueOwnerLookup)
	if !ok {
		return ""
	}
	o, err := ol.MessageQueueOwner(ctx, ref)
	if err != nil {
		return ""
	}
	return o
}

func (s *Service) addresses(ctx context.Context, ref model.RecordRef) (string, string) {
	al, ok := s.store.(AddressLookup)
	if !ok {
		return "", ""
	}
	src, dst, err := al.MessageAddresses(ctx, ref)
	if err != nil {
		return "", ""
	}
	return src, dst
}

// PushSubmitDLR sends the intermediate submit-to-network notification (status
// 'sent' / SMPP ACCEPTD) for one client-visible message — called by the egress
// processes the moment the vendor accepts (PO ruling 2026-08-10: per message
// ULID, so a multipart group yields one per part). Best effort; never blocks
// the egress path on client-delivery problems.
func (s *Service) PushSubmitDLR(ctx context.Context, m model.Message) {
	if s.pusher == nil {
		return
	}
	if err := s.pusher.PushToClient(ctx, m.AccountID, NoticeReceipt(m, StatAccptd, 0)); err != nil {
		// logged by callers' stores if it matters; the terminal DLR still follows
		_ = err
		return
	}
	// The gateway's own ACCEPTD is a progress receipt the client was sent: its
	// fee under all_levels (billing.go). The account's progress receipt in
	// its place (PushNotice with a word or a hold-back) is not billed.
	s.BookNotice(ctx, m)
}

// PushNotice sends the notice SubmitNotice decided: the gateway's own ACCEPTD
// (exactly PushSubmitDLR), or an account's progress receipt with its word and
// hold-back. A progress receipt is an intermediate: it changes nothing on the
// message and is not billed.
func (s *Service) PushNotice(ctx context.Context, m model.Message, st Stat, delay time.Duration) {
	if st == StatAccptd && delay == 0 {
		s.PushSubmitDLR(ctx, m)
		return
	}
	if s.pusher == nil {
		return
	}
	_ = s.pusher.PushToClient(ctx, m.AccountID, NoticeReceipt(m, st, delay))
}

// NoticeReceipt is the receipt a submit notice puts in the client's queue:
// the gateway's own ACCEPTD (no word of its own, no hold-back — exactly what
// PushSubmitDLR has always queued), or an account's progress receipt with its
// word and hold-back. One builder, so a notice pushed alone and one pushed
// in a batch (PushMany) can never differ.
func NoticeReceipt(m model.Message, st Stat, delay time.Duration) Receipt {
	r := Receipt{MsgID: m.ID, Status: model.StatusSent, Src: m.Sender, Dst: m.Destination,
		BindSlot: m.BindSlot, Owner: m.QueueOwner}
	if st == StatAccptd && delay == 0 {
		return r
	}
	r.Stat, r.Delay = st, delay
	return r
}

// TerminalReceipt is the receipt PushTerminalDLR queues for a final state the
// gateway decided itself.
func TerminalReceipt(m model.Message, st model.Status, errCode string) Receipt {
	return Receipt{MsgID: m.ID, Status: st, ErrCode: errCode, Src: m.Sender, Dst: m.Destination,
		BindSlot: m.BindSlot, Owner: m.QueueOwner}
}

// PushMany queues many client receipts in a few round trips for all of them,
// in the order given — for a role that has many at once (the HTTP vendor
// role after a bulk call: one notice or one failure receipt a message,
// 2026-10-12). With a pusher that cannot take a batch they go one by one.
// Best effort, like every other push site.
func (s *Service) PushMany(ctx context.Context, pushes []Push) {
	if s == nil || s.pusher == nil || len(pushes) == 0 {
		return
	}
	if b, ok := s.pusher.(interface {
		PushBatch(ctx context.Context, pushes []Push) error
	}); ok {
		if err := b.PushBatch(ctx, pushes); err != nil {
			log.Printf("[dlr] %d client receipt(s) were not queued: %v", len(pushes), err)
		}
		return
	}
	for _, x := range pushes {
		_ = s.pusher.PushToClient(ctx, x.AccountID, x.Receipt)
	}
}

// PushTerminalDLR notifies the client of a locally-finalized terminal state:
// the receipt is synthesized here and travels the same channel as every other
// receipt (client_dlr -> P2 bind or P5 dlr_url), its word derived from the
// status. errCode is the vendor's or the gateway's reason, "" = none. A
// vendor's refusal at submit uses PushRefusal (REJECTD, own.go) since
// 2026-10-27.
func (s *Service) PushTerminalDLR(ctx context.Context, m model.Message, st model.Status, errCode string) {
	if s.pusher == nil {
		return
	}
	if err := s.pusher.PushToClient(ctx, m.AccountID, TerminalReceipt(m, st, errCode)); err != nil {
		_ = err // same stance as PushSubmitDLR: the record is already terminal
		return
	}
	// The client's first final, decided by us (a refusal, a write-off): its
	// receipt fee when the message was charged (billing.go).
	s.BookFinal(ctx, m, st)
}
