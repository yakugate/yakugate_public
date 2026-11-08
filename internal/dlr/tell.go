package dlr

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
)

// Answers of the gateway's own told to a client (Tell, TellOnce), and
// "told once": what a message's client has been told is kept.

// Teller is an optional Store capability: record what the client was told
// about a message, and read it back (record_index.told_*, hot/0044).
//
// SetTold reports whether THIS caller is the one that decided. The vendor
// role, a receipt arriving on another instance and the no-receipt sweep can
// all reach for the same message at once, and only one of them may send the
// client a final answer.
type Teller interface {
	SetTold(ctx context.Context, ref model.RecordRef, st model.Status, errCode, by string) (bool, error)
	ToldStatus(ctx context.Context, ref model.RecordRef) (string, error)
}

// teller is where "told once" is kept without the receipt facts in Redis: the
// store, if it can. With the facts it is the facts themselves (Facts.Told, and
// their GETDEL): MarkTold and TellOnce.
func (s *Service) teller() (Teller, bool) {
	t, ok := s.store.(Teller)
	return t, ok
}

// MarkTold records what a message's client is being told in the vendor's
// place when a vendor role decides it at submit (the account's on_accepted or
// on_refused rule): a receipt the vendor sends later is then recorded and not
// pushed. false = somebody told this client already, or nothing can keep the
// mark. A progress word marks nothing.
//
// With the receipt facts in Redis the mark rides the facts: the vendor role
// stamps the answer on the message (Message.ToldStatus) before it files them
// (Facts.Told), and nothing else can have told the client at that moment.
func (s *Service) MarkTold(ctx context.Context, m model.Message, o receiptpolicy.Outcome) bool {
	if s == nil || !o.Final() {
		return false
	}
	if s.HasFacts() {
		return true
	}
	t, ok := s.teller()
	if !ok {
		return false
	}
	mine, err := t.SetTold(ctx, m.Ref(), o.Status, o.Err, o.Rule)
	if err != nil {
		log.Printf("[dlr] receipt policy: could not mark what %s is told (%v)", m.ID, err)
		return false
	}
	return mine
}

// Told names one message whose client is being given an answer the gateway
// decided itself: everything a receipt needs, without the model.Message,
// because the sweep and the receipt path work from record rows.
type Told struct {
	MsgID     string
	AccountID int64
	// CompanyID is the message's customer company: its record is in
	// mt_<company>_<month> (D-136).
	CompanyID int64
	Src, Dst  string
	BindSlot  int
	// The client's own registered_delivery preference still applies: an
	// invented receipt is a receipt, and a client that asked for none gets
	// none (the record is still stamped, so nothing waits for ever).
	NoReceipt, FailureReceiptOnly bool
	// Owner is the message's queue owner, whose deployment pushes the
	// receipt (D-121). "" = read it from the record (ownerOf).
	Owner string
	// Bill is what the message's receipts may book (billing.go); "" = read
	// it from the record.
	Bill string
	// VendorID is the vendor that sent it (0 = none did), and VendorMsgID
	// the vendor's id for it: with the receipt facts in Redis, TellOnce
	// closes the message by them.
	VendorID    int64
	VendorMsgID string
}

// TellFor builds a Told from a message the vendor roles still hold.
func TellFor(m model.Message) Told {
	return Told{MsgID: m.ID, AccountID: m.AccountID, CompanyID: m.CompanyID, Src: m.Sender, Dst: m.Destination,
		BindSlot: m.BindSlot, NoReceipt: m.NoReceipt, FailureReceiptOnly: m.FailureReceiptOnly,
		Owner: m.QueueOwner, Bill: noteOfMessage(m), VendorID: m.VendorID}
}

// ref is the record of the message a Told names.
func (t Told) ref() model.RecordRef { return model.RecordRef{MsgID: t.MsgID, CompanyID: t.CompanyID} }

// Tell sends the client an answer of ours and records it as a receipt of its
// own (a dlrlog row whose raw is `policy:<rule>`), so support can see what the
// customer was told and billing can charge for the receipt the customer
// actually got. The caller has already claimed the message (its record insert
// carried told_status, or TellOnce did it).
//
// Best effort, like every other push site: the record is already correct, and
// a failure to deliver a receipt is the push retry policy's problem, not the
// caller's.
//
// The receipt carries the policy's own word, one of the eight (2026-10-12):
// REJECTD, DELETED and UNKNOWN reach the client as written, where until then
// every `failed` answer went out as UNDELIV. A PROGRESS word (ACCEPTD,
// ENROUTE — a refusal an account answers that way) is sent like the account's
// progress receipt: to every client unless the account takes final receipts
// only, and with nothing claimed, because it closes nothing.
func (s *Service) Tell(ctx context.Context, t Told, o receiptpolicy.Outcome) {
	if s == nil || o.Status == "" {
		return
	}
	stat := Stat(o.Stat)
	if stat == "" {
		stat = StatFor(o.Status)
	}
	// Whether the client is sent it: the client's own preference for a final
	// answer, the account's levels for a progress word.
	push := s.pusher != nil
	if push && o.Final() {
		push = model.WantsTerminalReceipt(t.NoReceipt, t.FailureReceiptOnly, o.Status)
	} else if push {
		push = !SuppressOwnSubmit(s.levelsFor(t.AccountID))
	}
	// What it books (billing.go): a final answer is the client's first final
	// (the caller claimed the message), a progress word a notice.
	var bs []billing.Booking
	var charge string
	if push && s.biller != nil {
		note := t.Bill
		if note == "" {
			note = s.recordNote(ctx, t.ref())
		}
		if o.Final() {
			bs, charge = s.finalBill(note, t.MsgID, t.CompanyID, t.AccountID, t.VendorID, o.Status, true, false)
		} else {
			bs, charge = s.noticeBill(note, t.MsgID, t.CompanyID, t.AccountID, t.VendorID)
		}
	}
	// The receipt log first: it is the evidence, and it must exist whether or
	// not the client's own preference lets the push happen.
	e := entry{Direction: "smsc", Status: string(o.Status), Stat: string(stat), ErrorCode: o.Err,
		Raw: "policy:" + o.Rule, At: time.Now().UTC().Format(time.RFC3339), Charge: charge}
	e.SellRate, e.BuyRate = billing.BookedRates(bs)
	b, _ := json.Marshal(e)
	if err := s.appendRaw(ctx, t.ref(), string(b)); err != nil {
		log.Printf("[dlr] receipt policy: %s told %s (%s) but the receipt row was not written: %v",
			t.MsgID, o.Status, o.Rule, err)
	}
	// What the client was told goes to the file log (issue 14: "the client
	// is told part be stored into file logs, not on the hot path"); the
	// `policy:` row above is the receipt itself.
	if o.Final() {
		logging.New("dlr").Info(logging.SystemScope, "receipt_told",
			"the account's receipt policy answered for the vendor",
			logging.Fields{"messageId": t.MsgID, "accountId": t.AccountID, "rule": o.Rule,
				"status": string(o.Status), "stat": string(stat), "errorCode": o.Err, "sent": push})
	}
	if c, on := logging.ActiveForAccountID(t.AccountID); on {
		logging.CaptureTo(c, logging.New("dlr"), "receipt", "receipt from the account's own policy", logging.Fields{
			"messageId": t.MsgID, "rule": o.Rule, "status": string(o.Status), "stat": string(stat),
			"errorCode": o.Err, "delaySec": int(o.Delay.Seconds()),
		})
	}
	if !push {
		return
	}
	owner := t.Owner
	if owner == "" {
		owner = s.ownerOf(ctx, t.ref())
	}
	if err := s.pusher.PushToClient(ctx, t.AccountID, Receipt{
		MsgID: t.MsgID, Status: o.Status, Stat: stat, ErrCode: o.Err,
		Src: t.Src, Dst: t.Dst, BindSlot: t.BindSlot, Delay: o.Delay, Owner: owner,
	}); err != nil {
		log.Printf("[dlr] receipt policy: %s told %s but the receipt was not queued: %v", t.MsgID, o.Status, err)
		return
	}
	s.book(ctx, t.CompanyID, bs)
}

// TellOnce claims the message first and tells the client only if this caller
// is the one that decided. false = somebody else already gave this client its
// final answer, and nothing was sent.
func (s *Service) TellOnce(ctx context.Context, t Told, o receiptpolicy.Outcome) (bool, error) {
	if s == nil || o.Status == "" || !o.Final() {
		// A progress word closes nothing, so there is nothing to claim; the
		// rules that reach here (no_receipt) cannot hold one anyway.
		return false, nil
	}
	if s.HasFacts() {
		// The answer closes the message for its client: its facts are taken
		// (GETDEL), so the vendor's own receipt, if it ever comes, finds
		// nothing and is filed as an orphan. Not there = closed already.
		mine, err := s.closeForTell(ctx, t)
		if err != nil || !mine {
			return false, err
		}
		s.Tell(ctx, t, o)
		return true, nil
	}
	teller, ok := s.teller()
	if !ok {
		// A store that cannot record what the client was told cannot promise
		// to say it once, and two final receipts are worse than none.
		return false, nil
	}
	mine, err := teller.SetTold(ctx, t.ref(), o.Status, o.Err, o.Rule)
	if err != nil || !mine {
		return false, err
	}
	s.Tell(ctx, t, o)
	return true, nil
}

// closeForTell takes the facts of the message t names, so that this caller,
// and only it, gives the client its final answer. A message sent as several
// PDUs is closed on its part counter (its parts' keys stay, and their
// receipts are recorded only). Facts of another message under the same
// vendor id (the id handed out again) are put back.
func (s *Service) closeForTell(ctx context.Context, t Told) (bool, error) {
	if t.VendorMsgID == "" || t.VendorID == 0 {
		return false, nil
	}
	scope := s.scope(t.VendorID)
	fs, err := s.facts.Take(ctx, scope, t.VendorMsgID, t.Dst)
	if err != nil {
		return false, err
	}
	var mine bool
	var other []Facts
	for _, f := range fs {
		if f.MsgID != t.MsgID {
			other = append(other, f)
			continue
		}
		mine = true
		if f.Parts > 1 {
			if pm, ok := s.facts.(PartMarker); ok {
				// Any part failed closes it: the counter's first close is ours.
				_, ready, _, perr := pm.MarkPart(ctx, f.MsgID, f.Parts, model.StatusFailed, f.Wait)
				if perr != nil {
					return false, perr
				}
				mine = ready
			}
		}
	}
	if len(other) > 0 {
		if err := s.facts.PutBack(ctx, scope, t.VendorMsgID, other); err != nil {
			noteFastTrouble("taken receipt facts could not be put back (%v)", err)
		}
	}
	return mine, nil
}
