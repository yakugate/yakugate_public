package dlr

import (
	"context"
	"encoding/json"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
)

// The receipt a client gets for an outcome the GATEWAY decided (issues 22 and
// 23, Saleem 2026-10-26): a vendor refused the submit, the message's tries
// ran out, or its validity ended before it was sent.
//
// The client's `stat` is always one of the eight SMPP words; our reason codes
// (VALIDITY_EXPIRED, MAX_ATTEMPTS_REACHED, HTTP_503 …) never go in it. `err`
// carries the vendor's code where there is one (the refusal's SMPP_45 or
// HTTP_400, as the record has it), else nothing (000).
//
// What the client is told: the account's receipt policy where it has a rule
// for the case, else the gateway-wide default's, else the standard word:
//
//	a vendor refused the submit      REJECTD   err = the vendor's code
//	out of tries (MAX_ATTEMPTS_REACHED)  UNDELIV   000
//	validity over (VALIDITY_EXPIRED)     EXPIRED   000
//
// The record keeps the truth (expired, failed, its own reason code); a
// policy's answer is written beside it (told_*), and a reject is charged as
// it was, whatever word the client is told.

// RefusalReceipt is the receipt for a vendor's refusal at submit when no rule
// says otherwise: REJECTD (it was UNDELIV until 2026-10-27), with the vendor's
// code. The message is `failed`.
func RefusalReceipt(m model.Message, code string) Receipt {
	r := TerminalReceipt(m, model.StatusFailed, code)
	r.Stat = StatRejectd
	return r
}

// DuplicateErr is the err of the receipt a refused duplicate's client is
// sent: our SMPP command_status for a duplicate (VIdempotencyConf, 0x408),
// the code a refusal after a long message's parts were acked always carried.
const DuplicateErr = "408"

// DuplicateTold is Message.RefusalReceipt for that receipt: what the
// refusal's dlr_ row says the client was told.
const DuplicateTold = string(StatRejectd) + " " + DuplicateErr

// DuplicateReceipts are the receipts for a refused duplicate (Saleem, 28 Oct
// 2026: "reject the duplicate message, put it in rejected and send REJECTD
// dlr if the client asks for a delivery report"): REJECTD err:408 on every id
// its client was given (the message's own, or a long SMPP message's part
// ids), delivered by owner (the gateway the client is connected to). None
// when the client wants no final receipt: the account sends none
// (noReceipts), or the message asked for none.
func DuplicateReceipts(m model.Message, noReceipts bool, ids []string, owner string) []Receipt {
	if noReceipts || !model.WantsTerminalReceipt(m.NoReceipt, m.FailureReceiptOnly, model.StatusFailed) {
		return nil
	}
	if len(ids) == 0 {
		ids = []string{m.ID}
	}
	out := make([]Receipt, 0, len(ids))
	for _, id := range ids {
		r := RefusalReceipt(m, DuplicateErr)
		r.MsgID, r.Owner = id, owner
		out = append(out, r)
	}
	return out
}

// RefuseDuplicate sends a refused duplicate's receipts (DuplicateReceipts)
// through push and, once every one is queued, notes on m what its client was
// told (Message.RefusalReceipt), so the refusal's writer files it as a dlr_
// row beside the rejects_ row. Nothing is noted when the client wants no
// receipt or one could not be queued (the error says why). A receipt is
// free: nothing is booked for a refusal.
func RefuseDuplicate(ctx context.Context, push ClientPusher, m *model.Message, noReceipts bool, ids []string, owner string) error {
	rs := DuplicateReceipts(*m, noReceipts, ids, owner)
	if len(rs) == 0 || push == nil {
		return nil
	}
	for _, r := range rs {
		if err := push.PushToClient(ctx, m.AccountID, r); err != nil {
			return err
		}
	}
	m.RefusalReceipt = DuplicateTold
	return nil
}

// PushRefusal queues RefusalReceipt and books the client's first final, as
// PushTerminalDLR does.
func (s *Service) PushRefusal(ctx context.Context, m model.Message, code string) {
	if s == nil || s.pusher == nil {
		return
	}
	if err := s.pusher.PushToClient(ctx, m.AccountID, RefusalReceipt(m, code)); err != nil {
		return // same stance as PushTerminalDLR: the record is already final
	}
	s.BookFinal(ctx, m, model.StatusFailed)
}

// OwnAnswer is what a client is told about one outcome the gateway decided.
type OwnAnswer struct {
	// Status is the status the word implies (what told_status would hold).
	Status model.Status
	// Stat is the word sent, one of the eight.
	Stat Stat
	// Err is the error code sent ("" = 000).
	Err string
	// Delay holds the receipt back (a policy's delay range).
	Delay time.Duration
	// Told is the policy's decision when a rule answered (the account's, or
	// the default's: its Rule starts with receiptpolicy.DefaultPrefix);
	// nil = the standard word.
	Told *receiptpolicy.Outcome
}

// GatewayAnswer decides what a client is told about an outcome the gateway
// decided, given the account's policy (with the default under it, as
// receiptpolicy.Cache.For gives it; nil = none) and the outcome's code: one
// of the gateway's own (receiptpolicy.OwnCode, either name) or a vendor's
// refusal code.
func GatewayAnswer(pol *receiptpolicy.Policy, code string) OwnAnswer {
	if a, rule := pol.Refused(code); a != nil {
		o := receiptpolicy.Decide(a, rule, nil)
		if o.Status != "" {
			stat := Stat(o.Stat)
			if stat == "" {
				stat = StatFor(o.Status)
			}
			return OwnAnswer{Status: o.Status, Stat: stat, Err: o.Err, Delay: o.Delay, Told: &o}
		}
	}
	return StandardAnswer(code)
}

// StandardAnswer is the word an outcome gets when no rule says otherwise.
func StandardAnswer(code string) OwnAnswer {
	switch receiptpolicy.OwnCode(code) {
	case receiptpolicy.CodeValidityExpired:
		return OwnAnswer{Status: model.StatusExpired, Stat: StatExpired}
	case receiptpolicy.CodeMaxAttempts:
		return OwnAnswer{Status: model.StatusFailed, Stat: StatUndeliv}
	}
	return OwnAnswer{Status: model.StatusFailed, Stat: StatRejectd, Err: code}
}

// Final reports whether the answer closes the message for the client. Only a
// policy's progress word (ACCEPTD, ENROUTE) for a vendor's refusal does not.
func (a OwnAnswer) Final() bool {
	if a.Told != nil {
		return a.Told.Final()
	}
	return true
}

// Stamp writes a policy's final answer onto the message's told_* (the record
// write then files it beside the truth). The standard word and a progress
// word leave the message as it is.
func (a OwnAnswer) Stamp(m *model.Message) {
	if a.Told == nil || !a.Told.Final() {
		return
	}
	m.ToldStatus, m.ToldErr, m.ToldBy = a.Told.Status, a.Told.Err, a.Told.Rule
}

// Wanted reports whether the client is sent the receipt: its own
// registered_delivery for a final answer, the account's levels for a progress
// word (as Service.Tell decides).
func (a OwnAnswer) Wanted(m model.Message, levels string) bool {
	if !a.Final() {
		return !SuppressOwnSubmit(levels)
	}
	return model.WantsTerminalReceipt(m.NoReceipt, m.FailureReceiptOnly, a.Status)
}

// Receipt is the receipt to queue for the message.
func (a OwnAnswer) Receipt(m model.Message) Receipt {
	return Receipt{MsgID: m.ID, Status: a.Status, Stat: a.Stat, ErrCode: a.Err,
		Src: m.Sender, Dst: m.Destination, BindSlot: m.BindSlot, Delay: a.Delay, Owner: m.QueueOwner}
}

// Entry is the receipt row of a policy's answer, as Service.Tell writes one
// (raw `policy:<rule>`): evidence of what the client was told, written beside
// the record. "" for the standard word, which has no row (as before). It
// books nothing: what an outcome of the gateway's costs is decided where it
// always was.
func (a OwnAnswer) Entry() string {
	if a.Told == nil {
		return ""
	}
	b, _ := json.Marshal(entry{Direction: "smsc", Status: string(a.Status), Stat: string(a.Stat),
		ErrorCode: a.Err, Raw: "policy:" + a.Told.Rule, At: time.Now().UTC().Format(time.RFC3339)})
	return string(b)
}
