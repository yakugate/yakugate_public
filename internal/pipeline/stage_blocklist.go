package pipeline

import (
	"context"

	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/seclog"
)

// --- stage 4/19: blocklist / DND --- [SCOPE P1-05]
//
// Also carries TRAP-number detection (config/0097, register row 1.2.8). A trap
// is a blocklist row with source='honeypot': a number nobody owns, planted by
// an anti-spam group, a regulator or an operator where list-scrapers will find
// it. It refuses exactly like any other blocked number — what differs is that
// it also raises an operator alarm, because nobody opts in on a number with no
// subscriber, so one hit says the sender's whole list was scraped or bought.
type blockStage struct {
	ref *refdata.Mem
	// seclog receives the trap alarm (Process 13 Type 2). nil = not reported,
	// which is what every test and any un-wired process gets.
	seclog func(seclog.Event)
}

func (blockStage) Name() string { return "blocklist" }
func (s blockStage) Run(_ context.Context, e *Envelope) *Reject {
	if !s.ref.Blocked(e.Msg.Destination, e.Account.ID, e.Account.CompanyID, e.Msg.Sender) {
		return nil
	}
	// Only now — after the number is known to be blocked at all — ask the
	// rarer question. An ordinary DND hit costs one extra map lookup, and the
	// vast majority of blocks are ordinary DND.
	if s.seclog != nil && s.ref.IsTrap(e.Msg.Destination) {
		s.seclog(seclog.Event{
			Type:    seclog.EventHoneypotHit,
			Account: e.Account.Username,
			IP:      e.ClientIP,
			// LAST FOUR DIGITS ONLY, and deliberately so.
			//
			// The whole number would tell an operator which trap was hit, and
			// that is worth having when a regulator's complaint names one. But
			// the log redactor masks any run of 7+ digits, so passing the full
			// number simply produced "trap number ************" — useful to
			// nobody. Four digits fall under that threshold, survive intact,
			// and are enough to pick one trap out of a list while revealing
			// almost nothing. The full number is on the rejected-submit record
			// if anyone needs it.
			Reason: "trap number ending " + lastFour(e.Msg.Destination),
		})
	}
	// The SAME refusal either way (PO 2026-09-09). A distinct code for a trap
	// would let anyone holding an account probe the gateway to find every trap,
	// strip them from their list, and carry on sending to the rest of it —
	// which destroys the traps and confirms the remainder is safe to hit.
	//
	// 403, not 400: a policy refusal (server understood, refuses), uniform
	// with the other policy blocks (P1 scope finalize 2026-08-09).
	return reject(403, "DESTINATION_BLOCKED")
}

// lastFour is the tail of a destination, short enough to survive the log
// redactor's 7-digit threshold. A number shorter than four digits is returned
// whole — it was never going to identify anybody.
func lastFour(dest string) string {
	if len(dest) <= 4 {
		return dest
	}
	return dest[len(dest)-4:]
}
