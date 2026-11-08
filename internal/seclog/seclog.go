// Package seclog writes the security/alert stream — Process 13 Type 2.
//
// It is the one place that decides how a security event becomes log records,
// so the HTTP edge (P1) and the SMPP edge (P2) cannot drift apart in what they
// record or how serious they call it.
//
// Each event is written TWICE when the account is known (PO ruling
// 2026-09-04): once to the system log, which is the single stream an operator
// or a SIEM reads, and once to that account's own file, so a customer can see
// their own failed logins. The system copy is deliberately not tenant-scoped
// — an account set to level "none" silences its own copy but can never
// silence the security trail an operator depends on.
package seclog

import (
	"time"

	"github.com/yakugate/yakugate/internal/platform/logging"
)

// Sink returns the callback to hand to auth.WithSecurityLog, and to the
// pipeline's SubmitDeps.
func Sink(component string) func(Event) {
	lg := logging.New(component)
	// One damper per sink. A blocked attacker keeps sending, and each refusal
	// would otherwise write its own line — see damper.go.
	d := newDamper()
	return func(e Event) {
		ok, repeated := d.allowBurst(damperKey(e), time.Now(), burstOf(e))
		if !ok {
			return
		}
		f := logging.Fields{}
		if e.Identity != "" {
			f["identity"] = e.Identity
		}
		if e.IP != "" {
			f["ip"] = e.IP
		}
		if e.Reason != "" {
			f["reason"] = e.Reason
		}
		if e.Rung > 0 {
			f["rung"] = e.Rung
		}
		if e.RetryAfter > 0 {
			f["retryAfterSec"] = int(e.RetryAfter.Seconds())
		}

		// A wrong password is an everyday mistake and logs at warn. A tripped
		// ladder or a spray block means an attack is in progress and logs at
		// error, so an account or a system set to "error" still sees it.
		write := lg.Warn
		if e.Serious() {
			write = lg.Error
		}
		msg := message(e)
		if repeated > 0 {
			msg += repeatedNote(repeated)
			f["repeated"] = repeated
		}

		// The system copy carries the account name as a field, since the
		// system scope has no account of its own.
		sysFields := logging.Fields{}
		for k, v := range f {
			sysFields[k] = v
		}
		if e.Account != "" {
			sysFields["account"] = e.Account
		}
		write(logging.SystemScope, e.Type, msg, sysFields)

		// The account's own copy — skipped for an operator-only event, which
		// the customer must never be able to read (see Event.OperatorOnly).
		if e.Account != "" && !e.OperatorOnly() {
			write(logging.AccountScope(e.Account), e.Type, msg, f)
		}
	}
}

// message is the human sentence on the record. Short, and it never repeats a
// field that is already on the line.
func message(e Event) string {
	switch e.Type {
	case EventAuthFail:
		return "authentication failed"
	case EventAuthLocked:
		return "refused: this identity is locked out"
	case EventIPThrottled:
		return "refused: this address is throttled"
	case EventSprayBlocked:
		return "refused: credential-spray pattern from this address"
	case EventLadderRung:
		return "a lockout ladder reached its third rung — someone should look at this"
	case EventIPNotAllowed:
		return "refused: address is not on this account's allow list"
	case EventHoneypotHit:
		return "refused: this account sent to a trap number — nobody is on that number, so its recipient list did not come from people who opted in"
	}
	return e.Reason
}
