package seclog

// The security event itself — kept in this package rather than in
// internal/auth because BOTH auth and the pipeline report events, and the
// pipeline cannot import auth (auth already imports pipeline for its Reject
// type). This package imports neither, so both can use it.

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

// Event types. Kept short and stable: these become the `type` field of a log
// record and, later, the thing a SIEM rule matches on.
const (
	EventAuthFail     = "auth_fail"     // wrong password, bad token, unknown user
	EventAuthLocked   = "auth_locked"   // identity ladder tripped; this request was refused
	EventIPThrottled  = "ip_throttled"  // per-IP ladder tripped, pre-auth
	EventSprayBlocked = "spray_blocked" // one IP trying many distinct identities
	EventLadderRung   = "ladder_rung"   // a ladder climbed to its third rung — look at this
	EventIPNotAllowed = "ip_not_allowed"
	// EventUnblocked is the odd one out: every other event here is a refusal,
	// this one is a human CANCELLING a refusal (POST /admin/unblock). It
	// belongs in the same stream anyway — the operator or SIEM reading a
	// lockout needs to see that someone lifted it, right beside the lockout
	// itself, and the account's own file should show it too (PO 2026-09-05).
	EventUnblocked = "unblocked"
	// EventHoneypotHit — an account tried to send to a trap number. Nobody is
	// on the other end of a trap, so this says the sender's recipient list was
	// scraped, bought or stolen, and the operator or regulator watching that
	// number just saw it happen. Serious, and OPERATOR-ONLY (see
	// OperatorOnly): telling the customer would let them find our traps.
	EventHoneypotHit = "honeypot_hit"
	// EventRetryStorm — the retry role parked a message whose terminal record
	// kept failing to write, or wrote off an unusual number of messages in
	// one sweep (PO 2026-09-17). Not a client's doing, so operator-only.
	EventRetryStorm = "retry_storm"
)

// Event is one security-relevant refusal at an edge.
//
// A struct rather than a formatted sentence: a sink can put the fields in a
// log record, ship them to a SIEM, or count them — none of which is possible
// once the facts have been baked into a string.
type Event struct {
	Type string
	// Account is the username when it is known. Empty when the caller never
	// resolved to an account (unknown user, malformed token) — such an event
	// has no tenant to file it under and belongs only to the system log.
	Account string
	// Identity is what was TRIED, safe to write down: "u:<username>" for the
	// password path, "t:<short hash>" for a token. The raw token is never
	// here — see SafeIdentity.
	Identity   string
	IP         string
	Reason     string
	Rung       int
	RetryAfter time.Duration
}

// Serious reports whether an event is an attack signal rather than an
// everyday mistake. A wrong password is warn-level noise; a tripped ladder or
// a spray block means someone should look.
func (e Event) Serious() bool {
	switch e.Type {
	case EventAuthLocked, EventIPThrottled, EventSprayBlocked, EventLadderRung,
		EventHoneypotHit, EventRetryStorm:
		return true
	}
	return false
}

// OperatorOnly reports whether an event must stay OUT of the account's own log
// file and appear only in the operator's system stream.
//
// Every other security event is something the customer should see about
// themselves — their own failed logins, their own lockouts. A trap hit is the
// opposite: the whole value of a trap is that the sender does not know it is
// one. Write "you sent to a trap number" into their log and they can work out
// which of their numbers are traps, strip them, and carry on with the rest of
// a list we now know is stolen. So this one is ours alone.
func (e Event) OperatorOnly() bool {
	return e.Type == EventHoneypotHit || e.Type == EventRetryStorm
}

// SafeIdentity turns a lockout key into something that can be written to a
// file on disk.
//
// The lockout key for the token path is "t:" + the raw JWT (internal/auth),
// because the ladder must count attempts against the exact credential
// presented. That value is a LIVE BEARER CREDENTIAL: written to a log it can
// be replayed by anyone who reads the file, for as long as it is valid.
// Before this, the third-rung alert printed it verbatim to stdout.
//
// A username is not a secret and stays readable — support needs it to answer
// "why can't this customer connect", and a SIEM rule needs it to spot one
// account being hammered. A token is replaced by the first 8 characters of
// its SHA-256: enough to tell two attackers apart across many lines, useless
// to anyone who steals the log (PO ruling 2026-09-04).
func SafeIdentity(id string) string {
	if rest, ok := strings.CutPrefix(id, "t:"); ok {
		if rest == "" {
			return "t:none"
		}
		sum := sha256.Sum256([]byte(rest))
		return "t:" + hex.EncodeToString(sum[:])[:8]
	}
	// "u:<username>" and anything else pass through — no secret in either.
	return id
}
