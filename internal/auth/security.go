package auth

// Security events — Process 13 Type 2 (the security/alert log).
//
// This package once reported only a THIRD-rung ladder trip, as a plain
// sentence on stdout; an ordinary wrong password produced no record anywhere.
// Now every refusal at the edge is an event and the process decides where it
// is written. The old sentence was removed on 2026-09-05 (PO) once it was
// purely a duplicate of the event beside it — and a duplicate stored at info
// while the event itself is at error.
//
// The event type itself lives in internal/seclog, not here, because the
// pipeline reports events too and cannot import this package (auth already
// imports pipeline for its Reject type).

import "github.com/yakugate/yakugate/internal/seclog"

// Re-exported so callers in this package read naturally.
const (
	EventAuthFail     = seclog.EventAuthFail
	EventAuthLocked   = seclog.EventAuthLocked
	EventIPThrottled  = seclog.EventIPThrottled
	EventSprayBlocked = seclog.EventSprayBlocked
	EventLadderRung   = seclog.EventLadderRung
)

// SecurityEvent is seclog.Event.
type SecurityEvent = seclog.Event

// safeIdentity redacts a lockout key for logging — see seclog.SafeIdentity.
// The token-path key is the raw JWT, a live credential.
func safeIdentity(id string) string { return seclog.SafeIdentity(id) }

// WithSecurityLog installs the sink every security event is reported to. nil
// (the default) reports nothing — the events are still counted and still
// change the auth outcome, they simply go unrecorded.
func WithSecurityLog(f func(SecurityEvent)) Option {
	return func(a *Authenticator) { a.seclog = f }
}

// report sends one event to the sink, if there is one. Never blocks and never
// returns an error: logging must not be able to change an auth outcome.
func (a *Authenticator) report(e SecurityEvent) {
	if a.seclog == nil {
		return
	}
	e.Identity = safeIdentity(e.Identity)
	a.seclog(e)
}
