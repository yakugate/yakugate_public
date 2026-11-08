package model

import "strings"

// RerouteInbox is the queue a vendor role hands a message to when the
// vendor's status code asks for it to go another way (failover, hlr_retry,
// pause_vendor — PO 2026-09-15). No vendor claims it: the mover in the
// routing process does, routes the message again and moves it on.
const RerouteInbox = "_reroute"

// Status-code actions that send a message to the re-route inbox.
const (
	ActionFailover    = "failover"
	ActionHLRRetry    = "hlr_retry"
	ActionPauseVendor = "pause_vendor"
	// ActionNext is a vendor's "not now" (a retry code, a failed call): the
	// message goes to the next vendor of its round — the rest of its level,
	// then the backups — and when the round is over, back to wait for the
	// next one (Saleem 2026-09-30).
	ActionNext = "next"
)

// RerouteAsk is what goes in Message.CodeReroute: the action, the vendor's
// code and the vendor that answered with it.
func RerouteAsk(action, code, vendorCID string) string {
	return action + "|" + code + "|" + vendorCID
}

// ParseRerouteAsk splits a CodeReroute value. Missing parts come back empty.
func ParseRerouteAsk(s string) (action, code, vendorCID string) {
	p := strings.SplitN(s, "|", 3)
	for len(p) < 3 {
		p = append(p, "")
	}
	return p[0], p[1], p[2]
}
