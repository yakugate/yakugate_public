package dlr

import (
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/receiptpolicy"
)

// The eight SMPP 3.4 delivery states (Appendix B, `stat:`) are the receipt
// vocabulary (PO 2026-09-23: "accepted itself is a dlr").
//
// Until now a receipt was collapsed straight into one of the gateway's five
// message statuses, so four of the eight had nowhere to land: ENROUTE and
// ACCEPTD were not in the vocabulary at all and were dropped with a log line,
// DELETED and REJECTD arrived as plain `failed`, and a client never saw the
// difference between a message the network is still carrying and one it gave
// up on.
//
// Two vocabularies now, on purpose:
//
//	Stat           what the RECEIPT says — the vendor's own word, all eight,
//	               carried to the client unchanged and rendered into the
//	               `stat:` field and the message_state TLV
//	model.Status   what the MESSAGE is — the five states everything else in
//	               the gateway runs on (retry, expiry, routing, quality,
//	               billing, the summaries, every report)
//
// Keeping them apart is what makes this safe to add: a new receipt word
// changes what a customer is told without changing a single decision the
// gateway makes about the message.
type Stat string

// The eight, exactly as SMPP 3.4 spells them.
const (
	StatEnroute Stat = "ENROUTE" // the network has it and is still trying
	StatDelivrd Stat = "DELIVRD" // delivered to the handset
	StatExpired Stat = "EXPIRED" // validity period ran out
	StatDeleted Stat = "DELETED" // cancelled at the SMSC
	StatUndeliv Stat = "UNDELIV" // undeliverable
	StatAccptd  Stat = "ACCEPTD" // accepted — manually read, or in place of delivery
	StatUnknown Stat = "UNKNOWN" // the SMSC does not know
	StatRejectd Stat = "REJECTD" // rejected
)

// allStats is the set, for validation and for the portal's choices.
var allStats = []Stat{StatEnroute, StatDelivrd, StatExpired, StatDeleted,
	StatUndeliv, StatAccptd, StatUnknown, StatRejectd}

// Stats returns the eight states, in SMPP's own order.
func Stats() []Stat { return append([]Stat(nil), allStats...) }

// synonyms are spellings vendors use for the same state. The map on a vendor
// handles anything stranger; these are the ones common enough that every
// deployment would otherwise write the same three lines.
var synonyms = map[string]Stat{
	"DELIVERED":     StatDelivrd,
	"DELIVRD":       StatDelivrd,
	"FAILED":        StatUndeliv,
	"UNDELIVERABLE": StatUndeliv,
	"UNDELIV":       StatUndeliv,
	"ACCEPTED":      StatAccptd,
	"ACCEPTD":       StatAccptd,
	"REJECTED":      StatRejectd,
	"REJECTD":       StatRejectd,
	"DELETED":       StatDeleted,
	"CANCELLED":     StatDeleted,
	"CANCELED":      StatDeleted,
	"EXPIRED":       StatExpired,
	"ENROUTE":       StatEnroute,
	"EN_ROUTE":      StatEnroute,
	"UNKNOWN":       StatUnknown,
	// The gateway's own five words: a vendor map written against the old
	// vocabulary ("delivered", "failed", "sent") keeps working unchanged.
	"SENT":   StatAccptd,
	"QUEUED": StatEnroute,
}

// ParseStat turns a vendor's raw status into one of the eight.
//
// The vendor's own map wins (adapter_config.status.map for an HTTP vendor,
// dlr_config.status_map for an SMPP one), so a vendor answering "2" or "DLVD"
// is read correctly; then the common spellings; then nothing.
//
// "" means the word means nothing to us. The caller RECORDS such a receipt
// and leaves the message alone — the one thing it must never do is guess,
// because guessing here is what marks a live message failed.
func ParseStat(raw string, m map[string]string) Stat {
	key := strings.ToUpper(strings.TrimSpace(raw))
	if key == "" {
		return ""
	}
	if m != nil {
		// Looked up on the raw text first (a map is written in the vendor's
		// own spelling, spaces and case included), then upper-cased.
		v, ok := m[strings.TrimSpace(raw)]
		if !ok {
			v, ok = m[key]
		}
		if ok {
			if s := normalise(v); s != "" {
				return s
			}
		}
	}
	return normalise(key)
}

// normalise reads one of the eight from a word, or "".
func normalise(v string) Stat {
	key := strings.ToUpper(strings.TrimSpace(v))
	for _, s := range allStats {
		if key == string(s) {
			return s
		}
	}
	if s, ok := synonyms[key]; ok {
		return s
	}
	return ""
}

// Final reports whether this state ends the message. ENROUTE and ACCEPTD do
// not: the network is still working on it, and a message must keep waiting
// for its real outcome (and keep its place in retry and the no-receipt sweep).
func (s Stat) Final() bool {
	switch s {
	case StatEnroute, StatAccptd:
		return false
	case "":
		return false
	}
	return true
}

// Record is the message status this receipt implies — the five the gateway
// runs on. An intermediate implies `sent`, which is what the message already
// is, so nothing moves.
func (s Stat) Record() model.Status {
	switch s {
	case StatDelivrd:
		return model.StatusDelivered
	case StatExpired:
		return model.StatusExpired
	case StatEnroute, StatAccptd:
		return model.StatusSent
	case StatUndeliv, StatDeleted, StatRejectd, StatUnknown:
		// All four are "it is not going to arrive". They are kept apart on
		// the RECEIPT, where the difference is the customer's to act on; the
		// message itself is simply failed, as it has always been.
		return model.StatusFailed
	}
	return ""
}

// State is the SMPP message_state TLV (0x0427) value for this word.
func (s Stat) State() byte {
	switch s {
	case StatEnroute:
		return 1
	case StatDelivrd:
		return 2
	case StatExpired:
		return 3
	case StatDeleted:
		return 4
	case StatUndeliv:
		return 5
	case StatAccptd:
		return 6
	case StatRejectd:
		return 8
	}
	return 7 // UNKNOWN
}

// StatFor is the word for a status the GATEWAY decided itself (its own
// submit-time ACCEPTD, an expiry, a refusal after the parts were acked). It
// is the old StatToken, kept as the fallback for a receipt with no word of
// its own — a row written before receipts carried one.
func StatFor(st model.Status) Stat {
	switch st {
	case model.StatusDelivered:
		return StatDelivrd
	case model.StatusFailed:
		return StatUndeliv
	case model.StatusExpired:
		return StatExpired
	case model.StatusSent:
		return StatAccptd
	case model.StatusRejected:
		return StatRejectd
	case model.StatusQueued:
		return StatEnroute
	}
	return StatUnknown
}

// Levels is an account's choice of which receipts reach its clients
// (sms_service_config.dlr_levels, config/0120). FINAL receipts always go,
// subject to the client's own registered_delivery exactly as before; this
// governs the intermediate ones — ENROUTE and ACCEPTD.
const (
	// LevelsRegistered follows what the client asked for: an SMPP client gets
	// intermediates when it set registered_delivery bit 4, which is what that
	// bit means, and an HTTP client — which has no such bit, and has always
	// been sent the gateway's submit notice — gets them. The default.
	LevelsRegistered = "registered"
	// LevelsAll forwards every level to every client of this account,
	// whatever their registered_delivery says.
	LevelsAll = "all"
	// LevelsFinalsOnly sends nothing intermediate at all — not the vendor's
	// progress notices, not the gateway's own submit one.
	LevelsFinalsOnly = "finals_only"
	// LevelsNone sends this account's clients no receipt at all (config/0142,
	// 2026-10-06): nothing in between, and no final one either. The final
	// half is not decided here — every message of such an account is accepted
	// as if the client asked for no receipt (Message.NoReceipt, set in the
	// enqueue stage from refdata.Account.NoReceipts), so the one shared
	// decision, model.WantsTerminalReceipt, already says no.
	LevelsNone = "none"
)

// LevelChoices lists the settings, for the Admin API, the portal and the
// console.
func LevelChoices() []string {
	return []string{LevelsRegistered, LevelsAll, LevelsFinalsOnly, LevelsNone}
}

// SuppressOwnSubmit reports whether the gateway's OWN submit notice (the
// ACCEPTD it sends the moment a vendor takes a message) must be held back for
// this account. Only `finals_only` does that; `registered` and `all` keep it,
// still gated by the client's registered_delivery at the call site.
func SuppressOwnSubmit(levels string) bool { return levels == LevelsFinalsOnly || levels == LevelsNone }

// SubmitNotice decides the notice a client gets when the vendor takes its
// message (2026-09-29): the account's progress rule if it has one (ACCEPTD or
// ENROUTE, sent to every client — the operator asked for it), else the
// gateway's own ACCEPTD for a client that wants progress notices and a
// vendor that does not send its own. "Final receipts only" sends neither.
// ok=false = no notice.
func SubmitNotice(levels string, pol *receiptpolicy.Policy, wantedByClient, vendorSendsOwn bool) (stat Stat, delay time.Duration, ok bool) {
	if SuppressOwnSubmit(levels) {
		return "", 0, false
	}
	if st, d, has := pol.ProgressOnAccept(); has {
		return Stat(st), d, true
	}
	if !wantedByClient || vendorSendsOwn {
		return "", 0, false
	}
	return StatAccptd, 0, true
}

// ForwardIntermediate reports whether an intermediate receipt may go to this
// client: the account's setting, and what the client itself asked for.
//
// wantedByClient is registered_delivery bit 4 for a message submitted over
// SMPP, and true for one submitted over HTTP (no such bit exists there, and
// those clients have always been sent the submit notice).
func ForwardIntermediate(levels string, wantedByClient bool) bool {
	switch levels {
	case LevelsAll:
		return true
	case LevelsFinalsOnly, LevelsNone:
		return false
	default: // registered, and every account that has never set it
		return wantedByClient
	}
}
