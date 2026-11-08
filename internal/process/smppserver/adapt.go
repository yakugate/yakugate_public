package smppserver

// submit_sm to the canonical message: address plan, schedule, validity,
// receipts asked for, priority and text.

import (
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// validateAddressPlan checks the destination's TON/NPI against what Yakugate
// can actually route — a destination must be a real MSISDN (never an
// alphanumeric address) on a numbering plan we resolve routes against.
// Reverses the previously documented non-goal ("source_addr passes through
// without TON/NPI interpretation") — PO 2026-07-27: "Npi / ton must be
// implemented." Source TON/NPI are decoded but deliberately NOT gated here:
// an alphanumeric SENDER (TON=5) is normal, and the sender-length check in
// stage_validate.go already classifies numeric-vs-alpha from the actual
// characters, which is more reliable than trusting a wire-declared type many
// real ESMEs set carelessly. Kept deliberately conservative (only the
// combinations that can never be a deliverable MSISDN) so this doesn't reject
// traffic from ESMEs that set NPI=0 "unknown" universally, a very common
// real-world sloppiness. [SCOPE P2-TONNPI]
func validateAddressPlan(b *pdu.SubmitSMBody) bool {
	if b.DestAddrTON == 5 { // Alphanumeric — a destination must be a real number
		return false
	}
	switch b.DestAddrNPI {
	case 0, 1: // Unknown, ISDN/E.164 — the only plans Yakugate routes against
		return true
	default:
		return false
	}
}

// scheduleAt parses submit_sm's schedule_delivery_time into the message's
// NextAttemptAt — the SMPP equivalent of the HTTP API's `sendAt` (both delay
// queue visibility rather than adding a separate scheduling mechanism; see
// adapter.go's toMessages and the store Enqueue changes, [SCOPE
// P1-SCHEDULE]). The wire encoding is IDENTICAL to validity_period's (SMPP
// 3.4 §7.1.1), so this reuses parseValidity rather than duplicating the
// format parser. A past/empty value sends immediately (zero time). [SCOPE
// P2-SCHEDULE, PO 2026-07-27]
func scheduleAt(raw string) time.Time {
	now := time.Now().UTC()
	if t := parseValidity(raw, now); t.After(now) {
		return t
	}
	return time.Time{}
}

// toMessage maps a submit_sm to the canonical Message (the SMPP adapter). [SCOPE P2-ADAPT]
//
// registered_delivery (b.RegisteredDelivery) is honoured in full (PO
// 2026-09-03, landing immediately, no per-account opt-in) via
// receiptDisposition below — this was decoded and thrown away until now, so
// every client got every receipt regardless of what it asked for.
func toMessage(b *pdu.SubmitSMBody, a *refdata.Account) *model.Message {
	// Strip any UDH so the body is real text (not header bytes) and encoding is
	// detected correctly; keep the concat group so egress can re-emit it
	// (UDH IEs or SAR TLVs — both recognized since 2026-07-26). [SCOPE P2-CONCAT]
	ref, total, seq, payload, _ := b.Concat()
	noRcpt, failOnly, wantIntermediate := receiptDisposition(b.RegisteredDelivery)
	return &model.Message{
		ID:            ulid.Make().String(),
		RequestID:     ulid.Make().String(),
		AccountID:     a.ID,
		CompanyID:     a.CompanyID, // its record's table (D-136)
		Username:      a.Username,
		Sender:        b.SourceAddr,
		Destination:   b.DestinationAddr,
		Ingress:       model.TransportSMPP, // client submitted over SMPP (egress transport set later by route)
		Body:          submitText(b, payload, a),
		Channel:       b.ServiceType, // service_type passthrough (2026-07-26)
		Gsm7:          gsm7FromDataCoding(b.DataCoding),
		Priority:      priorityFromFlag(b.PriorityFlag),
		ValidUntil:    parseValidity(b.ValidityPeriod, time.Now().UTC()),
		NextAttemptAt: scheduleAt(b.ScheduleDelivery), // [SCOPE P2-SCHEDULE]
		ConcatRef:     ref,
		ConcatTotal:   total,
		ConcatSeq:     seq,
		// user_message_reference TLV (0x0204) is the SMPP carrier for the client
		// idempotency key — the submit_sm equivalent of the HTTP API's requestId. [SCOPE P2-IDEMP]
		IdempotencyKey:          b.UserMessageRef,
		NoReceipt:               noRcpt,
		FailureReceiptOnly:      failOnly,
		WantIntermediateReceipt: wantIntermediate,
	}
}

// receiptDisposition decodes registered_delivery per SMPP 3.4 §5.2.17.
//
//	bits 0-1 (mask 0x03): SMSC Delivery Receipt
//	  00 (0x00) no receipt requested
//	  01 (0x01) receipt on final outcome, success OR failure
//	  10 (0x02) receipt on failure only
//	  11       reserved — treated as 01 (send everything), the safe side of
//	           an unspecified value: never silently drop delivery truth for a
//	           byte we don't recognize.
//	bit 4 (mask 0x10): intermediate notification requested
//
// Bits 2-3 (SME-originated acknowledgement) are decoded nowhere in this
// codebase — we don't implement SME acks — and are ignored here too.
func receiptDisposition(registeredDelivery byte) (noReceipt, failureOnly, wantIntermediate bool) {
	switch registeredDelivery & 0x03 {
	case 0x00:
		noReceipt = true
	case 0x02:
		failureOnly = true
	}
	wantIntermediate = registeredDelivery&0x10 != 0
	return
}

// parseValidity converts an SMPP validity_period into ValidUntil (2026-07-26 —
// closes the "decoded past, never read" gap). Two forms per SMPP 3.4 §7.1.1:
//
//	relative: "YYMMDDhhmmss000R" — offsets added to now
//	absolute: "YYMMDDhhmmssTNN±" — UTC via the quarter-hour offset; the
//	          sub-second digit and offset are parsed loosely (real ESMEs vary)
//
// Empty or malformed → zero time (the enqueue default TTL applies).
func parseValidity(v string, now time.Time) time.Time {
	if len(v) < 12 {
		return time.Time{}
	}
	num := func(s string) int {
		n := 0
		for _, c := range s {
			if c < '0' || c > '9' {
				return -1
			}
			n = n*10 + int(c-'0')
		}
		return n
	}
	yy, mo, dd := num(v[0:2]), num(v[2:4]), num(v[4:6])
	hh, mi, ss := num(v[6:8]), num(v[8:10]), num(v[10:12])
	if yy < 0 || mo < 0 || dd < 0 || hh < 0 || mi < 0 || ss < 0 {
		return time.Time{}
	}
	if strings.HasSuffix(v, "R") { // relative
		return now.AddDate(yy, mo, dd).Add(
			time.Duration(hh)*time.Hour + time.Duration(mi)*time.Minute + time.Duration(ss)*time.Second)
	}
	if mo < 1 || mo > 12 || dd < 1 || dd > 31 {
		return time.Time{}
	}
	t := time.Date(2000+yy, time.Month(mo), dd, hh, mi, ss, 0, time.UTC)
	// absolute form carries a UTC offset in quarter hours + '+'/'-'
	if len(v) >= 16 {
		if q := num(v[13:15]); q >= 0 {
			off := time.Duration(q) * 15 * time.Minute
			switch v[15] {
			case '+':
				t = t.Add(-off)
			case '-':
				t = t.Add(off)
			}
		}
	}
	return t
}

// priorityFromFlag maps SMPP priority_flag 0-3 -> pipeline priority (PO decision). [SCOPE P2-ADAPT]
func priorityFromFlag(f byte) int {
	switch f {
	case 1:
		return 5 // transactional
	case 2, 3:
		return 1 // OTP/highest
	default:
		return 9 // default/bulk
	}
}

// decodeShortMessage turns submit_sm short_message octets into a UTF-8 Body per
// data_coding. Raw UCS-2 bytes wrapped as a Go string are invalid UTF-8 and fail
// the utf8mb4 enqueue (RMSGQFUL); decode them so SMPP matches the HTTP edge. [SCOPE P2-ADAPT]
func decodeShortMessage(raw []byte, dc byte) string { return pdu.DecodeShortMessage(raw, dc) }

// submitText decodes a submit_sm's text as its data_coding says
// (pdu.ReadText): 0, and the GSM codings with a message class, is the GSM
// 03.38 alphabet (Saleem, 19 Oct 2026: "use smpp defaults, whatever
// datacoding means use that"), 1 ASCII, 3 Latin-1, 8 UCS-2. The account's
// setting (octates_to_septets_unpacking, 2026-09-14) says how its GSM-7 comes:
// packed, 7-bit, or one septet in each octet — never a guess from the octets
// (Saleem, 19 Oct 2026: "the setting separates packing or unpacking"). Packed
// was read only under data_coding 0; a flash message (0xF0) from a packed
// account was read as if unpacked.
//
// A part of a longer message keeps a GSM-7 escape that ends it, as "\x1b": a
// client that splits by octets can end a part with the ESC of '~' and open the
// next with its value, '='. Decoded alone the ESC was dropped and the joined
// message read '=' for '~' (Saleem, 18 Oct 2026); concat.Assemble mends the
// joined text.
func submitText(b *pdu.SubmitSMBody, payload []byte, a *refdata.Account) string {
	_, total, _, _, isPart := b.Concat()
	msg := b.MessageBytes()
	packed := a != nil && a.SMPPPackedGSM7
	return pdu.ReadText(msg, len(msg)-len(payload), b.DataCoding, packed, isPart && total > 1)
}

// gsm7FromDataCoding maps data_coding to the encoding preference the pipeline's
// encoding stage passes to encoding.Plan (Msg.Gsm7 carries the full pref string,
// same field the HTTP adapter fills with "latin1"). [SCOPE P2-ADAPT]
func gsm7FromDataCoding(dc byte) string {
	switch dc {
	case 0x08:
		return "ucs2" // UCS-2 forced (encoding.UCS2)
	case 0x03:
		return "latin1" // ISO-8859-1 (SMPP data_coding 3) — explicit Latin-1 (encoding.Latin1), mirrors the HTTP encoding="latin1" path
	default:
		return "" // auto-detect (0x00 SMSC default / 0x01 IA5 -> GSM-7 for Latin text)
	}
}
