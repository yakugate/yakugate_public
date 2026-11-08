// What goes on the wire: the text in the vendor's encoding, split into
// parts, and the submit_sm for each.

package smppvendor

import (
	"strings"
	"time"
	"unicode/utf16"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/lib/encoding"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// encodeOne writes one part's text in the WIRE encoding (already resolved
// against the vendor's rules, so it fits) and returns it with its data_coding.
// GSM-7 follows the vendor's gsm7_packing: "packed" is packed septets;
// "unpacked" (the default since 19 Oct 2026) and "gsm" are the GSM 03.38
// alphabet with one septet in each octet — data_coding 0 as SMPP defines it (Saleem, 19 Oct
// 2026: "use smpp defaults, whatever datacoding means use that"). "unpacked"
// was the text's own bytes until then: '€' went as E2 82 AC and '£' as C2 A3
// (encoding test enc-47, 48), which no SMSC alphabet reads right, and 160 'é'
// were 320 bytes, more than a short_message holds (enc-114, 115).
func encodeOne(text, wire, packing string, dc byte) ([]byte, byte) {
	switch wire {
	case "ucs2":
		u := utf16.Encode([]rune(text))
		b := make([]byte, 2*len(u))
		for i, r := range u {
			b[2*i], b[2*i+1] = byte(r>>8), byte(r)
		}
		return b, dc
	case "latin1":
		// One byte per character (2026-09-21). Latin-1 used to fall through
		// to GSM-7 below: sent as data_coding 0, split at 160/153 while it
		// was billed at 140/134, and "â"/"ê" — not in GSM-7 — arrived garbled.
		b, _ := pdu.EncodeShortMessage(text, "latin1")
		return b, dc
	}
	if onePerOctet(packing) {
		// The GSM 03.38 alphabet, one septet value in each byte: what most
		// SMSCs read data_coding 0 as. Resolved text is GSM-7, so this
		// cannot fail; the old fail-safe below stays for the same reason.
		if b, err := pdu.GSM7Octets(text); err == nil {
			return b, dc
		}
		return pdu.UnpackedGSM7(text), dc
	}
	packed, err := pdu.PackGSM7(text)
	if err != nil {
		// Cannot happen for resolved text (Resolve sends anything GSM-7 cannot
		// carry as UCS-2); kept as the old fail-safe rather than a drop.
		return pdu.UnpackedGSM7(text), dc
	}
	return packed, dc
}

// thresholds returns the single-segment / per-part-of-a-concat character
// counts for an encoding + multipart mode. TLV (SAR) concatenation carries no
// UDH bytes in the short_message body, so a part can carry the FULL
// single-segment capacity instead of losing 6 bytes (3-4 packed GSM-7
// septets, or 3 UCS-2 chars) to the header — a real efficiency edge TLV has
// over UDH (item 4.1).
func thresholds(enc, multipartMode string) (single, multi int) {
	return encoding.WireSizes(enc, multipartMode)
}

// concatRef hands out 8-bit concatenation reference numbers (wraps at 256).
var concatRef uint32

// udh builds a 6-byte concatenation header: 05 00 03 <ref> <total> <seq>.
func udh(ref byte, total, seq int) []byte {
	return []byte{0x05, 0x00, 0x03, ref, byte(total), byte(seq)}
}

// wireParts turns a queued message into the submit_sm payload(s) to put on
// the wire, plus the data_coding/esm_class to use and, when the vendor uses
// TLV concatenation, the per-part SAR info (nil entries for UDH mode or a
// single-part send). [SCOPE P3 §concat, D-014, P3-HADAL multipart_mode]
//
//   - A message that ARRIVED as a concat part (ConcatTotal>1, e.g. SMPP
//     ingress per-part model) is re-emitted as a single PDU carrying its
//     ORIGINAL group header/SAR, so the handset reassembles the original group.
//   - A whole message longer than one SMS is split here, by character count
//     (thresholds), each part encoded/packed independently. Single-part
//     messages go out unchanged with no header.
func wireParts(m model.Message, v vendorConn) (parts [][]byte, dc byte, esm byte, sars []*pdu.SAR) {
	if m.ConcatTotal > 1 {
		wire, wdc, _ := v.wireRules().Resolve(m.Body, m.Encoding)
		if v.multipartMode == "tlv" {
			body, dcv := encodeOne(m.Body, wire, v.gsm7Packing, wdc)
			return [][]byte{body}, dcv, 0x00, []*pdu.SAR{{Ref: m.ConcatRef, Total: m.ConcatTotal, Seq: m.ConcatSeq}}
		}
		h := udh(byte(m.ConcatRef), m.ConcatTotal, m.ConcatSeq)
		body, dcv := encodeAfter(h, m.Body, wire, v.gsm7Packing, wdc)
		return [][]byte{body}, dcv, 0x40, []*pdu.SAR{nil}
	}

	// Split and write in the encoding the vendor is SENT, not the one the
	// message was planned in: they differ when the vendor's rules say so.
	wire, wdc, _ := v.wireRules().Resolve(m.Body, m.Encoding)
	single, multi := thresholds(wire, v.multipartMode)
	// Every GSM-7 wire form has one septet in at most one octet, so a part of
	// 160 septets (153 after a header) always fits a short_message. The
	// "unpacked" form was the text's own bytes until 19 Oct 2026 and was split
	// by its bytes too (enc-114/115) — a vendor was then sent more parts than
	// the message was priced at; it is the GSM alphabet now (encodeOne).
	if encoding.UnitLen(m.Body, wire) <= single || v.multipartMode == "payload" {
		// One PDU. In payload mode a long text is ONE piece too: longer
		// than 140 bytes, it goes in message_payload (see the send loop).
		body, dcv := encodeOne(m.Body, wire, v.gsm7Packing, wdc)
		return [][]byte{body}, dcv, 0x00, []*pdu.SAR{nil}
	}
	// Split by WIRE COST, not rune count (chunk-4 ruling 4.1, 2026-08-03):
	// slicing 67/153 runes per part ignored that an astral rune is TWO UTF-16
	// units and a GSM-7 extension char TWO septets, so emoji/€-heavy parts
	// blew past the 140-byte short_message capacity on the wire. Same unit
	// arithmetic the ingress segment planner uses (encoding.UnitCost).
	return splitParts(splitByCost(m.Body, wire, multi), wire, v, wdc)
}

// splitParts writes the chunks of a long message as its parts: SAR fields, or
// a concatenation header in front of each.
func splitParts(chunks []string, wire string, v vendorConn, wdc byte) (parts [][]byte, dc byte, esm byte, sars []*pdu.SAR) {
	if len(chunks) > 255 {
		chunks = chunks[:255] // 8-bit total field; matches the old cap's truncation
	}
	total := len(chunks)
	concatRef++
	ref := byte(concatRef)
	for i, chunk := range chunks {
		if v.multipartMode == "tlv" {
			body, dcv := encodeOne(chunk, wire, v.gsm7Packing, wdc)
			dc = dcv
			parts = append(parts, body)
			sars = append(sars, &pdu.SAR{Ref: int(ref), Total: total, Seq: i + 1})
		} else {
			body, dcv := encodeAfter(udh(ref, total, i+1), chunk, wire, v.gsm7Packing, wdc)
			dc = dcv
			parts = append(parts, body)
			sars = append(sars, nil)
			esm = 0x40
		}
	}
	return parts, dc, esm, sars
}

// gsm7PackingOf reads a vendor's protocol_config.gsm7_packing. GSM-7 is
// carried packed (7-bit, eight septets in seven octets) or unpacked (8-bit,
// one septet in each octet): the one setting says which, both ways — what the
// vendor is sent and how its MO is read. Unpacked is the default (Saleem, 19
// Oct 2026: "set default as unpacked, for both vendor and client sides"; it was
// packed, item 4.2): only "packed" packs. "gsm" (2026-10-12) is the older name
// of "unpacked".
func gsm7PackingOf(setting string) string {
	if strings.ToLower(strings.TrimSpace(setting)) == "packed" {
		return "packed"
	}
	return "unpacked"
}

// onePerOctet reports a gsm7_packing whose GSM-7 is carried unpacked, one
// septet in each octet: everything but "packed" — "unpacked", "gsm"
// (2026-10-12, its older name) and no setting at all, since unpacked is the
// default (Saleem, 19 Oct 2026).
func onePerOctet(packing string) bool { return packing != "packed" }

// encodeAfter writes a part's concatenation header and its text. Packed
// GSM-7 text starts at the first SEPTET boundary after the header, with fill
// bits between (GSM 03.40 §9.2.3.24): a 6-octet header is 7 septets, one fill
// bit, and the text from septet 7. Every other wire form is the header's
// octets and then the text's.
//
// The text used to be packed on its own and put after the header (go-live
// test of 18 Oct 2026, P3 B5): a phone, reading from septet 7 as the standard
// says, showed every part of a long GSM-7 message as garbage — with the
// defaults of then, gsm7_packing packed and multipart_mode udh.
func encodeAfter(h []byte, text, wire, packing string, dc byte) ([]byte, byte) {
	if wire != "ucs2" && wire != "latin1" && !onePerOctet(packing) {
		if b, err := pdu.PackGSM7After(h, text); err == nil {
			return b, dc
		}
	}
	body, dcv := encodeOne(text, wire, packing, dc)
	return append(append([]byte(nil), h...), body...), dcv
}

// submitPDU builds the submit_sm for one wire piece. Only a payload-mode
// vendor gets message_payload, only for a piece longer than one SMS, and
// never for a piece that carries a UDH (a part re-sent in its original
// group keeps its header and esm_class 0x40).
//
// It used to be ANY piece over 140 bytes (QA SMPP-001, 2026-09-29): with
// gsm7_packing "unpacked" a septet is one byte, so a UDH part is 6 + 153 =
// 159 bytes — and that part went out in message_payload with esm_class 0,
// its header shown as text, while the next part carried a real one; the
// handset could not join them. An unpacked part belongs in short_message
// (SMPP 3.4 allows 254 octets there; the SMSC packs the septets), with its
// UDH and esm_class 0x40, as Kannel and Jasmin send it.
func submitPDU(v vendorConn, m model.Message, short []byte, dc, esm byte, sar *pdu.SAR, reg byte, seq uint32, opt pdu.SubmitOptions) []byte {
	if v.multipartMode == "payload" && esm&0x40 == 0 && len(short) > 140 {
		return pdu.SubmitSMPayloadPDU(m.Sender, m.Destination, short, dc, priorityFlag(m.Priority), reg, seq, opt)
	}
	return pdu.SubmitSMPDU(m.Sender, m.Destination, short, dc, esm, priorityFlag(m.Priority), reg, seq, opt, sar)
}

// splitByCost slices text into chunks whose wire cost (encoding.UnitCost
// units) never exceeds limit. Boundaries land between runes by construction,
// so a surrogate pair or an extension-char escape is never torn in half.
//
// Thin wrapper (2026-09-03): the arithmetic moved to encoding.SplitByCost so
// MO delivery (internal/process/smppserver, P6-B6) could reuse it without a
// second implementation to keep in step. Behaviour is byte-for-byte
// unchanged — same loop, same rune-cost rule — this file's own tests
// (group3_test.go etc.) still exercise it via this name.
func splitByCost(text, enc string, limit int) []string {
	return encoding.SplitByCost(text, enc, limit)
}

func priorityFlag(pri int) byte {
	switch {
	case pri <= 1:
		return 2 // highest / OTP
	case pri <= 5:
		return 1 // transactional
	default:
		return 0 // bulk
	}
}

// validityFor is the submit_sm validity_period for one message (2026-09-14).
// Until then it was always empty, so a client's validity never reached the
// vendor, and a vendor could keep retrying for days after the gateway had
// written the message off. Every message carries an expiry (the client's
// validityPeriod, else the account's default TTL, else the gateway's), so it
// is always sent unless the vendor is set to "off". A message with no expiry
// at all (a row from before the expiry was carried) sends none.
func validityFor(mode string, validUntil, now time.Time) string {
	if mode == "off" || validUntil.IsZero() {
		return ""
	}
	if mode == "absolute" {
		return pdu.AbsoluteValidity(validUntil)
	}
	return pdu.RelativeValidity(validUntil.Sub(now))
}

// wireRules is this vendor's encoding rules; a vendorConn built without them
// (tests, older call sites) sends every encoding as itself.
func (v vendorConn) wireRules() encoding.WireRules {
	if v.wire == nil {
		return encoding.DefaultWireRules()
	}
	return v.wire
}

// vendorParts is how many parts the vendor bills a message for. Normally the
// PDUs it was sent; a payload-mode vendor gets ONE PDU for a long message and
// splits it for the handset itself — into standard parts, which is what it
// bills — so those are counted, in the encoding it was sent in.
func vendorParts(m model.Message, v vendorConn, pdus int) int {
	if v.multipartMode != "payload" || pdus != 1 {
		return pdus
	}
	return v.wireRules().Parts(m.Body, m.Encoding, v.multipartMode)
}
