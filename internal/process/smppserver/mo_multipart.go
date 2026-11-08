package smppserver

// MO multipart delivery to a bound client (P6-B6, PO 2026-09-03).
//
// The MO service (internal/mo) never merges anything — it stages exactly one
// message per deliver_sm PDU it is handed. A body can still exceed the 254
// octets SMPP's short_message field allows because DecodeShortMessage prefers
// the message_payload TLV (lib/smpp/pdu/pdu.go: MessageBytes), which a
// vendor can fill with an inbound message of any real length, no UDH needed.
// So a genuinely long MO is staged correctly as one record — the bug was only
// on the OUTBOUND leg, delivering it back to OUR bound client: onDeliverSM's
// caller hardcoded a single deliver_sm PDU and silently clipped anything past
// 254 octets, logging the loss rather than fixing it.
//
// The fix mirrors what MT egress already does toward vendors
// (internal/process/smppvendor's wireParts): split by wire cost into
// UDH-framed parts. Simpler here, because MO delivery never negotiates a
// per-vendor multipartMode — a bound client is just a client, not a
// configurable vendor profile — so UDH is the only framing used; it is the
// universally-supported choice, unlike the TLV/SAR mode MT sometimes needs
// for a vendor that prefers it.

import (
	"sync/atomic"

	"github.com/yakugate/yakugate/lib/encoding"
	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// moConcatRef hands out 8-bit concatenation reference numbers for MO delivery
// groups, wrapping at 256 — the same UDH IE width MT egress uses, kept
// separate from MT's own counter so the two legs' reference numbers can never
// collide in a way that would confuse a handset reassembling both directions.
var moConcatRef uint32

// moUDH builds the 6-byte concatenated-SMS User Data Header (8-bit reference
// form, IE 0x00): 05 00 03 ref total seq. Identical wire shape to MT egress's
// own udh() — duplicated rather than shared because it is two lines and pulling
// in smppvendor from smppserver would be the wrong dependency direction
// (client-facing importing vendor-facing).
func moUDH(ref byte, total, seq int) []byte {
	return []byte{0x05, 0x00, 0x03, ref, byte(total), byte(seq)}
}

// moWireParts turns one MO's body into the deliver_sm payload(s) for a bound
// client, UDH-framing each part when more than one is needed.
//
// maxParts caps the split at YAKU_MAX_SEGMENTS (shared with MT's own cap —
// PO 2026-09-03: one ceiling to reason about, not two). A body that would
// need more parts than that is TRUNCATED to the cap — the pre-existing
// safety valve, now reached only by a genuinely excessive body (with
// YAKU_MAX_SEGMENTS=10, over roughly 1530 GSM-7 characters or 670 UCS-2 code
// units) rather than every body past 254 raw octets. truncated reports
// whether that happened, so the caller can log it.
func moWireParts(body, enc string, maxParts int) (parts [][]byte, dataCoding byte, esmClass byte, truncated bool) {
	return moWirePartsFor(body, enc, maxParts, false)
}

// moWirePartsFor is moWireParts for a client that wants GSM-7 packed into
// septets (octates_to_septets_unpacking, 2026-09-14): the same 254-octet
// single-PDU ceiling and 153-character parts, but each part's text packed
// after its header with the fill bits the standard asks for. Text outside the
// GSM-7 alphabet is not GSM-7 at all, so it goes out as before.
//
// A bind that is not packed is written data_coding 0 as SMPP defines it: the
// GSM 03.38 alphabet, one septet in each octet (pdu.EncodeShortMessage; it was
// the text's own bytes until 19 Oct 2026). Text with a character the alphabet
// does not have goes as UCS-2, split 67 a part.
func moWirePartsFor(body, enc string, maxParts int, packed bool) (parts [][]byte, dataCoding byte, esmClass byte, truncated bool) {
	enc = pdu.EncodingOf(body, enc)
	if packed && enc != encoding.UCS2 && pdu.PackedGSM7Fits(body, 1<<30) {
		if whole, err := pdu.PackGSM7(body); err == nil && len(whole) <= 254 {
			return [][]byte{whole}, 0x00, 0x00, false
		}
		chunks := encoding.SplitByCost(body, encoding.GSM7, 153)
		if len(chunks) > maxParts {
			chunks = chunks[:maxParts]
			truncated = true
		}
		ref := byte(atomic.AddUint32(&moConcatRef, 1))
		out := make([][]byte, len(chunks))
		for i, c := range chunks {
			b, err := pdu.PackGSM7After(moUDH(ref, len(chunks), i+1), c)
			if err != nil {
				return moWirePartsFor(body, enc, maxParts, false)
			}
			out[i] = b
		}
		return out, 0x00, 0x40, truncated
	}
	payload, dc := pdu.EncodeShortMessage(body, enc)
	if len(payload) <= 254 {
		return [][]byte{payload}, dc, 0x00, false
	}

	// Multi-part budget per PDU: 153 (gsm7) / 67 (ucs2) UnitCost units,
	// leaving room for the 6-byte UDH inside the 140-byte short_message
	// capacity — the same standard SMPP/GSM-3.40 constants MT egress's UDH
	// mode uses (thresholds(enc, "udh") there), hardcoded here since MO
	// delivery has no vendor multipartMode to branch on.
	limit := 153
	if enc == encoding.UCS2 {
		limit = 67
	}
	chunks := splitFitting(body, enc, limit, 254-6)
	if len(chunks) > maxParts {
		chunks = chunks[:maxParts]
		truncated = true
	}

	ref := byte(atomic.AddUint32(&moConcatRef, 1))
	total := len(chunks)
	out := make([][]byte, total)
	for i, c := range chunks {
		cb, cdc := pdu.EncodeShortMessage(c, enc)
		dc = cdc // identical across parts (one encoding for the whole body); keep the last
		out[i] = append(moUDH(ref, total, i+1), cb...)
	}
	return out, dc, 0x40, truncated
}

// splitFitting cuts text between characters into parts of at most limit
// units of wire cost (153 GSM-7 septets, 67 UCS-2 units) AND at most max
// octets as they are written. A part was sized in septets only, but a bind
// that is not packed was written the text's own bytes until 19 Oct 2026: an
// accent-heavy part was two or three times its septets, and the PDU builder
// cut it at 254 octets — 300 'é' reached the client as 248 (encoding test of
// 19 Oct 2026, enc-91). Written in the GSM alphabet, one septet an octet, a
// GSM-7 part's octets are its septets; the octet limit stays as the guard.
// max leaves room for the 6-octet header.
func splitFitting(text, enc string, limit, max int) []string {
	var out []string
	start, cost, size := 0, 0, 0
	for i, r := range text {
		c := encoding.UnitCost(r, enc)
		b, _ := pdu.EncodeShortMessage(string(r), enc)
		if (cost+c > limit || size+len(b) > max) && i > start {
			out = append(out, text[start:i])
			start, cost, size = i, 0, 0
		}
		cost += c
		size += len(b)
	}
	if start < len(text) {
		out = append(out, text[start:])
	}
	return out
}

// moWirePayload encodes a whole inbound message for ONE deliver_sm carrying
// message_payload (PO 2026-09-17): no UDH, no part cap, the TLV has no
// 254-octet limit. Packed GSM-7 when the account asks for it and the text
// fits the alphabet; otherwise the plain encoding of the body.
func moWirePayload(body, enc string, packed bool) (payload []byte, dataCoding byte) {
	enc = pdu.EncodingOf(body, enc)
	if packed && enc != encoding.UCS2 && pdu.PackedGSM7Fits(body, 1<<30) {
		if whole, err := pdu.PackGSM7(body); err == nil {
			return whole, 0x00
		}
	}
	return pdu.EncodeShortMessage(body, enc)
}
