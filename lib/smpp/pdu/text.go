package pdu

import (
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// ReadText reads the text of msg — a whole short_message or message_payload —
// that starts at octet textAt (after a UDH; 0 without one), as its
// data_coding says (SMPP 3.4 §5.2.19; Saleem, 19 Oct 2026: "use smpp
// defaults, whatever datacoding means use that"):
//
//   - 0, and 0x10–0x13 and 0xF0–0xF3 (the same with a message class), is the
//     GSM 03.38 alphabet. Each extension character (^ { } \ [ ~ ] | € and form
//     feed) is the escape 0x1B and one more septet: '~' is 1B 3D, '€' 1B 65.
//   - 1 (IA5) is ASCII; 3 is Latin-1; 8 is UCS-2.
//
// packed is how GSM-7 is carried, and it is the account's or the vendor's
// setting, never a guess from the octets (Saleem, 19 Oct 2026: "the setting
// separates packing or unpacking"): packed is 7-bit, eight septets in seven
// octets, the text starting at the first septet boundary after the header
// (GSM 03.40 §9.2.3.24), so msg is unpacked from its start and the header's
// septets skipped; unpacked is 8-bit, one septet in each octet. A short packed
// text can have no octet over 0x7F ("Ab" packs to 41 31), so the octets cannot
// tell the two apart: guessing read such an MO as "A1".
//
// One septet an octet never sets an octet's high bit. Unpacked text that has
// one was not written in the GSM alphabet: it is read as UTF-8 when it is
// valid UTF-8 (a sender that writes UTF-8 under data_coding 0), else as
// Latin-1, so that nothing is lost.
//
// part keeps what a part boundary may have cut in two — an escape that ends
// the part, half of a UCS-2 surrogate pair — for MendSplitEscapes to join once
// the parts are.
func ReadText(msg []byte, textAt int, dc byte, packed, part bool) string {
	if textAt < 0 || textAt > len(msg) {
		textAt = 0
	}
	body := msg[textAt:]
	switch {
	case IsUCS2Coding(dc): // UCS-2 (UTF-16BE)
		return decodeUCS2(body, part)
	case dc == 0x03: // Latin-1 / ISO-8859-1
		return latin1Text(body)
	case isGSMDefaultCoding(dc) && packed:
		return endEscape(septetText(unpackSeptets(msg, HeaderSeptets(textAt)), true), part)
	case isGSMDefaultCoding(dc) && oneSeptetEach(body):
		return GSM7OctetsText(body, part)
	default: // 0x01 IA5; unpacked GSM-7 that is not one septet an octet
		if utf8.Valid(body) {
			// The escape pairs are read as their characters for every GSM-7
			// or plain-text data_coding, not only 0 (encoding test of 19 Oct
			// 2026, enc-66/67: with 1, or 0xF0 for a flash message, "1B 3D"
			// was stored as ESC and "=" and sent on as UCS-2). 0x1B is not a
			// character anybody types into a message.
			if isGSM7Coding(dc) {
				if text, ok := unescapeGSM7Keep(body, part); ok {
					return text
				}
			}
			return string(body) // ASCII, or UTF-8 from a sender that writes it
		}
		return latin1Text(body) // keeps every octet, and the enqueue valid
	}
}

// DecodeShortMessage reads a whole message with no header, unpacked:
// ReadText(raw, 0, dc, false, false).
func DecodeShortMessage(raw []byte, dc byte) string { return ReadText(raw, 0, dc, false, false) }

// DecodeShortMessagePart is DecodeShortMessage for one part of a longer
// message: a GSM-7 escape that ends it is kept, as "\x1b", so the joined
// text can be mended (MendSplitEscapes) — its partner opens the next part.
func DecodeShortMessagePart(raw []byte, dc byte) string { return ReadText(raw, 0, dc, false, true) }

// endEscape drops an escape left at the very end of a whole message (nothing
// follows it, so it escapes nothing) and keeps it on a part, as "\x1b": its
// partner opens the next part.
func endEscape(text string, part bool) string {
	if part {
		return text
	}
	return strings.TrimSuffix(text, "\x1b")
}

// latin1Text reads raw as ISO-8859-1, one character an octet.
func latin1Text(raw []byte) string {
	r := make([]rune, len(raw))
	for i, c := range raw {
		r[i] = rune(c)
	}
	return string(r)
}

// oneSeptetEach reports whether raw can be GSM 03.38 written one septet in
// each octet: no octet has its high bit set.
func oneSeptetEach(raw []byte) bool {
	for _, b := range raw {
		if b >= 0x80 {
			return false
		}
	}
	return true
}

// EncodeShortMessage is the mirror of DecodeShortMessage: it turns UTF-8 text
// into a short_message payload plus the data_coding that describes it.
//
// Needed because deliver_sm to a bound client used to send every body as raw
// UTF-8 bytes with data_coding=0 (P6-B5). For plain ASCII that is harmless; for
// Urdu, Arabic or any non-GSM text it is not — the handset is told "GSM-7
// default" and shown UTF-8 bytes, so it renders rubbish. UCS-2 is not a flag on
// the same bytes: the payload itself must be UTF-16BE.
//
// GSM-7 (and an unknown label) is data_coding 0 written as SMPP defines it
// (Saleem, 19 Oct 2026: "use smpp defaults"): the GSM 03.38 alphabet, one
// septet in each octet (GSM7Octets) — '@' is 0x00, '_' 0x11, '~' ESC 0x3D.
// It was the text's own bytes, so a client reading data_coding 0 as GSM saw
// '~' as 'ü' and '€' as three wrong characters (encoding test of 19 Oct 2026,
// enc-81). Text the alphabet has no character for goes as UCS-2, dc 8.
func EncodeShortMessage(text, encoding string) (payload []byte, dataCoding byte) {
	switch encoding {
	case "ucs2":
		return ucs2Octets(text), 0x08
	case "latin1":
		out := make([]byte, 0, len(text))
		for _, r := range text {
			if r > 0xFF {
				// Unrepresentable in Latin-1. '?' matches what every other
				// gateway does and keeps the length honest; silently dropping
				// the rune would shift the rest of the message.
				out = append(out, '?')
				continue
			}
			out = append(out, byte(r))
		}
		return out, 0x03
	default: // gsm7 / unknown: the SMSC default alphabet, GSM 03.38
		if b, err := GSM7Octets(text); err == nil {
			return b, 0x00
		}
		return ucs2Octets(text), 0x08
	}
}

// ucs2Octets writes text as UTF-16BE.
func ucs2Octets(text string) []byte {
	u := utf16.Encode([]rune(text))
	out := make([]byte, 0, len(u)*2)
	for _, c := range u {
		out = append(out, byte(c>>8), byte(c))
	}
	return out
}

// EncodingOf is the encoding EncodeShortMessage writes text in when asked
// for enc: GSM-7 text the alphabet cannot carry is UCS-2.
func EncodingOf(text, enc string) string {
	if enc == "ucs2" || enc == "latin1" {
		return enc
	}
	if _, err := septets(text); err != nil {
		return "ucs2"
	}
	return "gsm7"
}

// EncodingFromDataCoding maps data_coding to the canonical encoding label.
func EncodingFromDataCoding(dc byte) string {
	if IsUCS2Coding(dc) {
		return "ucs2"
	}
	return "gsm7"
}

// IsUCS2Coding reports a data_coding whose text is UCS-2: 0x08, and the GSM
// 03.38 general-group values with a message class that name UCS-2 (0x18 to
// 0x1B).
func IsUCS2Coding(dc byte) bool { return dc == 0x08 || dc >= 0x18 && dc <= 0x1B }

// isGSM7Coding reports a data_coding whose text is the GSM default alphabet
// or plain text: 0x00 (the SMSC default), 0x01 (IA5/ASCII), and the GSM 03.38
// values for the default alphabet with a message class — 0x10 to 0x13, and
// 0xF0 to 0xF3 (a flash message is 0xF0).
func isGSM7Coding(dc byte) bool {
	return dc == 0x01 || isGSMDefaultCoding(dc)
}

// isGSMDefaultCoding reports a data_coding that names the GSM 03.38 default
// alphabet: 0x00 (the SMSC default, SMPP 3.4 §5.2.19), and 0x10 to 0x13 and
// 0xF0 to 0xF3 (the same alphabet with a message class). Not 0x01: IA5 is
// ASCII.
func isGSMDefaultCoding(dc byte) bool {
	return dc == 0x00 || dc >= 0x10 && dc <= 0x13 || dc >= 0xF0 && dc <= 0xF3
}

// IsBinaryCoding reports a data_coding of 8-bit data, not text: 0x02 and 0x04
// (SMPP's octet strings), and the GSM 03.38 8-bit data values with a message
// class (0x14 to 0x17, 0xF4 to 0xF7).
func IsBinaryCoding(dc byte) bool {
	return dc == 0x02 || dc == 0x04 || dc >= 0x14 && dc <= 0x17 || dc >= 0xF4 && dc <= 0xF7
}

// decodeUCS2 reads UTF-16BE. A surrogate pair that a part boundary cut in two
// — the high half ending one part, the low half opening the next — is kept,
// when keep, as a marker per half ("\x1bU" and the half's four hex digits),
// for MendSplitEscapes to join once the parts are; decoded alone each half was
// U+FFFD, so an emoji split across two parts read "��" (encoding test of 19
// Oct 2026, enc-76/89). Any other lone half is U+FFFD, as before.
func decodeUCS2(raw []byte, keep bool) string {
	if len(raw)%2 == 1 {
		raw = raw[:len(raw)-1] // defensively drop a dangling odd byte
	}
	u := make([]uint16, len(raw)/2)
	for i := range u {
		u[i] = uint16(raw[2*i])<<8 | uint16(raw[2*i+1])
	}
	if !keep || len(u) == 0 {
		return string(utf16.Decode(u))
	}
	var head, tail string
	if isLowSurrogate(u[0]) {
		head, u = surrogateMarker(u[0]), u[1:]
	}
	if n := len(u); n > 0 && isHighSurrogate(u[n-1]) {
		tail, u = surrogateMarker(u[n-1]), u[:n-1]
	}
	return head + string(utf16.Decode(u)) + tail
}

func isHighSurrogate(c uint16) bool { return c >= 0xD800 && c <= 0xDBFF }
func isLowSurrogate(c uint16) bool  { return c >= 0xDC00 && c <= 0xDFFF }

// surrogateMarker is how decodeUCS2 keeps half of a cut surrogate pair.
func surrogateMarker(c uint16) string {
	const hex = "0123456789ABCDEF"
	return string([]byte{gsm7Escape, 'U', hex[c>>12], hex[c>>8&0xF], hex[c>>4&0xF], hex[c&0xF]})
}
