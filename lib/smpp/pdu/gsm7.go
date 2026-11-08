package pdu

import "fmt"

// GSM 03.38 default alphabet: rune -> 7-bit septet value. Septet 0x1B (27) is
// never assigned a character here — it is the ESCAPE marker into the
// extension table below, exactly as internal/encoding.gsm7Basic already
// treats it (that table pads position 27 with a duplicate space so
// strings.ContainsRune keeps working; this table omits it entirely since a
// wrong septet there would silently corrupt every following character).
var gsm7BasicSeptet = map[rune]byte{
	'@': 0x00, '£': 0x01, '$': 0x02, '¥': 0x03, 'è': 0x04, 'é': 0x05, 'ù': 0x06, 'ì': 0x07,
	'ò': 0x08, 'Ç': 0x09, '\n': 0x0A, 'Ø': 0x0B, 'ø': 0x0C, '\r': 0x0D, 'Å': 0x0E, 'å': 0x0F,
	'Δ': 0x10, '_': 0x11, 'Φ': 0x12, 'Γ': 0x13, 'Λ': 0x14, 'Ω': 0x15, 'Π': 0x16, 'Ψ': 0x17,
	'Σ': 0x18, 'Θ': 0x19, 'Ξ': 0x1A, 'Æ': 0x1C, 'æ': 0x1D, 'ß': 0x1E, 'É': 0x1F,
	' ': 0x20, '!': 0x21, '"': 0x22, '#': 0x23, '¤': 0x24, '%': 0x25, '&': 0x26, '\'': 0x27,
	'(': 0x28, ')': 0x29, '*': 0x2A, '+': 0x2B, ',': 0x2C, '-': 0x2D, '.': 0x2E, '/': 0x2F,
	'0': 0x30, '1': 0x31, '2': 0x32, '3': 0x33, '4': 0x34, '5': 0x35, '6': 0x36, '7': 0x37,
	'8': 0x38, '9': 0x39, ':': 0x3A, ';': 0x3B, '<': 0x3C, '=': 0x3D, '>': 0x3E, '?': 0x3F,
	'¡': 0x40, 'A': 0x41, 'B': 0x42, 'C': 0x43, 'D': 0x44, 'E': 0x45, 'F': 0x46, 'G': 0x47,
	'H': 0x48, 'I': 0x49, 'J': 0x4A, 'K': 0x4B, 'L': 0x4C, 'M': 0x4D, 'N': 0x4E, 'O': 0x4F,
	'P': 0x50, 'Q': 0x51, 'R': 0x52, 'S': 0x53, 'T': 0x54, 'U': 0x55, 'V': 0x56, 'W': 0x57,
	'X': 0x58, 'Y': 0x59, 'Z': 0x5A, 'Ä': 0x5B, 'Ö': 0x5C, 'Ñ': 0x5D, 'Ü': 0x5E, '§': 0x5F,
	'¿': 0x60, 'a': 0x61, 'b': 0x62, 'c': 0x63, 'd': 0x64, 'e': 0x65, 'f': 0x66, 'g': 0x67,
	'h': 0x68, 'i': 0x69, 'j': 0x6A, 'k': 0x6B, 'l': 0x6C, 'm': 0x6D, 'n': 0x6E, 'o': 0x6F,
	'p': 0x70, 'q': 0x71, 'r': 0x72, 's': 0x73, 't': 0x74, 'u': 0x75, 'v': 0x76, 'w': 0x77,
	'x': 0x78, 'y': 0x79, 'z': 0x7A, 'ä': 0x7B, 'ö': 0x7C, 'ñ': 0x7D, 'ü': 0x7E, 'à': 0x7F,
}

// gsm7ExtSeptet is the GSM 03.38 extension table, the characters
// internal/encoding.gsm7Ext counts as two septets ("^{}\\[~]|€" and form
// feed). On the wire each is the escape 0x1B followed by its value below: '~'
// is 1B 3D, and 3D alone is '='.
var gsm7ExtSeptet = map[rune]byte{
	'^': 0x14, '{': 0x28, '}': 0x29, '\\': 0x2F, '[': 0x3C, '~': 0x3D, ']': 0x3E, '|': 0x40, '€': 0x65,
	'\f': 0x0A, // form feed, the page break
}

// septets converts text to its GSM 7-bit default-alphabet septet stream.
func septets(text string) ([]byte, error) {
	out := make([]byte, 0, len(text))
	for _, r := range text {
		if v, ok := gsm7ExtSeptet[r]; ok {
			out = append(out, 0x1B, v)
			continue
		}
		if v, ok := gsm7BasicSeptet[r]; ok {
			out = append(out, v)
			continue
		}
		return nil, fmt.Errorf("pdu: rune %q is not in the GSM-7 default alphabet", r)
	}
	return out, nil
}

// packSeptets packs 7-bit values 8-into-7 per GSM 03.38 §6.1.2.1: septets are
// treated as a continuous LSB-first bitstream and re-sliced into octets.
//
// When len(sept)%8==7 the last octet has seven spare bits, and seven zero
// bits read as '@': GSM 03.38 §6.1.2.1.1 fills them with CR, which a reader
// drops. They were zero until go-live test P3 B4 (18 Oct 2026), so a strict
// SMSC could show a message of 8n-1 characters with an '@' on the end.
func packSeptets(sept []byte) []byte {
	if len(sept) == 0 {
		return nil
	}
	out := make([]byte, 0, (len(sept)*7+7)/8)
	var buf uint16
	var bits uint
	for _, s := range sept {
		buf |= uint16(s&0x7F) << bits
		bits += 7
		if bits >= 8 {
			out = append(out, byte(buf))
			buf >>= 8
			bits -= 8
		}
	}
	if bits > 0 {
		if bits == 1 {
			buf |= uint16(gsm7CR) << bits // the seven spare bits: CR, not '@'
		}
		out = append(out, byte(buf))
	}
	return out
}

// gsm7CR is the septet that fills seven spare bits at the end of a packed
// message (GSM 03.38 §6.1.2.1.1).
const gsm7CR = 0x0D

// PackGSM7 encodes text as packed GSM 7-bit octets (data_coding 0x00 wire
// format). Use GSM7Octets instead for a vendor whose protocol_config says
// gsm7_packing "unpacked" or "gsm" (one septet in each octet).
func PackGSM7(text string) ([]byte, error) {
	s, err := septets(text)
	if err != nil {
		return nil, err
	}
	return packSeptets(s), nil
}

// UnpackedGSM7 is the text's own bytes, NOT the GSM alphabet: '@' as 0x40 and
// '~' as 0x7E. It was what gsm7_packing "unpacked" sent under data_coding 0
// until 19 Oct 2026 (Saleem: "use smpp defaults, whatever datacoding means use
// that"); "unpacked" is now GSM7Octets, the same as "gsm". It is no wire form
// any more: the vendor role keeps it only as the fail-safe for text that is
// not GSM-7, which the wire rules never hand it (they send that as UCS-2).
func UnpackedGSM7(text string) []byte { return []byte(text) }

// GSM7Octets writes text in the GSM 03.38 default alphabet with ONE septet
// value in each octet and no packing — what data_coding 0 means over SMPP,
// and what Kannel and Jasmin send (gsm7_packing "gsm", 2026-10-12, and
// "unpacked" since 19 Oct 2026). '@' is 0x00, '_' is 0x11, and the extension
// characters ^ { } \ [ ~ ] | € are the escape 0x1B and their value: two
// octets, as the part sizes already count them.
//
// It exists because of go-live test P3 B4 ("7 bit ex characters are invalid,
// showing = instead of ~"): the gateway had only the packed form, which few
// SMSCs take over SMPP, and UnpackedGSM7, which is not the GSM alphabet.
func GSM7Octets(text string) ([]byte, error) { return septets(text) }

// gsm7Escape is the septet that opens the extension table.
const gsm7Escape = 0x1B

// unescapeGSM7 reads the GSM-7 extension escapes in a payload whose other
// bytes are taken as they are — data_coding 1 (ASCII), or 0 with an octet
// over 0x7F (UTF-8; one septet an octet is GSM7OctetsText since 19 Oct
// 2026): 0x1B and the value of an extension character become that character
// ("\x1b=" is "~", "\x1be" is "€").
//
// A client that writes GSM 03.38 sends those two octets for one character,
// and the payload was read as plain bytes: the text was stored as an escape
// control character and "=", shown as "=", and — the control character not
// being GSM-7 — sent on as UCS-2 (go-live P3 B4, a message of 2026-10-09
// stored as "Hello from SMPP Tester \x1b="). The escape cannot be mistaken
// for text: 0x1B is not a character anybody types into a message.
//
// An escape before anything else is dropped and the octet after it kept, which
// is what GSM 03.38 asks of a receiver that does not know the extension. ok
// reports whether anything was changed.
func unescapeGSM7(raw []byte) (text string, ok bool) { return unescapeGSM7Keep(raw, false) }

// unescapeGSM7Keep is unescapeGSM7 that, with keepEsc, keeps an ESC at the
// very end as "\x1b" (a part of a longer message: MendSplitEscapes).
func unescapeGSM7Keep(raw []byte, keepEsc bool) (text string, ok bool) {
	at := -1
	for i, b := range raw {
		if b == gsm7Escape {
			at = i
			break
		}
	}
	if at < 0 {
		return "", false
	}
	out := make([]byte, 0, len(raw)+2)
	out = append(out, raw[:at]...)
	for i := at; i < len(raw); i++ {
		if raw[i] != gsm7Escape {
			out = append(out, raw[i])
			continue
		}
		if i+1 < len(raw) {
			if r, isExt := gsm7ExtRune[raw[i+1]]; isExt {
				out = append(out, string(r)...)
				i++
			}
		} else if keepEsc {
			out = append(out, gsm7Escape) // its partner opens the next part
		}
		// A lone escape, or one before an unknown value: dropped.
	}
	return string(out), true
}
