package pdu

import (
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// Packed GSM-7: an SMPP client whose account is set to packed
// (sms_service_config.octates_to_septets_unpacking, 2026-09-14), or a vendor
// whose gsm7_packing is "packed". UnpackGSM7 decodes through the GSM 03.38
// tables and handles the fill bits a concatenation header (UDH) leaves before
// the text: the text starts at the first septet boundary after the header,
// not at the next octet. Packed or not is the setting's to say (ReadText);
// the guess that read octets as packed when they were not valid UTF-8
// (Unpack7Bit) was retired on 19 Oct 2026.

var (
	gsm7BasicRune = func() map[byte]rune {
		m := make(map[byte]rune, len(gsm7BasicSeptet))
		for r, v := range gsm7BasicSeptet {
			m[v] = r
		}
		return m
	}()
	gsm7ExtRune = func() map[byte]rune {
		m := make(map[byte]rune, len(gsm7ExtSeptet))
		for r, v := range gsm7ExtSeptet {
			m[v] = r
		}
		return m
	}()
)

// HeaderSeptets is how many septets a header of udhOctets octets (the length
// byte included) occupies in a packed message, fill bits included.
func HeaderSeptets(udhOctets int) int {
	if udhOctets <= 0 {
		return 0
	}
	return (udhOctets*8 + 6) / 7
}

// UnpackGSM7 decodes packed GSM-7 octets. skipSeptets is the header's share
// (HeaderSeptets of the UDH length, 0 without a header); raw is the WHOLE
// short_message, header included, because the text's septets are aligned to
// the start of the message, not to the end of the header.
func UnpackGSM7(raw []byte, skipSeptets int) string {
	sept := unpackSeptets(raw, skipSeptets)
	return septetText(sept, false)
}

// UnpackGSM7Part is UnpackGSM7 for one part of a longer message: an ESC that
// ends it is kept, as "\x1b", for MendSplitEscapes to join with the first
// character of the next part. UnpackGSM7 writes a space for it.
func UnpackGSM7Part(raw []byte, skipSeptets int) string {
	return septetText(unpackSeptets(raw, skipSeptets), true)
}

// unpackSeptets is the septets of packed raw, the padding and the header's
// septets taken off.
func unpackSeptets(raw []byte, skipSeptets int) []byte {
	n := len(raw) * 8 / 7
	sept := make([]byte, 0, n)
	var buf uint32
	var bits uint
	for _, b := range raw {
		buf |= uint32(b) << bits
		bits += 8
		for bits >= 7 {
			sept = append(sept, byte(buf&0x7F))
			buf >>= 7
			bits -= 7
		}
	}
	// Exactly seven spare bits at the end hold padding, not a character:
	// zeros from most senders, CR from strict ones (GSM 03.38 §6.1.2.1).
	if len(raw)%7 == 0 && len(sept) > 0 {
		if last := sept[len(sept)-1]; last == 0x00 || last == 0x0D {
			sept = sept[:len(sept)-1]
		}
	}
	if skipSeptets > len(sept) {
		return nil
	}
	return sept[skipSeptets:]
}

// septetText reads GSM 03.38 septet values — packed or one in each octet —
// through the alphabet's tables: ESC and a value is that extension character,
// ESC and an unknown value shows the base character (GSM 03.38). An ESC at
// the very end is a space, or "\x1b" when keepEsc (a part of a longer
// message: its partner opens the next part).
func septetText(sept []byte, keepEsc bool) string {
	var sb strings.Builder
	for i := 0; i < len(sept); i++ {
		v := sept[i]
		if v == 0x1B && i+1 < len(sept) {
			if r, ok := gsm7ExtRune[sept[i+1]]; ok {
				sb.WriteRune(r)
				i++
				continue
			}
			// An unknown extension shows the base character (GSM 03.38).
			i++
			v = sept[i]
		}
		if r, ok := gsm7BasicRune[v]; ok {
			sb.WriteRune(r)
		} else if v == 0x1B {
			if keepEsc && i == len(sept)-1 {
				sb.WriteByte(gsm7Escape) // its partner opens the next part
			} else {
				sb.WriteRune(' ') // an escape with nothing to escape (GSM 03.38: shown as a space)
			}
		}
	}
	return sb.String()
}

// GSM7OctetsText reads data_coding 0 text written in the GSM 03.38 alphabet
// with one septet value in each octet — what data_coding 0 means over SMPP
// (19 Oct 2026; GSM7Octets is the other way). '@' is 0x00, '_' 0x11, 'ü'
// 0x7E, and '~' the two octets 1B 3D. Read as bytes, as before, '@' was a
// NUL, 'ü' a '~' and 'ä' a '{'. keepEsc as for UnpackGSM7Part; without it an
// ESC at the very end is dropped, as the escape reader always did.
func GSM7OctetsText(raw []byte, keepEsc bool) string {
	sept := make([]byte, len(raw))
	for i, b := range raw {
		sept[i] = b & 0x7F
	}
	text := septetText(sept, true)
	if !keepEsc {
		text = strings.TrimSuffix(text, "\x1b") // septetText writes ESC only for one left at the end
	}
	return text
}

// escPartner is, for each extension character, how the octet after its ESC
// reads once a part was decoded without it — as a byte, or through the GSM
// table (packed, or one septet an octet): '~' is ESC 0x3D, and 0x3D alone is
// '='.
var escPartner = func() map[rune]rune {
	m := map[rune]rune{}
	for r, v := range gsm7ExtSeptet {
		m[rune(v)] = r // the octet read as a byte
		if b, ok := gsm7BasicRune[v]; ok {
			m[b] = r // the octet read through the GSM table
		}
	}
	return m
}()

// MendSplitEscapes joins an extension character that a part boundary cut in
// two (go-live test of 18 Oct 2026; Saleem: "previously it was showing =
// instead of ~"). A sender that splits a long message by octets can end a
// part with the ESC and open the next with its partner; each part decoded by
// itself dropped the ESC, and the joined text read '=' for '~', '(' for '{',
// '<' for '['. Parts decoded with UnpackGSM7Part, GSM7OctetsText(…, true) or
// DecodeShortMessagePart keep that ESC as "\x1b"; this, run on the joined
// text, makes ESC and partner the one character again and drops an ESC left
// with nothing after it. Text with no ESC comes back as it was.
func MendSplitEscapes(text string) string {
	if !strings.ContainsRune(text, gsm7Escape) {
		return text
	}
	rs := []rune(text)
	var sb strings.Builder
	for i := 0; i < len(rs); i++ {
		if rs[i] != gsm7Escape {
			sb.WriteRune(rs[i])
			continue
		}
		// Half of a UCS-2 surrogate pair a part boundary cut (decodeUCS2):
		// joined with the other half when it follows, else U+FFFD.
		if hi, ok := markerAt(rs, i); ok {
			if lo, ok := markerAt(rs, i+6); ok && isHighSurrogate(hi) && isLowSurrogate(lo) {
				sb.WriteRune(utf16.DecodeRune(rune(hi), rune(lo)))
				i += 11
				continue
			}
			sb.WriteRune(utf8.RuneError)
			i += 5
			continue
		}
		if i+1 < len(rs) {
			if r, ok := escPartner[rs[i+1]]; ok {
				sb.WriteRune(r)
				i++
			}
		}
		// A lone ESC, or one before something that is not an extension's
		// partner: dropped, as GSM 03.38 asks of a receiver.
	}
	return sb.String()
}

// markerAt reads a surrogate marker (surrogateMarker: ESC, 'U', four hex
// digits) at rs[i].
func markerAt(rs []rune, i int) (uint16, bool) {
	if i+5 >= len(rs) || rs[i] != gsm7Escape || rs[i+1] != 'U' {
		return 0, false
	}
	var v uint16
	for _, r := range rs[i+2 : i+6] {
		switch {
		case r >= '0' && r <= '9':
			v = v<<4 | uint16(r-'0')
		case r >= 'A' && r <= 'F':
			v = v<<4 | uint16(r-'A'+10)
		default:
			return 0, false
		}
	}
	if !isHighSurrogate(v) && !isLowSurrogate(v) {
		return 0, false
	}
	return v, true
}

// PackGSM7After packs text after an optional header: it returns the header
// followed by the text's septets, starting at the first septet boundary after
// the header (the fill bits are zero). Without a header it is PackGSM7.
func PackGSM7After(udh []byte, text string) ([]byte, error) {
	s, err := septets(text)
	if err != nil {
		return nil, err
	}
	if len(udh) == 0 {
		return packSeptets(s), nil
	}
	lead := HeaderSeptets(len(udh))
	all := make([]byte, lead+len(s))
	copy(all[lead:], s) // the header's septet slots stay zero for now
	packed := packSeptets(all)
	// The header's own octets overwrite the zero slots; the fill bits after
	// it are the zero bits already in place.
	copy(packed, udh)
	return packed, nil
}

// PackedGSM7Fits reports whether text packs into at most maxOctets.
func PackedGSM7Fits(text string, maxOctets int) bool {
	s, err := septets(text)
	return err == nil && (len(s)*7+7)/8 <= maxOctets
}
