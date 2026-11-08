package encoding

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// How each encoding goes out to one vendor (PO 2026-09-21: "a setting per
// vendor … for all encodings"). [SCOPE P3-ENCODINGS]
//
// A message is planned and billed in the encoding the client asked for, or
// the one chosen for it (Plan). What an SMSC accepts differs by vendor: one
// has no Latin-1, one wants GSM-7 labelled data_coding 1, one takes Latin-1
// only as data_coding 0. So each vendor may say, per encoding, what to send it
// as and with which data_coding:
//
//	protocol_config.encodings = {
//	  "gsm7":   {"send_as": "gsm7",   "data_coding": 0},
//	  "latin1": {"send_as": "latin1", "data_coding": 3},
//	  "ucs2":   {"send_as": "ucs2",   "data_coding": 8}
//	}
//
// Those are the defaults, and any part may be left out. "latin1": "ucs2" (a
// plain string) is short for {"send_as": "ucs2"}, labelled as this vendor
// labels UCS-2.
//
// One rule keeps text intact: if a message's text cannot be written in the
// encoding it is to be sent as (Greek capitals asked to go as Latin-1, "â"
// asked to go as GSM-7), that message goes as UCS-2, which can carry any
// text. The alternative — sending bytes the handset will misread — is the
// bug this replaced (Latin-1 went out as GSM-7, and "â" arrived garbled).
//
// This file is the one reading of the setting: the SMPP vendor role, the
// console and the Admin API's check all follow it.

// WireRule is how one encoding is sent to a vendor.
type WireRule struct {
	SendAs     string // gsm7 | latin1 | ucs2
	DataCoding byte   // the submit_sm data_coding to label it with
}

// WireRules maps a message's encoding (gsm7, latin1, ucs2) to its rule.
type WireRules map[string]WireRule

// Encodings is every encoding, in the order screens show them.
func Encodings() []string { return []string{GSM7, Latin1, UCS2} }

// DefaultDataCoding is the standard SMPP data_coding for an encoding.
func DefaultDataCoding(enc string) byte {
	switch enc {
	case Latin1:
		return 0x03
	case UCS2:
		return 0x08
	}
	return 0x00
}

// DefaultWireRules sends every encoding as itself, with its standard label.
func DefaultWireRules() WireRules {
	w := WireRules{}
	for _, e := range Encodings() {
		w[e] = WireRule{SendAs: e, DataCoding: DefaultDataCoding(e)}
	}
	return w
}

func knownEnc(e string) bool { return e == GSM7 || e == Latin1 || e == UCS2 }

// ParseWireRules reads protocol_config.encodings. Whatever it cannot use is
// reported in problems and left at the default, so one bad entry never stops
// a vendor from sending. An empty or null value is all defaults.
func ParseWireRules(raw json.RawMessage) (WireRules, []string) {
	w := DefaultWireRules()
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return w, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return w, []string{"encodings is not an object: " + err.Error()}
	}
	var problems []string
	labelled := map[string]bool{} // rules given their own data_coding
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		enc := strings.ToLower(strings.TrimSpace(k))
		if !knownEnc(enc) {
			problems = append(problems, fmt.Sprintf("encodings.%s: not an encoding (gsm7, latin1 or ucs2)", k))
			continue
		}
		var sendAs string
		var dc *int
		if json.Unmarshal(m[k], &sendAs) != nil { // not the string shorthand
			var obj struct {
				SendAs     string `json:"send_as"`
				DataCoding *int   `json:"data_coding"`
			}
			if err := json.Unmarshal(m[k], &obj); err != nil {
				problems = append(problems, fmt.Sprintf("encodings.%s: %v", k, err))
				continue
			}
			sendAs, dc = obj.SendAs, obj.DataCoding
		}
		sendAs = strings.ToLower(strings.TrimSpace(sendAs))
		if sendAs == "" {
			sendAs = enc
		}
		if !knownEnc(sendAs) {
			problems = append(problems, fmt.Sprintf("encodings.%s.send_as %q: send as gsm7, latin1 or ucs2", k, sendAs))
			continue
		}
		rule := WireRule{SendAs: sendAs, DataCoding: DefaultDataCoding(sendAs)}
		if dc != nil {
			if *dc < 0 || *dc > 255 {
				problems = append(problems, fmt.Sprintf("encodings.%s.data_coding %d: a data_coding is 0 to 255", k, *dc))
				continue
			}
			rule.DataCoding = byte(*dc)
			labelled[enc] = true
		}
		w[enc] = rule
	}
	// A rule that sends as another encoding and names no data_coding takes
	// the label this vendor uses for that encoding — "latin1": "ucs2" on a
	// vendor that labels UCS-2 as 24 sends 24, not the textbook 8.
	for _, e := range Encodings() {
		r := w[e]
		if labelled[e] || r.SendAs == e {
			continue
		}
		if t := w[r.SendAs]; t.SendAs == r.SendAs {
			r.DataCoding = t.DataCoding
			w[e] = r
		}
	}
	return w, problems
}

// Resolve says how a text of encoding enc goes to this vendor: the encoding
// its bytes are written in, the data_coding that labels them, and whether the
// vendor's choice could not carry the text so it went as UCS-2 instead.
func (w WireRules) Resolve(text, enc string) (wire string, dc byte, fellBack bool) {
	if !knownEnc(enc) {
		enc = GSM7
	}
	rule, ok := w[enc]
	if !ok {
		rule = WireRule{SendAs: enc, DataCoding: DefaultDataCoding(enc)}
	}
	fits := true
	switch rule.SendAs {
	case GSM7:
		fits = isGSM7(text)
	case Latin1:
		fits = isLatin1(text)
	}
	if fits {
		return rule.SendAs, rule.DataCoding, false
	}
	// UCS-2 carries any text. Labelled the way this vendor labels UCS-2, when
	// its own UCS-2 rule sends UCS-2; else the standard 8.
	u := w[UCS2]
	if u.SendAs != UCS2 {
		u.DataCoding = DefaultDataCoding(UCS2)
	}
	return UCS2, u.DataCoding, true
}

// Describe is a rule set in words, for a screen: "gsm7 → gsm7 (0) · latin1 → ucs2 (8) · …".
func (w WireRules) Describe() string {
	var parts []string
	for _, e := range Encodings() {
		r := w[e]
		parts = append(parts, fmt.Sprintf("%s → %s (%d)", e, r.SendAs, r.DataCoding))
	}
	return strings.Join(parts, " · ")
}

// IsLatin1 reports whether every character fits ISO-8859-1.
func IsLatin1(text string) bool { return isLatin1(text) }

// VendorMultipartMode reads a vendor's protocol_config.multipart_mode:
// "payload"/"message_payload" = ONE submit_sm carrying message_payload;
// "tlv"/"sar" = parts with SAR TLVs; anything else = UDH parts.
func VendorMultipartMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "payload", "message_payload":
		return "payload"
	case "tlv", "sar":
		return "tlv"
	}
	return "udh"
}

// WireSizes is how many units one part carries alone and within a long
// message, for the encoding a vendor is SENT and its multipart mode. SAR
// parts carry no header, so a part holds the full single-part size; UDH and
// payload (which the SMSC splits into standard parts) lose room to the
// header.
func WireSizes(wire, multipartMode string) (single, multi int) {
	sar := multipartMode == "tlv"
	switch wire {
	case Latin1:
		if sar {
			return 140, 140
		}
		return 140, 134
	case UCS2:
		if sar {
			return 70, 70
		}
		return 70, 67
	}
	if sar {
		return 160, 160
	}
	return 160, 153
}

// Parts is how many parts a vendor receives — and bills — for a text of
// encoding enc: written as its rules say (Resolve), split as its multipart
// mode splits. The client is charged the same count (PO 2026-09-21: "client
// needs to be billed by vendor's encoding, so that we are never in loss"),
// so accept-time pricing, the vendor role and billing all count through here.
func (w WireRules) Parts(text, enc, multipartMode string) int {
	if w == nil {
		w = DefaultWireRules()
	}
	wire, _, _ := w.Resolve(text, enc)
	single, multi := WireSizes(wire, VendorMultipartMode(multipartMode))
	return segmentsByCost(text, wire, single, multi)
}

// ParseVendorEncodings reads BOTH of a vendor's encoding settings into one
// rule set (PO 2026-09-21, "we need it"):
//
//   - accepted: vendors.encodings, the portal's tick-list (["gsm7","ucs2"]).
//     An encoding NOT ticked is sent to this vendor as UCS-2, which carries
//     any text. Empty or missing = no restriction — a new vendor is saved
//     with [] and must not suddenly get everything as UCS-2.
//   - rules: protocol_config.encodings (ParseWireRules), the detailed
//     "send as / data_coding" setting. Where it names an encoding it wins
//     over the tick-list.
//
// The tick-list had been saved since 2026-08-21 and read by nothing (its
// column, config/0032, was added "inert until a follow-up code pass reads
// it"); this is that pass. UCS-2 itself cannot be turned away — nothing else
// carries its text — so an unticked UCS-2 is reported and kept.
func ParseVendorEncodings(rules, accepted json.RawMessage) (WireRules, []string) {
	w, problems := ParseWireRules(rules)
	explicit := map[string]bool{}
	var named map[string]json.RawMessage
	if json.Unmarshal(rules, &named) == nil {
		for k := range named {
			explicit[strings.ToLower(strings.TrimSpace(k))] = true
		}
	}
	s := strings.TrimSpace(string(accepted))
	if s == "" || s == "null" {
		return w, problems
	}
	var list []string
	if err := json.Unmarshal(accepted, &list); err != nil {
		return w, append(problems, "encodings list is not a list of names: "+err.Error())
	}
	ticked := map[string]bool{}
	for _, e := range list {
		e = strings.ToLower(strings.TrimSpace(e))
		if !knownEnc(e) {
			problems = append(problems, fmt.Sprintf("encodings list: %q is not gsm7, latin1 or ucs2", e))
			continue
		}
		ticked[e] = true
	}
	if len(ticked) == 0 {
		return w, problems // nothing usable ticked = no restriction
	}
	if !ticked[UCS2] {
		problems = append(problems, "encodings list leaves out ucs2 — Urdu, Arabic and emoji can only go as UCS-2, so they still will")
	}
	for _, e := range Encodings() {
		if ticked[e] || explicit[e] || e == UCS2 {
			continue
		}
		// Not ticked: send it as UCS-2, labelled as this vendor labels UCS-2.
		w[e] = WireRule{SendAs: UCS2, DataCoding: w[UCS2].DataCoding}
		if w[UCS2].SendAs != UCS2 {
			w[e] = WireRule{SendAs: UCS2, DataCoding: DefaultDataCoding(UCS2)}
		}
	}
	return w, problems
}
