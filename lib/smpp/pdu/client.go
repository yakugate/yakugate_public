package pdu

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

// Client-side codec for the SMPP vendor sender (P3): it binds OUT to a vendor
// SMSC, so it ENCODES requests (bind, submit_sm) and DECODES responses
// (bind_resp, submit_sm_resp) plus inbound deliver_sm (DLRs). [SCOPE P3]

const (
	DeliverSM     uint32 = 0x00000005
	DeliverSMResp uint32 = respBit | DeliverSM // 0x80000005
)

// BindOptions carries the vendor-configurable bind fields beyond
// system_id/password/mode. The zero value reproduces Yakugate's original
// hardcoded bind (interface_version 3.4, empty system_type/address_range,
// addr_ton/npi 0x00) so an unconfigured vendor is unaffected.
type BindOptions struct {
	SystemType       string
	InterfaceVersion byte // 0x00 defaults to 0x34 (SMPP 3.4) in BindPDU
	AddrTON          byte
	AddrNPI          byte
	AddressRange     string
}

// BindPDU encodes a bind_transmitter/receiver/transceiver REQUEST. mode is one
// of BindTransmitter/BindReceiver/BindTransceiver.
func BindPDU(mode uint32, systemID, password string, seq uint32, opt BindOptions) []byte {
	iv := opt.InterfaceVersion
	if iv == 0 {
		iv = 0x34
	}
	body := append(cstr(systemID), cstr(password)...)
	body = append(body, cstr(opt.SystemType)...)
	body = append(body, iv, opt.AddrTON, opt.AddrNPI)
	body = append(body, cstr(opt.AddressRange)...)
	return encode(mode, 0, seq, body)
}

// SubmitOptions carries the vendor-configurable submit_sm addressing fields.
// The zero value reproduces the original hardcoded submit (source ton/npi
// 0x00, dest ton/npi international 0x01).
type SubmitOptions struct {
	SourceTON, SourceNPI byte
	DestTON              byte
	DestNPI              byte
	// ValidityPeriod is the submit_sm validity_period, already formatted
	// (RelativeValidity / AbsoluteValidity). "" = the SMSC's own default.
	ValidityPeriod string
}

// DefaultSubmitOptions is the pre-existing hardcoded addressing (dest
// ton/npi=1,1 "international"; source left at 0,0) — the fallback whenever a
// vendor has no explicit override configured.
var DefaultSubmitOptions = SubmitOptions{DestTON: 1, DestNPI: 1}

// maxRelative is the longest relative validity written: the DD field holds
// at most 99 days, and the months/years fields are left at zero, because
// SMSCs disagree about how long a "month" is.
const maxRelative = 99*24*time.Hour + 23*time.Hour + 59*time.Minute + 59*time.Second

// RelativeValidity formats d as an SMPP 3.4 relative time, "YYMMDDhhmmss000R"
// (§7.1.1): how long the SMSC may keep trying. Under a second counts as one
// second; over 99 days is capped there.
func RelativeValidity(d time.Duration) string {
	if d < time.Second {
		d = time.Second
	}
	if d > maxRelative {
		d = maxRelative
	}
	secs := int64(d / time.Second)
	days, secs := secs/86400, secs%86400
	h, secs := secs/3600, secs%3600
	m, s := secs/60, secs%60
	return fmt.Sprintf("0000%02d%02d%02d%02d000R", days, h, m, s)
}

// AbsoluteValidity formats t as an SMPP 3.4 absolute time in UTC,
// "YYMMDDhhmmsstnnp" with t=tenths, nn=quarter hours from UTC (00), p=+.
func AbsoluteValidity(t time.Time) string {
	u := t.UTC()
	return fmt.Sprintf("%s%d00+", u.Format("060102150405"), u.Nanosecond()/100000000)
}

// SAR carries the TLV alternative to a UDH concatenation header (sar_msg_ref_num
// 0x020C / sar_total_segments 0x020E / sar_segment_seqnum 0x020F) — used
// instead of a UDH when a vendor's multipart_mode is configured as "tlv".
type SAR struct {
	Ref, Total, Seq int
}

// SubmitSMPDU encodes a submit_sm REQUEST toward the vendor. registeredDelivery
// !=0 asks the SMSC for a delivery receipt (DLR). sar is nil unless the vendor
// uses TLV-based concatenation instead of a UDH.
func SubmitSMPDU(src, dst string, short []byte, dataCoding, esmClass, priority, registeredDelivery byte, seq uint32, opt SubmitOptions, sar *SAR) []byte {
	body := cstr("") // service_type
	body = append(body, opt.SourceTON, opt.SourceNPI)
	body = append(body, cstr(src)...)
	body = append(body, opt.DestTON, opt.DestNPI)
	body = append(body, cstr(dst)...)
	body = append(body, esmClass, 0, priority)                // esm_class, protocol_id, priority_flag
	body = append(body, cstr("")...)                          // schedule_delivery_time
	body = append(body, cstr(opt.ValidityPeriod)...)          // validity_period
	body = append(body, registeredDelivery, 0, dataCoding, 0) // reg_delivery, replace, data_coding, sm_default
	if len(short) > 254 {
		short = short[:254]
	}
	body = append(body, byte(len(short)))
	body = append(body, short...)
	if sar != nil {
		body = appendTLV(body, 0x020C, []byte{byte(sar.Ref >> 8), byte(sar.Ref)})
		body = appendTLV(body, 0x020E, []byte{byte(sar.Total)})
		body = appendTLV(body, 0x020F, []byte{byte(sar.Seq)})
	}
	return encode(SubmitSM, 0, seq, body)
}

// SubmitSMPayloadPDU encodes a submit_sm carrying the WHOLE text in the
// message_payload TLV (0x0424) with an empty short_message — how a vendor set
// to multipart_mode "payload" gets a long message: one PDU, no UDH, no SAR,
// and the SMSC splits it for the handset (2026-09-21). SMPP 3.4 only.
func SubmitSMPayloadPDU(src, dst string, payload []byte, dataCoding, priority, registeredDelivery byte, seq uint32, opt SubmitOptions) []byte {
	body := cstr("") // service_type
	body = append(body, opt.SourceTON, opt.SourceNPI)
	body = append(body, cstr(src)...)
	body = append(body, opt.DestTON, opt.DestNPI)
	body = append(body, cstr(dst)...)
	body = append(body, 0, 0, priority)                       // esm_class, protocol_id, priority_flag
	body = append(body, cstr("")...)                          // schedule_delivery_time
	body = append(body, cstr(opt.ValidityPeriod)...)          // validity_period
	body = append(body, registeredDelivery, 0, dataCoding, 0) // reg_delivery, replace, data_coding, sm_default
	body = append(body, 0)                                    // sm_length = 0: the text rides the TLV
	body = appendTLV(body, 0x0424, payload)
	return encode(SubmitSM, 0, seq, body)
}

// appendTLV appends one optional-parameter TLV (tag, length, value) to body.
func appendTLV(body []byte, tag uint16, val []byte) []byte {
	var t [4]byte
	binary.BigEndian.PutUint16(t[0:2], tag)
	binary.BigEndian.PutUint16(t[2:4], uint16(len(val)))
	body = append(body, t[:]...)
	return append(body, val...)
}

// DecodeSubmitSMResp returns the vendor-assigned message_id from a submit_sm_resp.
func DecodeSubmitSMResp(body []byte) string {
	i := 0
	id, _ := readCStr(body, &i)
	return id
}

// DeliverSMRespPDU acknowledges an inbound deliver_sm (empty message_id body).
func DeliverSMRespPDU(seq uint32) []byte { return encode(DeliverSMResp, ESME_ROK, seq, cstr("")) }

// DeliverSMRespStatusPDU answers an inbound deliver_sm with a status:
// ESME_ROK once what it carried is kept, ESME_RX_T_APPN when it could not be.
func DeliverSMRespStatusPDU(status, seq uint32) []byte {
	return encode(DeliverSMResp, status, seq, cstr(""))
}

// DeliverSMPDU encodes a deliver_sm to PUSH toward a peer (server->client DLR, or
// client->vendor). esm_class 0x04 marks a delivery receipt. [SCOPE P5 smsc leg]
// DeliverSMPDU builds a deliver_sm. dataCoding describes `short` and MUST match
// how it was encoded (see EncodeShortMessage): a UCS-2 payload announced as
// data_coding=0 renders as rubbish on the handset. It was hardcoded to 0 here
// until 2026-08-31 (P6-B5) — harmless for the ASCII delivery receipts that
// share this builder, wrong for any non-GSM MO body.
func DeliverSMPDU(seq uint32, src, dst string, esmClass, dataCoding byte, short []byte) []byte {
	if len(short) > 254 {
		short = short[:254]
	}
	body := cstr("")          // service_type
	body = append(body, 0, 0) // source_addr_ton, npi
	body = append(body, cstr(src)...)
	body = append(body, 1, 1) // dest_addr_ton, npi
	body = append(body, cstr(dst)...)
	body = append(body, esmClass, 0, 0)   // esm_class, protocol_id, priority
	body = append(body, cstr("")...)      // schedule
	body = append(body, cstr("")...)      // validity
	body = append(body, 0, 0)             // reg_delivery, replace_if_present
	body = append(body, dataCoding, 0)    // data_coding, sm_default_msg_id
	body = append(body, byte(len(short))) // sm_length
	body = append(body, short...)
	return encode(DeliverSM, ESME_ROK, seq, body)
}

// ReceiptAddr is one address on a receipt deliver_sm, with its TON/NPI.
type ReceiptAddr struct {
	Addr     string
	TON, NPI byte
}

// AddrFor classifies an address for a PDU we originate: digits are an
// international number (TON 1, NPI 1), anything else an alphanumeric sender
// (TON 5, NPI 0), and empty is SMPP's unknown "0" (TON 0, NPI 0).
func AddrFor(a string) ReceiptAddr {
	if a == "" {
		return ReceiptAddr{Addr: "0"}
	}
	for _, c := range a {
		if c < '0' || c > '9' {
			return ReceiptAddr{Addr: a, TON: 5, NPI: 0}
		}
	}
	return ReceiptAddr{Addr: a, TON: 1, NPI: 1}
}

// DeliverReceiptPDU encodes a delivery receipt for a client (SMPP 3.4 §4.6.1,
// Appendix B): esm_class 0x04, the receipt text in short_message, and the two
// TLVs a client can match on without parsing the text — receipted_message_id
// (0x001E, our message id) and message_state (0x0427, one byte). src is the
// handset (the original destination), dst the original sender.
func DeliverReceiptPDU(seq uint32, src, dst ReceiptAddr, text []byte, receiptedID string, state byte) []byte {
	if len(text) > 254 {
		text = text[:254]
	}
	body := cstr("")                      // service_type
	body = append(body, src.TON, src.NPI) // source_addr_ton, npi
	body = append(body, cstr(src.Addr)...)
	body = append(body, dst.TON, dst.NPI) // dest_addr_ton, npi
	body = append(body, cstr(dst.Addr)...)
	body = append(body, 0x04, 0, 0)      // esm_class = MC delivery receipt, protocol_id, priority
	body = append(body, cstr("")...)     // schedule
	body = append(body, cstr("")...)     // validity
	body = append(body, 0, 0)            // reg_delivery, replace_if_present
	body = append(body, 0x00, 0)         // data_coding 0: text is the GSM alphabet (or packed), sm_default_msg_id
	body = append(body, byte(len(text))) // sm_length
	body = append(body, text...)
	id := cstr(receiptedID)
	body = append(body, 0x00, 0x1E, byte(len(id)>>8), byte(len(id)))
	body = append(body, id...)
	body = append(body, 0x04, 0x27, 0x00, 0x01, state)
	return encode(DeliverSM, ESME_ROK, seq, body)
}

// DeliverSMWithPayloadPDU encodes a deliver_sm carrying its body in the
// message_payload TLV (0x0424) rather than short_message, with an empty
// short_message field — the same shape a real vendor uses to deliver an
// inbound MO longer than 254 octets in one PDU. Unlike DeliverSMPDU, payload
// is NOT capped: message_payload has no 254-byte limit, which is exactly why
// a vendor prefers it for a long message instead of splitting into UDH parts
// itself (MessageBytes() on the receiving side already prefers this TLV over
// short_message when both could be present). Used by test harnesses that need
// to exercise P6-B6 (MO multipart delivery to a bound client) without a real
// vendor SMSC.
func DeliverSMWithPayloadPDU(seq uint32, src, dst string, esmClass, dataCoding byte, payload []byte) []byte {
	body := cstr("")          // service_type
	body = append(body, 0, 0) // source_addr_ton, npi
	body = append(body, cstr(src)...)
	body = append(body, 1, 1) // dest_addr_ton, npi
	body = append(body, cstr(dst)...)
	body = append(body, esmClass, 0, 0) // esm_class, protocol_id, priority
	body = append(body, cstr("")...)    // schedule
	body = append(body, cstr("")...)    // validity
	body = append(body, 0, 0)           // reg_delivery, replace_if_present
	body = append(body, dataCoding, 0)  // data_coding, sm_default_msg_id
	body = append(body, 0)              // sm_length = 0 — the real body rides the TLV below
	body = appendTLV(body, 0x0424, payload)
	return encode(DeliverSM, ESME_ROK, seq, body)
}

// Receipt builds a standard SMPP delivery-receipt short_message for a message id
// and status token (DELIVRD|UNDELIV|EXPIRED|...).
func Receipt(msgID, stat string) []byte {
	return []byte("id:" + msgID + " sub:001 dlvrd:001 submit date:0000000000 done date:0000000000 stat:" +
		stat + " err:000 text:")
}

// DLR is a parsed delivery receipt from a deliver_sm short_message.
type DLR struct {
	VendorMsgID string // "id:" — the SMSC message id we stored at submit time
	Stat        string // "stat:" — DELIVRD | UNDELIV | EXPIRED | ...
	Err         string // "err:"
	// From the deliver_sm itself (2026-10-14, Saleem: "field first, text
	// later"): FieldID is the receipted_message_id TLV, State the
	// message_state TLV (0 = not sent), Number the handset (source_addr —
	// a receipt comes FROM the number the message went to).
	FieldID string
	State   byte
	Number  string
	// Text is the receipt text the fields were read from: the octets as
	// they came, or unpacked when the vendor packed them.
	Text string
}

// ID is the vendor's message id for the receipt: the receipted_message_id
// field when the vendor sent it, else the id in the text. Other is the
// second one when both are there and differ.
func (d *DLR) ID() (id, other string) {
	if d.FieldID == "" {
		return d.VendorMsgID, ""
	}
	if d.VendorMsgID == d.FieldID {
		return d.FieldID, ""
	}
	return d.FieldID, d.VendorMsgID
}

// Word is the receipt's status word: the text's stat, else the word for the
// message_state field.
func (d *DLR) Word() string {
	if d.Stat != "" {
		return d.Stat
	}
	return StateWord(d.State)
}

// StateWord is SMPP 3.4's word for a message_state value (5.2.28); "" for 0
// and for a value it does not name.
func StateWord(state byte) string {
	switch state {
	case 1:
		return "ENROUTE"
	case 2:
		return "DELIVRD"
	case 3:
		return "EXPIRED"
	case 4:
		return "DELETED"
	case 5:
		return "UNDELIV"
	case 6:
		return "ACCEPTD"
	case 7:
		return "UNKNOWN"
	case 8:
		return "REJECTD"
	}
	return ""
}

// IsReceipt reports the esm_class MC-delivery-receipt bit (0x04) on a deliver_sm.
func IsReceipt(esmClass byte) bool { return esmClass&0x3C != 0 }

// DecodeDeliverSM decodes an inbound deliver_sm (same wire shape as submit_sm) and,
// when it is a delivery receipt, parses the DLR key:value fields (never by
// position — pitfall #? in domain/dlr-states). ok=false if it isn't a receipt.
func DecodeDeliverSM(body []byte) (*SubmitSMBody, *DLR, error) {
	b, err := DecodeSubmitSM(body)
	if err != nil {
		return nil, nil, err
	}
	if !IsReceipt(b.ESMClass) {
		return b, nil, nil // an inbound MO message, not a receipt
	}
	text := string(b.MessageBytes())
	d := parseDLR(text)
	if d.VendorMsgID == "" && d.Stat == "" && isGSM7Coding(b.DataCoding) {
		// The receipt's fields (SMPP 3.4 Appendix B) are letters, digits,
		// ':' and ' ', the same octets in ASCII and in the GSM alphabet one
		// septet an octet, so they are read as they come. A vendor that
		// PACKS its receipt text under data_coding 0 is read unpacked when
		// no id: and no stat: could be read as it came (19 Oct 2026). The
		// fields decide, not a guess about the octets: a text of the other
		// form never yields them.
		un := UnpackGSM7(b.MessageBytes(), 0)
		if p := parseDLR(un); p.VendorMsgID != "" || p.Stat != "" {
			text, d = un, p
		}
	}
	d.Text = text
	d.FieldID, d.State, d.Number = b.ReceiptedMessageID, b.MessageState, b.SourceAddr
	return b, d, nil
}

// parseDLR extracts id/stat/err from a standard receipt string by KEY, tolerating
// arbitrary field order and spacing.
func parseDLR(s string) *DLR {
	d := &DLR{}
	for _, tok := range strings.Fields(s) {
		k, v, ok := strings.Cut(tok, ":")
		if !ok {
			continue
		}
		switch strings.ToLower(k) {
		case "id":
			d.VendorMsgID = v
		case "stat":
			d.Stat = v
		case "err":
			d.Err = v
		}
	}
	return d
}
