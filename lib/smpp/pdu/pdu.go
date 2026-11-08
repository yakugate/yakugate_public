// Package pdu implements the SMPP v3.4 PDU wire format used by the SMPP edges
// (client-facing smpp-server P2; vendor-facing smpp-vendor P3). It encodes and
// decodes the 16-byte header + bodies for the subset Process 2 needs: binds,
// submit_sm, enquire_link, unbind, generic_nack. Field layouts follow
// products_docs/engineering/processes/process-2/smpp-3.4-reference.docx (reconcile
// against the official SMPP 3.4 PDF, Stage 0.4). All integers are big-endian;
// C-Octet strings are NUL-terminated.
package pdu

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// Command IDs.
const (
	BindReceiver     uint32 = 0x00000001
	BindTransmitter  uint32 = 0x00000002
	SubmitSM         uint32 = 0x00000004
	Unbind           uint32 = 0x00000006
	BindTransceiver  uint32 = 0x00000009
	EnquireLink      uint32 = 0x00000015
	SubmitMulti      uint32 = 0x00000021
	GenericNack      uint32 = 0x80000000
	respBit          uint32 = 0x80000000
	BindReceiverResp uint32 = respBit | BindReceiver
	BindTxResp       uint32 = respBit | BindTransmitter
	BindTrxResp      uint32 = respBit | BindTransceiver
	SubmitSMResp     uint32 = respBit | SubmitSM
	UnbindResp       uint32 = respBit | Unbind
	EnquireLinkResp  uint32 = respBit | EnquireLink
)

// command_status values (subset). Vendor-specific 0x400+ per process-2 scope §9.
const (
	ESME_ROK         uint32 = 0x00000000
	ESME_RINVMSGLEN  uint32 = 0x00000001
	ESME_RINVCMDLEN  uint32 = 0x00000002
	ESME_RINVCMDID   uint32 = 0x00000003
	ESME_RINVBNDSTS  uint32 = 0x00000004
	ESME_RALYBND     uint32 = 0x00000005
	ESME_RINVSRCADR  uint32 = 0x0000000A
	ESME_RINVDSTADR  uint32 = 0x0000000B
	ESME_RBINDFAIL   uint32 = 0x0000000D
	ESME_RINVPASWD   uint32 = 0x0000000E
	ESME_RINVSYSID   uint32 = 0x0000000F
	ESME_RMSGQFUL    uint32 = 0x00000014
	ESME_RSUBMITFAIL uint32 = 0x00000045
	ESME_RTHROTTLED  uint32 = 0x00000058
	ESME_RSYSERR     uint32 = 0x00000008
	// ESME_RX_T_APPN is SMPP 3.4's "ESME receiver temporary app error": the
	// answer to a deliver_sm we could not keep just now, which the SMSC is to
	// send again.
	ESME_RX_T_APPN uint32 = 0x00000064
	// ESME_RINVDCS is SMPP 5.0's "invalid data coding scheme": the answer to a
	// submit whose data_coding names 8-bit data, which the gateway does not
	// carry (it carries text).
	ESME_RINVDCS uint32 = 0x00000104
	// vendor-specific (Yakugate)
	VNotWhitelisted   uint32 = 0x00000400
	VCountryBlocked   uint32 = 0x00000401
	VInsufficientBal  uint32 = 0x00000402
	VQuotaExhausted   uint32 = 0x00000403
	VTemplateMismatch uint32 = 0x00000404
	VSpam             uint32 = 0x00000405
	VNoRoute          uint32 = 0x00000406
	VQuietHours       uint32 = 0x00000407
	VIdempotencyConf  uint32 = 0x00000408
	VAccountDisabled  uint32 = 0x00000409 // account/company disabled at submit time
	VIPNotAllowed     uint32 = 0x0000040A // source IP outside the account's allowlist
	VHLRUnresolved    uint32 = 0x0000040B // HLR could not name the operator and the account's policy is 'reject' (B-11)
	VLossPrevented    uint32 = 0x0000040C // the chosen route would sell below its buy cost and the company opted into loss detection (S-03)
	VDestCapReached   uint32 = 0x0000040D // too many messages to THIS destination in the account's window (config/0096) — distinct from VQuotaExhausted, which is an account/company allowance
	// VWrongDeployment: this account is not served by the gateway it bound
	// to (config/0122). Its own code because the right reaction is unlike
	// every other refusal — not "slow down" or "fix the message", but
	// "connect somewhere else". Without it this fell to ESME_RSUBMITFAIL,
	// which tells an ESME nothing and reads as our fault.
	VWrongDeployment uint32 = 0x0000040E
)

// statusNames renders a command_status as the name the SMPP spec (and every
// vendor's support desk) uses. Added for the Process 13 flow capture, where a
// bare 0x58 in a diagnostic file is a lookup an operator should not have to
// do — but it is the codec's vocabulary, so it belongs here rather than in a
// process.
var statusNames = map[uint32]string{
	ESME_ROK: "ESME_ROK", ESME_RINVMSGLEN: "ESME_RINVMSGLEN", ESME_RINVCMDLEN: "ESME_RINVCMDLEN",
	ESME_RINVCMDID: "ESME_RINVCMDID", ESME_RINVBNDSTS: "ESME_RINVBNDSTS", ESME_RALYBND: "ESME_RALYBND",
	ESME_RINVSRCADR: "ESME_RINVSRCADR", ESME_RINVDSTADR: "ESME_RINVDSTADR", ESME_RBINDFAIL: "ESME_RBINDFAIL",
	ESME_RINVPASWD: "ESME_RINVPASWD", ESME_RINVSYSID: "ESME_RINVSYSID", ESME_RMSGQFUL: "ESME_RMSGQFUL",
	ESME_RSUBMITFAIL: "ESME_RSUBMITFAIL", ESME_RTHROTTLED: "ESME_RTHROTTLED", ESME_RSYSERR: "ESME_RSYSERR",
	ESME_RX_T_APPN: "ESME_RX_T_APPN", ESME_RINVDCS: "ESME_RINVDCS",
	VNotWhitelisted: "VNotWhitelisted", VCountryBlocked: "VCountryBlocked", VInsufficientBal: "VInsufficientBal",
	VQuotaExhausted: "VQuotaExhausted", VTemplateMismatch: "VTemplateMismatch", VSpam: "VSpam",
	VNoRoute: "VNoRoute", VQuietHours: "VQuietHours", VIdempotencyConf: "VIdempotencyConf",
	VAccountDisabled: "VAccountDisabled", VIPNotAllowed: "VIPNotAllowed", VHLRUnresolved: "VHLRUnresolved",
	VLossPrevented: "VLossPrevented", VDestCapReached: "VDestCapReached",
	VWrongDeployment: "VWrongDeployment",
}

// StatusName is the spec name for a command_status, falling back to hex for
// anything outside the subset we define — never an empty string, because the
// caller is writing a diagnostic line and a blank there is worse than a number.
func StatusName(status uint32) string {
	if n, ok := statusNames[status]; ok {
		return n
	}
	return fmt.Sprintf("0x%08X", status)
}

const HeaderLen = 16
const maxPDU = 1 << 20 // 1 MB sanity cap

// Header is the 16-byte SMPP PDU header.
type Header struct {
	CommandLength  uint32
	CommandID      uint32
	CommandStatus  uint32
	SequenceNumber uint32
}

// PDU is a header plus its raw (undecoded) body.
type PDU struct {
	Header Header
	Body   []byte
}

var ErrShort = errors.New("smpp/pdu: short read")

// BadLengthError is a command_length no PDU can have: under the 16 bytes of
// the header, or over the 1 MB cap. The stream cannot be read past it, so the
// reader has one move left: say so (generic_nack, ESME_RINVCMDLEN) and close
// (pitfall 12). Sequence is the header's sequence_number, for that answer.
// It still matches ErrShort, which is what Read returned for this before.
type BadLengthError struct{ Length, Sequence uint32 }

func (e *BadLengthError) Error() string {
	return fmt.Sprintf("smpp/pdu: command_length %d is not a PDU's", e.Length)
}

func (e *BadLengthError) Is(target error) bool { return target == ErrShort }

// Read reads one full PDU from r.
func Read(r *bufio.Reader) (*PDU, error) {
	var hb [HeaderLen]byte
	if _, err := io.ReadFull(r, hb[:]); err != nil {
		return nil, err
	}
	h := Header{
		CommandLength:  binary.BigEndian.Uint32(hb[0:4]),
		CommandID:      binary.BigEndian.Uint32(hb[4:8]),
		CommandStatus:  binary.BigEndian.Uint32(hb[8:12]),
		SequenceNumber: binary.BigEndian.Uint32(hb[12:16]),
	}
	if h.CommandLength < HeaderLen || h.CommandLength > maxPDU {
		return nil, &BadLengthError{Length: h.CommandLength, Sequence: h.SequenceNumber}
	}
	body := make([]byte, h.CommandLength-HeaderLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return &PDU{Header: h, Body: body}, nil
}

// encode assembles a PDU from parts, computing command_length.
func encode(commandID, status, seq uint32, body []byte) []byte {
	buf := make([]byte, HeaderLen+len(body))
	binary.BigEndian.PutUint32(buf[0:4], uint32(HeaderLen+len(body)))
	binary.BigEndian.PutUint32(buf[4:8], commandID)
	binary.BigEndian.PutUint32(buf[8:12], status)
	binary.BigEndian.PutUint32(buf[12:16], seq)
	copy(buf[HeaderLen:], body)
	return buf
}

// ---- C-Octet string helpers ----

func cstr(s string) []byte { return append([]byte(s), 0) }

// readCStr reads a NUL-terminated string from b starting at *i, advancing *i.
func readCStr(b []byte, i *int) (string, error) {
	start := *i
	for *i < len(b) {
		if b[*i] == 0 {
			s := string(b[start:*i])
			*i++
			return s, nil
		}
		*i++
	}
	return "", ErrShort
}

func readByte(b []byte, i *int) (byte, error) {
	if *i >= len(b) {
		return 0, ErrShort
	}
	v := b[*i]
	*i++
	return v, nil
}

// ---- Bind ----

// Bind is a decoded bind_* body.
type Bind struct {
	SystemID         string
	Password         string
	SystemType       string
	InterfaceVersion byte
	AddrTON          byte
	AddrNPI          byte
	AddressRange     string
}

// DecodeBind parses a bind_transmitter/receiver/transceiver body.
func DecodeBind(body []byte) (*Bind, error) {
	i := 0
	var b Bind
	var err error
	if b.SystemID, err = readCStr(body, &i); err != nil {
		return nil, err
	}
	if b.Password, err = readCStr(body, &i); err != nil {
		return nil, err
	}
	if b.SystemType, err = readCStr(body, &i); err != nil {
		return nil, err
	}
	if b.InterfaceVersion, err = readByte(body, &i); err != nil {
		return nil, err
	}
	if b.AddrTON, err = readByte(body, &i); err != nil {
		return nil, err
	}
	if b.AddrNPI, err = readByte(body, &i); err != nil {
		return nil, err
	}
	b.AddressRange, _ = readCStr(body, &i) // optional/last
	return &b, nil
}

// BindResp encodes a bind_*_resp (respID = BindTxResp/BindReceiverResp/BindTrxResp).
func BindResp(respID, status, seq uint32, systemID string) []byte {
	var body []byte
	if status == ESME_ROK {
		body = cstr(systemID)
	}
	return encode(respID, status, seq, body)
}

// ---- submit_sm ----

// SubmitSMBody is a decoded submit_sm.
type SubmitSMBody struct {
	ServiceType        string
	SourceAddrTON      byte
	SourceAddrNPI      byte
	SourceAddr         string
	DestAddrTON        byte
	DestAddrNPI        byte
	DestinationAddr    string
	ESMClass           byte
	ProtocolID         byte
	PriorityFlag       byte
	ScheduleDelivery   string
	ValidityPeriod     string
	RegisteredDelivery byte
	ReplaceIfPresent   byte
	DataCoding         byte
	SMDefaultMsgID     byte
	ShortMessage       []byte
	MessagePayload     []byte // TLV 0x0424 if present
	// UserMessageRef is TLV 0x0204 user_message_reference — client
	// idempotency key. Printable ASCII passes through unchanged; a binary
	// value (see printableRef) arrives here already hex-encoded with a
	// "hex:" prefix, never as raw bytes.
	UserMessageRef string
	// SAR TLVs (0x020C/0x020E/0x020F) — the TLV alternative to UDH
	// concatenation (smpp-3.4-reference §5). Read by Concat() as a fallback.
	SarRef   int
	SarTotal int
	SarSeq   int
	// On a deliver_sm receipt: TLV 0x001E receipted_message_id (the message
	// id the vendor gave at submit) and 0x0427 message_state (0 = not sent).
	ReceiptedMessageID string
	MessageState       byte
}

// DecodeSubmitSM parses a submit_sm body (mandatory params + optional TLVs).
func DecodeSubmitSM(body []byte) (*SubmitSMBody, error) {
	i := 0
	var s SubmitSMBody
	var err error
	get := func(dst *string) bool { *dst, err = readCStr(body, &i); return err == nil }
	getb := func(dst *byte) bool { *dst, err = readByte(body, &i); return err == nil }
	if !get(&s.ServiceType) || !getb(&s.SourceAddrTON) || !getb(&s.SourceAddrNPI) || !get(&s.SourceAddr) {
		return nil, err
	}
	if !getb(&s.DestAddrTON) || !getb(&s.DestAddrNPI) || !get(&s.DestinationAddr) {
		return nil, err
	}
	if !getb(&s.ESMClass) || !getb(&s.ProtocolID) || !getb(&s.PriorityFlag) {
		return nil, err
	}
	if !get(&s.ScheduleDelivery) || !get(&s.ValidityPeriod) {
		return nil, err
	}
	if !getb(&s.RegisteredDelivery) || !getb(&s.ReplaceIfPresent) || !getb(&s.DataCoding) || !getb(&s.SMDefaultMsgID) {
		return nil, err
	}
	var smLen byte
	if !getb(&smLen) {
		return nil, err
	}
	if i+int(smLen) > len(body) {
		return nil, ErrShort
	}
	s.ShortMessage = append([]byte(nil), body[i:i+int(smLen)]...)
	i += int(smLen)
	// optional TLVs
	for i+4 <= len(body) {
		tag := binary.BigEndian.Uint16(body[i : i+2])
		length := int(binary.BigEndian.Uint16(body[i+2 : i+4]))
		i += 4
		if i+length > len(body) {
			break
		}
		val := body[i : i+length]
		i += length
		switch tag {
		case 0x0424: // message_payload
			s.MessagePayload = append([]byte(nil), val...)
		case 0x0204: // user_message_reference — carried as the idempotency key
			s.UserMessageRef = printableRef(val)
		case 0x020C: // sar_msg_ref_num
			if len(val) == 2 {
				s.SarRef = int(val[0])<<8 | int(val[1])
			}
		case 0x020E: // sar_total_segments
			if len(val) == 1 {
				s.SarTotal = int(val[0])
			}
		case 0x020F: // sar_segment_seqnum
			if len(val) == 1 {
				s.SarSeq = int(val[0])
			}
		case 0x001E: // receipted_message_id (a C-string)
			s.ReceiptedMessageID = string(bytes.TrimRight(val, "\x00"))
		case 0x0427: // message_state
			if len(val) == 1 {
				s.MessageState = val[0]
			}
		}
	}
	return &s, nil
}

// printableRef returns raw as text verbatim when every byte is printable
// ASCII (0x20-0x7E) — the vast majority of real ESMEs, zero change from
// before this existed. Anything else is hex-encoded with a "hex:" prefix
// instead of passed through as a raw byte-string, which used to fail two
// different ways depending on WHICH bytes: a low control byte (0x00-0x1F —
// how a client sending user_message_reference as the 2-octet unsigned
// integer the SMPP 3.4 spec actually defines it as, e.g. ref 1 = 0x00 0x01,
// would normally look) tripped the pipeline's own control-character reject —
// a clean 400, at least honest. A high non-ASCII byte sailed straight past
// that check and then broke at the database insert: idempotency_key is a
// strict-mode utf8mb4 VARCHAR that refuses invalid UTF-8 outright (confirmed
// live: MySQL error 1366, "Incorrect string value"), turning a client's
// otherwise-valid submit into an opaque server-side failure instead of a
// clean rejection. Hex is always valid, always fits under the 64-char cap
// stage_validate.go already enforces for anything genuinely oversized, and
// is losslessly reversible — strip the prefix and decode. [P2, PO 2026-09-03]
func printableRef(raw []byte) string {
	for _, b := range raw {
		if b < 0x20 || b > 0x7e {
			return "hex:" + hex.EncodeToString(raw)
		}
	}
	return string(raw)
}

// SubmitSMRespPDU encodes submit_sm_resp with a message_id.
func SubmitSMRespPDU(status, seq uint32, messageID string) []byte {
	var body []byte
	if status == ESME_ROK {
		body = cstr(messageID)
	}
	return encode(SubmitSMResp, status, seq, body)
}

// Simple bodyless responses.
func EnquireLinkRespPDU(seq uint32) []byte { return encode(EnquireLinkResp, ESME_ROK, seq, nil) }
func UnbindRespPDU(seq uint32) []byte      { return encode(UnbindResp, ESME_ROK, seq, nil) }

// UnbindPDU builds an outbound unbind request (server->client graceful shutdown).
func UnbindPDU(seq uint32) []byte { return encode(Unbind, ESME_ROK, seq, nil) }
func GenericNackPDU(status, seq uint32) []byte {
	return encode(GenericNack, status, seq, nil)
}

// EnquireLinkPDU builds an outbound enquire_link (server→client keepalive).
func EnquireLinkPDU(seq uint32) []byte { return encode(EnquireLink, ESME_ROK, seq, nil) }

// Body returns the message text: message_payload TLV if set, else short_message.
func (s *SubmitSMBody) MessageBytes() []byte {
	if len(s.MessagePayload) > 0 {
		return s.MessagePayload
	}
	return s.ShortMessage
}

// UDHPresent reports the esm_class UDHI bit (0x40) — concatenation indicator.
func (s *SubmitSMBody) UDHPresent() bool { return s.ESMClass&0x40 != 0 }

// Concat parses a User Data Header for a concatenation IE and returns the group
// (ref, total, seq) plus the payload with the UDH removed. It handles the 8-bit
// reference IE (0x00, len 3) and the 16-bit reference IE (0x08, len 4). If the
// UDHI bit is unset or no concat IE is found, ok is false and the original bytes
// are returned unchanged. [SCOPE P2-CONCAT]
func (s *SubmitSMBody) Concat() (ref, total, seq int, rest []byte, ok bool) {
	msg := s.MessageBytes()
	if !s.UDHPresent() || len(msg) == 0 {
		// TLV fallback: SAR concatenation (0x020C/0x020E/0x020F) carries the
		// group in optional TLVs instead of a UDH — same semantics, no header
		// bytes to strip.
		if s.SarTotal > 0 && s.SarSeq > 0 {
			return s.SarRef, s.SarTotal, s.SarSeq, msg, true
		}
		return 0, 0, 0, msg, false
	}
	udhl := int(msg[0])
	if udhl == 0 || 1+udhl > len(msg) {
		return 0, 0, 0, msg, false
	}
	udh := msg[1 : 1+udhl]
	rest = msg[1+udhl:] // payload after the whole UDH (encoding decode happens on this)
	// walk IEs: [IEI][IEIlen][data...]
	for i := 0; i+2 <= len(udh); {
		iei, ielen := udh[i], int(udh[i+1])
		i += 2
		if i+ielen > len(udh) {
			break
		}
		d := udh[i : i+ielen]
		i += ielen
		switch {
		case iei == 0x00 && ielen == 3: // 8-bit reference
			return int(d[0]), int(d[1]), int(d[2]), rest, true
		case iei == 0x08 && ielen == 4: // 16-bit reference
			return int(d[0])<<8 | int(d[1]), int(d[2]), int(d[3]), rest, true
		}
	}
	return 0, 0, 0, rest, false // UDH present but no concat IE; still strip the UDH
}
