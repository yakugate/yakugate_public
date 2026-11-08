package dlr

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
)

// Receipt is what the gateway tells a client about one of its messages: the
// facts, not a pre-rendered string. The queue pusher renders the text once per
// id it goes to — a long message sent in parts has several ids, and each
// receipt must quote its own (PO 2026-09-11).
//
// Until 2026-09-11 callers passed a ready-made string instead, which is how the
// vendor's raw receipt (with the VENDOR's message id in it) ended up in the
// customer's dlr_url body, and how the SMPP push came to send zero dates and a
// fixed err:000.
type Receipt struct {
	MsgID  string       // our id — the one the client holds
	Status model.Status // sent | delivered | failed | expired | rejected
	// ErrCode is the vendor's error code for this outcome, or the gateway's own
	// (for a refusal after the parts of a long message were acked, the SMPP
	// command_status it would have got). "" = none, rendered as 000.
	ErrCode string
	// Src/Dst are the original message's sender and destination. The SMPP push
	// swaps them onto the deliver_sm (source = the handset).
	Src, Dst string
	// DoneAt is when this state was reached; zero = now.
	DoneAt time.Time
	// BindSlot routes the receipt back down the bind the message came in on
	// (0 = any live receive-capable bind).
	BindSlot int
	// Stat is the receipt's own SMPP word, one of the eight (2026-09-23) —
	// what the vendor actually said, which can be finer than the status:
	// DELETED and REJECTD are both `failed`, ENROUTE and ACCEPTD both `sent`.
	// Empty means the gateway decided this receipt itself, and the word is
	// then derived from Status exactly as it always was.
	Stat Stat
	// Delay holds this receipt back before anything may deliver it. Only the
	// account's receipt policy sets it, to spread invented answers out
	// instead of resolving every message in the millisecond it was accepted
	// (internal/receiptpolicy). 0 = as soon as possible, which is every
	// receipt that came from a vendor.
	Delay time.Duration
	// Owner is the deployment that delivers this receipt to the client
	// (D-121): the queue owner of the original message, whose client is
	// connected there. "" = the shared receipt queue, any gateway delivers.
	Owner string
	// FactsKey is, on a progress receipt, the Redis key of its message's
	// receipt facts (FactsKey): a key gone means the message was closed, and
	// a progress receipt parked for another gateway is then not sent on
	// (internal/handoff). "" = not known.
	FactsKey string `json:",omitempty"`
}

// StatToken is the SMPP 3.4 receipt `stat:` word for a canonical status —
// the five-state view, kept for every caller that has only a status. Where
// the receipt's own word is known, pass it instead: see states.go, which
// carries all eight.
func StatToken(st model.Status) string { return string(StatFor(st)) }

// MessageState is the SMPP 3.4 message_state TLV (0x0427) value for a
// canonical status: ENROUTE 1, DELIVERED 2, EXPIRED 3, DELETED 4,
// UNDELIVERABLE 5, ACCEPTED 6, UNKNOWN 7, REJECTED 8. A receipt that knows
// its own word uses Stat.State() instead, which can also say DELETED.
func MessageState(st model.Status) byte { return StatFor(st).State() }

// receiptDate is SMPP 3.4 Appendix B's YYMMDDhhmm, in UTC.
const receiptDate = "0601021504"

// ReceiptText renders the standard SMPP 3.4 delivery receipt text (Appendix B)
// for one of our message ids:
//
//	id:<id> sub:001 dlvrd:001 submit date:2609111030 done date:2609111031 stat:DELIVRD err:000 text:
//
// The submit date is read from the id itself (a ULID carries its creation
// time, which is when we accepted the message); done is when the state was
// reached. dlvrd is 001 only for a delivered message. The `text:` field is
// left empty on purpose — it would copy the customer's message into every
// receipt, and an account on masked or encrypted bodies must not see it here.
func ReceiptText(id string, st model.Status, errCode string, done time.Time) string {
	if done.IsZero() {
		done = time.Now()
	}
	submit := done
	if at, ok := monitor.ULIDTime(id); ok {
		submit = at
	}
	dlvrd := "000"
	if st == model.StatusDelivered {
		dlvrd = "001"
	}
	return "id:" + id + " sub:001 dlvrd:" + dlvrd +
		" submit date:" + submit.UTC().Format(receiptDate) +
		" done date:" + done.UTC().Format(receiptDate) +
		" stat:" + StatToken(st) + " err:" + errField(errCode) + " text:"
}

// ReceiptTextStat renders the same receipt for a known SMPP word, so a
// vendor's DELETED reaches the client as DELETED and not as the UNDELIV its
// status would imply. An empty word falls back to the status.
func ReceiptTextStat(id string, st model.Status, stat Stat, errCode string, done time.Time) string {
	text := ReceiptText(id, st, errCode, done)
	if stat == "" || stat == StatFor(st) {
		return text
	}
	return strings.Replace(text, " stat:"+StatToken(st)+" ", " stat:"+string(stat)+" ", 1)
}

// IsReceiptTextFor reports whether text is our own rendered receipt for id —
// the smpp-server uses it to tell a row written by ReceiptText from one written
// before it existed (which carried a vendor's raw receipt instead).
func IsReceiptTextFor(text, id string) bool {
	return strings.HasPrefix(text, "id:"+id+" ")
}

// errField renders an error code for the receipt's `err:` field: 000 when
// there is none, otherwise the code without spaces (a space would end the
// field for every client that splits on it). The code is the exact one the
// record carries — SMPP_45, HTTP_403 (PO 2026-09-17) — so the cap only has
// to keep a vendor-specific SMPP status whole (SMPP_ plus up to 8 hex).
func errField(code string) string {
	code = strings.Join(strings.Fields(code), "")
	if code == "" {
		return "000"
	}
	if len(code) < 3 {
		code = strings.Repeat("0", 3-len(code)) + code
	}
	if len(code) > 13 {
		code = code[:13]
	}
	return code
}

// ReceiptSettings reads a vendor's receipt settings from its dlr_config
// (D-136, Saleem 2026-10-14: "it is configured per vendor … if vendor does not
// provide dlr there should not be any waiting at all"): receipts "no" = the
// vendor sends none, so nothing waits for one; wait_hours = how long a sent
// message waits for its receipt (0 = model.DefaultReceiptWaitHours). The
// Admin API and the console keep wait_hours to 1..720.
// FirstReceiptCloses reads dlr_config.first_receipt_closes: the vendor sends
// one final receipt per long message, on any one part, so the first part's
// final closes the whole message (Saleem 4 Nov 2026). Off when not set.
// Change with the Admin API's services/recordSettings.js and provision.
//
// true or 1 (the console's door writes a bool as 1 into JSON; the Admin
// API's writes true); anything else is off.
func FirstReceiptCloses(dlrConfigJSON string) bool {
	var c struct {
		On json.RawMessage `json:"first_receipt_closes"`
	}
	_ = json.Unmarshal([]byte(dlrConfigJSON), &c)
	v := strings.TrimSpace(string(c.On))
	return v == "true" || v == "1"
}

func ReceiptSettings(dlrConfigJSON string) (sendsNone bool, waitHours int) {
	var c struct {
		Receipts  string `json:"receipts"`
		WaitHours int    `json:"wait_hours"`
	}
	_ = json.Unmarshal([]byte(dlrConfigJSON), &c)
	if c.WaitHours < 0 || c.WaitHours > 720 {
		c.WaitHours = 0
	}
	return strings.EqualFold(strings.TrimSpace(c.Receipts), "no"), c.WaitHours
}
