package model

import (
	"strings"
	"time"
)

// Status is the lifecycle state of a message.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusSent      Status = "sent"
	StatusDelivered Status = "delivered"
	StatusFailed    Status = "failed"
	StatusExpired   Status = "expired"
	// StatusRejected is a submit refused by the accept pipeline — it never
	// entered a queue. Recorded so support and audit can see refused traffic.
	StatusRejected Status = "rejected"
)

// The billing models a message can be accepted under (Message.BillingModel).
const (
	BillingPrepaid  = "prepaid"
	BillingPostpaid = "postpaid"
)

// SplitReason separates a refusal reason of the form "CODE: sentence" into
// the code the reports group by and the sentence the sender saw. A bare code
// comes back with an empty sentence. The same rule the client response has
// always used; the records store the two apart since 2026-09-17 (I-18),
// because a reason_code carrying the sentence split one refusal across as
// many report buckets as there were wordings.
func SplitReason(reason string) (code, detail string) {
	if i := strings.Index(reason, ":"); i > 0 {
		return strings.TrimSpace(reason[:i]), strings.TrimSpace(reason[i+1:])
	}
	return strings.TrimSpace(reason), ""
}

// IsTerminal reports a final state: nothing further will happen to the message.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusDelivered, StatusFailed, StatusExpired, StatusRejected:
		return true
	}
	return false
}

// Transport is how a message leaves toward the vendor (egress). The mirror value
// on the client side — how the message was submitted — is Message.Ingress.
type Transport string

const (
	TransportSMPP Transport = "smpp"
	TransportHTTP Transport = "http"
)

// Direction distinguishes mobile-terminated (outbound, the MT path) from
// mobile-originated (inbound, the MO path / Process 6). Empty defaults to MT.
type Direction string

const (
	DirectionMT Direction = "MT"
	DirectionMO Direction = "MO"
)

// Priced is a record's price as its row holds it.
type Priced struct {
	Buy      *float64 `json:"b,omitempty"` // gross, in the buying card's currency
	Sell     *float64 `json:"s,omitempty"` // gross, in the selling card's currency
	Currency string   `json:"c,omitempty"` // the selling card's (the record's one currency column)
	Detail   string   `json:"d,omitempty"` // rate_cards: the card and rate of each side
	// Charge is what this record's money events took, and what its receipt
	// may still book (billing.ChargeMark with its note).
	Charge string `json:"g,omitempty"`
}

// Message is the canonical internal representation of an SMS (one recipient).
// [SCOPE P1-MODEL] canonical message model
type Message struct {
	ID          string // ULID, msg_id
	AccountID   int64
	Username    string
	Sender      string // sender id / mask
	Destination string
	Body        string
	Encoding    string // gsm7 | ucs2
	Gsm7        string // "true" forces GSM-7, "false" allows UCS-2, "" = auto-detect
	Channel     string // e.g. ATM, Marketing
	// Custom client pass-through parameters (published Hadal contract, S-06).
	// AP1/AP2 reach the message record only; APG1/APG2 additionally act as
	// grouping dimensions for reporting rollups. The engine never interprets
	// them — they are carried verbatim from submit to record.
	AP1      string
	AP2      string
	APG1     string
	APG2     string
	Segments int
	// SentParts is how many PDUs the vendor was actually sent, set by the
	// SMPP vendor role at egress (2026-09-21). It prices the VENDOR side of
	// the message — the client is always charged by Segments. 0 = not known
	// (an HTTP vendor, one call), so the vendor side uses Segments too.
	SentParts int
	// Concatenation group (SMPP UDH). Per-part model: each part is its own message
	// with its own ID; these link the parts of one long SMS so vendor egress can
	// re-emit the UDH and DLRs correlate. 0 = not concatenated (single-part). [SCOPE P2-CONCAT]
	ConcatRef   int // UDH reference number (shared across the group)
	ConcatTotal int // total parts in the group
	ConcatSeq   int // this part's sequence (1-based)
	// PartIDs are the message ids an SMPP client was given for each part of a
	// long message it sent in parts, in part order (part 1 first — which is
	// also this whole message's ID). Set when the parts are joined at ingress
	// (internal/concat.Assemble) so every one of those ids can get its receipt
	// (PO 2026-09-11). In memory only: the smpp-server saves the list in the
	// queue store for the receipt path, and nothing else carries it.
	PartIDs    []string `json:"-"`
	TemplateID int64
	Vendor     string // vendor cid (selects the per-vendor queue)
	VendorID   int64
	RouteID    int64 // routes.id of the winning route (0 = none; audit trail)
	// RouteLevel is the level of the rule the message is queued for: 0 =
	// Main, 1 = Backup 1, … SendRound is how many rounds (Main then every
	// backup) have already failed. TriedVendors are the vendor cids tried in
	// THIS round, so no vendor is offered the message twice in a round
	// (config/0131, queue/0035; Saleem 2026-09-30).
	RouteLevel   int
	SendRound    int
	TriedVendors []string  `json:",omitempty"`
	Transport    Transport // egress: how the message leaves toward the vendor (set by route stage)
	Ingress      Transport // ingress: how the client submitted it (http|smpp), set by the edge
	Direction    Direction // MT (outbound) or MO (inbound); "" = MT [SCOPE P6]
	MCC          string    // mobile country code (up to 3 digits); "" = unresolved
	MNC          string    // mobile network code (up to 3 digits); "" = unresolved
	// HLRSource records HOW the operator was resolved (GW-19, 2026-08-27):
	// "operator" (client-supplied, HLR skipped) | "db" (cache/shard hit) |
	// "dip" (live external lookup) | "miss" (consulted, not found) | "" (HLR
	// not consulted). Persisted on the MT record — the record IS the HLR log.
	HLRSource string
	HLRPorted bool
	// HLROperator and HLRCCC (6 Nov 2026): the operator's name and the
	// country calling code the lookup found (our HLR data, a live lookup or
	// the number range); "" = not known. Written on the lookup's log row.
	HLROperator string
	HLRCCC      string
	// HLRCall (6 Nov 2026, Saleem: "in hlr logs cant see request and
	// response of hlr hit"): the call a live lookup made to the HLR
	// provider, kept on the lookup's log row; nil when no call was made or
	// YAKU_HLR_CALL_LOG did not keep it. It rides to the record (the record
	// queue's task) but never on a vendor queue: the enqueue stage drops it.
	HLRCall *LookupCall `json:"hc,omitempty"`

	// The client's delivery-receipt preference, decoded from SMPP's
	// registered_delivery byte bits 0-1 (P2 ingress only; PO 2026-09-03,
	// full SMPP 3.4 semantics, no opt-in — every account, immediately).
	// HTTP has no per-message equivalent in the native V4 contract, so an
	// HTTP-submitted message takes the zero value on both fields — false and
	// false reproduces every pre-2026-09-03 message: a receipt, always.
	// Mutually exclusive by construction (see toMessage); NoReceipt wins if
	// both were somehow set.
	NoReceipt          bool // bits 0-1 == 00: the client asked for nothing at all
	FailureReceiptOnly bool // bits 0-1 == 10: only a failure, never DELIVRD

	// WantIntermediateReceipt is registered_delivery bit 4 (0x10) — the
	// client separately asked for the "sent to network" notice as well as the
	// final outcome. Per spec this defaults OFF, which is the OPPOSITE
	// polarity from NoReceipt/FailureReceiptOnly above: an HTTP message has
	// always gotten the intermediate notice unconditionally (there is no bit
	// to read, and that behaviour is out of scope here), so this field's zero
	// value would silently break HTTP if read on its own. Callers MUST gate
	// it behind `Ingress == TransportSMPP` — see the callers in
	// httpvendor/workings.go and smppvendor/workings.go — never on this field
	// alone. For SMPP: send unless the client explicitly asked (bit 4 set).
	WantIntermediateReceipt bool

	// BindSlot names the SMPP bind this message was submitted on, so its
	// receipt can go back down the same one (PO 2026-09-05: "if message came
	// from bind/session 1 dlr should also go to 1 if 9 then 9"). A slot is the
	// lowest free number for that account, assigned at bind time and released
	// when the connection closes — SMPP itself gives a bind no name, and a
	// per-connection id would stop matching the moment a client reconnected.
	//
	// 0 = no originating bind: an HTTP submit, or a row written before the
	// slot existed. 0, an unknown slot and a send-only (TX) bind all mean the
	// same thing to the delivery loop: use the account's live receive-capable
	// binds, taking turns.
	BindSlot int

	// MaskBody is the company's pii_masked policy, decided at accept time and
	// carried because the processes that write the record and handle receipts
	// have no reference cache to ask. It NEVER changes what goes to the
	// handset — only what is stored and shown back.
	MaskBody bool
	// KeepRejectBody says a REFUSED message's text may be written to the
	// reject log (hot/0057). Set only by the edge that records the refusal,
	// from the account's company privacy: plain or masked keeps it (masked
	// through MaskBody), encrypted does not, and neither does a refusal
	// before the account is known. Never read for an accepted message.
	KeepRejectBody bool

	// Told* is what the client was told about this message when that is not
	// what really happened — the account's receipt policy inventing an answer
	// the vendor never gave (internal/receiptpolicy, config/0119). It is set
	// by the vendor role BEFORE the record is written, so the truth
	// (Status/ErrorCode) and the client's version land in one insert and no
	// reader can see one without the other. In memory and on record_index
	// only; it never rides the queue.
	//
	// Empty, which is every message on a gateway with no policy, means the
	// client was told exactly what Status says.
	ToldStatus Status
	ToldErr    string
	ToldBy     string // the rule that decided: on_accepted, on_refused, submit_code…

	Status      Status
	Priority    int
	VendorMsgID string
	ErrorCode   string
	// ErrorText is the words behind ErrorCode when the message failed: the
	// vendor's answer to a refused send, or the gateway's own sentence. Kept
	// on the record (mt_.error_text, hot/0064) so the log can say why.
	ErrorText string
	// Rejection context — set only when Status is StatusRejected.
	// RequestInfo is pre-sanitized JSON (never carries credential headers).
	RejectReason string
	// SpamFlag records that a spam rule with action='flag' matched at accept
	// (config/0098). The message was SENT anyway — this is observe mode, so
	// the customer never learns a rule fired. It rides the queue row because
	// the record is written later by a vendor process that has no reference
	// cache (the same "carry the flag" pattern as NoReceipt and MaskBody).
	SpamFlag string
	// SenderBy says who chose the sender the message goes out with, when it
	// was not the client (Saleem 2026-09-29: two defaults are hard to debug
	// unless the record says which one acted): "" = the client's own sender,
	// "template:<id>" = that template's own sender, "account" = the account's
	// default sender. ClientSender is what the client sent in that case
	// (possibly empty). Both ride the queue row onto the record.
	SenderBy     string
	ClientSender string
	// BillingModel is how the company paid when the message was ACCEPTED:
	// "prepaid", "postpaid", or "" (not known — an unlinked account, or a
	// row written before queue/0036). Stamped by the enqueue stage from the
	// same account view that decided the acceptance reservation, carried on
	// the queue row onto the record (hot/0051), and read by the billing role:
	// a company switched from postpaid to prepaid before its last messages
	// were rated must have them invoiced, not taken from the new balance
	// (QA companies BUG-024). "" = the company's model at billing time.
	BillingModel string // BillingPrepaid | BillingPostpaid | ""
	// Quote is what the message costs, fixed when it was ACCEPTED (queue/0038,
	// hot/0059; Saleem 2026-10-13): the JSON of internal/billing.Quote — the
	// selling price of the message, of one receipt and of one lookup, and
	// whether the amount was taken from a prepaid balance at acceptance. The
	// enqueue stage writes it, it rides the queue row onto the record, and the
	// billing role charges by it: a card set or changed later never prices a
	// message that was already accepted, and no row in balance_history is
	// needed to know an acceptance charge was taken. "" = not known (accepted
	// before this field, or by a gateway with no prices wired): billing prices
	// the record by the rates of its accept time, as before.
	Quote string `json:",omitempty"`
	// Priced is the price the message's record is written with (issues 3
	// and 18; Saleem, 26 Oct 2026: the billing role is retired). Worked out
	// by the role that writes the record — the vendor role when a vendor
	// answered — and never updated after. nil = written unpriced.
	Priced *Priced `json:",omitempty"`
	// OrigSender / OrigDestination / OrigBody hold what the winning route's
	// rewrite replaced, and are empty when it changed nothing (queue/0029).
	// When the route's vendor goes down and the message moves to the next
	// route, that route starts from these — the client's own values — not
	// from the first route's rewrite. OrigBody is sealed like Body when the
	// company's messages are encrypted.
	OrigSender      string
	OrigDestination string
	OrigBody        string
	// RejectAudit is operator-only detail about the refusal — today which
	// spam category matched. Stored in its own rejects column, never rendered
	// to the client and never folded into RejectReason, which operators group
	// and count by.
	RejectAudit string
	// RefusalReceipt is the receipt a refused message's client was sent, as
	// "<stat> <err>" ("REJECTD 408" for a duplicate, Saleem 28 Oct 2026); the
	// refusal's writer files it as a dlr_ row (source system) beside its
	// rejects_ row. "" = none was sent (the usual refusal, answered at submit).
	RefusalReceipt string
	StatusCode     int
	IPAddress      string
	RequestInfo    string
	IdempotencyKey string
	RequestID      string // batch/submission id (multi-recipient)
	// CampaignID groups the multiple requests of one campaign (P1 scope
	// finalize 2026-08-09) — carried to records for campaign reporting, echoed
	// in the response; deliberately NOT part of the dedup fingerprint.
	CampaignID string
	// CampaignHash is the gateway's own signature of the text
	// (abuse.CampaignHash), taken at accept while the text is readable and
	// carried to the record (queue/0030, hot/0039): "what else went out like
	// this", across accounts. For investigation only — nothing gates on it.
	CampaignHash string
	// CodeReroute is set when an SMPP vendor's status code sent this message
	// another way — "<action>|<code>|<vendor cid>" (queue/0031, PO
	// 2026-09-15). It stays on the message, so a second such code fails it
	// instead of bouncing it between vendors. See RerouteAsk.
	CodeReroute   string
	TraceID       string // request correlation (X-Request-Id)
	Attempts      int
	ValidUntil    time.Time
	NextAttemptAt time.Time
	SubmitDate    time.Time
	DeliveryDate  time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
	// DeploymentID is the gateway that ACCEPTED this message
	// (YAKU_DEPLOYMENT_ID, config/0122). Set by the enqueue stage, carried on
	// the queue row, written to mtlog.deployment_id by whoever finalises the
	// record. Empty on a single-deployment install that never named itself.
	DeploymentID string
	// SentDeploymentID is the gateway that SENT it to the vendor. Set by the
	// vendor roles only — deliberately not defaulted to this process's id,
	// because retry and expiry also write terminal records for messages that
	// were never sent at all, and claiming they sent them would be a lie the
	// record could not be talked out of.
	SentDeploymentID string
	// QueueOwner is the deployment whose queue holds this message — the one
	// that will send it (D-121, 2026-10-02): the accepting gateway's own
	// YAKU_DEPLOYMENT_ID (deployment.Owner), never a hostname. "" = a shared
	// queue, which any gateway placed on the vendor drains — every message
	// queued before separate queues, and every install that never named
	// itself. Kept on the queue row (queue/0037 queue_owner) and in the Redis
	// queue name (<vendor>@<owner>).
	QueueOwner string `json:",omitempty"`
	// ShortCode is the vendor short code whose queue holds this message
	// (issue 22: a vendor's short codes share its rate, each in a queue of its
	// own); "" = the vendor's common queue. Set at acceptance from the
	// vendor's list (queues.Sub); kept on the queue row (queue/0044
	// shortcode) and in the Redis item.
	ShortCode string `json:",omitempty"`
	// QueueItem is the item a message was read from in a Redis queue, as it is
	// kept there (queues.Item): what the re-route mover takes out when it
	// moves the message. Never written anywhere.
	QueueItem string `json:"-"`
	// CompanyID is the customer company of the message's account, put on it
	// when it is accepted (D-136, 2026-10-14): its record is filed in
	// mt_<company>_<day> by whichever role writes it, often a gateway that
	// never saw the account. Kept on the queue row (queue/0039 company_id)
	// and in the Redis payload. 0 = no company (or a message queued before
	// this field): filed under company 0.
	CompanyID int64 `json:",omitempty"`
	// The sending vendor's receipt settings (vendors.dlr_config, D-136), put
	// on the message by the vendor role that sends it so the record writer
	// knows how long the message waits for its receipt: ReceiptWaitHours
	// (0 = the default, 48) and VendorSendsNoReceipts (the vendor sends none,
	// so nothing waits).
	ReceiptWaitHours      int  `json:",omitempty"`
	VendorSendsNoReceipts bool `json:",omitempty"`
	// VendorFirstReceiptCloses: the sending vendor sends one final receipt
	// per long message, so the first part's final closes it
	// (dlr_config.first_receipt_closes, 4 Nov 2026).
	VendorFirstReceiptCloses bool `json:",omitempty"`
}

// DefaultReceiptWaitHours is how long a sent message waits for its receipt
// when its vendor sets nothing.
const DefaultReceiptWaitHours = 48

// ReceiptWait is how long this message waits for its receipt; 0 when its
// vendor sends none.
func (m Message) ReceiptWait() time.Duration {
	if m.VendorSendsNoReceipts {
		return 0
	}
	h := m.ReceiptWaitHours
	if h <= 0 {
		h = DefaultReceiptWaitHours
	}
	return time.Duration(h) * time.Hour
}

// WantsIntermediateReceipt reports whether this message's client should be
// sent the "accepted by network" notice (SMPP: the ACCEPTD deliver_sm; the
// SubmitDLR leg). Honours registered_delivery bit 4 in full for SMPP ingress;
// every other ingress (currently only HTTP, which has no such bit) keeps its
// pre-2026-09-03 default of always sending it — see WantIntermediateReceipt's
// doc comment for why that default cannot be read from the field alone.
func (m Message) WantsIntermediateReceipt() bool {
	return m.Ingress != TransportSMPP || m.WantIntermediateReceipt
}

// WantsTerminalReceipt reports whether a receipt for terminal status st
// should reach the client, given its two declared preferences (PO 2026-09-03,
// honouring registered_delivery bits 0-1 in full):
//
//	NoReceipt          asked for nothing at all — never send
//	FailureReceiptOnly asked to hear about failure only — suppress a
//	                   StatusDelivered push, send everything else
//
// A free function rather than a method: the caller in internal/dlr resolves
// these two bools from record_index, not from a live Message — the message
// that carried them at submit time is long gone by the time a vendor receipt
// arrives.
func WantsTerminalReceipt(noReceipt, failureOnly bool, st Status) bool {
	if noReceipt {
		return false
	}
	return !failureOnly || st != StatusDelivered
}

// ClientDLR is one pending client-facing delivery receipt awaiting push to a
// bound SMPP client (deliver_sm) or an HTTP client (dlr_url). Shared across
// queue backends (MySQL client_dlr table, Redis client-DLR keys) so the
// consumers (smpp-server, the dlr role) stay backend-agnostic. [SCOPE P5]
// DLRClaim narrows a client_dlr claim to one account AND the attempt window
// that account's delivery channel owns (P5-B03).
//
// `client_dlr` is a single queue that two legs drain: the SMPP bind and the
// HTTP poster. Before this they were separated only by a 5-second staleness
// gap, so a slow bind let both deliver the same receipt. Now each account's
// `dlr_channel` turns into a window over `attempts`, and the two legs cannot
// overlap by construction:
//
//	bind   claims attempts < dlr_bind_attempts   (MaxAttempts set)
//	poster claims attempts >= dlr_bind_attempts  (MinAttempts set)
//
// 'bind' is that threshold at the retry ceiling and 'url' is it at zero, so
// one number expresses all three choices. Attempts are compared BEFORE the
// claim increments them — the count is deliveries already tried.
type DLRClaim struct {
	AccountID   int64
	MinAttempts int // inclusive lower bound
	MaxAttempts int // exclusive upper bound; 0 = unbounded
}

// MOClaim is the same window for an account's incoming messages, decided by
// `mo_channel` / `mo_bind_attempts` (config/0146) exactly as DLRClaim is by
// `dlr_channel`: the bind claims attempts < mo_bind_attempts, the webhook
// poster attempts >= it.
type MOClaim = DLRClaim

type ClientDLR struct {
	ID          int64
	AccountID   int64
	ClientMsgID string
	// MsgID is the message the receipt is about, when ClientMsgID is not it
	// (a part's id, for a long message an SMPP client sent in parts): each
	// try to give the receipt to the client is filed with its message
	// (dlr_sub_, D-140; queue/0050). "" = ClientMsgID.
	MsgID   string
	Status  string
	Receipt string
	// BindSlot is the bind the original submit arrived on (see
	// Message.BindSlot). The smpp-server's delivery loop pushes down that slot
	// when it is still live and can receive; 0 or a missing slot falls back to
	// the account's other binds in turn. Ignored by the HTTP dlr_url poster.
	BindSlot int
	// Src/Dst are the ORIGINAL message's sender and destination, so a receipt
	// pushed as deliver_sm can carry the addresses SMPP clients expect
	// (source = the handset, destination = the sender; PO 2026-09-11). Empty on
	// rows written before they existed; the push then falls back to "0".
	Src string
	Dst string
	// Stat is the SMPP word this receipt carries — one of the eight, when the
	// vendor's own receipt said something finer than the status (DELETED,
	// REJECTD, ENROUTE). Empty = derive it from Status, as every receipt did
	// before the eight were kept (queue/0032).
	Stat string
	// Delay holds a receipt back before anyone may deliver it: the account's
	// receipt policy can spread invented receipts over a range of seconds
	// instead of answering in the same millisecond the message was accepted
	// (internal/receiptpolicy). MySQL sets next_attempt_at/created_at that
	// far ahead; Redis scores the entry that far ahead — in both the receipt
	// simply is not claimable until then. 0 = deliver as soon as possible,
	// which is every receipt that is not invented.
	Delay time.Duration
	// Attempts/CreatedAt drive the client-push retry policy
	// (internal/pushretry): how many deliveries have been tried, and how long
	// this receipt has been undelivered. Both were already columns; nothing
	// read them until 2026-08-31 (P5-B02).
	Attempts  int
	CreatedAt time.Time
	// Owner is the deployment that delivers this receipt to the client (D-121):
	// the one that ACCEPTED the original message, where the client sent it
	// and so where its bind is. "" = shared, any gateway delivers it.
	Owner string
	// DoneAt is when the receipt's state was reached: the vendor's delivery
	// time when it gave one, else when the receipt reached the gateway — the
	// instant the receipt text's "done date" is written from, to the
	// millisecond (queue/0042, Redis job "done"). Zero on receipts queued
	// before it was carried; the webhook then says CreatedAt.
	DoneAt time.Time
}

// OrphanDLR is one filed receipt that correlated to no message of ours, read
// back so it can be tried again (2026-09-16). Most orphans are foreign or
// stale and stay orphans; the ones worth re-trying lost a race with their own
// record row, which the vendor role writes in batches.
//
// Table names the monthly partition the row sits in, so the row can be dropped
// from it once the receipt has been applied to its message.
type OrphanDLR struct {
	Table       string
	ID          int64
	VendorID    int64
	VendorMsgID string
	Status      string // the vendor's raw status token
	ErrorCode   string
	Raw         string
	ReceivedAt  time.Time
}

// Privacy is what was kept of one message's text, recorded on its locator row
// (record_index.mask_body) so that a receipt arriving later is kept the same
// way: its `text:` part repeats the start of the message.
type Privacy int

const (
	PrivacyPlain     Privacy = 0 // stored as sent
	PrivacyMasked    Privacy = 1 // digit runs hidden (pii_masked)
	PrivacyEncrypted Privacy = 2 // stored encrypted (all_encrypted, PO 2026-09-13)
)

// ForQueue is m as a vendor queue keeps it: without the HLR provider's call
// (HLRCall), which only the lookup's log row keeps (6 Nov 2026) — a vendor
// queue never carries kilobytes nobody there reads.
func ForQueue(m Message) Message {
	m.HLRCall = nil
	return m
}
