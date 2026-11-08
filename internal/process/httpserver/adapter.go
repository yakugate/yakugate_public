package httpserver

import (
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/msisdn"
	"github.com/yakugate/yakugate/internal/refdata"
)

// sendRequest is the native V4 /sms/v4/send body.
type sendRequest struct {
	To   string `json:"to"`
	From string `json:"from"`
	Text string `json:"text"`
	// MessageID is the client's dedup key — ONE per request (PO 2026-07-26,
	// reverting the short-lived per-recipient form). For a multi-recipient
	// batch every recipient shares it; the dedup stage scopes its key per
	// destination, so messageId identifies the REQUEST and
	// messageId+recipient identifies each message.
	MessageID string `json:"messageId"`
	// RequestID is an explicit input alias for the batch/correlation id (P1
	// scope finalize 2026-08-09). Precedence: an explicit requestId wins over
	// messageId, which in turn beats a server-generated ULID. messageId stays
	// the idempotency key regardless. See resolveRequestID.
	RequestID string `json:"requestId"`
	// CampaignID groups the multiple requests of one campaign (unlike
	// requestId, which is per-request). Carried verbatim to records and echoed
	// in the response; never part of the dedup fingerprint. [P1 scope 2026-08-09]
	CampaignID string `json:"campaignId"`
	Channel    string `json:"channel"`
	Operator   string `json:"operator"` // combined MCC+MNC (e.g. "41001"); skips HLR if supplied
	GSM7       *bool  `json:"gsm7"`     // pointer: nil = omitted (auto)
	// Encoding explicitly picks "gsm7"|"ucs2"|"latin1", overriding GSM7 when
	// both are supplied. "" (the common case) falls back to GSM7/auto-detect.
	// [SCOPE P1-09]
	Encoding string `json:"encoding"`

	// Priority is THE priority input (1..9, lower = more urgent; default 9,
	// or a matched template's own default — see resolvePriority).
	// The old `type` ladder (otp/transactional/default) was retired 2026-07-26
	// — a `type` field in a request is now just an ignored unknown field.
	Priority *int `json:"priority"`

	// SendAt schedules delivery: a Unix UTC timestamp (seconds). The message is
	// accepted and durably enqueued immediately as usual, but stays ineligible
	// for vendor claim until this instant — a past/omitted value sends
	// immediately (nil = now). [SCOPE P1-SCHEDULE]
	SendAt *int64 `json:"sendAt"`

	// ValidityPeriod is the message's lifetime IN MINUTES from acceptance,
	// mirroring SMPP submit_sm's validity_period field for HTTP clients. Omitted
	// falls back to the account/global default TTL (see stage_enqueue.go).
	// [SCOPE P1-VALIDITY]
	ValidityPeriod *int `json:"validityPeriod"`

	// Custom pass-through metadata, carried verbatim to the record. [S-06]
	AP1  string `json:"ap1"`
	AP2  string `json:"ap2"`
	APG1 string `json:"apg1"`
	APG2 string `json:"apg2"`

	// Messages is the optional PERSONALIZED-BULK array (P1 scope finalize
	// 2026-08-09): each item is an independent message with its own `to`+`text`
	// (both mandatory) and an optional `from` that inherits the request-level
	// `from` — as does every other field an item omits. When present, the
	// single-message fields above act purely as the inherited defaults. See
	// expandRequest. Capped by YAKU_MAX_BATCH; total recipients still bounded by
	// YAKU_MAX_RECIPIENTS.
	Messages []messageItem `json:"messages,omitempty"`
}

// messageItem is one entry of the personalized-bulk `messages[]` array. Any
// field left empty/nil inherits the request-level value; `to` and `text` are
// mandatory per item (an item missing either is rejected as its own results[]
// entry, never failing the whole batch).
type messageItem struct {
	To         string `json:"to"`
	Text       string `json:"text"`
	From       string `json:"from"`
	MessageID  string `json:"messageId"`
	CampaignID string `json:"campaignId"`
	Operator   string `json:"operator"`
	Encoding   string `json:"encoding"`
	GSM7       *bool  `json:"gsm7"`
	Priority   *int   `json:"priority"`
	Channel    string `json:"channel"`
	AP1        string `json:"ap1"`
	AP2        string `json:"ap2"`
	APG1       string `json:"apg1"`
	APG2       string `json:"apg2"`
}

// merge returns the request-level defaults overridden by this item's non-empty
// fields — the inheritance rule from the scope ("any field an item omits
// inherits the request-level default"). `to`/`text` always come from the item.
func (it messageItem) merge(parent sendRequest) sendRequest {
	sub := parent
	sub.Messages = nil // the derived single-message request never recurses
	sub.To, sub.Text = it.To, it.Text
	if it.From != "" {
		sub.From = it.From
	}
	if it.MessageID != "" {
		sub.MessageID = it.MessageID
	}
	if it.CampaignID != "" {
		sub.CampaignID = it.CampaignID
	}
	if it.Operator != "" {
		sub.Operator = it.Operator
	}
	if it.Encoding != "" {
		sub.Encoding = it.Encoding
	}
	if it.GSM7 != nil {
		sub.GSM7 = it.GSM7
	}
	if it.Priority != nil {
		sub.Priority = it.Priority
	}
	if it.Channel != "" {
		sub.Channel = it.Channel
	}
	if it.AP1 != "" {
		sub.AP1 = it.AP1
	}
	if it.AP2 != "" {
		sub.AP2 = it.AP2
	}
	if it.APG1 != "" {
		sub.APG1 = it.APG1
	}
	if it.APG2 != "" {
		sub.APG2 = it.APG2
	}
	return sub
}

// clampPriority is the last line of defence on the documented 1..9 range (PO
// 2026-09-05, narrowed from 1..999). An out-of-range value from a CLIENT is
// already refused 400 in request.go's validation — this only guards paths that
// do not go through it, such as a dialect mapping that produced a number of its
// own, where clamping is better than letting a stray value into queue ordering.
func clampPriority(p int) int {
	switch {
	case p < 1:
		return 1
	case p > 9:
		return 9
	default:
		return p
	}
}

// resolveEncodingPref returns the encoding preference the pipeline plans with:
// "gsm7" | "ucs2" | "latin1", or "" for auto-detect.
//
// `encoding` wins over the boolean `gsm7` when both are present — the rule
// that lets a client migrate by ADDING a field rather than removing one. The
// boolean alone is refused at validation (retired 2026-09-05); it is still
// read here so a mid-migration request keeps its meaning, and because the
// SMPP edge sets the same canonical field from data_coding.
func resolveEncodingPref(r sendRequest) string {
	if e := strings.ToLower(strings.TrimSpace(r.Encoding)); e != "" {
		return e
	}
	return gsm7Str(r.GSM7)
}

// resolvePriority: explicit number wins; 0 ("unset") is resolved later in the
// pipeline — a matched template's own default priority (stage_template.go)
// wins over the generic default (pipeline.defaultPriority, applied at
// enqueue) when the client didn't say. Clamping a client-supplied value here
// is safe because a legitimate 1..999 value can never collide with the 0
// sentinel. [SCOPE P1-TEMPLATE-PRIORITY]
func resolvePriority(r sendRequest) int {
	if r.Priority != nil {
		return clampPriority(*r.Priority)
	}
	return 0
}

// splitOperator splits a combined MCC+MNC value ("41001") into MCC (first 3
// digits) and MNC (remaining digits). Any value that is not 4–6 digits
// (accepted since 2026-11-05, see validateRequest) yields empty MCC/MNC, so
// the HLR stage resolves the network instead.
func splitOperator(s string) (mcc, mnc string) {
	s = strings.TrimSpace(s)
	if !operatorRe.MatchString(s) {
		return "", ""
	}
	return s[:3], s[3:]
}

// recipientSep: recipients may be separated by comma, semicolon, or newline —
// pasted lists commonly use any of the three. [SCOPE P1-RCPT]
func recipientSep(r rune) bool { return r == ',' || r == ';' || r == '\n' || r == '\r' }

// recipients splits `to` on comma/semicolon/newline, normalises each token
// (shared internal/msisdn — D-064 direction: one rule, both edges), drops empties, de-dupes. Cleaning runs BEFORE
// de-duplication so "+92 300-1234567" and "923001234567" in one batch are
// recognised as the same recipient instead of being fanned out twice. [SCOPE P1-RCPT]
func (r sendRequest) recipients() []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range strings.FieldsFunc(r.To, recipientSep) {
		// Destination, not Clean: a national number keeps its 0 so the
		// validate stage refuses it instead of sending 3001234567 for
		// 03001234567 (BUG-YK-API-001, 2026-10-01).
		p = msisdn.Destination(p)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	return out
}

func gsm7Str(b *bool) string {
	if b == nil {
		return ""
	}
	if *b {
		return "true"
	}
	return "false"
}

// resolveRequestID resolves the batch/correlation id with the precedence ruled
// in the P1 scope finalize (2026-08-09): an explicit client `requestId` wins
// over `messageId`, which in turn beats a freshly minted server ULID. The
// message's own idempotency key stays `messageId` regardless (set below), so
// this only governs the shared batch id echoed as `requestId`.
func resolveRequestID(_ sendRequest) string {
	// ALWAYS ours (PO 2026-09-06: "we generate request id that's unique and is
	// sent to client"). Until now a client-supplied requestId won, and a
	// client `messageId` was used as the batch id when no requestId came —
	// which is what made one word mean two things.
	//
	// A client that still sends `requestId` is not refused, it is simply not
	// used: their own id comes back as `reference`, which is the field that
	// actually carries their value. Generating ours also keeps request_id
	// ULID-shaped, which is what portal_campaigns.request_id CHAR(26) expects.
	return ulid.Make().String()
}

// expandRequest turns a submission into canonical messages, handling BOTH the
// single-message form and the personalized-bulk `messages[]` array (P1 scope
// finalize 2026-08-09). For bulk, every item is expanded independently with
// request-level inheritance (messageItem.merge), and ALL messages share ONE
// batch requestId (the request-level id) — each item still carries its own
// messageId as its idempotency key. An item with no valid recipient becomes a
// single placeholder message so the validate stage rejects it as its own
// results[] entry, rather than the item silently vanishing from the batch.
func expandRequest(r sendRequest, a *refdata.Account) []*model.Message {
	if len(r.Messages) == 0 {
		return toMessages(r, a)
	}
	batchID := resolveRequestID(r)
	var out []*model.Message
	for _, it := range r.Messages {
		sub := it.merge(r)
		itemMsgs := toMessages(sub, a)
		if len(itemMsgs) == 0 {
			itemMsgs = []*model.Message{{
				ID:             ulid.Make().String(),
				AccountID:      a.ID,
				CompanyID:      a.CompanyID,
				Username:       a.Username,
				Sender:         strings.TrimSpace(sub.From),
				Destination:    "", // empty -> validate stage rejects (per-item 400)
				Body:           sub.Text,
				Ingress:        model.TransportHTTP,
				CampaignID:     strings.TrimSpace(sub.CampaignID),
				IdempotencyKey: strings.TrimSpace(sub.MessageID),
			}}
		}
		for _, m := range itemMsgs {
			m.RequestID = batchID // the whole batch shares one requestId
		}
		out = append(out, itemMsgs...)
	}
	return out
}

// toMessages fans a request out into one canonical Message per recipient. [SCOPE P1-ADAPT]
// Every recipient of the same request shares one RequestID — returned to the
// client as `requestId`. PO 2026-07-26: "your id names your request; our ids
// name the messages" — a client-supplied messageId BECOMES the requestId
// (single and multi recipient alike, so the field never changes meaning with
// recipient count); omitted, the server generates a ULID. Each recipient
// always gets its own server msg_id.
func toMessages(r sendRequest, a *refdata.Account) []*model.Message {
	mcc, mnc := splitOperator(r.Operator)
	requestID := resolveRequestID(r)
	campaignID := strings.TrimSpace(r.CampaignID)
	// `from` is MANDATORY (PO 2026-07-26, reverting the brief optional-from
	// experiment the same day — several use cases need an explicit sender).
	// Blank falls through to validate's missing-field 400; edges trimmed only.
	from := strings.TrimSpace(r.From)
	encPref := resolveEncodingPref(r)
	var validUntil time.Time
	if r.ValidityPeriod != nil && *r.ValidityPeriod > 0 {
		validUntil = time.Now().Add(time.Duration(*r.ValidityPeriod) * time.Minute)
	}
	var sendAt time.Time
	if r.SendAt != nil {
		if t := time.Unix(*r.SendAt, 0).UTC(); t.After(time.Now()) {
			sendAt = t // past/now values send immediately (zero = no delay)
		}
	}
	var msgs []*model.Message
	for _, to := range r.recipients() {
		msgs = append(msgs, &model.Message{
			ID:             ulid.Make().String(),
			AccountID:      a.ID,
			CompanyID:      a.CompanyID,
			Username:       a.Username,
			Sender:         from,
			Destination:    to,
			Ingress:        model.TransportHTTP, // client submitted over HTTP
			Body:           r.Text,
			Channel:        r.Channel,
			AP1:            r.AP1,
			AP2:            r.AP2,
			APG1:           r.APG1,
			APG2:           r.APG2,
			Gsm7:           encPref,
			MCC:            mcc,
			MNC:            mnc,
			Priority:       resolvePriority(r),
			ValidUntil:     validUntil,
			NextAttemptAt:  sendAt,
			IdempotencyKey: strings.TrimSpace(r.MessageID),
			RequestID:      requestID,
			CampaignID:     campaignID,
		})
	}
	return msgs
}
