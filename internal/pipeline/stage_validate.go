package pipeline

import (
	"context"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/yakugate/yakugate/internal/msisdn"
	"github.com/yakugate/yakugate/internal/senderid"
)

// msisdnRe: 8-15 digits, never starting with 0 — E.164 country codes never
// do, and msisdn.Destination leaves a leading 0 only on a national number
// (refused with its own reason) or an all-zero one (2026-10-01; the floor
// was `^\d{8,15}$`, which let "00000000" through).
var msisdnRe = regexp.MustCompile(`^[1-9]\d{7,14}$`)

// nationalNumber is the detail of the refusal for a number in national form.
const nationalNumber = "INVALID_MSISDN: national number (starts with 0) — send it in international form with the country code, e.g. 923001234567, not 03001234567"

// CleanMSISDN normalises a number exactly as the validate stage does and says
// whether the result is a number the gateway accepts. For callers that judge a
// number outside the pipeline (the price check).
func CleanMSISDN(raw string) (string, bool) {
	n := msisdn.Destination(raw)
	return n, msisdnRe.MatchString(n)
}

// MSISDNRefusal is the validate stage's refusal for a number already cleaned
// by msisdn.Destination (CleanMSISDN's first result) — "" when the number is
// accepted. The price check uses it so it refuses a national number with the
// same words a send does, not a bare INVALID_MSISDN (2026-10-01).
func MSISDNRefusal(n string) string {
	switch {
	case msisdn.National(n):
		return nationalNumber
	case !msisdnRe.MatchString(n):
		return "INVALID_MSISDN"
	}
	return ""
}

// Sender-ID length rules (PO 2026-07-26): an all-numeric sender (a long code /
// MSISDN) may be up to YAKU_SENDER_MAX_NUMERIC digits; anything containing a
// letter (an alphanumeric mask) is capped at YAKU_SENDER_MAX_ALPHA — the GSM
// 03.38/03.40 alphanumeric-originator limit. Violations reject 400
// INVALID_SENDER. Replaces the earlier flat 32-rune cap.
var (
	senderMaxAlpha   = envInt("YAKU_SENDER_MAX_ALPHA", 11)
	senderMaxNumeric = envInt("YAKU_SENDER_MAX_NUMERIC", 15)
)

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// blankText reports a text with nothing to read: only spaces, line breaks
// and invisible format marks (zero-width spaces, direction marks, BOM).
func blankText(s string) bool {
	for _, r := range s {
		if !unicode.IsSpace(r) && !unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}

// --- stage 1/18: validate --- [SCOPE P1-01]
type validateStage struct{}

func (validateStage) Name() string { return "validate" }
func (validateStage) Run(_ context.Context, e *Envelope) *Reject {
	m := e.Msg
	resolveSender(e)
	if m.Destination == "" || strings.TrimSpace(m.Sender) == "" || m.Body == "" {
		return reject(400, "INVALID_REQUEST: missing required field (to/from/text)")
	}
	// A text of only spaces (or invisible marks) is as empty as no text, but
	// it used to be accepted, charged and sent as a blank SMS
	// (BUG-YK-API-002, 2026-10-01). The text itself is never trimmed.
	if blankText(m.Body) {
		return reject(400, "INVALID_REQUEST: text is blank (only spaces)")
	}
	// Destination normalization is a shared business rule (D-064 direction,
	// PO 2026-07-26): skim paste junk, convert Urdu/Arabic digits, trim the
	// international 00 prefix — HERE in stage 1, so HTTP and SMPP ingress
	// normalize identically (a number must not be valid on one port and
	// invalid on the other). The HTTP adapter also cleans during recipient
	// splitting (cross-format fan-out de-dupe compares normalized values);
	// this call is then a no-op for HTTP and the ONLY cleaning for SMPP.
	//
	// A NATIONAL number (one leading 0: 03001234567) is refused with its own
	// reason (2026-10-01, BUG-YK-API-001/SND-001). It used to lose its 0 and
	// go out as 3001234567 — another country's number — accepted and charged:
	// the gateway adds no country code and keeps no per-account country to
	// add one from, so it must not guess.
	m.Destination = msisdn.Destination(m.Destination)
	if why := MSISDNRefusal(m.Destination); why != "" {
		// 400, not 406: 406 is content-negotiation; a bad phone number is a
		// validation error (P1 scope finalize 2026-08-09, error-code correction).
		return reject(400, why)
	}
	// Sender-ID rules (PO 2026-07-26), one rule in internal/senderid shared
	// with the console and mirrored by the Admin API: numeric senders (an
	// optional '+') up to YAKU_SENDER_MAX_NUMERIC digits; alphanumeric names
	// up to YAKU_SENDER_MAX_ALPHA characters, made only of letters A-Z,
	// digits, space and . - _ — what operators accept (2026-10-01,
	// BUG-YK-API-003; was "any GSM-7 character", which let @ # $ % through).
	if why := senderid.Problem(m.Sender, senderMaxAlpha, senderMaxNumeric); why != "" {
		return reject(400, "INVALID_SENDER: "+why)
	}
	// Body pre-cap derives from YAKU_MAX_SEGMENTS (chunk-4 ruling 4.2,
	// 2026-08-03 — was a hardcoded 1073 that wrongly rejected valid 8-segment
	// GSM-7 messages and ignored the operator's configured cap): 153 chars per
	// part is the loosest alphabet, so anything beyond 153×max can never fit
	// regardless of encoding and is cut early; the encoding stage remains the
	// true per-alphabet gate at exactly maxSegments.
	if utf8.RuneCountInString(m.Body) > maxSegments*153 {
		return reject(400, "MESSAGE_TOO_LONG")
	}
	if m.IdempotencyKey != "" {
		if len(m.IdempotencyKey) > 64 {
			return reject(400, "INVALID_REQUEST: messageId exceeds 64 chars")
		}
		// Control characters have no business in a client message id — it
		// becomes a dedup key, record_index.request_id, and a portal query
		// value (chunk-4 ruling 4.6).
		for _, r := range m.IdempotencyKey {
			if r < 0x20 || r == 0x7f {
				return reject(400, "INVALID_REQUEST: messageId contains control characters")
			}
		}
	}
	// Pass-through metadata caps (chunk-4 ruling 4.6): ap1/ap2/apg1/apg2 are
	// carried verbatim onto record rows — bound them before they can overflow
	// a column or bloat the hot store. Counted in CHARACTERS, as the columns
	// (VARCHAR(255), utf8mb4) and the Admin API count them (QA templates
	// API-017): len() counted bytes, so 128 Urdu letters were refused.
	for _, ap := range []string{m.AP1, m.AP2, m.APG1, m.APG2} {
		if utf8.RuneCountInString(ap) > 255 {
			return reject(400, "INVALID_REQUEST: ap1/ap2/apg1/apg2 exceeds 255 chars")
		}
	}
	return nil
}

// resolveSender settles, ONCE and first, which sender the message goes out
// with, so every later rule (blocklist, templates, spam, routing, dedupe)
// sees the final one and the record can say who chose it:
//
//  1. the client's own sender, when it is well formed and the account may
//     use it — never replaced;
//  2. otherwise the account's default sender (config/0129), if the account
//     may use it;
//  3. otherwise nothing changes, and the message is refused as it always was.
//
// Templates set no sender (their default_sender is retired, Saleem
// 2026-09-29): a template only says which senders may use it.
// Msg.SenderBy/ClientSender record step 2.
func resolveSender(e *Envelope) {
	a := e.Account
	if a == nil {
		return
	}
	cur := e.Msg.Sender
	usable := func(s string) bool { return senderWellFormed(s) && senderAllotted(a.AllowAllSC, a.Shortcodes, s) }
	if usable(cur) {
		return
	}
	if def := strings.TrimSpace(a.DefaultSender); def != "" && usable(def) {
		e.Msg.ClientSender, e.Msg.Sender, e.Msg.SenderBy = cur, def, SenderByAccount
	}
}

// SenderByAccount marks a sender the account's default supplied.
const SenderByAccount = "account"

// senderWellFormed is the validate stage's own sender rule, as a yes/no.
func senderWellFormed(s string) bool {
	return senderid.Problem(s, senderMaxAlpha, senderMaxNumeric) == ""
}

// senderAllotted is the sender-id stage's rule, as a yes/no.
func senderAllotted(allowAll bool, allotted []string, s string) bool {
	if allowAll {
		return true
	}
	for _, sc := range allotted {
		if sc == s {
			return true
		}
	}
	return false
}
