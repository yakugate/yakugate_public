package dialect

import (
	"encoding/json"
	"encoding/xml"
	"strings"
)

// Result is one recipient's outcome, in canonical terms. The HTTP edge builds
// these; the renderer turns them into whatever the account's dialect asks for.
type Result struct {
	To        string
	MessageID string
	// RequestID is the batch/submission ULID shared by every recipient of one
	// request — rendered as `requestId` in each item AND on the multi
	// envelope. Empty on request-level failures (no batch was formed).
	RequestID string
	// Reference is the id the CLIENT supplied at submit (HTTP `messageId`,
	// SMPP `user_message_reference`), returned under its own name so it is
	// never confused with OUR MessageID. Empty when they supplied none, in
	// which case the key is omitted rather than rendered blank.
	Reference        string
	Accepted         bool
	StatusCode       int    // native HTTP status for this recipient
	Reason           string // reason code, "" when accepted
	Detail           string // human-readable detail
	AcceptedTime     string
	Sender           string // FieldSender source — empty on a request-level failure with no parsed sender
	Body             string // FieldBody source
	ResponseDateTime string // FieldResponseDateTime source — see the field's doc comment on why it's separate from AcceptedTime
	// Cost/Currency/RemainingBalance are the optional accept-time cost
	// disclosure (PO 2026-07-27) — set by the HTTP edge only when the
	// account's sms_service_config flags are on AND the route priced a sell
	// rate; nil/empty otherwise, in which case canonical() omits the key
	// entirely rather than render a placeholder. Currency only accompanies a
	// non-nil Cost.
	Cost             *float64
	Currency         string
	RemainingBalance *float64
	// The breakdown behind Cost (PO 2026-09-03): Net + Tax = Total, and Total
	// equals Cost. Rendered only alongside a non-nil Cost, so an account that
	// has not opted into cost disclosure sees none of them. TaxPercentage is
	// nil on a zero-tax rate row — "no tax" and "taxed at 0%" are the same
	// thing, and neither should render a field.
	Net           *float64
	Tax           *float64
	TaxPercentage *float64
	Total         *float64
	// ChargedAt is "submit" or "delivery" when the account is charged later
	// than acceptance (FieldChargedAt); "" otherwise. Rendered only with Cost.
	ChargedAt string
	// CampaignID echoes the request's campaignId — rendered only when non-empty
	// (P1 scope finalize 2026-08-09).
	CampaignID string
	// Segments is the optional per-recipient segment count — rendered only when
	// non-nil (the HTTP edge sets it iff the account opted in, like Cost).
	Segments *int
	// ShowMessage gates FieldMessage (the echo of the client's submitted text):
	// message became an OPTIONAL field (P1 scope finalize 2026-08-09), present
	// only when the account opts in — so it is set per account, like cost.
	ShowMessage bool
}

// Rendered is a ready-to-write HTTP response.
type Rendered struct {
	Body        []byte
	StatusCode  int
	ContentType string
}

// Render turns per-recipient results into the response body and HTTP status.
//
// The NATIVE contract is produced by this same function with a nil Config, so
// native and dialect-mapped responses cannot drift apart — there is exactly one
// renderer. The native shape follows the published upstream contract's field
// names (statusCode / status / message / messageId / acceptedTime / error) per
// the PO decision of 2026-07-22, keeping Yakugate's uppercase reason
// vocabulary as the `status` value (PO 2026-07-27: `message` was split off to
// echo the client's own submitted text instead).
//
// Multi-recipient responses keep our fan-out: results are wrapped in `results`,
// with the envelope's own statusCode following the uniform-code rule (a shared
// code surfaces as the HTTP status, a mix returns 200 partial).
func Render(results []Result, c *Config) Rendered {
	status := envelopeStatus(results, c)
	format := FormatJSON
	if c != nil && c.Response.Format != "" {
		format = c.Response.Format
	}

	switch format {
	case FormatText:
		return Rendered{Body: []byte(renderText(results, c)), StatusCode: status, ContentType: "text/plain; charset=utf-8"}
	case FormatXML:
		return Rendered{Body: renderXML(results, c), StatusCode: status, ContentType: "application/xml; charset=utf-8"}
	default:
		return Rendered{Body: renderJSON(results, c, status), StatusCode: status, ContentType: "application/json"}
	}
}

// fieldsFor picks the mapping that applies to one result: a per-reason-code
// error rule when present, otherwise the success mapping.
//
// A rejection with NO configured rule for its reason must NOT fall through to
// the success mapping (a bug fixed 2026-07-23) — that silently rendered an
// unmapped rejection (e.g. INVALID_MSISDN, or a lockout) as if it had
// succeeded, since the success mapping's hardcoded defaults (e.g. a literal
// "300"/"Message Sent Successfully!" alias with no real source) apply to ANY
// result they're given, accepted or not. Falling back to nil here instead
// makes project() return the raw canonical field set for that result — the
// honest native shape — rather than lying.
//
// Changed 2026-09-29 (Saleem: "status code mapping be in status, rest of the
// response body mapping be same as if i dont set any status code mapping"):
// a rule with NO fields changes the HTTP status only. Its body is exactly the
// body the refusal has with no rule — the native field set — instead of the
// success mapping, which knows nothing of refusals and dropped the reason. A
// rule that lists its own fields still renders them.
//
// Since 6 Nov 2026 a dialect may keep ONE list, Response.Fields, for every
// reply (Saleem: success and error are both responses, no double effort):
// an accepted reply uses it in place of Success, and a refusal whose rule
// lists no fields — or that has no rule — uses it too. It does not drop the
// reason as the old success reuse did: a field reads the refusal's own
// values and falls back through its Else items (project). A field's When
// keeps it to accepted or refused replies.
func fieldsFor(r Result, c *Config) []Field {
	if c == nil {
		return nil
	}
	if r.Accepted {
		if len(c.Response.Fields) > 0 {
			return fieldsWhen(c.Response.Fields, true)
		}
		return fieldsWhen(c.Response.Success, true)
	}
	if rule, ok := errorRuleFor(r.Reason, c); ok && len(rule.Fields) > 0 {
		return fieldsWhen(rule.Fields, false)
	}
	if len(c.Response.Fields) > 0 {
		return fieldsWhen(c.Response.Fields, false)
	}
	return nil
}

// fieldsWhen keeps the fields sent on this kind of reply. A list with no
// When at all is returned as it is.
func fieldsWhen(fields []Field, accepted bool) []Field {
	skip := WhenSuccess
	if accepted {
		skip = WhenError
	}
	n := 0
	for _, f := range fields {
		if f.When == skip {
			n++
		}
	}
	if n == 0 {
		return fields
	}
	out := make([]Field, 0, len(fields)-n)
	for _, f := range fields {
		if f.When != skip {
			out = append(out, f)
		}
	}
	return out
}

// errorRuleFor resolves the error rule for a reason code: the exact-code rule
// wins; otherwise the OPT-IN "*" wildcard applies (chunk-3 ruling 3.4,
// 2026-08-03 — see Response's doc comment). Neither configured keeps the
// honest native fallback.
func errorRuleFor(reason string, c *Config) (ErrorRule, bool) {
	if rule, ok := c.Response.Errors[reason]; ok {
		return rule, true
	}
	rule, ok := c.Response.Errors["*"]
	return rule, ok
}

// statusFor resolves one recipient's HTTP status after any dialect override.
func statusFor(r Result, c *Config) int {
	if c == nil {
		return r.StatusCode
	}
	if r.Accepted {
		if c.Response.StatusCode != 0 {
			return c.Response.StatusCode
		}
		return r.StatusCode
	}
	if rule, ok := errorRuleFor(r.Reason, c); ok && rule.StatusCode != 0 {
		return rule.StatusCode
	}
	return r.StatusCode
}

// RefusalStatus is the HTTP status a refusal with this native status and
// reason code is answered with under dialect c (nil = native): the dialect's
// rule for the reason, else its "*" rule, else the native status. The "Will
// this send?" check uses it to say what the client itself is told (QA spam
// BUG-006: it said 403 for SPAM_KEYWORD where the account's dialect answers 451).
func RefusalStatus(nativeStatus int, reason string, c *Config) int {
	return statusFor(Result{StatusCode: nativeStatus, Reason: reason}, c)
}

// envelopeStatus resolves the HTTP status line.
//
// ONE rule for every account, native and dialect alike (PO 2026-09-16,
// replacing the native always-200 of 2026-07-26): when every recipient shares
// an outcome, that outcome IS the HTTP status — a refused single-recipient
// submit answers 403, 409, 422, 429 and so on, not 200 with the refusal buried
// in the body. A mix of outcomes still answers 200, because no single status
// line can honestly describe "three of a hundred were refused" and the body
// carries each recipient's own code.
//
// Request-level failures (auth, malformed body, unknown path — Results with no
// recipient) are the same rule: they are the only result, so their code is the
// shared one.
//
// A dialect account's own overrides (success status_code, per-reason-code
// statuses) still apply through statusFor, so this changes only WHICH status
// is chosen, never a configured one.
func envelopeStatus(results []Result, c *Config) int {
	if len(results) == 0 {
		return 200
	}
	first := statusFor(results[0], c)
	for _, r := range results[1:] {
		if statusFor(r, c) != first {
			return 200
		}
	}
	return first
}

// canonical builds the canonical field set for one result — the values a
// mapping's `source` can refer to.
//
// The native shape is a FIXED field set (PO 2026-07-26: "single and multi
// response fields must match"; split 2026-07-27 into status/message — see
// FieldStatus's doc comment): every item — single response, each results[]
// entry, accepted or rejected, even a request-level failure — carries exactly
// {statusCode, status, message, messageId, requestId, acceptedTime, error,
// to}. Since 2026-10-02 a field with nothing in it is LEFT OUT of the native
// reply (withoutEmpty) rather than sent as "" — the same rule for single and
// multi-recipient items. A dialect's mapped keys still stay when empty (its
// default when it has one, else ""; project(), 2026-09-29): a dialect copies a
// customer's existing API, whose keys it keeps.
//
// cost/currency/remainingBalance are a deliberate exception to "no key ever
// appears or vanishes" above (PO 2026-07-27, S-05): they are genuinely
// optional, gated per-account on sms_service_config.send_sms_cost /
// send_remaining_balance and further on whether the route priced a sell rate
// at all, so they are added to the map only when Result carries them, never
// as an empty placeholder.
func canonical(r Result, c *Config) map[string]any {
	m := map[string]any{
		FieldStatusCode:   statusFor(r, c),
		FieldTo:           r.To,
		FieldMessageID:    r.MessageID,
		FieldRequestID:    r.RequestID,
		FieldAcceptedTime: r.AcceptedTime,
		FieldError:        errorText(r), // never empty on a refusal (errors.go)
		// The sender the message goes out with — after the account's default
		// sender or a template's own sender replaced the client's (Saleem
		// 2026-09-29: "sender also be part of response"). Part of the native
		// field set since then; empty on a request-level failure.
		FieldSender: r.Sender,
	}
	// FieldMessage (echo of the client's submitted text) became OPTIONAL (P1
	// scope finalize 2026-08-09) — present only when the account opted in, like
	// cost. A dialect that maps `message` as a source still resolves it from
	// r.Body via FieldBody below when it wants the text irrespective of ShowMessage.
	if r.ShowMessage {
		m[FieldMessage] = r.Body
	}
	// campaignId echoed only when the request carried one.
	if r.CampaignID != "" {
		m[FieldCampaignID] = r.CampaignID
	}
	// The client's own id, echoed only when they sent one — a client that
	// supplied nothing should not receive an empty key inviting them to
	// wonder what it means.
	if r.Reference != "" {
		m[FieldReference] = r.Reference
	}
	// segments: optional per-recipient count, present only when the edge set it
	// (account opted in), never as a placeholder — same discipline as cost.
	if r.Segments != nil {
		m[FieldSegments] = *r.Segments
	}
	// Sender/Body/ResponseDateTime exist SOLELY as dialect-mappable sources —
	// gated on c != nil (bug fixed 2026-07-23: handler.go populates them on
	// every Result unconditionally, dialect or not, so without this gate they
	// leaked into the NATIVE contract too). Set regardless of accept/reject —
	// the reference vendor's own XML API echoes originator/messagedata on
	// error responses too (whatever the client submitted), not just on
	// success.
	if c != nil {
		if r.Body != "" {
			m[FieldBody] = r.Body
		}
		if r.ResponseDateTime != "" {
			m[FieldResponseDateTime] = r.ResponseDateTime
		}
	}
	// One vocabulary: uppercase reason codes, and ACCEPTED for success — the
	// success value was lowercase "accepted" until 2026-07-26, an odd one out.
	// This now lives under `status` (FieldMessage was repurposed above).
	if r.Accepted {
		m[FieldStatus] = "ACCEPTED"
	} else {
		m[FieldStatus] = r.Reason
	}
	if r.Cost != nil {
		m[FieldCost] = *r.Cost
		if r.Currency != "" {
			m[FieldCurrency] = r.Currency
		}
		// Breakdown rides with cost, never on its own: these explain a price,
		// so without the price they explain nothing.
		if r.Net != nil {
			m[FieldNet] = *r.Net
		}
		if r.Tax != nil {
			m[FieldTax] = *r.Tax
		}
		if r.TaxPercentage != nil {
			m[FieldTaxPercentage] = *r.TaxPercentage
		}
		if r.Total != nil {
			m[FieldTotal] = *r.Total
		}
		if r.ChargedAt != "" {
			m[FieldChargedAt] = r.ChargedAt
		}
	}
	if r.RemainingBalance != nil {
		m[FieldRemainingBalance] = *r.RemainingBalance
	}
	return m
}

// project applies a field mapping to a canonical set. With no mapping the
// canonical set is returned as-is (the native shape). Alias may be a
// dot-separated path (e.g. "result.status") to nest the client's desired
// output at any depth [SCOPE P1-DIALECT-NEST, PO 2026-07-27] — canonical()'s
// own field set is always flat, so only Source-as-flat-lookup is needed here,
// never getPath. XML/text rendering treat a nested value as an opaque Go
// value (no special nested-element/placeholder handling); nested aliases are
// a JSON-oriented feature.
func project(cn map[string]any, fields []Field) map[string]any {
	if len(fields) == 0 {
		return cn
	}
	out := map[string]any{}
	for _, f := range fields {
		src := f.Source
		if src == "" {
			src = f.Alias
		}
		v, ok := cn[src]
		if len(f.Else) > 0 {
			if out2, keep := projectElse(cn, f, v, ok); keep {
				setPath(out, f.Alias, out2)
			}
			continue
		}
		if !ok || isEmpty(v) {
			switch {
			case f.Default != "":
				v = f.Default
			case !ok:
				// Not in the canonical set at all — an optional field this
				// response does not carry (cost without opt-in, no client
				// reference). It stays absent, as in the native shape.
				continue
			default:
				// In the set but empty: the key stays, with an empty value.
				// The native shape never drops a key, and a mapped one must
				// not either — dropping them shrank a refusal rendered
				// through the success mapping (a status-only error rule) to
				// little more than its status code (Saleem 2026-09-29).
				v = ""
			}
		}
		if len(f.Map) > 0 {
			if mapped, found := f.Map[toString(v)]; found {
				v = mapped
			}
		}
		setPath(out, f.Alias, v)
	}
	return out
}

// projectElse is one field that has Else items, in the order Saleem set on
// 6 Nov 2026: the value through the map; not in the map (or empty) — each
// Else item in order, the first with a value; none — the original value;
// empty — the default. keep=false: nothing at all, and the source is not
// one this reply carries (as project leaves such a field out).
func projectElse(cn map[string]any, f Field, v any, ok bool) (any, bool) {
	empty := !ok || isEmpty(v)
	if !empty {
		if len(f.Map) == 0 {
			return v, true
		}
		if mapped, found := f.Map[toString(v)]; found {
			return mapped, true
		}
	}
	for _, e := range f.Else {
		if e.Value != "" {
			return e.Value, true
		}
		if alt, has := cn[e.Source]; has && !isEmpty(alt) && strings.TrimSpace(toString(alt)) != "" {
			return alt, true
		}
	}
	switch {
	case !empty:
		return v, true
	case f.Default != "":
		return f.Default, true
	case !ok:
		return nil, false
	default:
		return "", true
	}
}

// shape is one result's rendered field set: the mapping that applies, then —
// when the dialect EXPLICITLY says "Pass through" (keep_unmapped true) — every
// canonical field the mapping did not use, under its own name (Saleem
// 2026-09-29: the portal offers the setting beside the response mapping and
// says unmapped fields "go out as they are"; the engine had applied it to the
// request only). A dialect that never set it (NULL) keeps its reply exactly as
// before, and "Drop them" sends only what is mapped.
//
// The dialect-only extras (body, responseDateTime — see extraFields)
// appear only when a mapping names them: never in the native field set a
// refusal falls back to, never through "Pass through". Each repeats something
// the reply already says — body is message, responseDateTime is acceptedTime
// in another format — and Saleem (2026-09-29) asked for "no duplicate keys and
// one time only, in utc, for all response formats".
func shape(r Result, c *Config) map[string]any {
	cn := canonical(r, c)
	fields := fieldsFor(r, c)
	if len(fields) == 0 {
		return withoutEmpty(withoutExtras(cn))
	}
	out := project(cn, fields)
	if c == nil || c.KeepUnmapped == nil || !*c.KeepUnmapped {
		return out
	}
	used := map[string]bool{}
	for _, f := range fields {
		src := f.Source
		if src == "" {
			src = f.Alias
		}
		used[src] = true
	}
	for k, v := range cn {
		if used[k] || extraFields[k] || isBlank(v) {
			continue
		}
		if _, taken := out[k]; taken {
			continue // a mapped alias already has this name: the mapping wins
		}
		out[k] = v
	}
	return out
}

// extraFields exist only as mapping sources for a dialect that must copy an
// older API's reply (the Jazz XML API echoes the message text and a
// "YYYY-MM-DD HH:MM:SS" time). They are never sent unless mapped. The sender
// was one of them until it became a native field (2026-09-29).
var extraFields = map[string]bool{FieldBody: true, FieldResponseDateTime: true}

func withoutExtras(cn map[string]any) map[string]any {
	for k := range extraFields {
		delete(cn, k)
	}
	return cn
}

// withoutEmpty leaves out every field with nothing to say (Saleem
// 2026-10-02: "fields with empty string … not to send them in response"): an
// accepted message carries no `error`, a refusal no `messageId`,
// `acceptedTime` or `requestId`, a request-level failure no `to` or `sender`.
// statusCode and status are always there. Single and multi-recipient items
// follow the same rule, so they still match (PO 2026-07-26). A dialect's own
// mapped fields are not touched: it copies a customer's existing API, and
// keeps its keys as configured (project).
func withoutEmpty(cn map[string]any) map[string]any {
	for k, v := range cn {
		if k != FieldStatusCode && k != FieldStatus && isBlank(v) {
			delete(cn, k)
		}
	}
	return cn
}

// isBlank is an empty string — the only "no value" the canonical set holds
// (counts and prices are left out when absent, never zero).
func isBlank(v any) bool {
	s, ok := v.(string)
	return ok && s == ""
}

func renderJSON(results []Result, c *Config, status int) []byte {
	items := make([]map[string]any, 0, len(results))
	for _, r := range results {
		items = append(items, shape(r, c))
	}
	var payload any
	switch len(items) {
	case 0:
		payload = map[string]any{FieldStatusCode: status, FieldStatus: "no_recipients"}
	case 1:
		payload = items[0]
	default:
		envelope := map[string]any{
			FieldStatusCode: status,
			FieldStatus:     uniformStatus(results),
			"results":       items,
		}
		// The batch id also sits on the envelope, so a multi-recipient client
		// need not dig into items for it. Every result of one request shares
		// the same RequestID by construction.
		if results[0].RequestID != "" {
			envelope[FieldRequestID] = results[0].RequestID
		}
		payload = envelope
	}
	b, err := json.Marshal(payload)
	if err != nil {
		// Marshalling a map of strings/ints cannot realistically fail; if it
		// somehow does, answer with something valid rather than an empty body.
		return []byte(`{"statusCode":500,"message":"INTERNAL_ERROR"}`)
	}
	return b
}

// uniformStatus mirrors envelopeStatus for the envelope's status field — same
// uppercase vocabulary as each item's status (ACCEPTED / <REASON> / PARTIAL).
func uniformStatus(results []Result) string {
	if len(results) == 0 {
		return "NO_RECIPIENTS"
	}
	first := "ACCEPTED"
	if !results[0].Accepted {
		first = results[0].Reason
	}
	for _, r := range results[1:] {
		cur := "ACCEPTED"
		if !r.Accepted {
			cur = r.Reason
		}
		if cur != first {
			return "PARTIAL"
		}
	}
	return first
}

// renderText renders one template line per recipient. Placeholders are
// {canonicalField} — or {alias} when a success mapping renames them.
func renderText(results []Result, c *Config) string {
	tmpl := ""
	if c != nil {
		tmpl = c.Response.TextTemplate
	}
	if tmpl == "" {
		tmpl = "{statusCode}|{status}|{messageId}"
	}
	lines := make([]string, 0, len(results))
	for _, r := range results {
		vals := shape(r, c)
		// A text template chooses its own placeholders, so it may still name
		// an extra ({sender}, {body}, {responseDateTime}) without mapping it.
		for k, v := range canonical(r, c) {
			if _, ok := vals[k]; !ok && extraFields[k] {
				vals[k] = v
			}
		}
		line := tmpl
		for k, v := range vals {
			line = strings.ReplaceAll(line, "{"+k+"}", toString(v))
		}
		// Any placeholder with no value resolves to empty rather than being
		// left as literal braces in a customer-facing response.
		line = stripPlaceholders(line)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func stripPlaceholders(s string) string {
	for {
		i := strings.Index(s, "{")
		if i < 0 {
			return s
		}
		j := strings.Index(s[i:], "}")
		if j < 0 {
			return s
		}
		s = s[:i] + s[i+j+1:]
	}
}

// xmlEntry is one key/value pair rendered as its own element.
type xmlEntry struct {
	XMLName xml.Name
	Value   string `xml:",chardata"`
}

func renderXML(results []Result, c *Config) []byte {
	root := "response"
	if c != nil && c.Response.XMLRoot != "" {
		root = c.Response.XMLRoot
	}
	var sb strings.Builder
	sb.WriteString(xml.Header)
	sb.WriteString("<" + root + ">")
	for _, r := range results {
		vals := shape(r, c)
		if len(results) > 1 {
			sb.WriteString("<result>")
		}
		for _, k := range sortedKeys(vals) {
			var esc strings.Builder
			_ = xml.EscapeText(&esc, []byte(toString(vals[k])))
			sb.WriteString("<" + k + ">" + esc.String() + "</" + k + ">")
		}
		if len(results) > 1 {
			sb.WriteString("</result>")
		}
	}
	sb.WriteString("</" + root + ">")
	return []byte(sb.String())
}

// sortedKeys keeps element order stable so responses are byte-comparable in
// tests and diffable in support tickets.
func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// PushNames is the dialect applied to what WE send the customer — receipt
// and inbound webhooks (PO 2026-09-17). The response mapping already says
// what the customer calls each of our canonical fields; a push uses the same
// names, so a customer who reads "msgId" from our accept reply reads "msgId"
// in their receipts too. The result maps our push key to the customer's
// name; keys with no mapping keep their native name. Nil when there is no
// mapping at all, so callers can skip the rename.
//
// Push keys: receipts send messageId, reference, status, receipt; inbound
// sends messageId, from, to, text. "from" follows the mapping of `sender`,
// "text" that of `body`.
//
// A response field is read as the reply builder reads it (project): Alias is
// the CUSTOMER's name, Source is OUR field (Alias again when Source is
// empty). Until 2026-10-12 this read the two the other way round, so it never
// matched a real dialect: a customer whose reply says "messageid" got
// receipts that said "messageId" (go-live test P1 B4, Jazz and V3 dialects).
// The test beside it had been written the same wrong way round.
//
//   - A nested name ("result.messageid") is used by its last part: a push
//     body is flat.
//   - A field with a value map is NOT followed: the map turns the accept
//     reply's status into the customer's codes (ACCEPTED -> 300), and a
//     receipt's status (delivered, failed) is a different thing under the
//     same key. Such a field keeps its native name in a push.
//   - The first mapping of a field wins, as the reply lists it.
//   - A dialect with one field list (Fields, 6 Nov 2026) is read from it,
//     leaving out the fields sent only on a refusal; Success when it has none.
func PushNames(r *Response) map[string]string {
	if r == nil {
		return nil
	}
	list := r.Success
	if len(r.Fields) > 0 {
		list = fieldsWhen(r.Fields, true)
	}
	if len(list) == 0 {
		return nil
	}
	canon := map[string]string{ // our push key -> the canonical field it follows
		FieldMessageID: FieldMessageID,
		FieldReference: FieldReference,
		FieldStatus:    FieldStatus,
		"from":         FieldSender,
		FieldTo:        FieldTo,
		"text":         FieldBody,
	}
	theirs := map[string]string{} // our canonical field -> the customer's name for it
	for _, f := range list {
		ours := f.Source
		if ours == "" {
			ours = f.Alias
		}
		name := f.Alias
		if i := strings.LastIndex(name, "."); i >= 0 {
			name = name[i+1:]
		}
		if ours == "" || name == "" || len(f.Map) > 0 {
			continue
		}
		if _, seen := theirs[ours]; !seen {
			theirs[ours] = name
		}
	}
	out := map[string]string{}
	for key, field := range canon {
		if name, ok := theirs[field]; ok && name != key {
			out[key] = name
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Renamed applies PushNames to a flat body: keys with a customer name move
// under it, the rest stay. Nil names = the body unchanged.
func Renamed(body map[string]string, names map[string]string) map[string]string {
	if len(names) == 0 {
		return body
	}
	out := make(map[string]string, len(body))
	for k, v := range body {
		if n, ok := names[k]; ok {
			out[n] = v
		} else {
			out[k] = v
		}
	}
	return out
}

// PushNamesJSON is PushNames over the stored account_dialects.response_map
// text; "" or unreadable JSON means no rename.
func PushNamesJSON(raw string) map[string]string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var r Response
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return nil
	}
	return PushNames(&r)
}
