package callbackmap

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dialect"
)

// Reply is what the gateway answers a callback it has authenticated — the
// vendor's expected acknowledgement. It is a client dialect's response
// (dialect.Response), with a vendor callback's own words:
//
//	format        json (default) | text | xml
//	success       the field list of the answer to a callback that was taken:
//	              {alias: THEIR key, source: OUR value, default, map}
//	                source given            our value; the default fills in
//	                                        when that value is empty
//	                no source, a default    a FIXED TEXT: the default, always
//	                                        ({"alias":"status","default":
//	                                        "success"} sends "success")
//	                no source, no default   our value of the key's own name
//	status_code   the HTTP status of that answer (empty = 200)
//	errors        {REASON: {status_code, fields}} — the answer for one reason
//	              a callback was not taken; "*" = any reason with no rule of
//	              its own
//	text_template format text: the body, with {value} or {alias} placeholders
//	xml_root      format xml: the root element (empty = "response")
//	content_type  the Content-Type to send instead of the format's own — the
//	              one setting a client's dialect does not have: a vendor that
//	              checks for "text/xml", or a JSON body written as a text
//	              template
//
// The values a field's source (or a text placeholder) may name:
//
//	statusCode         the HTTP status of this answer
//	status             "OK", or the reason code
//	error              "" when taken, else the reason in words
//	message_id         a receipt: the id the receipt named (the vendor's id of
//	                   our message; the last one of a batch). An incoming
//	                   message: OUR id for it (the last one of a batch).
//	vendor_message_id  an incoming message: the vendor's own id for it. Empty
//	                   on a receipt, whose message_id already is the vendor's.
//	count              how many callbacks the call carried
//
// The reason codes an error rule may name:
//
//	STORE_UNAVAILABLE  we could not keep it now (the store is down, or an
//	                   incoming message could not be encrypted) and WANT it
//	                   sent again. With no rule: 503 "temporarily unavailable".
//	                   A rule for it, or "*", may not answer 2xx — the vendor
//	                   would never resend.
//	UNREADABLE         the call could not be read as this dialect says: the
//	                   body is not what it claims, the wrapper field or the
//	                   list is missing, or no callback in it has its required
//	                   fields. With no rule it is answered as taken — sending
//	                   it again would not help — and it is kept as an orphan.
//	*                  either of them, when it has no rule of its own
//
// A rule with a status and no fields changes the status only; the body stays
// what that reason has with no rule. A rule with fields is rendered in the
// dialect's format (in text format, through the same template).
//
// An authentication refusal is NEVER shaped by any of this: a stranger gets
// the same bare 403 whatever the dialect says, so the reply cannot be used to
// tell a real vendor's door from a wrong one. The same holds for everything
// else the door's guard answers (a throttled caller's 429, doors not loaded).
type Reply struct {
	dialect.Response
	// ContentType overrides the Content-Type of a mapped answer.
	ContentType string `json:"content_type,omitempty"`

	keepUnmapped bool
	legacy       *legacyReply
}

// The reasons a callback is not answered as taken.
const (
	ReasonAny        = "*"
	ReasonStore      = "STORE_UNAVAILABLE"
	ReasonUnreadable = "UNREADABLE"
)

// Reasons lists the reason codes an error rule may name, "*" last.
func Reasons() []string { return []string{ReasonStore, ReasonUnreadable, ReasonAny} }

// Values a reply field may read.
const (
	ValueStatusCode      = "statusCode"
	ValueStatus          = "status"
	ValueError           = "error"
	ValueMessageID       = "message_id"
	ValueVendorMessageID = "vendor_message_id"
	ValueCount           = "count"
)

// ResponseValues lists them, in screen order.
func ResponseValues() []string {
	return []string{ValueStatusCode, ValueStatus, ValueError, ValueMessageID, ValueVendorMessageID, ValueCount}
}

const (
	plainText      = "text/plain; charset=utf-8"
	nativeFailBody = "temporarily unavailable\n"
	maxContentType = 100
)

var (
	// A placeholder is {name}: a value above or a mapped alias. Anything else
	// in braces — the braces of a JSON body written as a template — is text.
	textPlaceholderRe = regexp.MustCompile(`\{([A-Za-z_][A-Za-z0-9_.]*)\}`)
	xmlNameRe         = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.:-]*$`)
)

// ParseReply reads and checks a stored reply. "" / "{}" / "null" = none.
func ParseReply(raw string) (*Reply, error) { return ParseReplyWith(raw, false) }

// ParseReplyWith reads a stored reply with the dialect's keep_unmapped: true
// adds, to a JSON or XML answer, every value no field of it used.
func ParseReplyWith(raw string, keepUnmapped bool) (*Reply, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" || raw == "null" {
		return nil, nil
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &keys); err != nil {
		return nil, fmt.Errorf("response mapping: %w", err)
	}
	_, hasOK := keys["ok"]
	_, hasFail := keys["fail"]
	if hasOK || hasFail {
		// The older shape (legacy.go): answered as it always was.
		l, err := parseLegacyReply(raw)
		if err != nil {
			return nil, err
		}
		return &Reply{legacy: l}, nil
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var r Reply
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("response mapping: %w", err)
	}
	r.keepUnmapped = keepUnmapped
	if err := r.check(); err != nil {
		return nil, err
	}
	return &r, nil
}

// check is every rule of the new shape. The Admin API applies the same rules
// at save (services/callbackMap.js cleanResponseMap) — change them together.
func (r *Reply) check() error {
	switch r.Format {
	case "", dialect.FormatJSON, dialect.FormatXML:
	case dialect.FormatText:
		if strings.TrimSpace(r.TextTemplate) == "" {
			return errors.New("response mapping: format text needs a text_template")
		}
	default:
		return fmt.Errorf("response mapping: format %q must be json, text or xml", r.Format)
	}
	if sc := r.StatusCode; sc != 0 && (sc < 100 || sc > 599) {
		return fmt.Errorf("response mapping: status_code %d is not an HTTP status", sc)
	}
	if n := r.fieldCount(); n > dialect.MaxMappings {
		return fmt.Errorf("response mapping: %d fields are more than the %d a dialect may hold", n, dialect.MaxMappings)
	}
	if err := checkReplyFields("success", r.Success); err != nil {
		return err
	}
	for code, rule := range r.Errors {
		if code != ReasonAny && code != ReasonStore && code != ReasonUnreadable {
			return fmt.Errorf("response mapping: %q is not a reason a callback can have (expected %s)",
				code, strings.Join(Reasons(), ", "))
		}
		if sc := rule.StatusCode; sc != 0 && (sc < 100 || sc > 599) {
			return fmt.Errorf("response mapping: errors.%s.status_code %d is not an HTTP status", code, sc)
		}
		if sc := rule.StatusCode; sc != 0 && sc < 300 && code != ReasonUnreadable {
			// Answering 2xx for a callback we did not keep tells the vendor
			// we kept it — it will never send it again.
			return fmt.Errorf("response mapping: errors.%s.status_code must not be a success (2xx): the vendor would never resend", code)
		}
		if err := checkReplyFields("errors."+code, rule.Fields); err != nil {
			return err
		}
	}
	if len(r.TextTemplate) > maxReplyBody {
		return fmt.Errorf("response mapping: text_template is longer than %d characters", maxReplyBody)
	}
	if r.TextTemplate != "" {
		known := map[string]bool{}
		for _, v := range ResponseValues() {
			known[v] = true
		}
		for _, f := range r.Success {
			known[f.Alias] = true
		}
		for _, rule := range r.Errors {
			for _, f := range rule.Fields {
				known[f.Alias] = true
			}
		}
		for _, m := range textPlaceholderRe.FindAllStringSubmatch(r.TextTemplate, -1) {
			if !known[m[1]] {
				return fmt.Errorf("response mapping: text_template uses {%s}, which is neither a value (%s) nor a key of the response mapping",
					m[1], strings.Join(ResponseValues(), ", "))
			}
		}
	}
	if strings.ContainsAny(r.ContentType, "\r\n") || len(r.ContentType) > maxContentType {
		return errors.New("response mapping: content_type is not a header value")
	}
	if r.XMLRoot != "" && !xmlNameRe.MatchString(r.XMLRoot) {
		return fmt.Errorf("response mapping: xml_root %q is not an XML element name", r.XMLRoot)
	}
	return nil
}

// checkReplyFields is the rule for one field list of a reply: a key, and a
// value that exists — a default, or one of the values a callback's answer has.
func checkReplyFields(where string, fields []dialect.Field) error {
	known := map[string]bool{}
	for _, v := range ResponseValues() {
		known[v] = true
	}
	for _, f := range fields {
		if strings.TrimSpace(f.Alias) == "" {
			return fmt.Errorf("response mapping: %s has an entry with no key (alias)", where)
		}
		if strings.Count(f.Alias, ".")+1 > dialect.MaxPathDepth {
			return fmt.Errorf("response mapping: %s key %q is deeper than %d levels", where, f.Alias, dialect.MaxPathDepth)
		}
		if f.SentIn != "" && f.SentIn != dialect.TokenSourceBody {
			return fmt.Errorf(`response mapping: %s key %q: an answer's fields go in the body ("in" must be empty or body)`, where, f.Alias)
		}
		if f.Default != "" {
			continue // always has a value, whatever the source
		}
		src := f.Source
		if src == "" {
			src = f.Alias
		}
		if !known[src] {
			return fmt.Errorf("response mapping: %s key %q reads %q, which is not a value of the answer (%s), and has no default — it would send nothing",
				where, f.Alias, src, strings.Join(ResponseValues(), ", "))
		}
	}
	return nil
}

// fieldCount is how many field lines the reply holds.
func (r *Reply) fieldCount() int {
	if r == nil || r.legacy != nil {
		return 0
	}
	n := len(r.Success)
	for _, rule := range r.Errors {
		n += len(rule.Fields)
	}
	return n
}

// CheckSize is the limit on a whole dialect: its request list and every field
// list of its reply, together, hold MaxMappings lines at most — the same
// limit a client's dialect has. A dialect still in the older shape was saved
// before the limit and is not counted.
func CheckSize(q *Request, r *Reply) error {
	n := r.fieldCount()
	if q != nil && !q.Legacy {
		n += len(q.Mapping)
		if q.List != "" {
			n++
		}
	}
	if n > dialect.MaxMappings {
		return fmt.Errorf("dialect: %d field mappings are more than the %d a dialect may hold", n, dialect.MaxMappings)
	}
	return nil
}

// Legacy reports whether the reply is still stored in the older shape.
func (r *Reply) Legacy() bool { return r != nil && r.legacy != nil }

// Has reports whether the dialect says what to answer for this reason: an
// error rule of its own or the "*" rule (in the older shape, a "fail" answer
// for STORE_UNAVAILABLE).
func (r *Reply) Has(reason string) bool {
	if r == nil {
		return false
	}
	if r.legacy != nil {
		return reason == ReasonStore && r.legacy.Fail != nil
	}
	_, ok := r.RuleFor(reason)
	return ok
}

// Outcome is what became of one HTTP callback, for its answer.
type Outcome struct {
	// Reason is "" when the callback was taken, else a reason code.
	Reason string
	// MessageID and VendorMessageID are the last callback's ids (see Reply).
	MessageID       string
	VendorMessageID string
	// Count is how many callbacks the call carried.
	Count int
}

// Rendered is a reply ready to write.
type Rendered struct {
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	Body        string `json:"body"`
}

// reasonText is a reason in words, for the "error" value.
func reasonText(reason string) string {
	switch reason {
	case "":
		return ""
	case ReasonStore:
		return "could not be kept now — send it again"
	case ReasonUnreadable:
		return "the call could not be read"
	}
	return reason
}

// Render builds the answer. A nil Reply is what the gateway always answered:
// 200 with nothing when taken (or unreadable), 503 when it must be resent.
func (r *Reply) Render(o Outcome) Rendered {
	if r != nil && r.legacy != nil {
		return r.legacy.render(o.Reason != ReasonStore, map[string]string{
			ValueMessageID: o.MessageID, ValueVendorMessageID: o.VendorMessageID, ValueCount: strconv.Itoa(o.Count)})
	}
	native := Rendered{Status: http.StatusOK, ContentType: plainText}
	if o.Reason == ReasonStore {
		native.Status, native.Body = http.StatusServiceUnavailable, nativeFailBody
	}
	if r == nil {
		return native
	}
	var (
		rule    dialect.ErrorRule
		hasRule bool
	)
	if o.Reason != "" {
		rule, hasRule = r.RuleFor(o.Reason)
	}
	// The status: the success status for a callback that was taken — and for
	// an unreadable one, which is answered as taken unless a rule says
	// otherwise; 503 for one that must be resent; a rule's own when it has one.
	status := http.StatusOK
	if r.StatusCode != 0 {
		status = r.StatusCode
	}
	if o.Reason == ReasonStore {
		status = http.StatusServiceUnavailable
	}
	if hasRule && rule.StatusCode != 0 {
		status = rule.StatusCode
	}
	// The body: a rule's own fields; else, for a callback that must be
	// resent, the plain text it always had; else the success answer.
	fields := r.Success
	switch {
	case hasRule && len(rule.Fields) > 0:
		fields = rule.Fields
	case o.Reason == ReasonStore:
		native.Status = status
		return native
	}
	fields = fixedTexts(fields)
	// With no rule an unreadable callback is answered exactly as one that
	// was taken, values and all.
	reason := o.Reason
	if !hasRule {
		reason = ""
	}
	vals := map[string]any{
		ValueStatusCode:      status,
		ValueStatus:          "OK",
		ValueError:           reasonText(reason),
		ValueMessageID:       o.MessageID,
		ValueVendorMessageID: o.VendorMessageID,
		ValueCount:           o.Count,
	}
	if reason != "" {
		vals[ValueStatus] = reason
	}
	out := Rendered{Status: status}
	switch r.Format {
	case dialect.FormatText:
		out.ContentType = plainText
		shaped := vals
		if len(fields) > 0 {
			// The template may name a mapped key or a value; the key wins.
			shaped = dialect.Project(vals, fields)
			for k, v := range vals {
				if _, taken := shaped[k]; !taken {
					shaped[k] = v
				}
			}
		}
		ct := out.ContentType
		if r.ContentType != "" {
			ct = r.ContentType
		}
		escape := escaperFor(ct)
		out.Body = textPlaceholderRe.ReplaceAllStringFunc(r.TextTemplate, func(m string) string {
			v, ok := shaped[m[1:len(m)-1]]
			if !ok {
				return ""
			}
			return escape(dialect.Text(v))
		})
	case dialect.FormatXML:
		out.ContentType = "application/xml; charset=utf-8"
		if len(fields) > 0 {
			out.Body = string(dialect.XMLDocument(r.XMLRoot, r.shaped(vals, fields)))
		}
	default:
		out.ContentType = "application/json"
		if len(fields) > 0 {
			b, err := json.Marshal(r.shaped(vals, fields))
			if err == nil {
				out.Body = string(b)
			}
		}
	}
	if out.Body == "" {
		// Nothing mapped: the empty answer the gateway always gave.
		out.ContentType = plainText
	}
	if r.ContentType != "" {
		out.ContentType = r.ContentType
	}
	return out
}

// neverAValue is a source no answer has: Project misses it and sends the
// field's default.
const neverAValue = "\x00fixed"

// fixedTexts makes a field with a default and no source what an acknowledgement
// most often needs, a fixed text: {"alias":"status","default":"success"} sends
// "success" — not our own "status" value, which is what a key named after a
// value would read if it were taken as its own source. (A key with neither a
// source nor a default still reads the value of its own name.)
func fixedTexts(fields []dialect.Field) []dialect.Field {
	var out []dialect.Field
	for i, f := range fields {
		if f.Source != "" || f.Default == "" {
			continue
		}
		if out == nil {
			out = append([]dialect.Field(nil), fields...)
		}
		out[i].Source = neverAValue
	}
	if out == nil {
		return fields
	}
	return out
}

// shaped is one JSON or XML answer's field set: the mapped keys, then — when
// the dialect says "pass through" (keep_unmapped) — every value no field
// used, under its own name. The rule a client's reply follows (dialect.shape).
func (r *Reply) shaped(vals map[string]any, fields []dialect.Field) map[string]any {
	out := dialect.Project(vals, fields)
	if !r.keepUnmapped {
		return out
	}
	used := map[string]bool{}
	for _, f := range fields {
		src := f.Source
		if src == "" || src == neverAValue {
			src = f.Alias
		}
		used[src] = true
	}
	for k, v := range vals {
		if used[k] {
			continue
		}
		if s, isText := v.(string); isText && s == "" {
			continue
		}
		if _, taken := out[k]; !taken {
			out[k] = v
		}
	}
	return out
}

// Write sends the reply.
func (r *Reply) Write(w http.ResponseWriter, o Outcome) {
	out := r.Render(o)
	if out.Body != "" || r != nil {
		w.Header().Set("Content-Type", out.ContentType)
	}
	w.WriteHeader(out.Status)
	if out.Body != "" {
		_, _ = w.Write([]byte(out.Body))
	}
}
