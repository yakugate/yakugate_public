package callbackmap

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"regexp"
	"strings"

	"github.com/yakugate/yakugate/internal/dialect"
)

// The older stored shape of a dlr / mo dialect (config/0125 on the door,
// config/0128 on the dialect), kept readable for ever:
//
//	request_map   {list, fields{ours: their path}, status_map, text_format,
//	               time_format, time_zone}
//	response_map  {ok{status, content_type, body}, fail{…}}
//
// config/0128 made one such dialect for every door that had a mapping
// ("<cid> DLR (door N)"), and operators have saved more since. None of them
// is rewritten: the REQUEST is turned into today's field list in memory and
// read by the one reader (so "Try it" and the live door cannot differ), and
// the REPLY is answered by the code below, which is the code that always
// answered it — a vendor sees the same bytes it saw before.
//
// The Admin API shows these rows in the new shape (services/callbackMap.js,
// upgradeRequestMap / upgradeResponseMap) and saves them in it. Its request
// conversion is this file's legacyFields, line for line — change them
// together; the shared cases in sqa/e2e/shared/callback-dialect-vectors.json
// hold both to one answer.

// legacyRequest is the older request_map.
type legacyRequest struct {
	List       string            `json:"list,omitempty"`
	Fields     map[string]string `json:"fields"`
	StatusMap  map[string]string `json:"status_map,omitempty"`
	TextFormat string            `json:"text_format,omitempty"`
	TimeFormat string            `json:"time_format,omitempty"`
	TimeZone   string            `json:"time_zone,omitempty"`
}

// parseLegacyRequest reads the older shape with the rules it was saved under,
// then makes the reader from the list it converts to.
func parseLegacyRequest(raw, kind string) (*Request, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var l legacyRequest
	if err := dec.Decode(&l); err != nil {
		return nil, fmt.Errorf("request mapping: %w", err)
	}
	allowed := map[string]bool{}
	for _, f := range FieldsFor(kind) {
		allowed[f] = true
	}
	for name := range l.Fields {
		if !allowed[name] {
			return nil, fmt.Errorf("request mapping: %q is not a %s field (expected %s)",
				name, strings.ToUpper(kind), strings.Join(FieldsFor(kind), ", "))
		}
	}
	for _, name := range required[kind] {
		if strings.TrimSpace(l.Fields[name]) == "" {
			return nil, fmt.Errorf("request mapping: %q is required — say where the vendor puts it", name)
		}
	}
	if kind == KindDLR {
		if l.TextFormat != "" {
			return nil, errors.New("request mapping: text_format is for incoming messages only")
		}
		if err := checkTimeSettings(l.TimeFormat, l.TimeZone); err != nil {
			return nil, err
		}
	} else {
		if len(l.StatusMap) > 0 {
			return nil, errors.New("request mapping: status_map is for receipts only")
		}
		if l.TimeFormat != "" || l.TimeZone != "" {
			return nil, errors.New("request mapping: time_format and time_zone are for receipts only")
		}
		switch l.TextFormat {
		case "", TextPlain, TextUCS2Hex, TextUTF8Hex:
		case TextByCode:
			if strings.TrimSpace(l.Fields["coding"]) == "" {
				return nil, errors.New("request mapping: text_format by_coding needs the coding field mapped")
			}
		default:
			return nil, errors.New("request mapping: text_format must be plain, ucs2_hex, utf8_hex or by_coding")
		}
	}
	return build(kind, legacyFields(kind, l), true)
}

// legacyFields turns the older request_map into today's list, value for
// value:
//
//	fields.X = "query:name"    {alias X, source name, in query}
//	fields.X = "header:name"   {alias X, source name, in header}
//	fields.X = "regex:(…)"     {alias X, source "regex:(…)"}
//	fields.X = "a.b.c"         {alias X, source "a.b.c", in body}
//	fields.X = "name"          {alias X, source "name"}   body, then query —
//	                           exactly what a plain name always meant
//	list                       {alias "list", source list}
//	status_map                 the "status" field's map
//	time_format / time_zone    the "done_at" field's format / zone
//	text_format                the "text" field's format
//
// Fields come out in the kind's own order, so two readings of one row agree.
func legacyFields(kind string, l legacyRequest) []Field {
	var out []Field
	if list := strings.TrimSpace(l.List); list != "" {
		out = append(out, Field{Field: dialect.Field{Alias: AliasList, Source: list}})
	}
	for _, name := range FieldsFor(kind) {
		path := strings.TrimSpace(l.Fields[name])
		if path == "" {
			continue
		}
		f := Field{Field: dialect.Field{Alias: name, Source: path}}
		switch {
		case strings.HasPrefix(path, "regex:"):
		case strings.HasPrefix(path, "query:") && len(path) > len("query:"):
			f.Source, f.SentIn = strings.TrimPrefix(path, "query:"), dialect.TokenSourceQuery
		case strings.HasPrefix(path, "header:") && len(path) > len("header:"):
			f.Source, f.SentIn = strings.TrimPrefix(path, "header:"), dialect.TokenSourceHeader
		case strings.Contains(path, "."), strings.HasPrefix(path, "query:"), strings.HasPrefix(path, "header:"):
			// A dotted path was read in the body only. ("query:" or "header:"
			// with no name read nothing, and still reads nothing.)
			f.SentIn = dialect.TokenSourceBody
		}
		switch {
		case kind == KindDLR && name == "status" && len(l.StatusMap) > 0:
			f.Map = l.StatusMap
		case kind == KindDLR && name == "done_at":
			f.Format, f.Zone = strings.TrimSpace(l.TimeFormat), strings.TrimSpace(l.TimeZone)
		case kind == KindMO && name == "text":
			f.Format = l.TextFormat
		}
		out = append(out, f)
	}
	return out
}

// legacyReply is the older response_map: one fixed answer for a callback that
// was taken, one for a callback we could not keep.
type legacyReply struct {
	OK   *legacyAnswer `json:"ok,omitempty"`
	Fail *legacyAnswer `json:"fail,omitempty"`
}

// legacyAnswer is one HTTP response of the older shape.
type legacyAnswer struct {
	Status      int    `json:"status,omitempty"`       // default 200 for OK, 503 for Fail
	ContentType string `json:"content_type,omitempty"` // default text/plain
	Body        string `json:"body,omitempty"`         // may use {{message_id}}, {{count}}, {{vendor_message_id}}
}

// Placeholders an older reply body may use.
var Placeholders = []string{"message_id", "vendor_message_id", "count"}

const maxReplyBody = 4096

var placeholderRe = regexp.MustCompile(`\{\{\s*([a-z_]+)\s*\}\}`)

// parseLegacyReply reads the older response_map with the rules it was saved
// under.
func parseLegacyReply(raw string) (*legacyReply, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var r legacyReply
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("response mapping: %w", err)
	}
	for which, a := range map[string]*legacyAnswer{"ok": r.OK, "fail": r.Fail} {
		if a == nil {
			continue
		}
		if a.Status != 0 && (a.Status < 100 || a.Status > 599) {
			return nil, fmt.Errorf("response mapping: %s.status %d is not an HTTP status", which, a.Status)
		}
		if which == "fail" && a.Status != 0 && a.Status < 300 {
			// A "fail" answered 2xx tells the vendor we kept a callback we
			// did not — it will never send it again.
			return nil, errors.New("response mapping: fail.status must not be a success (2xx): the vendor would never resend")
		}
		if len(a.Body) > maxReplyBody {
			return nil, fmt.Errorf("response mapping: %s.body is longer than %d characters", which, maxReplyBody)
		}
		for _, m := range placeholderRe.FindAllStringSubmatch(a.Body, -1) {
			if !knownPlaceholder(m[1]) {
				return nil, fmt.Errorf("response mapping: %s.body uses {{%s}} (expected %s)",
					which, m[1], strings.Join(Placeholders, ", "))
			}
		}
		if strings.ContainsAny(a.ContentType, "\r\n") {
			return nil, fmt.Errorf("response mapping: %s.content_type is not a header value", which)
		}
	}
	return &r, nil
}

func knownPlaceholder(s string) bool {
	for _, p := range Placeholders {
		if s == p {
			return true
		}
	}
	return false
}

// render builds the older shape's answer, as it always was. A missing side
// is what the gateway always answered: 200 with nothing for OK, 503 for Fail.
func (r *legacyReply) render(ok bool, vars map[string]string) Rendered {
	var a *legacyAnswer
	if r != nil {
		if ok {
			a = r.OK
		} else {
			a = r.Fail
		}
	}
	out := Rendered{Status: http.StatusOK, ContentType: plainText}
	if !ok {
		out.Status, out.Body = http.StatusServiceUnavailable, nativeFailBody
	}
	if a == nil {
		return out
	}
	if a.Status != 0 {
		out.Status = a.Status
	}
	if a.ContentType != "" {
		out.ContentType = a.ContentType
	}
	escape := escaperFor(out.ContentType)
	out.Body = placeholderRe.ReplaceAllStringFunc(a.Body, func(m string) string {
		name := placeholderRe.FindStringSubmatch(m)[1]
		return escape(vars[name])
	})
	return out
}

// escaperFor is how a value is written inside a body of this content type, so
// a vendor id with a quote or an angle bracket cannot break the body an
// operator wrote around it.
func escaperFor(contentType string) func(string) string {
	ct := strings.ToLower(contentType)
	switch {
	case strings.Contains(ct, "json"):
		// A value lands inside a JSON string: escaped as a string's contents.
		return func(s string) string {
			b, _ := json.Marshal(s)
			return string(b[1 : len(b)-1])
		}
	case strings.Contains(ct, "xml") || strings.Contains(ct, "html"):
		return html.EscapeString
	}
	return func(s string) string { return s }
}
