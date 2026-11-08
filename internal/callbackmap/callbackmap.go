// Package callbackmap reads a vendor's callback — a delivery receipt on
// /dlr/<cid> or an incoming message on /mo/<cid> — by the vendor's OWN field
// names, and answers it the way the vendor expects (PO 2026-09-24: "I can't
// see any request and response mapping of bodies anywhere").
//
// It is the inbound twin of an account's dialect, and since 2026-10-08 it is
// written in the SAME model (Saleem: "i need dialects of mo and dlr, to be
// similar to mt, i mean mapping, etc be same as mt, request, response, status
// code, each and everything"). A dialect of kind dlr or mo is stored exactly
// as a client's (kind mt) dialect is — internal/dialect:
//
//	request_map     a LIST of fields {alias, source, in, default, map}
//	                alias  = OUR field        source = THEIR name or dot path
//	                in     = header | query | body | "" (body, then the query)
//	                default = the value when they send none
//	                map    = their value -> ours
//	request_format  json | form | xml | empty (read the Content-Type, and the
//	                body's own shape)
//	request_wrapper the form field (or query parameter) that holds the whole
//	                document, for a vendor that posts xml=<Report>…
//	response_map    {format json|text|xml, success:[fields], status_code,
//	                 errors:{REASON:{status_code, fields}}, text_template,
//	                 xml_root, content_type}
//	keep_unmapped   true = the reply also carries the values no field used
//
// Only the WORDS differ per kind:
//
//	our request fields   dlr: message_id*, status*, error_code, done_at, to
//	                     mo:  from*, to*, text, message_id, operator, coding
//	                     (* = must be in the list)
//	values of a reply    statusCode, status, error, message_id,
//	                     vendor_message_id, count          (see reply.go)
//	reason codes         STORE_UNAVAILABLE, UNREADABLE, and "*" (see reply.go)
//
// What a callback needs and a client's API has no word for sits beside the
// thing it belongs to, in the same list:
//
//	a batch         one more entry of the list, {"alias":"list","source":
//	                "results"}: the path to an array. One callback is made of
//	                every item, and body paths are then read inside the item.
//	status words    the value map of the dlr "status" field: the vendor's word
//	                -> one of the eight states. (The word itself is kept; the
//	                map is what dlr.ParseStat reads it through.)
//	delivery time   "format" and "zone" on the dlr "done_at" field (doneat.go)
//	text format     "format" on the mo "text" field: plain | ucs2_hex |
//	                utf8_hex | by_coding
//
// Where a value is (source), in the body:
//
//	a.b.0.c        a JSON body (objects and arrays), an XML body (elements,
//	               "@attr" for an attribute) or a form body. Names match
//	               without regard to case; one wrapping element may be left out
//	regex:(...)    the first capture group, over the query string and body —
//	               for bodies that are neither JSON, XML nor a form
//
// THE OLDER STORED SHAPE STILL LOADS. Until 2026-10-08 a dlr / mo dialect was
// stored as request_map {list, fields{ours: their path}, status_map,
// text_format, time_format, time_zone} and response_map {ok, fail: {status,
// content_type, body}} (config/0125, moved to dialects by config/0128). Rows
// in that shape are read with no manual step: the request is turned into the
// list above in memory (legacy.go) and read by the one reader; the reply is
// answered by the code that always answered it. Nothing rewrites the row —
// it takes the new shape when an operator next saves it.
//
// A door with no dialect, or a dialect with no request list, reads a receipt
// with the default reading (default.go, issue 19, 2026-10-27: it used to need
// the vendor's "DLR webhook" HTTP call) and an incoming message by guessing
// common names (the mo role); it answers an empty 200.
package callbackmap

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/yakugate/yakugate/internal/dialect"
)

// The two kinds of door.
const (
	KindDLR = "dlr"
	KindMO  = "mo"
)

// Fields each kind reads, and which of them a callback cannot do without.
var (
	// A receipt's "to" is the handset it is about (issue 19, 2026-10-27): it
	// tells apart the messages of a bulk call the vendor gave ONE id (dlr
	// groups.go, rule 1). Only the vendor's "DLR webhook" call could say it
	// before.
	dlrFields = []string{"message_id", "status", "error_code", "done_at", "to"}
	moFields  = []string{"from", "to", "text", "message_id", "operator", "coding"}
	required  = map[string][]string{KindDLR: {"message_id", "status"}, KindMO: {"from", "to"}}
)

// FieldsFor lists the fields a dialect of this kind can map, in screen order.
func FieldsFor(kind string) []string {
	if kind == KindMO {
		return append([]string(nil), moFields...)
	}
	return append([]string(nil), dlrFields...)
}

// RequiredFor lists the fields a mapping for this kind must name.
func RequiredFor(kind string) []string { return append([]string(nil), required[kind]...) }

// AliasList is the one entry of a request list that is not a field: its
// source is the path to an array, and one callback is made of every item.
const AliasList = "list"

// maxSourceDepth bounds one source path. A vendor's document nests deeper
// than a client's submit (a SOAP envelope is four levels before the first
// value), so this is wider than a client dialect's five.
const maxSourceDepth = 8

// Text formats an incoming message's text may arrive in.
const (
	TextPlain   = "plain"
	TextUCS2Hex = "ucs2_hex"  // UTF-16 big-endian bytes as hex: "0633064406270645"
	TextUTF8Hex = "utf8_hex"  // UTF-8 bytes as hex
	TextByCode  = "by_coding" // the mapped "coding" field decides: 8 / ucs2 / unicode = ucs2_hex
)

// TextFormats lists them, in screen order.
func TextFormats() []string { return []string{TextPlain, TextUCS2Hex, TextUTF8Hex, TextByCode} }

// States a receipt's word may be mapped onto: SMPP 3.4's eight (dlr.Stat).
var states = []string{"DELIVRD", "EXPIRED", "DELETED", "UNDELIV", "ACCEPTD", "UNKNOWN", "REJECTD", "ENROUTE"}

// States lists what a status word may be translated into.
func States() []string { return append([]string(nil), states...) }

// Field is one line of a request list: a client dialect's field, plus the two
// settings only a callback has.
type Field struct {
	dialect.Field
	// Format says how the value is written. On a receipt's done_at: auto |
	// unix | unix_ms | smpp | iso | a pattern like DD/MM/YYYY HH:mm:ss. On an
	// incoming message's text: plain | ucs2_hex | utf8_hex | by_coding.
	// Refused on any other field.
	Format string `json:"format,omitempty"`
	// Zone is the time zone of a done_at written without one: UTC, +05:00 or
	// a name like Asia/Karachi. Refused on any other field.
	Zone string `json:"zone,omitempty"`
}

// Options are the two request settings a dialect keeps beside its list.
type Options struct {
	Format  string // request_format: "" | json | form | xml
	Wrapper string // request_wrapper: the form field holding the document
}

// Request says where each of our fields is in the vendor's callback.
type Request struct {
	// Mapping is the list, without its "list" entry.
	Mapping []Field
	// List, when set, is the path to an array: one callback per item.
	List string
	// Format pins how the body is read; Wrapper names the field holding it.
	Format  string
	Wrapper string
	// StatusMap is the receipt status field's value map: the vendor's word ->
	// one of the eight states (DLR only). Words it does not list still go
	// through the common spellings.
	StatusMap map[string]string
	// TextFormat is the format of the incoming text field (MO only).
	TextFormat string
	// TimeFormat and TimeZone are the done_at field's format and zone (DLR
	// only; see doneat.go). Empty = auto and UTC.
	TimeFormat string
	TimeZone   string
	// Legacy is true when the row is still stored in the older shape.
	Legacy bool

	kind    string
	regexes map[string]*regexp.Regexp
	// guess, when set, makes this the default reading of a receipt door with
	// no dialect (default.go): each field by the first of its names.
	guess []DefaultField
}

// Kind is the door this mapping was parsed for.
func (q *Request) Kind() string { return q.kind }

// ParseRequest reads and checks a stored request list with no other setting.
func ParseRequest(raw, kind string) (*Request, error) {
	return ParseRequestWith(raw, kind, Options{})
}

// ParseRequestWith reads and checks a stored request mapping — the list, or
// the older object — with the dialect's request_format and request_wrapper.
// "" / "[]" / "{}" / "null" = no mapping (nil, nil): the door keeps the old
// behaviour. Strict — an unknown key, an unknown field, a missing required
// field or a pattern that does not compile is refused, because a mapping that
// half-loads reads the wrong value out of a live callback.
func ParseRequestWith(raw, kind string, opt Options) (*Request, error) {
	raw = strings.TrimSpace(raw)
	opt.Format, opt.Wrapper = strings.TrimSpace(opt.Format), strings.TrimSpace(opt.Wrapper)
	if raw == "" || raw == "{}" || raw == "null" || raw == "[]" {
		if opt.Format != "" || opt.Wrapper != "" {
			// With no list the door reads callbacks the old way, which knows
			// neither setting: saying so beats ignoring them.
			return nil, errors.New("request mapping: request_format and request_wrapper need a request mapping — without one the door reads callbacks the old way")
		}
		return nil, nil
	}
	if kind != KindDLR && kind != KindMO {
		return nil, fmt.Errorf("kind must be %s or %s", KindDLR, KindMO)
	}
	var (
		q   *Request
		err error
	)
	if strings.HasPrefix(raw, "[") {
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.DisallowUnknownFields()
		var fields []Field
		if derr := dec.Decode(&fields); derr != nil {
			return nil, fmt.Errorf("request mapping: %w", derr)
		}
		q, err = build(kind, fields, false)
	} else {
		q, err = parseLegacyRequest(raw, kind)
	}
	if err != nil {
		return nil, err
	}
	switch opt.Format {
	case "", dialect.RequestFormatJSON, dialect.RequestFormatForm, dialect.RequestFormatXML:
	default:
		return nil, fmt.Errorf("request mapping: request_format %q must be json, form, xml, or empty (read the Content-Type)", opt.Format)
	}
	if werr := dialect.CheckWrapper(opt.Wrapper); werr != nil {
		return nil, fmt.Errorf("request mapping: %s", strings.TrimPrefix(werr.Error(), "dialect: "))
	}
	q.Format, q.Wrapper = opt.Format, opt.Wrapper
	return q, nil
}

// build checks a list and makes the reader. legacy = the list was made from
// the older stored shape: its paths were accepted before the size rules of
// the list existed, and still load.
func build(kind string, fields []Field, legacy bool) (*Request, error) {
	q := &Request{kind: kind, Legacy: legacy, regexes: map[string]*regexp.Regexp{}}
	allowed := map[string]bool{}
	for _, f := range FieldsFor(kind) {
		allowed[f] = true
	}
	seen := map[string]bool{}
	for _, f := range fields {
		f.Alias, f.Source = strings.TrimSpace(f.Alias), strings.TrimSpace(f.Source)
		f.Format, f.Zone = strings.TrimSpace(f.Format), strings.TrimSpace(f.Zone)
		if f.Alias == "" {
			return nil, errors.New("request mapping: an entry has no field name (alias)")
		}
		if seen[f.Alias] {
			return nil, fmt.Errorf("request mapping: %q is mapped twice", f.Alias)
		}
		seen[f.Alias] = true
		if !legacy && strings.Count(f.Source, ".")+1 > maxSourceDepth && !strings.HasPrefix(f.Source, "regex:") {
			return nil, fmt.Errorf("request mapping: %s: the path %q is deeper than %d levels", f.Alias, f.Source, maxSourceDepth)
		}
		if f.Alias == AliasList {
			if f.Source == "" {
				return nil, errors.New(`request mapping: "list" needs the path of the list in the body (source)`)
			}
			if f.Default != "" || len(f.Map) > 0 || f.Format != "" || f.Zone != "" ||
				(f.SentIn != "" && f.SentIn != dialect.TokenSourceBody) || strings.HasPrefix(f.Source, "regex:") {
				return nil, errors.New(`request mapping: "list" takes only the path of the list in the body`)
			}
			q.List = f.Source
			continue
		}
		if !allowed[f.Alias] {
			return nil, fmt.Errorf("request mapping: %q is not a %s field (expected %s)",
				f.Alias, strings.ToUpper(kind), strings.Join(FieldsFor(kind), ", "))
		}
		switch f.SentIn {
		case "", dialect.TokenSourceHeader, dialect.TokenSourceQuery, dialect.TokenSourceBody:
		default:
			return nil, fmt.Errorf(`request mapping: %s: "in" %q must be header, query, body, or empty (the body, then the query string)`, f.Alias, f.SentIn)
		}
		if pat, ok := strings.CutPrefix(f.Source, "regex:"); ok {
			re, err := regexp.Compile(pat)
			if err != nil {
				return nil, fmt.Errorf("request mapping: %s: the pattern does not compile: %v", f.Alias, err)
			}
			if re.NumSubexp() < 1 {
				return nil, fmt.Errorf("request mapping: %s: the pattern needs one (…) group around the value", f.Alias)
			}
			if f.SentIn != "" {
				return nil, fmt.Errorf(`request mapping: %s: a regex reads the query string and the body — leave "in" empty`, f.Alias)
			}
			q.regexes[f.Alias] = re
		} else if !legacy && (strings.HasPrefix(f.Source, "query:") || strings.HasPrefix(f.Source, "header:")) {
			// The older shape wrote the place into the path. One spelling now.
			return nil, fmt.Errorf(`request mapping: %s: write the name alone in "source" and the place in "in" (query or header)`, f.Alias)
		}
		switch {
		case kind == KindDLR && f.Alias == "done_at":
			if err := checkTimeSettings(f.Format, f.Zone); err != nil {
				return nil, err
			}
			q.TimeFormat, q.TimeZone = f.Format, f.Zone
		case kind == KindMO && f.Alias == "text":
			if f.Zone != "" {
				return nil, errors.New("request mapping: text takes no zone (a zone is for a receipt's done_at)")
			}
			switch f.Format {
			case "", TextPlain, TextUCS2Hex, TextUTF8Hex, TextByCode:
			default:
				return nil, fmt.Errorf("request mapping: text: format must be %s", strings.Join(TextFormats(), ", "))
			}
			q.TextFormat = f.Format
		default:
			if f.Format != "" || f.Zone != "" {
				return nil, fmt.Errorf("request mapping: %s takes no format or zone (they are for a receipt's done_at and an incoming message's text)", f.Alias)
			}
		}
		if kind == KindDLR && f.Alias == "status" {
			for word, st := range f.Map {
				if !validState(st) {
					return nil, fmt.Errorf("request mapping: status word %q → %q is not one of %s",
						word, st, strings.Join(states, ", "))
				}
			}
			if len(f.Map) > 0 {
				q.StatusMap = f.Map
			}
		}
		q.Mapping = append(q.Mapping, f)
	}
	for _, name := range required[kind] {
		if !seen[name] {
			return nil, fmt.Errorf("request mapping: %q is required — say where the vendor puts it", name)
		}
	}
	if q.TextFormat == TextByCode && !seen["coding"] {
		return nil, errors.New("request mapping: text format by_coding needs the coding field mapped")
	}
	return q, nil
}

func validState(s string) bool {
	for _, v := range states {
		if strings.EqualFold(strings.TrimSpace(s), v) {
			return true
		}
	}
	return false
}

// Values is one callback's values, by our field names. Text is already
// decoded from its text format.
type Values map[string]string

// Where a value was found, for "Try it".
const (
	FromBody    = "body"
	FromQuery   = "query"
	FromHeader  = "header"
	FromRegex   = "regex"
	FromDefault = "default"
)

// FieldRead is how one field of one callback was read: what the dialect asked
// for, where the value was found, and what the gateway takes it as.
type FieldRead struct {
	Field  string `json:"field"`           // our name
	Source string `json:"source"`          // their name or path
	In     string `json:"in,omitempty"`    // where the dialect says it is; "" = the body, then the query
	From   string `json:"from,omitempty"`  // where it was found: body, query, header, regex, default; "" = nowhere
	Raw    string `json:"raw,omitempty"`   // as the vendor sent it
	Value  string `json:"value,omitempty"` // as the gateway takes it: after the default, the value map and the text format
}

// Item is one callback: its values, and its own text to keep as the raw
// record — the whole request for a single callback, the item alone (as
// JSON) for one of a batch, so each receipt's record shows its own line.
type Item struct {
	Values Values
	Raw    string
	// Read is filled by ReadTraced only: one line per field of the list.
	Read []FieldRead
}

// ErrUnreadable is returned when the body could not be decoded as what the
// dialect says it is, the wrapper field is missing, or the list path did not
// lead to a list.
var ErrUnreadable = errors.New("callback body could not be read")

// Read decodes one HTTP callback into one Item per callback it carries.
func (q *Request) Read(r *http.Request, body []byte) ([]Item, error) {
	return q.read(r, body, false)
}

// ReadTraced is Read that also says, field by field, where each value was
// found — what "Try it" shows.
func (q *Request) ReadTraced(r *http.Request, body []byte) ([]Item, error) {
	return q.read(r, body, true)
}

func (q *Request) read(r *http.Request, body []byte, trace bool) ([]Item, error) {
	if q.guess != nil {
		return q.readDefault(r, body, trace)
	}
	doc, err := q.document(r, body)
	if err != nil {
		return nil, err
	}
	raw := r.URL.RawQuery + "\n" + string(body)
	items := []any{doc}
	if q.List != "" {
		v, ok := getPath(doc, q.List)
		arr, isArr := v.([]any)
		if !ok || !isArr {
			// An XML list of one comes back as a single element, not a list.
			if m, isMap := v.(map[string]any); ok && isMap {
				arr, isArr = []any{m}, true
			}
		}
		if !isArr {
			return nil, fmt.Errorf("%w: %q is not a list", ErrUnreadable, q.List)
		}
		items = arr
	}
	out := make([]Item, 0, len(items))
	for _, it := range items {
		itemRaw := raw
		if q.List != "" {
			if b, err := json.Marshal(it); err == nil {
				itemRaw = string(b)
			}
		}
		vals := Values{}
		var reads []FieldRead
		for _, f := range q.Mapping {
			v, from := q.lookup(f, it, r, raw)
			sent := v
			if v == "" && f.Default != "" {
				v, from = f.Default, FromDefault
			}
			// A receipt's status keeps the vendor's own word: its value map is
			// the status words dlr.ParseStat reads it through (StatusMap), and
			// the word itself is what the record and the receipt rules see.
			if v != "" && len(f.Map) > 0 && !(q.kind == KindDLR && f.Alias == "status") {
				if mapped, ok := f.Map[v]; ok {
					v = mapped
				}
			}
			if v != "" {
				vals[f.Alias] = v
			}
			if trace {
				reads = append(reads, FieldRead{Field: f.Alias, Source: sourceOf(f), In: f.SentIn, From: from, Raw: sent, Value: v})
			}
		}
		if q.kind == KindMO {
			if t, ok := vals["text"]; ok {
				vals["text"] = DecodeText(t, q.TextFormat, vals["coding"])
				for i := range reads {
					if reads[i].Field == "text" {
						reads[i].Value = vals["text"]
					}
				}
			}
		}
		out = append(out, Item{Values: vals, Raw: itemRaw, Read: reads})
	}
	return out, nil
}

// sourceOf is the vendor's name for a field: an empty source means the same
// name as ours, as in a client's dialect.
func sourceOf(f Field) string {
	if f.Source == "" {
		return f.Alias
	}
	return f.Source
}

// lookup finds one field's value and says where it was.
func (q *Request) lookup(f Field, item any, r *http.Request, raw string) (string, string) {
	src := sourceOf(f)
	found := func(v, where string) (string, string) {
		if v == "" {
			return "", ""
		}
		return v, where
	}
	if strings.HasPrefix(src, "regex:") {
		re := q.regexes[f.Alias]
		if re == nil {
			return "", ""
		}
		if m := re.FindStringSubmatch(raw); len(m) > 1 {
			return found(strings.TrimSpace(m[1]), FromRegex)
		}
		return "", ""
	}
	switch f.SentIn {
	case dialect.TokenSourceQuery:
		return found(queryValue(r, src), FromQuery)
	case dialect.TokenSourceHeader:
		return found(r.Header.Get(src), FromHeader)
	case dialect.TokenSourceBody:
		v, _ := bodyValue(item, src)
		return found(v, FromBody)
	}
	// No place named: the body, and only when the body does not have the name
	// at all, the query string — vendors that GET their callbacks put every
	// field there. Never a header: a request carries headers of its own
	// (From, Date) that nobody meant as a field; a header is read only when
	// the dialect names it.
	if v, ok := bodyValue(item, src); ok {
		return found(v, FromBody)
	}
	return found(queryValue(r, src), FromQuery)
}

// bodyValue reads a path from one item of the body. ok says the path is
// there (its value may still be empty).
func bodyValue(item any, path string) (string, bool) {
	if v, ok := getPath(item, path); ok {
		return scalar(v), true
	}
	// One wrapping element the operator left out of the path — an XML root
	// (<DeliveryReport>…) or a JSON envelope ({"data":{…}}): look inside it.
	if m, ok := item.(map[string]any); ok && len(m) == 1 {
		for _, inner := range m {
			if v, ok := getPath(inner, path); ok {
				return scalar(v), true
			}
		}
	}
	return "", false
}

// queryValue reads one query parameter: by its exact name, else without
// regard to case.
func queryValue(r *http.Request, name string) string {
	vals := r.URL.Query()
	if v := vals.Get(name); v != "" {
		return v
	}
	for k, vs := range vals {
		if len(vs) > 0 && vs[0] != "" && strings.EqualFold(k, name) {
			return vs[0]
		}
	}
	return ""
}

// document is the body as a tree: what is inside the wrapper field when the
// dialect names one, read as the pinned format when it pins one.
func (q *Request) document(r *http.Request, body []byte) (any, error) {
	ct := r.Header.Get("Content-Type")
	src := body
	if q.Wrapper != "" {
		doc := wrapped(q.Wrapper, r, body)
		if doc == "" {
			return nil, fmt.Errorf("%w: the wrapper field %q is not in the call", ErrUnreadable, q.Wrapper)
		}
		// The document inside says what it is by its own shape.
		src, ct = []byte(doc), ""
	}
	trimmed := bytes.TrimSpace(src)
	switch q.Format {
	case dialect.RequestFormatJSON:
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("%w: the body is not JSON", ErrUnreadable)
		}
		return v, nil
	case dialect.RequestFormatXML:
		v, err := decodeXML(trimmed)
		if err != nil {
			return nil, fmt.Errorf("%w: the body is not XML", ErrUnreadable)
		}
		return v, nil
	case dialect.RequestFormatForm:
		vals, err := url.ParseQuery(string(trimmed))
		if err != nil {
			return nil, fmt.Errorf("%w: the body is not a form", ErrUnreadable)
		}
		out := map[string]any{}
		for k, v := range vals {
			if len(v) > 0 {
				out[k] = v[0]
			}
		}
		return out, nil
	}
	return decode(ct, src), nil
}

// wrapped is the document a vendor sends inside one field: a form field of
// the body, else a query parameter, by the wrapper's name (case does not
// matter). "" = not there.
func wrapped(name string, r *http.Request, body []byte) string {
	if form, err := url.ParseQuery(string(bytes.TrimSpace(body))); err == nil {
		for k, vs := range form {
			if strings.EqualFold(k, name) && len(vs) > 0 && strings.TrimSpace(vs[0]) != "" {
				return strings.TrimSpace(vs[0])
			}
		}
	}
	return strings.TrimSpace(queryValue(r, name))
}

// decode turns a body into a tree: JSON by content type or shape, XML the
// same, else a form. Nothing is refused here — an unreadable body is an
// empty tree, and the fields that were needed are simply absent.
func decode(contentType string, body []byte) any {
	ct := strings.ToLower(contentType)
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return map[string]any{}
	}
	isJSON := strings.Contains(ct, "json") || trimmed[0] == '{' || trimmed[0] == '['
	isXML := strings.Contains(ct, "xml") || trimmed[0] == '<'
	if isJSON {
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		dec.UseNumber()
		var v any
		if dec.Decode(&v) == nil {
			return v
		}
	}
	if isXML {
		if v, err := decodeXML(trimmed); err == nil {
			return v
		}
	}
	out := map[string]any{}
	if vals, err := url.ParseQuery(string(trimmed)); err == nil {
		for k, v := range vals {
			if len(v) > 0 {
				out[k] = v[0]
			}
		}
	}
	return out
}

// getPath walks a decoded tree by a dot path; a number steps into a list.
// A name matches case-insensitively when there is no exact match, because
// vendors are not consistent about "MsgId" and "msgid" between endpoints.
func getPath(doc any, path string) (any, bool) {
	cur := doc
	for _, part := range strings.Split(path, ".") {
		switch node := cur.(type) {
		case map[string]any:
			v, ok := node[part]
			if !ok {
				for k, kv := range node {
					if strings.EqualFold(k, part) {
						v, ok = kv, true
						break
					}
				}
			}
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(node) {
				return nil, false
			}
			cur = node[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

// scalar is a leaf as text. An object or a list is not a value.
func scalar(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(t)
	case json.Number:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case map[string]any:
		// An XML element with attributes carries its text under "#text".
		if s, ok := t["#text"].(string); ok {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

// DecodeText turns an incoming message's text into what the handset wrote.
// Text that is not valid hex in a hex format is kept as it came rather than
// dropped: a vendor that sends plain text on some messages loses nothing.
func DecodeText(text, format, coding string) string {
	if format == TextByCode {
		switch strings.ToLower(strings.TrimSpace(coding)) {
		case "8", "08", "ucs2", "ucs-2", "unicode", "utf16", "utf-16":
			format = TextUCS2Hex
		default:
			format = TextPlain
		}
	}
	switch format {
	case TextUCS2Hex:
		b, err := hex.DecodeString(strings.TrimSpace(text))
		if err != nil || len(b)%2 != 0 {
			return text
		}
		u := make([]uint16, len(b)/2)
		for i := range u {
			u[i] = uint16(b[2*i])<<8 | uint16(b[2*i+1])
		}
		return string(utf16.Decode(u))
	case TextUTF8Hex:
		b, err := hex.DecodeString(strings.TrimSpace(text))
		if err != nil || !utf8.Valid(b) {
			return text
		}
		return string(b)
	}
	return text
}

// Describe is the mapping in words, for a log line or a screen.
func (q *Request) Describe() string {
	if q == nil {
		return "none"
	}
	if q.guess != nil {
		return "the default reading"
	}
	parts := make([]string, 0, len(q.Mapping))
	for _, f := range q.Mapping {
		src := sourceOf(f)
		if f.SentIn != "" {
			src = f.SentIn + ":" + src
		}
		parts = append(parts, f.Alias+"←"+src)
	}
	s := strings.Join(parts, ", ")
	if q.List != "" {
		s = "each of " + q.List + ": " + s
	}
	return s
}

// Decode reads a request body into the tree Lookup walks — JSON, XML or a
// form — for callers outside the mapping: the inbound doors read a vendor's
// credential out of the body with it (inboundauth, 2026-09-28).
func Decode(contentType string, body []byte) any { return decode(contentType, body) }

// Lookup reads one value from a decoded body by a dot path, with the same
// rules a mapping uses: names match case-insensitively, a number steps into a
// list, and one wrapping element the path leaves out is looked inside.
func Lookup(doc any, path string) string {
	if v, ok := getPath(doc, path); ok {
		return scalar(v)
	}
	if m, ok := doc.(map[string]any); ok && len(m) == 1 {
		for _, inner := range m {
			if v, ok := getPath(inner, path); ok {
				return scalar(v)
			}
		}
	}
	return ""
}
