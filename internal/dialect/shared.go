package dialect

import (
	"encoding/xml"
	"strings"
)

// The parts of a dialect that a second kind of dialect uses as they are.
//
// A client's API (kind mt) is this package. A vendor's callback — a delivery
// receipt (kind dlr) or an incoming message (kind mo), internal/callbackmap —
// is written, stored and shown in the SAME model since 2026-10-08 (Saleem:
// "mapping, etc be same as mt, request, response, status code, each and
// everything"): the request is a list of Field, the reply a Response with its
// field list, formats, status code and per-reason error rules. Only the words
// differ (the field names, the values a reply may carry, the reason codes),
// and each kind brings its own.
//
// Nothing here changes what a client's dialect accepts or how it is rendered:
// these are doors onto the functions it already runs.

// The size limits of every dialect (see Validate).
const (
	MaxMappings  = maxMappings
	MaxPathDepth = maxPathDepth
)

// Project applies a response field list to a set of values: each field's
// alias takes the value its source names, else its default; a value map
// translates it; a dotted alias nests. An empty list returns the values as
// they are. It is the function a client's reply is built with.
func Project(values map[string]any, fields []Field) map[string]any {
	return project(values, fields)
}

// RuleFor is the error rule for one reason code: its own, else the "*" rule.
func (r *Response) RuleFor(reason string) (ErrorRule, bool) {
	if r == nil {
		return ErrorRule{}, false
	}
	if rule, ok := r.Errors[reason]; ok {
		return rule, true
	}
	rule, ok := r.Errors["*"]
	return rule, ok
}

// XMLDocument writes one answer as XML, as a client's XML reply is written:
// the XML header, the root element ("response" when none is named) and one
// element per value, in name order, its text escaped.
func XMLDocument(root string, vals map[string]any) []byte {
	if root == "" {
		root = "response"
	}
	var sb strings.Builder
	sb.WriteString(xml.Header)
	sb.WriteString("<" + root + ">")
	for _, k := range sortedKeys(vals) {
		var esc strings.Builder
		_ = xml.EscapeText(&esc, []byte(toString(vals[k])))
		sb.WriteString("<" + k + ">" + esc.String() + "</" + k + ">")
	}
	sb.WriteString("</" + root + ">")
	return []byte(sb.String())
}

// CheckWrapper is the rule for request_wrapper: a plain form field name.
func CheckWrapper(name string) error { return validateWrapper(name) }

// CheckPlace is the rule for where a value travels: header, query, body, or
// nothing.
func CheckPlace(what, place string) error { return validateSource(what, place) }

// Text is a value as the text it is sent as.
func Text(v any) string { return toString(v) }
