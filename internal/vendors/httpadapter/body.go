package httpadapter

import (
	"encoding/json"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// listPrefix marks a render-context entry that holds a LIST, as JSON array
// text, for the field named after it: "[]to" is every recipient of a bulk
// chunk sent as an Array (BulkSpec.Array). A {placeholder} cannot name it —
// placeholders match \w only — so it never leaks into a template as text.
const listPrefix = "[]"

// listFor is the list a parameter template stands for: a template that is
// exactly one placeholder whose field has a list in c.
func listFor(tmpl string, c map[string]string) ([]string, bool) {
	m := phRe.FindStringSubmatch(tmpl)
	if m == nil || m[0] != tmpl {
		return nil, false
	}
	raw, ok := c[listPrefix+m[1]]
	if !ok {
		return nil, false
	}
	var out []string
	if json.Unmarshal([]byte(raw), &out) != nil {
		return nil, false
	}
	return out, true
}

// withLists puts a list into a JSON body template where the template has the
// field as a quoted placeholder — "to": "{to}" becomes "to": ["…","…"].
func withLists(tmpl string, c map[string]string) string {
	for k, raw := range c {
		if name, ok := strings.CutPrefix(k, listPrefix); ok {
			tmpl = strings.ReplaceAll(tmpl, `"{`+name+`}"`, raw)
		}
	}
	return tmpl
}

// setParams renders params into a query string or form, a list as the
// parameter repeated (to=1&to=2).
func setParams(q url.Values, params map[string]string, maps map[string]ParamMap, c map[string]string) {
	for k, v := range params {
		pm := maps[k]
		if l, ok := listFor(v, c); ok {
			q.Del(k)
			for _, item := range pm.list(l) {
				q.Add(k, item)
			}
			continue
		}
		q.Set(k, pm.fill(v, c, renderParam))
	}
}

// renderParam fills one query or form value. A value that is itself an XML
// document — Jazz CMT's xmldoc field, `<SMSRequest>…<Message>{text}</Message>…`
// — gets each of our values XML-escaped, as a raw XML body does: a message
// with & or < made the document unreadable and Jazz refused it (found 2 Nov
// 2026, fixed 6 Nov). Any other value is sent as it is; the query or form
// encoding keeps it safe.
func renderParam(tmpl string, c map[string]string) string {
	if strings.HasPrefix(strings.TrimSpace(tmpl), "<") {
		return renderEscaped(tmpl, c, xmlEscape)
	}
	return render(tmpl, c)
}

// xmlFromParams is the XML body of a call that has parameters and no body
// template (QA EP-006): one element per parameter, in name order, inside a
// <request> root; a list repeats its element. A vendor that needs another
// shape (a SOAP envelope, a fixed root name, attributes) uses a Body Template.
func xmlFromParams(params map[string]string, maps map[string]ParamMap, c map[string]string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><request>`)
	for _, k := range keys {
		name := xmlName(k)
		pm := maps[k]
		vals := []string{pm.fill(params[k], c, render)}
		if l, ok := listFor(params[k], c); ok {
			vals = pm.list(l)
		}
		for _, v := range vals {
			b.WriteString("<" + name + ">" + xmlEscape(v) + "</" + name + ">")
		}
	}
	b.WriteString("</request>")
	return b.String()
}

var xmlNameBad = regexp.MustCompile(`[^A-Za-z0-9_.\-]`)

// xmlName makes a parameter name a valid XML element name.
func xmlName(k string) string {
	n := xmlNameBad.ReplaceAllString(strings.TrimSpace(k), "_")
	if n == "" || !(n[0] == '_' || (n[0] >= 'A' && n[0] <= 'Z') || (n[0] >= 'a' && n[0] <= 'z')) {
		n = "_" + n
	}
	return n
}

// extractAll reads every value one response name yields — a JSON path that
// reaches a list (or crosses a list of objects: "results.messageId"), or
// every match of an XML/regex extraction. One bulk call's ids come back this
// way when the vendor answers with a list (QA EP-011, Bulk Value Type Array).
func extractAll(call Call, name, body string) []string {
	if p := call.ExtractJSON[name]; p != "" {
		if vals := extractPathAll(body, p); len(vals) > 0 {
			for i, v := range vals {
				vals[i] = call.mapRead(name, v)
			}
			return vals
		}
	}
	pattern := call.Extract[name]
	if pattern == "" {
		return nil
	}
	// (a value with only a default is never a list: extractVal gives it)
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil
	}
	var out []string
	for _, m := range re.FindAllStringSubmatch(body, -1) {
		if len(m) > 1 {
			if v := strings.TrimSpace(m[1]); v != "" {
				out = append(out, call.mapRead(name, v))
			}
		}
	}
	return out
}

// extractPathAll walks decoded JSON by dot-path, fanning out over every list
// on the way, and returns each scalar it ends on.
func extractPathAll(body, path string) []string {
	var v any
	if json.Unmarshal([]byte(body), &v) != nil {
		return nil
	}
	cur := []any{v}
	for _, seg := range strings.Split(path, ".") {
		var next []any
		for _, x := range flatten(cur) {
			if m, ok := x.(map[string]any); ok {
				if y, ok := m[seg]; ok {
					next = append(next, y)
				}
			}
		}
		cur = next
	}
	var out []string
	for _, x := range flatten(cur) {
		switch t := x.(type) {
		case string:
			if s := strings.TrimSpace(t); s != "" {
				out = append(out, s)
			}
		case float64:
			out = append(out, strings.TrimSuffix(strconv.FormatFloat(t, 'f', -1, 64), ".0"))
		case bool:
			out = append(out, strconv.FormatBool(t))
		}
	}
	return out
}

// flatten opens one level of lists.
func flatten(xs []any) []any {
	var out []any
	for _, x := range xs {
		if l, ok := x.([]any); ok {
			out = append(out, l...)
		} else {
			out = append(out, x)
		}
	}
	return out
}
