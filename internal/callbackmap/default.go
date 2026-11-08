package callbackmap

import (
	"encoding/json"
	"net/http"
	"strings"
)

// THE DEFAULT READING OF A RECEIPT DOOR (issue 19, Saleem 2026-10-23: "if it
// does not standard format then i will create dialect for it not http
// endpoint, and it has nothing to do with vendor" · "no dialect should use
// default, but creating inbound is necessary").
//
// A vendor's receipts are read by its Inbound DLR door and by nothing else.
// A door with a dialect reads them with it. A door with NO dialect reads them
// by the names vendors use most, below — it used to need a "DLR webhook" HTTP
// call on the vendor to be read at all, and without one the receipts were
// answered 404 and lost.
//
// Each field takes the first of its names that the callback carries, in the
// order listed. A name matches whatever its letter case, and with or without
// "_" and "-" (messageId = message_id = MESSAGE-ID). The body is read first —
// JSON, XML or a form; its top level, then inside one wrapping element
// ({"data":{…}}, <Report>…</Report>) — and the query string after it. A body
// that is a list of objects, or one element holding such a list
// ({"results":[…]}), is several receipts, one an object.
//
// The status word goes through dlr.ParseStat as every receipt's does: the
// common spellings (DELIVRD, delivered, failed, …), and nothing is guessed.
// No delivery time is read: its format cannot be guessed safely.
//
// The Admin API names the same lists to people (services/nativeContract.js,
// the Dialects screen's "no dialect" page) and the shared cases hold the two
// together (sqa/e2e/shared/callback-dialect-vectors.json, "default_dlr").

// DefaultDLRNames is each field of the default reading with the callback
// names it is read from, in the order they are tried.
var DefaultDLRNames = []DefaultField{
	{Field: "message_id", Names: []string{"message_id", "messageId", "msgid", "msg_id", "sms_id", "id"}},
	{Field: "status", Names: []string{"status", "stat", "dlr_status", "deliveryStatus", "delivery_status", "state"}},
	{Field: "error_code", Names: []string{"error_code", "errorCode", "err", "error", "err_code", "reason_code"}},
	{Field: "to", Names: []string{"to", "msisdn", "destination", "dest", "recipient", "receiver", "mobile", "phone", "number"}},
}

// DefaultField is one field of a default reading and the names it is read
// from.
type DefaultField struct {
	Field string   `json:"field"`
	Names []string `json:"names"`
}

// DefaultDLR is the reading of a receipt door that has no dialect. It is a
// Request like any dialect's, so a door reads with it exactly as it reads
// with a dialect: Read, ReadTraced, DoneAt (always none), an empty StatusMap.
func DefaultDLR() *Request {
	return &Request{kind: KindDLR, guess: DefaultDLRNames}
}

// IsDefault reports whether this is the default reading of a door with no
// dialect.
func (q *Request) IsDefault() bool { return q != nil && q.guess != nil }

// nameKey is a name as the default reading compares it: lower case, without
// "_" and "-".
func nameKey(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '_', '-':
			return -1
		}
		return r
	}, strings.ToLower(strings.TrimSpace(s)))
}

// readDefault is read for the default reading: the receipts in the body, each
// field by the first of its names that is there.
func (q *Request) readDefault(r *http.Request, body []byte, trace bool) ([]Item, error) {
	doc := decode(r.Header.Get("Content-Type"), body)
	raw := r.URL.RawQuery + "\n" + string(body)
	items, isList := defaultItems(doc)
	query := map[string]string{}
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 && strings.TrimSpace(vs[0]) != "" {
			if key := nameKey(k); query[key] == "" {
				query[key] = strings.TrimSpace(vs[0])
			}
		}
	}
	out := make([]Item, 0, len(items))
	for _, it := range items {
		itemRaw := raw
		if isList {
			if b, err := json.Marshal(it); err == nil {
				itemRaw = string(b)
			}
		}
		top, inner := defaultLevels(it)
		// Where a value may be, in the order it is looked for.
		levels := []struct {
			vals map[string]string
			from string
		}{{top, FromBody}, {inner, FromBody}, {query, FromQuery}}
		vals := Values{}
		var reads []FieldRead
		for _, f := range q.guess {
			v, name, from := "", "", ""
		look:
			for _, level := range levels {
				for _, n := range f.Names {
					if x := level.vals[nameKey(n)]; x != "" {
						v, name, from = x, n, level.from
						break look
					}
				}
			}
			if v != "" {
				vals[f.Field] = v
			}
			if trace {
				src := name
				if src == "" {
					src = strings.Join(f.Names, ", ")
				}
				reads = append(reads, FieldRead{Field: f.Field, Source: src, From: from, Raw: v, Value: v})
			}
		}
		out = append(out, Item{Values: vals, Raw: itemRaw, Read: reads})
	}
	return out, nil
}

// defaultItems is the receipts a body holds for the default reading: a list
// of objects, or one element holding one, is several; anything else is one.
func defaultItems(doc any) ([]any, bool) {
	if arr, ok := objects(doc); ok {
		return arr, true
	}
	if m, ok := doc.(map[string]any); ok && len(m) == 1 {
		for _, v := range m {
			if arr, ok := objects(v); ok {
				return arr, true
			}
		}
	}
	return []any{doc}, false
}

// objects is a list whose every item is an object, and not empty.
func objects(v any) ([]any, bool) {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return nil, false
	}
	for _, it := range arr {
		if _, ok := it.(map[string]any); !ok {
			return nil, false
		}
	}
	return arr, true
}

// defaultLevels is one receipt's scalar values by nameKey: its top level, and
// inside the one element that wraps the rest when there is one.
func defaultLevels(item any) (top, inner map[string]string) {
	m, ok := item.(map[string]any)
	if !ok {
		return nil, nil
	}
	top = flatOf(m)
	if len(m) == 1 {
		for _, v := range m {
			if w, ok := v.(map[string]any); ok {
				inner = flatOf(w)
			}
		}
	}
	return top, inner
}

// flatOf is an object's scalar values by nameKey. Two names that read alike
// in one object ("msgId" and "msg_id") are a vendor's own muddle: the one
// with fewer marks in it wins, so the answer does not change from one call
// to the next.
func flatOf(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	won := make(map[string]string, len(m))
	for k, v := range m {
		s := scalar(v)
		if s == "" {
			continue
		}
		key := nameKey(k)
		if w, taken := won[key]; taken && (len(w) < len(k) || (len(w) == len(k) && w < k)) {
			continue
		}
		out[key], won[key] = s, k
	}
	return out
}
