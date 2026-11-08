package httpserver

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/yakugate/yakugate/internal/dialect"
	"github.com/yakugate/yakugate/internal/refdata"
)

// authQueryKeys are stripped from a submission before field mapping, so a
// credential supplied in the query string or body can never be mistaken for a
// message field (and can never reach the reject log through the payload).
var authQueryKeys = map[string]bool{
	"username": true, "password": true,
	"yaku-sms-key": true,
}

// parseFields extracts a submission's field map from the wire, before any auth
// or per-account dialect mapping runs — GET's query string, POST's JSON body,
// POST's application/x-www-form-urlencoded body (optionally wrapping the
// real fields inside an xmldoc=<...> value — the shape some legacy XML APIs
// use: an embedded XML document POSTed as one form field), or a raw XML body
// (Content-Type application/xml or text/xml — chunk-3 ruling 3.5, 2026-08-03:
// the xmldoc-in-a-form-field shape was accepted but the plain raw-XML POST
// style, which our own P4 sample set includes on the vendor side, was not).
// Both XML shapes decode through the same decodeXMLFlat into the same flat
// field map, so dialect mapping and auth work identically.
//
// This runs ONCE, before auth, because some dialects (that same legacy XML
// shape) carry their credentials INSIDE the body rather than in a header or the query
// string — auth needs the same parsed map finishRequest later applies the
// dialect mapping to. r.Body can only be read once, so this cannot be
// deferred until after auth the way it used to be.
//
// Deliberately does NOT strip auth-looking keys (username/password) — that
// happens once, after auth has read what it needed, in stripAuthFields.
func parseFields(r *http.Request, d *dialect.Config) (map[string]any, int, string) {
	switch r.Method {
	case http.MethodGet:
		fields := map[string]any{}
		for k, vs := range r.URL.Query() {
			if len(vs) > 0 {
				fields[k] = vs[0]
			}
		}
		return fields, 0, ""
	case http.MethodPost:
		ct := strings.ToLower(r.Header.Get("Content-Type"))
		// A dialect may say how its client's body is really shaped, for one
		// whose Content-Type cannot be trusted — JSON sent as text/plain, XML
		// sent with no header at all (PO 2026-09-16). Pinning it replaces the
		// header entirely: that is the point, since the header is the part
		// that is wrong.
		switch d.BodyFormat() {
		case dialect.RequestFormatJSON:
			ct = "application/json"
		case dialect.RequestFormatForm:
			ct = "application/x-www-form-urlencoded"
		case dialect.RequestFormatXML:
			ct = "application/xml"
		}
		switch {
		case strings.HasPrefix(ct, "application/json"):
			var fields map[string]any
			if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					return nil, 413, "REQUEST_TOO_LARGE: body exceeds 64KB"
				}
				return nil, 400, "INVALID_JSON: malformed request body"
			}
			return fields, 0, ""
		case strings.HasPrefix(ct, "application/x-www-form-urlencoded"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					return nil, 413, "REQUEST_TOO_LARGE: body exceeds 64KB"
				}
				return nil, 400, "INVALID_REQUEST: could not read body"
			}
			form, err := url.ParseQuery(string(body))
			if err != nil {
				return nil, 400, "INVALID_REQUEST: malformed form body"
			}
			// The field this client wraps its request in, named by its own
			// dialect (PO 2026-09-16: nothing hardcoded). Falling back to
			// `xmldoc`, the name the first such API used and the only one
			// this understood before.
			//
			// The dialect comes from the URL, since the body has to be parsed
			// before anyone is authenticated — which is the whole reason this
			// shape works at all, credentials living inside the document.
			wrapper := d.Wrapper()
			if wrapper == "" {
				wrapper = "xmldoc"
			}
			if doc := formValue(form, wrapper); doc != "" {
				fields, err := decodeXMLFlat(doc)
				if err != nil {
					return nil, 400, "INVALID_REQUEST: malformed " + wrapper
				}
				return fields, 0, ""
			}
			// The wrapper is not always called "xmldoc" (2026-09-16). Every
			// legacy XML API invents its own name — xmldoc, data, request,
			// xml — and a client posting under any other name used to arrive
			// as ONE opaque field, so nothing mapped and the submit refused
			// for a missing `to`.
			//
			// The name cannot be configured per account here: this runs before
			// auth, precisely so credentials inside the document can be read,
			// so there is no account yet to ask. Recognised by SHAPE instead:
			// a form carrying exactly one field whose value is a real XML
			// document with more than one tag in it. A message that merely
			// contains markup is not that — it arrives beside `to` and `from`,
			// so the form has more than one field.
			if unwrapped, ok := unwrapSoleXMLField(form); ok {
				return unwrapped, 0, ""
			}
			fields := map[string]any{}
			for k, vs := range form {
				if len(vs) > 0 {
					fields[k] = vs[0]
				}
			}
			return fields, 0, ""
		case strings.HasPrefix(ct, "application/xml"), strings.HasPrefix(ct, "text/xml"):
			body, err := io.ReadAll(r.Body)
			if err != nil {
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					return nil, 413, "REQUEST_TOO_LARGE: body exceeds 64KB"
				}
				return nil, 400, "INVALID_REQUEST: could not read body"
			}
			fields, err := decodeXMLFlat(string(body))
			if err != nil {
				return nil, 400, "INVALID_REQUEST: malformed xml body"
			}
			return fields, 0, ""
		default:
			return nil, 400, "INVALID_REQUEST: content-type must be application/json, application/x-www-form-urlencoded, application/xml or text/xml"
		}
	default:
		return nil, 405, "METHOD_NOT_ALLOWED"
	}
}

// finishRequest strips auth-looking keys (now that auth has read whatever it
// needed from the same map) and applies the account's dialect + type
// coercion. Split from parseFields so auth can run in between on the SAME
// parsed map, without reading the body twice.
// elsewhere lets a mapping read a field the client did not put in the body —
// a key in a header, a destination in the query string (PO 2026-09-16). Names
// are matched case-insensitively, as everywhere else a client's own casing
// meets ours.
func elsewhere(r *http.Request) dialect.Elsewhere {
	if r == nil {
		return nil
	}
	return func(where, name string) (string, bool) {
		switch where {
		case dialect.TokenSourceHeader:
			if v := strings.TrimSpace(r.Header.Get(name)); v != "" {
				return v, true
			}
		case dialect.TokenSourceQuery:
			for k, vs := range r.URL.Query() {
				if len(vs) > 0 && strings.EqualFold(k, name) {
					if v := strings.TrimSpace(vs[0]); v != "" {
						return v, true
					}
				}
			}
		}
		return "", false
	}
}

func finishRequest(r *http.Request, fields map[string]any, acct *refdata.Account) (sendRequest, int, string) {
	stripAuthFields(fields)
	// Per-account dialect: rename the client's field names to canonical ones.
	// nil dialect is the overwhelmingly common case and costs nothing.
	fields = dialect.MapRequestFrom(fields, acct.Dialect, elsewhere(r))

	req, err := decodeFields(fields)
	if err != nil {
		return sendRequest{}, 400, "INVALID_REQUEST: " + err.Error()
	}
	if code, reason := validateRequest(req); code != 0 {
		return sendRequest{}, code, reason
	}
	return req, 0, ""
}

// validateRequest is the adapter's own contract checks on a decoded request —
// the refusals that happen before the pipeline runs. Shared with the "Will
// this send?" check so it refuses what a real submit refuses.
func validateRequest(req sendRequest) (int, string) {
	// operator is the client's claim of the destination network. Any value
	// longer than one character is accepted (Saleem 2026-11-05: "remove this
	// validation, should be greater than 1, accept it"; it was 4–6 digits,
	// chunk-4 ruling 4.3). Only a value that can be an MCC+MNC (4–6 digits)
	// is used as the network and skips the number lookup (splitOperator);
	// any other is accepted and our own lookup finds the network, so it never
	// reaches routing or rate cards as a fake MCC/MNC.
	if op := strings.TrimSpace(req.Operator); op != "" && utf8.RuneCountInString(op) < 2 {
		return 400, "INVALID_REQUEST: operator must be more than 1 character"
	}
	// An explicit-but-unknown encoding is a contract error, not an invitation
	// to guess: encoding.Plan's auto-detect default would silently pick
	// something the client did not ask for (chunk-4 ruling 4.4, 2026-08-03).
	switch strings.ToLower(strings.TrimSpace(req.Encoding)) {
	case "", "gsm7", "ucs2", "latin1":
	default:
		return 400, "INVALID_REQUEST: encoding must be gsm7, ucs2 or latin1"
	}
	// The boolean `gsm7` is RETIRED as a way to choose an encoding (PO
	// 2026-09-05): `encoding` (gsm7|ucs2|latin1) is the only one, and it is
	// strictly more capable — the boolean cannot express latin1 at all.
	//
	// Refused rather than ignored, and only when it arrives ALONE. A client
	// still sending `gsm7:true` believes it is choosing an encoding; silently
	// dropping to auto-detect would change what goes on the wire without
	// telling anyone, which is the failure mode worth avoiding. A client
	// mid-migration that sends BOTH keeps working, with `encoding` winning as
	// it always has — so the fix for this 400 is to add one field, never to
	// remove one.
	if req.GSM7 != nil && strings.TrimSpace(req.Encoding) == "" {
		return 400, "INVALID_REQUEST: gsm7 is retired — use encoding=gsm7, ucs2 or latin1"
	}
	for i, it := range req.Messages {
		if it.GSM7 != nil && strings.TrimSpace(it.Encoding) == "" && strings.TrimSpace(req.Encoding) == "" {
			return 400, fmt.Sprintf("INVALID_REQUEST: messages[%d].gsm7 is retired — use encoding=gsm7, ucs2 or latin1", i)
		}
	}
	// priority is 1..9, 1 = most urgent (PO 2026-09-05, closing the
	// docs-ahead item that had specified this since 2026-07-26). The adapter
	// used to CLAMP anything up to 999 instead; the PO chose an honest refusal
	// over silently changing what the client asked for — clamping 100 to 9
	// would make a message MORE urgent than requested, since higher numbers
	// are less urgent.
	if req.Priority != nil && (*req.Priority < 1 || *req.Priority > 9) {
		return 400, "INVALID_REQUEST: priority must be 1-9 (1 = most urgent)"
	}
	return 0, ""
}

// operatorRe: a value that can be a combined MCC+MNC, digits only (e.g.
// "41001"): MCC 3 digits, MNC 1–3. Only such an operator names the network.
var operatorRe = regexp.MustCompile(`^\d{4,6}$`)

func stripAuthFields(fields map[string]any) {
	for k := range fields {
		if authQueryKeys[strings.ToLower(k)] {
			delete(fields, k)
		}
	}
}

// decodeXMLFlat parses a simple, single-level-deep XML document (one root,
// flat child elements — e.g. <SMSRequest><Username>x</Username>...</SMSRequest>)
// into the same flat field map GET/JSON submissions produce, so dialect.MapRequest
// and finishRequest work identically regardless of wire format.
// unwrapSoleXMLField decodes a form whose entire payload is one XML document
// in a single field, whatever that field is called. ok=false leaves the form
// to be read as ordinary fields — which is what every non-XML client sends.
//
// Deliberately strict, because guessing wrong would swallow a real message:
// exactly one field, one value, its text starting with "<", and a decode that
// yields at least two tags. One tag is not a document worth unwrapping, and
// could plausibly be somebody's message body.
// formValue reads one form field case-insensitively: a published spec's casing
// is its own business, and `XmlDoc` must not miss a dialect that says `xmldoc`.
func formValue(form url.Values, name string) string {
	if v := strings.TrimSpace(form.Get(name)); v != "" {
		return v
	}
	for k, vs := range form {
		if strings.EqualFold(k, name) && len(vs) > 0 {
			if v := strings.TrimSpace(vs[0]); v != "" {
				return v
			}
		}
	}
	return ""
}

func unwrapSoleXMLField(form url.Values) (map[string]any, bool) {
	if len(form) != 1 {
		return nil, false
	}
	var raw string
	for _, vs := range form {
		if len(vs) != 1 {
			return nil, false
		}
		raw = strings.TrimSpace(vs[0])
	}
	if !strings.HasPrefix(raw, "<") {
		return nil, false
	}
	fields, err := decodeXMLFlat(raw)
	if err != nil || len(fields) < 2 {
		return nil, false
	}
	return fields, true
}

func decodeXMLFlat(raw string) (map[string]any, error) {
	dec := xml.NewDecoder(strings.NewReader(raw))
	fields := map[string]any{}
	var current string
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			current = t.Name.Local
		case xml.CharData:
			if current == "" {
				continue
			}
			if v := strings.TrimSpace(string(t)); v != "" {
				fields[current] = v
			}
		case xml.EndElement:
			current = ""
		}
	}
	if len(fields) == 0 {
		return nil, errors.New("no fields found in xmldoc")
	}
	return fields, nil
}

// decodeFields converts the canonical field map into sendRequest. It goes via
// JSON so one set of struct tags defines the contract for both methods, and so
// GET's string-typed query values coerce the same way a JSON body would.
func decodeFields(fields map[string]any) (sendRequest, error) {
	normalizeScalars(fields)
	b, err := json.Marshal(fields)
	if err != nil {
		return sendRequest{}, err
	}
	var req sendRequest
	if err := json.Unmarshal(b, &req); err != nil {
		return sendRequest{}, errors.New("field has the wrong type")
	}
	return req, nil
}

// normalizeScalars coerces query-string strings into the types sendRequest
// expects. A GET can only ever deliver strings, so "3" must become 3 for
// priority and "true" must become true for gsm7 — otherwise every GET with
// those fields would fail decoding.
func normalizeScalars(fields map[string]any) {
	if v, ok := fields["priority"].(string); ok {
		if n, err := atoiSafe(v); err == nil {
			fields["priority"] = n
		} else {
			delete(fields, "priority") // unparseable: fall back to the type ladder
		}
	}
	if v, ok := fields["gsm7"].(string); ok {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "1", "yes":
			fields["gsm7"] = true
		case "false", "0", "no":
			fields["gsm7"] = false
		default:
			delete(fields, "gsm7") // unparseable: auto-detect
		}
	}
}

func atoiSafe(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty")
	}
	n := 0
	neg := false
	for i, ch := range s {
		if i == 0 && ch == '-' {
			neg = true
			continue
		}
		if ch < '0' || ch > '9' {
			return 0, errors.New("not a number")
		}
		n = n*10 + int(ch-'0')
		if n > 1_000_000 { // far beyond any sane priority; stop before overflow
			return 0, errors.New("out of range")
		}
	}
	if neg {
		n = -n
	}
	return n, nil
}

// normalizePath trims a single trailing slash so "/api/v1/send" and
// "/api/v1/send/" resolve to the same registered custom path. Root ("/") is
// left as-is: it can never match a real path (validateCustomPath rejects
// anything shorter than 2 characters and the native path is fixed), so no
// special-casing is needed to keep it from over-matching.
func normalizePath(p string) string {
	if len(p) > 1 && strings.HasSuffix(p, "/") {
		return strings.TrimSuffix(p, "/")
	}
	return p
}

// drainBody consumes whatever is left of the request body. Replying while the
// client is still uploading resets the connection mid-request, so every exit
// path — including early rejects — must drain.
func drainBody(r *http.Request) { _, _ = io.Copy(io.Discard, r.Body) }
