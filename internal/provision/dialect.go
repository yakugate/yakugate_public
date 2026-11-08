package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// A dialect lets a customer keep the field names their old gateway used, so
// they can move to Yakugate without touching their code. It maps their names
// onto ours on the way in, and ours onto theirs on the way out, and it can
// give them their own submit path. [SCOPE P14-DIALECT]
//
// All of it is confined to the HTTP edge: the pipeline, the queue and every
// other process see only canonical fields.
//
// ASKED FIELD BY FIELD, never as JSON. A customer's mapping is a list of "what
// do you call the destination number?" questions, and that is how it is built
// here (PO 2026-09-18).

// DialectField is one of our names, with what it means, so a menu can ask for
// the customer's word for it.
type DialectField struct {
	Key  string
	What string
}

// RequestFields are the values a customer may send us. Exactly the set
// dialect.Parse accepts — mapping to anything else silently does nothing.
func RequestFields() []DialectField {
	return []DialectField{
		{"to", "the destination number"},
		{"text", "the message"},
		{"from", "the sender id"},
		{"messageId", "their own id for the message"},
		{"encoding", "gsm7 or ucs2"},
		{"priority", "1 (most urgent) to 9"},
		{"operator", "the network, when they know it"},
		{"sendAt", "send later, as a Unix time"},
		{"validityPeriod", "how long to keep trying, in minutes"},
		{"ap1", "pass-through field 1"},
		{"ap2", "pass-through field 2"},
	}
}

// ResponseFields are the values we can put in a reply.
func ResponseFields() []DialectField {
	return []DialectField{
		{"statusCode", "the numeric result, e.g. 200"},
		{"status", "the word, e.g. ACCEPTED or INSUFFICIENT_BALANCE"},
		{"messageId", "our id for the message"},
		{"message", "their own text, echoed back"},
		{"error", "the error text, when there is one"},
		{"to", "the destination number"},
		{"acceptedTime", "when we took it"},
		{"requestId", "the id for the whole request"},
		{"segments", "how many parts it became"},
	}
}

// RequestFormats is how a customer's request arrives.
func RequestFormats() []string { return []string{"query", "form", "json", "xml"} }

// ResponseFormats is how their reply is written.
func ResponseFormats() []string { return []string{"json", "xml", "text"} }

// DialectMapping is one line of a map: our field, and their word for it.
type DialectMapping struct {
	Ours   string            // our canonical name
	Theirs string            // what the customer calls it
	Values map[string]string // optional: their value -> ours, e.g. "1" -> "ucs2"
}

// Dialect is a customer's shape.
type Dialect struct {
	// Kind is what the dialect is for. This door writes a client's API only:
	// "" or "mt". A vendor's callback dialect ("dlr", "mo") has the same model
	// since 2026-10-08, but its words (the fields a receipt or an incoming
	// message has, the values and reasons of its answer, the list of a batch,
	// the delivery time's format) are not asked by the console, so it is
	// refused here rather than written in a shape the engine would not load.
	// The engine's judge for those is internal/callbackmap; the portal's
	// Dialects screen and the Admin API write them.
	Kind           string
	Name           string
	URL            string // their own submit path, e.g. /api/v1/send; empty = the native one
	RequestFormat  string
	ResponseFormat string
	Request        []DialectMapping
	Response       []DialectMapping
	KeepUnmapped   bool
}

// ValidateDialect checks a built dialect the way dialect.Parse will read it.
func ValidateDialect(d Dialect) error {
	if k := strings.ToLower(strings.TrimSpace(d.Kind)); k != "" && k != "mt" {
		return refusef("the console makes a client's API (MT) dialect only. A vendor's %s dialect — how its callbacks are read and answered — is made on the portal's Dialects screen, so that it is checked by the gateway's own rules", strings.ToUpper(k))
	}
	if strings.TrimSpace(d.Name) == "" {
		return refusef("a dialect needs a name, so it can be recognised in a list")
	}
	if u := strings.TrimSpace(d.URL); u != "" && !strings.HasPrefix(u, "/") {
		return refusef("a submit path starts with a slash, like /api/v1/send — %q does not", u)
	}
	if d.RequestFormat != "" && !oneOf(d.RequestFormat, RequestFormats()) {
		return refusef("a request arrives as %s; %q is not one of them", strings.Join(RequestFormats(), ", "), d.RequestFormat)
	}
	if d.ResponseFormat != "" && !oneOf(d.ResponseFormat, ResponseFormats()) {
		return refusef("a reply is written as %s; %q is not one of them", strings.Join(ResponseFormats(), ", "), d.ResponseFormat)
	}

	okReq := map[string]bool{}
	for _, f := range RequestFields() {
		okReq[f.Key] = true
	}
	okResp := map[string]bool{}
	for _, f := range ResponseFields() {
		okResp[f.Key] = true
	}
	// A cap the engine enforces too, so a dialect cannot grow into something
	// that costs real time on every request.
	if len(d.Request)+len(d.Response) > 50 {
		return refusef("a dialect may hold 50 mappings; this one has %d", len(d.Request)+len(d.Response))
	}
	for _, m := range d.Request {
		if !okReq[m.Ours] {
			return refusef("%q is not a value a customer can send us, so mapping it would do nothing", m.Ours)
		}
		if strings.TrimSpace(m.Theirs) == "" {
			return refusef("no name given for %s: what does this customer call it?", m.Ours)
		}
	}
	for _, m := range d.Response {
		if !okResp[m.Ours] {
			return refusef("%q is not a value we can put in a reply, so it would silently vanish from it", m.Ours)
		}
		if strings.TrimSpace(m.Theirs) == "" {
			return refusef("no name given for %s: what should this customer see it called?", m.Ours)
		}
	}
	return nil
}

// CreateDialect writes the dialect row and returns its id.
func (s Store) CreateDialect(ctx context.Context, d Dialect) (int64, error) {
	if err := ValidateDialect(d); err != nil {
		return 0, err
	}
	// A genuinely custom path must be free: two accounts on one path is how a
	// message ends up authenticated as the wrong customer. Nor a vendor door's
	// own path or a DLR / MO dialect's URL, nor one under or over it (issue
	// 20 and its follow-up of 27 Oct): the roles may share an address with
	// the http-server.
	if err := s.assertDialectPathFree(ctx, "mt", d.URL, 0); err != nil {
		return 0, err
	}

	type fieldJSON struct {
		Alias  string            `json:"alias"`
		Source string            `json:"source"`
		Map    map[string]string `json:"map,omitempty"`
	}
	req := make([]fieldJSON, 0, len(d.Request))
	for _, m := range d.Request {
		req = append(req, fieldJSON{Alias: m.Ours, Source: m.Theirs, Map: m.Values})
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	success := make([]fieldJSON, 0, len(d.Response))
	for _, m := range d.Response {
		// On the way out the customer's name is the alias, and OUR value is
		// the source — the mirror of the request side.
		success = append(success, fieldJSON{Alias: m.Theirs, Source: m.Ours, Map: m.Values})
	}
	resp := map[string]any{"success": success}
	if d.ResponseFormat != "" {
		resp["format"] = d.ResponseFormat
	}
	respJSON, err := json.Marshal(resp)
	if err != nil {
		return 0, err
	}

	url := strings.TrimSpace(d.URL)
	if url == "" {
		url = "/sms/v4/send"
	}
	format := d.RequestFormat
	if format == "" {
		format = "json"
	}
	// No kind is named: the column's default is 'mt' (config/0128), which is
	// the only kind ValidateDialect lets through — and a store older than
	// that migration has no such column to name.
	id, err := s.DB.InsertDialect(ctx, dal.DialectInsert{
		Name: strings.TrimSpace(d.Name), URL: url, Format: format,
		RequestMap: string(reqJSON), ResponseMap: string(respJSON), KeepUnmapped: boolInt(d.KeepUnmapped),
	})
	if err != nil {
		return 0, fmt.Errorf("create the dialect: %w", err)
	}
	return id, nil
}

// DialectRow is one line of the dialect list.
type DialectRow struct {
	ID       int64
	Name     string
	URL      string
	Format   string
	Mappings int
	Accounts int
}

// ListDialects shows each shape and how many accounts use it.
func (s Store) ListDialects(ctx context.Context, limit int) ([]DialectRow, error) {
	if limit <= 0 {
		limit = 200
	}
	// Client API shapes only: a dlr/mo dialect is a VENDOR's callback
	// (config/0128) and is managed with its inbound doors.
	list, err := s.DB.DialectList(ctx, limit)
	if err != nil {
		return nil, wordedErr(err, "list dialects")
	}
	var out []DialectRow
	for _, r := range list {
		out = append(out, DialectRow(r))
	}
	return out, nil
}

// AssignDialect points an account at a shape.
func (s Store) AssignDialect(ctx context.Context, accountID, dialectID int64) error {
	var id dal.NullInt64
	if dialectID > 0 {
		ok, err := s.DB.DialectExists(ctx, dialectID)
		if err != nil {
			return err
		}
		if !ok {
			return refusef("there is no dialect #%d", dialectID)
		}
		// An account speaks a client API. A vendor-callback dialect (dlr/mo,
		// config/0128) would be read as one and misread every request —
		// the Admin API refuses the same (accountRoutes).
		isMT, err := s.DB.DialectIsMT(ctx, dialectID)
		if err != nil {
			return err
		}
		if !isMT {
			return refusef("dialect #%d is for a vendor's callbacks (DLR or MO), not a client's API", dialectID)
		}
		id = dal.NullInt64{Int64: dialectID, Valid: true}
	}
	n, err := s.DB.SetAccountDialect(ctx, accountID, id)
	if err != nil {
		return fmt.Errorf("assign the dialect: %w", err)
	}
	if n == 0 {
		return refusef("there is no account #%d", accountID)
	}
	return nil
}

// DeleteDialect removes a shape, and refuses while an account still uses it —
// an account whose dialect vanished would fall back to the native shape and
// every one of that customer's requests would stop being understood.
func (s Store) DeleteDialect(ctx context.Context, id int64) error {
	n, err := s.DB.DialectAccountUsers(ctx, id)
	if err != nil {
		return err
	}
	if n > 0 {
		return refusef("%d account(s) still use this dialect. Move them to another shape first, or their requests would stop being understood", n)
	}
	// A vendor's inbound door may point at it too (config/0128).
	if n, err = s.DB.DialectDoorUsers(ctx, id); err != nil {
		return err
	}
	if n > 0 {
		return refusef("%d vendor inbound door(s) still read callbacks with this dialect. Move them first", n)
	}
	rows, err := s.DB.DeleteDialectRow(ctx, id)
	if err != nil {
		return fmt.Errorf("delete dialect %d: %w", id, err)
	}
	if rows == 0 {
		return refusef("there is no dialect #%d", id)
	}
	return nil
}
