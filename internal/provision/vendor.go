package provision

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yakugate/yakugate/internal/billing"
	"regexp"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/secrets"
	"github.com/yakugate/yakugate/internal/vendors/httpadapter"
	"github.com/yakugate/yakugate/lib/encoding"
)

// Vendor is an upstream carrier link. [SCOPE P14-VENDOR]
type Vendor struct {
	CID       string // names the queue table and the webhook path — fixed for life
	Name      string // the display name, freely editable
	CompanyID int64
	Transport string // smpp | http | soap | blackhole

	// Deployments is which gateways claim this vendor's queue and hold its
	// bind (config/0122 + config/0124), and its rate on each. Required once
	// any deployment is registered, exactly as the Admin API requires it —
	// and it has to be, or the console could create a vendor that no routing
	// rule may then point at, because an account's deployments must be a
	// subset of its vendor's.
	Deployments deployment.List

	// SMPP
	Host     string
	Port     int
	SystemID string
	Password string // a literal, or a secret reference (env:// or file://)
	Sessions int

	// HTTP or SOAP: the adapter profile, as JSON. Checked with the engine's
	// own parser, so what the console accepts is exactly what the vendor role
	// will later load.
	AdapterConfig string
}

// Transports lets a caller offer the real choices.
func Transports() []string { return []string{"smpp", "http", "soap", "blackhole"} }

// cidRE is deliberately strict: the cid becomes a TABLE NAME (queue_<cid>) and
// a URL path segment (the vendor's inbound webhook). A character that is legal
// in one and not the other would produce a vendor that looks saved and cannot
// be reached, or a table name that has to be quoted everywhere.
var cidRE = regexp.MustCompile(`^[a-z0-9_]{2,48}$`)

// ValidateVendor applies every rule that needs nothing but the values. The
// store-level checks (is the cid taken, does the company exist) are in Create.
func ValidateVendor(v Vendor) error {
	cid := strings.TrimSpace(v.CID)
	if cid == "" {
		return refusef("a cid is required: it names the vendor's queue table and its webhook path")
	}
	if !cidRE.MatchString(cid) {
		return refusef("cid %q cannot be used: it becomes the table name queue_%s and a web address, so it may hold only lower-case letters, digits and underscores, and must be 2 to 48 characters",
			cid, cid)
	}
	if v.CompanyID <= 0 {
		return refusef("every vendor must belong to a company, the same rule accounts and rate cards follow")
	}
	if !oneOf(v.Transport, Transports()) {
		return refusef("transport %q is not one this gateway speaks: %s", v.Transport, strings.Join(Transports(), ", "))
	}

	switch strings.ToLower(v.Transport) {
	case "smpp":
		if strings.TrimSpace(v.Host) == "" {
			return refusef("an SMPP vendor needs a host to bind to")
		}
		// The Admin API's rules (p13-backend/src/services/vendorSmpp.js, QA
		// SMPP-005/006) — change together: a host the dialler can dial, and
		// the bind fields' SMPP sizes. A secret reference is not the value,
		// so its length does not count.
		if why := smppHostProblem(strings.TrimSpace(v.Host)); why != "" {
			return refusef("host %q %s", strings.TrimSpace(v.Host), why)
		}
		if v.Port <= 0 || v.Port > 65535 {
			return refusef("port %d is not a port number", v.Port)
		}
		if strings.TrimSpace(v.SystemID) == "" {
			return refusef("an SMPP vendor needs a system_id — the name the carrier knows this bind by")
		}
		if sid := strings.TrimSpace(v.SystemID); !IsSecretRef(sid) && len([]rune(sid)) > 15 {
			return refusef("system_id: at most 15 characters (SMPP allows no more) — this one has %d", len([]rune(sid)))
		}
		if strings.TrimSpace(v.Password) == "" {
			return refusef("an SMPP vendor needs a bind password, or a reference to one (env://NAME or file:///path)")
		}
		// The bind password has no length limit for now (Saleem 2026-11-03):
		// SMPP 3.4 gives it 8 characters, but carriers' SMSCs take longer
		// ones. Same rule as the Admin API's vendorSmpp.js.
		if v.Sessions < 0 {
			return refusef("sessions cannot be negative")
		}

	case "http", "soap":
		// An HTTP vendor may be created before its calls exist: the console
		// makes the vendor, then builds its send call with guided questions
		// (PO 2026-09-18 — nobody should have to know the profile's shape).
		// Nothing is hidden by allowing it: until a send call is attached the
		// vendor reads "will not load" everywhere it appears, and the flow
		// walks the operator straight into building one.
		if strings.TrimSpace(v.AdapterConfig) == "" {
			return nil
		}
		// A profile given inline (a hand-written vendor, or one being edited)
		// is still checked with the engine's own parser.
		var prof httpadapter.Profile
		if err := json.Unmarshal([]byte(v.AdapterConfig), &prof); err != nil {
			return refusef("the adapter profile is not readable JSON: %v", err)
		}
		// The engine's OWN check, not a copy of it. Whatever this accepts, the
		// vendor role will load; whatever it refuses would have been skipped
		// at load with the vendor silently doing nothing.
		//
		// This is the rule `testvendor` fails: no success rule on its send
		// call, so the role has refused to load it, and 703 messages sat in
		// its queue accepted and never sent.
		if err := prof.Validate(); err != nil {
			return refusef("%v", err)
		}
	}
	return nil
}

var (
	hostLabelRE = regexp.MustCompile(`^[A-Za-z0-9_](?:[A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?$`)
	ipv6RE      = regexp.MustCompile(`^\[[0-9a-fA-F:.]+\]$`)
	schemeRE    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*://`)
	digitsRE    = regexp.MustCompile(`^[0-9]+$`)
)

// smppHostProblem says why h cannot be dialled as an SMPP host, or "" when it
// can: a host name, an IPv4 address, or an IPv6 address in brackets — no
// scheme, no port (the vendor role appends ":<port>"), no spaces. The same
// reading as the Admin API's hostProblem (services/vendorSmpp.js).
func smppHostProblem(h string) string {
	switch {
	case strings.ContainsAny(h, " \t\r\n"):
		return "has a space in it"
	case schemeRE.MatchString(h):
		return "is a web address — give only the host name or IP address, without smpp:// or http://"
	case ipv6RE.MatchString(h):
		return ""
	case strings.Contains(h, ":"):
		return "has a port (or a colon) in it — put the port in the port field; an IPv6 address goes in square brackets"
	case len(h) > 253:
		return "is longer than 253 characters"
	}
	labels := strings.Split(h, ".")
	numeric := true
	for _, l := range labels {
		if !digitsRE.MatchString(l) {
			numeric = false
			break
		}
	}
	if numeric {
		if len(labels) != 4 {
			return "is not an IPv4 address (four numbers from 0 to 255)"
		}
		for _, l := range labels {
			if n, err := strconv.Atoi(l); err != nil || n > 255 {
				return "is not an IPv4 address (four numbers from 0 to 255)"
			}
		}
		return ""
	}
	for _, l := range labels {
		if !hostLabelRE.MatchString(l) {
			return "is not a host name or IP address (letters, digits, dots and hyphens only)"
		}
	}
	return ""
}

// assertBuyingCompany refuses a selling company as a vendor's owner: a selling
// company is a customer, and a carrier filed under one puts its costs on the
// wrong side of every margin. The Admin API's rule (assertVendorCompany in
// routes/resources.js, QA VEN-005) — buying, both, or an older company with
// no type pass.
func (s Store) assertBuyingCompany(ctx context.Context, companyID int64) error {
	name, typ, err := s.DB.VendorCompanyKind(ctx, companyID)
	if err != nil {
		return nil // existence is checked, and refused, by the caller
	}
	if strings.EqualFold(strings.TrimSpace(typ), "selling") {
		return refusef("company %q is a selling company; a vendor's company is one we buy from — choose a buying or both company", name)
	}
	return nil
}

// CreateVendor writes the vendor and its queue table, and returns the id.
func (s Store) CreateVendor(ctx context.Context, v Vendor) (int64, error) {
	v.CID = strings.ToLower(strings.TrimSpace(v.CID))
	v.Transport = strings.ToLower(strings.TrimSpace(v.Transport))
	if err := ValidateVendor(v); err != nil {
		return 0, err
	}
	// The same two rules the Admin API applies: name one once any exist, and
	// never name one the register has not heard of.
	if err := s.assertDeploymentsChosen(ctx, v.Deployments); err != nil {
		return 0, err
	}
	if err := s.assertDeploymentsExist(ctx, v.Deployments); err != nil {
		return 0, err
	}

	ok, err := s.DB.CompanyExists(ctx, v.CompanyID)
	if err != nil {
		return 0, fmt.Errorf("look up company %d: %w", v.CompanyID, err)
	}
	if !ok {
		return 0, refusef("there is no company #%d", v.CompanyID)
	}
	if err := s.assertBuyingCompany(ctx, v.CompanyID); err != nil {
		return 0, err
	}
	ok, err = s.DB.VendorCIDTaken(ctx, v.CID)
	if err != nil {
		return 0, fmt.Errorf("look up cid %q: %w", v.CID, err)
	}
	if ok {
		return 0, refusef("a vendor with cid %q already exists, and a cid cannot be reused: it names that vendor's queue table", v.CID)
	}

	queueCfg := "{}"
	if v.Transport == "smpp" {
		qc := map[string]any{"host": v.Host, "port": v.Port, "system_id": v.SystemID, "password": v.Password}
		if v.Sessions > 0 {
			qc["sessions"] = v.Sessions
		}
		b, err := json.Marshal(qc)
		if err != nil {
			return 0, err
		}
		queueCfg = string(b)
	}
	adapter := "{}"
	if v.AdapterConfig != "" {
		adapter = v.AdapterConfig
	}

	// No inbound token is made here any more. The old shared setting
	// (vendors.inbound_config) is retired (PO 2026-09-24): a vendor's
	// callbacks are guarded by its two doors, set up under Vendors → Inbounds,
	// and until then they are refused — which is what a random token nobody
	// was ever shown amounted to anyway.
	deps, _ := deploymentsJSON(v.Deployments).(string) // "" = NULL
	id, err := s.DB.InsertVendor(ctx, dal.VendorInsert{
		CID: v.CID, Name: strings.TrimSpace(v.Name), CompanyID: v.CompanyID, Transport: v.Transport,
		QueueConfig: queueCfg, AdapterConfig: adapter, Deployments: deps,
	})
	if err != nil {
		return 0, fmt.Errorf("create the vendor: %w", err)
	}

	// The queue table. The store creates it on first use anyway, but making it
	// now means the vendor is complete the moment it exists — an operator can
	// look at queue_<cid> and see an empty queue rather than a missing table,
	// and a depth check does not have to special-case "not created yet".
	if s.Queues != nil {
		if err := s.Queues.Make(ctx, v.CID); err != nil {
			return id, fmt.Errorf("vendor %d created, but its queue table could not be made: %w", id, err)
		}
	}
	return id, nil
}

// VendorRow is one line of the vendor list.
type VendorRow struct {
	ID        int64
	CID       string
	Name      string
	Company   string
	Transport string
	Status    string // active | paused | disabled
	Queued    int    // messages waiting in its queue
	Trouble   string // "" when the vendor can be loaded; otherwise why not
}

// ListVendors is the console's list, and it answers the question the list
// exists for: would this vendor actually work? A vendor whose profile the
// engine cannot load looks perfectly healthy in a table of names and
// statuses — which is how 703 messages piled up behind `testvendor`.
func (s Store) ListVendors(ctx context.Context, limit int) ([]VendorRow, error) {
	if limit <= 0 {
		limit = 200
	}
	list, err := s.DB.VendorList(ctx, limit)
	if err != nil {
		var re dal.RowsError
		if errors.As(err, &re) {
			return nil, re.Err
		}
		return nil, fmt.Errorf("list vendors: %w", err)
	}
	var out []VendorRow
	for _, l := range list {
		r := VendorRow{ID: l.ID, CID: l.CID, Name: l.Name, Company: l.Company, Transport: l.Transport, Status: l.Status}
		r.Trouble = s.vendorTrouble(ctx, r.Transport, l.AdapterJSON)
		out = append(out, r)
	}
	// Queue depth, per vendor, from the queue store.
	if s.Queues != nil {
		for i := range out {
			n, err := s.Queues.Count(ctx, out[i].CID)
			if err == nil {
				out[i].Queued = n
			}
		}
	}
	return out, nil
}

// vendorTrouble asks the engine's own parser whether this vendor could be
// loaded, and turns the answer into one short phrase.
//
// It must follow BOTH shapes an HTTP vendor can take, or it lies. A vendor
// saved by the portal holds endpoint REFERENCES — {"endpoints":{"send":74}} —
// and its profile is hydrated from those http_endpoints rows (GW-15); only a
// hand-written vendor carries the profile inline. Checking the inline shape
// alone reported the fixture vendor e2ehv as broken when it works perfectly
// (found 2026-09-18, by reading this column's own output).
func (s Store) vendorTrouble(ctx context.Context, transport, adapter string) string {
	if transport != "http" && transport != "soap" {
		return ""
	}
	var refs struct {
		Endpoints map[string]int64 `json:"endpoints"`
	}
	if err := json.Unmarshal([]byte(adapter), &refs); err == nil && refs.Endpoints["send"] != 0 {
		prof, err := s.profileFromEndpoints(ctx, refs.Endpoints)
		if err != nil {
			return shortTrouble(err)
		}
		if err := prof.Validate(); err != nil {
			return shortTrouble(err)
		}
		return ""
	}
	var prof httpadapter.Profile
	if err := json.Unmarshal([]byte(adapter), &prof); err != nil || prof.Send.URL == "" {
		return "no readable adapter profile"
	}
	if err := prof.Validate(); err != nil {
		return shortTrouble(err)
	}
	return ""
}

// profileFromEndpoints mirrors what the http-vendor role does at load: the
// send row is required, the login row is discovered through the send row's
// session_endpoint_id, and the poll row is optional.
func (s Store) profileFromEndpoints(ctx context.Context, refs map[string]int64) (httpadapter.Profile, error) {
	send, err := s.endpointRow(ctx, refs["send"])
	if err != nil {
		return httpadapter.Profile{}, err
	}
	var login *httpadapter.EndpointRow
	if sid, ok := send.Config["session_endpoint_id"].(float64); ok && sid != 0 {
		l, err := s.endpointRow(ctx, int64(sid))
		if err != nil {
			return httpadapter.Profile{}, fmt.Errorf("login row: %w", err)
		}
		login = &l
	}
	var poll *httpadapter.EndpointRow
	if pid := refs["poll"]; pid != 0 {
		pl, err := s.endpointRow(ctx, pid)
		if err != nil {
			return httpadapter.Profile{}, fmt.Errorf("poll row: %w", err)
		}
		poll = &pl
	}
	return httpadapter.ProfileFromEndpoints(send, login, poll, nil)
}

func (s Store) endpointRow(ctx context.Context, id int64) (httpadapter.EndpointRow, error) {
	var row httpadapter.EndpointRow
	r, err := s.DB.EndpointForProfile(ctx, id)
	row.ID, row.Name, row.URL, row.Method = r.ID, r.Name, r.URL, r.Method
	row.TimeoutMS, row.CacheTTLSec, row.BodyTemplate = r.TimeoutMS, r.CacheTTLSec, r.BodyTemplate
	headers, auth, cfg := r.Headers, r.Auth, r.Config
	if err != nil {
		return row, fmt.Errorf("send call #%d is missing or switched off", id)
	}
	_ = json.Unmarshal([]byte(headers), &row.Headers)
	_ = json.Unmarshal([]byte(auth), &row.Auth)
	if err := json.Unmarshal([]byte(cfg), &row.Config); err != nil {
		return row, fmt.Errorf("endpoint %d has unreadable config", id)
	}
	return row, nil
}

// shortTrouble keeps the first clause of a load failure — enough to act on in
// a table column, with the whole sentence available on the vendor's own page.
func shortTrouble(err error) string {
	msg := err.Error()
	if i := strings.Index(msg, ":"); i > 0 && i < 40 {
		msg = msg[:i]
	}
	return trimTo(msg, 44)
}

// SetVendorStatus pauses, resumes or disables a vendor.
func (s Store) SetVendorStatus(ctx context.Context, id int64, status string) error {
	if !oneOf(status, []string{"active", "paused", "disabled"}) {
		return refusef("%q is not a vendor state: active, paused or disabled", status)
	}
	n, err := s.DB.SetVendorStatus(ctx, id, status)
	if err != nil {
		return fmt.Errorf("update vendor %d: %w", id, err)
	}
	if n == 0 {
		return refusef("there is no vendor #%d", id)
	}
	return nil
}

// IsSecretRef reports whether a password was given as a reference rather than
// the value itself, so a console can say which one it stored.
func IsSecretRef(v string) bool { return secrets.IsRef(strings.TrimSpace(v)) }

func trimTo(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

func newToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

var _ = newToken // kept for the webhook-token field when endpoints land here

// VendorDetail is everything the console shows on one vendor's page.
type VendorDetail struct {
	VendorRow
	CompanyID     int64
	Host          string
	Port          int
	SystemID      string
	PasswordShown string // a reference as written, or a note that a value is stored
	Sessions      int
	AdapterConfig string
	HasInbound    bool
	Routes        int // routing rules pointing here
	// Wire is how each encoding is sent (protocol_config.encodings, read as
	// the SMPP vendor role reads it), and WireProblems what it would skip.
	Wire         encoding.WireRules
	WireProblems []string
	// How its rate is shared between its queues, and its short codes
	// (config/0161, issue 22). QueueColumns is false on a store the gateway
	// has not upgraded yet: the two are then the defaults and cannot be set.
	QueueSplit   string
	ShortCodes   []string
	QueueColumns bool
}

// GetVendor reads one vendor in full. A stored password is never returned: a
// reference is shown as written (it is not a secret), a literal is reported as
// present. A console prints what it is given, and a secret on a screen is a
// secret in a scrollback buffer.
func (s Store) GetVendor(ctx context.Context, id int64) (VendorDetail, error) {
	var d VendorDetail
	r, err := s.DB.VendorDetail(ctx, id)
	if err != nil {
		return d, refusef("there is no vendor #%d", id)
	}
	d.ID, d.CID, d.Name, d.Company, d.Transport, d.Status, d.CompanyID =
		r.ID, r.CID, r.Name, r.Company, r.Transport, r.Status, r.CompanyID
	queueCfg, adapter, inbound, encJSON, acceptedJSON := r.QueueConfig, r.AdapterConfig, r.Inbound, r.Encodings, r.Accepted
	var qc struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		SystemID string `json:"system_id"`
		Password string `json:"password"`
		Sessions int    `json:"sessions"`
	}
	_ = json.Unmarshal([]byte(queueCfg), &qc)
	d.Host, d.Port, d.SystemID, d.Sessions = qc.Host, qc.Port, qc.SystemID, qc.Sessions
	switch {
	case qc.Password == "":
		d.PasswordShown = "(none set)"
	case IsSecretRef(qc.Password):
		d.PasswordShown = qc.Password + "  (a reference — the value is not in the database)"
	default:
		d.PasswordShown = "(a value is stored in the database; a reference would keep it out)"
	}
	d.Wire, d.WireProblems = encoding.ParseVendorEncodings(json.RawMessage(encJSON), json.RawMessage(acceptedJSON))
	d.QueueSplit, d.ShortCodes, d.QueueColumns = s.vendorQueueSettings(ctx, id)
	d.AdapterConfig = adapter
	d.HasInbound = strings.Contains(inbound, "token")
	d.Trouble = s.vendorTrouble(ctx, d.Transport, adapter)
	if s.Queues != nil {
		if n, err := s.Queues.Count(ctx, d.CID); err == nil {
			d.Queued = n
		}
	}
	// A rule sends to a vendor through vendor_id (old rules) or anywhere in
	// its plan — Main, a backup or an exception (config/0131).
	if n, err := s.DB.VendorRouteCount(ctx, id); err == nil {
		d.Routes = n
	}
	return d, nil
}

const wireHint = "gsm7, latin1 or ucs2, with an optional data_coding after a colon (ucs2:8, gsm7:1). " +
	"Empty = send it as itself, standard label. Text the choice cannot carry goes as UCS-2"

// ParseWireSetting reads the console's "send_as[:data_coding]" for one
// encoding, checked by the same rules the SMPP vendor role reads it with.
// Empty = the default (sent as itself). ok=false with no error = the default.
func ParseWireSetting(enc, value string) (rule map[string]any, err error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return nil, nil
	}
	sendAs, dcText, hasDC := strings.Cut(value, ":")
	rule = map[string]any{"send_as": strings.TrimSpace(sendAs)}
	if hasDC {
		n, err := strconv.Atoi(strings.TrimSpace(dcText))
		if err != nil {
			return nil, refusef("%q: the data_coding after the colon is a number, e.g. ucs2:8", value)
		}
		rule["data_coding"] = n
	}
	raw, _ := json.Marshal(map[string]any{enc: rule})
	if _, problems := encoding.ParseWireRules(raw); len(problems) > 0 {
		return nil, refusef("%s", strings.Join(problems, "; "))
	}
	return rule, nil
}

// VendorFields are the settings an operator may change on a vendor of this
// transport. The cid is not among them, deliberately — see VendorFixed.
func VendorFields(transport string) []Field {
	f := []Field{
		{Key: "name", Label: "Display name", Kind: "text"},
		{Key: "company", Label: "Company", Kind: "int"},
		// Its receipts (D-136, vendors.dlr_config): the Admin API's rules.
		{Key: "receipts", Label: "Sends delivery receipts", Kind: "choice", Choices: []string{"yes", "no"},
			Hint: "no = this vendor sends none, so nothing waits for one"},
		{Key: "receipt_wait_hours", Label: "Hours a message waits for its receipt", Kind: "int",
			Hint: "1 to 720 (48 when not set); a receipt that comes later is kept as an orphan"},
		// One final receipt per long message (Saleem 4 Nov 2026): the Admin
		// API's rule (services/recordSettings.js), change together.
		{Key: "first_receipt_closes", Label: "One receipt closes a long message", Kind: "choice", Choices: []string{"no", "yes"},
			Hint: "yes = the first final receipt for any part closes the whole message (SMPP; for a vendor that sends one receipt per long message)"},
		// When this vendor charges us (config/0150; Saleem, 26 Oct 2026): the
		// buy side of a message is booked at this moment. The Admin API's
		// rule (services/vendorChargePoint.js), change together.
		{Key: "charge_point", Label: "Vendor charges on", Kind: "choice",
			Choices: []string{"submission", "acceptance", "delivery"},
			Hint:    "submission = every submit we send; acceptance = the vendor accepted it (default); delivery = delivered only"},
		// Its queues (config/0161, issue 22): a common queue and one per short
		// code, sharing its rate. The rule is vendorqueue.go, the Admin API's
		// services/vendorQueue.js; change together.
		{Key: "queue_split", Label: "How its rate is shared between its queues", Kind: "choice",
			Choices: QueueSplits(),
			Hint:    "fair = each of its queues with work gets an equal share (default); priority = priority 1 from all its queues first, then 2, and so on"},
		{Key: "short_codes", Label: "Short codes with a queue of their own", Kind: "text",
			Hint: "a message whose sender is one of these waits in that short code's queue, every other one in the common queue. " +
				"Separate them with commas (e.g. 8585, 3344, AcmeBank); each 1 to 16 of A-Z a-z 0-9 + _ -, at most 50; none removes them all"},
	}
	switch strings.ToLower(transport) {
	case "smpp":
		f = append(f,
			Field{Key: "host", Label: "Host", Kind: "text"},
			Field{Key: "port", Label: "Port", Kind: "int"},
			Field{Key: "system_id", Label: "System id", Kind: "text"},
			Field{Key: "password", Label: "Password or reference", Kind: "text",
				Hint: "env://NAME or file:///path keeps the value out of the database"},
			Field{Key: "sessions", Label: "Concurrent binds", Kind: "int"},
			// How each encoding goes to this SMSC (PO 2026-09-21).
			Field{Key: "enc_gsm7", Label: "GSM-7 sent as", Kind: "text", Hint: wireHint},
			Field{Key: "enc_latin1", Label: "Latin-1 sent as", Kind: "text", Hint: wireHint},
			Field{Key: "enc_ucs2", Label: "UCS-2 sent as", Kind: "text", Hint: wireHint},
		)
	case "http", "soap":
		f = append(f, Field{Key: "send_call", Label: "Which call it sends with", Kind: "int",
			Hint: "the id of a send call from HTTP calls → List calls"},
			Field{Key: "poll_call", Label: "Which call asks for status", Kind: "int",
				Hint: "a poll call's id, or 0 for none"})
	}
	return f
}

// MaxReceiptWaitHours is the longest a vendor's sent message may wait for
// its receipt (the Admin API's rule, routes/resources.js).
const MaxReceiptWaitHours = 720

// VendorFixed is what cannot be changed, and why. Shown rather than omitted:
// an operator hunting for a setting that is not on the menu assumes the
// console is unfinished.
func VendorFixed() []FixedField {
	return []FixedField{{
		Label: "cid",
		Why:   "it names this vendor's queue table (queue_<cid>) and its webhook path. Changing it would strand whatever is queued and break the carrier's callbacks. Make a new vendor instead.",
	}}
}

// UpdateVendor changes one setting, checking it the way creation would.
func (s Store) UpdateVendor(ctx context.Context, id int64, key, value string) error {
	cur, err := s.GetVendor(ctx, id)
	if err != nil {
		return err
	}
	want := Vendor{
		CID: cur.CID, Name: cur.Name, CompanyID: cur.CompanyID, Transport: cur.Transport,
		Host: cur.Host, Port: cur.Port, SystemID: cur.SystemID, Sessions: cur.Sessions,
		AdapterConfig: cur.AdapterConfig,
		// The stored password is not readable here, so stand in a value that
		// satisfies the "must have one" rule when another field is edited.
		Password: "kept",
	}
	value = strings.TrimSpace(value)
	switch key {
	case "queue_split", "short_codes":
		// Columns of their own, judged by the shared rule (vendorqueue.go);
		// nothing else about the vendor is read for them.
		return s.setVendorQueueSetting(ctx, id, key, value)
	case "name":
		want.Name = value
	case "company":
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return refusef("%q is not a company id", value)
		}
		want.CompanyID = n
	case "host":
		want.Host = value
	case "port":
		n, err := strconv.Atoi(value)
		if err != nil {
			return refusef("%q is not a number", value)
		}
		want.Port = n
	case "system_id":
		want.SystemID = value
	case "password":
		want.Password = value
	case "sessions":
		n, err := strconv.Atoi(value)
		if err != nil {
			return refusef("%q is not a number", value)
		}
		want.Sessions = n
	case "send_call", "poll_call", "enc_gsm7", "enc_latin1", "enc_ucs2":
		// Handled below: calls point the vendor at existing rows, and the
		// encoding rules live in protocol_config, which nothing above checks.
	case "adapter":
		want.AdapterConfig = value
	case "receipts":
		if v := strings.ToLower(value); v != "yes" && v != "no" {
			return refusef("%q: a vendor sends delivery receipts (yes) or not (no)", value)
		}
		value = strings.ToLower(value)
	case "first_receipt_closes":
		if v := strings.ToLower(strings.TrimSpace(value)); v != "yes" && v != "no" {
			return refusef("%q: yes = the first final receipt for any part closes a long message, no = every part's is waited for", value)
		}
		value = strings.ToLower(strings.TrimSpace(value))
	case "receipt_wait_hours":
		if n, err := strconv.Atoi(value); err != nil || n < 1 || n > MaxReceiptWaitHours {
			return refusef("%q: the wait for a receipt is a whole number of hours from 1 to %d", value, MaxReceiptWaitHours)
		}
	case "charge_point":
		p, ok := billing.CleanVendorPoint(value)
		if !ok {
			return refusef("%q: a vendor charges on submission, acceptance or delivery", value)
		}
		value = p
	case "cid":
		return refusef("%s", VendorFixed()[0].Why)
	default:
		return refusef("%q is not a setting on a vendor", key)
	}
	// A system id saved before the SMPP size rule is the carrier's own: an edit
	// of something else must not be refused over it, as the stored password is
	// not (the Admin API's rule, services/vendorSmpp.js). A new one is checked.
	if key != "system_id" && strings.TrimSpace(want.SystemID) != "" {
		want.SystemID = "kept"
	}
	if err := ValidateVendor(want); err != nil {
		return err
	}

	switch key {
	case "charge_point":
		err = s.DB.SetVendorChargePoint(ctx, id, value)
	case "receipts", "receipt_wait_hours", "first_receipt_closes":
		// Into dlr_config, every other key of it kept (D-136).
		path, v := "$.receipts", any(value)
		switch key {
		case "receipt_wait_hours":
			n, _ := strconv.Atoi(value)
			path, v = "$.wait_hours", n
		case "first_receipt_closes":
			path, v = "$.first_receipt_closes", value == "yes"
		}
		err = s.DB.SetVendorDLRConfig(ctx, id, path, v)
	case "name":
		err = s.DB.SetVendorName(ctx, id, want.Name)
	case "company":
		ok, e := s.DB.CompanyExists(ctx, want.CompanyID)
		if e != nil {
			return e
		}
		if !ok {
			return refusef("there is no company #%d", want.CompanyID)
		}
		if e := s.assertBuyingCompany(ctx, want.CompanyID); e != nil {
			return e
		}
		err = s.DB.SetVendorCompany(ctx, id, want.CompanyID)
	case "send_call", "poll_call":
		n, cerr := strconv.ParseInt(value, 10, 64)
		if cerr != nil {
			return refusef("%q is not a call id", value)
		}
		refs := s.endpointRefs(ctx, id)
		if key == "send_call" {
			refs["send"] = n
		} else {
			refs["poll"] = n
		}
		return s.AttachEndpoints(ctx, id, refs)
	case "adapter":
		err = s.DB.SetVendorAdapterConfig(ctx, id, want.AdapterConfig)
	case "enc_gsm7", "enc_latin1", "enc_ucs2":
		if cur.Transport != "smpp" {
			return refusef("encoding rules are for SMPP vendors")
		}
		enc := strings.TrimPrefix(key, "enc_")
		rule, perr := ParseWireSetting(enc, value)
		if perr != nil {
			return perr
		}
		if rule == nil { // back to the default: drop the entry
			err = s.DB.RemoveVendorProtocolPath(ctx, id, "$.encodings."+enc)
			break
		}
		b, _ := json.Marshal(map[string]any{enc: rule})
		// JSON_SET cannot create $.encodings.<enc> when $.encodings is
		// missing, so the object is merged in whole.
		err = s.DB.MergeVendorEncodings(ctx, id, string(b))
	case "host", "port", "system_id", "password", "sessions":
		path := map[string]string{"host": "$.host", "port": "$.port", "system_id": "$.system_id",
			"password": "$.password", "sessions": "$.sessions"}[key]
		var v any = value
		if key == "port" || key == "sessions" {
			n, _ := strconv.Atoi(value)
			v = n
		}
		err = s.DB.SetVendorQueueConfig(ctx, id, path, v)
	}
	if err != nil {
		return fmt.Errorf("update vendor %d: %w", id, err)
	}
	return nil
}

// DeleteVendor removes a vendor, and refuses while doing so would lose
// messages or leave a routing rule pointing at nothing.
//
// Both refusals are the point. A queue table is dropped with its vendor, so
// deleting one with messages in it discards traffic a client was told had been
// accepted; and a rule pointing at a deleted vendor sends every matching
// message nowhere.
func (s Store) DeleteVendor(ctx context.Context, id int64) error {
	d, err := s.GetVendor(ctx, id)
	if err != nil {
		return err
	}
	if d.Queued > 0 {
		return refusef("vendor %q still has %d message(s) waiting in queue_%s. Deleting it would throw them away, and each one was answered 200 ACCEPTED. Pause it and let the queue drain, or empty the queue deliberately first",
			d.CID, d.Queued, d.CID)
	}
	if d.Routes > 0 {
		return refusef("%d routing rule(s) still send to vendor %q. Point them elsewhere first, or every message they match would have nowhere to go",
			d.Routes, d.CID)
	}
	// Its HTTP calls are kept, as nobody's: free for another vendor to pick
	// (the Admin API's vendor delete does the same).
	if err := s.releaseCalls(ctx, id, nil); err != nil {
		return err
	}
	if err := s.DB.DeleteVendorRow(ctx, id); err != nil {
		return fmt.Errorf("delete vendor %d: %w", id, err)
	}
	if s.Queues != nil {
		if err := s.Queues.Drop(ctx, d.CID); err != nil {
			return fmt.Errorf("vendor deleted, but its empty queue table could not be dropped: %w", err)
		}
	}
	return nil
}

// endpointRefs reads which calls a vendor currently uses, so changing one
// leaves the others alone.
func (s Store) endpointRefs(ctx context.Context, vendorID int64) map[string]int64 {
	raw, _ := s.DB.VendorAdapterConfig(ctx, vendorID)
	var refs struct {
		Endpoints map[string]int64 `json:"endpoints"`
	}
	_ = json.Unmarshal([]byte(raw), &refs)
	if refs.Endpoints == nil {
		return map[string]int64{}
	}
	return refs.Endpoints
}

// deploymentsJSON renders an assignment for the column. An empty list is
// stored as NULL rather than "[]", so the engine's "nothing chosen" and the
// database's "no value" are the same thing to read back.
func deploymentsJSON(l deployment.List) any {
	if len(l) == 0 {
		return nil
	}
	b, err := json.Marshal(l)
	if err != nil {
		return nil
	}
	return string(b)
}
