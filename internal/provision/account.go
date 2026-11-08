package provision

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/senderid"
)

// Account is what an operator gives us to create a sending identity. Only the
// fields a gateway cannot send a message without are here; everything else
// keeps its column default and is changed later. [SCOPE P14-ACCT]
type Account struct {
	Username  string // the ONE identity: HTTP username, SMPP system_id, JWT sub
	CompanyID int64  // required — balance, suspension and quotas all resolve here
	HTTP      bool   // may submit over HTTP
	SMPP      bool   // may bind over SMPP

	// Optional, with the column's own default when left empty.
	ChargePoint   string // acceptance | submit | delivery
	MultipartMode string // udh | tlv (sar, payload, message_payload mean tlv)

	// Deployments is which gateways serve this account and its rate on each
	// (config/0122 + config/0123). Choosing is MANDATORY, exactly as it is in
	// the Admin API — a provision rule must never be looser than the door
	// that manages the same table.
	//
	// Throughput lives HERE and nowhere else now. The old account-wide
	// Throughput field is gone: a rate belongs to a gateway, and what an
	// operator is quoted is the sum across them.
	Deployments deployment.List
}

// maxUsername is the width of the column AND of an SMPP system_id on the wire.
// The username is the system_id (PO 2026-08-25), so this is not merely a
// database limit: a longer name could be stored and then never bind.
const maxUsername = 15

// chargePoints and multipartModes are the engine's own vocabularies. Kept as
// lists rather than a free string so the console can offer them and a typo is
// refused instead of silently becoming the default — which is what made
// multipart_mode worth checking in the first place (2026-09-18).
var (
	chargePoints   = []string{"acceptance", "submit", "delivery"}
	multipartModes = []string{"udh", "tlv", "sar", "payload", "message_payload"}
)

// ChargePoints and MultipartModes let a caller offer the real choices.
func ChargePoints() []string   { return append([]string(nil), chargePoints...) }
func MultipartModes() []string { return append([]string(nil), multipartModes...) }

// ValidateAccount applies every rule that does not need the store. Split out
// so a console can check an answer the moment it is typed, rather than after
// the operator has answered eight more questions.
func ValidateAccount(a Account) error {
	name := strings.TrimSpace(a.Username)
	switch {
	case name == "":
		return refusef("a username is required")
	case len(name) > maxUsername:
		return refusef("username %q is %d characters: the limit is %d, because the username IS the SMPP system_id and a longer one could be stored and then never bind",
			name, len(name), maxUsername)
	case strings.ContainsAny(name, " \t\r\n"):
		return refusef("username %q contains a space: it travels in an SMPP bind and an HTTP header, where a space cannot survive", name)
	}
	if a.CompanyID <= 0 {
		return refusef("every account must belong to a company: balance, suspension and quotas all settle at company level, so an unlinked account bills against nothing")
	}
	if !a.HTTP && !a.SMPP {
		return refusef("an account that may use neither HTTP nor SMPP cannot send anything: switch at least one on")
	}
	if a.ChargePoint != "" && !oneOf(a.ChargePoint, chargePoints) {
		return refusef("charge point %q is not one this gateway understands: %s. It says WHEN the account is charged, not whether",
			a.ChargePoint, strings.Join(chargePoints, ", "))
	}
	if a.MultipartMode != "" && !oneOf(a.MultipartMode, multipartModes) {
		return refusef("multipart mode %q is not one this gateway understands: use %q for concatenated parts or %q for one message_payload (%s mean the same as %q). An unrecognised value would silently fall back to %q",
			a.MultipartMode, "udh", "tlv", `"sar", "payload", "message_payload"`, "tlv", "udh")
	}
	// Choosing is checked by the caller, not here: it is mandatory only once
	// a deployment has been registered, and that needs the database. See
	// Store.assertDeploymentsChosen.
	for _, d := range a.Deployments {
		if d.MTThroughput < 0 || d.MOThroughput < 0 || d.DLRThroughput < 0 {
			return refusef("throughput cannot be negative: leave it empty for uncapped")
		}
	}
	return nil
}

// CreateAccount writes the account and returns its id.
//
// The checks that need the store are done here: the company must exist and the
// username must be free. Both are also enforced by the schema (uq_ssc_username,
// and the company foreign key), but a driver error naming a constraint is not
// something to show an operator.
func (s Store) CreateAccount(ctx context.Context, a Account) (int64, error) {
	a.Username = strings.TrimSpace(a.Username)
	a.ChargePoint = strings.ToLower(strings.TrimSpace(a.ChargePoint))
	a.MultipartMode = strings.ToLower(strings.TrimSpace(a.MultipartMode))
	if err := ValidateAccount(a); err != nil {
		return 0, err
	}
	// Mandatory once a deployment exists, and refusing an id nobody
	// registered — the same two rules the Admin API applies. A provision rule
	// must never be looser than the door that manages the same table.
	if err := s.assertDeploymentsChosen(ctx, a.Deployments); err != nil {
		return 0, err
	}
	if err := s.assertDeploymentsExist(ctx, a.Deployments); err != nil {
		return 0, err
	}

	ok, err := s.DB.CompanyExists(ctx, a.CompanyID)
	if err != nil {
		return 0, fmt.Errorf("look up company %d: %w", a.CompanyID, err)
	}
	if !ok {
		return 0, refusef("there is no company #%d", a.CompanyID)
	}
	if err := s.assertSellingCompany(ctx, a.CompanyID); err != nil {
		return 0, err
	}
	ok, err = s.DB.AccountUsernameTaken(ctx, a.Username)
	if err != nil {
		return 0, fmt.Errorf("look up username %q: %w", a.Username, err)
	}
	if ok {
		return 0, refusef("an account called %q already exists: the username is the bind identity, so it cannot be shared", a.Username)
	}

	// Every account authenticates by JWT, so it needs its own secret from the
	// moment it exists — an account with no way in is not a usable account.
	// The dev seed shares one secret across its fixtures; a real one must not.
	secret, err := newSecret()
	if err != nil {
		return 0, fmt.Errorf("generate the account's JWT secret: %w", err)
	}
	jwtCfg, err := json.Marshal(map[string]any{"alg": "HS256", "secret": secret})
	if err != nil {
		return 0, err
	}

	// smpp_config carries the system_id the portal also stamps. The engine
	// derives the bind identity from the username column and ignores this
	// field (loader_mysql.go:461), but a row that disagreed would confuse a
	// human reading the table, and the exports read it.
	smpp := map[string]any{"system_id": a.Username}
	if a.MultipartMode != "" {
		smpp["multipart_mode"] = a.MultipartMode
	}
	smppCfg, err := json.Marshal(smpp)
	if err != nil {
		return 0, err
	}

	// charge_point is written only when given (the column default otherwise).
	row := dal.AccountInsert{
		Username: a.Username, CompanyID: a.CompanyID, JWT: string(jwtCfg), AllowedSenders: "[]",
		SMPPConfig: string(smppCfg), HTTP: boolInt(a.HTTP), SMPP: boolInt(a.SMPP), ChargePoint: a.ChargePoint,
	}
	// The account's gateways and its rate on each (config/0123). Written on
	// create because it is required: an account nobody has placed has no
	// throughput at all and no gateway that admits it.
	if b, err := json.Marshal(a.Deployments); err == nil {
		row.Deployments = dal.NullString{String: string(b), Valid: true}
	}

	id, err := s.DB.InsertAccount(ctx, row)
	if err != nil {
		return 0, fmt.Errorf("create the account: %w", err)
	}
	// The dialect row is created by a trigger (config/0021), so there is no
	// implicit "no row = native" state to get wrong here.
	return id, nil
}

// AccountRow is one line of the account list.
type AccountRow struct {
	ID       int64
	Username string
	Company  string
	Disabled bool
	// Throughput is the TOTAL across this account's deployments
	// (config/0123) — the number an operator is quoted. 0 means nothing is
	// capped per gateway, either because it is on every gateway or because
	// no rate was set.
	Throughput  int
	Deployments deployment.List
	ChargePoint string
}

// ListAccounts is the console's list view, newest last so a just-created
// account is where the eye ends up.
func (s Store) ListAccounts(ctx context.Context, limit int) ([]AccountRow, error) {
	if limit <= 0 {
		limit = 200
	}
	list, err := s.DB.AccountList(ctx, limit)
	if err != nil {
		return nil, wordedErr(err, "list accounts")
	}
	var out []AccountRow
	for _, l := range list {
		r := AccountRow{ID: l.ID, Username: l.Username, Company: l.Company, ChargePoint: l.ChargePoint}
		// The total across its gateways is what the list shows (config/0123).
		r.Deployments, _ = deployment.Parse(l.Deployments)
		r.Throughput = r.Deployments.Total(deployment.MT)
		r.Disabled = l.Disabled != 0
		out = append(out, r)
	}
	return out, nil
}

// wordedErr words a list read's failure as before the move into the DAL:
// the statement's own failure after what (as fmt "what: err"), a row that
// would not read, or a read cut short, as it is (dal.RowsError).
func wordedErr(err error, what string) error {
	var re dal.RowsError
	if errors.As(err, &re) {
		return re.Err
	}
	return fmt.Errorf("%s: %w", what, err)
}

// assertSellingCompany refuses a buying company as an account's owner: a
// buying company is a carrier, and a customer account filed under it is
// charged against our supplier's balance and shown on our supplier's screens.
// The mirror of assertBuyingCompany (a vendor's company), and the Admin API's
// rule (accountRoutes.js assertAccountCompany) — selling, both, or an older
// company with no type pass.
func (s Store) assertSellingCompany(ctx context.Context, companyID int64) error {
	name, typ, err := s.DB.CompanyNameType(ctx, companyID)
	if err != nil {
		return nil // existence is checked, and refused, by the caller
	}
	if strings.EqualFold(strings.TrimSpace(typ), "buying") {
		return refusef("company %q is a buying company; an account's company is one we sell to — choose a selling or both company", name)
	}
	return nil
}

// SetAccountDisabled switches an account off or on. Disabling is the reversible
// answer to "stop this customer now"; deleting is not, and the console offers
// this first for that reason.
//
// A switch that really moved is written to the audit trail as the Admin API
// writes it: action accounts.update, after_data naming is_disabled. That row
// is how "Clear abuse" knows a PERSON switched the account off and leaves it
// off. The console wrote none, so an account the abuse monitor had suspended
// and a person then switched on and off again here was switched back on by
// the next "Clear abuse" (QA 2026-10-08, B-07). A switch to what the account
// already is is written too: it is still a person's own decision.
func (s Store) SetAccountDisabled(ctx context.Context, id int64, disabled bool) error {
	row, err := s.DB.AccountForSwitch(ctx, id)
	if errors.Is(err, dal.ErrNoRows) {
		return refusef("there is no account #%d", id)
	}
	if err != nil {
		return fmt.Errorf("read account %d: %w", id, err)
	}
	username, companyID, company, was := row.Username, row.CompanyID, row.Company, row.Disabled
	if err := s.DB.SetAccountDisabled(ctx, id, boolInt(disabled)); err != nil {
		return fmt.Errorf("update account %d: %w", id, err)
	}
	// Written whether or not the switch moved, as the Admin API writes a
	// save that names is_disabled (go-live test 18 Oct 2026, P10 D7; its
	// http/crud.js auditSent): a person who switches off an account the
	// abuse monitor has already suspended changes no value, and without this
	// row "Clear abuse" would take the suspension as the only reason it is
	// off and switch it back on.
	return s.audit(ctx, auditEntry{
		Action: "accounts.update", Entity: "account", Feature: "accounts", EntityID: id,
		AccountID: id, CompanyID: companyID, CompanyName: company,
		Label:   username,
		Summary: "Updated account " + username + " from the console",
		Detail:  map[string]any{"fields": []string{"is_disabled"}},
		Before:  map[string]any{"is_disabled": was},
		After:   map[string]any{"is_disabled": boolInt(disabled)},
	})
}

// CompanyRow is one company, for the picker an account needs.
type CompanyRow struct {
	ID      int64
	Name    string
	Balance string
	Prepaid bool // billing_config.prepaid, read as the engine reads it
}

// ListCompanies backs the "which company?" question, so an operator picks from
// what exists instead of guessing an id.
func (s Store) ListCompanies(ctx context.Context, limit int) ([]CompanyRow, error) {
	if limit <= 0 {
		limit = 200
	}
	list, err := s.DB.CompanyList(ctx, limit)
	if err != nil {
		return nil, wordedErr(err, "list companies")
	}
	var out []CompanyRow
	for _, c := range list {
		out = append(out, CompanyRow(c))
	}
	return out, nil
}

// oneOf compares without case on BOTH sides. It lowered only the value once,
// which worked for the lower-case vocabularies (transports, charge points) and
// silently failed for an upper-case one: an endpoint method of "GET" was
// refused with "GET is not one we send: GET, POST or PUT" (2026-09-18).
func oneOf(v string, set []string) bool {
	v = strings.TrimSpace(v)
	for _, s := range set {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// newSecret is 32 bytes of randomness, hex-encoded — long enough that an HS256
// token cannot be brute-forced, and printable so an operator can copy it.
func newSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

var _ = dal.ErrNoRows // kept: callers switch on it as this package grows

// AccountDetail is everything the console shows on one account's page.
type AccountDetail struct {
	AccountRow
	CompanyID     int64
	HTTP, SMPP    bool
	MultipartMode string
	SystemID      string
	HasJWTSecret  bool
	DedupeTTL     string
	Created       string
	// SpamCheck: the spam rules apply to this account at all (enforce_spam_check,
	// on by default). SpamStrict: they are also run on the leet-folded text.
	SpamCheck, SpamStrict bool
	// DefaultSender is sent with when a message's sender is missing, badly
	// formed or not allotted (config/0129); "" = none.
	DefaultSender string
	// AllowedSenders is allowed_sender_ids as stored: empty or ["*"] = any
	// sender (refdata parseSenderIDs reads both so).
	AllowedSenders []string
	// Where its receipts and incoming messages go (D-137): bind, url or both,
	// and how often the bind is tried before the webhook takes over. See
	// pushchannel.go. Read as refdata reads them: both and 3 when unset.
	DLRChannel, MOChannel           string
	DLRBindAttempts, MOBindAttempts int
	// The account's SMPP windows (smppwindow.go): smpp_config.window_size and
	// .deliver_window; 0 = the gateway's own.
	AcceptWindow, DeliverWindow int
}

// GetAccount reads one account in full. Credential material is reported as
// present or absent, never returned: a console prints what it is given, and a
// secret on a screen is a secret in a scrollback buffer.
func (s Store) GetAccount(ctx context.Context, id int64) (AccountDetail, error) {
	var d AccountDetail
	r, err := s.DB.AccountDetail(ctx, id)
	d.ID, d.Username, d.Company, d.Disabled, d.ChargePoint = r.ID, r.Username, r.Company, r.Disabled, r.ChargePoint
	d.CompanyID, d.HTTP, d.SMPP, d.Created = r.CompanyID, r.HTTP, r.SMPP, r.Created
	d.SpamCheck, d.SpamStrict, d.DefaultSender = r.SpamCheck, r.SpamStrict, r.DefaultSender
	d.DLRChannel, d.DLRBindAttempts, d.MOChannel, d.MOBindAttempts = r.DLRChannel, r.DLRBindAttempts, r.MOChannel, r.MOBindAttempts
	jwtCfg, smppCfg, deployJSON, sendersJSON, dedupe := r.JWT, r.SMPPConfig, r.Deployments, r.AllowedSenders, r.DedupeTTL
	if err == dal.ErrNoRows {
		return d, refusef("there is no account #%d", id)
	}
	if err != nil {
		return d, fmt.Errorf("read account %d: %w", id, err)
	}
	// Its gateways, and the total rate across them (config/0123).
	d.Deployments, _ = deployment.Parse(deployJSON)
	d.Throughput = d.Deployments.Total(deployment.MT)
	// Unreadable reads as empty — any sender — as the engine reads it.
	_ = json.Unmarshal([]byte(sendersJSON), &d.AllowedSenders)
	var j struct {
		Secret string `json:"secret"`
	}
	_ = json.Unmarshal([]byte(jwtCfg), &j)
	d.HasJWTSecret = j.Secret != ""
	var sc struct {
		SystemID  string `json:"system_id"`
		Multipart string `json:"multipart_mode"`
	}
	_ = json.Unmarshal([]byte(smppCfg), &sc)
	d.SystemID, d.MultipartMode = sc.SystemID, sc.Multipart
	d.AcceptWindow, d.DeliverWindow = smppWindowsOf(smppCfg)
	if dedupe.Valid {
		d.DedupeTTL = strconv.FormatInt(dedupe.Int64, 10) + "s"
	} else {
		d.DedupeTTL = "gateway default"
	}
	return d, nil
}

// AccountFields are the settings an operator may change, and what each means.
func AccountFields() []Field {
	return []Field{
		{Key: "username", Label: "Username", Kind: "text",
			Hint: "the one identity: HTTP username, SMPP system_id and JWT subject. Changing it changes how the customer signs in"},
		{Key: "company", Label: "Company", Kind: "int",
			Hint: "balance, suspension and quotas settle here. Set once: an account stays with its company"},
		{Key: "deployments", Label: "Deployments & throughput", Kind: "text",
			Hint: `which gateways serve it, and its MT segments/sec on each: "*" for every gateway (uncapped), "lhr-1" for one, "lhr-1=100,khi-1=50" to set rates. Required — throughput lives here now`},
		{Key: "charge_point", Label: "Charge point", Kind: "choice", Choices: chargePoints,
			Hint: "when the account is charged — not whether"},
		{Key: "multipart", Label: "Long inbound messages", Kind: "choice", Choices: []string{"udh", "tlv"},
			Hint: "how a long message is delivered to this account's binds"},
		{Key: "http", Label: "May submit over HTTP", Kind: "bool"},
		{Key: "smpp", Label: "May bind over SMPP", Kind: "bool"},
		{Key: "spam_check", Label: "Spam check", Kind: "bool",
			Hint: "off = the spam rules are not run for this account at all"},
		{Key: "spam_strict", Label: "Strict spam check", Kind: "bool",
			Hint: "also catch spellings like l0an or c.a.s.i.n.o"},
		{Key: "allowed_senders", Label: "Allowed senders", Kind: "text",
			Hint: `the senders this account may send with, separated by commas: "BRAND, 8123". "*" allows any sender, and so does an EMPTY list. Its approved sender IDs count too`},
		{Key: "default_sender", Label: "Default sender", Kind: "text",
			Hint: "sent with when a message has no sender, a badly formed one, or one not allotted to this account. Must be one of the account's senders; empty clears it"},
		// Last (D-137), so the numbers of the settings above stay.
		{Key: "dlr_channel", Label: "Receipts go to", Kind: "choice", Choices: PushChannels,
			Hint: "bind = the client's SMPP bind only; url = its DLR webhook only; both = the bind first, the webhook once the bind has failed the attempts below. url and both need an active DLR webhook (HTTP calls)"},
		{Key: "dlr_bind_attempts", Label: "Receipt bind attempts", Kind: "int",
			Hint: "with both: how often the bind is tried before the DLR webhook takes over. 1 to 1000, and less than the webhook's own retry attempts"},
		{Key: "mo_channel", Label: "Incoming messages go to", Kind: "choice", Choices: PushChannels,
			Hint: "bind = the client's SMPP bind only; url = its MO webhook only; both = the bind first, the webhook once the bind has failed the attempts below. url and both need an active MO webhook (HTTP calls)"},
		{Key: "mo_bind_attempts", Label: "Incoming bind attempts", Kind: "int",
			Hint: "with both: how often the bind is tried before the MO webhook takes over. 1 to 1000, and less than the webhook's own retry attempts"},
		// After those (28 Oct 2026), for the same reason.
		{Key: "accept_window", Label: "Accept window", Kind: "text",
			Hint: "submits one SMPP bind may have unanswered. Any whole number above 0; none = the gateway's own (YAKU_SMPP_WINDOW, 16)"},
		{Key: "deliver_window", Label: "Delivery window", Kind: "text",
			Hint: "receipts and incoming messages one SMPP bind may have unanswered, one setting for both. Any whole number above 0; none = the gateway's own (YAKU_SMPP_DELIVER_WINDOW, 64)"},
	}
}

// UpdateAccount changes one setting, checking it the way creation would.
func (s Store) UpdateAccount(ctx context.Context, id int64, key, value string) error {
	cur, err := s.GetAccount(ctx, id)
	if err != nil {
		return err
	}
	// Judged on its own: nothing in ValidateAccount depends on it, and an
	// edit is not refused for a value it is not changing.
	if key == "allowed_senders" {
		return s.setAllowedSenders(ctx, cur, value)
	}
	// Where receipts and incoming messages go: judged with the other half
	// of their pair and the account's webhooks (pushchannel.go).
	if _, _, ok := pushKey(key); ok {
		return s.setPushChannel(ctx, cur, key, value)
	}
	// An SMPP window: judged on its own (smppwindow.go).
	if _, ok := smppWindowKeys[key]; ok {
		return s.setSMPPWindow(ctx, cur, key, value)
	}
	// Build the account as it WOULD be and run the same rules, so an edit can
	// never reach a state creation would have refused.
	want := Account{
		Username: cur.Username, CompanyID: cur.CompanyID, HTTP: cur.HTTP, SMPP: cur.SMPP,
		ChargePoint: cur.ChargePoint, MultipartMode: cur.MultipartMode,
		Deployments: cur.Deployments,
	}
	value = strings.TrimSpace(value)
	switch key {
	case "username":
		want.Username = value
	case "company":
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return refusef("%q is not a company id", value)
		}
		want.CompanyID = n
	case "deployments":
		// Compact entry, because this editor is one text field per setting:
		//   *                       every gateway, uncapped per gateway
		//   lhr-1                   that gateway, uncapped
		//   lhr-1=100,khi-1=50      MT segments/sec on each
		list, err := ParseDeploymentSpec(value)
		if err != nil {
			return err
		}
		want.Deployments = list
	case "charge_point":
		want.ChargePoint = strings.ToLower(value)
	case "multipart":
		want.MultipartMode = strings.ToLower(value)
	case "http":
		want.HTTP = truthy(value)
	case "smpp":
		want.SMPP = truthy(value)
	case "spam_check", "spam_strict", "default_sender":
		// Not part of Account: nothing in ValidateAccount depends on them.
	default:
		return refusef("%q is not a setting on an account", key)
	}
	if err := ValidateAccount(want); err != nil {
		return err
	}

	switch key {
	case "username":
		if want.Username != cur.Username {
			taken, err := s.DB.AccountUsernameTakenByOther(ctx, want.Username, id)
			if err != nil {
				return err
			}
			if taken {
				return refusef("another account is already called %q", want.Username)
			}
		}
		// The username IS the system_id, so the stored copy is restamped with
		// it — a row that disagreed would confuse anyone reading the table,
		// and the exports read that field.
		err = s.DB.SetAccountUsername(ctx, id, want.Username)
	case "company":
		ok, e := s.DB.CompanyExists(ctx, want.CompanyID)
		if e != nil {
			return e
		}
		if !ok {
			return refusef("there is no company #%d", want.CompanyID)
		}
		// An account stays with the company it was made under; only an older
		// account that has none may be given one (the Admin API's rule,
		// accountRoutes.js assertAccountCompany — change together).
		has, e := s.DB.AccountCompanyOf(ctx, id)
		if e != nil {
			return fmt.Errorf("read account %d: %w", id, e)
		}
		if has == want.CompanyID {
			return nil // already there
		}
		if has != 0 {
			return refusef("an account stays with the company it was made under: its balance, invoices and message history are that company's — make a new account under company #%d instead", want.CompanyID)
		}
		if e := s.assertSellingCompany(ctx, want.CompanyID); e != nil {
			return e
		}
		err = s.DB.SetAccountCompany(ctx, id, want.CompanyID)
	case "deployments":
		if err := s.assertDeploymentsChosen(ctx, want.Deployments); err != nil {
			return err
		}
		if err := s.assertDeploymentsExist(ctx, want.Deployments); err != nil {
			return err
		}
		b, _ := json.Marshal(want.Deployments)
		err = s.DB.SetAccountDeployments(ctx, id, string(b))
	case "charge_point":
		err = s.DB.SetAccountChargePoint(ctx, id, want.ChargePoint)
	case "multipart":
		err = s.DB.SetAccountMultipart(ctx, id, want.MultipartMode)
	case "http":
		err = s.DB.SetAccountHTTP(ctx, id, boolInt(want.HTTP))
	case "smpp":
		err = s.DB.SetAccountSMPP(ctx, id, boolInt(want.SMPP))
	case "spam_check":
		err = s.DB.SetAccountSpamCheck(ctx, id, boolInt(truthy(value)))
	case "spam_strict":
		err = s.DB.SetAccountSpamStrict(ctx, id, boolInt(truthy(value)))
	case "default_sender":
		if value != "" {
			if err := s.assertDefaultSender(ctx, id, value); err != nil {
				return err
			}
		}
		err = s.DB.SetAccountDefaultSender(ctx, id, value)
	}
	if err != nil {
		return fmt.Errorf("update account %d: %w", id, err)
	}
	return nil
}

// DeleteAccount removes an account. What goes with it is listed by the caller
// first: an account carries its dialect row, its stored credentials and its
// message history, and none of that comes back.
//
// An account a routing rule still names is refused (QA ACC-002): the rule was
// left pointing at nothing. Every rule belongs to one account since routing
// plans (2026-09-30), so its rules are deleted or moved first. The Admin API
// refuses the same (accountRoutes.js assertNoRulesFor). It is a check, not a
// lock: a rule saved for the account between the read and the delete is left
// naming nothing, as before (no foreign key); that race is accepted.
func (s Store) DeleteAccount(ctx context.Context, id int64) error {
	if rules, err := s.rulesOfAccount(ctx, id); err != nil {
		return err
	} else if len(rules) > 0 {
		shown := rules
		if len(shown) > 5 {
			shown = shown[:5]
		}
		more := ""
		if len(rules) > 5 {
			more = fmt.Sprintf(" and %d more", len(rules)-5)
		}
		return refusef("account #%d is still used by %s: %s%s. %s",
			id, plural(len(rules), "1 routing rule", fmt.Sprintf("%d routing rules", len(rules))), strings.Join(shown, ", "), more,
			plural(len(rules), "Delete it, or move it to another account, first", "Delete them, or move them to another account, first"))
	}
	n, err := s.DB.DeleteAccountRow(ctx, id)
	if err != nil {
		return fmt.Errorf("delete account %d: %w", id, err)
	}
	if n == 0 {
		return refusef("there is no account #%d", id)
	}
	return nil
}

// rulesOfAccount names the routing rules (any direction, on or off) that
// belong to an account, as `#id "name" (MT)`, in id order.
func (s Store) rulesOfAccount(ctx context.Context, id int64) ([]string, error) {
	rules, err := s.DB.AccountRuleRows(ctx, id)
	if err != nil {
		return nil, wordedErr(err, fmt.Sprintf("read account %d's routing rules", id))
	}
	var out []string
	for _, r := range rules {
		out = append(out, fmt.Sprintf("#%d %q (%s)", r.ID, r.Name, r.Direction))
	}
	return out, nil
}

func truthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "y", "yes", "true", "1", "on":
		return true
	}
	return false
}

// ParseDeploymentSpec reads the console's compact form of a deployments list.
//
// The account editor is one text field per setting, so the portal's row of
// checkboxes and number boxes has to fit on one line:
//
//   - every gateway, with no per-gateway cap
//     lhr-1                 that gateway, uncapped
//     lhr-1=100,khi-1=50    MT segments/sec on each
//
// Only the MT rate is settable this way. MO and DLR are rarer and would make
// the line unreadable; they are set from the portal, and an edit here leaves
// whatever they already hold untouched at the entry level.
func ParseDeploymentSpec(spec string) (deployment.List, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, refusef("name at least one deployment; throughput is set on each")
	}
	var out deployment.List
	seen := map[string]bool{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, rate, hasRate := strings.Cut(part, "=")
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, refusef("%q names no deployment", part)
		}
		if id == "*" {
			// Offered and withdrawn on 2026-09-24. Say so, rather than
			// letting it fall through to "there is no deployment \"*\"",
			// which blames the operator for a name we invented.
			return nil, refusef(`"*" for every gateway was withdrawn: a rate needs a gateway to count on. Name the deployments instead`)
		}
		if len(id) > deployment.MaxIDLen {
			return nil, refusef("deployment id %q is longer than %d characters", id, deployment.MaxIDLen)
		}
		if seen[id] {
			return nil, refusef("%q is listed twice", id)
		}
		seen[id] = true
		e := deployment.Assignment{ID: id}
		if hasRate {
			n, err := strconv.Atoi(strings.TrimSpace(rate))
			if err != nil {
				return nil, refusef("%q is not a number of segments per second", rate)
			}
			if n < 0 {
				return nil, refusef("throughput cannot be negative: leave it off for uncapped")
			}
			e.MTThroughput = n
		}
		out = append(out, e)
	}
	return out, nil
}

// assertDeploymentsExist refuses an id the register has never heard of. A
// typo here does not fail loudly — it confines the account to a gateway that
// does not exist, so every submit is refused everywhere.
func (s *Store) assertDeploymentsExist(ctx context.Context, list deployment.List) error {
	for _, e := range list {
		n, err := s.DB.DeploymentRegistered(ctx, e.ID)
		if err != nil {
			return fmt.Errorf("check the deployment: %w", err)
		}
		if n == 0 {
			return refusef("there is no deployment %q — register it first, or an account assigned to it can submit nowhere", e.ID)
		}
	}
	return nil
}

// assertDeploymentsChosen refuses a save that names no deployment — but only
// once at least one is registered.
//
// That condition is the whole point. Throughput is set per deployment, so an
// account with none has no rate and no gateway that admits it, and the doors
// should insist. But on an install that has never registered a deployment
// there is nothing to choose FROM, and insisting would make it impossible to
// create an account at all — a single-gateway install must keep working
// exactly as it did before any of this existed.
func (s *Store) assertDeploymentsChosen(ctx context.Context, list deployment.List) error {
	if list.Chosen() {
		return nil
	}
	n, err := s.DB.ActiveDeploymentCount(ctx)
	if err != nil {
		return nil // cannot tell: do not block on our own uncertainty
	}
	if n == 0 {
		return nil // nothing registered, so nothing to choose
	}
	return refusef("name at least one deployment: there %s %d registered, and an account's throughput is set on the gateways it runs on",
		plural(n, "is", "are"), n)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// assertDefaultSender refuses a default sender that is not one of the
// account's senders — allowed_sender_ids, or an approved sender_ids row
// assigned to it — or, when every sender is allowed ('*'), one the gateway
// cannot send with. The Admin API's rule (services/defaultSender.js); the
// engine ignores a default that later stops being allotted.
func (s Store) assertDefaultSender(ctx context.Context, id int64, sender string) error {
	if !wellFormedSender(sender) {
		return refusef("%q is not a sender the gateway can send with: up to 15 digits, or up to 11 letters, digits, spaces and . - _", sender)
	}
	raw, err := s.DB.AccountAllowedSenders(ctx, id)
	if err != nil {
		return fmt.Errorf("read account %d's senders: %w", id, err)
	}
	var allowed []string
	_ = json.Unmarshal([]byte(raw.String), &allowed)
	approved, err := s.ApprovedSenders(ctx, id)
	if err != nil {
		return err
	}
	allotted := append(append([]string{}, allowed...), approved...)
	for _, v := range allotted {
		if v == "*" || v == sender {
			return nil
		}
	}
	if len(allotted) == 0 {
		return refusef("%q is not one of this account's senders — the account has no senders yet", sender)
	}
	return refusef("%q is not one of this account's senders — choose one of: %s", sender, strings.Join(allotted, ", "))
}

// wellFormedSender is the gateway's sender rule — the one internal/senderid
// rule the validate stage applies: up to 15 digits (an optional '+'), or up
// to 11 letters, digits, spaces and . - _
func wellFormedSender(v string) bool { return senderid.WellFormed(v) }
