// Package mo is the shared mobile-originated (inbound) receiver service: it takes
// a raw MO from a vendor edge (SMPP deliver_sm or HTTP webhook), resolves the
// owning client via the MO routes, and stages it durably for delivery. The
// inverse of the MT accept pipeline, kept deliberately small. [SCOPE P6/D-032]
package mo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/msisdn"
	"github.com/yakugate/yakugate/internal/netcode"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/lib/encoding"
)

// Stager persists an accepted MO into the shared staging queue (mo_queue).
type Stager interface {
	StageMO(ctx context.Context, m model.Message) error
}

// OrphanRecorder is an optional Stager capability: keep an inbound message no
// MO route owns (PO 2026-08-31). Recorded HERE, in the one place both the HTTP
// webhook and the SMPP deliver_sm edge funnel through, so neither can forget.
// RECORD ONLY — never re-ingested, even once a matching route exists: a
// weeks-old inbound message arriving at a customer who has no context for it
// would be worse than the gap it fills.
type OrphanRecorder interface {
	WriteOrphanMO(ctx context.Context, vendorID int64, source, destination, body, enc, mcc, mnc, reason string) error
}

// Logger is the minimal logging surface the service needs.
type Logger interface {
	Printf(format string, args ...any)
}

// Via is m as given to its client over t — a bind (smpp) or the webhook
// (http).
func Via(m model.Message, t model.Transport) model.Message {
	m.Ingress = t
	return m
}

// route is a compiled MO route: it owns a shortcode (receiver) for one account,
// optionally split by keyword (shared shortcodes) and MCC/MNC.
type route struct {
	id        int64
	accountID int64
	username  string // owning client's username (for the MO record table)
	priority  int
	recv      *regexp.Regexp // matches the client shortcode (destination); nil = any
	keyword   *regexp.Regexp // matches the body; nil = any
	// sender matches the SUBSCRIBER's number — the handset the message came
	// from (PO 2026-09-24). Until then the rule's sender_regex was offered on
	// MT rules only, because this matcher never saw the source address; an MO
	// rule could not be narrowed to particular originators at all.
	sender *regexp.Regexp // nil = any
	// The rule's changes (4 Nov 2026), as on MT rules: each condition's
	// "replace", applied with its own "match" to the message the rule wins —
	// recvRepl to the short code (to), senderRepl to the subscriber (from),
	// keywordRepl to the text. nil = no change; "" = replace with nothing.
	recvRepl, keywordRepl, senderRepl *string
	// vendorID is the vendor this rule accepts MO from; 0 = any vendor
	// (PO 2026-09-24). The engine always knew which vendor delivered an MO
	// and never used it, so an MO for a short code arriving from a vendor
	// with no business on it was routed to that short code's owner as though
	// genuine. Pinned, it becomes an unrouted orphan instead — which shows.
	// It is also what lets one short code, leased through two aggregators,
	// belong to a different account on each.
	vendorID int64
	mccmncs  []string // combined "mccmnc" strings (0048); empty = any network
	// Opt-out handling for the owning account (config/0095). Cached with the
	// route because an MO already resolves to a route, so honouring STOP costs
	// no extra lookup on the inbound path.
	optOutMode     string // "customer" (default, deliver only) or "gateway"
	optOutKeywords string
	optInKeywords  string
	// The owning company and its message-privacy setting (companies.
	// body_privacy, PO 2026-09-13). An all_encrypted company's inbound text
	// is staged and stored encrypted, like its outgoing text.
	companyID int64
	privacy   string
	// deployments is where the owning account is served (config/0122);
	// empty = everywhere. Decides which deployment delivers its MO (D-121).
	deployments deployment.List
}

// deliverer is the deployment that delivers an MO for this route's account
// (D-121, 2026-10-02): this one when the account is served here — it may be
// connected here — else the first deployment the account is placed on, which
// is where its client is. A vendor placed on several deployments can hand an
// MO to any of them, so the one that received it is not always the one that
// can deliver it. "" (shared, as before) when this gateway names no
// deployment.
func (r route) deliverer() string {
	own := deployment.Owner()
	if own == "" || r.deployments.Allows(own) {
		return own
	}
	return r.deployments[0].ID
}

// Inbound is a raw MO as seen at a vendor edge.
type Inbound struct {
	VendorID int64
	Source   string // subscriber MSISDN (becomes sender)
	Dest     string // client shortcode / number (becomes destination)
	Text     string
	Encoding string // gsm7 | ucs2
	MCC, MNC string
	// Segments is how many parts the subscriber's handset sent: a multi-part
	// message is joined before it gets here (2026-09-14) and is ONE message
	// of that many parts. 0 = one.
	Segments int
	// Transport is how the vendor gave it to us: smpp (a deliver_sm) or http
	// (its MO door) — the record's vendor_protocol (issue 17).
	Transport model.Transport
}

// Service resolves + stages MO. Routes are cached and refreshed by Reload.
type Service struct {
	// rules is the MO rules and the opt-outs in the config store (the DAL).
	rules dal.MORules
	stage Stager
	log   Logger
	mu    sync.RWMutex
	rts   []route
	// emptied remembers the rule changes already said to leave an address
	// empty (warnEmpty), so each is logged once.
	emptied sync.Map
	// orphan is where unowned MO is filed. Explicit rather than a type
	// assertion on the Stager: the stager is the QUEUE store, and under the
	// Redis gate that is Redis — but an orphan is a RECORD and belongs in the
	// hot store regardless of which queue backend is in use.
	orphan OrphanRecorder
	// seal encrypts the text of MO for an encrypted company. nil = this
	// process has no crypto, and such MO is refused rather than kept plain.
	seal Encryptor
	// hot writes the incoming record the moment the message is taken in
	// (2026-09-16). nil keeps the old behaviour, where an MO reached the
	// incoming log only once it had been delivered — so a customer whose
	// webhook was down could see none of their inbound traffic, and an MO we
	// eventually gave up on was never recorded at all.
	hot HotWriter
	// bill prices and books an incoming message when it is taken in
	// (billing.Moments.MOIn; issues 3 and 18). nil = not booked.
	bill *billing.Moments
}

// SetBilling wires the billing queue: an incoming message is priced and its
// money booked the moment it is taken in.
func (s *Service) SetBilling(b *billing.Moments) { s.bill = b }

// Encryptor seals text under a company's key (crypto.Service satisfies it).
type Encryptor interface {
	Encrypt(ctx context.Context, accountID, companyID int64, plaintext string) (string, error)
}

// SetEncryptor installs the encryptor for MO of companies set to encrypted.
func (s *Service) SetEncryptor(e Encryptor) { s.seal = e }

// SetOrphanRecorder wires the hot store that keeps unowned inbound messages.
// Optional: without it an unrouted MO is still ACKed and logged, just not kept.
func (s *Service) SetOrphanRecorder(rec OrphanRecorder) { s.orphan = rec }

// SetHotWriter wires the hot store that keeps the incoming log, so a message
// is recorded when it is taken in rather than when it is delivered.
func (s *Service) SetHotWriter(h HotWriter) { s.hot = h }

// New builds the service on the config store's MO rules (dal.MORules) and
// loads them once.
func New(ctx context.Context, rules dal.MORules, stage Stager, log Logger) *Service {
	s := &Service{rules: rules, stage: stage, log: log}
	if err := s.Reload(ctx); err != nil && log != nil {
		log.Printf("mo: initial route load: %v", err)
	}
	return s
}

// Reload refreshes the MO route cache from the config store.
func (s *Service) Reload(ctx context.Context) error {
	if s.rules == nil {
		return errors.New("no config store to read the MO rules from")
	}
	rows, readErr := s.rules.MORoutes(ctx)
	if rows == nil && readErr != nil {
		return readErr
	}
	var out []route
	for _, row := range rows {
		r := route{id: row.ID, accountID: row.AccountID, username: row.Username, priority: row.Priority,
			vendorID: row.VendorID, optOutMode: row.OptOutMode, optOutKeywords: row.OptOutWords,
			optInKeywords: row.OptInWords, companyID: row.CompanyID, privacy: row.Privacy}
		recv, kw, snd := row.Receiver, row.Keyword, row.Sender
		mccmncs, deps := row.MCCMNCs, row.Deployments
		// Forgiving, as everywhere the engine reads placement: an unreadable
		// value means everywhere.
		r.deployments, _ = deployment.Parse(deps)
		// A pattern that does not compile must never leave the rule in place
		// with that condition missing (PO 2026-09-14, the same ruling as MT
		// routes, GW-32): the rule would then match EVERY shortcode and take
		// every account's inbound messages. The rule is skipped instead — its
		// messages go unrouted and are kept as orphans, which shows.
		var okR, okK, okS bool
		r.recv, okR = compileMatch(recv)
		r.keyword, okK = compileMatch(kw)
		r.sender, okS = compileMatch(snd)
		r.recvRepl, r.keywordRepl, r.senderRepl = replaceOf(recv), replaceOf(kw), replaceOf(snd)
		if !okR || !okK || !okS {
			if s.log != nil {
				s.log.Printf("mo: route %d has a pattern that does not compile — ROUTE SKIPPED until it is fixed", r.id)
			}
			continue
		}
		if mccmncs != "" && mccmncs != "null" {
			_ = json.Unmarshal([]byte(mccmncs), &r.mccmncs)
		}
		out = append(out, r)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].priority < out[j].priority })
	s.mu.Lock()
	s.rts = out
	s.mu.Unlock()
	return readErr
}

// Ingest resolves the owning client for an MO and stages it. Returns the assigned
// canonical msg id and staged=true; staged=false means no MO route owns this
// shortcode — the MO is still ACKed at the edge (refusing it would only make the
// vendor redeliver something we still could not place), but since 2026-08-31 it
// is RECORDED as an orphan instead of vanishing.
func (s *Service) Ingest(ctx context.Context, in Inbound) (msgID string, staged bool, err error) {
	r, ok := s.match(in.VendorID, in.Source, in.Dest, in.Text, in.MCC, in.MNC)
	if !ok {
		if rec := s.orphan; rec != nil {
			if err := rec.WriteOrphanMO(ctx, in.VendorID, in.Source, in.Dest, in.Text,
				orEnc(in.Encoding), in.MCC, in.MNC, "no MO route owns this shortcode"); err != nil {
				// Best effort — failing to file the orphan must not turn into an
				// error the vendor sees and retries.
				if s.log != nil {
					s.log.Printf("orphan MO from %s to %s not recorded: %v", in.Source, in.Dest, err)
				}
			}
		}
		return "", false, nil
	}
	// The winning rule's changes (4 Nov 2026). The rule was matched, and the
	// opt-out words below are read, on what the vendor sent; the client is
	// given, and the record keeps, the changed message.
	out := s.rewrite(r, in)
	m := model.Message{
		ID:          ulid.Make().String(),
		Direction:   model.DirectionMO,
		VendorID:    in.VendorID,
		AccountID:   r.accountID,
		CompanyID:   r.companyID, // its record's table (D-136)
		Username:    r.username,
		RouteID:     r.id,
		Sender:      out.Source,
		Destination: out.Dest,
		Body:        out.Text,
		Encoding:    orEnc(out.Encoding),
		Segments:    max(in.Segments, 1),
		MCC:         in.MCC,
		MNC:         in.MNC,
		Transport:   in.Transport, // how it reached us (the record's vendor_protocol)
		// The deployment that delivers it to the client (D-121).
		QueueOwner: r.deliverer(),
	}
	// [SCOPE P1-OPTOUT] Honour STOP before staging, for accounts that asked us
	// to (config/0095). The message is staged and delivered EITHER WAY — the
	// customer needs to see the opt-out to keep their own records straight;
	// this only decides whether we ALSO write it down.
	//
	// Failure here is logged and swallowed rather than failing the MO: an
	// opt-out we could not record is a compliance problem to chase, but
	// dropping the customer's inbound message on the floor because of it would
	// be a second, larger one.
	if r.optOutMode == "gateway" {
		switch ReadIntent(in.Text, r.optOutKeywords, r.optInKeywords) {
		case IntentOptOut:
			if err := RecordOptOut(ctx, s.rules, r.accountID, in.Source, in.Dest); err != nil && s.log != nil {
				s.log.Printf("opt-out for account %d not recorded: %v", r.accountID, err)
			} else if s.log != nil {
				s.log.Printf("opt-out recorded: account %d will no longer send to this subscriber", r.accountID)
			}
		case IntentOptIn:
			if err := RecordOptIn(ctx, s.rules, r.accountID, in.Source); err != nil && s.log != nil {
				s.log.Printf("opt-in for account %d not recorded: %v", r.accountID, err)
			} else if s.log != nil {
				s.log.Printf("opt-in recorded: account %d may send to this subscriber again", r.accountID)
			}
		}
	}
	// An encrypted company's inbound text is encrypted BEFORE it is staged
	// (PO 2026-09-13), the way outgoing text is encrypted before its queue:
	// mo_queue can hold a message for days while the customer is offline.
	// Everything above (route match, opt-out words) and the capture below saw
	// the real text; the consumers decrypt it just before delivery. Refused
	// rather than staged plain when it cannot be encrypted.
	queued := m
	if r.privacy == "all_encrypted" && m.Body != "" {
		if s.seal == nil {
			return "", false, fmt.Errorf("mo for account %d: its company is set to encrypted and this process has no encryption", r.accountID)
		}
		ct, err := s.seal.Encrypt(ctx, r.accountID, r.companyID, m.Body)
		if err != nil {
			return "", false, fmt.Errorf("mo for account %d not encrypted: %w", r.accountID, err)
		}
		queued.Body = ct
	}
	// Priced by the account's incoming-message card before it is queued, so
	// its record has the price whichever writes it (this intake, or its
	// delivery when the client takes it first); booked once it is taken in.
	billCo, bill := s.bill.PriceMO(ctx, &queued)
	if err := s.stage.StageMO(ctx, queued); err != nil {
		return "", false, err
	}
	s.bill.Book(ctx, billCo, bill)
	// On the incoming log from the moment we take it in (2026-09-16), in the
	// form it was staged in — sealed for an encrypted company, as the record
	// has always been. Delivery then settles its status instead of creating
	// the record, so a customer whose webhook is down still sees every message
	// that arrived for them, and one we eventually abandon is on the log as
	// expired rather than nowhere.
	//
	// Best effort: an inbound message already accepted from the carrier must
	// not be dropped because our own log write failed. Delivery writes the
	// record in that case, exactly as it did before.
	if s.hot != nil {
		rec := queued
		rec.Status = model.StatusQueued
		if err := s.hot.Write(ctx, rec); err != nil && s.log != nil {
			s.log.Printf("mo %s not written to the incoming log yet (%v) — delivery will write it", m.ID, err)
		}
	}
	// [SCOPE P13-CAPTURE] The inbound message as received, against the account
	// the MO route just handed it to.
	//
	// Emitted HERE rather than at either edge, because this is the first point
	// at which an MO belongs to an account at all — before the route matches
	// there is no capture to write to, and both edges (P3's deliver_sm and
	// P6's webhook) reach this same function, so one call covers both instead
	// of two that could drift.
	//
	// An UNROUTED MO deliberately records nothing: it belongs to no account,
	// so there is no capture that could own it. Those are already filed as
	// orphans above, which is where an operator looks for them.
	if c, on := logging.ActiveForAccountID(m.AccountID); on {
		f := logging.Fields{
			"messageId": m.ID, "from": m.Sender, "to": m.Destination,
			"text": m.Body, "encoding": m.Encoding,
			"vendorId": m.VendorID, "routeId": m.RouteID,
			"mcc": m.MCC, "mnc": m.MNC,
		}
		// What the rule changed, in the vendor's own values.
		if out.Source != in.Source {
			f["receivedFrom"] = in.Source
		}
		if out.Dest != in.Dest {
			f["receivedTo"] = in.Dest
		}
		if out.Text != in.Text {
			f["receivedText"] = in.Text
		}
		logging.CaptureTo(c, logging.New("mo"), "mo_received", "inbound message received", f)
	}
	return m.ID, true, nil
}

// match returns the first (lowest-priority) MO route whose vendor, shortcode,
// sender, keyword and MCC/MNC constraints all accept this MO.
//
// `vendorID` is the vendor that DELIVERED it and `src` the subscriber's own
// number. Neither was a condition before 2026-09-24 — the function took
// neither — so an MO rule could not be narrowed to one delivering vendor or
// to particular originators.
//
// First match wins, as for every other condition: a rule pinned to a vendor
// must sit ABOVE an any-vendor rule for the same short code, or the any-vendor
// rule takes the traffic first.
//
// One exception, between ACCOUNTS: a rule with no keyword never takes a
// message another account's keyword rule takes (a shared short code split by
// keyword, D-032: "SUB …" to one account, other text to the next). Each
// account numbers its own rules, so the priorities of two accounts say
// nothing about which of them comes first, and the doors let the two rules
// stand side by side only because the keyword rule is tried first
// (provision.MOClash, the Admin API's moOverlap; full go-live retest of
// 19 Oct 2026, P6 B7). Within one account priority alone decides, as before.
func (s *Service) match(vendorID int64, src, dest, text, mcc, mnc string) (route, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, r := range s.rts {
		if !r.takes(vendorID, src, dest, text, mcc, mnc) {
			continue
		}
		if r.keyword == nil {
			for _, k := range s.rts[i+1:] {
				if k.keyword != nil && k.accountID != r.accountID && k.takes(vendorID, src, dest, text, mcc, mnc) {
					return k, true
				}
			}
		}
		return r, true
	}
	return route{}, false
}

// takes reports whether every condition of the rule accepts this MO.
func (r route) takes(vendorID int64, src, dest, text, mcc, mnc string) bool {
	if r.vendorID != 0 && r.vendorID != vendorID {
		return false
	}
	if r.recv != nil && !r.recv.MatchString(dest) {
		return false
	}
	if r.sender != nil && !r.sender.MatchString(src) {
		return false
	}
	if r.keyword != nil && !r.keyword.MatchString(text) {
		return false
	}
	if len(r.mccmncs) > 0 && !containsMccMnc(r.mccmncs, mcc, mnc) {
		return false
	}
	return true
}

// RecordIncomplete files an incoming multi-part message whose parts did not
// all arrive within the assembly wait (2026-09-14, PO: drop and record, as
// for MT). in carries what DID arrive, joined in part order. Nothing is
// delivered. The message goes on the owning account's MO log as expired, with
// error INCOMPLETE_PARTS — sealed like any of that company's MO when it is set
// to encrypted — or, when no MO rule owns the short code, among the unrouted
// incoming messages with the reason.
func (s *Service) RecordIncomplete(ctx context.Context, hot HotWriter, in Inbound, got, total int) error {
	why := fmt.Sprintf("incomplete multi-part message: %d of %d parts arrived", got, total)
	r, ok := s.match(in.VendorID, in.Source, in.Dest, in.Text, in.MCC, in.MNC)
	if !ok || hot == nil {
		if rec := s.orphan; rec != nil {
			return rec.WriteOrphanMO(ctx, in.VendorID, in.Source, in.Dest, in.Text, orEnc(in.Encoding), in.MCC, in.MNC, why)
		}
		return nil
	}
	out := s.rewrite(r, in) // the rule's changes, as on a delivered MO
	m := model.Message{
		ID: ulid.Make().String(), Direction: model.DirectionMO, VendorID: in.VendorID,
		AccountID: r.accountID, CompanyID: r.companyID, Username: r.username, RouteID: r.id,
		Sender: out.Source, Destination: out.Dest, Body: out.Text, Encoding: orEnc(out.Encoding),
		Segments: max(got, 1), MCC: in.MCC, MNC: in.MNC, Transport: in.Transport,
		Status: model.StatusExpired, ErrorCode: "INCOMPLETE_PARTS",
	}
	if r.privacy == "all_encrypted" && m.Body != "" {
		if s.seal == nil {
			m.Body = "" // never kept plain for an encrypted company
		} else if ct, err := s.seal.Encrypt(ctx, r.accountID, r.companyID, m.Body); err == nil {
			m.Body = ct
		} else {
			m.Body = ""
		}
	}
	if s.log != nil {
		s.log.Printf("mo from %s to %s for account %d: %s — recorded expired, not delivered", in.Source, in.Dest, r.accountID, why)
	}
	return hot.Write(ctx, m)
}

// HotWriter writes a finalized record (mysql.Store satisfies it).
type HotWriter interface {
	Write(ctx context.Context, m model.Message) error
}

// Acker removes an MO staging row by canonical msg id (mysql.Store satisfies it).
type Acker interface {
	AckMO(ctx context.Context, msgIDs ...string) error
}

// Finalize files the try its client took as the MO's final row (its
// mo_sub_ row, D-140: the MO's own row is never changed) and drops the
// staging row. Called by whichever consumer handed it off — the smpp-server
// bind or the webhook poster — with the try it made.
func Finalize(ctx context.Context, hot HotWriter, q Acker, m model.Message, t dal.ClientTry) error {
	if err := RecordTry(ctx, hot, m, t); err != nil {
		return err
	}
	return q.AckMO(ctx, m.ID)
}

// FinalTry is t as the final row of m: its kind, message, company and
// account, and the message itself, so the row writes the MO's record when it
// is not there yet (an MO taken in while the store was away).
func FinalTry(m model.Message, t dal.ClientTry) dal.ClientTry {
	m.Direction = model.DirectionMO
	t.Kind, t.MsgID, t.CompanyID, t.AccountID, t.Final = dal.TryMO, m.ID, m.CompanyID, m.AccountID, true
	if t.TryID == "" {
		t.TryID = dal.NewTryID()
	}
	if t.At.IsZero() {
		t.At = time.Now()
	}
	if t.Attempt == 0 && m.Attempts > 0 {
		t.Attempt = m.Attempts
	}
	m.Status = ""
	t.Message = &m
	return t
}

// TryOf is t as a try to give m to its client that did not end it (refused,
// no answer): its kind, message, company, account and attempt.
func TryOf(m model.Message, t dal.ClientTry) dal.ClientTry {
	t.Kind, t.MsgID, t.CompanyID, t.AccountID = dal.TryMO, m.ID, m.CompanyID, m.AccountID
	if t.Attempt == 0 && m.Attempts > 0 {
		t.Attempt = m.Attempts
	}
	return t
}

// RecordTry writes t as m's final row, without touching the staging row.
func RecordTry(ctx context.Context, hot HotWriter, m model.Message, t dal.ClientTry) error {
	w, ok := hot.(dal.ClientTries)
	if !ok {
		return errors.New("this record writer cannot write an incoming message's tries")
	}
	return w.WriteClientTries(ctx, []dal.ClientTry{FinalTry(m, t)}, false)
}

// RecordOutcome files that the gateway gave up on an MO (status expired or
// failed), without touching the staging row — the give-up path needs exactly
// this, since an abandoned message keeps its staging row (marked expired)
// rather than being acked away. Its final row is no try (no channel).
func RecordOutcome(ctx context.Context, hot HotWriter, m model.Message, status model.Status) error {
	return RecordTry(ctx, hot, m, dal.ClientTry{TryID: m.ID, Result: dal.TryResultOf(status), ErrorCode: m.ErrorCode})
}

// compileMatch parses a routes regex column ({"match":"..."} JSON) into a
// compiled regexp. nil with ok=true means no condition (match any): the column
// is NULL (v nil), empty or has no pattern. ok=false means the column holds a
// pattern that cannot be used — the caller must not treat that as "match any".
func compileMatch(v *string) (*regexp.Regexp, bool) {
	if v == nil || *v == "" || *v == "null" {
		return nil, true
	}
	var spec struct {
		Match string `json:"match"`
	}
	if json.Unmarshal([]byte(*v), &spec) != nil {
		return nil, false
	}
	if spec.Match == "" {
		return nil, true
	}
	re, err := regexp.Compile(spec.Match)
	if err != nil {
		return nil, false
	}
	return re, true
}

// replaceOf reads a routes regex column's "replace" ({"match","replace"}
// JSON): nil when there is none (no key, a JSON null, no pattern to apply it
// with, or a column that does not read); "" is a real replace-with-nothing.
func replaceOf(v *string) *string {
	if v == nil || *v == "" || *v == "null" {
		return nil
	}
	var spec struct {
		Match   string  `json:"match"`
		Replace *string `json:"replace"`
	}
	if json.Unmarshal([]byte(*v), &spec) != nil || spec.Match == "" {
		return nil
	}
	return spec.Replace
}

// rewrite applies the winning rule's changes to an incoming message (4 Nov
// 2026, the same match+replace MT rules have): the short code (to), the
// subscriber (from) and the text. A changed from is cleaned like a number an
// MT rule rewrites. A change that leaves from or to empty is an operator's
// mistake: the vendor's value is kept and it is said once a rule and field.
// A changed text is planned again for its encoding; its parts stay as the
// handset sent them (that is what arrived and what is priced).
func (s *Service) rewrite(r route, in Inbound) Inbound {
	out := in
	if r.recv != nil && r.recvRepl != nil {
		if d := strings.TrimSpace(r.recv.ReplaceAllString(in.Dest, *r.recvRepl)); d != "" {
			out.Dest = d
		} else {
			s.warnEmpty(r.id, "to")
		}
	}
	if r.sender != nil && r.senderRepl != nil {
		if src := msisdn.Clean(r.sender.ReplaceAllString(in.Source, *r.senderRepl)); src != "" {
			out.Source = src
		} else {
			s.warnEmpty(r.id, "from")
		}
	}
	if r.keyword != nil && r.keywordRepl != nil {
		out.Text = r.keyword.ReplaceAllString(in.Text, *r.keywordRepl)
		if out.Text != in.Text {
			if enc, _, err := encoding.Plan(out.Text, ""); err == nil && enc != "" {
				out.Encoding = enc
			} else if s.log != nil {
				s.log.Printf("mo: route %d text change could not be planned (%v) — keeping the encoding received", r.id, err)
			}
		}
	}
	if s.log != nil && (out.Source != in.Source || out.Dest != in.Dest || out.Text != in.Text) {
		s.log.Printf("mo: route %d changed the message for account %d: from %s->%s, to %s->%s, text changed=%t",
			r.id, r.accountID, in.Source, out.Source, in.Dest, out.Dest, out.Text != in.Text)
	}
	return out
}

// warnEmpty says once a rule and field that its change left an address
// empty, so the vendor's value was kept.
func (s *Service) warnEmpty(routeID int64, field string) {
	if _, seen := s.emptied.LoadOrStore(fmt.Sprintf("%d|%s", routeID, field), true); seen || s.log == nil {
		return
	}
	s.log.Printf("mo: route %d's change of %q leaves it empty — the value received is kept (fix the rule)", routeID, field)
}

func orEnc(e string) string {
	if e == "" {
		return "gsm7"
	}
	return e
}

// containsMccMnc reports whether the route's combined-network list covers
// this MO's network. Codes compare the way MT routes compare them
// (internal/netcode, PO 2026-09-14): "410003" and "41003" are one network, and
// an MCC-only entry covers the whole country. An MO with no resolved network
// never matches a network-constrained route.
func containsMccMnc(list []string, mcc, mnc string) bool {
	if mcc == "" {
		return false
	}
	for _, v := range list {
		if netcode.Matches(v, mcc, mnc) {
			return true
		}
	}
	return false
}
