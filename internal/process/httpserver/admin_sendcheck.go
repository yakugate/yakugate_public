package httpserver

// POST /admin/send/check — the "Will this send?" check and the price check
// (Tools, PO 2026-09-12).
//
// An operator (or, through the Admin API, a customer for their own accounts)
// gives an account, a number, a sender and a text. The engine runs its real
// accept pipeline over them in dry-run mode — the same stages, in the same
// order, over the same cached settings — and reports:
//
//   - whether the message would be accepted, and if not, which stage refused
//     it and with what status;
//   - what each stage did (the text auto-replace produced, the encoding and
//     part count, the template matched, the network HLR found);
//   - which routing rule would carry it and why every other rule would not;
//   - what it would cost, priced the way billing prices it.
//
// Nothing is sent, queued, charged, counted or captured. See
// internal/pipeline/dryrun.go for what the dry run leaves out and why.
//
// mode "price" answers a narrower question — what would a message to this
// number cost this account? — with just the HLR and route stages, so it works
// before the text is written.
//
// view "customer" drops what a customer must not see: which vendor carries
// their traffic, the routing rules, buying prices, observe-mode spam flags and
// the operator-only reason detail. The Admin API asks for it on every call
// from a customer login; the engine does the dropping so the rule lives next
// to the fields it protects.

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/auth"
	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/dialect"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/pipeline"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/lib/encoding"
)

type sendCheckRequest struct {
	AccountID int64  `json:"accountId"`
	Account   string `json:"account"` // username, when no id is given
	Mode      string `json:"mode"`    // "send" (default) | "price"
	View      string `json:"view"`    // "operator" (default) | "customer"
	To        string `json:"to"`
	From      string `json:"from"`
	Text      string `json:"text"`
	Encoding  string `json:"encoding"`
	Operator  string `json:"operator"` // MCC+MNC, skips HLR as on the API
	Priority  *int   `json:"priority"`
	// Segments prices a message of this many parts when no text is given
	// (price mode only). Default 1.
	Segments int `json:"segments"`
	// ClientIP tests the account's IP allow list against this address. Empty
	// = the allow list is reported as not checked.
	ClientIP string `json:"clientIp"`
	// LiveHLR allows a paid live HLR dip on a cache miss.
	LiveHLR bool `json:"liveHlr"`
	// The client's pass-through fields, as a submit would carry them
	// (2026-10-14): a matched template fills only the ones left empty.
	AP1  string `json:"ap1"`
	AP2  string `json:"ap2"`
	APG1 string `json:"apg1"`
	APG2 string `json:"apg2"`
}

type checkAccount struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	CompanyID int64  `json:"companyId,omitempty"`
}

type checkRefusal struct {
	Stage      string `json:"stage"`
	HTTPStatus int    `json:"httpStatus"`
	Status     string `json:"status"`
	Detail     string `json:"detail,omitempty"`
	RetryAfter int    `json:"retryAfter,omitempty"`
	// OperatorDetail is what the client is never told: which spam category
	// matched, or that the blocked number is a trap. Customer view: empty.
	OperatorDetail string `json:"operatorDetail,omitempty"`
	// DialectStatus is the HTTP status the account is answered with on its
	// own address, when it has a dialect whose rules change it (QA spam
	// BUG-006: SPAM_KEYWORD is 403 natively, 451 under a dialect that says
	// so). 0 = the same as HTTPStatus. The native path keeps HTTPStatus.
	DialectStatus int `json:"dialectStatus,omitempty"`
}

type checkMessage struct {
	From       string   `json:"from"`
	To         string   `json:"to"`
	Text       string   `json:"text"`
	Encoding   string   `json:"encoding,omitempty"`
	Segments   int      `json:"segments"`
	Priority   int      `json:"priority"`
	MCC        string   `json:"mcc,omitempty"`
	MNC        string   `json:"mnc,omitempty"`
	HLRSource  string   `json:"hlrSource,omitempty"`
	Ported     bool     `json:"ported,omitempty"`
	TemplateID int64    `json:"templateId,omitempty"`
	SpamFlags  []string `json:"spamFlags,omitempty"` // operator view only
	// SenderBy / ClientSender: who chose From when it is not the client's own
	// ("template:<id>" or "account"), and what the client sent (2026-09-29).
	SenderBy     string  `json:"senderBy,omitempty"`
	ClientSender *string `json:"clientSender,omitempty"`
	// What the message would carry in ap1/ap2/apg1/apg2 — the client's own,
	// or a matched template's where the client left one empty (2026-10-14,
	// for "Will this send?"). Always present, "" = none.
	AP1  string `json:"ap1"`
	AP2  string `json:"ap2"`
	APG1 string `json:"apg1"`
	APG2 string `json:"apg2"`
}

type checkRoute struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Scope     string `json:"scope"`
	Type      string `json:"type"`
	Priority  int    `json:"priority"`
	Vendor    string `json:"vendor"`
	Transport string `json:"transport"`
}

type checkPrice struct {
	Priced   bool `json:"priced"`
	Segments int  `json:"segments"`
	// Sell is what the account is charged, in the selling card's currency,
	// tax included; SellNet + SellTax add up to it.
	Sell       *float64 `json:"sell,omitempty"`
	SellNet    *float64 `json:"sellNet,omitempty"`
	SellTax    *float64 `json:"sellTax,omitempty"`
	SellTaxPct *float64 `json:"sellTaxPct,omitempty"`
	Currency   string   `json:"currency,omitempty"`
	// Charge is Sell converted into the company's balance currency — the
	// amount billing would take from the balance.
	Charge         *float64 `json:"charge,omitempty"`
	ChargeCurrency string   `json:"chargeCurrency,omitempty"`
	// FromAccountDefault: the route has no selling card, so the account's
	// default card priced it (billing's own fallback).
	FromAccountDefault bool `json:"fromAccountDefault,omitempty"`
	// Buy is the vendor cost in the buying card's currency — BuyCurrency,
	// which need not be the selling card's (a EUR vendor under a USD price was
	// shown with no currency at all, QA billing report #8). Operator view only.
	Buy         *float64 `json:"buy,omitempty"`
	BuyCurrency string   `json:"buyCurrency,omitempty"`
	Note        string   `json:"note,omitempty"`
}

type checkBalance struct {
	Prepaid bool     `json:"prepaid"`
	Balance float64  `json:"balance"`
	After   *float64 `json:"after,omitempty"`
}

type checkNote struct {
	Stage string `json:"stage"`
	Why   string `json:"why"`
	// Applies describes what the stage would enforce, e.g. the configured
	// caps, so "not checked" still tells the reader what to look out for.
	Applies string `json:"applies,omitempty"`
}

type sendCheckResponse struct {
	Mode    string          `json:"mode"`
	View    string          `json:"view"`
	Verdict string          `json:"verdict"` // would_send | refused | priced | not_priced
	Account checkAccount    `json:"account"`
	Refusal *checkRefusal   `json:"refusal,omitempty"`
	Stages  []pipeline.Step `json:"stages,omitempty"`
	Message *checkMessage   `json:"message,omitempty"`
	// Routed is true when a routing rule would carry the message — the one
	// routing fact a customer sees.
	Routed     bool                  `json:"routed"`
	Route      *checkRoute           `json:"route,omitempty"`  // operator view only
	Routes     []pipeline.RouteCheck `json:"routes,omitempty"` // operator view only
	Price      *checkPrice           `json:"price,omitempty"`
	Balance    *checkBalance         `json:"balance,omitempty"`
	NotChecked []checkNote           `json:"notChecked,omitempty"`
	CheckedAt  string                `json:"checkedAt"`
}

// routingFields are the stage-change keys that reveal routing. Dropped from a
// customer's view of the stages, with the route itself.
var routingFields = []string{"routeId", "vendor", "transport"}

func (p *Process) handleSendCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	var req sendCheckRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if !p.ready() {
		writeJSON(w, 503, map[string]string{"error": "reference data not loaded, retry later"})
		return
	}
	acct, ok := p.checkAccount(req)
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "no such account"})
		return
	}
	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = "send"
	}
	if mode != "send" && mode != "price" {
		writeJSON(w, 400, map[string]string{"error": `mode must be "send" or "price"`})
		return
	}
	view := strings.ToLower(strings.TrimSpace(req.View))
	if view != "customer" {
		view = "operator"
	}
	// One number per check. A list would need a list of answers, and the
	// question support is asked is always about one message.
	if n := len((sendRequest{To: req.To}).recipients()); n > 1 {
		writeJSON(w, 400, map[string]string{"error": "one number at a time"})
		return
	}

	out := sendCheckResponse{
		Mode: mode, View: view,
		Account:   checkAccount{ID: acct.ID, Username: acct.Username, CompanyID: acct.CompanyID},
		CheckedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if mode == "price" {
		p.priceCheck(r.Context(), acct, req, &out)
	} else {
		p.sendCheck(r.Context(), acct, req, &out)
	}
	if view == "customer" {
		customerView(&out)
	}
	writeJSON(w, 200, out)
}

// checkAccount finds the account a check is about, by id or username.
func (p *Process) checkAccount(req sendCheckRequest) (*refdata.Account, bool) {
	if req.AccountID > 0 {
		return p.ref.AccountByID(req.AccountID)
	}
	if u := strings.TrimSpace(req.Account); u != "" {
		return p.ref.AccountByUsername(u)
	}
	return nil, false
}

// sendCheck runs the whole dry-run pipeline.
func (p *Process) sendCheck(ctx context.Context, acct *refdata.Account, req sendCheckRequest, out *sendCheckResponse) {
	// The refusals that come before the pipeline, in the order a real submit
	// meets them: the account's own state, then the request's shape.
	if rej := auth.AccountState(acct); rej != nil {
		out.Verdict, out.Refusal = "refused", refusalOf("account", rej, acct, "", p.ref)
		return
	}
	sr := sendRequest{To: req.To, From: req.From, Text: req.Text, Encoding: req.Encoding,
		Operator: req.Operator, Priority: req.Priority,
		AP1: req.AP1, AP2: req.AP2, APG1: req.APG1, APG2: req.APG2}
	if code, reason := validateRequest(sr); code != 0 {
		out.Verdict, out.Refusal = "refused", refusalOf("request", pipeline.NewReject(code, reason), acct, "", p.ref)
		return
	}
	m := checkMessageFor(sr, acct)
	m.AP1, m.AP2, m.APG1, m.APG2 = sr.AP1, sr.AP2, sr.APG1, sr.APG2
	ip := strings.TrimSpace(req.ClientIP)
	pipe := pipeline.NewDryRun(p.checkDeps, pipeline.DryRunOptions{LiveHLR: req.LiveHLR, SkipIPAllow: ip == ""})
	e := &pipeline.Envelope{Msg: m, Account: acct, ClientIP: ip}
	tr := pipe.Trace(ctx, e)

	out.Stages = tr.Steps
	out.Message = messageOf(e)
	if tr.BeforeRoute != nil {
		out.Routes = pipeline.ExplainRoutes(p.ref, p.checkDeps.VendorHealth, acct, tr.BeforeRoute)
	}
	if e.Msg.RouteID != 0 {
		out.Routed = true
		out.Route = p.routeOf(e.Msg, acct)
	}
	if tr.Reject != nil {
		out.Verdict = "refused"
		out.Refusal = refusalOf(tr.RefusedAt, tr.Reject, acct, e.Msg.Destination, p.ref)
	} else {
		out.Verdict = "would_send"
	}
	if out.Routed {
		out.Price = p.checkPriceOf(ctx, e.Msg, acct)
		out.Balance = balanceOf(acct, out.Price)
	}
	out.NotChecked = p.notCheckedNotes(tr.Steps, acct, e.Msg)
}

// priceCheck runs only HLR and route, then prices the result.
func (p *Process) priceCheck(ctx context.Context, acct *refdata.Account, req sendCheckRequest, out *sendCheckResponse) {
	sr := sendRequest{To: req.To, From: req.From, Text: req.Text, Encoding: req.Encoding, Operator: req.Operator, Priority: req.Priority}
	if code, reason := validateRequest(sr); code != 0 {
		out.Verdict, out.Refusal = "not_priced", refusalOf("request", pipeline.NewReject(code, reason), acct, "", p.ref)
		return
	}
	m := checkMessageFor(sr, acct)
	dest, ok := pipeline.CleanMSISDN(m.Destination)
	if !ok {
		// The send path's own words: a national number says to add the
		// country code here too (2026-10-01).
		out.Verdict, out.Refusal = "not_priced", refusalOf("validate", pipeline.NewReject(400, pipeline.MSISDNRefusal(dest)), acct, "", p.ref)
		return
	}
	m.Destination = dest
	// Parts: from the text when there is one (planned exactly as the
	// encoding stage plans it), else the count asked for.
	if strings.TrimSpace(m.Body) != "" {
		enc, segs, err := encoding.Plan(m.Body, m.Gsm7)
		if err != nil {
			out.Verdict, out.Refusal = "not_priced", refusalOf("encoding", pipeline.NewReject(400, "INVALID_REQUEST: "+err.Error()), acct, "", p.ref)
			return
		}
		m.Encoding, m.Segments = enc, segs
	} else {
		m.Segments = max(req.Segments, 1)
	}
	if m.Priority == 0 {
		m.Priority = 9 // what enqueue applies when nothing set one
	}
	pipe := pipeline.NewPriceCheck(p.checkDeps, pipeline.DryRunOptions{LiveHLR: req.LiveHLR})
	e := &pipeline.Envelope{Msg: m, Account: acct}
	tr := pipe.Trace(ctx, e)
	out.Stages = tr.Steps
	out.Message = messageOf(e)
	if tr.BeforeRoute != nil {
		out.Routes = pipeline.ExplainRoutes(p.ref, p.checkDeps.VendorHealth, acct, tr.BeforeRoute)
	}
	if tr.Reject != nil || e.Msg.RouteID == 0 {
		out.Verdict = "not_priced"
		if tr.Reject != nil {
			out.Refusal = refusalOf(tr.RefusedAt, tr.Reject, acct, e.Msg.Destination, p.ref)
		}
		return
	}
	out.Routed = true
	out.Route = p.routeOf(e.Msg, acct)
	out.Price = p.checkPriceOf(ctx, e.Msg, acct)
	out.Balance = balanceOf(acct, out.Price)
	out.Verdict = "priced"
	if !out.Price.Priced {
		out.Verdict = "not_priced"
	}
}

// checkMessageFor builds the one canonical message a check is about, through
// the adapter's own toMessages — so defaults (priority sentinel, encoding
// preference, operator split, number cleaning) are the API's, not a copy.
func checkMessageFor(sr sendRequest, acct *refdata.Account) *model.Message {
	if msgs := toMessages(sr, acct); len(msgs) == 1 {
		return msgs[0]
	}
	// No usable number: an empty destination, which the validate stage
	// refuses as a real submit would.
	mcc, mnc := splitOperator(sr.Operator)
	return &model.Message{AccountID: acct.ID, Username: acct.Username, Sender: strings.TrimSpace(sr.From),
		Body: sr.Text, Ingress: model.TransportHTTP, Gsm7: resolveEncodingPref(sr),
		MCC: mcc, MNC: mnc, Priority: resolvePriority(sr)}
}

func messageOf(e *pipeline.Envelope) *checkMessage {
	m := e.Msg
	// 0 means "not set yet": the enqueue stage (not run by a check) gives it
	// the default 9. Show what the message would actually carry.
	prio := m.Priority
	if prio == 0 {
		prio = 9
	}
	return &checkMessage{From: m.Sender, To: m.Destination, Text: m.Body, Encoding: m.Encoding,
		Segments: m.Segments, Priority: prio, MCC: m.MCC, MNC: m.MNC, HLRSource: m.HLRSource,
		Ported: m.HLRPorted, TemplateID: m.TemplateID, SpamFlags: e.SpamFlags,
		SenderBy: m.SenderBy, ClientSender: clientSenderOf(m),
		AP1: m.AP1, AP2: m.AP2, APG1: m.APG1, APG2: m.APG2}
}

// clientSenderOf is what the client sent, shown only when the gateway chose
// another sender (an empty string there means "sent none").
func clientSenderOf(m *model.Message) *string {
	if m.SenderBy == "" {
		return nil
	}
	v := m.ClientSender
	return &v
}

// refusalOf renders a refusal. dest is the number as the stage saw it, for
// the trap test ("" when there is no number yet).
func refusalOf(stage string, rej *pipeline.Reject, acct *refdata.Account, dest string, ref *refdata.Mem) *checkRefusal {
	out := &checkRefusal{Stage: stage, HTTPStatus: rej.Code, Status: rejectCode(rej.Reason),
		Detail: refusalDetail(rej.Reason), RetryAfter: rej.RetryAfter, OperatorDetail: rej.Audit}
	if acct != nil && acct.Dialect != nil {
		if st := dialect.RefusalStatus(rej.Code, out.Status, acct.Dialect); st != rej.Code {
			out.DialectStatus = st
		}
	}
	// A trap number refuses with the same status as any block, so a client
	// cannot find the traps by probing. The operator is told which it is.
	if stage == "blocklist" && dest != "" && ref != nil && ref.IsTrap(dest) {
		out.OperatorDetail = strings.TrimSpace(out.OperatorDetail + " trap number (blocklist source 'honeypot')")
	}
	return out
}

func (p *Process) routeOf(m *model.Message, acct *refdata.Account) *checkRoute {
	rt := &checkRoute{ID: m.RouteID, Vendor: m.Vendor, Transport: string(m.Transport)}
	own, global := p.ref.RoutesFor(acct.ID)
	for _, set := range [][]*refdata.Route{own, global} {
		for _, r := range set {
			if r.ID == m.RouteID {
				rt.Name, rt.Type, rt.Priority = r.Name, r.RouteType, r.Seq
				rt.Scope = "account"
				if r.AccountID == 0 {
					rt.Scope = "global"
				}
				return rt
			}
		}
	}
	return rt
}

// checkPriceOf prices a routed message exactly as the billing role will: the
// route's cards, the account's default selling card when the route has none,
// tax included, converted into the company's currency.
func (p *Process) checkPriceOf(ctx context.Context, m *model.Message, acct *refdata.Account) *checkPrice {
	out := &checkPrice{Segments: max(m.Segments, 1)}
	if p.rater == nil {
		out.Note = "prices cannot be read: the config store is not reachable"
		return out
	}
	rated := p.rater.RateFull(ctx, m.RouteID, m.Destination, m.MCC, m.MNC, m.Segments, billing.RateOpts{
		DefaultSellCard: p.defaultSellCard(ctx, acct.ID),
		CompanyCurrency: acct.CompanyCurrency,
		FX:              p.fx,
		VendorID:        m.VendorID,
	})
	out.Buy, out.BuyCurrency = roundPtr(rated.Buy), rated.BuyCurrency
	if rated.Sell == nil {
		out.Note = "no selling price: the route has no selling rate card (and the account no default card), or no rate in it covers this number"
		return out
	}
	out.Priced = true
	out.Sell, out.SellNet, out.SellTax = roundPtr(rated.Sell), roundPtr(rated.SellNet), roundPtr(rated.SellTax)
	out.SellTaxPct = rated.SellTaxPct
	out.Currency = rated.Currency
	out.Charge = roundPtr(rated.SellCharge)
	out.ChargeCurrency = rated.Currency
	if acct.CompanyCurrency != "" {
		out.ChargeCurrency = acct.CompanyCurrency
	}
	out.FromAccountDefault = strings.Contains(rated.Detail, `"source":"account_default"`)
	return out
}

// defaultSellCard is the account's default MT selling card — the one setting
// billing reads that the reference cache does not hold. One indexed row.
func (p *Process) defaultSellCard(ctx context.Context, accountID int64) int64 {
	if p.traffic == nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	id, _ := p.traffic.AccountLookups().DefaultSellCard(ctx, accountID)
	return id
}

func balanceOf(acct *refdata.Account, pr *checkPrice) *checkBalance {
	b := &checkBalance{Prepaid: acct.Prepaid(), Balance: roundMoney(acct.EffectiveBalance())}
	if pr != nil && pr.Charge != nil {
		after := roundMoney(b.Balance - *pr.Charge)
		b.After = &after
	}
	return b
}

// notCheckedNotes lists the stages a check skipped, with what each would
// enforce for this account — so "not checked" is still useful advice.
func (p *Process) notCheckedNotes(steps []pipeline.Step, acct *refdata.Account, m *model.Message) []checkNote {
	var out []checkNote
	for _, s := range steps {
		if s.Outcome != "not_checked" {
			continue
		}
		n := checkNote{Stage: s.Stage, Why: s.Note}
		switch s.Stage {
		case "rate-limit":
			n.Applies = p.rateCaps(acct, m)
		case "dedup":
			if !acct.DuplicateCheck {
				n.Applies = "duplicate check is off for this account"
			} else {
				ttl := p.checkDeps.DedupeDefaultTTL
				if acct.DedupeTTLSec > 0 {
					ttl = time.Duration(acct.DedupeTTLSec) * time.Second
				}
				n.Applies = "duplicate window " + ttl.String()
			}
		case "ip-allow":
			if len(acct.AllowedIPs) == 0 {
				n.Applies = "the account has no IP allow list, so any address may send"
			} else {
				n.Applies = strconv.Itoa(len(acct.AllowedIPs)) + " address(es) on the allow list"
			}
		}
		out = append(out, n)
	}
	return out
}

// rateCaps describes the segments-per-second caps that would apply.
func (p *Process) rateCaps(acct *refdata.Account, m *model.Message) string {
	var caps []string
	if p.checkDeps.GlobalTPS > 0 {
		caps = append(caps, "gateway "+strconv.Itoa(p.checkDeps.GlobalTPS)+"/s")
	}
	if acct.CompanyID != 0 && acct.CompanyThroughput > 0 {
		caps = append(caps, "company "+strconv.Itoa(acct.CompanyThroughput)+"/s shared by its accounts")
	}
	// This gateway's share (config/0123). The account-wide column is retired,
	// so the honest answer names the deployment the check is running on —
	// another gateway may well allow a different rate.
	if id := deployment.ID(); id != "" {
		if n := acct.Deployments.Rate(id, deployment.MT); n > 0 {
			caps = append(caps, "account "+strconv.Itoa(n)+"/s on "+id)
			if total := acct.Deployments.Total(deployment.MT); total > n {
				caps = append(caps, strconv.Itoa(total)+"/s across all its deployments")
			}
		}
	}
	// The abuse monitor's penalty, which applies across every deployment and
	// is the cap an operator is most likely to be surprised by.
	if acct.AbuseThrottleTPS > 0 {
		caps = append(caps, "throttled by the abuse monitor to "+strconv.Itoa(acct.AbuseThrottleTPS)+"/s")
	}
	if m.Destination != "" {
		if cc, limit, owned, ok := p.ref.CountryRateLimit(acct.ID, m.Destination); ok && limit > 0 {
			who := "shared by all traffic to +" + cc
			if owned {
				who = "this account's own rule for +" + cc
			}
			caps = append(caps, "country "+strconv.Itoa(limit)+"/s ("+who+")")
		}
	}
	if len(caps) == 0 {
		return "no caps are set"
	}
	return "segments per second: " + strings.Join(caps, "; ")
}

// customerView removes what a customer must not see. See the file comment.
func customerView(out *sendCheckResponse) {
	out.Route, out.Routes = nil, nil
	if out.Refusal != nil {
		out.Refusal.OperatorDetail = ""
	}
	if out.Message != nil {
		out.Message.SpamFlags = nil
	}
	if out.Price != nil {
		out.Price.Buy, out.Price.BuyCurrency = nil, ""
	}
	for i := range out.Stages {
		// What the route stage noticed names vendors and their prices.
		if out.Stages[i].Stage == "route" {
			out.Stages[i].Note = ""
		}
		for _, k := range routingFields {
			delete(out.Stages[i].Changed, k)
		}
		if len(out.Stages[i].Changed) == 0 {
			out.Stages[i].Changed = nil
		}
	}
}

func roundPtr(v *float64) *float64 {
	if v == nil {
		return nil
	}
	r := roundMoney(*v)
	return &r
}
