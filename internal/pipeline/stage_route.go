package pipeline

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yakugate/yakugate/internal/billing"
	"github.com/yakugate/yakugate/internal/deployment"
	"github.com/yakugate/yakugate/internal/model"
	"github.com/yakugate/yakugate/internal/monitor"
	"github.com/yakugate/yakugate/internal/msisdn"
	"github.com/yakugate/yakugate/internal/netcode"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/platform/logging"
	"github.com/yakugate/yakugate/internal/queues"
	"github.com/yakugate/yakugate/internal/refdata"
	"github.com/yakugate/yakugate/internal/sendplan"
	"github.com/yakugate/yakugate/lib/encoding"
)

// --- stage 16/18: routing (per-account plans, Main + backups) --- [SCOPE P1-15]
// Every account has its own rules (Saleem 2026-09-30: no rules for every
// account — global rows are no longer read). The first rule that catches the
// message decides, in priority order. A rule sends through its levels: Main,
// then Backup 1, Backup 2… A level picks among its vendors that can send now
// by its way of choosing — direct (the operator's order), cheap (lowest buy
// price after converting currencies), best (best recent delivery ratio to the
// message's network), smart (60% price, 40% delivery) or least (fewest
// waiting) — or, when it shares by %, splits by the shares and gives a down
// vendor's share to whoever its way of choosing puts next. A vendor is down
// when an operator paused or disabled it, or (PO 2026-09-14) when it has had
// no working connection for its down_after_sec.
//
// When no vendor of a rule can send, the account's next rule that catches the
// message is tried. When none can, the message is still ACCEPTED and waits on
// the first rule's Main vendor, following that rule's retries; it is refused
// (503 NO_ROUTE) only when no rule catches it at all (Saleem 2026-09-30).
//
// An old rule (routes.send_plan NULL) is one Main level with its own vendor,
// and several old rules at the same priority still form one shared level
// picked by their route types (lcr > quality > capacity > loadbalance >
// direct), so nothing already configured changes.
// Blackhole simulation is a VENDOR property (vendors.transport='blackhole',
// D-016), not a route_type — routes reach a blackhole vendor the same way
// they reach any other.
type routeStage struct {
	ref *refdata.Mem
	rr  *rrState
	// fx backs loss detection (S-03, PO 2026-09-03) — nil is fine, every
	// call site already falls back to the row-pinned conversion_rate.
	fx *billing.FXResolver
	// qualityMinSample is how many judged messages a vendor needs in the
	// window before route_type='quality' will hold its ratio against it. 0
	// means every vendor's ratio counts immediately, however thin.
	qualityMinSample int64
	// dryRun is the "Will this send?" check (NewDryRun): choose exactly as a
	// real submit would, but count nothing in monitoring and write nothing to
	// a running flow capture — a check is not traffic.
	dryRun bool
	// health says whether a vendor has had no working connection for its
	// "down after" time (PO 2026-09-14). nil = only the operator's
	// paused/disabled switch counts, as before.
	health VendorHealth
	// leaving is the vendor a waiting message is being moved away from
	// (Rerouter): never chosen again, whatever its health says this second.
	leaving string
	// avoid are the vendors already tried in this round (Message.TriedVendors).
	avoid map[string]bool
	// noWait refuses (NO_ROUTE) instead of accepting a message no vendor can
	// take now. The Rerouter sets it: a message already waiting is moved only
	// to a vendor that can send.
	noWait bool
	// lossAcceptNoRate is what the loss check does when the two prices cannot
	// be put into one currency — no base conversion rate, and no provider
	// with a rate. The default REFUSES the message LOSS_PREVENTED (Saleem
	// 2026-10-07): an account that asked never to sell below cost is not sent
	// a message nobody could check. YAKU_LOSS_NO_RATE=accept lets it through
	// and says so.
	lossAcceptNoRate bool
	// baseCur is the system base currency (YAKU_BASE_CURRENCY, USD unless
	// set): the one currency every price is compared in.
	baseCur string
}

// lossAcceptsNoRate reads YAKU_LOSS_NO_RATE once, when a pipeline is built.
func lossAcceptsNoRate() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("YAKU_LOSS_NO_RATE")), "accept")
}

// BaseCurrency is the system base currency (config.BaseCurrency:
// YAKU_BASE_CURRENCY, USD unless set).
func BaseCurrency() string { return config.BaseCurrency() }

// base is the stage's base currency (USD for a stage built without one).
func (s routeStage) base() string {
	if s.baseCur == "" {
		return "USD"
	}
	return s.baseCur
}

// VendorHealth answers whether a vendor is really down — no working
// connection for its down_after_sec (vendorhealth.View). The reason is for
// operators.
type VendorHealth interface {
	Down(vendorID int64) (bool, string)
}

// routeMismatch says why one route does not take this message, or "" when it
// does. The route stage and the dry-run explainer (ExplainRoutes) both ask
// this one function, so the explanation an operator reads can never drift
// from the decision the engine makes.
func routeMismatch(r *refdata.Route, e *Envelope) string {
	if r.Disabled {
		return "rule is disabled"
	}
	if r.Direction != "" && r.Direction != "MT" {
		return "rule is for incoming (MO) messages" // MO belongs to the inbound receiver
	}
	if len(r.MatchMCCMNCs) > 0 {
		if e.Msg.MCC == "" && e.Msg.MNC == "" {
			// Undetected network: this route only matches if it opted in,
			// regardless of which networks it lists — an empty destination
			// network can never equal a specific mccmnc entry. [2026-07-28]
			if !r.AcceptUndetectedNetwork {
				return "network of the number is unknown and the rule does not accept unknown networks"
			}
		} else if !containsMCCMNC(r.MatchMCCMNCs, e.Msg.MCC, e.Msg.MNC) {
			return "network " + e.Msg.MCC + e.Msg.MNC + " is not in the rule's networks"
		}
	}
	// A network-only rule (Saleem 2026-09-30) carries only the networks it
	// has vendors for: any other number is not its to take, and the account's
	// next rule gets it.
	if r.NetworksOnly() {
		if lv, _ := r.LevelsFor(e.Msg.MCC, e.Msg.MNC); len(lv) == 0 {
			return "the rule has vendors only for networks " + strings.Join(r.ExceptionCodes(), ", ") + ", and this number's network is not one of them"
		}
	}
	if r.SenderRegex != nil && r.SenderRegex.Match != nil && !r.SenderRegex.Match.MatchString(e.Msg.Sender) {
		return "sender does not match the rule's sender pattern"
	}
	if r.ReceiverRegex != nil && r.ReceiverRegex.Match != nil && !r.ReceiverRegex.Match.MatchString(e.Msg.Destination) {
		return "number does not match the rule's receiver pattern"
	}
	if r.KeywordRegex != nil && r.KeywordRegex.Match != nil && !r.KeywordRegex.Match.MatchString(e.Msg.Body) {
		return "text does not match the rule's keyword pattern"
	}
	if r.PriorityRegex != nil && r.PriorityRegex.Match != nil {
		// An HTTP client that sent no priority carries the 0 sentinel until
		// the enqueue stage makes it 9 (stage_enqueue.go); the rule is
		// judged on the priority the message will really have.
		p := e.Msg.Priority
		if p == 0 {
			p = 9
		}
		if !r.PriorityRegex.Match.MatchString(strconv.Itoa(p)) {
			return "priority " + strconv.Itoa(p) + " does not match the rule's priority pattern"
		}
	}
	return ""
}

func (routeStage) Name() string { return "route" }
func (s routeStage) Run(ctx context.Context, e *Envelope) *Reject {
	// Pre-indexed at cache refresh (chunk-7 ruling 7.2). Only the account's
	// OWN rules: rules for every account are retired (Saleem 2026-09-30).
	own, _ := s.ref.RoutesFor(e.Account.ID)
	var cand []*refdata.Route
	for _, r := range own {
		if routeMismatch(r, e) == "" {
			cand = append(cand, r)
		}
	}
	if len(cand) == 0 {
		// 503, not 400: the client's request is valid; there is simply no
		// rule of this account for it — a server-side condition, not a
		// client error (P1 scope finalize 2026-08-09).
		if !s.dryRun {
			monitor.Routes.NoRoute(false)
		}
		return reject(503, "NO_ROUTE")
	}
	s = s.withTried(e.Msg)
	groups := ruleGroups(sortedRoutes(cand))
	for gi, g := range groups {
		for li, lv := range g.levels(e.Msg) {
			if w := s.pickLevel(ctx, lv, e); w != nil {
				return s.take(ctx, e, w, li, gi > 0 || li > 0)
			}
		}
	}
	// Every vendor of every rule that catches the message is down (or was
	// tried this round). The Rerouter moves only to a vendor that can send.
	if s.noWait {
		if !s.dryRun {
			monitor.Routes.NoRoute(true)
		}
		return reject(503, "NO_ROUTE")
	}
	// A rule caught it, so it is accepted and waits on the first rule's Main
	// vendor (Saleem 2026-09-30: "refuse if there is no rule at all; if there
	// is any but out, accept and retry"). The mover sends it on the moment a
	// vendor of its rule can take it; its validity bounds the wait.
	//
	// With separate queues (D-121) it waits on the first such vendor that is
	// placed on THIS deployment: only this deployment will ever send it, so a
	// vendor that does not run here would hold it for ever. None placed here
	// is a configuration this gateway cannot serve, and says so.
	for _, g := range groups {
		for li, lv := range g.levels(e.Msg) {
			for _, w := range lv.Vendors {
				if placedHere(w) {
					return s.take(ctx, e, w, li, true)
				}
			}
		}
	}
	if !s.dryRun {
		monitor.Routes.NoRoute(true)
	}
	return reject(503, "NO_ROUTE: no vendor of this account's rules runs on this gateway")
}

// take puts the message on the chosen vendor entry and checks the loss.
func (s routeStage) take(ctx context.Context, e *Envelope, w *refdata.Route, level int, fell bool) *Reject {
	if rej := applyRoute(e, w, s.dryRun); rej != nil {
		return rej
	}
	e.Msg.RouteLevel = level
	if !s.dryRun {
		monitor.Routes.Picked(w.ID, w.Name, w.VendorCID, fell)
	}
	return s.checkLoss(ctx, e, w)
}

// withTried returns the stage with this round's tried vendors to avoid.
func (s routeStage) withTried(m *model.Message) routeStage {
	if len(m.TriedVendors) == 0 {
		return s
	}
	s.avoid = make(map[string]bool, len(m.TriedVendors))
	for _, v := range m.TriedVendors {
		s.avoid[v] = true
	}
	return s
}

// ruleGroup is one rule, or several OLD rules at the same priority, which
// have always formed one shared tier.
type ruleGroup []*refdata.Route

// ruleGroups groups sorted rules: old rules sharing a priority stay together;
// a new rule is always its own group (the doors keep new rules' priorities
// unique within an account).
func ruleGroups(sorted []*refdata.Route) []ruleGroup {
	var out []ruleGroup
	for _, r := range sorted {
		if n := len(out); n > 0 && isLegacy(r) {
			last := out[n-1]
			if isLegacy(last[0]) && last[0].Seq == r.Seq {
				out[n-1] = append(last, r)
				continue
			}
		}
		out = append(out, ruleGroup{r})
	}
	return out
}

func isLegacy(r *refdata.Route) bool {
	return r.Legacy || (len(r.Levels) == 0 && len(r.Exceptions) == 0)
}

// levels is what the group sends through for this message. One rule: its own
// levels (or the exception for the message's network). Several old rules: one
// level holding each rule's vendor, picked by the tier's route type.
func (g ruleGroup) levels(m *model.Message) []refdata.Level {
	if len(g) == 1 {
		return ruleLevels(g[0], m)
	}
	tier := []*refdata.Route(g)
	lv := refdata.Level{Method: sendplan.MethodFromRouteType(tierType(tier)), Share: tierType(tier) == "loadbalance", Legacy: true}
	for _, r := range g {
		lv.Vendors = append(lv.Vendors, ruleLevels(r, m)[0].Vendors...)
	}
	return []refdata.Level{lv}
}

// ruleLevels is a rule's levels for this message. A rule built without levels
// (older snapshots, tests) is its own one-vendor Main level.
func ruleLevels(r *refdata.Route, m *model.Message) []refdata.Level {
	if len(r.Levels) > 0 || len(r.Exceptions) > 0 {
		lv, _ := r.LevelsFor(m.MCC, m.MNC)
		return lv
	}
	return []refdata.Level{{Method: sendplan.MethodFromRouteType(r.RouteType), Share: r.RouteType == "loadbalance",
		Vendors: []*refdata.Route{r}, Legacy: true}}
}

// pickLevel chooses a vendor entry of one level, or nil when none of its
// vendors can send now.
func (s routeStage) pickLevel(ctx context.Context, lv refdata.Level, e *Envelope) *refdata.Route {
	var up []*refdata.Route
	for _, v := range lv.Vendors {
		if s.vendorUp(v) {
			up = append(up, v)
		}
	}
	if len(up) == 0 {
		return nil
	}
	if len(up) == 1 {
		return up[0]
	}
	if lv.Share {
		return s.rr.pickWeighted(up, s.shareWeights(ctx, lv, up, e))
	}
	return s.byMethod(ctx, lv.Method, up, e)
}

// byMethod is the level's way of choosing among vendors that can send.
func (s routeStage) byMethod(ctx context.Context, method string, up []*refdata.Route, e *Envelope) *refdata.Route {
	switch method {
	case sendplan.Cheapest:
		return s.cheapest(ctx, up, e)
	case sendplan.Best:
		return s.bestQuality(up, e)
	case sendplan.Least:
		return s.leastLoaded(up)
	case sendplan.Value:
		return s.bestValue(ctx, up, e)
	default: // direct: the operator's order
		return up[0]
	}
}

// shareWeights are the shares of the vendors that can send, after each
// vendor that cannot has handed its share on: to the next vendor in the
// operator's order on a Direct level, else to the vendor the level's way of
// choosing puts first (Saleem 2026-09-30). An old shared tier splits the way
// it always did: among the vendors that can send, by their own weights.
func (s routeStage) shareWeights(ctx context.Context, lv refdata.Level, up []*refdata.Route, e *Envelope) []int {
	w := make(map[*refdata.Route]int, len(up))
	for _, v := range up {
		w[v] = weightOf(v)
	}
	isUp := func(v *refdata.Route) bool { _, ok := w[v]; return ok }
	if !lv.Legacy {
		var taker *refdata.Route
		if lv.Method != sendplan.Direct {
			taker = s.byMethod(ctx, lv.Method, up, e)
		}
		for i, v := range lv.Vendors {
			if isUp(v) {
				continue
			}
			t := taker
			if t == nil { // Direct: the next one down the list, else the nearest above
				for _, n := range lv.Vendors[i+1:] {
					if isUp(n) {
						t = n
						break
					}
				}
				for k := i - 1; t == nil && k >= 0; k-- {
					if isUp(lv.Vendors[k]) {
						t = lv.Vendors[k]
					}
				}
			}
			w[t] += v.Pct
		}
	}
	out := make([]int, len(up))
	for i, v := range up {
		out[i] = w[v]
	}
	return out
}

// checkLoss refuses the ALREADY-CHOSEN route when it would sell below its
// buy cost (S-03, PO 2026-09-03). Deliberately does not affect route
// SELECTION itself — a mispriced candidate elsewhere in the same tier is not
// tried instead; the PO ruled this checks only the winner, not every
// candidate.
//
// The two prices are compared in the SYSTEM BASE currency (Saleem
// 2026-10-07, as the reference gateway does): the customer's company and the
// vendor's company may keep different currencies, so each price goes card
// currency -> its owner company's currency (the row's own rate, or its
// provider — what is really charged, and really paid) -> base (that
// company's Base Conversion Rate, or its base currency provider). See inBase.
//
// Both prices are the ones the billing role will charge (QA EP-017/RR-001,
// 2026-10-01): the same row choice as billing.Rater (the most specific row,
// the "Any destination" row included — it used to be ignored, so a card
// priced only by it never triggered the check), the same cards (the vendor's
// own cards for this rule, else the rule's, else the vendor's default buy
// card and the account's default selling card — a card that is disabled or
// holds no rate is stepped past, as billing steps past it: buyCard/sellCard)
// and gross totals (net + tax).
//
// Three ways this is a no-op, all by explicit ruling:
//   - the account never opted in (LossDetection false, the default; per
//     account since PO 2026-09-17, was per company)
//   - no selling card (rule, vendor-for-rule or account default), or no sell
//     row matches this destination — "unpriced," not "a loss"
//   - the route has no buy price either — nothing to compare against
func (s routeStage) checkLoss(ctx context.Context, e *Envelope, winner *refdata.Route) *Reject {
	if !e.Account.LossDetection {
		return nil
	}
	sell, ok := s.ref.CardRateFor(s.sellCard(winner, e.Account), e.Msg.MCC, e.Msg.MNC, e.Msg.Destination)
	if !ok {
		return nil
	}
	buy, ok := s.ref.CardRateFor(s.buyCard(winner), e.Msg.MCC, e.Msg.MNC, e.Msg.Destination)
	if !ok {
		return nil
	}
	segs := e.Msg.Segments
	if segs < 1 {
		segs = 1
	}
	sellTotal, buyTotal, in, known := s.comparable(ctx, sell, buy, segs, e.Account)
	if !known {
		why := "the loss check could not compare: the customer's price is in " + orUnknown(sell.Currency) +
			" and the vendor's in " + orUnknown(buy.Currency) + ", and neither a company's Base Conversion Rate nor a currency provider gives a rate to " + in
		if !s.lossAcceptNoRate {
			r := reject(403, "LOSS_PREVENTED")
			r.Audit = why
			return r
		}
		// Accepted only because the operator said so (YAKU_LOSS_NO_RATE=accept)
		// — and said: a check that cannot be made must never look like one
		// that passed.
		e.Notes = append(e.Notes, why+" — accepted unchecked")
		if !s.dryRun {
			sayRarely("loss|"+strconv.FormatInt(e.Account.ID, 10)+"|"+buy.Currency+">"+in,
				"[pipeline] account %s (rule %d): %s — accepted unchecked (YAKU_LOSS_NO_RATE=accept)",
				e.Account.Username, winner.ID, why)
		}
		return nil
	}
	if sellTotal < buyTotal {
		return reject(403, "LOSS_PREVENTED")
	}
	return nil
}

func orUnknown(cur string) string {
	if cur == "" {
		return "an unknown currency"
	}
	return cur
}

// comparable puts the customer's price and the vendor's into ONE currency
// and says which. First the system base currency (inBase), the ruled way.
// Where a base rate is missing it falls back to what can still be said
// honestly: the cards' own figures when both are in one currency, else both
// in the customer's currency (the selling row's own rate; the vendor's by a
// rate for ITS pair, buyIn). known=false: nothing gives a rate — `in` is then
// the currency that was aimed for.
func (s routeStage) comparable(ctx context.Context, sell, buy refdata.RateRow, parts int, acct *refdata.Account) (sellTotal, buyTotal float64, in string, known bool) {
	sb, okS := s.inBase(ctx, sell.Gross(parts), sell, acct)
	bb, okB := s.inBase(ctx, buy.Gross(parts), buy, nil)
	if okS && okB {
		return sb, bb, s.base(), true
	}
	if sameCurrency(buy.Currency, sell.Currency) {
		return sell.Gross(parts), buy.Gross(parts), sell.Currency, true
	}
	in = acct.CompanyCurrency
	if in == "" {
		in = sell.Currency // no company currency: the customer pays in the card's
	}
	sellTotal = s.toCompanyCurrency(ctx, sell.Gross(parts), sell.Currency, sell.ConvRate, sell.CRPRef, acct.CompanyCurrency)
	if buyTotal, known = s.buyIn(ctx, buy.Gross(parts), buy, in); known {
		return sellTotal, buyTotal, in, true
	}
	return 0, 0, s.base(), false
}

// inBase turns a rate row's amount into the SYSTEM BASE currency, in the two
// steps of the reference model:
//
//	card currency  ->  the owning company's currency   the row's own rate
//	                                                   (pinned, or its provider)
//	company's currency  ->  base currency              the company's Base
//	                                                   Conversion Rate, or its
//	                                                   base currency provider
//
// The first step is what is really charged to the customer (a selling row)
// or really paid to the vendor (a buying row); the second is the same for
// every amount of that company. owner stands in for a card that names no
// company — the customer, for a selling card. A card nobody owns is taken
// straight from its own currency by whatever rate the gateway has (Pair).
// ok=false: a step has no rate.
func (s routeStage) inBase(ctx context.Context, amount float64, row refdata.RateRow, owner *refdata.Account) (float64, bool) {
	ownerCur, rate, crp := row.ConvTo, row.OwnerBaseRate, row.OwnerBaseCRP
	if ownerCur == "" && owner != nil {
		ownerCur, rate, crp = owner.CompanyCurrency, owner.CompanyBaseRate, owner.CompanyBaseCRP
	}
	if ownerCur == "" {
		if sameCurrency(row.Currency, s.base()) {
			return amount, true
		}
		if r, ok := s.fx.Pair(ctx, row.Currency, s.base()); ok {
			return amount * r, true
		}
		return 0, false
	}
	factor, ok := s.toBase(ctx, ownerCur, rate, crp)
	if !ok {
		return 0, false
	}
	return s.toCompanyCurrency(ctx, amount, row.Currency, row.ConvRate, row.CRPRef, ownerCur) * factor, true
}

// toBase is what one unit of a company's currency is worth in the base
// currency. The rule itself is billing.ToBase, which the abuse monitor reads
// too (its spend floor is an amount in the base currency).
func (s routeStage) toBase(ctx context.Context, cur string, rate float64, crp string) (float64, bool) {
	return billing.ToBase(ctx, s.fx, s.base(), cur, rate, crp)
}

func sameCurrency(a, b string) bool {
	return a == "" || b == "" || strings.EqualFold(a, b)
}

// buyIn converts a vendor's price, held in its buying row's currency, into
// `in` — for a comparison, never a charge. It is the fallback of comparable
// and buyPrices, used where the base currency cannot be reached (inBase). The row's own rate is used only
// when it really converts into that currency: a buying card's pinned rate is
// for its OWNER's currency (the vendor's company), so a EUR card of a EUR
// vendor carries 1, and read against a USD customer its 0.0115 EUR counted
// as 0.0115 USD (QA billing report #2, 2026-10-07). Otherwise the gateway's
// rate for the pair, whichever provider has it (billing.FXResolver.Pair).
// ok=false: nothing gives a rate.
func (s routeStage) buyIn(ctx context.Context, amount float64, row refdata.RateRow, in string) (float64, bool) {
	if sameCurrency(row.Currency, in) {
		return amount, true
	}
	if row.ConvTo != "" && strings.EqualFold(row.ConvTo, in) {
		return s.toCompanyCurrency(ctx, amount, row.Currency, row.ConvRate, row.CRPRef, in), true
	}
	if rate, ok := s.fx.Pair(ctx, row.Currency, in); ok {
		return amount * rate, true
	}
	// A card with no company (older than the rule that every card has one):
	// what its rate converts into is not recorded. A rate somebody really set
	// — not the 0 of a row with none (config/0171) — is read as it always
	// was, only here, where nothing better is known.
	if row.ConvTo == "" && (row.CRPRef != "" || row.ConvRate > 0) {
		return s.toCompanyCurrency(ctx, amount, row.Currency, row.ConvRate, row.CRPRef, in), true
	}
	return 0, false
}

// sayRarely writes a log line at most once a minute per key: a condition that
// holds for every message of an account must not write a line per message.
func sayRarely(key, format string, args ...any) {
	now := time.Now().Unix()
	if last, ok := saidAt.Load(key); ok && now-last.(int64) < 60 {
		return
	}
	saidAt.Store(key, now)
	log.Printf(format, args...)
}

var saidAt sync.Map // key -> unix seconds of the last line

// buyCard is the card billing.Rater will price this vendor entry's buy side
// by: the first card of its ladder (the vendor's own card for this rule, the
// rule's buying card, the vendor's default) that is enabled and holds a rate.
// A disabled or empty card is stepped past, as billing steps past it (QA
// BILL-004) — reading it as "unpriced" hid a loss and sent LCR's traffic to
// the wrong vendor. An entry built without a ladder keeps its one card.
func (s routeStage) buyCard(r *refdata.Route) int64 {
	if len(r.BuyCards) == 0 {
		return r.BuyRateCardID
	}
	return s.ref.PricedCard(r.BuyCards...)
}

// sellCard is the card billing.Rater will charge the client by: the first
// priced card of the entry's sell ladder (the vendor's own card for this
// rule, then the rule's), else the account's default selling card (D-082.4).
func (s routeStage) sellCard(r *refdata.Route, a *refdata.Account) int64 {
	ladder := r.SellCards
	if len(ladder) == 0 {
		ladder = []int64{r.SellRateCardID}
	}
	if c := s.ref.PricedCard(ladder...); c != 0 {
		return c
	}
	if a == nil {
		return 0
	}
	return a.DefaultMTSellCard
}

// toCompanyCurrency converts amount, priced in cardCur, into companyCur by a
// rate row's OWN rate — the SAME resolution order billing.Rater uses for the
// real charge: same currency needs no conversion; otherwise the row's own
// pinned rate is the baseline, and the FXResolver's rate (when the row names
// a provider) wins over it when there is one. companyCur == "" (a company row
// somehow missing) skips conversion rather than dividing by an unknown
// quantity. Right for a SELLING row, whose rate is for the customer's
// currency; a buying row goes through buyIn.
func (s routeStage) toCompanyCurrency(ctx context.Context, amount float64, cardCur string, pinnedRate float64, crpRef, companyCur string) float64 {
	if companyCur == "" || cardCur == "" || strings.EqualFold(cardCur, companyCur) {
		return amount
	}
	// No pinned rate (0 since config/0171; the column's default 1 before it)
	// converts at 1, as it always did.
	rate := pinnedRate
	if rate <= 0 {
		rate = 1
	}
	if crpRef != "" && s.fx != nil {
		if live, ok := s.fx.Resolve(ctx, crpRef, cardCur, companyCur); ok {
			rate = live.Rate
		}
	}
	return amount * rate
}

// tierType returns the tier's selection strategy — lcr wins over quality over
// capacity over loadbalance (2026-07-28 routes review adds quality/capacity
// to the existing lcr/loadbalance precedence; unchanged otherwise).
func tierType(tier []*refdata.Route) string {
	lb := false
	for _, r := range tier {
		if r.RouteType == "lcr" {
			return "lcr"
		}
	}
	for _, r := range tier {
		if r.RouteType == "quality" {
			return "quality"
		}
	}
	for _, r := range tier {
		switch r.RouteType {
		case "capacity":
			return "capacity"
		case "loadbalance":
			lb = true
		}
	}
	if lb {
		return "loadbalance"
	}
	return "direct"
}

// cheapest picks the LCR route with the lowest buying rate for this
// destination. Prices in different currencies are converted first (PO
// 2026-09-14): compared as plain numbers, a USD card at 0.002 always beat a
// PKR card at 0.55, cheaper or not.
func (s routeStage) cheapest(ctx context.Context, tier []*refdata.Route, e *Envelope) *refdata.Route {
	prices, priced := s.buyPrices(ctx, tier, e)
	best := 0
	for i := 1; i < len(tier); i++ {
		if priced[i] && (!priced[best] || prices[i] < prices[best]) {
			best = i
		}
	}
	return tier[best]
}

// buyPrices are one part's buy price for this destination through each
// vendor entry, in ONE currency, so they can be compared. An entry's buy card
// is its price for this rule, else the rule's, else the vendor's default
// (refdata). The row is the one billing charges — the "Any destination" row
// included (QA SMPP-003: cards priced only by it all looked unpriced, so LCR
// always gave the traffic to the first rule) — and the price is gross, tax
// included.
//
// When every priced entry is in the same currency the prices compare as they
// stand. Otherwise each is converted into the customer's currency by a rate
// for ITS pair (buyIn) — until 2026-10-07 by the buying row's own pinned
// rate, which is for the vendor's company and made a EUR price read as the
// same number of dollars. An entry nothing can convert counts as unpriced,
// and loses to any that is priced.
func (s routeStage) buyPrices(ctx context.Context, tier []*refdata.Route, e *Envelope) (prices []float64, priced []bool) {
	prices, priced = make([]float64, len(tier)), make([]bool, len(tier))
	rows := make([]refdata.RateRow, len(tier))
	first, mixed := "", false
	// In the system base currency when every priced entry can be taken there
	// (inBase) — the ruled way to compare vendors whose companies keep
	// different currencies.
	inBase, allBase := make([]float64, len(tier)), true
	for i, r := range tier {
		rows[i], priced[i] = s.ref.CardRateFor(s.buyCard(r), e.Msg.MCC, e.Msg.MNC, e.Msg.Destination)
		if !priced[i] {
			continue
		}
		if v, ok := s.inBase(ctx, rows[i].Gross(1), rows[i], nil); ok {
			inBase[i] = v
		} else {
			allBase = false
		}
		if rows[i].Currency == "" {
			continue
		}
		switch {
		case first == "":
			first = rows[i].Currency
		case !strings.EqualFold(first, rows[i].Currency):
			mixed = true
		}
	}
	if allBase {
		return inBase, priced
	}
	in := ""
	if mixed {
		in = first // no company currency: any one currency serves a comparison
		if e.Account != nil && e.Account.CompanyCurrency != "" {
			in = e.Account.CompanyCurrency
		}
	}
	for i := range tier {
		if !priced[i] {
			continue
		}
		prices[i] = rows[i].Gross(1)
		if !mixed {
			continue
		}
		if prices[i], priced[i] = s.buyIn(ctx, prices[i], rows[i], in); !priced[i] {
			why := "vendor " + tier[i].VendorCID + "'s price is in " + rows[i].Currency + " and no currency provider has a rate to " + in +
				": it is left out of the price comparison"
			e.Notes = append(e.Notes, why)
			if !s.dryRun {
				sayRarely("lcr|"+tier[i].VendorCID+"|"+rows[i].Currency+">"+in, "[pipeline] %s", why)
			}
		}
	}
	return prices, priced
}

// leastLoaded picks the route whose vendor currently has the lowest live
// queue depth (route_type='capacity', 2026-07-28 routes review) — reads the
// depth the reference cache already refreshed periodically (refdata.Mem.
// QueueDepthFor), never a per-message query. A route whose vendor has no
// depth data at all (backend doesn't support it, or the vendor queue table
// doesn't exist yet) is treated as "unknown," not "empty" — it loses to any
// candidate with real (even nonzero) data, and only wins over other
// depth-less candidates by tier order. [SCOPE P1-15]
func (s routeStage) leastLoaded(tier []*refdata.Route) *refdata.Route {
	best := tier[0]
	bd, bok := s.ref.QueueDepthFor(best.VendorCID)
	for _, r := range tier[1:] {
		d, ok := s.ref.QueueDepthFor(r.VendorCID)
		if ok && (!bok || d.Depth < bd.Depth) {
			best, bd, bok = r, d, true
		}
	}
	return best
}

// bestQuality picks the route whose vendor has the best recent delivery ratio
// to THIS message's network (route_type='quality', 2026-07-28 routes review;
// built 2026-09-04; per network since 2026-09-14). Reads the counts the
// reference cache already refreshed periodically (refdata.Mem.QualityFor) —
// never a per-message query, same shape as leastLoaded.
//
// A vendor with fewer than minSample judged messages in the window is NOT
// treated as unknown. It is treated as good, and only loses once it has
// enough traffic to prove otherwise (PO ruling 2026-09-04). The alternative —
// "no data, so it loses" — starves a brand new vendor permanently: it gets no
// traffic, so it earns no score, so it is never chosen.
//
// Ties keep tier order, so a tier where every vendor is delivering behaves
// exactly as it does today. [SCOPE P1-15]
func (s routeStage) bestQuality(tier []*refdata.Route, e *Envelope) *refdata.Route {
	best := tier[0]
	bestRatio, _ := s.ref.QualityFor(best.VendorID, e.Msg.MCC, e.Msg.MNC, s.qualityMinSample)
	for _, r := range tier[1:] {
		ratio, _ := s.ref.QualityFor(r.VendorID, e.Msg.MCC, e.Msg.MNC, s.qualityMinSample)
		if ratio > bestRatio {
			best, bestRatio = r, ratio
		}
	}
	return best
}

// bestValue scores each vendor 60% on price and 40% on delivery (the
// "Best value" way, Saleem 2026-09-30): price score = cheapest price / its
// price, delivery score = its ratio / the best ratio. A vendor with no price
// for the destination scores 0 on price. Ties keep the level's order.
func (s routeStage) bestValue(ctx context.Context, up []*refdata.Route, e *Envelope) *refdata.Route {
	found, priced := s.buyPrices(ctx, up, e)
	prices := make([]float64, len(up))
	ratios := make([]float64, len(up))
	minP, maxR := 0.0, 0.0
	for i, r := range up {
		if p := found[i]; priced[i] && p > 0 {
			prices[i] = p
			if minP == 0 || p < minP {
				minP = p
			}
		}
		ratios[i], _ = s.ref.QualityFor(r.VendorID, e.Msg.MCC, e.Msg.MNC, s.qualityMinSample)
		if ratios[i] > maxR {
			maxR = ratios[i]
		}
	}
	best, bestScore := 0, -1.0
	for i := range up {
		score := 0.0
		if prices[i] > 0 {
			score += 0.6 * minP / prices[i]
		}
		if maxR > 0 {
			score += 0.4 * ratios[i] / maxR
		}
		if score > bestScore {
			best, bestScore = i, score
		}
	}
	return up[best]
}

// containsMCCMNC reports whether a route's configured networks cover the
// message's network. Codes compare numerically on the MNC ("410003" covers
// "410"+"03"), and an MCC-only code ("410") covers the whole country.
func containsMCCMNC(list []string, mcc, mnc string) bool {
	for _, v := range list {
		if netcode.Matches(v, mcc, mnc) {
			return true
		}
	}
	return false
}

// vendorUp reports whether a route's vendor can take traffic now: switched on
// by its operator, not down by its own health, and not the vendor a waiting
// message is being moved away from.
func (s routeStage) vendorUp(r *refdata.Route) bool {
	if !vendorSwitchedOn(r) {
		return false
	}
	if !placedHere(r) {
		return false
	}
	if s.leaving != "" && r.VendorCID == s.leaving {
		return false
	}
	if s.avoid[r.VendorCID] {
		return false
	}
	if s.health != nil {
		if down, _ := s.health.Down(r.VendorID); down {
			return false
		}
	}
	return true
}

// placedHere reports whether a route's vendor runs on this deployment. With
// separate queues (D-121) a deployment sends only its own messages, so a
// message put on a vendor placed elsewhere would wait for ever: such a vendor
// is passed over like a down one. A vendor placed nowhere runs everywhere;
// so does every vendor when this gateway names no deployment (shared queues,
// as before).
func placedHere(r *refdata.Route) bool {
	own := deployment.Owner()
	return own == "" || r.VendorDeployments.Allows(own)
}

// vendorSwitchedOn is the operator's switch alone: active, not paused or
// disabled.
func vendorSwitchedOn(r *refdata.Route) bool {
	return r.VendorStatus == "" || r.VendorStatus == "active"
}

// applyRoute stamps the winning route onto the message and applies its rewrites.
//
// Rewrites run AFTER the stages that certified the message — deliberately so:
// route config is OPERATOR-trusted (never client input), so policy gates
// (blocklist/country/sender-id) are not re-run. What IS refreshed (chunk-7
// ruling 7.1, 2026-08-04): a rewritten destination is re-normalized, and a
// rewritten body gets its encoding/segments RE-PLANNED so rate-limit spend,
// records, billing, and the egress part split all describe the text actually
// sent. Whole messages only — an SMPP concat part (ConcatTotal>1) is a
// fragment whose body/UDH must stay untouched; the PO's assemble-first model
// (collect all parts, then run message-level rules once — D-096) is the
// designed fix for those.
func applyRoute(e *Envelope, winner *refdata.Route, dryRun bool) *Reject {
	// [SCOPE P13-CAPTURE] Which rule won, in the operator's own words. The
	// generic per-stage record only shows that routeId changed; this says
	// which rule, what kind, at what priority, and to which vendor — the
	// actual question behind "why did this message go there". Not for a dry
	// run: a capture records traffic, and a check is not traffic.
	if e.Account != nil && !dryRun {
		sc := logging.AccountScope(e.Account.Username)
		if logging.Capturing(sc) {
			name := winner.Name
			if name == "" {
				name = "(unnamed)"
			}
			logging.CaptureRecord(logging.New("pipeline"), sc, "route_chosen",
				"matched rule "+name, logging.Fields{
					"messageId":    e.Msg.ID,
					"account":      e.Account.Username,
					"to":           e.Msg.Destination,
					"from":         e.Msg.Sender,
					"routeId":      winner.ID,
					"routeName":    name,
					"routeType":    winner.RouteType,
					"priority":     winner.Seq,
					"weight":       winner.Weight,
					"vendor":       winner.VendorCID,
					"vendorId":     winner.VendorID,
					"transport":    winner.Transport,
					"ruleScope":    routeScope(winner),
					"level":        sendplan.LevelName(winner.Level),
					"buyRateCard":  winner.BuyRateCardID,
					"sellRateCard": winner.SellRateCardID,
				})
		}
	}
	e.Msg.Vendor = winner.VendorCID
	e.Msg.VendorID = winner.VendorID
	e.Msg.RouteID = winner.ID
	e.Msg.Transport = model.Transport(winner.Transport)
	// match+replace rewrites apply ONLY on the winning route. What a rewrite
	// replaces is kept (queue/0029), so that if this route's vendor goes down
	// the next route starts from the client's own values (PO 2026-09-14).
	if re := winner.SenderRegex; re != nil && re.Match != nil && re.Replace != nil {
		if ns := re.Match.ReplaceAllString(e.Msg.Sender, *re.Replace); ns != e.Msg.Sender {
			if e.Msg.OrigSender == "" {
				e.Msg.OrigSender = e.Msg.Sender
			}
			e.Msg.Sender = ns
		}
	}
	if re := winner.ReceiverRegex; re != nil && re.Match != nil && re.Replace != nil {
		if nd := msisdn.Clean(re.Match.ReplaceAllString(e.Msg.Destination, *re.Replace)); nd != e.Msg.Destination {
			if e.Msg.OrigDestination == "" {
				e.Msg.OrigDestination = e.Msg.Destination
			}
			e.Msg.Destination = nd
		}
	}
	if re := winner.KeywordRegex; re != nil && re.Match != nil && re.Replace != nil {
		before := e.Msg.Body
		e.Msg.Body = re.Match.ReplaceAllString(before, *re.Replace)
		if e.Msg.Body != before && e.Msg.OrigBody == "" {
			e.Msg.OrigBody = before
		}
		if e.Msg.Body != before && e.Msg.ConcatTotal <= 1 {
			if enc, segs, err := encoding.Plan(e.Msg.Body, e.Msg.Gsm7); err == nil {
				if segs != e.Msg.Segments {
					log.Printf("[pipeline] route %d keyword rewrite changed segments %d->%d for %s", winner.ID, e.Msg.Segments, segs, e.Msg.ID)
				}
				e.Msg.Encoding, e.Msg.Segments = enc, segs
			} else {
				// The rewrite produced text the forced encoding can't carry —
				// operator config bug. Keep the pre-rewrite plan (message still
				// flows) but say so loudly.
				log.Printf("[pipeline] route %d keyword rewrite broke encoding (%v) for %s — keeping pre-rewrite plan", winner.ID, err, e.Msg.ID)
			}
		}
	}
	// The client is charged the parts THIS vendor will receive (PO
	// 2026-09-21: "client needs to be billed by vendor's encoding, so that we
	// are never in loss"): an SMPP vendor's encoding rules and multipart mode
	// can send more parts than the plan (Latin-1 taken as UCS-2) or fewer
	// (SAR parts carry 160). Counted by the same function the vendor role
	// sends with, so the price reserved now is the price of what goes out; a
	// later failover to another vendor is settled by the billing role against
	// the parts actually sent. HTTP and blackhole vendors take the message
	// whole, so the plan stands. The 10-part cap and the rate limit, earlier,
	// stay on the plan — no vendor is known yet there.
	if winner.Transport == "smpp" && e.Msg.ConcatTotal <= 1 {
		e.Msg.Segments = winner.VendorWire.Parts(e.Msg.Body, e.Msg.Encoding, winner.VendorMultipart)
	}
	// The vendor's queue it waits in (issue 22): its sender's own when that is
	// one of the vendor's short codes, else the common one. The sender is
	// final here (a rule's rewrite is above).
	e.Msg.ShortCode = ""
	if sub := queues.Sub(e.Msg.Sender, winner.VendorShortCodes); sub != queues.Common {
		e.Msg.ShortCode = sub
	}
	return nil
}

// rrState holds per-group smooth-weighted-round-robin state for loadbalance
// (in-memory, single-node; a clustered deployment needs shared state). [SCOPE P1-15]
type rrState struct {
	mu        sync.Mutex
	cw        map[string]*rrGroup // per-group current weights, parallel to the tier order
	lastSweep time.Time
}

// rrGroup is one tier composition's SWRR state; touch drives the sweep.
type rrGroup struct {
	cw    []int
	touch time.Time
}

func newRR() *rrState { return &rrState{cw: map[string]*rrGroup{}} }

// pick is smooth weighted round-robin over the routes' own weights.
func (s *rrState) pick(tier []*refdata.Route) *refdata.Route {
	w := make([]int, len(tier))
	for i, r := range tier {
		w[i] = weightOf(r)
	}
	return s.pickWeighted(tier, w)
}

// pickWeighted is smooth weighted round-robin (the nginx SWRR algorithm): each
// call adds every member's weight to its running current-weight, selects the
// highest, and subtracts the group total from the winner. This interleaves
// picks in weight proportion (70/30 -> A A B A A B A A B A ...) rather than
// blocky bursts. Group key = the members' rule and vendor ids plus weights,
// so a change of membership or of shares starts a fresh split.
func (s *rrState) pickWeighted(tier []*refdata.Route, weights []int) *refdata.Route {
	total, key := 0, ""
	for i, r := range tier {
		if weights[i] <= 0 {
			weights[i] = 0
		}
		total += weights[i]
		key += strconv.FormatInt(r.ID, 10) + ":" + strconv.FormatInt(r.VendorID, 10) + "=" + strconv.Itoa(weights[i]) + ","
	}
	if total == 0 {
		return tier[0]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	// Route-config churn strands dead tier compositions forever without this
	// (chunk-7 ruling 7.3) — same paced-sweep hygiene as the lockout/usage maps.
	if now.Sub(s.lastSweep) >= time.Hour {
		s.lastSweep = now
		for k, g := range s.cw {
			if now.Sub(g.touch) >= 24*time.Hour {
				delete(s.cw, k)
			}
		}
	}
	g := s.cw[key]
	if g == nil || len(g.cw) != len(tier) {
		g = &rrGroup{cw: make([]int, len(tier))}
		s.cw[key] = g
	}
	g.touch = now
	best := 0
	for i := range tier {
		g.cw[i] += weights[i]
		if g.cw[i] > g.cw[best] {
			best = i
		}
	}
	g.cw[best] -= total
	return tier[best]
}

func weightOf(r *refdata.Route) int {
	if r.Weight <= 0 {
		return 1
	}
	return r.Weight
}

// routeScope says whether a rule belongs to this account or is a global
// fallback — the difference an operator asks about when a message went
// somewhere unexpected.
func routeScope(r *refdata.Route) string {
	if r.AccountID == 0 {
		return "global"
	}
	return "account"
}
