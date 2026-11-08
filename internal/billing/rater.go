// Package billing implements Process 9 (Rating & Billing) shared logic: rate a
// message from its route's buy/sell rate cards and settle prepaid balances with
// an append-only ledger. Used by the async `billing` role — off the hot path.
// [SCOPE P9/D-033]
package billing

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/netcode"
	"github.com/yakugate/yakugate/internal/platform/config"
	"github.com/yakugate/yakugate/internal/ratematch"
	"github.com/yakugate/yakugate/internal/sendplan"
)

// rateRow is one rate_card_rates row (NULL match fields = wildcard). prefix maps
// to destination_country_code — a dialing-prefix match on the destination (the
// same column LCR routing prices by).
type rateRow struct {
	id            int64
	mcc, mnc      string    // "" = any
	prefix        string    // "" = any
	pricePerMsg   float64   // price_per_segment
	taxPct        float64   // tax_percentage (GW-13, PO 2026-08-20: totals are GROSS = net + net*tax%)
	effectiveFrom time.Time // zero = always
	convRate      float64   // pinned FX factor card->company currency (D-083.6; 0 = not set, config/0171)
	crpID         string    // exchange-rate provider ref (http_endpoints crp name/id; "" = pinned only)

	// validFrom and validTo are the life of this version of the rate
	// (config/0018): made at validFrom, replaced or removed at validTo (zero =
	// still the current one). Read only when a message is priced as of the
	// time it was accepted (RateOpts.At).
	validFrom, validTo time.Time
}

// card is one rate card with its rows.
type card struct {
	id       int64
	currency string
	// owner is the company the card belongs to (rate_cards.company_id; 0 =
	// none): a row's conversion_rate turns the card's currency into THIS
	// company's, so it is the right rate for a vendor's company only when the
	// vendor's company is the owner (27 Oct 2026).
	owner int64
	rows  []rateRow
}

// companyMoney is what a company's amounts are converted by (27 Oct 2026):
// its currency, and its rate to the base currency (base_conversion_rate, or
// its provider's live rate) — ToBase's inputs.
type companyMoney struct {
	currency string
	baseRate float64
	baseCRP  string
}

// routeCards is a route's buy/sell card refs (0 = none). dlrBuy/dlrSell are
// the same pair for the delivery receipts of the messages this route carried
// (GW-38, PO 2026-08-28: "take dlr rate card in mt rule") — a receipt has no
// route of its own, so it is priced by the MT route its original message went
// out on.
type routeCards struct {
	buy, sell, dlrBuy, dlrSell int64
	// Which vendor this route sends to, and what it carries — the two things
	// needed to fall back to that vendor's own price list when the route
	// names no buying card (config/0111).
	vendorID  int64
	direction string
}

// vendorCards is one vendor's own buy price list, per traffic kind, and the
// company it belongs to (vendors.company_id; 0 = none): what the vendor
// charges is converted into that company's currency (27 Oct 2026).
type vendorCards struct{ mt, mo, dlr, company int64 }

// planCards are the per-vendor cards of one rule's send plan (config/0131),
// kept apart per network exactly as the reference cache resolves them
// (refdata resolveLevels / Route.LevelsFor): the rule's own levels, and each
// exception's. A vendor is in one level only within each, but the same
// vendor may sit in Main with one pair of cards and in an exception with
// another — keyed by vendor alone, the exception's cards priced every
// network, and an exception entry with no cards of its own priced nothing
// differently (review of QA BILL-004, 2026-10-01).
type planCards struct {
	main map[int64][2]int64 // vendor id -> {buy, sell}; 0 = not set
	// exceptions holds each network's own entries; codes lists the networks
	// in the order the reference cache tries them (sorted), only those with a
	// vendor that still exists — refdata drops the others, and a message to
	// such a network is then sent by the rule's own levels.
	exceptions map[string]map[int64][2]int64
	codes      []string
}

// cardsFor is the {buy, sell} pair the rule gives the vendor that carried a
// message to mcc/mnc: its entry in that network's exception when the rule has
// one, else its entry in the rule's own levels.
func (pc planCards) cardsFor(vendorID int64, mcc, mnc string) ([2]int64, bool) {
	if mcc != "" && mnc != "" {
		for _, code := range pc.codes {
			if netcode.Matches(code, mcc, mnc) {
				c, ok := pc.exceptions[code][vendorID]
				return c, ok
			}
		}
	}
	c, ok := pc.main[vendorID]
	return c, ok
}

// Rated is the outcome of rating one message. Nil prices mean no card or no
// matching rate row (recorded as NULL, never charged).
type Rated struct {
	Buy, Sell *float64 // totals: price_per_msg x segments, in each card's currency
	Currency  string   // selling card's currency (buy card's if no sell card)
	// BuyCurrency is the buying card's own currency — NOT always Currency: a
	// EUR vendor's price under a USD selling card was shown, exported and
	// summed as USD (QA billing report #8, 2026-10-07). "" when Buy is nil.
	BuyCurrency string
	Detail      string // audit JSON: which card + rate row matched each side (+fx)

	// SellCharge is what a prepaid balance is actually debited: Sell converted
	// into the company's balance currency (FX round, 2026-08-10). Same-currency
	// (or unknown company currency) charges Sell unchanged. Nil when Sell is.
	SellCharge *float64

	// The sell side's tax split, so a quoted price is explainable without
	// parsing Detail (PO 2026-09-03: expose net, tax, percentage and total to
	// the client). Sell already IS the total — these break it down rather than
	// change it, and they are nil together whenever Sell is nil. SellTaxPct is
	// nil on a zero-tax row, matching Detail's omitempty: "no tax applied" and
	// "taxed at 0%" are the same thing and neither should render a field.
	SellNet    *float64
	SellTax    *float64
	SellTaxPct *float64

	// The conversions, fixed when the price is (Saleem, 27 Oct 2026: every
	// conversion kept, card -> company -> base). 0 = not known.
	//
	// SellFX is what one unit of the selling card's currency was charged as
	// in the customer company's (1 = the same currency); SellBase what one
	// unit of the company's currency is worth in the base currency.
	SellFX, SellBase float64
	// BuyOwn is the buy side in the currency of the vendor's own company
	// (BuyOwnCurrency; the card's for a vendor in no company), BuyFX the rate
	// card -> that company, BuyBase that company -> base. BuyOwn is nil when
	// no rate to the vendor's company is known.
	BuyOwn         *float64
	BuyOwnCurrency string
	BuyFX, BuyBase float64
}

// RateOpts carries the per-account context for a full rating pass.
type RateOpts struct {
	// DefaultSellCard is the account-level selling card used when the route
	// has none (D-082.4 pricing resolution: route card -> account default ->
	// unrated). 0 = no default. Buy stays route-only — account defaults price
	// the client side, not the vendor side.
	DefaultSellCard int64
	// CompanyCurrency is the balance currency the charge settles in. "" skips
	// conversion (charge = sell, today's single-currency behavior).
	CompanyCurrency string
	// CompanyID is the customer company: its rate to the base currency is
	// fixed with the price (SellBase). 0 = not known (no base fact).
	CompanyID int64
	// FX resolves provider-fed rates (nil = row-pinned conversion_rate only).
	FX *FXResolver
	// DLR prices a delivery RECEIPT rather than the message itself (GW-38):
	// the same route, but its dlr_buying/dlr_selling cards. DefaultSellCard is
	// then the account's default_dlr_rate_card_id, so the resolution ladder
	// (route card -> account default -> unrated) is identical for all three
	// traffic kinds.
	DLR bool
	// BuySegments prices the VENDOR side by the parts the vendor was actually
	// sent (mtlog.sent_parts, PO 2026-09-21) — it bills the PDUs it receives.
	// 0 = not known: the buy side uses the same count as the sell side.
	BuySegments int
	// VendorID is the vendor that sent (or will send) the message. A rule's
	// vendor may carry its own buy and sell card (routes.send_plan,
	// config/0131), and the vendor-default buy card is that vendor's — a rule
	// with backups sends through several. 0 = the rule's own vendor.
	VendorID int64
	// At prices the message by the rates as they stood at that moment — the
	// time it was ACCEPTED (go-live test P3 C1, 2026-10-12: "after application
	// or change of a rate card, all new messages get that card, no impact on
	// previous ones"). A rate made later does not price it, and a rate changed
	// or removed since still does. Zero = now, the accept path's reading. The
	// billing role sets it; it also keeps the older versions (KeepHistory).
	At time.Time
}

// Rater caches rate cards, their rates, and route->card links from the config
// store and prices messages against them.
type Rater struct {
	db     dal.RateTables
	mu     sync.RWMutex
	cards  map[int64]card
	routes map[int64]routeCards
	// overrides are the per-vendor cards of rules with a send plan, per
	// network exception: route id -> planCards.
	overrides map[int64]planCards
	// vendors is each vendor's own buy price list, consulted only when a
	// route names no buying card of its own (config/0111).
	vendors map[int64]vendorCards
	// companies is every company's currency and base rate (27 Oct 2026).
	companies map[int64]companyMoney
	// base is the system base currency (YAKU_BASE_CURRENCY).
	base string
	// sig is the change signature the loaded cache was read under (see
	// signature); loaded says a Reload has succeeded at least once.
	sig    string
	loaded bool
	// reloadMu lets one reload run at a time. The backstop timer and the
	// reference-data follower could overlap: a slow reload that read the
	// cards BEFORE a change could swap them in AFTER a quick one that read
	// them after it, putting the old prices back until the next look — and
	// the next look only comes with the next reference reload or the
	// backstop, minutes later.
	reloadMu sync.Mutex
	// history is how far back replaced and removed rate versions are kept
	// (KeepHistory). 0 = the current versions only.
	history time.Duration
}

// KeepHistory makes the rater also hold the rate versions replaced or removed
// in the last d, so a message can be priced by the rates as they stood when
// it was accepted (RateOpts.At). A message accepted before that is priced as
// of d ago — the oldest prices still held. Call it before the first Reload.
func (r *Rater) KeepHistory(d time.Duration) {
	if r == nil || d < 0 {
		return
	}
	r.mu.Lock()
	r.history = d
	r.mu.Unlock()
}

// AcceptedAt is when a message was accepted, read from its id (a ULID carries
// the time it was made). Zero when the id is not one: such a record is priced
// by the rates as they stand now.
func AcceptedAt(msgID string) time.Time {
	u, err := ulid.ParseStrict(msgID)
	if err != nil {
		return time.Time{}
	}
	return ulid.Time(u.Time()).UTC()
}

// NewRater builds a rater over the config store; call Reload before Rate.
func NewRater(cfgDB dal.RateTables) *Rater {
	return &Rater{db: cfgDB, cards: map[int64]card{}, routes: map[int64]routeCards{}, base: config.BaseCurrency()}
}

// The signature's one round trip (the store's RateMark) reads every table
// the rater reads: row counts and the last change of each (every one carries
// updated_at ON UPDATE), plus the database clock.

// signature is a cheap fingerprint of the rate cards, their rates, the rules
// and the vendors — the same kind of signal the reference cache reloads on
// (refdata ChangeSignature). updated_at has whole seconds, so a change made
// in the same second as the look could be missed by a second change in that
// second: a signature taken while any table's last change is that recent is
// made unique, so the next look reloads again.
func (r *Rater) signature(ctx context.Context) (string, error) {
	m, err := r.db.RateMark(ctx)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	settled := true
	for i := range m.Count {
		b.WriteString(strconv.FormatInt(m.Count[i], 10) + "@" + m.Last[i] + ";")
		if m.LastOK[i] && m.NowOK && m.Last[i] >= m.Now {
			settled = false
		}
	}
	if !settled {
		b.WriteString("~" + strconv.FormatInt(time.Now().UnixNano(), 10))
	}
	return b.String(), nil
}

// Reload refreshes the card/rate/route caches — always, whatever the
// signature says (a role's start, the Admin API's nudge, the backstop).
func (r *Rater) Reload(ctx context.Context) error {
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()
	// Taken BEFORE reading, so a change landing during the read moves the
	// signature and is picked up by the next look. Unreadable = "" (the next
	// look reloads again).
	sig, _ := r.signature(ctx)
	if err := r.reload(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	r.sig, r.loaded = sig, true
	r.mu.Unlock()
	return nil
}

// Loaded reports whether the prices have been read at least once. A role
// that has never read them cannot price a message: it must not take one
// (go-live test P1 E4, 2026-10-12 — see pipeline.RaterPricer.Ready).
func (r *Rater) Loaded() bool {
	if r == nil {
		return true // no rater: this gateway prices nothing at accept
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loaded
}

// KeepTrying reloads every `every` until the prices have been read once, then
// returns. A role that started while the config store was down would
// otherwise wait for its backstop timer — five minutes on the http-server —
// before it could take a message again.
func (r *Rater) KeepTrying(ctx context.Context, every time.Duration, logf func(string, ...any)) {
	if r == nil || r.Loaded() {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for !r.Loaded() {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := r.Reload(ctx); err == nil && logf != nil {
			logf("rate cards read: this role can price messages now")
		}
	}
}

// ReloadIfChanged reloads only when the config store moved since the last
// load (or the signature cannot be read). It reports whether it reloaded.
func (r *Rater) ReloadIfChanged(ctx context.Context) (bool, error) {
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()
	sig, err := r.signature(ctx)
	if err == nil {
		r.mu.RLock()
		same := r.loaded && sig != "" && sig == r.sig
		r.mu.RUnlock()
		if same {
			return false, nil
		}
	}
	if err := r.reload(ctx); err != nil {
		return false, err
	}
	r.mu.Lock()
	r.sig, r.loaded = sig, true
	r.mu.Unlock()
	return true, nil
}

// FollowReferenceData returns the hook a role registers with the reference
// cache (refdata.Mem.OnReload): whenever the reference data reloads — its
// fingerprint moved, the Admin API nudged it, a new Redis snapshot arrived —
// the rate cards reload too if their own tables changed (one cheap query
// otherwise). The accept path's prices used to reload only on a 5-minute
// timer, so for minutes after a rate change a message was reserved at the
// old price while the billing role recorded the new one (QA BILL-001).
func (r *Rater) FollowReferenceData(logf func(string, ...any)) func(context.Context) {
	return func(ctx context.Context) {
		if _, err := r.ReloadIfChanged(ctx); err != nil && logf != nil {
			logf("rate cards reload failed (old prices kept): %v", err)
		}
	}
}

// reload reads everything and swaps it in.
func (r *Rater) reload(ctx context.Context) error {
	// valid_to IS NULL = the currently-active version of each rate. Superseded
	// rows stay in the table forever so a historical charge (whose audit JSON
	// pins RateID) can always be explained against the exact row that priced
	// it. [0018_rate_versioning]
	//
	// With a history (KeepHistory) the versions closed inside it are read too:
	// they price the messages accepted while they were in force.
	r.mu.RLock()
	history := r.history
	r.mu.RUnlock()
	var closedSince time.Time
	if history > 0 {
		closedSince = time.Now().UTC().Add(-history)
	}
	read, err := r.db.ReadRates(ctx, closedSince)
	if err != nil {
		return err
	}

	cards := map[int64]card{}
	for _, c := range read.Cards {
		cards[c.ID] = card{id: c.ID, currency: c.Currency, owner: c.Owner}
	}

	// Every company's currency and base rate: what a price is converted by.
	companies := map[int64]companyMoney{}
	for _, c := range read.Companies {
		companies[c.ID] = companyMoney{currency: strings.ToUpper(strings.TrimSpace(c.Currency)), baseRate: c.BaseRate, baseCRP: c.BaseCRP}
	}

	for _, x := range read.Rates {
		row := rateRow{id: x.ID, mcc: x.MCC, mnc: x.MNC, prefix: x.Prefix, pricePerMsg: x.PricePerPart,
			taxPct: x.TaxPct, effectiveFrom: x.EffectiveFrom, convRate: x.ConvRate, crpID: x.CRP,
			validFrom: x.ValidFrom, validTo: x.ValidTo}
		if c, ok := cards[x.Card]; ok {
			c.rows = append(c.rows, row)
			cards[x.Card] = c
		}
	}

	routes := map[int64]routeCards{}
	plans := map[int64]*sendplan.Plan{}
	for _, x := range read.Routes {
		routes[x.ID] = routeCards{buy: x.Buy, sell: x.Sell, dlrBuy: x.DLRBuy, dlrSell: x.DLRSell,
			vendorID: x.Vendor, direction: x.Direction}
		if p, err := sendplan.Parse([]byte(x.SendPlan)); err == nil && p != nil {
			plans[x.ID] = p
		}
	}

	// Each vendor's own price list (config/0111), read here so pricing stays
	// one in-memory lookup — the same discipline as the cards above.
	vendors := map[int64]vendorCards{}
	for _, x := range read.Vendors {
		vendors[x.ID] = vendorCards{mt: x.MT, mo: x.MO, dlr: x.DLR, company: x.Company}
	}

	// The plans' per-vendor cards, resolved per network after the vendors
	// are known (an exception whose vendors are all gone is not one).
	overrides := map[int64]planCards{}
	for id, p := range plans {
		if pc, ok := planCardsOf(p, vendors); ok {
			overrides[id] = pc
		}
	}

	r.mu.Lock()
	r.cards, r.routes, r.vendors, r.overrides, r.companies = cards, routes, vendors, overrides, companies
	r.mu.Unlock()
	return nil
}

// Rate prices one message by its route — the pre-FX single-currency shape,
// kept for existing call sites and tests. Equivalent to RateFull with empty
// opts (no account default, no conversion: SellCharge == Sell).
func (r *Rater) Rate(routeID int64, dest, mcc, mnc string, segments int) Rated {
	return r.RateFull(context.Background(), routeID, dest, mcc, mnc, segments, RateOpts{})
}

// RateFull prices one message with the full D-082.4 resolution (route card ->
// account default -> unrated) and the FX round's conversion: SellCharge is the
// sell total converted into the company's balance currency using the matched
// rate row's provider-fed rate (thirdparty_crp_id via cr_providers) when
// available, else its pinned conversion_rate (D-083.6). The applied FX is
// recorded in the audit Detail. Missing route/cards/rates still yield nil
// prices on that side (rated but unpriced).
func (r *Rater) RateFull(ctx context.Context, routeID int64, dest, mcc, mnc string, segments int, opts RateOpts) Rated {
	if segments < 1 {
		segments = 1
	}
	// The lock covers the reads of the cached cards only. It is let go before
	// the currency lookup below, which may ask the store or a provider: held
	// across that, one slow provider stopped a rate-card reload, and the
	// waiting reload stopped every other pricing call on the process — every
	// account's submit waited out the provider's timeout (QA billing report
	// #1, 2026-10-07).
	r.mu.RLock()
	locked := true
	unlock := func() {
		if locked {
			locked = false
			r.mu.RUnlock()
		}
	}
	defer unlock()
	rc := r.routes[routeID]
	// The moment the message is priced at: now, or the time it was accepted —
	// never further back than the versions held.
	at, asOf := time.Now().UTC(), false
	if !opts.At.IsZero() && opts.At.Before(at) {
		asOf = true
		if oldest := at.Add(-r.history); opts.At.Before(oldest) {
			at = oldest
		} else {
			at = opts.At.UTC()
		}
	}

	type side struct {
		CardID int64   `json:"card_id"`
		RateID int64   `json:"rate_id"`
		PPM    float64 `json:"price_per_msg"`
		// Currency is the card's own: the record's one currency column is the
		// selling card's, so the buying side says its own here.
		Currency string  `json:"currency,omitempty"`
		TaxPct   float64 `json:"tax_percentage,omitempty"` // GW-13: keeps every gross charge explainable
		Tax      float64 `json:"tax,omitempty"`            // the amount added on top of net
		Source   string  `json:"source,omitempty"`         // "account_default" when D-082.4 fallback applied
	}
	detail := map[string]any{}
	var out Rated
	// split carries the sell side's breakdown back out of price(); Sell itself
	// stays the gross total it has been since GW-13.
	type split struct{ net, tax, pct float64 }
	var sellSplit *split
	price := func(cardID int64, segments int) (*float64, string, *side, *rateRow) {
		c, ok := r.cards[cardID]
		if !ok {
			return nil, "", nil, nil
		}
		row, ok := bestRate(c.rows, dest, mcc, mnc, at, asOf)
		if !ok {
			if later, is := bestRate(c.rows, dest, mcc, mnc, time.Now().UTC(), false); asOf && is {
				// A rate prices this number now and did not when the message
				// was accepted: the record says so, or "no price" beside a
				// card that plainly has one reads as a fault.
				was, _ := detail["not_priced"].([]map[string]any)
				detail["not_priced"] = append(was, map[string]any{
					"card_id": cardID, "rate_id": later.id, "rate_from": rateBegan(later).Format(time.RFC3339),
					"accepted": at.Format(time.RFC3339),
					"note":     "this rate began after the message was accepted, so it does not price it",
				})
			}
			return nil, c.currency, nil, nil
		}
		// GW-13 (PO 2026-08-20, "not for future"): the side total is GROSS —
		// net price plus the rate row's tax_percentage. Applied to BOTH sides
		// (a buy row's tax models vendor-invoiced tax, keeping buy/sell margin
		// math like-for-like). Zero-tax rows price exactly as before. FX below
		// converts the gross, so tax converts with it — no separate step.
		net := row.pricePerMsg * float64(segments)
		total := net + net*row.taxPct/100
		s := &side{CardID: cardID, RateID: row.id, PPM: row.pricePerMsg, Currency: c.currency}
		if row.taxPct != 0 {
			s.TaxPct = row.taxPct
			// Rounded to a millionth for the audit JSON only: total - net
			// left 0.0008000000000000004 on the record.
			s.Tax = roundMicro(total - net)
		}
		// Recorded for whichever side is priced last; only the sell side's copy
		// survives, because the buy side is vendor cost and never quoted to a
		// client. Derived here rather than recomputed by the caller so the
		// quoted split can never drift from the charged total.
		sellSplit = &split{net: net, tax: total - net, pct: row.taxPct}
		return &total, c.currency, s, &row
	}

	// A receipt is priced by the MT route's DLR pair; everything else by the
	// route's own pair (GW-38).
	buyCardID, sellCardID := rc.buy, rc.sell
	if opts.DLR {
		buyCardID, sellCardID = rc.dlrBuy, rc.dlrSell
	}
	// The vendor that sent it: a rule with backups sends through several,
	// and each may carry its own cards for this rule (config/0131).
	vendorID := rc.vendorID
	if opts.VendorID > 0 {
		vendorID = opts.VendorID
	}
	// A card prices only while it is enabled and holds a rate: a disabled one
	// ("excluded from pricing") is not loaded at all. The ladder steps past it
	// exactly as if the rule named none — on to the account's default selling
	// card, or the vendor's own buying card — instead of leaving the side
	// unpriced, which sent a prepaid customer's messages out free (QA
	// BILL-004). The loss check reads the same rule from the reference cache
	// (pipeline checkLoss: a card with no loaded rows is skipped), so it
	// compares the price billing will really take.
	// Priced as of the accept time, "holds a rate" means a version that was
	// in being then: a card whose every rate was made later is stepped past
	// too, as the accept path stepped past it when it was still empty.
	usable := func(id int64) bool {
		c, ok := r.cards[id]
		if id <= 0 || !ok {
			return false
		}
		for i := range c.rows {
			if (!asOf && c.rows[i].validTo.IsZero()) || (asOf && inBeing(&c.rows[i], at)) {
				return true
			}
		}
		return false
	}
	// The vendor's cards for this rule, from the entry that carried the
	// message: the exception for its network when the rule has one, else
	// the rule's own levels — the entry the route stage priced (refdata
	// Route.LevelsFor), so LCR, the loss check and the charge agree.
	if ov, ok := r.overrides[routeID].cardsFor(vendorID, mcc, mnc); ok && !opts.DLR {
		if usable(ov[0]) {
			buyCardID = ov[0]
		}
		if usable(ov[1]) {
			sellCardID = ov[1]
		}
	}
	if !usable(buyCardID) {
		buyCardID = 0
	}
	if !usable(sellCardID) {
		sellCardID = 0
	}

	// The vendor's own price list when this route names no buying card
	// (config/0111) — the buy-side twin of the account default below. The
	// route always wins where it has one: a per-route deal is deliberate.
	buyFromVendor := false
	if buyCardID == 0 {
		if vc, ok := r.vendors[vendorID]; ok {
			switch {
			case opts.DLR:
				buyCardID = vc.dlr
			case rc.direction == "MO":
				buyCardID = vc.mo
			default:
				buyCardID = vc.mt
			}
			buyFromVendor = buyCardID != 0
		}
	}

	var buyCur, sellCur string
	var s *side
	buySegs := segments
	if opts.BuySegments > 0 {
		buySegs = opts.BuySegments
	}
	var buyRow *rateRow
	out.Buy, buyCur, s, buyRow = price(buyCardID, buySegs)
	buyOwner := r.cards[buyCardID].owner
	if out.Buy != nil {
		out.BuyCurrency = buyCur
	}
	if s != nil {
		if buyFromVendor {
			s.Source = "vendor_default"
		}
		detail["buy"] = *s
		if buySegs != segments {
			// The audit trail says why the two sides count differently.
			detail["buy_parts"] = buySegs
			detail["sell_parts"] = segments
		}
	}
	fromDefault := false
	if sellCardID == 0 && opts.DefaultSellCard != 0 {
		sellCardID = opts.DefaultSellCard // D-082.4: route card -> account default
		fromDefault = true
	}
	var sellRow *rateRow
	out.Sell, sellCur, s, sellRow = price(sellCardID, segments)
	if s != nil {
		if fromDefault {
			s.Source = "account_default"
		}
		detail["sell"] = *s
	}
	if out.Sell != nil && sellSplit != nil {
		net, tax := sellSplit.net, sellSplit.tax
		out.SellNet, out.SellTax = &net, &tax
		if sellSplit.pct != 0 {
			pct := sellSplit.pct
			out.SellTaxPct = &pct
		}
	}
	if sellCur != "" {
		out.Currency = sellCur // balance settles in the selling card's currency
	} else {
		out.Currency = buyCur
	}

	// What the conversions need, read while the cache is held.
	customer, haveCustomer := r.companies[opts.CompanyID]
	vendorCo := r.vendors[vendorID].company
	vendor, haveVendor := r.companies[vendorCo]
	base := r.base

	// Everything below works on copies: the cache is not read again.
	unlock()

	// FX: convert the sell total into the company's balance currency.
	if out.Sell != nil {
		charge := *out.Sell
		if opts.CompanyCurrency != "" && sellCur != "" && !strings.EqualFold(sellCur, opts.CompanyCurrency) && sellRow != nil {
			// A sell row with no pinned rate (0 since config/0171; the column's
			// default 1 before it) converts at 1, as it always did.
			fx := FXRate{Rate: pinnedOrOne(sellRow.convRate), From: sellCur, To: opts.CompanyCurrency}
			if sellRow.crpID != "" {
				if live, ok := opts.FX.Resolve(ctx, sellRow.crpID, sellCur, opts.CompanyCurrency); ok {
					fx = live
				}
			}
			charge = *out.Sell * fx.Rate
			detail["fx"] = fx
			out.SellFX = fx.Rate
		} else {
			out.SellFX = 1
		}
		out.SellCharge = &charge
		// The customer company's rate to the base currency, fixed with the
		// price (Saleem, 27 Oct 2026: "base is fixed at booking"). Not known
		// = the price stands and the base fact is left out.
		if haveCustomer {
			cur := opts.CompanyCurrency
			if cur == "" {
				cur = customer.currency
			}
			if b, ok := ToBase(ctx, opts.FX, base, cur, customer.baseRate, customer.baseCRP); ok {
				out.SellBase = b
				detail["base"] = map[string]any{"currency": base, "rate": b}
			}
		}
	}

	// The buy side in the vendor's own company's currency, and that to base.
	if out.Buy != nil {
		own, ownCur := 0.0, strings.ToUpper(buyCur)
		known := false
		switch {
		case !haveVendor || vendor.currency == "":
			// A vendor in no company: its bill is in the card's currency.
			own, known = 1, true
		case strings.EqualFold(buyCur, vendor.currency):
			own, known, ownCur = 1, true, vendor.currency
		default:
			ownCur = vendor.currency
			own, known = r.buyToOwn(ctx, opts.FX, buyRow, buyOwner, vendorCo, buyCur, vendor.currency)
		}
		out.BuyOwnCurrency = ownCur
		if known {
			v := *out.Buy * own
			out.BuyOwn, out.BuyFX = &v, own
			if own != 1 {
				detail["buy_fx"] = FXRate{Rate: own, From: strings.ToUpper(buyCur), To: ownCur}
			}
		}
		ownRate, ownCRP := 0.0, ""
		if haveVendor && vendor.currency != "" {
			ownRate, ownCRP = vendor.baseRate, vendor.baseCRP
		}
		if b, ok := ToBase(ctx, opts.FX, base, ownCur, ownRate, ownCRP); ok {
			out.BuyBase = b
			detail["buy_base"] = map[string]any{"currency": base, "rate": b}
		}
	}

	if len(detail) > 0 {
		b, _ := json.Marshal(detail)
		out.Detail = string(b)
	}
	return out
}

// buyToOwn is the rate a buying card's currency is turned into the vendor's
// own company's currency by (27 Oct 2026). A rate row's conversion_rate (or
// its provider) turns the card's currency into its OWNER's currency, so it is
// used only when the card belongs to the vendor's company; otherwise — or when
// the row names no rate (a stored 0, "not set" since config/0171; a 1 is a
// rate) — the gateway's rate for the pair (FXResolver.Pair).
// Not known: the buy side stays in the card's currency and is said so.
func (r *Rater) buyToOwn(ctx context.Context, fx *FXResolver, row *rateRow, owner, vendorCo int64, from, to string) (float64, bool) {
	if row != nil && owner != 0 && owner == vendorCo {
		if row.crpID != "" {
			if live, ok := fx.Resolve(ctx, row.crpID, from, to); ok && live.Rate > 0 {
				return live.Rate, true
			}
		}
		if row.convRate > 0 {
			return row.convRate, true
		}
	}
	return fx.Pair(ctx, from, to)
}

// pinnedOrOne is a row's pinned conversion rate where it is multiplied in as
// it stands: 0 ("not set", config/0171) is 1, which is what the column held
// for an unset rate before that day, so nothing is priced differently.
func pinnedOrOne(r float64) float64 {
	if r > 0 {
		return r
	}
	return 1
}

// roundMicro rounds to a millionth — the grain money is kept at.
func roundMicro(v float64) float64 { return math.Round(v*1e6) / 1e6 }

// bestRate picks the row that prices a number: internal/ratematch, the one
// rule the reference cache's LCR and loss check choose by too (refdata
// rateMatch), so a rule is routed and charged by the same row.
//
// asOf says at is a moment in the past (the message's accept time): then a
// version prices only inside its own life. For "now" the versions' lives are
// not read at all — a rater that prices now holds only current versions, and
// a version made a moment ago by another machine's clock must not be skipped.
func bestRate(rows []rateRow, dest, mcc, mnc string, at time.Time, asOf bool) (rateRow, bool) {
	i, ok := ratematch.Best(rows, func(r *rateRow) ratematch.Key {
		k := ratematch.Key{MCC: r.mcc, MNC: r.mnc, Prefix: r.prefix, From: r.effectiveFrom, ID: r.id}
		if asOf {
			k.Since, k.Until = r.validFrom, r.validTo
		} else if !r.validTo.IsZero() {
			// An older version, held for messages accepted under it: it
			// prices nothing now.
			k.Until = r.validTo
		}
		return k
	}, mcc, mnc, dest, at)
	if !ok {
		return rateRow{}, false
	}
	return rows[i], true
}

// inBeing reports whether this version of a rate existed at that moment —
// made by then, and not yet replaced or removed.
func inBeing(r *rateRow, at time.Time) bool {
	return (r.validFrom.IsZero() || !r.validFrom.After(at)) && (r.validTo.IsZero() || r.validTo.After(at))
}

// rateBegan is when a rate started to price: the later of when its version
// was made and its effective date.
func rateBegan(r rateRow) time.Time {
	if r.effectiveFrom.After(r.validFrom) {
		return r.effectiveFrom.UTC()
	}
	return r.validFrom.UTC()
}

// planCardsOf collects one plan's per-vendor cards, per network. ok=false
// when the plan gives no vendor a card of its own anywhere.
func planCardsOf(p *sendplan.Plan, vendors map[int64]vendorCards) (planCards, bool) {
	collect := func(levels []sendplan.Level) map[int64][2]int64 {
		m := map[int64][2]int64{}
		for _, lv := range levels {
			for _, v := range lv.Vendors {
				if v.BuyRateCardID > 0 || v.SellRateCardID > 0 {
					m[v.VendorID] = [2]int64{v.BuyRateCardID, v.SellRateCardID}
				}
			}
		}
		return m
	}
	// exists mirrors refdata resolveLevels: an exception counts only while
	// one of its vendors still exists.
	exists := func(levels []sendplan.Level) bool {
		for _, lv := range levels {
			for _, v := range lv.Vendors {
				if _, ok := vendors[v.VendorID]; ok {
					return true
				}
			}
		}
		return false
	}
	pc := planCards{main: collect(p.Levels), exceptions: map[string]map[int64][2]int64{}}
	has := len(pc.main) > 0
	for code, lv := range p.Exceptions {
		if !exists(lv) {
			continue
		}
		pc.codes = append(pc.codes, code)
		pc.exceptions[code] = collect(lv)
		has = has || len(pc.exceptions[code]) > 0
	}
	sort.Strings(pc.codes)
	return pc, has
}
