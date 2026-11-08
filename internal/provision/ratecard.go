package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// Rate cards and their prices. [SCOPE P14-RATES]
//
// THE ONE RULE THAT SHAPES ALL OF THIS: a price is never changed in place.
//
// Rates are versioned (config/0018). Changing a price INSERTS a new row at
// version+1 and closes the old one (valid_to = now, superseded_by = the new
// id). Readers take the open rows and pick the most specific one whose
// effective_from has arrived (internal/billing.bestRate). That is what lets a
// charge made last month still be explained: every charge's audit JSON pins a
// rate row id, and that row is still there, untouched, saying what it said.
//
// So this offers "set the price for a destination" and never "edit this row".
// The rest of the file is the eight rules the Admin API enforces on the same
// table (p13-backend/src/routes/rateCardRateRoutes.js, PO 2026-08-20), because
// the two doors open on ONE database and a console that broke a rule the
// portal keeps would corrupt what the portal manages:
//
//  1. one destination, one open chain — a second open row for the same
//     destination would be tie-broken invisibly by the engine;
//  2. a destination's FIRST price cannot be scheduled — there is no earlier
//     version to charge the traffic until the date arrives;
//  3. a future-dated change leaves the old row OPEN and adds a second one;
//     the engine switches over by itself when the date arrives;
//  4. a SCHEDULED row is corrected in place — it never priced anything, so
//     immutability protects nothing, and versioning it would build a chain of
//     rows that never charged a message;
//  5. a live row that already has a scheduled successor is not versioned
//     again — the pending schedule is what must be changed, or the chain
//     forks (see the note in SetRate: here that falls out of the lookup);
//  6. an identical price is refused rather than minted as a new version;
//  7. a price is retired by closing it, never by DELETE;
//  8. the any-destination price can never be closed while it is live, and a
//     specific one may close only while an any-destination price is there to
//     catch the traffic — otherwise those messages quietly become unpriced,
//     and unpriced traffic is charged nothing while looking perfectly normal.
//
// The currency-provider link (thirdparty_crp_id) is deliberately not offered
// (PO 2026-09-18): a rate that follows a feed is superseded by the Admin API's
// FX worker, and a gateway-only install has no such worker — offering the
// field would promise a thing that will never happen.

// RateCard is a price list: what a customer pays, or what a vendor costs us.
type RateCard struct {
	Name      string
	Kind      string // sell (a customer pays it) | buy (a vendor charges it)
	Type      string // MT | MO | DLR | HLR
	Currency  string
	CompanyID int64 // required: there is no shared card (PO 2026-08-20)
	Notify    bool  // sell cards: tell the customer when a price changes
}

// Rate is one price on a card, for one destination.
type Rate struct {
	RateCardID  int64
	MCC         string    // network's country code, e.g. 410; empty = any
	MNC         string    // network within it, e.g. 01; empty = any there
	CountryCode string    // dialling prefix, e.g. 92; empty = not by prefix
	Price       string    // per segment, decimal, as typed — money is not a float
	TaxPercent  string    // added on top; empty = none
	EffectiveAt time.Time // zero = now; a future time schedules the change
}

// RateKinds and RateTypes are the vocabularies the columns accept.
func RateKinds() []string { return []string{"sell", "buy"} }
func RateTypes() []string { return []string{"MT", "MO", "DLR", "HLR"} }

// RateKindWhat and RateTypeWhat explain a choice in one line.
func RateKindWhat(k string) string {
	if strings.EqualFold(k, "sell") {
		return "what a customer pays us — goes on an account"
	}
	return "what a vendor charges us — goes on a vendor"
}

func RateTypeWhat(t string) string {
	switch strings.ToUpper(t) {
	case "MT":
		return "messages a customer sends"
	case "MO":
		return "messages arriving for a customer"
	case "DLR":
		return "delivery receipts"
	case "HLR":
		return "number lookups"
	}
	return ""
}

var (
	moneyRE    = regexp.MustCompile(`^[0-9]+(\.[0-9]{1,6})?$`)
	taxRE      = regexp.MustCompile(`^[0-9]{1,3}(\.[0-9]{1,2})?$`)
	currencyRE = regexp.MustCompile(`^[A-Za-z]{3}$`)
	mccRE      = regexp.MustCompile(`^[0-9]{3}$`)
	mncRE      = regexp.MustCompile(`^[0-9]{1,3}$`)
	prefixRE   = regexp.MustCompile(`^[0-9]{1,6}$`)
)

// ValidateRateCard checks a card before it is written.
func ValidateRateCard(c RateCard) error {
	name := strings.TrimSpace(c.Name)
	if name == "" {
		return refusef("a rate card needs a name, so it can be recognised when it is put on an account or a vendor")
	}
	if len([]rune(name)) > 128 {
		return refusef("a rate card name may be 128 characters; this one is %d", len([]rune(name)))
	}
	if !oneOf(c.Kind, RateKinds()) {
		return refusef("a card is either %q (what a customer pays) or %q (what a vendor costs us); %q is neither", "sell", "buy", c.Kind)
	}
	if !oneOf(c.Type, RateTypes()) {
		return refusef("a card prices one kind of traffic: %s. %q is not one of them", strings.Join(RateTypes(), ", "), c.Type)
	}
	if !currencyRE.MatchString(strings.TrimSpace(c.Currency)) {
		return refusef("%q is not a currency: it is three letters, like PKR or USD", c.Currency)
	}
	// The portal requires this too (PO 2026-08-20: "there is no shared
	// version of rate cards, company association is necessary"): a card's
	// currency and pinned FX mean something only against a known company.
	if c.CompanyID <= 0 {
		return refusef("every rate card belongs to a company — there is no shared card. Its currency only means something against the company it is priced for")
	}
	return nil
}

// ValidateRate checks one price.
func ValidateRate(r Rate) error {
	p := strings.TrimSpace(r.Price)
	if p == "" {
		return refusef("a price is required: this is what one message part costs")
	}
	if !moneyRE.MatchString(p) {
		return refusef("%q is not a price: give a number like 0.05, up to six decimal places, and no currency symbol — the card already says the currency", r.Price)
	}
	// The column is DECIMAL(12,6): six digits before the point. The Admin
	// API refuses a larger price too (rateCardRateRoutes.js MAX_PRICE).
	if whole, _, _ := strings.Cut(strings.TrimLeft(p, "0"), "."); len(whole) > 6 {
		return refusef("%q is too large: a price can be at most 999999.999999", r.Price)
	}
	// Balances and the ledger keep four decimals, so a price above 0 but under
	// 0.0001 is charged as 0.0000 — free traffic with revenue on the record
	// (QA BUG-YK-RC-013). 0 itself stays allowed (a deliberately free route).
	// The Admin API refuses the same (rateCardRateRoutes.js MIN_PRICE).
	if v, err := strconv.ParseFloat(p, 64); err == nil && v > 0 && v < 0.0001 {
		return refusef("%q is below 0.0001, the smallest amount a balance can be charged — it would be charged as 0. Use 0 for a free price, or at least 0.0001", r.Price)
	}
	if r.MCC != "" && !mccRE.MatchString(r.MCC) {
		return refusef("%q is not a country code: three digits, like 410 for Pakistan", r.MCC)
	}
	if r.MNC != "" && !mncRE.MatchString(r.MNC) {
		return refusef("%q is not a network code: one to three digits, like 01", r.MNC)
	}
	if r.MNC != "" && r.MCC == "" {
		return refusef("a network code needs its country code too: 01 on its own could be a network in any country")
	}
	if r.CountryCode != "" && !prefixRE.MatchString(r.CountryCode) {
		return refusef("%q is not a dialling prefix: digits only, like 92 for Pakistan", r.CountryCode)
	}
	if r.CountryCode != "" && r.MCC != "" {
		return refusef("name a network (country code %s) or a dialling prefix (+%s), not both: they are two ways of saying where the message goes, and a row carrying both would only match a message that satisfies each of them", r.MCC, r.CountryCode)
	}
	if t := strings.TrimSpace(r.TaxPercent); t != "" && !taxRE.MatchString(t) {
		return refusef("%q is not a tax percentage: give a number like 17, or 17.50", r.TaxPercent)
	}
	// A percentage, 0 to 100 — the Admin API's rule too (QA RC-003).
	if t, err := strconv.ParseFloat(strings.TrimSpace(r.TaxPercent), 64); err == nil && t > 100 {
		return refusef("%q is not a tax percentage: it is 0 to 100", r.TaxPercent)
	}
	return nil
}

// CreateRateCard writes the card.
func (s Store) CreateRateCard(ctx context.Context, c RateCard) (int64, error) {
	c.Kind = strings.ToLower(strings.TrimSpace(c.Kind))
	c.Type = strings.ToUpper(strings.TrimSpace(c.Type))
	c.Currency = strings.ToUpper(strings.TrimSpace(c.Currency))
	c.Name = strings.TrimSpace(c.Name)
	if err := ValidateRateCard(c); err != nil {
		return 0, err
	}
	// The name carries a UNIQUE key, and it is how an operator recognises the
	// card in every other list — so say plainly when it is taken.
	taken, err := s.DB.RateCardNameTaken(ctx, c.Name)
	if err != nil {
		return 0, err
	}
	if taken {
		return 0, refusef("there is already a rate card called %q, and the name is how it is picked out of a list", c.Name)
	}
	coName, coType, err := s.DB.RateCardCompany(ctx, c.CompanyID)
	if err == dal.ErrNoRows {
		return 0, refusef("there is no company #%d", c.CompanyID)
	}
	if err != nil {
		return 0, err
	}
	// A card's kind follows its company's type, as at the Admin API
	// (services/rateCardRules.js, QA RC-001): a buying company is one we buy
	// from, so its cards are buy cards; a selling company is a customer, so
	// its cards are sell cards; a "both" company, or one with no type, either.
	switch strings.ToLower(strings.TrimSpace(coType)) {
	case "buying":
		if c.Kind != "buy" {
			return 0, refusef("%q is a buying company — we buy from it — so its cards are buy cards (what a vendor costs us)", coName)
		}
	case "selling":
		if c.Kind != "sell" {
			return 0, refusef("%q is a selling company — we sell to it — so its cards are sell cards (what a customer pays)", coName)
		}
	}
	company := c.CompanyID
	id, err := s.DB.InsertRateCard(ctx, dal.RateCardInsert{
		Name: c.Name, Kind: c.Kind, Type: c.Type, Currency: c.Currency,
		CompanyID: company, Notify: boolInt(c.Notify),
	})
	if err != nil {
		return 0, fmt.Errorf("create the rate card: %w", err)
	}
	return id, nil
}

// SetRateOutcome says what SetRate actually did, so the console can tell the
// operator which of the rules above applied to the price they just set.
type SetRateOutcome struct {
	RateID    int64
	Version   int
	First     bool      // the first price for this destination
	Scheduled bool      // takes effect later; the old price keeps charging
	InPlace   bool      // a schedule that had not started yet was corrected
	When      time.Time // when it starts charging; zero = now
	Dropped   bool      // a first price cannot be scheduled, so the date went
}

// SetRate is the ONE way a price is written: it adds one, or versions the one
// already there. It never UPDATEs a row that has priced a message.
func (s Store) SetRate(ctx context.Context, r Rate) (SetRateOutcome, error) {
	var out SetRateOutcome
	r.MCC, r.MNC = strings.TrimSpace(r.MCC), strings.TrimSpace(r.MNC)
	r.CountryCode = strings.TrimSpace(r.CountryCode)
	if err := ValidateRate(r); err != nil {
		return out, err
	}
	if _, err := s.DB.RateCardKind(ctx, r.RateCardID); err != nil {
		return out, refusef("there is no rate card #%d", r.RateCardID)
	}
	if err := s.settleDueSchedules(ctx); err != nil {
		return out, err
	}

	tax := strings.TrimSpace(r.TaxPercent)
	if tax == "" {
		tax = "0"
	}
	scheduled := !r.EffectiveAt.IsZero() && r.EffectiveAt.After(time.Now())

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()

	// The open row for exactly this destination, locked — two people setting
	// the same price at once must not leave two open rows, which the engine
	// would tie-break invisibly. NULL-safe compare (<=>), because an empty
	// match field is stored as NULL and `NULL = NULL` is never true.
	dest := dal.RateDestination{CardID: r.RateCardID, MCC: r.MCC, MNC: r.MNC, Prefix: r.CountryCode}
	cur, err := tx.OpenRateForUpdate(ctx, dest)
	curID, curVersion, curPrice, curTax, curEffective := cur.ID, cur.Version, cur.Price, cur.Tax, cur.Effective
	if err != nil && err != dal.ErrNoRows {
		return out, fmt.Errorf("look for the price this replaces: %w", err)
	}
	// What the customer's notice will call the old price — read now, before
	// any branch below changes it.
	var before map[string]any
	if curID != 0 {
		before = rateSnapshot(ctx, tx, curID)
	}

	// RULE 5 is satisfied by the ORDER BY above rather than by a guard.
	//
	// When a change is already scheduled, the destination has TWO open rows:
	// the one still charging, and its successor waiting for its day. The
	// successor is always the higher id — it was inserted later — so the newest
	// open row is the pending one, and that is what the branches below act on.
	// The portal refuses this case and tells the operator to go and edit the
	// scheduled version instead; here there is nothing to redirect them to,
	// because they named a destination rather than a row, so the console just
	// does it. Either way the chain never forks, which is what the rule is for.
	//
	// A guard here would be unreachable code that reads like protection, which
	// is worse than none: the next person would trust it.

	switch {
	// RULE 2: the first price for a destination cannot be scheduled — there is
	// nothing to charge the traffic in the meantime.
	case curID == 0:
		out.First = true
		if scheduled {
			out.Dropped, scheduled = true, false
		}
		id, err := insertRate(ctx, tx, r, tax, 1, time.Now(), false)
		if err != nil {
			return out, err
		}
		out.RateID, out.Version = id, 1

	// RULE 4: a schedule that has not started is corrected in place.
	case !isLiveRate(curEffective):
		same, err := sameMoney(curPrice, r.Price, curTax, tax)
		if err != nil {
			return out, err
		}
		if same && bothAt(curEffective, r.EffectiveAt) {
			return out, refusef("that is already the price waiting to start, so there is nothing to change")
		}
		when := r.EffectiveAt
		if !scheduled {
			when = time.Time{} // starts now
		}
		if err := tx.CorrectWaitingRate(ctx, curID, strings.TrimSpace(r.Price), tax, nullIfZeroTime(when)); err != nil {
			return out, fmt.Errorf("correct the waiting price: %w", err)
		}
		// Starting it now ends the previous version's reign at this moment.
		if !scheduled {
			if err := tx.CloseRatesSupersededBy(ctx, curID); err != nil {
				return out, fmt.Errorf("close the price this one replaces: %w", err)
			}
		}
		out.RateID, out.Version, out.InPlace = curID, curVersion, true
		out.Scheduled, out.When = scheduled, when

	// RULE 6, then 1 and 3: a live price is versioned.
	default:
		same, err := sameMoney(curPrice, r.Price, curTax, tax)
		if err != nil {
			return out, err
		}
		if same {
			return out, refusef("that is the price already, so there is nothing to record — a new version carrying the same figure would say a change happened when none did")
		}
		from := time.Now()
		if scheduled {
			from = r.EffectiveAt
		}
		// The next number in the destination's chain: a cancelled schedule
		// keeps its row and number, so curVersion+1 could repeat it (QA
		// BUG-YK-RC-020; rateVersions.js supersede does the same).
		next := curVersion + 1
		if maxv, err := tx.MaxRateVersion(ctx, dest); err == nil && maxv+1 > next {
			next = maxv + 1
		}
		id, err := insertRate(ctx, tx, r, tax, next, from, scheduled)
		if err != nil {
			return out, err
		}
		// A future-dated price leaves the old row OPEN: it must keep charging
		// until the day arrives. An immediate one closes it now.
		if err := tx.SupersedeRate(ctx, curID, id, !scheduled); err != nil {
			return out, fmt.Errorf("close the price this replaces: %w", err)
		}
		out.RateID, out.Version, out.Scheduled = id, next, scheduled
		if scheduled {
			out.When = from
		}
	}

	if err := tx.Commit(); err != nil {
		return out, err
	}
	// The same change types the portal records, so its banner and its email
	// read a console change exactly as they read their own: a destination's
	// first price is news of a new price, not of a changed one.
	change := "update"
	if out.First {
		change = "create"
	}
	s.noticePending(ctx, r.RateCardID, out.RateID, change, before, rateSnapshot(ctx, s.DB, out.RateID), out.When)
	return out, nil
}

// rateSnapshot is one price as the portal's rate email reads it. The field
// names are the column names, because that is what the portal stores in
// before_json/after_json and what its email builder looks up: it files each
// change under match_mcc|match_mnc|destination_country_code and takes the old
// figure from before.price_per_segment. A thinner snapshot is not harmless —
// without the match fields a change to one network is filed under "any
// destination", and without `before` the customer is told the old price was
// the new one (found 2026-09-18, reading rateChangeAlerts.buildRows).
func rateSnapshot(ctx context.Context, q dal.ConfigQueries, id int64) map[string]any {
	row, err := q.RateSnapshotRow(ctx, id)
	if err != nil {
		return nil
	}
	mcc, mnc, cc := row.MCC, row.MNC, row.Prefix
	price, tax, unit, conv := row.Price, row.Tax, row.Unit, row.Conv
	version, eff := row.Version, row.Effective
	str := func(v dal.NullString) any {
		if !v.Valid {
			return nil
		}
		return v.String
	}
	m := map[string]any{
		"id": id, "version": version,
		"match_mcc": str(mcc), "match_mnc": str(mnc), "destination_country_code": str(cc),
		"price_per_segment": price, "tax_percentage": tax,
		"costing_unit": unit, "conversion_rate": conv,
		"effective_from": nil,
	}
	if eff.Valid {
		m["effective_from"] = eff.Time.UTC().Format(time.RFC3339)
	}
	return m
}

func insertRate(ctx context.Context, tx dal.ConfigTx, r Rate, tax string, version int, from time.Time, scheduled bool) (int64, error) {
	// effective_from is what the engine compares against the clock; a price
	// starting now leaves it NULL, which reads as "always".
	var eff dal.NullTime
	if scheduled {
		eff = dal.NullTime{Time: from, Valid: true}
	}
	id, err := tx.InsertRate(ctx, dal.RateInsert{
		CardID: r.RateCardID, Version: version, MCC: r.MCC, MNC: r.MNC, Prefix: r.CountryCode,
		Price: strings.TrimSpace(r.Price), Tax: tax, ValidFrom: time.Now(), Effective: eff,
	})
	if err != nil {
		return 0, fmt.Errorf("write the price: %w", err)
	}
	return id, nil
}

// CloseRate retires one price. RULE 7: it is closed, never deleted — a row
// that priced a real charge has to stay where that charge's audit points.
// RULE 8 is the pair of guards below.
func (s Store) CloseRate(ctx context.Context, cardID, rateID int64) error {
	if err := s.settleDueSchedules(ctx); err != nil {
		return err
	}
	row, err := s.DB.RateForClose(ctx, cardID, rateID)
	if err != nil {
		return refusef("there is no price #%d on card #%d", rateID, cardID)
	}
	mcc, mnc, cc, eff, closed := row.MCC, row.MNC, row.Prefix, row.Effective, row.Closed
	if closed.Valid {
		return refusef("price #%d is already retired; it is kept so the charges it made can still be explained", rateID)
	}
	before := rateSnapshot(ctx, s.DB, rateID)

	// Cancelling a schedule that never started is always safe: the price it
	// was going to replace simply keeps charging. It is still a notice: the
	// customer may already have been told the new price is coming, and the
	// portal records this the same way.
	if !isLiveRate(eff) {
		tx, err := s.DB.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := tx.CloseRateNow(ctx, rateID); err != nil {
			return fmt.Errorf("cancel the waiting price: %w", err)
		}
		if err := tx.ClearSuccessor(ctx, rateID); err != nil {
			return fmt.Errorf("hand the destination back to the price in force: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		s.noticePending(ctx, cardID, rateID, "deactivate", before, nil, time.Time{})
		return nil
	}

	if !mcc.Valid && !mnc.Valid && !cc.Valid {
		return refusef("this is the any-destination price: it is what catches every message no other price covers, so one version of it must always be in force. Set a different figure on it instead, or switch the whole card off")
	}
	fallback, err := s.DB.LiveAnyDestinationRates(ctx, cardID)
	if err != nil {
		return err
	}
	if fallback == 0 {
		return refusef("this card has no any-destination price to fall back on, so retiring this one would leave its traffic with no price at all — and unpriced traffic is charged nothing while looking perfectly normal. Add an any-destination price first")
	}
	if err := s.DB.CloseRateNow(ctx, rateID); err != nil {
		return fmt.Errorf("retire price %d: %w", rateID, err)
	}
	s.noticePending(ctx, cardID, rateID, "deactivate", before, nil, time.Time{})
	return nil
}

// settleDueSchedules stamps a predecessor's valid_to once its successor's date
// has arrived. The engine is already charging with the successor by then
// (latest-effective wins), but until this runs the old row still reads as open,
// and the history says two prices were in force at once. The portal does this
// on every list; a gateway with no portal has to do it itself.
func (s Store) settleDueSchedules(ctx context.Context) error {
	err := s.DB.SettleDueRateSchedules(ctx)
	if err != nil {
		return fmt.Errorf("settle finished schedules: %w", err)
	}
	return nil
}

// noticePending records a sell card's price change as a pending customer
// notice (GW-30) — the same row the portal's banner and its auto-send read, so
// a price changed from the console is announced like any other. before/after
// are rateSnapshot maps, the shape the portal writes. It never fails the
// change: the price is already committed by the time we are here, and a
// notification problem must not undo it.
func (s Store) noticePending(ctx context.Context, cardID, rateID int64, kind string,
	before, after map[string]any, when time.Time) {
	cardKind, notify, err := s.DB.RateCardNotice(ctx, cardID)
	if err != nil {
		return
	}
	if cardKind != "sell" || notify == 0 {
		return
	}
	asJSON := func(m map[string]any) dal.NullString {
		if m == nil {
			return dal.NullString{}
		}
		b, err := json.Marshal(m)
		if err != nil {
			return dal.NullString{}
		}
		return dal.NullString{String: string(b), Valid: true}
	}
	var eff dal.NullTime
	if !when.IsZero() && when.After(time.Now()) {
		eff = dal.NullTime{Time: when, Valid: true}
	}
	_ = s.DB.InsertRateNotice(ctx, dal.RateNotice{
		CardID: cardID, RateID: rateID, Change: kind,
		Before: asJSON(before), After: asJSON(after), Effective: eff,
	})
}

// RateCardRow is one line of the card list.
type RateCardRow struct {
	ID       int64
	Name     string
	Kind     string
	Type     string
	Currency string
	Company  string
	Disabled bool
	Prices   int    // prices in force on it
	UsedBy   int    // accounts, vendors and rules pointing at it
	Trouble  string // why this card would not price a message
}

// ListRateCards shows every card, and says which ones cannot actually price.
func (s Store) ListRateCards(ctx context.Context, limit int) ([]RateCardRow, error) {
	if limit <= 0 {
		limit = 200
	}
	if err := s.settleDueSchedules(ctx); err != nil {
		return nil, err
	}
	rows, err := s.DB.RateCardList(ctx, limit)
	if err != nil {
		var re dal.RowsError
		if errors.As(err, &re) {
			return nil, re.Err
		}
		return nil, fmt.Errorf("list rate cards: %w", err)
	}
	var out []RateCardRow
	for _, row := range rows {
		r := RateCardRow{ID: row.ID, Name: row.Name, Kind: row.Kind, Type: row.Type,
			Currency: row.Currency, Company: row.Company, Prices: row.Prices}
		dis, wildcard := row.Disabled, row.Wildcard
		r.Disabled = dis != 0
		switch {
		case r.Prices == 0:
			r.Trouble = "no prices yet — traffic on this card is charged nothing"
		case wildcard == 0:
			r.Trouble = "no any-destination price — anything the listed destinations miss is charged nothing"
		}
		out = append(out, r)
	}
	for i := range out {
		n, err := s.rateCardUsers(ctx, out[i].ID)
		if err != nil {
			return nil, err
		}
		out[i].UsedBy = n
	}
	return out, nil
}

// cardColumns is every column in the config store that names a rate card,
// taken from information_schema on 2026-09-18 rather than remembered. The
// reselling three are read by no engine code, but the portal reads them to
// decide who is told about a price change — so a card named only there is
// still in use, and deleting it would leave them pointing at nothing.
var cardColumns = map[string][]string{
	"sms_service_config": {
		"default_mt_rate_card_id", "default_mo_rate_card_id",
		"default_dlr_rate_card_id", "default_hlr_rate_card_id",
		"default_reselling_mt_rate_card_id", "default_reselling_mo_rate_card_id",
		"default_reselling_dlr_rate_card_id",
	},
	"vendors": {
		"default_mt_buying_rate_card_id", "default_mo_buying_rate_card_id",
		"default_dlr_buying_rate_card_id",
	},
	"routes": {
		"buying_rate_card_id", "selling_rate_card_id",
		"dlr_buying_rate_card_id", "dlr_selling_rate_card_id",
	},
}

// rateCardUsers counts the rows pointing at a card, across every column above.
// An error is returned, never swallowed: this count is what stands between a
// card and its deletion, and a guard that answers "nobody" when it could not
// look is how a card in use gets deleted.
func (s Store) rateCardUsers(ctx context.Context, id int64) (int, error) {
	total := 0
	for table, cols := range cardColumns {
		n, err := s.DB.RateCardColumnUsers(ctx, table, cols, id)
		if err != nil {
			return 0, fmt.Errorf("check what uses rate card %d (%s): %w", id, table, err)
		}
		total += n
	}
	// A vendor's own buy or sell card for one rule, inside the rule's plan
	// (config/0131).
	n, err := s.DB.RateCardPlanUsers(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("check what uses rate card %d (rule plans): %w", id, err)
	}
	return total + n, nil
}

// RateCardUsers says how many accounts, vendors and rules price their traffic
// from a card — what stands between it and being deleted.
func (s Store) RateCardUsers(ctx context.Context, id int64) (int, error) {
	return s.rateCardUsers(ctx, id)
}

// RateRow is one price, as an operator reads it.
type RateRow struct {
	ID          int64
	Version     int
	Destination string // "any destination", "410-01", "+92"
	Price       string
	Tax         string
	State       string // in force | starts <when> | retired <date>
	Open        bool
	Replaces    int64
}

// ListRates shows a card's prices. history=false shows what is open: what is
// charging now, plus anything scheduled to start.
func (s Store) ListRates(ctx context.Context, cardID int64, history bool) ([]RateRow, error) {
	if err := s.settleDueSchedules(ctx); err != nil {
		return nil, err
	}
	rows, err := s.DB.RateList(ctx, cardID, history)
	var re dal.RowsError
	if err != nil && !errors.As(err, &re) {
		return nil, fmt.Errorf("list prices: %w", err)
	}
	if err != nil && rows == nil {
		return nil, re.Err
	}
	var out []RateRow
	for _, row := range rows {
		r := RateRow{ID: row.ID, Version: row.Version, Price: row.Price, Tax: row.Tax, Replaces: row.Replaces}
		mcc, mnc, cc := row.MCC, row.MNC, row.Prefix
		eff, closed := row.Effective, row.Closed
		r.Open = !closed.Valid
		r.Destination = describeDestination(mcc, mnc, cc)
		switch {
		case closed.Valid:
			r.State = "retired " + closed.Time.Format("2006-01-02")
		case !isLiveRate(eff):
			r.State = "starts " + eff.Time.Format("2006-01-02 15:04")
		default:
			r.State = "in force"
		}
		out = append(out, r)
	}
	if err != nil {
		return out, re.Err // the read was cut short: what was read, and why
	}
	return out, nil
}

// SetRateCardDisabled switches a card off or on.
func (s Store) SetRateCardDisabled(ctx context.Context, id int64, disabled bool) error {
	n, err := s.DB.SetRateCardDisabled(ctx, id, boolInt(disabled))
	if err != nil {
		return fmt.Errorf("update rate card %d: %w", id, err)
	}
	if n == 0 {
		return refusef("there is no rate card #%d", id)
	}
	return nil
}

// DeleteRateCard removes a card that never priced anything. A card holding
// prices is refused: those rows are what historical charges point at to
// explain themselves, and this card's row is what names their currency.
func (s Store) DeleteRateCard(ctx context.Context, id int64) error {
	n, err := s.rateCardUsers(ctx, id)
	if err != nil {
		return err // could not look, so do not delete
	}
	if n > 0 {
		return refusef("%d account(s), vendor(s) or rule(s) still price their traffic from this card. Point them at another one first, or their messages would stop being rated and the money would simply stop being counted", n)
	}
	prices, err := s.DB.RateCardPriceCount(ctx, id)
	if err != nil {
		return err
	}
	if prices > 0 {
		return refusef("this card holds %d price(s), including retired ones. They are what past charges point at to explain themselves, so they are never deleted — switch the card off instead", prices)
	}
	gone, err := s.DB.DeleteRateCardRow(ctx, id)
	if err != nil {
		return fmt.Errorf("delete rate card %d: %w", id, err)
	}
	if gone == 0 {
		return refusef("there is no rate card #%d", id)
	}
	return nil
}

// assertSellCardCompany refuses a SELL card owned by a company other than
// the account's (QA BUG-YK-RC-018): a customer is charged only by its own
// company's cards. A card or account with no company (older rows) passes.
// The Admin API's copy is resources.js assertSellCardsOfCompany — change
// together; this door must never be looser.
func (s Store) assertSellCardCompany(ctx context.Context, cardID, accountID int64) error {
	if cardID <= 0 || accountID <= 0 {
		return nil
	}
	cardCo, name, err := s.DB.RateCardOwner(ctx, cardID)
	if err != nil {
		return nil // whether the card exists is the caller's check
	}
	acctCo, err := s.DB.SellCardAccountCompany(ctx, accountID)
	if err != nil {
		return nil
	}
	if cardCo != 0 && acctCo != 0 && cardCo != acctCo {
		return refusef("card #%d %q belongs to another company — a customer is charged only by its own company's cards", cardID, name)
	}
	return nil
}

// AssignRateCard puts a card on an account or a vendor. The side is checked: a
// buy card used as a customer's price would undercharge every message it
// touched and look perfectly normal doing it.
func (s Store) AssignRateCard(ctx context.Context, what string, ownerID, cardID int64, traffic string) error {
	traffic = strings.ToUpper(strings.TrimSpace(traffic))
	if !oneOf(traffic, RateTypes()) {
		return refusef("%q is not a kind of traffic: %s", traffic, strings.Join(RateTypes(), ", "))
	}
	kind, cardType, err := s.DB.RateCardKindType(ctx, cardID)
	if err != nil {
		return refusef("there is no rate card #%d", cardID)
	}
	if cardType != "" && !strings.EqualFold(cardType, traffic) {
		return refusef("card #%d prices %s traffic, not %s", cardID, cardType, traffic)
	}

	switch what {
	case "account":
		if kind != "sell" {
			return refusef("card #%d is a %s card. An account is what we CHARGE, so it takes a sell card — a buy card here would undercharge every message and look perfectly normal doing it", cardID, kind)
		}
		if err := s.assertSellCardCompany(ctx, cardID, ownerID); err != nil {
			return err
		}
		col := map[string]string{
			"MT": "default_mt_rate_card_id", "MO": "default_mo_rate_card_id",
			"DLR": "default_dlr_rate_card_id", "HLR": "default_hlr_rate_card_id",
		}[traffic]
		ok, err := s.DB.AccountExists(ctx, ownerID)
		if err != nil {
			return err
		}
		if !ok {
			return refusef("there is no account #%d", ownerID)
		}
		if err := s.DB.SetAccountRateCard(ctx, col, cardID, ownerID); err != nil {
			return fmt.Errorf("put the card on the account: %w", err)
		}
	case "vendor":
		if kind != "buy" {
			return refusef("card #%d is a %s card. A vendor is what we PAY, so it takes a buy card", cardID, kind)
		}
		col := map[string]string{
			"MT": "default_mt_buying_rate_card_id", "MO": "default_mo_buying_rate_card_id",
			"DLR": "default_dlr_buying_rate_card_id",
		}[traffic]
		if col == "" {
			return refusef("a vendor is priced for MT, MO or DLR traffic, not %s — a number lookup is something we charge a customer for, not something a vendor bills us for here", traffic)
		}
		ok, err := s.DB.VendorExists(ctx, ownerID)
		if err != nil {
			return err
		}
		if !ok {
			return refusef("there is no vendor #%d", ownerID)
		}
		if err := s.DB.SetVendorRateCard(ctx, col, cardID, ownerID); err != nil {
			return fmt.Errorf("put the card on the vendor: %w", err)
		}
	default:
		return refusef("a rate card goes on an account or a vendor, not on %q", what)
	}
	return nil
}

// isLiveRate says whether a row is charging now, or still waiting for its day.
func isLiveRate(eff dal.NullTime) bool {
	return !eff.Valid || !eff.Time.After(time.Now())
}

// bothAt reports whether a stored start time and a wanted one are the same
// minute — the resolution a person types.
func bothAt(stored dal.NullTime, wanted time.Time) bool {
	if !stored.Valid {
		return wanted.IsZero()
	}
	if wanted.IsZero() {
		return false
	}
	return stored.Time.Truncate(time.Minute).Equal(wanted.Truncate(time.Minute))
}

// sameMoney compares a stored DECIMAL against what was typed. "0.050000" and
// "0.05" are the same price, and a new version saying otherwise would claim a
// change that never happened.
func sameMoney(oldPrice, newPrice, oldTax, newTax string) (bool, error) {
	a, err := strconv.ParseFloat(strings.TrimSpace(oldPrice), 64)
	if err != nil {
		return false, nil // an unreadable stored price is never "the same"
	}
	b, err := strconv.ParseFloat(strings.TrimSpace(newPrice), 64)
	if err != nil {
		return false, refusef("%q is not a price", newPrice)
	}
	c, _ := strconv.ParseFloat(strings.TrimSpace(oldTax), 64)
	d, _ := strconv.ParseFloat(strings.TrimSpace(newTax), 64)
	return a == b && c == d, nil
}

func describeDestination(mcc, mnc, cc string) string {
	switch {
	case mcc != "" && mnc != "":
		return mcc + "-" + mnc
	case mcc != "":
		return mcc + " (any network)"
	case cc != "":
		return "+" + cc
	default:
		return "any destination"
	}
}

func nullIfZeroTime(t time.Time) dal.NullTime {
	if t.IsZero() {
		return dal.NullTime{}
	}
	return dal.NullTime{Time: t, Valid: true}
}
