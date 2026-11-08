package dal

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// The billing family (plan of 29 Oct 2026). Billing's rules — what a moment
// costs, who pays, when — stay in internal/billing and internal/billingq;
// what they keep and read is here, by what the data is:
//
//	LiveBalances    the live balance counters a deduction moves (the main Redis)
//	CompanyBalances the balance of record and its ledger (the config store)
//	BillingQueue    the selling and buying queues of money events (the queue Redis)
//	UsageBooks      the usage buckets the queues are booked into (the config store)
//	BillingConfig   what prices and terms are read from (the config store)
//
// Amounts cross these interfaces in ledger units (ten-thousandths, the
// DECIMAL(14,4) of the money columns; the live counters use the same unit),
// except where today's code reads or writes a column as a float: those keep
// the float, so nothing is rounded where it was not before.

// LiveBalances is the live balance counters: one per prepaid company (and
// one per unlinked account, the early build's), moved by every deduction,
// and the index of the counters seeded from the balance of record. Every
// method is the one Redis step it was before the move: one command, one
// script or one pipeline.
type LiveBalances interface {
	// Deduct moves a company's counter down by units (negative = given back)
	// and answers the counter after: the one call a balance is.
	Deduct(ctx context.Context, company, units int64) (left int64, err error)
	// DeductMany is Deduct for many companies in one round trip; each
	// answer is in the order asked.
	DeductMany(ctx context.Context, ds []Deduction) []Deducted
	// IsSeeded says the company's counter was seeded from the balance of
	// record (it is on the index).
	IsSeeded(ctx context.Context, company int64) (bool, error)
	// SeedOnTop adds units (the balance of record) to a counter not yet on
	// the index and puts it there — once, whoever asks. Answers the counter
	// after.
	SeedOnTop(ctx context.Context, company, units int64) (int64, error)
	// SeedMissing makes each company's counter that is not there (from its
	// balance of record, in units) and names every one on the index, in
	// one round trip. It answers how many it made.
	SeedMissing(ctx context.Context, bs []CounterSeed) (made int, err error)
	// Seed makes one counter when it is not there (accountID is used only
	// when companyID is 0: the early build's per-account counter).
	Seed(ctx context.Context, accountID, companyID, units int64) error
	// Hold takes units iff the counter covers them: 1 taken, 0 short,
	// -2 no counter (seed it first).
	Hold(ctx context.Context, accountID, companyID, units int64) (int, error)
	// HoldLeft is Hold that also answers what the hold left: {1, left},
	// {0} short, {-2} no counter.
	HoldLeft(ctx context.Context, accountID, companyID, units int64) ([]int64, error)
	// AddIfThere gives units back to a counter that is there; false = no
	// counter (the caller seeds first: adding to nothing would mint money).
	AddIfThere(ctx context.Context, accountID, companyID, units int64) (bool, error)
	// Add gives units back unconditionally.
	Add(ctx context.Context, accountID, companyID, units int64) error
	// Current is a counter as it stands; found=false when there is none.
	Current(ctx context.Context, accountID, companyID int64) (units int64, found bool, err error)
	// Drop removes a counter and takes it off the index (a resync: the next
	// hold seeds it again from the balance of record).
	Drop(ctx context.Context, accountID, companyID int64) error
	// DropAll removes every counter; it answers how many.
	DropAll(ctx context.Context) (int, error)
	// Seeded is every counter on the index.
	Seeded(ctx context.Context) ([]Counter, error)
	// Read is one counter of the index as it stands; found=false when it is
	// gone.
	Read(ctx context.Context, c Counter) (units int64, found bool, err error)
	// Forget takes a counter that is gone off the index.
	Forget(ctx context.Context, c Counter) error
	// Credit applies a manual change of a company's balance to its counter:
	// added when the counter is there, else the counter is made at total.
	Credit(ctx context.Context, company, delta, total int64) error
	// Undo takes back a Credit whose transaction did not commit.
	Undo(ctx context.Context, company, delta int64) error
	// Unsent says a failed call certainly never ran (refused before it was
	// sent, or refused whole), so trying it again cannot do it twice.
	Unsent(err error) bool
}

// Deduction is one company's move for DeductMany.
type Deduction struct {
	Company, Units int64
}

// Deducted is what one Deduction answered: the counter after, or why not.
type Deducted struct {
	Left int64
	Err  error
}

// CounterSeed is one company's balance of record, for SeedMissing.
type CounterSeed struct {
	Company, Units int64
}

// Counter is one live counter: a company's, or (Company 0) an unlinked
// account's. Both are 0 for a name the index holds that is neither; Key is
// the counter's own name in its store, for keeping track of it.
type Counter struct {
	Key              string
	Account, Company int64
}

// CompanyBalances is the balance of record — companies.balance — and the
// ledger rows written beside it. With live counters it is what they are
// seeded from and copied back to; without them, the holds are taken here.
type CompanyBalances interface {
	// Balance is a company's balance of record.
	Balance(ctx context.Context, companyID int64) (float64, error)
	// AccountBalance is an unlinked account's own balance (the early build's).
	AccountBalance(ctx context.Context, accountID int64) (float64, error)
	// Prepaid is every prepaid company's balance of record.
	Prepaid(ctx context.Context) ([]CompanyBalance, error)
	// SetBalance and SetAccountBalance write a live counter's figure back.
	SetBalance(ctx context.Context, companyID int64, balance float64) error
	SetAccountBalance(ctx context.Context, accountID int64, balance float64) error
	// HoldUnits takes units from a company's balance under its row lock,
	// only when the balance covers them, and writes the hold's ledger row
	// when row is set — one transaction. left is the balance after.
	HoldUnits(ctx context.Context, h Hold, row bool) (ok bool, left int64, err error)
	// GiveBackUnits returns a hold under the row lock, with its ledger row
	// when row is set — one transaction.
	GiveBackUnits(ctx context.Context, h Hold, row bool) error
	// HoldIfCovered takes amount in one statement when the balance covers it
	// (false = it does not); GiveBack adds it back. No row, no transaction:
	// the early build's reservation.
	HoldIfCovered(ctx context.Context, companyID int64, amount float64) (bool, error)
	GiveBack(ctx context.Context, companyID int64, amount float64) error
	// AddHoldRow and AddGiveBackRow write the ledger row of a hold, or of a
	// hold given back, taken elsewhere (on a live counter).
	AddHoldRow(ctx context.Context, r LedgerRow) error
	AddGiveBackRow(ctx context.Context, r LedgerRow) error
	// HoldRows counts the hold rows written under a reference.
	HoldRows(ctx context.Context, reference string) (int, error)
}

// CompanyBalance is one company's balance of record.
type CompanyBalance struct {
	Company int64
	Balance float64
}

// Hold is one hold on a company's balance of record, in ledger units.
// Reference names it in the ledger (the message id); Reason is the reason
// of its row.
type Hold struct {
	Account, Company, Units int64
	Reference, Reason       string
}

// LedgerRow is one balance_history row of a hold or of a hold given back,
// in ledger units: Units is the amount of the row (negative = taken).
type LedgerRow struct {
	Account, Company   int64
	Units, Prev, After int64
	Reference, Reason  string
}

// BillingQueue is one deployment's queues of money events, one per side
// (sell, buy; "" = the one mixed queue of 26 Oct), each split in lanes. An
// event waits in a lane's ready list (scored by its moment), is claimed
// into the lane's doing list under a batch id, and goes to delayed when the
// books refused it. A lane is booked by one worker at a time (its turn).
type BillingQueue interface {
	// Put adds events to lanes, and a lane's name to its side's list of
	// lanes where asked: one round trip for every add.
	Put(ctx context.Context, adds []LaneAdd) error
	// Waiting counts what waits on the ready lists of the sides (the lanes
	// 0..lanes-1 and every lane named) and how long the oldest has (ms).
	Waiting(ctx context.Context, scope string, sides []string, lanes int) (n, oldestMS int64, err error)
	// Lanes is every lane named on a side (as written).
	Lanes(ctx context.Context, scope, side string) ([]string, error)
	// TakeTurn takes a lane's turn when it is free, or renews it when it is
	// token's, for ttl.
	TakeTurn(ctx context.Context, l Lane, token string, ttl time.Duration) (bool, error)
	// DropTurn lets a lane's turn go when it is token's.
	DropTurn(ctx context.Context, l Lane, token string) error
	// Claim gives the batch to book while the turn is token's: the one left
	// in doing (repeat), else up to max events — the delayed ones whose time
	// (nowMS) has come first, then the oldest ready ones — moved into doing
	// under newID. Answers {id, "1"|"0", member…}, or nothing.
	Claim(ctx context.Context, l Lane, token string, max int, newID string, nowMS int64) ([]string, error)
	// SetBatchID names the batch in doing.
	SetBatchID(ctx context.Context, l Lane, id string) error
	// Done ends a booked batch while the turn is token's: doing is cleared
	// and later go to delayed. 1 = done, 0 = the turn was lost.
	Done(ctx context.Context, l Lane, token string, later []Scored) (int, error)
	// DropIfEmpty takes an empty lane (nothing ready, delayed or doing) off
	// its side's list and says so.
	DropIfEmpty(ctx context.Context, l Lane) (bool, error)
	// Count is what a lane holds now, in one round trip.
	Count(ctx context.Context, l Lane) (LaneCount, error)
	// Unsent: as LiveBalances.Unsent.
	Unsent(err error) bool
}

// Lane is one lane of one side of a deployment's billing queue.
type Lane struct {
	Scope, Side string
	Lane        int
}

// LaneAdd is the events going to one lane in a Put; Name also names the
// lane on its side's list.
type LaneAdd struct {
	Lane
	Events []Scored
	Name   bool
}

// Scored is one queued event: its member and its score (ms).
type Scored struct {
	Score  float64
	Member string
}

// LaneCount is what a lane holds: ready, delayed and doing, and the score of
// the oldest ready one (HasOldest false when none waits).
type LaneCount struct {
	Ready, Delayed, Doing int64
	Oldest                float64
	HasOldest             bool
}

// UsageBooks is the usage buckets the billing queues are booked into
// (usage_buckets), each batch once (usage_batches).
type UsageBooks interface {
	// Book writes a batch's rows under its guard id in one transaction.
	// already = the guard was there: the batch was booked before, and
	// nothing was written.
	Book(ctx context.Context, guard string, rows []UsageRow) (already bool, err error)
	// Away says a failed Book failed for a reason that is not its rows'
	// values (the store away, busy, read-only, a table missing): nothing
	// was written, and the batch is tried again whole.
	Away(err error) bool
}

// UsageRow is one usage_buckets row of a batch: amounts in ledger units.
// Conv says the events carried their conversions (the three amounts are
// written; else they are NULL).
type UsageRow struct {
	Bucket                        time.Time
	Side                          string
	Company, Account, Vendor      int64
	Kind, Model, Currency         string
	CardCurrency, CompanyCurrency string
	FX                            string
	Items, Parts, Units           int64
	Conv                          bool
	CardUnits, CompanyUnits       int64
	BaseUnits                     int64
	ConvMissing, BaseMissing      int64
}

// BillingTerms is every account's and vendor's billing terms.
type BillingTerms interface {
	AccountTerms(ctx context.Context) ([]AccountTermsRow, error)
	// VendorPoints is each vendor's charge point as written; nil with no
	// error when the store has no such column yet (every vendor charges on
	// acceptance).
	VendorPoints(ctx context.Context) (map[int64]string, error)
}

// AccountTermsRow is one account's terms as the store holds them.
type AccountTermsRow struct {
	ID, Company                         int64
	Currency, Point, DLRMode            string
	Prepaid                             bool
	MOSellCard, MTSellCard, DLRSellCard int64
}

// RateTables is the rate cards, their rates, the rules and the vendors, as
// pricing reads them into memory.
type RateTables interface {
	// RateMark fingerprints the tables pricing reads: a count and the last
	// change of each, and the store's clock.
	RateMark(ctx context.Context) (RateMark, error)
	// ReadRates reads them all. closedSince, when not zero, also brings the
	// rate versions replaced or removed since then.
	ReadRates(ctx context.Context, closedSince time.Time) (RateRead, error)
	// RatePrices reads the price of the rate versions named, with their
	// cards' currency.
	RatePrices(ctx context.Context, ids []int64) ([]RatePriceRow, error)
}

// RateMark is RateTables.RateMark's answer: Now and each Last as the store
// writes them ("" with ok false when NULL).
type RateMark struct {
	Now    string
	NowOK  bool
	Count  [5]int64
	Last   [5]string
	LastOK [5]bool
}

// RateRead is every table pricing keeps.
type RateRead struct {
	Cards     []RateCardRow
	Companies []CompanyMoneyRow
	Rates     []RateRow
	Routes    []RouteCardsRow
	Vendors   []VendorCardsRow
}

// RateCardRow is one enabled rate card.
type RateCardRow struct {
	ID       int64
	Currency string
	Owner    int64
}

// CompanyMoneyRow is a company's currency and its rate to the base currency.
type CompanyMoneyRow struct {
	ID       int64
	Currency string
	BaseRate float64
	BaseCRP  string
}

// RateRow is one rate version (zero times = not set).
type RateRow struct {
	ID, Card             int64
	MCC, MNC, Prefix     string
	PricePerPart, TaxPct float64
	EffectiveFrom        time.Time
	ConvRate             float64
	CRP                  string
	ValidFrom, ValidTo   time.Time
}

// RouteCardsRow is one rule's cards, vendor, direction and send plan ("" =
// none).
type RouteCardsRow struct {
	ID, Buy, Sell, DLRBuy, DLRSell, Vendor int64
	Direction, SendPlan                    string
}

// VendorCardsRow is one vendor's own buying cards and its company.
type VendorCardsRow struct {
	ID, MT, MO, DLR, Company int64
}

// RatePriceRow is one rate version's price per part, tax and card currency,
// as written.
type RatePriceRow struct {
	ID                   int64
	PricePerPart, TaxPct float64
	Currency             string
}

// FXRates is the exchange-rate providers and the rates they gave.
type FXRates interface {
	// FXProviders is every active provider (a row that cannot be read is
	// left out).
	FXProviders(ctx context.Context) ([]FXProviderRow, error)
	// StoredRate is the rate a provider published for a pair that ran out
	// less than staleSec ago; fresh = it has not run out.
	StoredRate(ctx context.Context, endpointID int64, from, to string, staleSec int64) (rate float64, fresh bool, err error)
	// SaveRate publishes a rate for ttlSec.
	SaveRate(ctx context.Context, endpointID int64, from, to string, rate float64, ttlSec int64) error
	// RateRequest is a provider's rate request endpoint, as written.
	RateRequest(ctx context.Context, endpointID int64) (FXRequestRow, error)
	// RecentRates is every rate published in the last keepSec, newest first
	// (a row that cannot be read is left out).
	RecentRates(ctx context.Context, keepSec int64) ([]FXRateRow, error)
}

// FXProviderRow is one provider: its type and config as written.
type FXProviderRow struct {
	ID          int64
	Name, Type  string
	Config      string
	CacheTTLSec int
}

// FXRequestRow is a rate request endpoint; the JSON columns as written.
type FXRequestRow struct {
	ID                                  int64
	Name, URL, Method                   string
	Headers, Auth, Config, BodyTemplate string
	TimeoutMS, CacheTTLSec              int
}

// FXRateRow is one published rate; Stale = it has run out.
type FXRateRow struct {
	From, To string
	Rate     float64
	Stale    bool
}

// BillingConfig is everything billing reads from the config store.
type BillingConfig interface {
	BillingTerms
	RateTables
	FXRates
}

// BillingStore is billing's part of the install's SQL database: the
// balance of record, the usage books and what prices are read from.
type BillingStore interface {
	CompanyBalances
	UsageBooks
	BillingConfig
}

// BalanceBook is the balance of record and the billing queue kept in ONE
// SQL database, for an install with no Redis (plan of 29 Oct 2026, step 3):
// a balance move and the money events that say why are one transaction,
// both or neither. It stands where LiveBalances + BillingQueue stand with
// Redis; the balance it moves is the balance of record itself
// (companies.balance), so there is no counter to seed, lose or write back.
type BalanceBook interface {
	// Move moves a company's balance by units (positive = taken, negative =
	// given back) and puts adds on the billing queue, in one transaction.
	// refuse: a move that takes units and would leave the balance below
	// zero is not made and nothing is put (ok=false). left is the balance
	// after (ledger units; 0 when nothing moved).
	Move(ctx context.Context, company, units int64, refuse bool, adds []LaneAdd) (ok bool, left int64, err error)
	// MoveMany moves many companies' balances (never refused: later
	// moments) and puts adds, in one transaction. The answers are in the
	// order the moves were asked, each the balance after that move.
	MoveMany(ctx context.Context, moves []Deduction, adds []LaneAdd) ([]Deducted, error)
	// Unsent says a failed call certainly changed nothing (refused, or
	// rolled back before its commit), so trying it again cannot do it twice.
	Unsent(err error) bool
}

// Billing is what OpenBilling built: each part on the DAL picked for it.
// With Redis set, Live and Queue are there and Book is nil; with none, Live
// is nil and Book and Queue are the SQL database's (when its DAL serves
// them). Earlier is the main Redis's queues of earlier builds, booked by the
// db-worker until they are empty (Redis only).
type Billing struct {
	Store   BillingStore
	Live    LiveBalances
	Queue   BillingQueue
	Earlier BillingQueue
	// Separate says the queue Redis is another server than the main one
	// (config.QueueRedisSeparate): only then are the main Redis's sell and
	// buy queues an earlier build's.
	Separate bool
	// Book is the balance of record and the billing queue in one SQL
	// database, when no Redis is set (step 3); Queue is then the same
	// queue.
	Book BalanceBook
}

// BillingOn says which DAL serves the live balances and the billing queues:
// Redis when the settings set one (config.RedisEnabled), as every build
// since 23 Jul 2026 does; "" when none is set (no live balances: the
// balance of record and, where its DAL serves one, the SQL billing queue —
// OpenBilling's Book).
func BillingOn(cfg config.Config) config.Backend {
	if cfg.RedisEnabled() {
		return config.BackendRedis
	}
	return ""
}

// OpenBilling opens the billing family: the store on the install's SQL
// database (the config store), the live balances on the main Redis and the
// queues on the deployment's queue Redis when BillingOn picks Redis.
func OpenBilling(cfg config.Config) (*Billing, error) {
	b := &Billing{Separate: cfg.QueueRedisSeparate()}
	sqlOn := SQL(cfg)
	d, ok := Lookup(sqlOn)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", sqlOn, Registered())
	}
	if d.BillingStore == nil {
		return nil, fmt.Errorf("dal: billing on %s: %w", sqlOn, ErrNotServed)
	}
	s, err := d.BillingStore(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: billing on %s: %w", sqlOn, err)
	}
	b.Store = s
	on := BillingOn(cfg)
	if on == "" {
		// No Redis (step 3): the balance moves and the money events in the
		// SQL database, one transaction, when its DAL serves them; else the
		// store alone, as before.
		if d.BalanceBook == nil || d.BillingQueue == nil {
			return b, nil
		}
		if b.Book, err = d.BalanceBook(cfg); err != nil {
			return nil, fmt.Errorf("dal: balance book on %s: %w", sqlOn, err)
		}
		if b.Queue, err = d.BillingQueue(cfg, false); err != nil {
			return nil, fmt.Errorf("dal: billing queue on %s: %w", sqlOn, err)
		}
		return b, nil
	}
	r, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if r.LiveBalances == nil || r.BillingQueue == nil {
		return nil, fmt.Errorf("dal: live balances on %s: %w", on, ErrNotServed)
	}
	if b.Live, err = r.LiveBalances(cfg); err != nil {
		return nil, fmt.Errorf("dal: live balances on %s: %w", on, err)
	}
	if b.Queue, err = r.BillingQueue(cfg, false); err != nil {
		return nil, fmt.Errorf("dal: billing queue on %s: %w", on, err)
	}
	if b.Earlier, err = r.BillingQueue(cfg, true); err != nil {
		return nil, fmt.Errorf("dal: billing queue on %s: %w", on, err)
	}
	return b, nil
}
