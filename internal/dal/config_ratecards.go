package dal

import (
	"context"
	"time"
)

// RateCards is rate cards, their rates (rate_card_rates) and the notices of
// a sell card's change (rate_change_notices).
//
// A list read (RateCardList, RateList, and Operators' FindNetworkRows and
// NetworkRangeRows) answers a row that would not scan, or a read cut short,
// as a RowsError — with the rows read so far when the read was cut short —
// so a caller can word it apart from the statement's own failure.
type RateCards interface {
	// RateCardNameTaken says a card with this name is there.
	RateCardNameTaken(ctx context.Context, name string) (bool, error)
	// RateCardCompany is a company's name and type ("" when none), for the
	// card about to be made for it. ErrNoRows: no such company.
	RateCardCompany(ctx context.Context, companyID int64) (name, typ string, err error)
	// InsertRateCard writes a new card made from the console.
	InsertRateCard(ctx context.Context, c RateCardInsert) (int64, error)
	// RateCardKind is a card's kind (sell | buy).
	RateCardKind(ctx context.Context, id int64) (string, error)
	// RateCardKindType is a card's kind and its type ("" when none).
	RateCardKindType(ctx context.Context, id int64) (kind, typ string, err error)
	// SettleDueRateSchedules stamps valid_to on every price whose scheduled
	// successor's day has come.
	SettleDueRateSchedules(ctx context.Context) error
	// OpenRateForUpdate is the newest open price of one destination on a
	// card, locked. ErrNoRows: there is none.
	OpenRateForUpdate(ctx context.Context, d RateDestination) (OpenRate, error)
	// RateSnapshotRow is one price as the portal's rate email reads it.
	RateSnapshotRow(ctx context.Context, id int64) (RateSnapshot, error)
	// InsertRate writes one price version.
	InsertRate(ctx context.Context, r RateInsert) (int64, error)
	// CorrectWaitingRate changes a price that has not started yet, in place.
	CorrectWaitingRate(ctx context.Context, id int64, price, tax string, effective NullTime) error
	// CloseRatesSupersededBy closes the open price a starting one replaces.
	CloseRatesSupersededBy(ctx context.Context, successor int64) error
	// MaxRateVersion is the highest version of one destination's chain.
	MaxRateVersion(ctx context.Context, d RateDestination) (int, error)
	// SupersedeRate names a price's successor; closeNow also closes it now
	// (else it stays open until the successor's day).
	SupersedeRate(ctx context.Context, oldID, newID int64, closeNow bool) error
	// RateForClose is what closing a price looks at.
	RateForClose(ctx context.Context, cardID, rateID int64) (RateCloseRow, error)
	// CloseRateNow closes one price now.
	CloseRateNow(ctx context.Context, rateID int64) error
	// ClearSuccessor hands a destination back to the price in force when its
	// waiting successor is cancelled.
	ClearSuccessor(ctx context.Context, successor int64) error
	// LiveAnyDestinationRates counts a card's any-destination prices in force.
	LiveAnyDestinationRates(ctx context.Context, cardID int64) (int, error)
	// RateCardNotice is a card's kind and whether its customers are told of
	// a change.
	RateCardNotice(ctx context.Context, cardID int64) (kind string, notify int, err error)
	// InsertRateNotice writes one pending customer notice of a price change.
	InsertRateNotice(ctx context.Context, n RateNotice) error
	// RateCardList is every card with its prices in force, at most limit.
	RateCardList(ctx context.Context, limit int) ([]RateCardListRow, error)
	// RateCardColumnUsers counts the rows of table whose columns (any of
	// cols) name the card.
	RateCardColumnUsers(ctx context.Context, table string, cols []string, id int64) (int, error)
	// RateCardPlanUsers counts the rules whose plan names the card as a
	// vendor's own buy or sell card.
	RateCardPlanUsers(ctx context.Context, id int64) (int, error)
	// RateList is a card's prices: the open ones, or (history) every one.
	RateList(ctx context.Context, cardID int64, history bool) ([]RateListRow, error)
	// SetRateCardDisabled switches a card off (1) or on (0); n = rows changed.
	SetRateCardDisabled(ctx context.Context, id int64, disabled int) (n int64, err error)
	// RateCardPriceCount counts every price of a card, retired ones too.
	RateCardPriceCount(ctx context.Context, id int64) (int, error)
	// DeleteRateCardRow removes a card; n = rows removed.
	DeleteRateCardRow(ctx context.Context, id int64) (n int64, err error)
	// RateCardOwner is a card's company (0 = none) and name.
	RateCardOwner(ctx context.Context, cardID int64) (company int64, name string, err error)
	// SellCardAccountCompany is an account's company (0 = none).
	SellCardAccountCompany(ctx context.Context, accountID int64) (int64, error)
	// SetAccountRateCard puts a card in one of an account's card columns.
	SetAccountRateCard(ctx context.Context, column string, cardID, accountID int64) error
	// SetVendorRateCard puts a card in one of a vendor's card columns.
	SetVendorRateCard(ctx context.Context, column string, cardID, vendorID int64) error
}

// RateCardInsert is a card the console makes.
type RateCardInsert struct {
	Name, Kind, Type, Currency string
	CompanyID                  int64
	Notify                     int
}

// RateDestination is one destination's chain on a card; an empty field
// (spaces trimmed) is matched as NULL.
type RateDestination struct {
	CardID           int64
	MCC, MNC, Prefix string
}

// OpenRate is the newest open price of a destination.
type OpenRate struct {
	ID         int64
	Version    int
	Price, Tax string
	Effective  NullTime
	Successor  NullInt64
}

// RateSnapshot is one price's columns as its notice records them.
type RateSnapshot struct {
	MCC, MNC, Prefix NullString
	Price, Tax       string
	Unit, Conv       string
	Effective        NullTime
	Version          int
}

// RateInsert is one price version written; an empty destination field
// (spaces trimmed) is written NULL, and Effective NULL starts it now.
type RateInsert struct {
	CardID           int64
	Version          int
	MCC, MNC, Prefix string
	Price, Tax       string
	ValidFrom        time.Time
	Effective        NullTime
}

// RateCloseRow is what closing a price looks at.
type RateCloseRow struct {
	MCC, MNC, Prefix  NullString
	Effective, Closed NullTime
}

// RateNotice is one rate_change_notices row; Before and After are JSON text
// (NULL when not Valid), Effective NULL = now.
type RateNotice struct {
	CardID, RateID int64
	Change         string
	Before, After  NullString
	Effective      NullTime
}

// RateCardListRow is one card of the list, with its prices in force and
// its any-destination prices in force.
type RateCardListRow struct {
	ID                                  int64
	Name, Kind, Type, Currency, Company string
	Disabled                            int
	Prices, Wildcard                    int
}

// RateListRow is one price of a card's list.
type RateListRow struct {
	ID                int64
	Version           int
	MCC, MNC, Prefix  string
	Price, Tax        string
	Effective, Closed NullTime
	Replaces          int64
}

// Operators is the networks and their number ranges (operators).
type Operators interface {
	// OperatorCounts is how many networks and ranges there are.
	OperatorCounts(ctx context.Context) (networks, ranges int, err error)
	// FindNetworkRows lists networks matching q (empty = all), at most limit.
	FindNetworkRows(ctx context.Context, q string, limit int) ([]NetworkRow, error)
	// NetworkSummary is one network over its ranges (Ranges 0 = none).
	NetworkSummary(ctx context.Context, mcc, mnc string) (NetworkRow, error)
	// NetworkSpelling is the mnc a network is written with, among the three
	// spellings given (the one with most ranges). ErrNoRows: none.
	NetworkSpelling(ctx context.Context, mcc string, forms [3]string) (string, error)
	// NetworkRangeCount counts a network's ranges starting with prefix.
	NetworkRangeCount(ctx context.Context, mcc, mnc, prefix string) (int, error)
	// NetworkRangeRows lists a network's ranges starting with prefix, at
	// most limit.
	NetworkRangeRows(ctx context.Context, mcc, mnc, prefix string, limit int) ([]NumberRangeRow, error)
	// OverlappingRange is a range of this length that overlaps from..to.
	// ErrNoRows: none.
	OverlappingRange(ctx context.Context, length int, from, to string) (RangeOverlap, error)
	// InsertRange writes one range (Region "" = NULL).
	InsertRange(ctx context.Context, r NumberRangeRow, shardKey string) (int64, error)
	// SetNetworkColumn sets one column on every range of a network (value
	// "" = NULL); n = rows changed.
	SetNetworkColumn(ctx context.Context, column, value, mcc, mnc string) (n int64, err error)
	// RangeByID is one range. ErrNoRows: none.
	RangeByID(ctx context.Context, id int64) (NumberRangeRow, error)
	// DeleteRangeRow removes one range.
	DeleteRangeRow(ctx context.Context, id int64) error
	// RoutesMatchingNetwork counts the rules whose networks name mccmnc.
	RoutesMatchingNetwork(ctx context.Context, mccmnc string) (int, error)
	// OpenPricesForNetwork counts the open prices set for a network under
	// any of the three spellings of its mnc.
	OpenPricesForNetwork(ctx context.Context, mcc string, forms [3]string) (int, error)
	// DeleteNetworkRows removes every range of a network; n = rows removed.
	DeleteNetworkRows(ctx context.Context, mcc, mnc string) (n int64, err error)
}

// NetworkRow is one network summarised over its ranges.
type NetworkRow struct {
	MCC, MNC, CCC         string
	Name, Country, Region string
	Ranges                int
}

// NumberRangeRow is one operators row.
type NumberRangeRow struct {
	ID                    int64
	From, To              string
	MCC, MNC, CCC         string
	Name, Country, Region string
}

// RangeOverlap is the range another one overlaps: its id, its network
// ("name (mcc-mnc)") and its two ends.
type RangeOverlap struct {
	ID       int64
	Other    string
	From, To string
}
