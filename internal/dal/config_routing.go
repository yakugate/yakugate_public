package dal

import (
	"context"
	"time"
)

// Routing is the routing rules (routes) and the plans they make, with what
// a rule's checks and screens read beside them: the accounts' and vendors'
// placement, the cards a rule names, the operators table's networks and
// number series.
type Routing interface {
	// RuleRows is an account's rules of one direction, by priority then id;
	// lock takes them FOR UPDATE (inside a transaction).
	RuleRows(ctx context.Context, accountID int64, direction string, lock bool) ([]RuleRow, error)
	// RuleByID is one rule. ErrNoRows: there is none.
	RuleByID(ctx context.Context, id int64) (RuleRow, error)
	// OutgoingRuleRows is every account's outgoing rule, by account, priority
	// and id.
	OutgoingRuleRows(ctx context.Context) ([]RuleRow, error)
	// PlanAccount is what a plan shows of its account. ErrNoRows: none.
	PlanAccount(ctx context.Context, id int64) (PlanAccountRow, error)
	// PlanAccounts is every account as the plans list shows it, by username.
	PlanAccounts(ctx context.Context) ([]PlanListAccount, error)
	// PlanVendors is every vendor's cid, transport and status.
	PlanVendors(ctx context.Context) ([]PlanVendorRow, error)
	// PlanCardNames is every rate card's name by id; a row that cannot be
	// read is left out.
	PlanCardNames(ctx context.Context) (map[int64]string, error)
	// SetRulePriority writes one rule's priority.
	SetRulePriority(ctx context.Context, id int64, priority int) error
	// RuleAccount is a rule's account (0 = none). ErrNoRows: no such rule.
	RuleAccount(ctx context.Context, id int64) (int64, error)
	// OutgoingRuleAccount is RuleAccount for an outgoing rule only.
	OutgoingRuleAccount(ctx context.Context, id int64) (int64, error)
	// EachOtherMORule calls fn with every incoming rule of the other accounts
	// (not accountID, not ignoreID), in the order read, while it reads them;
	// an error from fn ends the read and is answered.
	EachOtherMORule(ctx context.Context, accountID, ignoreID int64, fn func(MORuleRow) error) error
	// RuleAccountName is an account's username.
	RuleAccountName(ctx context.Context, id int64) (string, error)
	// RuleList is the console's list of rules, by direction, priority and
	// id, at most limit.
	RuleList(ctx context.Context, limit int) ([]RuleListRow, error)
	// RuleVendorCIDs is every vendor's cid by id; a row that cannot be read
	// is left out.
	RuleVendorCIDs(ctx context.Context) (map[int64]string, error)
	// SetRuleDisabled switches a rule off (1) or on (0); n = rows changed.
	SetRuleDisabled(ctx context.Context, id int64, disabled int) (n int64, err error)
	// DeleteRuleRow removes a rule; n = rows removed.
	DeleteRuleRow(ctx context.Context, id int64) (n int64, err error)
	// RuleCards is a rule's direction and its four card columns (0 = none).
	// ErrNoRows: no such rule.
	RuleCards(ctx context.Context, id int64) (RuleCardsRow, error)
	// RuleCardName is a rate card's name.
	RuleCardName(ctx context.Context, cardID int64) (string, error)
	// RuleShape is a rule's direction, send plan ("" = none) and account (0 =
	// none).
	RuleShape(ctx context.Context, id int64) (direction, plan string, accountID int64, err error)
	// RuleCardKind is a rate card's kind and type ("" when none).
	RuleCardKind(ctx context.Context, cardID int64) (kind, typ string, err error)
	// SetRuleCard puts a card in one of a rule's card columns (0 = NULL).
	SetRuleCard(ctx context.Context, column string, cardID, ruleID int64) error
	// RuleAccountDeployments is an account's deployments as written ("" when
	// none).
	RuleAccountDeployments(ctx context.Context, accountID int64) (string, error)
	// RuleVendorDeployments is a vendor's deployments as written ("" when
	// none).
	RuleVendorDeployments(ctx context.Context, vendorID int64) (string, error)
	// RulePriorityTaken is another rule of the account and direction (MT or
	// MO) at this priority, not ignoreID. ErrNoRows: none.
	RulePriorityTaken(ctx context.Context, direction string, accountID int64, priority int, ignoreID int64) (id int64, name string, err error)
	// MaxIncomingPriority is the highest priority of an account's incoming
	// rules (not Valid when it has none).
	MaxIncomingPriority(ctx context.Context, accountID int64) (NullInt64, error)
	// InsertRule writes a new rule made from the console.
	InsertRule(ctx context.Context, r RuleWrite) (int64, error)
	// RuleStoredPlan is a rule's send plan ("" = none), rule type and weight.
	RuleStoredPlan(ctx context.Context, id int64) (plan, routeType string, weight int, err error)
	// UpdateRule writes every column of a rule a save changes.
	UpdateRule(ctx context.Context, id int64, r RuleWrite) error
	// RuleCardCompany is a rate card's company (0 = none).
	RuleCardCompany(ctx context.Context, cardID int64) (int64, error)
	// RuleVendorCID is a vendor's cid.
	RuleVendorCID(ctx context.Context, vendorID int64) (string, error)
	// OperatorCatalog is one row per network spelling of the operators table:
	// its first range and how many ranges it has.
	OperatorCatalog(ctx context.Context) ([]OperatorCatalogRow, error)
	// NumberSeriesBelow is the 60 ranges starting at or below p, nearest
	// first.
	NumberSeriesBelow(ctx context.Context, p string) ([]NumberSeriesRow, error)
	// NumberSeriesLike is up to 300 ranges whose start is LIKE pattern, in
	// number order.
	NumberSeriesLike(ctx context.Context, pattern string) ([]NumberSeriesRow, error)
}

// RowsError is an error met while reading the rows a list answered (a row
// that would not scan, or the read cut short), as against the statement's
// own failure. Its text is the error's own: a caller that words the two
// differently still can, as it did before the move.
type RowsError struct{ Err error }

func (e RowsError) Error() string { return e.Err.Error() }
func (e RowsError) Unwrap() error { return e.Err }

// RuleRow is one routes row as provision reads it (the COALESCEs of the
// statement applied; the four retry columns as stored).
type RuleRow struct {
	ID                             int64
	Name                           string
	Priority                       int
	RouteType                      string
	Weight                         int
	Direction                      string
	AccountID                      int64
	Networks                       string
	AcceptUnknown                  int
	Sender, Receiver, Keyword, Pri string
	VendorID                       int64
	Plan                           string
	Rounds, First, MaxWait         NullInt64
	Backoff                        NullString
	Sell, Buy, DLRSell, DLRBuy     int64
	Disabled                       int
}

// PlanAccountRow is what a plan shows of its account; Deployments as
// written, HLRCheck and LossCheck 0 or not.
type PlanAccountRow struct {
	Username, Company, Deployments, DefaultSender string
	CompanyID                                     int64
	HLRCheck, LossCheck                           int
}

// PlanListAccount is one account of the plans list.
type PlanListAccount struct {
	ID          int64
	Username    string
	CompanyID   int64
	Company     string
	Deployments string
}

// PlanVendorRow is what the plan's words need of a vendor.
type PlanVendorRow struct {
	ID                     int64
	CID, Transport, Status string
}

// MORuleRow is another account's incoming rule, as the short-code check
// reads it (the patterns and networks as stored).
type MORuleRow struct {
	ID, AccountID, VendorID int64
	Name                    string
	Receiver, Keyword, Nets string
}

// RuleListRow is one line of the console's rule list.
type RuleListRow struct {
	ID                               int64
	Name, Direction                  string
	Priority                         int
	Account, Vendor                  string
	Receiver, Sender, Keyword        string
	Disabled                         int
	Transport, AdapterConfig, Status string
	Plan                             string
	Global                           int
}

// RuleCardsRow is a rule's direction and its card columns (0 = none).
type RuleCardsRow struct {
	Direction                  string
	Sell, Buy, DLRSell, DLRBuy int64
}

// RuleWrite is the columns a rule's save writes. Zero ids and the patterns,
// networks and plan when not Valid are written NULL; Retry nil writes the
// four retry columns NULL. AccountID is written NULL for 0 on an insert and
// as it is on an update, as before the move. Weight is written on an update
// only.
type RuleWrite struct {
	Name, Direction, Type                    string
	Priority, Weight                         int
	AccountID, VendorID                      int64
	Receiver, Sender, Keyword, PriorityMatch NullString
	Networks                                 NullString
	Disabled, AcceptUnknown                  int
	Plan                                     NullString
	Retry                                    *RuleRetry
	Sell, Buy, DLRSell, DLRBuy               int64
}

// RuleRetry is a rule's own retries.
type RuleRetry struct {
	Rounds, FirstSec int
	Backoff          string
	MaxWaitSec       int
}

// OperatorCatalogRow is one network spelling of the operators table: its
// calling code, codes, country and name, its first range (not Valid when
// none) and how many ranges it has.
type OperatorCatalogRow struct {
	CCC, MCC, MNC, Country, Name string
	First                        NullString
	Ranges                       int
}

// NumberSeriesRow is one range of the operators table.
type NumberSeriesRow struct {
	ID                           int64
	CCC, MCC, MNC, Name, Country string
	From, To                     string
}

// ContentRules is the spam rules (spam_keywords) and the blocked numbers
// (blocklist).
type ContentRules interface {
	// SpamRuleRows is every spam rule: universal first, then by company.
	SpamRuleRows(ctx context.Context) ([]SpamRuleRow, error)
	// SpamRuleClash is another rule (not self) with the same scope, prefix,
	// kind and name, any case. ErrNoRows: none. companyID 0 and prefix ""
	// are matched as NULL.
	SpamRuleClash(ctx context.Context, companyID int64, prefix, kind, name string, self int64) (int64, error)
	// InsertSpamRule writes a new rule made from the console; patternJSON is
	// keyword_regex as JSON text.
	InsertSpamRule(ctx context.Context, r SpamRuleInsert) (int64, error)
	// SetSpamPattern writes a rule's keyword_regex (JSON text) and empties
	// its old word list.
	SetSpamPattern(ctx context.Context, id int64, patternJSON string) error
	// SetSpamName writes a rule's name (category).
	SetSpamName(ctx context.Context, id int64, name string) error
	// SetSpamAction writes a rule's action.
	SetSpamAction(ctx context.Context, id int64, action string) error
	// DeleteSpamRow removes a rule.
	DeleteSpamRow(ctx context.Context, id int64) error
	// SpamCheckAccounts is how many accounts have the spam check on, of all.
	SpamCheckAccounts(ctx context.Context) (on, total int, err error)

	// BlockCount counts the blocked numbers whose number is LIKE pattern ("" =
	// all, no filter).
	BlockCount(ctx context.Context, pattern string) (int, error)
	// BlockRows lists the blocked numbers whose number is LIKE pattern ("" =
	// all, no filter),
	// newest first, at most limit.
	BlockRows(ctx context.Context, pattern string, limit int) ([]BlockRow, error)
	// BlockByID is one blocked-number row. ErrNoRows: none.
	BlockByID(ctx context.Context, id int64) (BlockRow, error)
	// BlockTwin is a row with the same number, account, company and sender
	// (NULL-safe; 0 and "" = NULL). ErrNoRows: none.
	BlockTwin(ctx context.Context, number string, accountID, companyID int64, sender string) (int64, error)
	// BlockTwinInImport is BlockTwin as the import asks it.
	BlockTwinInImport(ctx context.Context, number string, accountID, companyID int64, sender string) (int64, error)
	// InsertBlock writes one blocked number made from the console.
	InsertBlock(ctx context.Context, b BlockInsert) (int64, error)
	// SetBlockDisabled switches a row off (1) or on (0).
	SetBlockDisabled(ctx context.Context, id int64, off int) error
	// SetBlockExpiry writes when a block ends (not Valid = never).
	SetBlockExpiry(ctx context.Context, id int64, until NullTime) error
	// DeleteBlockRow removes a row.
	DeleteBlockRow(ctx context.Context, id int64) error
	// BlockIDsFor is every row's id that blocks the number, oldest first.
	BlockIDsFor(ctx context.Context, number string) ([]int64, error)
}

// SpamRuleRow is one spam rule as stored (company 0 = every company).
type SpamRuleRow struct {
	ID                     int64
	CompanyID              int64
	Company                string
	Kind, Action, Name     string
	Prefix                 string
	PatternJSON, WordsJSON string
}

// SpamRuleInsert is a new spam rule; CompanyID 0 and Prefix "" are written
// NULL.
type SpamRuleInsert struct {
	CompanyID          int64
	Prefix             string
	Kind, Action, Name string
	PatternJSON        string
}

// BlockRow is one blocked-number row, with its account's and company's
// names ("" when none).
type BlockRow struct {
	ID                   int64
	Number               string
	AccountID, CompanyID int64
	Account, Company     string
	Sender, Source       string
	Reason               string
	Expires              NullTime
	Disabled             bool
	Created              string
}

// BlockInsert is a new blocked number; zero ids and empty texts are written
// NULL, a zero Expires too.
type BlockInsert struct {
	Number               string
	AccountID, CompanyID int64
	Sender, Source       string
	Reason               string
	Expires              time.Time
}
