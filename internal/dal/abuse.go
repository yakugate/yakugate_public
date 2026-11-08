package dal

import (
	"context"
	"fmt"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// What the abuse monitor (P10, internal/process/abusemonitor) reads and
// writes, moved here on 30 Oct 2026 (the DAL plan): the abuse rules and each
// account's overrides, the facts of accounts and companies the signals need,
// the audit rows that keep an action's cooldown and the one transaction that
// acts, the reputation's inputs and scores, and the windows of messages read
// from the day tables. How a signal is judged, how a score is made and what
// is done about it stayed in the role.

// ---- the rules (config store) ----

// AbuseRuleRow is one enabled gateway rule: its signal and its params as
// stored (JSON).
type AbuseRuleRow struct {
	Signal string
	Params []byte
}

// AbuseOverrideRow is one account's override of a rule (config/0106): its
// params as stored (an empty JSON object when none) and whether it switches
// the rule off for that account.
type AbuseOverrideRow struct {
	Signal   string
	Account  int64
	Params   []byte
	Disabled bool
}

// AbuseRules is the abuse rules and the per-account overrides. A row that
// cannot be read is left out, as the role always did.
type AbuseRules interface {
	// Enabled is every gateway rule that is switched on.
	Enabled(ctx context.Context) ([]AbuseRuleRow, error)
	// Overrides is every account's override. A store older than the table
	// answers its error (1146), which the role takes as "none".
	Overrides(ctx context.Context) ([]AbuseOverrideRow, error)
}

// ---- accounts, companies, senders, opt-outs (config store) ----

// AccountCompany is an account and its company (0 = none).
type AccountCompany struct {
	Account, Company int64
}

// AccountMoney is an account with its company's balance currency, its base
// conversion rate and its third-party rate id ("" / 0 when not set).
type AccountMoney struct {
	Account, Company int64
	Currency         string
	BaseRate         float64
	RateProvider     string
}

// AccountCount is a count per account.
type AccountCount struct {
	Account int64
	N       int
}

// SenderAccount is a sender id held by an account.
type SenderAccount struct {
	Sender  string
	Account int64
}

// AccountSenderList is an account's own allowed sender list, as stored
// (a JSON array).
type AccountSenderList struct {
	Account int64
	List    []byte
}

// AbuseAccounts is what the signals read of accounts and companies: which
// company an account is in, its currency, the protected names and who may
// use them, and its STOP opt-outs.
type AbuseAccounts interface {
	// Companies is every account and its company.
	Companies(ctx context.Context) ([]AccountCompany, error)
	// Unenforced is every account whose templates are not checked at accept
	// (enforce_template_check=0), with its company.
	Unenforced(ctx context.Context) ([]AccountCompany, error)
	// Money is every account with its company's currency and rates.
	Money(ctx context.Context) ([]AccountMoney, error)
	// ProtectedNames is every sender id marked protected in message text,
	// upper case and trimmed.
	ProtectedNames(ctx context.Context) ([]string, error)
	// ApprovedSenders is every approved sender id held by an account, upper
	// case and trimmed.
	ApprovedSenders(ctx context.Context) ([]SenderAccount, error)
	// AllowedSenderLists is every account that has its own sender list.
	AllowedSenderLists(ctx context.Context) ([]AccountSenderList, error)
	// OptOuts is each account's STOP opt-outs (blocklist, source 'unsub',
	// on) made or renewed in the last windowMin minutes.
	OptOuts(ctx context.Context, windowMin int) ([]AccountCount, error)
}

// ---- acting (config store) ----

// AbuseTarget is the account an action is about, as the action reads it.
type AbuseTarget struct {
	Username    string
	Deployments string // the account's per-deployment rates, as stored ("" = none)
	ThrottleTPS NullInt64
	Disabled    bool
	CompanyID   int64 // 0 = none
	CompanyName string
}

// AbuseChange is what an action does to the account beside its audit row.
type AbuseChange int

const (
	// ChangeNone writes the audit row only (a warning).
	ChangeNone AbuseChange = iota
	// ChangeThrottle sets the account's abuse throttle to ThrottleTPS.
	ChangeThrottle
	// ChangeSuspend switches the account off.
	ChangeSuspend
)

// AbuseAct is one action: the audit row and the change of the account.
type AbuseAct struct {
	Account     int64
	CompanyID   int64  // 0 = none: stored as NULL
	CompanyName string // "" = none: stored as NULL
	Action      string // abuse.warn, abuse.throttle, abuse.suspend
	EntityID    string
	Label       string // the account's username
	Detail      string // JSON
	Change      AbuseChange
	ThrottleTPS int
}

// AuditRowError is what Act answers when the audit row could not be
// written: the account was not changed.
type AuditRowError struct{ Err error }

func (e *AuditRowError) Error() string { return e.Err.Error() }
func (e *AuditRowError) Unwrap() error { return e.Err }

// AbuseActions is the audit trail an action's cooldown is read from, the
// account an action reads, and the action itself.
type AbuseActions interface {
	// Recent counts the account's audit rows of action in the last minutes.
	// bySignal counts only the rows of that signal's rule (a warning).
	Recent(ctx context.Context, account int64, action string, minutes int, bySignal bool, signal string) (int, error)
	// Target reads the account an action is about.
	Target(ctx context.Context, account int64) (AbuseTarget, error)
	// Act writes the audit row and makes the change in ONE transaction, the
	// row first: both are there or neither. A failed row answers
	// *AuditRowError.
	Act(ctx context.Context, a AbuseAct) error
}

// ---- reputation (config store and hot store) ----

// ReputationResetRow is one account's reset (config/0143), as text on both
// clocks it is compared on: UTC, and the config store's session clock.
type ReputationResetRow struct {
	Account     int64
	UTC, Config string
}

// AccountAge is an account and its age in whole days.
type AccountAge struct {
	Account int64
	AgeDays NullInt64
}

// AccountCreated is an account and when it was made.
type AccountCreated struct {
	Account int64
	Created NullTime
}

// ActionCount is how many audit rows of one action an account has.
type ActionCount struct {
	Account int64
	Action  string
	N       int
}

// ReputationData is what the daily reputation score is made from and where
// it is kept. A grouped read answers each account's figures in the order
// its query names them; a read of one account answers its figures.
type ReputationData interface {
	// Resets is every reset of the last 90 days.
	Resets(ctx context.Context) ([]ReputationResetRow, error)
	// Clock is the config store's own UTC clock, as text.
	Clock(ctx context.Context) (NullString, error)
	// Pending is every account whose reset is not in its score yet, with
	// its age.
	Pending(ctx context.Context) ([]AccountAge, error)
	// WriteScore stores one account's score; the reset columns are left.
	WriteScore(ctx context.Context, account int64, score int, reasons, inputs, stamp string) error
	// LastComputed is when a score was last written.
	LastComputed(ctx context.Context) (NullTime, error)
	// Accounts is every account and when it was made.
	Accounts(ctx context.Context) ([]AccountCreated, error)
	// MonitorActions is the abuse monitor's own actions of the last 90 days.
	MonitorActions(ctx context.Context) ([]ActionCount, error)
	// MonitorActionsSince is one account's, since cutConfig (config clock).
	MonitorActionsSince(ctx context.Context, account int64, cutConfig string) ([]ActionCount, error)
	// Delivery is delivered, final and submitted parts per account, last 30
	// days (hot store summaries).
	Delivery(ctx context.Context) (map[int64][]int, error)
	// DeliverySince is one account's, since cutUTC.
	DeliverySince(ctx context.Context, account int64, cutUTC string) (delivered, final, sent int, err error)
	// Lookups is numbers looked up and numbers unknown per account, last 30
	// days.
	Lookups(ctx context.Context) (map[int64][]int, error)
	// LookupsSince is one account's, since cutUTC.
	LookupsSince(ctx context.Context, account int64, cutUTC string) (lookedUp, unknown int, err error)
	// OptOuts is STOP opt-outs per account, last 30 days.
	OptOuts(ctx context.Context) (map[int64][]int, error)
	// OptOutsSince is one account's, since cutConfig.
	OptOutsSince(ctx context.Context, account int64, cutConfig string) (int, error)
	// Refusals is parts refused for spam or a blocked number per account in
	// one rejects table, last 30 days.
	Refusals(ctx context.Context, table string) (map[int64][]int, error)
	// RefusalsSince is one account's in one rejects table, since cutUTC.
	RefusalsSince(ctx context.Context, table string, account int64, cutUTC string) (int, error)
}

// ---- the message windows (hot store day tables) ----

// WindowCount is an account's messages in a window and how many different
// numbers they went to.
type WindowCount struct {
	Account    int64
	Msgs, Uniq int
}

// PrefixCount is an account's messages to one number prefix.
type PrefixCount struct {
	Account int64
	Prefix  string
	Msgs    int
}

// SpendRow is an account's parts priced by one rate version at one
// conversion rate (0 rate = none; fx 1 = the company's own currency).
type SpendRow struct {
	Account, Rate int64
	FX, Parts     float64
}

// LookupCount is an account's numbers the HLR checked and how many it did
// not know.
type LookupCount struct {
	Account           int64
	Misses, Consulted int
}

// EncodingCount is an account's messages in one encoding ("" = not stored).
type EncodingCount struct {
	Account  int64
	Encoding string
	N        int
}

// SampledMessage is one message as the content signals read it, its text
// as stored (sealed when the store seals it).
type SampledMessage struct {
	Account                   int64
	Body, Destination, Sender string
}

// MessageWindows is the windows of messages the signals read from a day
// table of outgoing messages (mt_<company>_<YYYYMMDD>, or a legacy
// records_ table). A window is the last windowMin minutes; a baseline the
// sinceMin minutes before it up to windowMin ago. A message log is read
// through its time key; one made before the key existed is read without it
// and remembered. A row that cannot be read is left out, as the role always
// did. The reads go to the hot store's read instance when it has one.
type MessageWindows interface {
	// Counts is each account's messages and different numbers.
	Counts(ctx context.Context, table string, windowMin int) ([]WindowCount, error)
	// Prefixes is each account's messages per number prefix of prefixLen.
	Prefixes(ctx context.Context, table string, prefixLen, windowMin int) ([]PrefixCount, error)
	// Distinct counts one account's different numbers over several tables.
	Distinct(ctx context.Context, tables []string, account int64, windowMin int) (int, error)
	// Spend is each account's parts by rate version and conversion rate,
	// in the window (sinceMin 0) or the baseline.
	Spend(ctx context.Context, table string, windowMin, sinceMin int) ([]SpendRow, error)
	// Messages is each account's messages, in the window (sinceMin 0) or
	// the baseline.
	Messages(ctx context.Context, table string, windowMin, sinceMin int) ([]AccountCount, error)
	// Lookups is each account's numbers the HLR checked, in the window.
	Lookups(ctx context.Context, table string, windowMin int) ([]LookupCount, error)
	// Encodings is each account's messages per encoding, in the window
	// (sinceMin 0) or the baseline.
	Encodings(ctx context.Context, table string, windowMin, sinceMin int) ([]EncodingCount, error)
	// Sent is the messages of the listed accounts in the window.
	Sent(ctx context.Context, table string, windowMin int, accounts []int64) ([]AccountCount, error)
	// Sample is the newest perAcct messages of each account accepted between
	// fromMin and toMin minutes ago (toMin 0 = now), at most limit rows;
	// only the listed accounts when any are listed.
	Sample(ctx context.Context, table string, fromMin, toMin int, accounts []int64, perAcct, limit int) ([]SampledMessage, error)
}

// AbuseData is what OpenAbuseData built: the rules, accounts, actions and
// reputation on the config store's one pool, the windows (and the
// reputation's summaries) on the hot store's.
type AbuseData struct {
	Rules      AbuseRules
	Accounts   AbuseAccounts
	Actions    AbuseActions
	Reputation ReputationData
	Windows    MessageWindows
}

// OpenAbuseData opens the abuse monitor's data on the install's SQL
// database (SQL): the config and hot stores' pools of this process.
func OpenAbuseData(cfg config.Config) (*AbuseData, error) {
	on := SQL(cfg)
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.AbuseData == nil {
		return nil, fmt.Errorf("dal: abuse data on %s: %w", on, ErrNotServed)
	}
	a, err := d.AbuseData(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: abuse data on %s: %w", on, err)
	}
	return a, nil
}
