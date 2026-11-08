package dal

import (
	"context"
	"time"
)

// The reference family (plan of 29 Oct 2026): what the reference cache
// (internal/refdata) reads into memory, and where it hands it on.
//
//	ReferenceData      the config store's rows the cache is built from
//	HLRNumbers         the HLR store: number shards, token revocations, and
//	                   the queue tables' depths the capacity routing reads
//	ReferenceSnapshots the snapshot in Redis every gateway loads from
//
// How a row becomes an account, a route or a rule — the parsing, the
// fallbacks, what is logged — stays in internal/refdata; so does the
// fingerprint built from TableMark. These interfaces carry the rows as the
// statements read them.
//
// A read that met an error while reading its rows (a row that would not
// scan, or the read cut short) answers a RowsError, with the rows read before
// it where the statement had any; any other error is the statement's own.

// ReferenceData is the config store's rows the reference cache is built
// from. The periodic reads go to the read instance when one is set; a
// context marked dbpool.Fresh (a forced refresh) reads the write instance,
// and so does every read for a minute after it.
type ReferenceData interface {
	// Accounts is every account with its company and dialect.
	Accounts(ctx context.Context) ([]AccountRef, error)
	// ApprovedSenders is every approved sender ID assigned to an account.
	ApprovedSenders(ctx context.Context) ([]ApprovedSender, error)
	// HLRDefaultSettings is the gateway's HLR defaults as written
	// (hlr_default_order, hlr_default_cache_ttl_days); a row that cannot be
	// read is left out. Read on the write instance.
	HLRDefaultSettings(ctx context.Context) ([]Setting, error)
	// HasVendorShortCodes says vendors has short_codes (config/0161).
	HasVendorShortCodes(ctx context.Context) bool
	// RouteVendors is every vendor as a rule needs it; withShortCodes reads
	// short_codes (else every vendor has "").
	RouteVendors(ctx context.Context, withShortCodes bool) ([]RouteVendorRef, error)
	// Routes is every routing rule, by priority.
	Routes(ctx context.Context) ([]RouteRef, error)
	// Rates is every open rate of every enabled card, with its card and the
	// card's company.
	Rates(ctx context.Context) ([]RateRef, error)
	// AutoReplace is every enabled auto-replace rule, by account and seq.
	AutoReplace(ctx context.Context) ([]AutoReplaceRef, error)
	// Spam is every spam rule, by id.
	Spam(ctx context.Context) ([]SpamRef, error)
	// Templates is every approved, enabled template, by id.
	Templates(ctx context.Context) ([]TemplateRef, error)
	// Blocks is every enabled blocked number.
	Blocks(ctx context.Context) ([]BlockRef, error)
	// CountryRules is every enabled country rule.
	CountryRules(ctx context.Context) ([]CountryRuleRef, error)
	// Now is the store's clock, as text.
	Now(ctx context.Context) (NullString, error)
	// TableMark is a table's row count and newest updated_at, and for a few
	// tables a checksum of the columns a same-second change can touch (not
	// Valid for the others).
	TableMark(ctx context.Context, table string) (n int64, last, extra NullString, err error)
	// MissingTable says err is the database's "no such table".
	MissingTable(err error) bool
}

// HLRNumbers is the HLR store (YAKU_HLR_DSN, else the queue database): the
// number shards hlr_<first4>, the JWT revocations, and the queue tables the
// capacity routing reads the depth of. Number data and revocations are
// read from the read instance when one is set.
type HLRNumbers interface {
	// Lookup is one number from its shard (hlr_<shard>, hlr.Shard); false =
	// not there (or deleted).
	Lookup(shard, msisdn string) (HLRRow, bool)
	// Prepare upgrades every shard to the cache's shape (once; a shard
	// already right is left alone).
	Prepare(ctx context.Context) error
	// Count is the rows of every shard.
	Count(ctx context.Context) (int64, error)
	// LoadSince is every row changed at or after since across the shards,
	// and the newest last_check seen. A zero since is the first full load
	// (deleted rows left out).
	LoadSince(ctx context.Context, since time.Time) ([]HLRRow, time.Time, error)
	// Revocations is every JWT id revoked and not yet expired.
	Revocations(ctx context.Context) ([]string, error)
	// QueueTables is every vendor's queue table (queue_template left out).
	QueueTables(ctx context.Context) ([]string, error)
	// QueueDepth is how many messages wait in a queue table and, when any
	// does, how old the oldest of its first thousand by id is (seconds, on
	// the store's clock).
	QueueDepth(ctx context.Context, table string) (depth, ageSec NullInt64, err error)
}

// ReferenceSnapshots is the reference snapshot in Redis: one document,
// versioned, written by the one instance holding the sync lease and read by
// every gateway; and the HLR numbers copied beside it.
type ReferenceSnapshots interface {
	// TakeLease takes the fleet-wide sync lease for holder, or renews it
	// when holder has it.
	TakeLease(ctx context.Context, holder string, ttl time.Duration) bool
	// Publish writes the snapshot, moves the version on and stamps the time,
	// in one transaction.
	Publish(ctx context.Context, snapshot []byte) error
	// Version is the snapshot's version (0 = none published yet).
	Version(ctx context.Context) (int64, error)
	// VersionText is the version as text, "0" when there is none or it
	// cannot be read.
	VersionText(ctx context.Context) string
	// Snapshot is the published snapshot.
	Snapshot(ctx context.Context) ([]byte, error)
	// HLRMark is where the last HLR copy got to ("" = none yet).
	HLRMark(ctx context.Context) (string, error)
	// PutHLR writes and removes numbers and moves the mark, in one round
	// trip.
	PutHLR(ctx context.Context, puts []HLRPut, mark string) error
}

// HLRPut is one number for PutHLR: its entry as JSON, or Delete.
type HLRPut struct {
	MSISDN string
	JSON   []byte
	Delete bool
}

// HLRRow is one number of the HLR store; CheckedAt is zero when last_check
// is NULL.
type HLRRow struct {
	MSISDN, MCC, MNC string
	// OperatorName and CCC (hlr/0003, 6 Nov 2026): the operator's name and
	// the country calling code, as a live lookup or an import gave them;
	// "" = not known.
	OperatorName, CCC string
	Ported, Deleted   bool
	CheckedAt         time.Time
}

// Setting is one gateway_settings row as text.
type Setting struct {
	Name, Value string
}

// AccountRef is one account as the reference cache reads it (the
// statement's COALESCEs applied; the 0/1 flags as read).
type AccountRef struct {
	ID                                   int64
	UserID                               NullString
	Username                             string
	JWT                                  NullString
	HTTP, SMPP                           int
	AllowedSenders, AllowedIPs           NullString
	Disabled                             int
	Expiry                               NullTime
	Spam, SpamStrict, Template, AutoRepl int
	Duplicate, HLRDB                     int
	HLRDipURL                            string
	HLRMissPolicy                        string
	BodyPrivacy                          string
	DLRChannel                           string
	DLRBindAttempts                      int
	MOChannel                            string
	MOBindAttempts                       int
	// DLRMaxAttempts and MOMaxAttempts: the most tries to give the client a
	// receipt or an MO, bind and webhook together (config/0173); 0 = none.
	DLRMaxAttempts, MOMaxAttempts   int
	NoReceipts                      bool
	MaxRecipients                   int
	Quiet                           NullString
	DedupeTTL, DefaultTTL           NullInt64
	Whitelist, SMPPConfig           NullString
	CompanyID                       int64
	CompanyDisabled, CompanyPrepaid int
	CompanyBalance                  float64
	CompanyThroughput               int
	CompanyQuota                    NullString
	CompanyTimezone                 string
	CompanyCurrency                 string
	CompanyBaseRate                 float64
	CompanyBaseCRP                  string
	LossDetection                   int
	DefaultMTSellCard               int64
	ChargePoint                     string
	DefaultDLRSellCard              int64
	DefaultHLRSellCard              int64
	ChargeFailedHLR                 bool
	APIPassword                     string
	DialectURL                      string
	RequestMap, ResponseMap         NullString
	KeepUnmapped                    NullBool
	AuthConfig                      NullString
	RequestWrapper, RequestFormat   string
	TokenHash                       string
	StoreAuth                       int
	AccessScope                     string
	CapHourly, CapDaily             NullInt64
	PackedGSM7                      int
	AbuseThrottleTPS                int
	HLROrder                        NullString
	HLRCacheTTL                     NullInt64
	HLRProviders                    NullString
	Deployments                     string
	DefaultSender                   string
	CompanyCountryQuota             NullString
}

// ApprovedSender is one approved sender ID of an account.
type ApprovedSender struct {
	AccountID int64
	Sender    string
}

// RouteVendorRef is one vendor as a rule needs it (JSON as text).
type RouteVendorRef struct {
	ID                                int64
	CID, Status, Transport            string
	WireJSON, MultipartMode, Accepted string
	DefaultBuy                        int64
	Deployments, ShortCodes           string
}

// RouteRef is one routes row as the reference cache reads it.
type RouteRef struct {
	ID                                 int64
	Name                               string
	Priority                           int
	RouteType                          string
	Weight                             int
	Direction                          string
	AccountID                          int64
	MCCMNCs                            NullString
	AcceptUndetected                   int
	Sender, Receiver, Keyword, PriRule NullString
	VendorID, BuyCard, SellCard        int64
	Disabled                           int
	Plan, Backoff                      NullString
	Rounds, First, MaxWait             NullInt64
}

// RateRef is one open rate with its card and the card's company.
type RateRef struct {
	CardID            int64
	Kind, Currency    string
	MCC, MNC, Country string
	Price             float64
	ConvRate          float64
	CRPRef            string
	ID                int64
	TaxPct            float64
	EffectiveFrom     NullTime
	ConvTo            string
	OwnerBaseRate     float64
	OwnerBaseCRP      string
}

// AutoReplaceRef is one auto-replace rule; Regex is the {match,replace}
// JSON as stored.
type AutoReplaceRef struct {
	AccountID int64
	Regex     string
	Seq       int
}

// SpamRef is one spam rule as stored (CompanyID not Valid = universal).
type SpamRef struct {
	CompanyID                       NullInt64
	ListType, Category, Action      string
	Country, Keywords, KeywordRegex string
}

// TemplateRef is one approved, enabled template as stored.
type TemplateRef struct {
	ID, AccountID, CompanyID int64
	Regex                    string
	DefaultPriority          int
	Shortcodes               NullString
	AP1, AP2, APG1, APG2     string
}

// BlockRef is one blocked number (ExpiresAt not Valid = for ever).
type BlockRef struct {
	MSISDN               string
	AccountID, CompanyID int64
	Sender               string
	ExpiresAt            NullTime
	Source               string
}

// CountryRuleRef is one country rule (Country not Valid = the account's
// default rule).
type CountryRuleRef struct {
	AccountID           int64
	Country             NullString
	Mode                string
	Quota               NullString
	RateLimit, Priority int
}
