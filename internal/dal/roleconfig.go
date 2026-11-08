package dal

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// The roles' own reads and writes of the config store (step 2's last part,
// 30 Oct 2026): what the traffic roles ran on their config pool themselves
// before the move — the vendors each vendor role loads, the customers'
// webhook rows the dlr and mo roles push to, a vendor's calls, the actions a
// vendor's status code asks for, packet capture rows, and two one-row
// lookups. They are parts of the TrafficConfig (traffic.go), on the same
// pool. Each role still decides what a row means; only the reading and
// writing is here.
//
// A reader that hands rows to a func gives each row as it is read, with the
// row's scan error (the row is then empty): the roles skip, or log and skip,
// such a row, as they did. Its own error is the query's, or the read's when
// the role checked that before.

// ---- the vendors a vendor role loads ----

// BlackholeVendorRow is one active blackhole vendor placed on the
// deployment: its receipt split (dlr_config) and its deployments entry.
type BlackholeVendorRow struct {
	ID                     int64
	CID                    string
	DLRConfig, Deployments string
}

// SMPPVendorRow is one active SMPP vendor placed on the deployment, the
// columns smpp-vendor binds with.
type SMPPVendorRow struct {
	ID                                                  int64
	CID, QueueConfig, ProtocolConfig, CredentialRef     string
	Sessions, WindowSize, RetrySec, EnquireSec, RespSec int
	ThrottleSec, BatchSize                              int
	MsgIDTransform, Deployments                         string
	DownAfterSec, RecheckAfterSec                       int
	Encodings, DLRConfig                                string
	OwnIntermediate                                     int
}

// HTTPVendorRow is one active HTTP or SOAP vendor placed on the deployment.
type HTTPVendorRow struct {
	ID                              int64
	CID, AdapterConfig, Deployments string
	DownAfterSec, RecheckAfterSec   int
	OwnIntermediate                 int
	DLRConfig                       string
}

// VendorRows is the vendors each vendor role loads: the active ones of its
// transport placed on the deployment named (deployment.SQLFilter's rule).
type VendorRows interface {
	BlackholeVendors(ctx context.Context, deploymentID string, each func(BlackholeVendorRow, error)) error
	// SMPPVendors' error is also a read cut short part way (rows.Err).
	SMPPVendors(ctx context.Context, deploymentID string, each func(SMPPVendorRow, error)) error
	HTTPVendors(ctx context.Context, deploymentID string, each func(HTTPVendorRow, error)) error
	// HTTPSettingsSignature is one line that moves whenever an HTTP or SOAP
	// vendor, or a vendor call, is added, removed or edited.
	HTTPSettingsSignature(ctx context.Context) (string, error)
	// VendorIDByCID is the vendor's id; ErrNoRows when none has the cid.
	VendorIDByCID(ctx context.Context, cid string) (int64, error)
}

// VendorName is a vendor's cid and its display name.
type VendorName struct {
	CID, Name string
}

// VendorNamer reads every vendor's cid and name, by id (4 Nov 2026: the MO
// webhook says which vendor an incoming message came from). A separate
// interface so the role's test fakes of VendorRows need not grow.
type VendorNamer interface {
	VendorNames(ctx context.Context) (map[int64]VendorName, error)
}

// ---- a vendor's calls (http_endpoints of type vendor) ----

// VendorCallRow is one switched-on call as http-vendor builds it; the JSON
// columns as text.
type VendorCallRow struct {
	ID                               int64
	Name, URL, Method, Headers, Auth string
	TimeoutMS, CacheTTLSec           int
	Config, BodyTemplate, TLS, Retry string
}

// VendorCalls is the calls http-vendor reads for a vendor.
type VendorCalls interface {
	// SendKnobs reads a send call's claim batch and concurrency (rate_limit,
	// submit_concurrency; 0 when NULL).
	SendKnobs(ctx context.Context, id int64) (batch, concurrency int, err error)
	// Call reads a switched-on call; ErrNoRows when it is off or gone.
	Call(ctx context.Context, id int64) (VendorCallRow, error)
	// CallActive reads a call's is_active, on or off; ErrNoRows when gone.
	CallActive(ctx context.Context, id int64) (int, error)
	// CircuitBreaker reads a call's circuit_breaker column ("" when NULL).
	CircuitBreaker(ctx context.Context, id int64) (string, error)
}

// ---- what a vendor's status code asks for ----

// VendorCodeActions is what smpp-vendor writes when a submit answer's code
// is mapped to an action: the vendor paused, the number blocked, and the
// audit row of each.
type VendorCodeActions interface {
	// PauseVendor pauses an active vendor; it answers how many rows moved
	// (0 = it was not active).
	PauseVendor(ctx context.Context, vendorID int64) (int64, error)
	// AuditVendorPaused files the vendor.paused_by_code row; detail is JSON.
	AuditVendorPaused(ctx context.Context, cid, detail string) error
	// BlockForAccount adds the number to the account's blocklist unless an
	// active block for it is there; it answers how many rows were added.
	BlockForAccount(ctx context.Context, msisdn string, accountID int64, reason string) (int64, error)
	// AuditBlockAdded files the blocklist.added_by_code row; detail is JSON.
	AuditBlockAdded(ctx context.Context, accountID int64, msisdn, detail string) error
}

// ---- the customers' webhooks (http_endpoints of type dlr and mo) ----

// WebhookRow is one customer webhook row and what the pushing role needs of
// its account. The fields a reader does not read stay empty.
type WebhookRow struct {
	ID, AccountID, CompanyID int64
	URL, Method, Retry       string
	Channel                  string // dlr_channel / mo_channel
	BindAttempts             int
	// MaxAttempts is the account's dlr_max_attempts / mo_max_attempts
	// (config/0173): the most tries, bind and webhook together; 0 = none.
	MaxAttempts                            int
	RateLimit, Concurrency, TimeoutMS      int
	Deployments                            string
	Name, Headers, Auth, BodyTemplate, TLS string
	Config, ResponseMap                    string
}

// CustomerWebhooks is the receipt and MO webhooks the dlr and mo roles push
// to, and one row the http-server's webhook test builds.
type CustomerWebhooks interface {
	// DLRWebhooks and MOWebhooks read, for every account that names an
	// active webhook of their kind with a URL (sms_service_config
	// dlr_webhook_id / mo_webhook_id, config/0174), that webhook: one row per
	// account, so a webhook many accounts share comes once for each. Like
	// the readers they came from, they do not ask whether the read was cut
	// short.
	DLRWebhooks(ctx context.Context, each func(WebhookRow, error)) error
	MOWebhooks(ctx context.Context, each func(WebhookRow, error)) error
	// Webhook reads one dlr or mo row (name, url, method, headers, auth,
	// body_template, tls, config); ErrNoRows when there is none.
	Webhook(ctx context.Context, id int64) (WebhookRow, error)
}

// ---- one-row lookups of an account ----

// AccountLookups is what a role asks of one account beside the reference
// cache.
type AccountLookups interface {
	// AccountCompany is the account's company (0 = none); ErrNoRows when
	// the account is not there.
	AccountCompany(ctx context.Context, accountID int64) (int64, error)
	// DefaultSellCard is the account's default MT selling card (0 = none).
	DefaultSellCard(ctx context.Context, accountID int64) (int64, error)
}

// ---- packet captures (log_captures of type traffic) ----

// PacketCaptures is the rows the http-server keeps for the packet captures
// it runs.
type PacketCaptures interface {
	// FailRunningOnHost marks the captures this host was running as failed
	// (the gateway was restarted during them).
	FailRunningOnHost(ctx context.Context, host string) error
	// Failed marks a running capture failed with its error.
	Failed(ctx context.Context, id int64, errText string) error
	// Ended marks a running capture expired with its size; expires_at only
	// moves back.
	Ended(ctx context.Context, id int64, fileBytes int64, truncated bool) error
	// Of reads a capture's listener and reason.
	Of(ctx context.Context, id int64) (listener, reason string, err error)
	// Purged marks a capture's file deleted.
	Purged(ctx context.Context, id int64) error
}

// The parts, read through a TrafficConfig that may be nil, as the others.
func (t *TrafficConfig) VendorRows() VendorRows {
	if t == nil {
		return nil
	}
	return t.Vendors
}

func (t *TrafficConfig) VendorCalls() VendorCalls {
	if t == nil {
		return nil
	}
	return t.Calls
}

func (t *TrafficConfig) CodeActions() VendorCodeActions {
	if t == nil {
		return nil
	}
	return t.Actions
}

func (t *TrafficConfig) CustomerWebhooks() CustomerWebhooks {
	if t == nil {
		return nil
	}
	return t.Webhooks
}

func (t *TrafficConfig) AccountLookups() AccountLookups {
	if t == nil {
		return nil
	}
	return t.Accounts
}

func (t *TrafficConfig) PacketCaptures() PacketCaptures {
	if t == nil {
		return nil
	}
	return t.Captures
}

// ---- the HTTP vendors' poll turns ----

// PollTurns is which process polls a vendor's statuses now: one at a time,
// gateway-wide (internal/process/httpvendor/pollturn.go).
type PollTurns interface {
	// TakePollTurn takes the vendor's turn for me when it is free, or keeps
	// it when it is mine, for hold; false = another process holds it.
	TakePollTurn(ctx context.Context, vendorID int64, me string, hold time.Duration) (bool, error)
}

// OpenPollTurns opens the turns on the main Redis. ErrNotServed when Redis is
// off (every process then polls).
func OpenPollTurns(cfg config.Config) (PollTurns, error) {
	if !cfg.RedisEnabled() {
		return nil, fmt.Errorf("dal: poll turns: %w", ErrNotServed)
	}
	d, ok := Lookup(config.BackendRedis)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", config.BackendRedis, Registered())
	}
	if d.PollTurns == nil {
		return nil, fmt.Errorf("dal: poll turns on %s: %w", config.BackendRedis, ErrNotServed)
	}
	return d.PollTurns(cfg)
}

// ---- retention's upkeep ----

// PaidPeriod is one sent and paid invoice whose period still holds raw
// commit rows.
type PaidPeriod struct {
	Company  int64
	From, To time.Time
}

// RetentionUpkeep is what the retention role keeps in the config store:
// each company's days in hot, the billing batches' guard rows, and the
// balance ledger's partitions and daily rollup.
type RetentionUpkeep interface {
	// CompanyHotDays gives each company's data_retention hot_days and
	// hot_months (0 when unset). Like the reader it came from it does not
	// ask whether the read was cut short.
	CompanyHotDays(ctx context.Context, each func(company int64, days, months int, err error)) error
	// PruneUsageBatches removes up to 5000 guard rows older than keepDays.
	PruneUsageBatches(ctx context.Context, keepDays int) (int64, error)
	// LedgerPartitions counts balance_history's partitions of that name.
	LedgerPartitions(ctx context.Context, name string) (int, error)
	// AddLedgerPartition splits partition name (rows before next, a
	// YYYY-MM-DD) out of pmax.
	AddLedgerPartition(ctx context.Context, name, next string) error
	// PaidPeriods reads the paid periods with commit rows left (rows that
	// cannot be read are left out).
	PaidPeriods(ctx context.Context) ([]PaidPeriod, error)
	// CommitDays is the company's days with commit rows from from to to.
	CommitDays(ctx context.Context, company int64, from, to time.Time) ([]string, error)
	// RollupDay adds the day's totals and removes its commit rows in one
	// transaction; it answers how many rows went.
	RollupDay(ctx context.Context, company int64, day string) (int64, error)
}

// ServerIdentity is a SQL server, asked which one it is.
type ServerIdentity interface {
	ServerUUID(ctx context.Context) (string, error)
}

// Retention is what OpenRetention built: the upkeep on the config store's
// pool (nil when it cannot be opened), and the cold DSN's server and the hot
// store, to be compared before a day moves (both nil when the cold DSN names
// no server, either nil when it cannot be opened).
type Retention struct {
	Upkeep     RetentionUpkeep
	ColdServer ServerIdentity
	HotServer  ServerIdentity
}

// OpenRetention opens retention's parts on the install's SQL database (SQL:
// MySQL unless the stores are on SQL Server, as the role always did on
// MySQL).
func OpenRetention(cfg config.Config) (*Retention, error) {
	on := SQL(cfg)
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.Retention == nil {
		return nil, fmt.Errorf("dal: retention on %s: %w", on, ErrNotServed)
	}
	return d.Retention(cfg)
}
