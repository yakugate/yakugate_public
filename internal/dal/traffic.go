package dal

import (
	"context"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// What the traffic roles keep beside their main work (plan of 29 Oct 2026):
// how well each vendor delivers to each network (internal/vendorquality),
// and the small parts of the config store they read and write — vendor
// health reports (internal/vendorhealth), vendors' status-code rules
// (internal/vendorstatus), vendors' companies (internal/vendorgroup),
// customer endpoints that are not answering (internal/webhookalert) and the
// MO rules with their opt-outs (internal/mo). The rules each package applies
// stay there; what is kept and read is here.

// ---- vendor quality ----

// QualityKey is one vendor to one network.
type QualityKey struct {
	VendorID int64
	MCC, MNC string
}

// QualityCounts is how many messages got a verdict and how many of those
// were delivered.
type QualityCounts struct {
	Delivered, Judged int64
}

// QualityBucket is one minute's counts for one key.
type QualityBucket struct {
	Minute time.Time // UTC, truncated to the minute
	Key    QualityKey
	Counts QualityCounts
}

// VendorQuality is the counts every process of a deployment adds to and the
// sending processes read back.
type VendorQuality interface {
	// Add adds buckets to what is kept (each writer adds; any number merge).
	Add(ctx context.Context, bs []QualityBucket) error
	// Load sums the last window.
	Load(ctx context.Context, window time.Duration) (map[QualityKey]QualityCounts, error)
}

// QualityOn says where the counts are: the main Redis when one is set, else
// the hot store when it is a SQL database (MySQL or SQL Server); "" = nowhere
// (quality routing then has no counts, and a quality level behaves as list
// order).
func QualityOn(cfg config.Config) config.Backend {
	if cfg.RedisEnabled() {
		return config.BackendRedis
	}
	if (cfg.HotBackend == config.BackendMySQL || cfg.HotBackend == config.BackendMSSQL) && cfg.HotDSN != "" {
		return cfg.HotBackend
	}
	return ""
}

// OpenVendorQuality opens the counts on the DAL QualityOn picks. ErrNotServed
// when there is nowhere to keep them.
func OpenVendorQuality(cfg config.Config) (VendorQuality, error) {
	on := QualityOn(cfg)
	if on == "" {
		return nil, fmt.Errorf("dal: vendor quality: %w", ErrNotServed)
	}
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.VendorQuality == nil {
		return nil, fmt.Errorf("dal: vendor quality on %s: %w", on, ErrNotServed)
	}
	q, err := d.VendorQuality(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: vendor quality on %s: %w", on, err)
	}
	return q, nil
}

// ---- the config store's small parts ----

// VendorHealthRow is one vendor joined with one of its health reports (the
// report's fields zero, its times not Valid, when no process has reported).
type VendorHealthRow struct {
	ID           int64
	CID, Status  string
	DownAfterSec int64
	FailingSince NullTime
	ReportedAt   NullTime
	Detail       string
	Transport    string
	Instance     string
}

// VendorHealthReport is one vendor as one process sees it: failing since
// when (zero = not failing) and why ("" = nothing to say).
type VendorHealthReport struct {
	VendorID     int64
	FailingSince time.Time
	Detail       string
}

// VendorHealth is vendor_health: each vendor role's reports, and every
// vendor with its reports for the routing processes.
type VendorHealth interface {
	// Rows reads every vendor and its reports once.
	Rows(ctx context.Context) ([]VendorHealthRow, error)
	// Report writes one instance's reports at `at` and drops its rows for
	// vendors it no longer reports (two statements; none reported = Forget).
	Report(ctx context.Context, instance, role string, at time.Time, reps []VendorHealthReport) error
	// Forget drops every row of an instance.
	Forget(ctx context.Context, instance string) error
	// Prune drops rows reported before `before`.
	Prune(ctx context.Context, before time.Time) error
}

// StatusCodeRow is one vendor_status_codes row.
type StatusCodeRow struct {
	VendorID                    int64
	Scope, Code, Action, Target string
}

// VendorReceiptSwitch is a vendor's dlr_code_overrides switch and its cid.
type VendorReceiptSwitch struct {
	VendorID int64
	On       bool
	CID      string
}

// VendorStatusCodes is the vendors' status-code rules.
type VendorStatusCodes interface {
	// StatusCodes reads every rule; Switches every vendor's receipt switch.
	StatusCodes(ctx context.Context) ([]StatusCodeRow, error)
	Switches(ctx context.Context) ([]VendorReceiptSwitch, error)
}

// VendorCompanies is every vendor's company.
type VendorCompanies interface {
	// VendorCompanies reads vendor id -> company id (0 = none).
	VendorCompanies(ctx context.Context) (map[int64]int64, error)
}

// WebhookAlert is one endpoint's counted failures, as they are added to its
// webhook_delivery_alerts row.
type WebhookAlert struct {
	EndpointID, AccountID int64
	Kind, URL, LastError  string
	Failed, GaveUp        int
}

// WebhookAlerts is webhook_delivery_alerts: one row per customer endpoint
// that is not answering.
type WebhookAlerts interface {
	// Failed adds counted failures to the endpoint's row (made when none).
	Failed(ctx context.Context, a WebhookAlert) error
	// Clear drops the endpoint's row of a kind.
	Clear(ctx context.Context, endpointID int64, kind string) error
}

// MORouteRow is one enabled MO rule with what the service needs of its
// account. A pattern column that is NULL is nil.
type MORouteRow struct {
	ID, AccountID                       int64
	Username                            string
	Priority                            int
	Receiver, Keyword, Sender           *string
	VendorID                            int64
	MCCMNCs                             string
	OptOutMode, OptOutWords, OptInWords string
	CompanyID                           int64
	Privacy                             string
	Deployments                         string
}

// MORules is the MO rules and the opt-outs they file.
type MORules interface {
	// MORoutes reads every enabled MO rule, by priority. A read that failed
	// part way answers nil; one whose end could not be read answers what
	// it read (never nil) with the error.
	MORoutes(ctx context.Context) ([]MORouteRow, error)
	// OptOut blocks msisdn for the account as an unsubscribe; OptIn undoes
	// an unsubscribe the gateway filed.
	OptOut(ctx context.Context, accountID int64, msisdn, sender string) error
	OptIn(ctx context.Context, accountID int64, msisdn string) error
}

// TrafficConfig is what OpenTrafficConfig built, every part on the config
// store's one pool. A part left nil is not served.
type TrafficConfig struct {
	Health    VendorHealth
	Codes     VendorStatusCodes
	Companies VendorCompanies
	Alerts    WebhookAlerts
	MO        MORules
	// The roles' own reads and writes (roleconfig.go).
	Vendors  VendorRows
	Calls    VendorCalls
	Actions  VendorCodeActions
	Webhooks CustomerWebhooks
	Accounts AccountLookups
	Captures PacketCaptures
}

// The parts, read through a TrafficConfig that may be nil (a role that has
// none): nil answers nil, which every user treats as "nothing to read or
// write".
func (t *TrafficConfig) VendorHealth() VendorHealth {
	if t == nil {
		return nil
	}
	return t.Health
}

func (t *TrafficConfig) StatusCodes() VendorStatusCodes {
	if t == nil {
		return nil
	}
	return t.Codes
}

func (t *TrafficConfig) VendorCompanies() VendorCompanies {
	if t == nil {
		return nil
	}
	return t.Companies
}

func (t *TrafficConfig) WebhookAlerts() WebhookAlerts {
	if t == nil {
		return nil
	}
	return t.Alerts
}

func (t *TrafficConfig) MORules() MORules {
	if t == nil {
		return nil
	}
	return t.MO
}

// TrafficConfigOrNone is OpenTrafficConfig, or an empty TrafficConfig when it
// cannot be opened: every part nil, so each user of one does nothing (what a
// role without its config store pool did before the move).
func TrafficConfigOrNone(cfg config.Config) *TrafficConfig {
	t, err := OpenTrafficConfig(cfg)
	if err != nil || t == nil {
		return &TrafficConfig{}
	}
	return t
}

// OpenTrafficConfig opens the parts on the install's SQL database (the
// config store).
func OpenTrafficConfig(cfg config.Config) (*TrafficConfig, error) {
	on := SQL(cfg)
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.TrafficConfig == nil {
		return nil, fmt.Errorf("dal: traffic config on %s: %w", on, ErrNotServed)
	}
	t, err := d.TrafficConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: traffic config on %s: %w", on, err)
	}
	return t, nil
}
