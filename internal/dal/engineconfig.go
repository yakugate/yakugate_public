package dal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yakugate/yakugate/internal/platform/config"
)

// The parts of the config store the shared engine packages keep in memory
// and read again on a timer (plan of 29 Oct 2026): the vendors' send
// settings (internal/attemptcap, internal/queues), the accounts' receipt
// policies and the gateway default under them (internal/receiptpolicy), the
// change marks a reload asks first (internal/reloadgate), the vendors'
// inbound doors (internal/inboundauth), the number ranges and the live HLR
// providers (internal/hlr). The rules each package applies stay there; what
// is read is here.

// ---- vendors' send settings ----

// VendorSendSettings is what the vendor roles read of every vendor beside
// their queues.
type VendorSendSettings interface {
	// MaxAttempts is each vendor's own max_attempts above 0, by cid. A row
	// that would not scan is left out; the error is the statement's.
	MaxAttempts(ctx context.Context) (map[string]int, error)
	// SplitModes is each vendor's vendors.queue_split as stored ('fair'
	// when NULL), by cid. A store without the column, or one that cannot be
	// read, answers none.
	SplitModes(ctx context.Context) map[string]string
}

// ---- receipt policies ----

// ReceiptPolicyRow is one account that asked for something: its stored
// receipt_policy (nil = NULL), its dlr_levels ("" = NULL) and its
// part_receipts (config/0172: "final" | "each").
type ReceiptPolicyRow struct {
	AccountID    int64
	Policy       []byte
	Levels       string
	PartReceipts string
}

// ReceiptPolicies is the accounts' receipt policies and the gateway-wide
// default (gateway_settings, read through GatewaySettingText).
type ReceiptPolicies interface {
	// ReceiptPolicyRows is every account with a receipt policy, a
	// dlr_levels other than 'registered' or part_receipts 'each'. A row that would not scan, or a
	// read cut short, is a RowsError.
	ReceiptPolicyRows(ctx context.Context) ([]ReceiptPolicyRow, error)
	// AccountIDs is every account. A row that would not scan, or a read cut
	// short, is a RowsError.
	AccountIDs(ctx context.Context) ([]int64, error)
	// DefaultReceiptPolicy is the default's stored text, as
	// GatewaySettings.GatewaySettingText reads it.
	DefaultReceiptPolicy
}

// DefaultReceiptPolicy reads one gateway setting as text, and tells a store
// that has no such table yet (receiptpolicy.ReadDefault). GatewaySettings and
// ConfigStore have both methods.
type DefaultReceiptPolicy interface {
	GatewaySettingText(ctx context.Context, name string) (NullString, error)
	MissingTable(err error) bool
}

// ---- change marks ----

// ChangeMarks answers "how many rows, and when did the newest change?" for
// whole tables or parts of them (internal/reloadgate).
type ChangeMarks interface {
	// Fingerprint builds the question for tables, each a table name
	// optionally followed by a WHERE clause written in code (never from
	// input) — "http_endpoints WHERE type='dlr'". Every table must have an
	// updated_at that moves on a change. The func answers the fingerprint
	// and whether a row changed within the last second of the store's clock.
	Fingerprint(tables ...string) func(ctx context.Context) (sig string, fresh bool, err error)
}

// ---- inbound doors ----

// The shapes of the doors' store, oldest first (inboundauth reads the
// columns a shape has).
const (
	DoorShapeNone    = iota // config/0121: credentials only
	DoorShapeMaps           // config/0125: + the door's request/response mapping
	DoorShapePlaces         // config/0127: + the door's credential places
	DoorShapeDialect        // config/0128: all three from the door's dialect
	DoorShapePath           // config/0152: + the door's own path
)

// DoorRow is one vendor joined with one of its doors (Kind "" = a vendor
// with no door rows yet), as the newest shape the store has gives it; the
// columns of a newer shape are "" or 0.
type DoorRow struct {
	VendorID                      int64
	CID, LegacyRaw                string
	ID                            int64
	Kind                          string
	Disabled                      int
	JWTRaw                        string
	TokenHash, TokenSrc, TokenHdr string
	Username, Password            string
	IPRaw                         string
	Throughput                    int
	ReqRaw, ReplyRaw              string // DoorShapeMaps
	PlacesRaw                     string // DoorShapePlaces
	DialectKind, DialectName      string // DoorShapeDialect
	DialectFormat, DialectWrapper string
	DialectKeep                   int
	DialectURL                    string
	CustomPath                    string // DoorShapePath
}

// InboundDoors is every vendor's doors (vendors not disabled).
type InboundDoors interface {
	// Doors reads every door in the newest shape the store has, and says
	// which. A statement error is returned as it is; a row that would not
	// scan, or a read cut short, is a RowsError.
	Doors(ctx context.Context) ([]DoorRow, int, error)
	// MoveLegacy runs fn while this process holds the one lock the move of
	// the vendors' old shared settings is done under (waiting up to 10 s).
	// got=false: another role holds it.
	MoveLegacy(ctx context.Context, fn func(LegacyDoorMove) error) (got bool, err error)
}

// LegacyVendor is one vendor whose old shared inbound settings
// (vendors.inbound_config, a JSON object) are still there.
type LegacyVendor struct {
	ID  int64
	CID string
	Raw string
}

// LegacyDoorMove is the move of the old shared settings, on the connection
// that holds its lock.
type LegacyDoorMove interface {
	// Vendors lists the vendors with old settings left (none when the store
	// has no such table or column).
	Vendors(ctx context.Context) ([]LegacyVendor, error)
	// MoveVendor runs fn in one transaction and clears the vendor's old
	// settings in it; committed when fn answers nil, rolled back otherwise.
	MoveVendor(ctx context.Context, vendorID int64, fn func(LegacyDoorTx) error) error
}

// LegacyDoorTx is one vendor's move.
type LegacyDoorTx interface {
	// Door reads (and locks) a vendor's door of one kind: whether it has a
	// credential of its own and its address list as JSON. found=false: none.
	Door(ctx context.Context, vendorID int64, kind string) (id int64, hasCred bool, ips string, found bool, err error)
	// AddDoor makes the door (its id; 0 when the driver gives none).
	AddDoor(ctx context.Context, vendorID int64, kind string, at time.Time) (int64, error)
	// SetDoorToken stores a door's token digest, its source and header (""
	// = NULL).
	SetDoorToken(ctx context.Context, doorID int64, hash, source, header string) error
	// SetDoorIPs stores a door's address list (JSON).
	SetDoorIPs(ctx context.Context, doorID int64, ips string) error
}

// ---- HLR: number ranges and live providers ----

// OperatorRangeRow is one operators row (COALESCEd to "").
type OperatorRangeRow struct {
	CCC, MCC, MNC, From, To string
	OperatorName            string // the operator's name (6 Nov 2026: kept on a range answer)
}

// HLRProviderRow is one active hlr provider of http_endpoints, its JSON
// columns as text.
type HLRProviderRow struct {
	ID                     int64
	Name, URL, Method      string
	Headers, Auth          string
	TimeoutMS, CacheTTLSec int
	Config                 string
	BodyTemplate           string
}

// HLRLookupConfig is what the HLR order's "range" and "live" steps load.
type HLRLookupConfig interface {
	// NumberRangesMark is the operators table's count and newest change
	// ("<count>/<YYYYMMDDhhmmss>").
	NumberRangesMark(ctx context.Context) (count int64, newest string, err error)
	// NumberRanges reads every operators row (capacity hint n).
	NumberRanges(ctx context.Context, n int64) ([]OperatorRangeRow, error)
	// HLRProviders reads the active hlr provider rows.
	HLRProviders(ctx context.Context) ([]HLRProviderRow, error)
}

// HLRLookupOf is a ConfigStore that can read the HLR parts of the engine's
// config on its own handle: the console loads its number-range index so
// (30 Oct 2026; it type-asserted the MySQL store until then).
type HLRLookupOf interface {
	HLRLookup() HLRLookupConfig
}

// ---- the bundle ----

// EngineConfig is what OpenEngineConfig built, every part on the config
// store's one pool a process. A part left nil is not served.
type EngineConfig struct {
	VendorSends VendorSendSettings
	Policies    ReceiptPolicies
	Marks       ChangeMarks
	Doors       InboundDoors
	HLR         HLRLookupConfig
	Audit       AuditLog
	Logs        LogSettings
}

// OpenEngineConfig opens the parts on the install's SQL database (the config
// store).
func OpenEngineConfig(cfg config.Config) (*EngineConfig, error) {
	on := SQL(cfg)
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.EngineConfig == nil {
		return nil, fmt.Errorf("dal: engine config on %s: %w", on, ErrNotServed)
	}
	e, err := d.EngineConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: engine config on %s: %w", on, err)
	}
	return e, nil
}

// EngineConfigOrNone is OpenEngineConfig, or an empty EngineConfig when it
// cannot be opened: every part nil, so each user of one does nothing (what a
// role without its config store pool did before the move).
func EngineConfigOrNone(cfg config.Config) *EngineConfig {
	e, err := OpenEngineConfig(cfg)
	if err != nil || e == nil {
		return &EngineConfig{}
	}
	return e
}

// ErrNoQueueStore is the reason given when a role has no queue store to ask
// (pipeline.QueueHealth).
var ErrNoQueueStore = errors.New("no queue store")
