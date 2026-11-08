package dal

import "context"

// Vendors is vendors and their HTTP calls (http_endpoints), with the queue
// settings and wire encodings kept on a vendor.
type Vendors interface {
	// VendorExists says a vendor with this id is there.
	VendorExists(ctx context.Context, id int64) (bool, error)

	// ---- vendors ----

	// VendorCompanyKind is a company's name and type ("" when not set), as
	// the rule on a vendor's owner reads them.
	VendorCompanyKind(ctx context.Context, companyID int64) (name, typ string, err error)
	// VendorCIDTaken says a vendor with this cid is there.
	VendorCIDTaken(ctx context.Context, cid string) (bool, error)
	// InsertVendor writes a new vendor (active) and answers its id.
	InsertVendor(ctx context.Context, v VendorInsert) (int64, error)
	// VendorList is the console's vendor list, by id, at most limit.
	VendorList(ctx context.Context, limit int) ([]VendorListRow, error)
	// EndpointForProfile is one switched-on call's row, as a vendor's
	// profile is built from it.
	EndpointForProfile(ctx context.Context, id int64) (EndpointProfileRow, error)
	// SetVendorStatus writes service_status and answers the rows changed.
	SetVendorStatus(ctx context.Context, id int64, status string) (int64, error)
	// VendorDetail is one vendor's row in full.
	VendorDetail(ctx context.Context, id int64) (VendorDetailRow, error)
	// VendorRouteCount is how many rules send to the vendor, by vendor_id
	// or anywhere in a send plan.
	VendorRouteCount(ctx context.Context, id int64) (int, error)
	// SetVendorChargePoint writes charge_point.
	SetVendorChargePoint(ctx context.Context, id int64, point string) error
	// SetVendorDLRConfig sets one key (path) of dlr_config, keeping the rest.
	SetVendorDLRConfig(ctx context.Context, id int64, path string, value any) error
	// SetVendorName writes the display name.
	SetVendorName(ctx context.Context, id int64, name string) error
	// SetVendorCompany writes company_id.
	SetVendorCompany(ctx context.Context, id, companyID int64) error
	// SetVendorAdapterConfig writes adapter_config (JSON text).
	SetVendorAdapterConfig(ctx context.Context, id int64, adapterJSON string) error
	// RemoveVendorProtocolPath removes one key (path) of protocol_config
	// when it is there.
	RemoveVendorProtocolPath(ctx context.Context, id int64, path string) error
	// MergeVendorEncodings merges an object (JSON text) into
	// protocol_config's encodings, making them when missing.
	MergeVendorEncodings(ctx context.Context, id int64, encJSON string) error
	// SetVendorQueueConfig sets one key (path) of queue_config.
	SetVendorQueueConfig(ctx context.Context, id int64, path string, value any) error
	// DeleteVendorRow removes the vendor's row.
	DeleteVendorRow(ctx context.Context, id int64) error
	// VendorAdapterConfig is adapter_config as text ("{}" when NULL).
	VendorAdapterConfig(ctx context.Context, id int64) (string, error)

	// ---- the vendor's queues (config/0161) ----

	// VendorQueueColumns counts which of queue_split and short_codes the
	// store's vendors table has (2 = both).
	VendorQueueColumns(ctx context.Context) (int, error)
	// VendorQueueSettings is a vendor's split mode and short codes (JSON
	// text, "" when NULL).
	VendorQueueSettings(ctx context.Context, id int64) (split, shortCodes string, err error)
	// SetVendorQueueSplit writes queue_split.
	SetVendorQueueSplit(ctx context.Context, id int64, split string) error
	// SetVendorShortCodes writes short_codes (JSON text; NULL when not Valid).
	SetVendorShortCodes(ctx context.Context, id int64, codes NullString) error

	// ---- HTTP calls (http_endpoints) ----

	// VendorCIDTransport is a vendor's cid and transport ("" when not set).
	VendorCIDTransport(ctx context.Context, id int64) (cid, transport string, err error)
	// VendorTransport is a vendor's transport ("" when not set).
	VendorTransport(ctx context.Context, id int64) (string, error)
	// InsertEndpoint writes a new call (switched on) and answers its id.
	InsertEndpoint(ctx context.Context, e EndpointInsert) (int64, error)
	// EndpointList is the console's list of calls of a type ("" = every
	// type), by id, at most limit.
	EndpointList(ctx context.Context, kind string, limit int) ([]EndpointListRow, error)
	// EndpointKind is a call's type, name and config (JSON text).
	EndpointKind(ctx context.Context, id int64) (typ, name, configJSON string, err error)
	// WebhookUsers are the accounts that name call id as their DLR or MO
	// webhook (config/0174), by username, then kind; a row that cannot be
	// read is left out.
	WebhookUsers(ctx context.Context, endpointID int64) ([]WebhookUser, error)
	// AllWebhookUsers is WebhookUsers for every webhook any account names.
	AllWebhookUsers(ctx context.Context) ([]WebhookUser, error)
	// VendorAdapterConfigs is every vendor's cid and adapter_config that has
	// one; a row that cannot be read is left out.
	VendorAdapterConfigs(ctx context.Context) ([]VendorAdapterRow, error)
	// OtherVendorAdapterConfigs is VendorAdapterConfigs without one vendor.
	OtherVendorAdapterConfigs(ctx context.Context, vendorID int64) ([]VendorAdapterRow, error)
	// EndpointConfigsExcept is every other call's id, type, name and config
	// that has one; a row that cannot be read is left out.
	EndpointConfigsExcept(ctx context.Context, id int64) ([]EndpointConfigRow, error)
	// AccountsUsingHLRProvider names the accounts whose HLR order names the
	// provider (id as JSON text); a row that cannot be read is left out.
	AccountsUsingHLRProvider(ctx context.Context, idJSON string) ([]string, error)
	// LiveRatesUsingCRP counts the live rates whose currency provider is
	// either reference.
	LiveRatesUsingCRP(ctx context.Context, ref1, ref2 string) (int, error)
	// CompaniesUsingCRP names the companies whose currency provider is
	// either reference; a row that cannot be read is left out.
	CompaniesUsingCRP(ctx context.Context, ref1, ref2 string) ([]string, error)
	// KMSAssignmentsOf counts the companies locked under a KMS provider.
	KMSAssignmentsOf(ctx context.Context, providerID int64) (int, error)
	// DataKeysLike counts the data keys whose kek_ref is LIKE pattern.
	DataKeysLike(ctx context.Context, pattern string) (int, error)
	// DeleteEndpointRow removes a call and answers the rows removed.
	DeleteEndpointRow(ctx context.Context, id int64) (int64, error)
	// EndpointForPick is a call's row as the rule on picking it reads it.
	EndpointForPick(ctx context.Context, id int64) (EndpointPickRow, error)
	// VendorCID is a vendor's cid.
	VendorCID(ctx context.Context, id int64) (string, error)
	// EndpointConfig is a call's config as text ("{}" when NULL).
	EndpointConfig(ctx context.Context, id int64) (string, error)
	// ClaimCall makes a vendor call the vendor's.
	ClaimCall(ctx context.Context, id, vendorID int64) error
	// ClaimFreeCall makes a vendor call the vendor's when it is nobody's.
	ClaimFreeCall(ctx context.Context, id, vendorID int64) error
	// VendorCompanyID is a vendor's company_id (not Valid when NULL).
	VendorCompanyID(ctx context.Context, vendorID int64) (NullInt64, error)
	// ReleaseCalls makes a vendor's calls nobody's — all of them, or all
	// but keep (ids as digits) — filing each under company when it has
	// none (company not Valid = NULL).
	ReleaseCalls(ctx context.Context, vendorID int64, company NullInt64, keep []string) error
	// VendorAdapterTransport is a vendor's adapter_config as text ("" when
	// NULL) and its transport in lower case.
	VendorAdapterTransport(ctx context.Context, id int64) (adapterJSON, transport string, err error)
}

// VendorInsert is a new vendor's row. Deployments is JSON text, "" = NULL.
type VendorInsert struct {
	CID, Name                  string
	CompanyID                  int64
	Transport                  string
	QueueConfig, AdapterConfig string
	Deployments                string
}

// VendorListRow is one line of the console's vendor list.
type VendorListRow struct {
	ID                             int64
	CID, Name, Company             string
	Transport, Status, AdapterJSON string
}

// EndpointProfileRow is a call's row as a vendor's profile reads it; the
// JSON columns as text.
type EndpointProfileRow struct {
	ID                     int64
	Name, URL, Method      string
	Headers, Auth          string
	TimeoutMS, CacheTTLSec int
	Config, BodyTemplate   string
}

// VendorDetailRow is one vendor's row in full; Inbound is "token" when one
// of its doors has a credential.
type VendorDetailRow struct {
	ID                            int64
	CID, Name, Company, Transport string
	Status                        string
	CompanyID                     int64
	QueueConfig, AdapterConfig    string
	Inbound                       string
	Encodings, Accepted           string
}

// EndpointInsert is a new call's row. A zero VendorID or AccountID and an
// empty URL are written NULL; Config is JSON text.
type EndpointInsert struct {
	Type, Name             string
	VendorID, AccountID    int64
	URL, Method            string
	TimeoutMS              int
	RateLimit, Concurrency int
	Config                 string
}

// WebhookUser is one account that names a webhook: Kind is "dlr" when it
// is the account's DLR webhook, "mo" its MO webhook.
type WebhookUser struct {
	EndpointID, AccountID int64
	Username, Kind        string
}

// EndpointListRow is one line of the console's list of calls.
type EndpointListRow struct {
	ID                             int64
	Type, Name, Owner, URL, Method string
	Active                         int
	Config                         string
}

// VendorAdapterRow is one vendor's cid and adapter_config text.
type VendorAdapterRow struct {
	CID, AdapterJSON string
}

// EndpointConfigRow is one call's id, type, name and config text.
type EndpointConfigRow struct {
	ID                 int64
	Type, Name, Config string
}

// EndpointPickRow is a call as the rule on picking it reads it.
type EndpointPickRow struct {
	Type, Name string
	Owner      NullInt64
	Active     int
	Config     string
	Headers    string
}
