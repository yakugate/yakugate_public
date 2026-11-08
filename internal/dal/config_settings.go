package dal

import "context"

// GatewaySettings is what is set for the whole gateway: gateway_settings,
// the deployments' client addresses, the dialects and the inbound doors.
//
// A list read answers a row that would not scan, or a read cut short, as a
// RowsError, so a caller can word it apart from the statement's own failure.
type GatewaySettings interface {
	// ---- deployments ----

	// DeploymentRegistered counts the deployments with this deployment_id.
	DeploymentRegistered(ctx context.Context, deploymentID string) (int, error)
	// ActiveDeploymentCount counts the deployments switched on.
	ActiveDeploymentCount(ctx context.Context) (int, error)
	// DeploymentClientList is every deployment's client addresses, by
	// deployment_id.
	DeploymentClientList(ctx context.Context) ([]DeploymentClientRow, error)
	// DeploymentClient is one deployment's client addresses.
	DeploymentClient(ctx context.Context, id int64) (DeploymentClientRow, error)
	// SetDeploymentClientColumn writes one of client_api_url,
	// client_smpp_host, client_smpp_port, client_smpp_tls_port (column) to
	// value (text, a port, or nil = NULL).
	SetDeploymentClientColumn(ctx context.Context, id int64, column string, value any) error

	// ---- gateway_settings ----

	// GatewaySettingText is one setting's value as text. ErrNoRows: none.
	GatewaySettingText(ctx context.Context, name string) (NullString, error)
	// DeleteGatewaySetting removes one setting.
	DeleteGatewaySetting(ctx context.Context, name string) error
	// SaveGatewaySetting writes one setting's value (JSON text).
	SaveGatewaySetting(ctx context.Context, name, valueJSON string) error

	// ---- a vendor's inbound doors ----

	// DoorKindPaths is a vendor's doors: each kind and its own path ("" =
	// none).
	DoorKindPaths(ctx context.Context, vendorID int64) ([]DoorKindPath, error)
	// DoorVendor is a vendor's cid and company.
	DoorVendor(ctx context.Context, vendorID int64) (cid string, company NullInt64, err error)
	// DoorID is the id of a vendor's door of one kind. ErrNoRows: none.
	DoorID(ctx context.Context, vendorID int64, kind string) (int64, error)
	// InsertDoor makes a vendor's door of one kind and answers its id (0
	// when the driver gives none).
	InsertDoor(ctx context.Context, vendorID int64, kind string) (int64, error)
	// SetDoorCustomPath writes a door's own path ("" = NULL).
	SetDoorCustomPath(ctx context.Context, doorID int64, path string) error
	// DoorCustomPaths is every door's own path that is set. A store without
	// the column answers its "Unknown column" error.
	DoorCustomPaths(ctx context.Context) ([]DoorCustomPath, error)

	// ---- dialects ----

	// DialectURLs is every dialect with a url other than "" and native.
	DialectURLs(ctx context.Context, native string) ([]DialectURLRow, error)
	// VendorDialectList is the DLR and MO dialects, by kind and id, with how
	// many doors read with each.
	VendorDialectList(ctx context.Context) ([]VendorDialectRow, error)
	// DialectForURL is a dialect's name, kind ("mt" when not set) and url.
	DialectForURL(ctx context.Context, id int64) (name, kind, url string, err error)
	// SetDialectURL writes a dialect's url.
	SetDialectURL(ctx context.Context, id int64, url string) error
	// InsertDialect writes a new client (MT) dialect; the id as the driver
	// gives it.
	InsertDialect(ctx context.Context, d DialectInsert) (int64, error)
	// DialectList is the client (MT) dialects, by id, at most limit, with
	// how many accounts use each.
	DialectList(ctx context.Context, limit int) ([]DialectListRow, error)
	// DialectExists says a dialect with this id is there.
	DialectExists(ctx context.Context, id int64) (bool, error)
	// DialectIsMT says the dialect is a client's API (kind mt).
	DialectIsMT(ctx context.Context, id int64) (bool, error)
	// SetAccountDialect points an account at a dialect (not Valid = NULL);
	// n = rows changed.
	SetAccountDialect(ctx context.Context, accountID int64, dialectID NullInt64) (n int64, err error)
	// DialectAccountUsers counts the accounts that use a dialect.
	DialectAccountUsers(ctx context.Context, id int64) (int, error)
	// DialectDoorUsers counts the vendor doors that read with a dialect.
	DialectDoorUsers(ctx context.Context, id int64) (int, error)
	// DeleteDialectRow removes a dialect; n = rows removed.
	DeleteDialectRow(ctx context.Context, id int64) (n int64, err error)
}

// DeploymentClientRow is one deployment and where its clients connect.
type DeploymentClientRow struct {
	ID           int64
	DeploymentID string
	Name         string
	Disabled     bool
	APIURL       string
	SMPPHost     string
	SMPPPort     int
	SMPPTLSPort  int
}

// DoorKindPath is one of a vendor's doors.
type DoorKindPath struct {
	Kind, Path string
}

// DoorCustomPath is one door's own path, with its vendor.
type DoorCustomPath struct {
	VendorID        int64
	CID, Kind, Path string
}

// DialectURLRow is a dialect's url (Kind "mt" when not set).
type DialectURLRow struct {
	ID              int64
	Name, Kind, URL string
}

// VendorDialectRow is one DLR or MO dialect, with how many doors read with it.
type VendorDialectRow struct {
	ID    int64
	Name  string
	Kind  string
	URL   string
	Doors int
}

// DialectInsert is a new client (MT) dialect: the maps are JSON text.
type DialectInsert struct {
	Name, URL, Format       string
	RequestMap, ResponseMap string
	KeepUnmapped            int
}

// DialectListRow is one client (MT) dialect of the list.
type DialectListRow struct {
	ID       int64
	Name     string
	URL      string
	Format   string
	Mappings int
	Accounts int
}
