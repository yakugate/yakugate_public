package dal

import (
	"context"
	"fmt"
	"time"

	cry "github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/kms"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// Two small parts of the SQL database the roles reached as *mysql.Store
// until 30 Oct 2026: the wrapped data keys in the config store (what
// crypto.FromEnv is given), and the HTTP vendors' shared login sessions in
// the HLR store.

// DataKeys is the encryption keys' rows in the config store: the wrapped
// data keys (crypto.Keyring), each company's KMS assignment and providers,
// and the counts the start-up check reads. internal/crypto and internal/kms
// ask the keyring for the parts after the first two by their own small
// interfaces (TenantKMSLookup, AccountCompanyLookup, DataKeyCounter,
// kms.ProviderSource); every DAL that serves DataKeys gives them all.
type DataKeys interface {
	GetWrappedDEK(ctx context.Context, keyRef string) (wrapped []byte, kekRef string, revoked, found bool, err error)
	PutWrappedDEK(ctx context.Context, keyRef string, wrapped []byte, kekRef string) error
	DataKeysByKEK(ctx context.Context) (map[string]int64, error)
	TenantKMS(ctx context.Context, companyID int64) (cry.TenantConfig, bool, error)
	AccountCompany(ctx context.Context, accountID int64) (int64, error)
	KMSProvider(ctx context.Context, id int64) (kms.HTTPProvider, bool, error)
	KMSEndpoint(ctx context.Context, id int64) (kms.Endpoint, bool, error)
}

// OpenDataKeys opens the data keys on the install's SQL database (the config
// store, YAKU_CONFIG_DSN), on the process's one pool.
func OpenDataKeys(cfg config.Config) (DataKeys, error) {
	on := SQL(cfg)
	d, ok := Lookup(on)
	if !ok {
		return nil, fmt.Errorf("dal: no %s DAL in this binary (have %v)", on, Registered())
	}
	if d.DataKeys == nil {
		return nil, fmt.Errorf("dal: data keys on %s: %w", on, ErrNotServed)
	}
	k, err := d.DataKeys(cfg)
	if err != nil {
		return nil, fmt.Errorf("dal: data keys on %s: %w", on, err)
	}
	return k, nil
}

// VendorSessions is the HTTP vendors' shared login sessions
// (vendor_sessions, httpadapter.SessionStore): one login a vendor for every
// process of the gateway.
type VendorSessions interface {
	LoadVendorSession(ctx context.Context, vendorID int64) (map[string]string, time.Time, error)
	SaveVendorSession(ctx context.Context, vendorID int64, cid string, vars map[string]string, expiresAt time.Time) error
	DeleteVendorSession(ctx context.Context, vendorID int64) error
	LockVendorLogin(ctx context.Context, vendorID int64) (func(), error)
}

// OpenVendorSessions opens the vendor sessions in the HLR store (YAKU_HLR_DSN,
// else the queue database: config.HLRStore), on the DAL its backend names.
// ErrNotServed when no HLR store is set, or its database keeps none.
func OpenVendorSessions(cfg config.Config) (VendorSessions, error) {
	b, dsn := cfg.HLRStore()
	if dsn == "" {
		return nil, fmt.Errorf("dal: vendor sessions: no HLR store set: %w", ErrNotServed)
	}
	on := norm(b)
	if on == "" {
		on = SQL(cfg)
	}
	d, ok := Lookup(on)
	if !ok || d.VendorSessions == nil {
		return nil, fmt.Errorf("dal: vendor sessions on %s: %w", on, ErrNotServed)
	}
	s, err := d.VendorSessions(dsn)
	if err != nil {
		return nil, fmt.Errorf("dal: vendor sessions on %s: %w", on, err)
	}
	return s, nil
}
