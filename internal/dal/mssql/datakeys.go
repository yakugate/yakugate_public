package mssql

// The config store's data keys and the HLR store's vendor sessions on SQL
// Server (dal.DataKeys, dal.VendorSessions; mirrors the data-key, KMS and
// vendor_sessions parts of internal/dal/mysql/mysql.go).

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	cry "github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/kms"
)

// KeyStmts is the data keys' and vendor sessions' statements.
var KeyStmts = newRegistry()

var (
	stKeyGet = KeyStmts.Add("keys.get", "SELECT wrapped_dek, kek_ref, revoked FROM data_keys WHERE key_ref = @p1", true)
	// A key there already is KEPT: two processes making the same key both
	// read it back and must meet on one (MySQL's ON DUPLICATE KEY UPDATE
	// key_ref=key_ref).
	stKeyPut = KeyStmts.Add("keys.put", `INSERT INTO data_keys (key_ref, wrapped_dek, kek_ref) SELECT @p1, @p2, @p3
		WHERE NOT EXISTS (SELECT 1 FROM data_keys WITH (UPDLOCK, HOLDLOCK) WHERE key_ref = @p1)`, false)
	stKeyByKEK  = KeyStmts.Add("keys.by_kek", "SELECT TOP (100000) COALESCE(kek_ref, N''), COUNT(*) FROM data_keys GROUP BY kek_ref", false)
	stKeyRevoke = KeyStmts.Add("keys.revoke", `UPDATE data_keys SET revoked = 1,
		updated_at = CASE WHEN revoked <> 1 THEN SYSUTCDATETIME() ELSE updated_at END WHERE key_ref = @p1`, false)
	stKeyTenant = KeyStmts.Add("keys.tenant_kms", `SELECT t.key_ref, t.dek_window_hours, e.id, COALESCE(JSON_VALUE(e.config, '$.kind'), N'')
		FROM tenant_kms_config AS t
		LEFT JOIN http_endpoints AS e ON e.id = t.kms_provider_id AND e.type = 'kms'
		WHERE t.company_id = @p1 AND t.status = 'active'`, true)
	stKeyAccountCompany = KeyStmts.Add("keys.account_company", "SELECT COALESCE(company_id, 0) FROM sms_service_config WHERE id = @p1", true)
	stKeyProvider       = KeyStmts.Add("keys.kms_provider", "SELECT COALESCE(config, N''), is_active FROM http_endpoints WHERE id = @p1 AND type = 'kms'", false)
	stKeyEndpoint       = KeyStmts.Add("keys.kms_endpoint", `SELECT id, COALESCE(name, N''), COALESCE(url, N''), COALESCE(method, N'POST'),
		COALESCE(headers, N'{}'), COALESCE(auth, N'{}'), COALESCE(timeout_ms, 0), COALESCE(cache_ttl_sec, 0), COALESCE(config, N'{}'), is_active,
		COALESCE(body_template, N'')
		FROM http_endpoints WHERE id = @p1 AND type = 'kms'`, false)
)

// DataKeys is the data keys and the KMS rows of one config store.
type DataKeys struct {
	db *sql.DB
}

var _ dal.DataKeys = (*DataKeys)(nil)

// NewDataKeys reads and writes the data keys on db (the config store).
func NewDataKeys(db *sql.DB) *DataKeys { return &DataKeys{db: db} }

// GetWrappedDEK returns a wrapped data key and whether it has been revoked.
func (k *DataKeys) GetWrappedDEK(ctx context.Context, keyRef string) (wrapped []byte, kekRef string, revoked, found bool, err error) {
	var rev int
	err = rRow(ctx, k.db, stKeyGet, stKeyGet.SQL, rText(keyRef)).Scan(&wrapped, &kekRef, &rev)
	if err == sql.ErrNoRows {
		return nil, "", false, false, nil
	}
	if err != nil {
		return nil, "", false, false, err
	}
	return wrapped, kekRef, rev != 0, true, nil
}

// PutWrappedDEK stores a wrapped data key, KEEPING any row that already
// exists.
func (k *DataKeys) PutWrappedDEK(ctx context.Context, keyRef string, wrapped []byte, kekRef string) error {
	_, err := rExec(ctx, k.db, stKeyPut, stKeyPut.SQL, rText(keyRef), wrapped, rText(kekRef))
	if IsDuplicate(err) {
		return nil // made by the other process: it stands
	}
	return err
}

// DataKeysByKEK counts the data keys under each key that seals them.
func (k *DataKeys) DataKeysByKEK(ctx context.Context) (map[string]int64, error) {
	rows, err := rQuery(ctx, k.db, stKeyByKEK, stKeyByKEK.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var ref string
		var n int64
		if err := rows.Scan(&ref, &n); err != nil {
			return nil, err
		}
		out[ref] += n
	}
	return out, rows.Err()
}

// RevokeDEK performs crypto-erasure: the row is kept, every value under the
// key becomes unreadable.
func (k *DataKeys) RevokeDEK(ctx context.Context, keyRef string) error {
	_, err := rExec(ctx, k.db, stKeyRevoke, stKeyRevoke.SQL, rText(keyRef))
	return err
}

// TenantKMS returns the active KMS assignment for a company, if any.
func (k *DataKeys) TenantKMS(ctx context.Context, companyID int64) (cry.TenantConfig, bool, error) {
	var cfg cry.TenantConfig
	var kind sql.NullString
	var pid sql.NullInt64
	err := rRow(ctx, k.db, stKeyTenant, stKeyTenant.SQL, companyID).Scan(&cfg.KeyRef, &cfg.DEKWindowHours, &pid, &kind)
	cfg.ProviderID, cfg.ProviderKind = pid.Int64, kind.String
	if err == sql.ErrNoRows {
		return cry.TenantConfig{}, false, nil
	}
	if err != nil {
		return cry.TenantConfig{}, false, err
	}
	return cfg, true, nil
}

// AccountCompany is the company an account belongs to (0 = none).
func (k *DataKeys) AccountCompany(ctx context.Context, accountID int64) (int64, error) {
	var id int64
	err := rRow(ctx, k.db, stKeyAccountCompany, stKeyAccountCompany.SQL, accountID).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// KMSProvider loads a KMS provider row (internal/kms.HTTPProvider).
func (k *DataKeys) KMSProvider(ctx context.Context, id int64) (kms.HTTPProvider, bool, error) {
	var cfgJSON string
	var active int
	err := rRow(ctx, k.db, stKeyProvider, stKeyProvider.SQL, id).Scan(&cfgJSON, &active)
	if err == sql.ErrNoRows {
		return kms.HTTPProvider{}, false, nil
	}
	if err != nil {
		return kms.HTTPProvider{}, false, err
	}
	var p kms.HTTPProvider
	if cfgJSON != "" {
		if err := json.Unmarshal([]byte(cfgJSON), &p); err != nil {
			return kms.HTTPProvider{}, false, fmt.Errorf("KMS provider %d: config is not valid JSON: %v", id, err)
		}
	}
	p.Active = active != 0
	return p, true, nil
}

// KMSEndpoint loads one KMS request row in the shape the shared builder
// takes.
func (k *DataKeys) KMSEndpoint(ctx context.Context, id int64) (kms.Endpoint, bool, error) {
	var ep kms.Endpoint
	var headers, auth, cfgJSON string
	var active int
	err := rRow(ctx, k.db, stKeyEndpoint, stKeyEndpoint.SQL, id).
		Scan(&ep.Row.ID, &ep.Row.Name, &ep.Row.URL, &ep.Row.Method, &headers, &auth,
			&ep.Row.TimeoutMS, &ep.Row.CacheTTLSec, &cfgJSON, &active, &ep.Row.BodyTemplate)
	if err == sql.ErrNoRows {
		return kms.Endpoint{}, false, nil
	}
	if err != nil {
		return kms.Endpoint{}, false, err
	}
	_ = json.Unmarshal([]byte(headers), &ep.Row.Headers)
	_ = json.Unmarshal([]byte(auth), &ep.Row.Auth)
	if err := json.Unmarshal([]byte(cfgJSON), &ep.Row.Config); err != nil {
		return kms.Endpoint{}, false, fmt.Errorf("KMS endpoint %d: config is not valid JSON: %v", id, err)
	}
	ep.Role, _ = ep.Row.Config["kms_role"].(string)
	ep.Active = active != 0
	return ep, true, nil
}

// ---- vendor_sessions: the HTTP vendors' shared login sessions ----

var (
	stSessLoad = KeyStmts.Add("sessions.load", "SELECT vars, expires_at FROM vendor_sessions WHERE vendor_id = @p1", false)
	stSessSave = AddUpsert(KeyStmts, "sessions.save", UpsertSpec{
		Table: "vendor_sessions", Keys: []string{"vendor_id"},
		Set:    []string{"cid = $cid", "vars = $vars", "expires_at = $expires_at", "updated_at = SYSUTCDATETIME()"},
		Insert: []string{"vendor_id", "cid", "vars", "expires_at"},
	}, false)
	stSessDelete = KeyStmts.Add("sessions.delete", "DELETE FROM vendor_sessions WHERE vendor_id = @p1", false)
	stSessLock   = KeyStmts.Add("sessions.lock", `SET NOCOUNT ON;
DECLARE @yk_got INT;
EXEC @yk_got = sp_getapplock @Resource = @p1, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 5000, @DbPrincipal = 'public';
SELECT @yk_got`, false)
)

// VendorSessions is the vendor sessions of one database (the HLR store).
type VendorSessions struct {
	db *sql.DB
}

var _ dal.VendorSessions = (*VendorSessions)(nil)

// NewVendorSessions keeps the vendor sessions on db.
func NewVendorSessions(db *sql.DB) *VendorSessions { return &VendorSessions{db: db} }

// LoadVendorSession returns the shared session vars and their expiry (zero
// values when no row exists). The expiry is compared in Go only.
func (v *VendorSessions) LoadVendorSession(ctx context.Context, vendorID int64) (map[string]string, time.Time, error) {
	var raw string
	var exp time.Time
	err := rRow(ctx, v.db, stSessLoad, stSessLoad.SQL, vendorID).Scan(&raw, &exp)
	if err == sql.ErrNoRows {
		return nil, time.Time{}, nil
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	vars := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &vars); err != nil {
		return nil, time.Time{}, err
	}
	return vars, exp.UTC(), nil
}

// SaveVendorSession upserts the vendor's shared session (last writer wins).
func (v *VendorSessions) SaveVendorSession(ctx context.Context, vendorID int64, cid string, vars map[string]string, expiresAt time.Time) error {
	b, err := json.Marshal(vars)
	if err != nil {
		return err
	}
	_, err = stSessSave.Exec(ctx, v.db, sql.Named("vendor_id", vendorID), sql.Named("cid", rText(cid)),
		sql.Named("vars", rText(string(b))), sql.Named("expires_at", Time(expiresAt)))
	return err
}

// DeleteVendorSession drops the shared session row.
func (v *VendorSessions) DeleteVendorSession(ctx context.Context, vendorID int64) error {
	_, err := rExec(ctx, v.db, stSessDelete, stSessDelete.SQL, vendorID)
	return err
}

// LockVendorLogin serializes vendor logins across all instances with an
// application lock held on a pinned connection. Waits up to 5 s; a timeout
// is an error (the caller then logs in unserialized).
func (v *VendorSessions) LockVendorLogin(ctx context.Context, vendorID int64) (func(), error) {
	conn, err := v.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("yaku_vendor_login_%d", vendorID)
	var got int
	if err := conn.QueryRowContext(ctx, stSessLock.SQL, rText(name)).Scan(&got); err != nil {
		dropConn(conn) // the lock may have been granted before the error
		return nil, err
	}
	if got < 0 {
		_ = conn.Close()
		return nil, fmt.Errorf("vendor login lock %s: timed out", name)
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(ctx, stLockRelease.SQL, rText(name)); err != nil {
			dropConn(conn)
			return
		}
		_ = conn.Close()
	}, nil
}
