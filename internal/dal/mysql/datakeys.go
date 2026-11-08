package mysql

// Data keys (envelope encryption) and each company's KMS, in the config
// store.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	cry "github.com/yakugate/yakugate/internal/crypto"
	"github.com/yakugate/yakugate/internal/kms"
)

// --- envelope-encryption keyring (data_keys, CONFIG store) [SCOPE P1-CRYPTO] ---

// GetWrappedDEK returns a wrapped data key and whether it has been revoked.
func (s *Store) GetWrappedDEK(ctx context.Context, keyRef string) (wrapped []byte, kekRef string, revoked, found bool, err error) {
	var rev int
	err = s.db.QueryRowContext(ctx,
		"SELECT wrapped_dek, kek_ref, revoked FROM data_keys WHERE key_ref=?", keyRef).
		Scan(&wrapped, &kekRef, &rev)
	if err == sql.ErrNoRows {
		return nil, "", false, false, nil
	}
	if err != nil {
		return nil, "", false, false, err
	}
	return wrapped, kekRef, rev != 0, true, nil
}

// PutWrappedDEK stores a wrapped data key, KEEPING any row that already exists.
// Two processes may create the same account-month key concurrently; both then
// re-read, so they must converge on one key — overwriting here would make data
// written by the loser permanently unreadable. [SCOPE P1-CRYPTO]
func (s *Store) PutWrappedDEK(ctx context.Context, keyRef string, wrapped []byte, kekRef string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO data_keys (key_ref, wrapped_dek, kek_ref) VALUES (?,?,?)
		 ON DUPLICATE KEY UPDATE key_ref=key_ref`, keyRef, wrapped, kekRef)
	return err
}

// DataKeysByKEK counts the data keys under each key that seals them
// (data_keys.kek_ref): crypto.DataKeyCounter. A start that finds its
// gateway key file missing asks it, and makes no new key once any data key
// is sealed under a gateway key (go-live P8 A4, 2026-10-18).
func (s *Store) DataKeysByKEK(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT COALESCE(kek_ref,''), COUNT(*) FROM data_keys GROUP BY kek_ref")
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

// RevokeDEK performs crypto-erasure: the row is kept for audit, but every value
// encrypted under this key becomes permanently unreadable. [SCOPE P1-CRYPTO]
func (s *Store) RevokeDEK(ctx context.Context, keyRef string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE data_keys SET revoked=1 WHERE key_ref=?", keyRef)
	return err
}

// --- multi-tenant KMS config (tenant_kms_config, CONFIG store) [SCOPE P1-KMS-MULTITENANT] ---

// TenantKMS returns the active KMS assignment for a company, if any.
// found=false (nil error) means the company has no row — the caller
// (internal/crypto) falls back to the gateway-wide default KEK, never an
// error, since most tenants are never onboarded onto their own KEK at all.
//
// The provider (kms_provider_id -> an http_endpoints row of type 'kms') comes
// along with its kind, so internal/crypto can build the master key's full ref
// (http-kms://<provider>/<key>). A provider row that is missing or of another
// type leaves ProviderID 0, and crypto refuses that company rather than guess.
func (s *Store) TenantKMS(ctx context.Context, companyID int64) (cry.TenantConfig, bool, error) {
	var cfg cry.TenantConfig
	var kind sql.NullString
	var pid sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT t.key_ref, t.dek_window_hours, e.id,
		        COALESCE(JSON_UNQUOTE(JSON_EXTRACT(e.config,'$.kind')),'')
		   FROM tenant_kms_config t
		   LEFT JOIN http_endpoints e ON e.id = t.kms_provider_id AND e.type = 'kms'
		  WHERE t.company_id=? AND t.status='active'`,
		companyID).Scan(&cfg.KeyRef, &cfg.DEKWindowHours, &pid, &kind)
	cfg.ProviderID, cfg.ProviderKind = pid.Int64, kind.String
	if err == sql.ErrNoRows {
		return cry.TenantConfig{}, false, nil
	}
	if err != nil {
		return cry.TenantConfig{}, false, err
	}
	return cfg, true, nil
}

// AccountCompany is the company an account belongs to (0 = none), for
// crypto.EncryptAccount: the receipt path knows a message's account, and an
// encrypted receipt must use the same key as the company's messages.
func (s *Store) AccountCompany(ctx context.Context, accountID int64) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(company_id,0) FROM sms_service_config WHERE id=?`, accountID).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// KMSProvider loads a KMS provider row: an http_endpoints row of type 'kms'
// whose config names its encrypt and decrypt request rows
// (internal/kms.HTTPProvider). found=false when there is no such row.
func (s *Store) KMSProvider(ctx context.Context, id int64) (kms.HTTPProvider, bool, error) {
	var cfgJSON string
	var active int
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(CAST(config AS CHAR),''), is_active FROM http_endpoints WHERE id=? AND type='kms'`, id).
		Scan(&cfgJSON, &active)
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

// KMSEndpoint loads one KMS request row (an encrypt or decrypt call) in the
// shape the shared builder takes — the same read the FX resolver and the HLR
// dip make for their rows.
func (s *Store) KMSEndpoint(ctx context.Context, id int64) (kms.Endpoint, bool, error) {
	var ep kms.Endpoint
	var headers, auth, cfgJSON string
	var active int
	err := s.db.QueryRowContext(ctx, `
		SELECT id, COALESCE(name,''), COALESCE(url,''), COALESCE(method,'POST'),
		       COALESCE(CAST(headers AS CHAR),'{}'), COALESCE(CAST(auth AS CHAR),'{}'),
		       COALESCE(timeout_ms,0), COALESCE(cache_ttl_sec,0), COALESCE(CAST(config AS CHAR),'{}'), is_active,
		       COALESCE(body_template,'')
		  FROM http_endpoints WHERE id=? AND type='kms'`, id).
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
