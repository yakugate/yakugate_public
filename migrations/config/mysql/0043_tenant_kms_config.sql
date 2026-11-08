-- Multi-tenant KMS, schema half (PO 2026-07-28): "there may be multiple gcp
-- projects, you need to create a table, we will store records there."
--
-- One row per TENANT (companies.id, not sms_service_config — the KEK is
-- per-company per the eOcean multi-tenant KMS architecture doc, not
-- per-account). kms_provider_id points at the kms_providers catalog entry
-- for WHICH backend/project this tenant is on — kms_providers already
-- supports many rows (e.g. one per GCP project, since a deployment may use
-- several), so "multiple GCP projects" needs no schema change there, only
-- more catalog rows plus one tenant_kms_config row per tenant pointing at
-- the right one.
--
-- key_ref is the full provider KEK handle (self-contained, e.g.
-- "gcp-kms://projects/<project>/locations/<location>/keyRings/<ring>/cryptoKeys/<key>"
-- per kms-provider-connections.md's ref-scheme table) rather than assembled
-- from separate project/location/ring columns — a row is readable on its own
-- without joining back to kms_providers for the exact path, and the same
-- shape works whichever provider `kms_provider_id` names (AWS/Azure/HSM
-- refs look different but are still one self-contained string).
--
-- credential_ref is nullable: most tenants use the credential already
-- configured at the provider/project level (kms_providers.config); a
-- specific tenant needing its OWN service account/credential (true
-- multi-project BYOK) overrides it here.
--
-- dek_window_hours defaults to 24 (daily, PO 2026-07-28 decision) rather
-- than hardcoding "daily" as an enum — an hourly or other window is then a
-- data change, not a migration, if a tenant's compliance team ever asks for
-- tighter granularity.
--
-- No seed data — schema only, same "table now, real records once the
-- customer relationship exists" pattern as every other new-capability table
-- in this reconciliation (cr_providers, hlr_providers, deployments, etc.).
-- Nothing reads or writes this table yet; internal/crypto's multi-tenant
-- rewire is separate follow-up work, not part of this migration.
CREATE TABLE tenant_kms_config (
  id                    BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  company_id            BIGINT UNSIGNED NOT NULL,
  kms_provider_id       BIGINT UNSIGNED NOT NULL,
  key_ref               VARCHAR(500) NOT NULL,
  credential_ref        VARCHAR(255) DEFAULT NULL,
  rotation_period_days  INT NOT NULL DEFAULT 90,
  dek_window_hours      INT NOT NULL DEFAULT 24,
  status                ENUM('active','disabled','decommissioned') NOT NULL DEFAULT 'active',
  last_rotated_at       DATETIME DEFAULT NULL,
  created_by_user_id    VARCHAR(64) DEFAULT NULL,
  created_at            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_tenant_kms_company (company_id),
  KEY idx_tenant_kms_provider (kms_provider_id),
  KEY idx_tenant_kms_status (status),
  CONSTRAINT fk_tenant_kms_company FOREIGN KEY (company_id) REFERENCES companies(id),
  CONSTRAINT fk_tenant_kms_provider FOREIGN KEY (kms_provider_id) REFERENCES kms_providers(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
