-- ============================================================================
-- Hadal architecture reconciliation (PO 2026-07-27, Salman + Saleem review):
-- sms_service_config replaces the accounts + account_dialects split with ONE
-- table — Hadal's table name, our column names/conventions throughout
-- (Salman's explicit preference: "keep my variable name from yakugate db, but
-- table names from hadal db where possible").
--
-- accounts/account_dialects are NOT dropped in this migration. The Go engine
-- (internal/refdata's loader) and every existing query still read them —
-- this is the "expand" half of an expand/contract migration; a follow-up code
-- pass repoints the loader at this table before the old tables are retired.
--
-- Four decisions behind the new columns (2026-07-27):
--   1. Rate cards: BOTH per-account (default_*_rate_card_id, fallback only)
--      AND per-route (existing LCR) — the ROUTE wins when it specifies one;
--      these columns are read only when a route doesn't.
--   2. Reseller: is_reseller + default_reselling_*_rate_card_id land here;
--      a matching is_reseller flag also lives on `roles` (admin/migrations
--      0003) — reseller is BOTH an account-billing mode and a portal-role
--      concept, not an either/or.
--   3. Throughput: three-way split (mt/mo/dlr) HERE — unlike `companies`,
--      which explicitly keeps its single max_throughput (Salman: "we dont
--      need it in companies... we will be working with 3 tps in sms service
--      config").
--   4. HLR/KMS: hlr_provider_ids/kms_provider_ids (JSON arrays of ids into
--      the new hlr_providers/kms_providers catalogs, migration 0030) instead
--      of Hadal's free-form per-account provider blob.
-- ============================================================================

CREATE TABLE sms_service_config (
  id                       BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  customer_id              CHAR(36) NOT NULL,
  company_id               BIGINT UNSIGNED DEFAULT NULL,
  username                 VARCHAR(64) NOT NULL,
  name                     VARCHAR(128) DEFAULT NULL,
  email                    VARCHAR(190) DEFAULT NULL,
  phone                    VARCHAR(32) DEFAULT NULL,
  password_hash            VARCHAR(100) DEFAULT NULL,
  jwt                      JSON NOT NULL,
  dlr_url                  VARCHAR(512) DEFAULT NULL,
  http                     TINYINT(1) NOT NULL DEFAULT 1,
  smpp                     TINYINT(1) NOT NULL DEFAULT 0,
  smpp_config              JSON DEFAULT NULL,
  allowed_sender_ids       JSON NOT NULL,
  allowed_ip_list          JSON DEFAULT NULL,
  enforce_ip_check         TINYINT(1) NOT NULL DEFAULT 0,
  is_disabled              TINYINT(1) NOT NULL DEFAULT 0,
  enforce_account_expiry   DATE DEFAULT NULL,
  enforce_spam_check       TINYINT(1) NOT NULL DEFAULT 1,
  enforce_spam_strict      TINYINT(1) NOT NULL DEFAULT 0,
  enforce_template_check   TINYINT(1) NOT NULL DEFAULT 0,
  enforce_auto_replace     TINYINT(1) NOT NULL DEFAULT 0,
  enforce_duplicate_check  TINYINT(1) NOT NULL DEFAULT 1,
  enforce_hlr_db_check     TINYINT(1) NOT NULL DEFAULT 1,
  enforce_hlr_dip_url      VARCHAR(512) DEFAULT NULL,
  body_privacy             ENUM('plain_text','pii_masked','all_encrypted') NOT NULL DEFAULT 'plain_text',
  prepaid_billing          TINYINT(1) NOT NULL DEFAULT 0,
  balance                  DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  quota                    JSON NOT NULL,
  dedupe_ttl_sec           INT UNSIGNED DEFAULT NULL,
  mt_throughput            INT NOT NULL DEFAULT 0,
  mo_throughput            INT NOT NULL DEFAULT 0,
  dlr_throughput           INT NOT NULL DEFAULT 0,
  max_recipients           INT NOT NULL DEFAULT 100,
  quiet_range              VARCHAR(11) DEFAULT NULL,
  timezone                 VARCHAR(64) NOT NULL DEFAULT 'UTC',
  portal_data_retention    INT NOT NULL DEFAULT 90,
  archival_data_retention  INT NOT NULL DEFAULT 365,
  is_whitelist_number      TINYINT(1) NOT NULL DEFAULT 0,
  whitelist_numbers        JSON DEFAULT NULL,
  mo_url                   VARCHAR(512) DEFAULT NULL,
  mo_method                VARCHAR(8) DEFAULT 'GET',
  -- account_dialects, merged in:
  url                      VARCHAR(190) NOT NULL DEFAULT '/sms/v4/send',
  url_exclusive            VARCHAR(190) GENERATED ALWAYS AS (NULLIF(url, '/sms/v4/send')) STORED,
  api_password             VARCHAR(100) DEFAULT NULL,
  request_map              JSON DEFAULT NULL,
  response_map             JSON DEFAULT NULL,
  auth_config              JSON DEFAULT NULL,
  token_hash               CHAR(64) DEFAULT NULL,
  keep_unmapped            TINYINT(1) DEFAULT NULL,
  -- new from Hadal, agreed 2026-07-27:
  log_level                VARCHAR(32) DEFAULT NULL,
  send_sms_cost            TINYINT(1) NOT NULL DEFAULT 1,
  template_approval_level  VARCHAR(32) NOT NULL DEFAULT 'NONE',
  octates_to_septets_unpacking TINYINT(1) NOT NULL DEFAULT 0,
  -- decision 1: rate cards, per-account fallback (route still wins when set)
  default_mt_rate_card_id  BIGINT UNSIGNED DEFAULT NULL,
  default_mo_rate_card_id  BIGINT UNSIGNED DEFAULT NULL,
  default_dlr_rate_card_id BIGINT UNSIGNED DEFAULT NULL,
  -- decision 2: reseller
  is_reseller              TINYINT(1) NOT NULL DEFAULT 0,
  default_reselling_mt_rate_card_id  BIGINT UNSIGNED DEFAULT NULL,
  default_reselling_mo_rate_card_id  BIGINT UNSIGNED DEFAULT NULL,
  default_reselling_dlr_rate_card_id BIGINT UNSIGNED DEFAULT NULL,
  -- decision 4: HLR/KMS provider selection, one or more from our catalog
  hlr_provider_ids         JSON DEFAULT NULL,
  kms_provider_ids         JSON DEFAULT NULL,
  created_by_user_id       VARCHAR(64) DEFAULT NULL,
  created_at               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_ssc_customer_id (customer_id),
  UNIQUE KEY uq_ssc_username (username),
  UNIQUE KEY uq_ssc_url_exclusive (url_exclusive),
  KEY idx_ssc_company (company_id),
  KEY idx_ssc_token_hash (token_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Backfill from the existing split tables. mt/mo/dlr throughput are all
-- seeded from the single existing `throughput` value (preserves today's
-- effective cap identically across all three — an operator can split them
-- apart deliberately later; this migration does not decide they diverge).
-- hlr/kms provider lists seed to the one real catalog row each (0030) so
-- every backfilled account has something valid to reference.
INSERT INTO sms_service_config (
  id, customer_id, company_id, username, name, email, phone, password_hash, jwt,
  dlr_url, http, smpp, smpp_config, allowed_sender_ids, allowed_ip_list,
  enforce_ip_check, is_disabled, enforce_account_expiry, enforce_spam_check,
  enforce_spam_strict, enforce_template_check, enforce_auto_replace,
  enforce_duplicate_check, enforce_hlr_db_check, enforce_hlr_dip_url,
  body_privacy, prepaid_billing, balance, quota, dedupe_ttl_sec,
  mt_throughput, mo_throughput, dlr_throughput, max_recipients, quiet_range,
  timezone, portal_data_retention, archival_data_retention,
  is_whitelist_number, whitelist_numbers, mo_url, mo_method,
  url, api_password, request_map, response_map, auth_config, token_hash, keep_unmapped,
  hlr_provider_ids, kms_provider_ids,
  created_by_user_id, created_at, updated_at
)
SELECT
  a.id, a.customer_id, a.company_id, a.username, a.name, a.email, a.phone, a.password_hash, a.jwt,
  a.dlr_url, a.http, a.smpp, a.smpp_config, a.allowed_sender_ids, a.allowed_ip_list,
  a.enforce_ip_check, a.is_disabled, a.enforce_account_expiry, a.enforce_spam_check,
  a.enforce_spam_strict, a.enforce_template_check, a.enforce_auto_replace,
  a.enforce_duplicate_check, a.enforce_hlr_db_check, a.enforce_hlr_dip_url,
  a.body_privacy, a.prepaid_billing, a.balance, a.quota, a.dedupe_ttl_sec,
  a.throughput, a.throughput, a.throughput, a.max_recipients, a.quiet_range,
  a.timezone, a.portal_data_retention, a.archival_data_retention,
  a.is_whitelist_number, a.whitelist_numbers, a.mo_url, a.mo_method,
  COALESCE(d.url, '/sms/v4/send'), d.api_password, d.request_map, d.response_map, d.auth_config, d.token_hash, d.keep_unmapped,
  JSON_ARRAY((SELECT id FROM hlr_providers WHERE name = 'default-db-hlr')),
  JSON_ARRAY((SELECT id FROM kms_providers WHERE name = 'default-file-kms')),
  a.created_by_user_id, a.created_at, a.updated_at
FROM accounts a
LEFT JOIN account_dialects d ON d.account_id = a.id;
