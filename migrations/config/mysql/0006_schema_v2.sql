-- ============================================================================
-- 0006  Schema v2 consolidation — CONFIG store (MySQL)
-- Rebuilds the config store to match database-structure.docx v2.0 (2026-07-13).
-- Pre-alpha, disposable data: changed tables are DROP+CREATE'd to the final
-- shape rather than ALTERed column-by-column. This is an ADDITION (new file),
-- not an edit of an applied migration. Standing conventions: *_regex = JSON
-- {match,replace}; is_disabled on/off; created_by_user_id everywhere; config
-- groups as JSON; JWT-only auth; destination_country_code vocabulary.
-- ============================================================================

-- Disable FK checks during the rebuild so drop order can't be blocked by a
-- cross-table constraint (routes -> rate_cards etc. from the pre-v2 schema).
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS rate_card_rates;
DROP TABLE IF EXISTS rate_cards;
DROP TABLE IF EXISTS vendor_status_codes;
DROP TABLE IF EXISTS auto_replace;      -- moved to QUEUE store as auto_replace_text
DROP TABLE IF EXISTS account_balance;   -- folded into accounts.balance + quota
DROP TABLE IF EXISTS sender_ids;        -- folded into accounts.allowed_sender_ids
DROP TABLE IF EXISTS country_rules;     -- renamed quota_by_countries
DROP TABLE IF EXISTS quota_by_countries;
DROP TABLE IF EXISTS balance_history;
DROP TABLE IF EXISTS webhooks;          -- moved to QUEUE store
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS operators;
DROP TABLE IF EXISTS templates;
DROP TABLE IF EXISTS spam_keywords;
DROP TABLE IF EXISTS blocklist;
DROP TABLE IF EXISTS vendors;
DROP TABLE IF EXISTS accounts;

SET FOREIGN_KEY_CHECKS = 1;

-- 1.1 accounts -----------------------------------------------------------------
CREATE TABLE accounts (
  id                      BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  customer_id             CHAR(36) NOT NULL UNIQUE,
  username                VARCHAR(64) NOT NULL UNIQUE,
  name                    VARCHAR(128),
  email                   VARCHAR(190),
  phone                   VARCHAR(32),
  password_hash           VARCHAR(100) NULL,               -- bcrypt; PORTAL login only
  jwt                     JSON NOT NULL,                   -- {alg,secret_ref,public_key,jwks_uri,issuer,audience,sub_claim,leeway_sec}
  http                    TINYINT(1) NOT NULL DEFAULT 1,
  smpp                    TINYINT(1) NOT NULL DEFAULT 0,
  allowed_sender_ids      JSON NOT NULL,                   -- ["BRAND","1234"] or ["*"]
  allowed_ip_list         JSON NULL,
  enforce_ip_check        TINYINT(1) NOT NULL DEFAULT 0,
  is_disabled             TINYINT(1) NOT NULL DEFAULT 0,
  enforce_account_expiry  DATE NULL,
  enforce_spam_check      TINYINT(1) NOT NULL DEFAULT 1,
  enforce_template_check  TINYINT(1) NOT NULL DEFAULT 0,
  enforce_auto_replace    TINYINT(1) NOT NULL DEFAULT 0,
  enforce_duplicate_check TINYINT(1) NOT NULL DEFAULT 1,
  enforce_hlr_db_check    TINYINT(1) NOT NULL DEFAULT 1,
  enforce_hlr_dip_url     VARCHAR(512) NULL,
  body_privacy            ENUM('plain_text','pii_masked','all_encrypted') NOT NULL DEFAULT 'plain_text',
  prepaid_billing         TINYINT(1) NOT NULL DEFAULT 0,
  balance                 DECIMAL(14,4) NOT NULL DEFAULT 0,
  quota                   JSON NOT NULL,                   -- {daily_quota,daily_used,monthly_quota,monthly_used}
  throughput              INT NOT NULL DEFAULT 0,
  max_recipients          INT NOT NULL DEFAULT 100,
  quiet_range             VARCHAR(11) NULL,                -- 'HH:MM-HH:MM'
  timezone                VARCHAR(64) NOT NULL DEFAULT 'UTC',
  portal_data_retention   INT NOT NULL DEFAULT 90,
  archival_data_retention INT NOT NULL DEFAULT 365,
  is_whitelist_number     TINYINT(1) NOT NULL DEFAULT 0,
  whitelist_numbers       JSON NULL,
  mo_url                  VARCHAR(512) NULL,
  mo_method               VARCHAR(8) DEFAULT 'GET',
  created_by_user_id      VARCHAR(64) NULL,
  created_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at              DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.2 vendors ------------------------------------------------------------------
CREATE TABLE vendors (
  id                 BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  cid                VARCHAR(64) NOT NULL UNIQUE,
  name               VARCHAR(128),
  transport          ENUM('smpp','http') NOT NULL,
  service_status     ENUM('active','paused','disabled') NOT NULL DEFAULT 'active',
  submit_throughput  INT NOT NULL DEFAULT 0,
  queue_config       JSON NOT NULL,
  dlr_config         JSON NOT NULL,
  log_config         JSON NOT NULL,
  smpp_config        JSON NULL,
  http_config        JSON NULL,
  credential_ref     VARCHAR(128),
  created_by_user_id  VARCHAR(64) NULL,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.3 vendor_status_codes ------------------------------------------------------
CREATE TABLE vendor_status_codes (
  id          BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  vendor_id   BIGINT UNSIGNED NOT NULL,
  scope       ENUM('submit','dlr') NOT NULL DEFAULT 'submit',
  code        INT UNSIGNED NOT NULL,
  action      ENUM('success','retry','permanent') NOT NULL,
  description VARCHAR(190),
  UNIQUE KEY uq_vendor_code (vendor_id, scope, code),
  CONSTRAINT fk_vsc_vendor FOREIGN KEY (vendor_id) REFERENCES vendors(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.4 routes -------------------------------------------------------------------
CREATE TABLE routes (
  id                   BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name                 VARCHAR(128) NULL,
  priority             INT NOT NULL DEFAULT 100,
  route_type           ENUM('direct','failover','loadbalance','blackhole','hq','lcr') NOT NULL DEFAULT 'direct',
  weight               INT NOT NULL DEFAULT 100,
  account_id           BIGINT UNSIGNED NULL,
  match_mcc            VARCHAR(3) NULL,
  match_mnc            VARCHAR(3) NULL,
  sender_regex         JSON NULL,
  receiver_regex       JSON NULL,
  keyword_regex        JSON NULL,
  vendor_id            BIGINT UNSIGNED NULL,
  buying_rate_card_id  INT UNSIGNED NULL,
  selling_rate_card_id INT UNSIGNED NULL,
  is_disabled          TINYINT(1) NOT NULL DEFAULT 0,
  created_by_user_id   VARCHAR(64) NULL,
  created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_lookup (account_id, match_mcc, match_mnc, priority)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.5 operators ----------------------------------------------------------------
CREATE TABLE operators (
  mcc                VARCHAR(3) NOT NULL,
  mnc                VARCHAR(3) NOT NULL,
  npi                VARCHAR(50) NOT NULL,            -- comma-separated mobile prefixes
  country_code       VARCHAR(4) NOT NULL,
  country_name       VARCHAR(64) NOT NULL,
  name               VARCHAR(64) NULL,
  allow_mcc_only     TINYINT(1) NOT NULL DEFAULT 0,
  created_by_user_id VARCHAR(64) NULL,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (mcc, mnc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.6 templates ----------------------------------------------------------------
CREATE TABLE templates (
  id                 BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id         BIGINT UNSIGNED NOT NULL,
  original_text      TEXT NOT NULL,
  regex              JSON NOT NULL,                   -- {"match": ...}
  variables          JSON NULL,
  channel            VARCHAR(32) NULL,
  priority           ENUM('OTP','TRANSACTIONAL','PROMOTIONAL','DEFAULT') NOT NULL DEFAULT 'DEFAULT',
  status             ENUM('pending','approved','rejected') NOT NULL DEFAULT 'pending',
  lastaction_by      VARCHAR(64) NULL,
  lastaction_at      DATETIME NULL,
  rejection_reason   VARCHAR(190) NULL,
  created_by_user_id VARCHAR(64) NULL,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_account (account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.7 spam_keywords (one row = one list) ---------------------------------------
CREATE TABLE spam_keywords (
  id                       BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id               BIGINT UNSIGNED NULL,           -- NULL = universal
  destination_country_code VARCHAR(6) NULL,                -- NULL = all destinations
  list_type                ENUM('blocked','allowed') NOT NULL,
  category                 VARCHAR(32) NOT NULL DEFAULT 'general',
  keywords                 JSON NOT NULL,
  keyword_regex            JSON NULL,
  created_by_user_id       VARCHAR(64) NULL,
  created_at               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_list (account_id, destination_country_code, list_type, category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.8 blocklist ----------------------------------------------------------------
CREATE TABLE blocklist (
  id                 BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  msisdn             VARCHAR(15) NOT NULL,
  account_id         BIGINT UNSIGNED NULL,           -- NULL = global DND
  sender_mask        VARCHAR(32) NULL,               -- NULL = all senders
  source             ENUM('manual','unsub','abuse') NOT NULL DEFAULT 'manual',
  reason             VARCHAR(190) NULL,
  is_disabled        TINYINT(1) NOT NULL DEFAULT 0,
  created_by_user_id VARCHAR(64) NULL,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_block (msisdn, account_id, sender_mask),
  KEY idx_msisdn (msisdn)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.9 quota_by_countries (renamed from country_rules) --------------------------
CREATE TABLE quota_by_countries (
  id                       BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id               BIGINT UNSIGNED NULL,           -- NULL = global rule
  destination_country_code VARCHAR(6) NOT NULL,
  mode                     ENUM('allow','deny') NOT NULL DEFAULT 'allow',
  quota                    JSON NULL,
  is_disabled              TINYINT(1) NOT NULL DEFAULT 0,
  created_by_user_id       VARCHAR(64) NULL,
  created_at               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_country (account_id, destination_country_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.10/1.11 rate cards ---------------------------------------------------------
CREATE TABLE rate_cards (
  id                 INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name               VARCHAR(128) NOT NULL UNIQUE,
  kind               ENUM('buy','sell') NOT NULL,
  currency           CHAR(3) NOT NULL DEFAULT 'USD',
  is_disabled        TINYINT(1) NOT NULL DEFAULT 0,
  created_by_user_id VARCHAR(64) NULL,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE rate_card_rates (
  id                       INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  rate_card_id             INT UNSIGNED NOT NULL,
  match_mcc                VARCHAR(3) NULL,
  match_mnc                VARCHAR(3) NULL,
  destination_country_code VARCHAR(6) NULL,
  price_per_segment        DECIMAL(12,6) NOT NULL,
  effective_from           DATE NULL,
  created_at               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_card (rate_card_id, match_mcc, match_mnc, destination_country_code),
  CONSTRAINT fk_rate_card FOREIGN KEY (rate_card_id) REFERENCES rate_cards(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 1.12 balance_history (append-only) -------------------------------------------
CREATE TABLE balance_history (
  id               BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id       BIGINT UNSIGNED NOT NULL,
  type             ENUM('topup','reserve','commit','refund','adjust') NOT NULL,
  amount           DECIMAL(14,4) NOT NULL,
  previous_balance DECIMAL(14,4) NOT NULL,
  new_balance      DECIMAL(14,4) NOT NULL,
  reference        VARCHAR(64) NULL,
  reason           VARCHAR(190) NULL,
  created_by_user_id VARCHAR(64) NULL,
  created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_account (account_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
