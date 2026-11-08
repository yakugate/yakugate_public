-- ============================================================================
-- Yakugate — STORE 2: CONFIGURATIONS  (slow-changing reference/config data)
-- Read into the in-memory cache at startup + periodic refresh. Backend:
-- MySQL / MSSQL / MongoDB. This is the source of truth for accounts, routing,
-- vendors, templates, and all anti-abuse config.
-- ============================================================================

CREATE TABLE accounts (
  id                 BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  customer_id        CHAR(36) NOT NULL UNIQUE,
  username           VARCHAR(64) NOT NULL UNIQUE,
  password_hash      VARCHAR(100) NOT NULL,            -- bcrypt
  secret_key         VARCHAR(128),
  name               VARCHAR(128), email VARCHAR(190), phone VARCHAR(32),
  http               TINYINT(1) NOT NULL DEFAULT 1,
  smpp               TINYINT(1) NOT NULL DEFAULT 0,
  soap               TINYINT(1) NOT NULL DEFAULT 0,
  allowed_ip_list    JSON,                             -- ["10.0.0.0/32"]
  ip_check           TINYINT(1) NOT NULL DEFAULT 0,    -- enforce allowed_ip_list
  is_disabled        TINYINT(1) NOT NULL DEFAULT 0,
  spam_check         TINYINT(1) NOT NULL DEFAULT 1,
  template_check     TINYINT(1) NOT NULL DEFAULT 0,    -- only approved templates may send
  auto_replace_check TINYINT(1) NOT NULL DEFAULT 0,
  prepaid_billing    TINYINT(1) NOT NULL DEFAULT 0,
  throughput         TINYINT(1) NOT NULL DEFAULT 0,
  throughput_limit   INT,
  campaign_tps_limit INT,
  local_capping      TINYINT(1) NOT NULL DEFAULT 0,
  local_capping_limit INT,
  quiet_start        INT NULL,                         -- minutes-of-day, inclusive
  quiet_end          INT NULL,                         -- minutes-of-day, exclusive
  is_whitelist_number TINYINT(1) NOT NULL DEFAULT 0,
  whitelist_numbers  JSON,
  retention_days     INT NOT NULL DEFAULT 90,          -- hot->cold window for this account's records
  timezone           VARCHAR(64) NOT NULL DEFAULT 'UTC',  -- IANA tz; drives per-user service-day/month boundary
  mo_url             VARCHAR(512), mo_method VARCHAR(8) DEFAULT 'GET',
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sender_ids (
  id          BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id  BIGINT UNSIGNED NOT NULL,
  mask        VARCHAR(32) NOT NULL,
  type        ENUM('alpha','numeric','shortcode') NOT NULL,
  status      ENUM('pending','approved','rejected') NOT NULL DEFAULT 'pending',
  UNIQUE KEY uq_account_mask (account_id, mask),
  CONSTRAINT fk_sid_acct FOREIGN KEY (account_id) REFERENCES accounts(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE vendors (
  id                BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  cid               VARCHAR(64) NOT NULL UNIQUE,        -- connector id -> queue_<cid>
  name              VARCHAR(128),
  transport         ENUM('smpp','http') NOT NULL,
  host              VARCHAR(190), port INT,
  username          VARCHAR(64), credential_ref VARCHAR(128),  -- secret in KMS, not plaintext
  base_url          VARCHAR(512),
  bind              ENUM('transmitter','receiver','transceiver'),
  submit_throughput INT,
  systype           VARCHAR(32), dlr_msgid VARCHAR(8),
  service_status    TINYINT(1) NOT NULL DEFAULT 1,      -- 0 = paused (route rejects)
  created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE operators (
  id      INT PRIMARY KEY,                              -- telco digit 1-6
  code    VARCHAR(32) NOT NULL,
  name    VARCHAR(64),
  country CHAR(2) NOT NULL DEFAULT 'PK'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE routes (
  id             BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  priority       INT NOT NULL DEFAULT 100,              -- lower = evaluated first (unique sequence)
  account_id     BIGINT UNSIGNED,                       -- NULL = any account
  match_mcc      VARCHAR(3),                            -- HLR-resolved MCC; NULL = any
  match_mnc      VARCHAR(3),                            -- HLR-resolved MNC; NULL = any
  match_prefix   VARCHAR(16),                           -- NULL = any prefix
  vendor_id      BIGINT UNSIGNED NOT NULL,
  status         TINYINT(1) NOT NULL DEFAULT 1,
  KEY idx_lookup (account_id, match_mcc, match_mnc, match_prefix, priority),
  CONSTRAINT fk_route_vendor FOREIGN KEY (vendor_id) REFERENCES vendors(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE templates (
  id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id    BIGINT UNSIGNED NOT NULL,
  channel       VARCHAR(32),
  original_text TEXT NOT NULL,
  regex         TEXT NOT NULL,                          -- compiled match pattern
  variables     JSON,                                  -- ["amount","otp"]
  priority      ENUM('TRANSACTIONAL','PROMOTIONAL') NOT NULL DEFAULT 'TRANSACTIONAL',
  status        TINYINT(1) NOT NULL DEFAULT 0,          -- 1 = approved
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_account (account_id, status),
  CONSTRAINT fk_tpl_acct FOREIGN KEY (account_id) REFERENCES accounts(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE spam_keywords (
  id         BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  scope      ENUM('universal','client') NOT NULL,
  account_id BIGINT UNSIGNED,                           -- NULL for universal
  traffic    ENUM('local','international') NOT NULL,
  list_type  ENUM('blocked','allowed') NOT NULL,
  keyword    VARCHAR(190) NOT NULL,
  status     ENUM('pending','approved','rejected') NOT NULL DEFAULT 'approved',
  KEY idx_lookup (scope, account_id, traffic, list_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE blocklist (
  id         BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  msisdn     VARCHAR(20) NOT NULL,
  account_id BIGINT UNSIGNED,                           -- NULL = global DND
  reason     VARCHAR(64),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_block (msisdn, account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE country_rules (
  id         BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id BIGINT UNSIGNED NOT NULL,
  cc         VARCHAR(6) NOT NULL,
  mode       ENUM('allow','deny') NOT NULL,
  UNIQUE KEY uq_cc (account_id, cc),
  CONSTRAINT fk_cr_acct FOREIGN KEY (account_id) REFERENCES accounts(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE account_balance (
  account_id    BIGINT UNSIGNED PRIMARY KEY,
  balance       DECIMAL(14,4) NOT NULL DEFAULT 0,       -- prepaid credit
  daily_quota   INT, daily_used   INT NOT NULL DEFAULT 0,
  monthly_quota INT, monthly_used INT NOT NULL DEFAULT 0,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_bal_acct FOREIGN KEY (account_id) REFERENCES accounts(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE auto_replace (
  id           BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id   BIGINT UNSIGNED NOT NULL,
  find_text    VARCHAR(190) NOT NULL,
  replace_text VARCHAR(190) NOT NULL DEFAULT '',        -- '' = delete
  seq          INT NOT NULL DEFAULT 0,
  match_type   ENUM('literal','regex') NOT NULL DEFAULT 'literal',
  status       TINYINT(1) NOT NULL DEFAULT 1,
  created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_account (account_id, status, seq),
  CONSTRAINT fk_ar_acct FOREIGN KEY (account_id) REFERENCES accounts(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- NEW: config-change / security-event audit trail (compliance: ISO 27001 A.12.4)
CREATE TABLE audit_log (
  id         BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  actor      VARCHAR(128),                              -- admin user / system
  account_id BIGINT UNSIGNED,                           -- affected account (nullable)
  action     VARCHAR(64) NOT NULL,                      -- e.g. route.update, token.revoke
  entity     VARCHAR(64), entity_id VARCHAR(64),
  detail     JSON,                                      -- before/after (no secrets)
  ip         VARCHAR(45),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_time (created_at), KEY idx_account (account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
