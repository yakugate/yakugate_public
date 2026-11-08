-- ============================================================================
-- 0004  Schema v2 consolidation — QUEUE store (MySQL)
-- Matches database-structure.docx v2.0 (2026-07-13). api_tokens DROPPED
-- (JWT-only auth). auto_replace_text + webhooks MOVED here from config.
-- Pre-alpha: DROP+CREATE changed tables to final shape (a new-file addition).
-- ============================================================================

DROP TABLE IF EXISTS api_tokens;        -- JWT-only auth; jwt_revocations is the only auth table
DROP TABLE IF EXISTS queue_template;
DROP TABLE IF EXISTS hlr_template;

-- 2.1 queue_template -> queue_<cid> --------------------------------------------
CREATE TABLE queue_template (
  id              BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  msg_id          CHAR(26) NOT NULL,
  trace_id        CHAR(26) NOT NULL,
  request_id      CHAR(26) NOT NULL,
  account_id      BIGINT UNSIGNED NOT NULL,
  sender          VARCHAR(32) NOT NULL,
  destination     VARCHAR(15) NOT NULL,
  body            TEXT NOT NULL,                        -- plaintext by design (vendor needs it)
  encoding        ENUM('gsm7','ucs2') NOT NULL DEFAULT 'gsm7',
  segments        INT NOT NULL DEFAULT 1,
  gsm7            TINYINT NULL,                         -- 1 force / 0 ucs2 / NULL auto
  template_id     BIGINT UNSIGNED NULL,
  channel         VARCHAR(32) NULL,
  priority        INT NOT NULL DEFAULT 9,               -- 1 OTP / 5 txn / 9 default
  vendor_id       BIGINT UNSIGNED NOT NULL,
  transport       ENUM('smpp','http') NOT NULL,
  mcc             VARCHAR(3) NULL,
  mnc             VARCHAR(3) NULL,
  status          ENUM('queued','sent','failed','expired') NOT NULL DEFAULT 'queued',
  idempotency_key VARCHAR(64) NULL,
  valid_until     DATETIME NULL,
  attempts        INT NOT NULL DEFAULT 0,
  next_attempt_at DATETIME NOT NULL,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_claim (status, next_attempt_at, id),
  KEY idx_msg (msg_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2.2 hlr_template -> hlr_<first4> ---------------------------------------------
CREATE TABLE hlr_template (
  msisdn     VARCHAR(15) PRIMARY KEY,
  mcc        VARCHAR(3) NOT NULL,
  mnc        VARCHAR(3) NOT NULL,
  ported     TINYINT(1) NOT NULL DEFAULT 0,
  last_check DATE NULL,
  KEY idx_network (mcc, mnc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2.3 jwt_revocations (kept; the only auth table) ------------------------------
CREATE TABLE IF NOT EXISTS jwt_revocations (
  jti        VARCHAR(128) PRIMARY KEY,
  account_id BIGINT UNSIGNED NULL,
  expires_at DATETIME NOT NULL,
  revoked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  reason     VARCHAR(64) NULL,
  KEY idx_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2.4 delivery_reports (DLR staging) -------------------------------------------
CREATE TABLE IF NOT EXISTS delivery_reports (
  id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  vendor_id     BIGINT UNSIGNED NOT NULL,
  vendor_msg_id VARCHAR(64) NOT NULL,
  msg_id        CHAR(26) NULL,
  part_seq      INT NOT NULL DEFAULT 1,
  parts_total   INT NOT NULL DEFAULT 1,
  status        VARCHAR(10) NOT NULL,
  error_code    VARCHAR(8) NULL,
  raw           TEXT NOT NULL,
  direction     ENUM('esme','smsc') NOT NULL DEFAULT 'esme',
  received_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  processed     TINYINT(1) NOT NULL DEFAULT 0,
  UNIQUE KEY uq_receipt (vendor_id, vendor_msg_id, part_seq, status),
  KEY idx_vmsg (vendor_msg_id),
  KEY idx_processed (processed, received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2.5 auto_replace_text (moved from config, renamed) ---------------------------
CREATE TABLE auto_replace_text (
  id                 BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id         BIGINT UNSIGNED NOT NULL,
  replace_regex      JSON NOT NULL,                    -- {match, replace}
  seq                INT NOT NULL,
  is_disabled        TINYINT(1) NOT NULL DEFAULT 0,
  created_by_user_id VARCHAR(64) NULL,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_account_seq (account_id, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2.6 webhooks (moved from config) ---------------------------------------------
CREATE TABLE webhooks (
  id                 BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id         BIGINT UNSIGNED NOT NULL,
  action             ENUM('dlr','mo','hlr_update') NOT NULL,
  url                VARCHAR(512) NOT NULL,
  method             VARCHAR(8) NOT NULL DEFAULT 'POST',
  secret_ref         VARCHAR(128) NULL,
  is_disabled        TINYINT(1) NOT NULL DEFAULT 0,
  created_by_user_id VARCHAR(64) NULL,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_account (account_id, action)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
