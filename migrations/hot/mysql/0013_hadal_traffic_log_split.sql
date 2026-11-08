-- ============================================================================
-- Hadal reconciliation, traffic-log group (PO 2026-07-27, Salman: "follow the
-- hadal completely" — full cutover, not schema-only). Replaces the unified
-- records_template (one row per message, updated in place through its whole
-- lifecycle) with Hadal's four-table split:
--
--   mtlog_template       — MT submission. IMMUTABLE after insert (no status
--                          column at all — delivery state never touches this
--                          row again, matching Hadal's MTLOG). Cloned per
--                          account-month, same mechanism as records_template
--                          was (Go's ensureRecordTable).
--   dlrlog_template       — ONE ROW PER RECEIPT (not per message) — a
--                          multi-part send or a retried receipt can produce
--                          several rows for the same msg_id, matching
--                          Hadal's DLRLOG. Cloned per account-month.
--   mologtemplate         — MO inbound. Cloned per-MONTH only, NOT per
--                          account — matches Hadal's actual partitioning
--                          (MOLOG_2026_04 carries no tenant suffix, unlike
--                          MTLOG/DLRLOG/REJECTEDLOG which do).
--   rejectedlog_template  — rejected submits. Cloned per account-month.
--
-- record_index is UNCHANGED in shape: msg_id -> (account_id, ym, rec_table,
-- vendor_id, vendor_msg_id, status) remains the one lightweight "what's the
-- current status, and which table holds the full submission row" pointer —
-- status here is now the SOURCE OF TRUTH for current message state (no other
-- row gets updated in place to reflect it).
--
-- Five columns adopted from the Hadal comparison land on mtlog_template AND
-- rejectedlog_template: company_id (denormalized reporting), attempt_count
-- (final retry-count snapshot — today's queue-side counter is lost once a
-- row leaves the queue), operator_bills_failure, and (mtlog only) rated_at/
-- buy_price/sell_price/currency (carried over from records_template) plus
-- charging_mode (new); rejectedlog gets is_charged instead of
-- rated/buy/sell (a rejected message was never priced).
--
-- records_template itself is NOT dropped by this migration — existing
-- records_<account>_<ym> partitions stay queryable as historical data; new
-- traffic writes into the four new families from this point on. Retention/
-- backup, once updated, archives all five families going forward.
-- ============================================================================

CREATE TABLE mtlog_template (
  id              BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  msg_id          CHAR(26) NOT NULL,
  trace_id        CHAR(26) DEFAULT NULL,
  request_id      VARCHAR(64) DEFAULT NULL,
  account_id      BIGINT UNSIGNED DEFAULT NULL,
  company_id      BIGINT UNSIGNED DEFAULT NULL,
  sender          VARCHAR(32) DEFAULT NULL,
  destination     VARCHAR(15) DEFAULT NULL,
  body            TEXT,
  body_enc        ENUM('plain_text','pii_masked','all_encrypted') NOT NULL DEFAULT 'plain_text',
  campaign_hash   CHAR(64) DEFAULT NULL,
  encoding        ENUM('gsm7','ucs2') DEFAULT NULL,
  segments        INT DEFAULT NULL,
  udh_ref         INT NOT NULL DEFAULT 0,
  udh_total       INT NOT NULL DEFAULT 0,
  udh_seq         INT NOT NULL DEFAULT 0,
  channel         VARCHAR(32) DEFAULT NULL,
  ap1             VARCHAR(50) DEFAULT NULL,
  ap2             VARCHAR(50) DEFAULT NULL,
  apg1            VARCHAR(50) DEFAULT NULL,
  apg2            VARCHAR(50) DEFAULT NULL,
  priority        INT DEFAULT NULL,
  protocol        ENUM('smpp','http','blackhole') NOT NULL,
  ingress         ENUM('http','smpp') DEFAULT NULL,
  ip_address      VARCHAR(64) DEFAULT NULL,
  mcc             VARCHAR(3) DEFAULT NULL,
  mnc             VARCHAR(3) DEFAULT NULL,
  vendor_id       BIGINT UNSIGNED DEFAULT NULL,
  transport       ENUM('smpp','http','blackhole') NOT NULL,
  route_id        BIGINT UNSIGNED DEFAULT NULL,
  template_id     BIGINT UNSIGNED DEFAULT NULL,
  accepted_at     DATETIME DEFAULT NULL,
  rate_cards      JSON DEFAULT NULL,
  buy_price       DECIMAL(12,6) DEFAULT NULL,
  sell_price      DECIMAL(12,6) DEFAULT NULL,
  currency        CHAR(3) DEFAULT NULL,
  rated_at        DATETIME DEFAULT NULL,
  charging_mode   VARCHAR(32) DEFAULT NULL,
  attempt_count   INT DEFAULT NULL,
  operator_bills_failure TINYINT(1) DEFAULT NULL,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY msg_id (msg_id),
  KEY idx_vendor_msg (vendor_id),
  KEY idx_unrated (rated_at),
  KEY idx_concat (account_id, udh_ref, udh_total),
  KEY idx_company (company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE dlrlog_template (
  id              BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  msg_id          CHAR(26) NOT NULL,
  account_id      BIGINT UNSIGNED DEFAULT NULL,
  vendor_id       BIGINT UNSIGNED DEFAULT NULL,
  vendor_msg_id   VARCHAR(64) DEFAULT NULL,
  status          VARCHAR(12) DEFAULT NULL,
  error_code      VARCHAR(16) DEFAULT NULL,
  raw             VARCHAR(512) DEFAULT NULL,
  delivered_at    DATETIME DEFAULT NULL,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_msg (msg_id),
  KEY idx_vendor_msg (vendor_id, vendor_msg_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- NOT cloned per account — one shared table per calendar month, matching
-- Hadal's actual MOLOG partitioning (no tenant suffix).
CREATE TABLE mologtemplate (
  id              BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  msg_id          CHAR(26) NOT NULL,
  account_id      BIGINT UNSIGNED DEFAULT NULL,
  sender          VARCHAR(32) DEFAULT NULL,
  destination     VARCHAR(15) DEFAULT NULL,
  body            TEXT,
  encoding        ENUM('gsm7','ucs2') DEFAULT NULL,
  segments        INT DEFAULT NULL,
  route_id        BIGINT UNSIGNED DEFAULT NULL,
  vendor_id       BIGINT UNSIGNED DEFAULT NULL,
  mcc             VARCHAR(3) DEFAULT NULL,
  mnc             VARCHAR(3) DEFAULT NULL,
  received_at     DATETIME DEFAULT NULL,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY msg_id (msg_id),
  KEY idx_account (account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE rejectedlog_template (
  id              BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  msg_id          CHAR(26) NOT NULL,
  account_id      BIGINT UNSIGNED DEFAULT NULL,
  company_id      BIGINT UNSIGNED DEFAULT NULL,
  sender          VARCHAR(32) DEFAULT NULL,
  destination     VARCHAR(15) DEFAULT NULL,
  body            TEXT,
  encoding        ENUM('gsm7','ucs2') DEFAULT NULL,
  segments        INT DEFAULT NULL,
  channel         VARCHAR(32) DEFAULT NULL,
  protocol        ENUM('smpp','http','blackhole') DEFAULT NULL,
  ingress         ENUM('http','smpp') DEFAULT NULL,
  mcc             VARCHAR(3) DEFAULT NULL,
  mnc             VARCHAR(3) DEFAULT NULL,
  reject_reason   VARCHAR(190) DEFAULT NULL,
  status_code     INT DEFAULT NULL,
  request_info    JSON DEFAULT NULL,
  ip_address      VARCHAR(64) DEFAULT NULL,
  template_id     BIGINT UNSIGNED DEFAULT NULL,
  route_id        BIGINT UNSIGNED DEFAULT NULL,
  is_charged      TINYINT(1) DEFAULT NULL,
  attempt_count   INT DEFAULT NULL,
  operator_bills_failure TINYINT(1) DEFAULT NULL,
  rate_cards      JSON DEFAULT NULL,
  accepted_at     DATETIME DEFAULT NULL,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY msg_id (msg_id),
  KEY idx_status_code (status_code),
  KEY idx_company (company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
