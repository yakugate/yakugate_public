-- ============================================================================
-- 0062_company_month_records.sql — message records by company and month
-- (Saleem 2026-10-14, D-136: "we need monthly partitioned table for each
-- company, each month for every mo, mt, dlr, hlr, rejected, orphans. user
-- should choose company and month on frontend in order to view the logs")
--
-- Until now the records were a table per ACCOUNT per DAY (mtlog_<user>_<day>,
-- dlrlog_<user>_<day>), month tables shared by everyone (mo_log_, rejects_,
-- orphan_*_) and one big record_index holding where each message was and its
-- only current status. From this build a record is filed by its CUSTOMER
-- company and its month:
--
--   mt_<company>_<YYYYMM>       outgoing messages, with their status
--   dlr_<company>_<YYYYMM>      receipts, in their message's month
--   mo_<company>_<YYYYMM>       incoming messages, with their status
--   hlr_<company>_<YYYYMM>      number lookups (every provider lookup; one
--                               answered from our own data only when charged)
--   rejects_<company>_<YYYYMM>  refused submits (company 0 = before login)
--   orphans_0_<YYYYMM>          receipts and MO that matched nothing
--
-- The month is the UTC month of the message id (a ULID carries its time); a
-- table is made from its template on its first write. Company 0 holds what has
-- no customer company. record_index is no longer written or read; the old
-- tables are left as they are for the operator to drop.
--
-- The templates are named tpl_<kind> so that no LIKE pattern of a family
-- (mt\_%, mo\_% …) matches a template, and so a hot store sharing a database
-- with the HLR store cannot meet its hlr_template.
-- ============================================================================

-- Outgoing: the mtlog columns, plus what lived in record_index.
CREATE TABLE IF NOT EXISTS tpl_mt (
  id                     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  msg_id                 CHAR(26)        NOT NULL,
  trace_id               CHAR(26)        DEFAULT NULL,
  request_id             VARCHAR(64)     DEFAULT NULL,
  account_id             BIGINT UNSIGNED DEFAULT NULL,
  company_id             BIGINT UNSIGNED DEFAULT NULL,
  sender                 VARCHAR(32)     DEFAULT NULL,
  destination            VARCHAR(15)     DEFAULT NULL,
  body                   TEXT,
  body_enc               ENUM('plain_text','pii_masked','all_encrypted') NOT NULL DEFAULT 'plain_text',
  campaign_hash          CHAR(64)        DEFAULT NULL,
  campaign_id            VARCHAR(100)    DEFAULT NULL,
  encoding               ENUM('gsm7','ucs2','latin1') DEFAULT NULL,
  segments               INT             DEFAULT NULL,
  sent_parts             SMALLINT UNSIGNED DEFAULT NULL,
  udh_ref                INT             NOT NULL DEFAULT 0,
  udh_total              INT             NOT NULL DEFAULT 0,
  udh_seq                INT             NOT NULL DEFAULT 0,
  channel                VARCHAR(32)     DEFAULT NULL,
  ap1                    VARCHAR(50)     DEFAULT NULL,
  ap2                    VARCHAR(50)     DEFAULT NULL,
  apg1                   VARCHAR(50)     DEFAULT NULL,
  apg2                   VARCHAR(50)     DEFAULT NULL,
  priority               INT             DEFAULT NULL,
  protocol               ENUM('smpp','http','blackhole') NOT NULL,
  ingress                ENUM('http','smpp') DEFAULT NULL,
  ip_address             VARCHAR(64)     DEFAULT NULL,
  mcc                    VARCHAR(3)      DEFAULT NULL,
  mnc                    VARCHAR(3)      DEFAULT NULL,
  hlr_source             VARCHAR(8)      NOT NULL DEFAULT '',
  hlr_ported             TINYINT(1)      NOT NULL DEFAULT 0,
  vendor_id              BIGINT UNSIGNED DEFAULT NULL,
  vendor_msg_id          VARCHAR(64)     DEFAULT NULL,
  transport              ENUM('smpp','http','soap','blackhole') NOT NULL,
  route_id               BIGINT UNSIGNED DEFAULT NULL,
  template_id            BIGINT UNSIGNED DEFAULT NULL,
  accepted_at            DATETIME        DEFAULT NULL,
  vendor_ack_at          DATETIME(3)     DEFAULT NULL,
  status                 VARCHAR(12)     DEFAULT NULL,
  no_receipt             TINYINT(1)      DEFAULT NULL,
  failure_receipt_only   TINYINT(1)      DEFAULT NULL,
  want_intermediate      TINYINT(1)      DEFAULT NULL,
  bind_slot              SMALLINT UNSIGNED DEFAULT NULL,
  mask_body              TINYINT(1)      DEFAULT NULL,
  told_status            VARCHAR(12)     DEFAULT NULL,
  told_err               VARCHAR(16)     DEFAULT NULL,
  told_by                VARCHAR(24)     DEFAULT NULL,
  told_at                DATETIME        DEFAULT NULL,
  queue_owner            VARCHAR(64)     DEFAULT NULL,
  rate_cards             JSON            DEFAULT NULL,
  buy_price              DECIMAL(12,6)   DEFAULT NULL,
  sell_price             DECIMAL(12,6)   DEFAULT NULL,
  currency               CHAR(3)         DEFAULT NULL,
  rated_at               DATETIME        DEFAULT NULL,
  charging_mode          VARCHAR(32)     DEFAULT NULL,
  attempt_count          INT             DEFAULT NULL,
  operator_bills_failure TINYINT(1)      DEFAULT NULL,
  created_at             DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at             DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  client_ref             VARCHAR(64)     DEFAULT NULL,
  rating_attempts        SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  rating_error           VARCHAR(255)    DEFAULT NULL,
  spam_flag              VARCHAR(190)    DEFAULT NULL,
  concat_parts           JSON            DEFAULT NULL,
  deployment_id          VARCHAR(64)     DEFAULT NULL,
  sent_deployment_id     VARCHAR(64)     DEFAULT NULL,
  sender_by              VARCHAR(32)     DEFAULT NULL,
  client_sender          VARCHAR(32)     DEFAULT NULL,
  route_level            INT             DEFAULT NULL,
  send_round             INT             DEFAULT NULL,
  billing_model          VARCHAR(8)      DEFAULT NULL,
  quote                  TEXT,
  charge_batch           CHAR(26)        DEFAULT NULL,
  charge                 TEXT,
  w_token                CHAR(26)        DEFAULT NULL COMMENT 'the last write that made or closed this row (summaries count what it changed)',
  PRIMARY KEY (id),
  UNIQUE KEY uq_msg (msg_id),
  KEY idx_acct_at (account_id, accepted_at),
  KEY idx_dest_at (destination, accepted_at),
  KEY idx_client_ref (account_id, client_ref),
  KEY idx_request (request_id),
  KEY idx_drill (accepted_at, vendor_id),
  KEY idx_unrated (rated_at),
  KEY idx_concat (account_id, udh_ref, udh_total),
  KEY idx_campaign_hash (campaign_hash),
  KEY idx_campaign_id (campaign_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Receipts: the dlrlog columns. A receipt is filed in its MESSAGE's month.
CREATE TABLE IF NOT EXISTS tpl_dlr (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  msg_id          CHAR(26)        NOT NULL,
  account_id      BIGINT UNSIGNED DEFAULT NULL,
  vendor_id       BIGINT UNSIGNED DEFAULT NULL,
  vendor_msg_id   VARCHAR(64)     DEFAULT NULL,
  status          VARCHAR(12)     DEFAULT NULL,
  stat            VARCHAR(8)      DEFAULT NULL,
  error_code      VARCHAR(16)     DEFAULT NULL,
  raw             VARCHAR(512)    DEFAULT NULL,
  delivered_at    DATETIME        DEFAULT NULL,
  done_date       DATETIME        DEFAULT NULL,
  created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  rate_cards      JSON            DEFAULT NULL,
  buy_price       DECIMAL(12,6)   DEFAULT NULL,
  sell_price      DECIMAL(12,6)   DEFAULT NULL,
  currency        CHAR(3)         DEFAULT NULL,
  rated_at        DATETIME        DEFAULT NULL,
  rating_attempts SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  rating_error    VARCHAR(255)    DEFAULT NULL,
  deployment_id   VARCHAR(64)     DEFAULT NULL,
  charge_batch    CHAR(26)        DEFAULT NULL,
  charge          TEXT,
  PRIMARY KEY (id),
  KEY idx_msg (msg_id),
  KEY idx_vendor_msg (vendor_id, vendor_msg_id),
  KEY idx_created (created_at),
  KEY idx_unrated (rated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Incoming: the mo_log columns, plus the status that lived in record_index.
CREATE TABLE IF NOT EXISTS tpl_mo (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  msg_id          CHAR(26)        NOT NULL,
  account_id      BIGINT UNSIGNED DEFAULT NULL,
  sender          VARCHAR(32)     DEFAULT NULL,
  destination     VARCHAR(15)     DEFAULT NULL,
  body            TEXT,
  encoding        ENUM('gsm7','ucs2','latin1') DEFAULT NULL,
  segments        INT             DEFAULT NULL,
  route_id        BIGINT UNSIGNED DEFAULT NULL,
  vendor_id       BIGINT UNSIGNED DEFAULT NULL,
  mcc             VARCHAR(3)      DEFAULT NULL,
  mnc             VARCHAR(3)      DEFAULT NULL,
  received_at     DATETIME        DEFAULT NULL,
  status          VARCHAR(12)     DEFAULT NULL,
  mask_body       TINYINT(1)      DEFAULT NULL,
  queue_owner     VARCHAR(64)     DEFAULT NULL,
  created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  rate_cards      JSON            DEFAULT NULL,
  buy_price       DECIMAL(12,6)   DEFAULT NULL,
  sell_price      DECIMAL(12,6)   DEFAULT NULL,
  currency        CHAR(3)         DEFAULT NULL,
  rated_at        DATETIME        DEFAULT NULL,
  rating_attempts SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  rating_error    VARCHAR(255)    DEFAULT NULL,
  error_code      VARCHAR(32)     DEFAULT NULL,
  deployment_id   VARCHAR(64)     DEFAULT NULL,
  charge_batch    CHAR(26)        DEFAULT NULL,
  charge          TEXT,
  w_token         CHAR(26)        DEFAULT NULL COMMENT 'the last write that made or closed this row',
  PRIMARY KEY (id),
  UNIQUE KEY uq_msg (msg_id),
  KEY idx_acct_at (account_id, received_at),
  KEY idx_sender_at (sender, received_at),
  KEY idx_received (received_at),
  KEY idx_unrated (rated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Number lookups: one row per lookup an HLR provider answered (live, dip),
-- and per lookup answered from our own data that was charged.
CREATE TABLE IF NOT EXISTS tpl_hlr (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  msg_id        CHAR(26)        DEFAULT NULL COMMENT 'NULL = a lookup with no message (GET /sms/v4/operator)',
  account_id    BIGINT UNSIGNED DEFAULT NULL,
  destination   VARCHAR(20)     NOT NULL,
  source        VARCHAR(8)      NOT NULL COMMENT 'live | dip | dip_miss | data | range | client | country',
  mcc           VARCHAR(3)      DEFAULT NULL,
  mnc           VARCHAR(3)      DEFAULT NULL,
  ported        TINYINT(1)      NOT NULL DEFAULT 0,
  charge        DECIMAL(12,6)   DEFAULT NULL COMMENT 'the price fixed at acceptance (quote), company currency',
  currency      CHAR(3)         DEFAULT NULL,
  looked_up_at  DATETIME(3)     NOT NULL,
  deployment_id VARCHAR(64)     DEFAULT NULL,
  PRIMARY KEY (id),
  KEY idx_acct_at (account_id, looked_up_at),
  KEY idx_dest_at (destination, looked_up_at),
  KEY idx_msg (msg_id),
  KEY idx_at (looked_up_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Refusals: the rejects columns.
CREATE TABLE IF NOT EXISTS tpl_rejects (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  rejected_at   DATETIME(3)     NOT NULL,
  account_id    BIGINT UNSIGNED DEFAULT NULL,
  channel       ENUM('http','smpp') NOT NULL,
  client_msg_id VARCHAR(64)     DEFAULT NULL,
  sender_id     VARCHAR(20)     DEFAULT NULL,
  msisdn        VARCHAR(20)     DEFAULT NULL,
  parts         SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  reason_code   VARCHAR(191)    NOT NULL,
  reason_text   VARCHAR(190)    DEFAULT NULL,
  reason_detail VARCHAR(128)    DEFAULT NULL,
  reason_audit  VARCHAR(190)    DEFAULT NULL,
  deployment_id VARCHAR(64)     DEFAULT NULL,
  body          TEXT,
  PRIMARY KEY (id),
  KEY idx_acct (account_id, rejected_at),
  KEY idx_client (account_id, client_msg_id),
  KEY idx_at (rejected_at),
  KEY idx_reason_at (reason_code, rejected_at),
  KEY idx_msisdn_at (msisdn, rejected_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Orphans: a receipt or an MO that matched nothing (always company 0).
CREATE TABLE IF NOT EXISTS tpl_orphans (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  kind          ENUM('dlr','mo')  NOT NULL,
  vendor_id     BIGINT UNSIGNED DEFAULT NULL,
  vendor_msg_id VARCHAR(64)     DEFAULT NULL,
  status        VARCHAR(16)     DEFAULT NULL,
  error_code    VARCHAR(32)     DEFAULT NULL,
  raw           TEXT,
  source        VARCHAR(32)     DEFAULT NULL,
  destination   VARCHAR(32)     DEFAULT NULL,
  body          TEXT,
  encoding      VARCHAR(8)      DEFAULT NULL,
  mcc           VARCHAR(3)      DEFAULT NULL,
  mnc           VARCHAR(3)      DEFAULT NULL,
  reason        VARCHAR(64)     DEFAULT NULL,
  received_at   DATETIME        NOT NULL,
  PRIMARY KEY (id),
  KEY idx_kind_at (kind, received_at),
  KEY idx_vendor_msg (vendor_id, vendor_msg_id),
  KEY idx_destination (destination),
  KEY idx_received (received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Messages still waiting for a receipt, on an install with NO Redis (with
-- Redis the receipt facts in the main Redis do this; never both). A row is
-- written when the vendor takes the message, deleted when the message is
-- final, and swept when its vendor's wait (dlr_config.wait_hours) runs out.
CREATE TABLE IF NOT EXISTS open_msgs (
  msg_id        CHAR(26)        NOT NULL,
  company_id    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  account_id    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  vendor_id     BIGINT UNSIGNED NOT NULL DEFAULT 0,
  vendor_msg_id VARCHAR(64)     DEFAULT NULL,
  destination   VARCHAR(20)     DEFAULT NULL,
  sent_at       DATETIME        NOT NULL,
  wait_until    DATETIME        NOT NULL,
  queue_owner   VARCHAR(64)     DEFAULT NULL,
  PRIMARY KEY (msg_id),
  KEY idx_vendor_msg (vendor_id, vendor_msg_id),
  KEY idx_vendor_sent (vendor_id, msg_id),
  KEY idx_acct_sent (account_id, sent_at),
  KEY idx_wait (wait_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Receipts per 15 minutes (UTC, by arrival), account, vendor, SMPP word and
-- error code. Added by whoever writes the receipt, in the same transaction.
CREATE TABLE IF NOT EXISTS summary_dlr (
  bucket_start DATETIME        NOT NULL,
  account_id   BIGINT UNSIGNED NOT NULL DEFAULT 0,
  vendor_id    INT UNSIGNED    NOT NULL DEFAULT 0,
  stat         VARCHAR(8)      NOT NULL DEFAULT '',
  error_code   VARCHAR(16)     NOT NULL DEFAULT '',
  receipts     INT UNSIGNED    NOT NULL DEFAULT 0,
  updated_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq (bucket_start, account_id, vendor_id, stat, error_code),
  KEY idx_account (account_id, bucket_start),
  KEY idx_vendor (vendor_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Orphans per 15 minutes (UTC, by arrival), kind, vendor, reason and
-- shortcode ('' for a receipt).
CREATE TABLE IF NOT EXISTS summary_orphans (
  bucket_start DATETIME     NOT NULL,
  kind         VARCHAR(4)   NOT NULL,
  vendor_id    INT UNSIGNED NOT NULL DEFAULT 0,
  reason       VARCHAR(64)  NOT NULL DEFAULT '',
  shortcode    VARCHAR(32)  NOT NULL DEFAULT '',
  orphans      INT UNSIGNED NOT NULL DEFAULT 0,
  updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq (bucket_start, kind, vendor_id, reason, shortcode),
  KEY idx_vendor (vendor_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- The parts of a long message sent as several PDUs: whose company each is,
-- so a part's receipt finds the message's table without record_index.
ALTER TABLE concat_parts ADD COLUMN company_id BIGINT UNSIGNED NULL;

-- Lookups that were charged, so the HLR log's header reads no record.
ALTER TABLE summary_lookup ADD COLUMN charged_lookups INT UNSIGNED NOT NULL DEFAULT 0;
ALTER TABLE summary_lookup ADD COLUMN charged_amount DECIMAL(16,6) NOT NULL DEFAULT 0;
