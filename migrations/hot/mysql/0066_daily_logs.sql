-- ============================================================================
-- 0066_daily_logs.sql — message logs per company per DAY, written once
-- (issues 12, 13, 14, 16, 17 and 25 of the design audit; Saleem and the PO,
-- 22–26 Oct 2026). Design: products_docs/engineering/data-stores/
-- storage-redesign-2026-10-27.md.
--
--   mt_<company>_<YYYYMMDD>      outgoing messages a vendor TOOK
--   sub_<company>_<YYYYMMDD>     every vendor answer: per part, per try
--   dlr_<company>_<YYYYMMDD>     receipts; final_of marks the one that closed
--   mo_<company>_<YYYYMMDD>      incoming messages
--   mo_out_<company>_<YYYYMMDD>  what became of an incoming message
--   hlr_<company>_<YYYYMMDD>     number lookups worth logging
--   rejects_<company>_<YYYYMMDD> refused messages: at acceptance or submission
--   orphans_0_<YYYYMMDD>         receipts and MO that matched nothing
--
-- The day is the UTC day of the message id (a ULID carries its time); a
-- receipt, a vendor answer and an MO outcome are filed in their message's day.
-- No row is ever updated: a message's status is its final receipt
-- (final_of), worked out when read; money is worked out from the rate version
-- ids on the row. The monthly tables of hot/0062 and their tpl_ templates are
-- left as they are and are not read.
-- ============================================================================

CREATE TABLE IF NOT EXISTS tpl_day_mt (
  id                   BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT,
  msg_id               CHAR(26)          NOT NULL,
  trace_id             CHAR(26)          DEFAULT NULL,
  request_id           VARCHAR(64)       DEFAULT NULL,
  account_id           BIGINT UNSIGNED   DEFAULT NULL,
  company_id           BIGINT UNSIGNED   DEFAULT NULL,
  sender               VARCHAR(32)       DEFAULT NULL,
  destination          VARCHAR(15)       DEFAULT NULL,
  body                 TEXT,
  body_enc             ENUM('plain_text','pii_masked','all_encrypted') NOT NULL DEFAULT 'plain_text',
  campaign_hash        CHAR(64)          DEFAULT NULL,
  campaign_id          VARCHAR(100)      DEFAULT NULL,
  encoding             ENUM('gsm7','ucs2','latin1') DEFAULT NULL,
  segments             INT               DEFAULT NULL,
  sent_parts           SMALLINT UNSIGNED DEFAULT NULL,
  udh_ref              INT               NOT NULL DEFAULT 0,
  udh_total            INT               NOT NULL DEFAULT 0,
  udh_seq              INT               NOT NULL DEFAULT 0,
  label                VARCHAR(32)       DEFAULT NULL COMMENT 'the client''s own label on the message (was channel)',
  ap1                  VARCHAR(255)      DEFAULT NULL,
  ap2                  VARCHAR(255)      DEFAULT NULL,
  apg1                 VARCHAR(255)      DEFAULT NULL,
  apg2                 VARCHAR(255)      DEFAULT NULL,
  priority             INT               DEFAULT NULL,
  client_protocol      ENUM('http','smpp') DEFAULT NULL,
  vendor_protocol      ENUM('smpp','http','soap','blackhole') DEFAULT NULL,
  ip_address           VARCHAR(64)       DEFAULT NULL,
  mcc                  VARCHAR(3)        DEFAULT NULL,
  mnc                  VARCHAR(3)        DEFAULT NULL,
  hlr_source           VARCHAR(8)        NOT NULL DEFAULT '',
  hlr_ported           TINYINT(1)        NOT NULL DEFAULT 0,
  vendor_id            BIGINT UNSIGNED   DEFAULT NULL,
  vendor_msg_id        VARCHAR(64)       DEFAULT NULL COMMENT 'the first part''s; every part''s is in sub_',
  route_id             BIGINT UNSIGNED   DEFAULT NULL,
  template_id          BIGINT UNSIGNED   DEFAULT NULL,
  route_level          INT               DEFAULT NULL,
  send_round           INT               DEFAULT NULL,
  attempts             SMALLINT UNSIGNED DEFAULT NULL,
  accepted_at          DATETIME(3)       DEFAULT NULL,
  vendor_ack_at        DATETIME(3)       DEFAULT NULL,
  no_receipt           TINYINT(1)        DEFAULT NULL,
  failure_receipt_only TINYINT(1)        DEFAULT NULL,
  want_intermediate    TINYINT(1)        DEFAULT NULL,
  bind_slot            SMALLINT UNSIGNED DEFAULT NULL,
  mask_body            TINYINT(1)        DEFAULT NULL,
  queue_owner          VARCHAR(64)       DEFAULT NULL,
  deployment_id        VARCHAR(64)       DEFAULT NULL,
  sent_deployment_id   VARCHAR(64)       DEFAULT NULL,
  sender_by            VARCHAR(32)       DEFAULT NULL,
  client_sender        VARCHAR(32)       DEFAULT NULL,
  client_ref           VARCHAR(64)       DEFAULT NULL,
  spam_flag            VARCHAR(190)      DEFAULT NULL,
  sell_rate_id         BIGINT UNSIGNED   DEFAULT NULL COMMENT 'rate_card_rates.id of the message''s selling price',
  buy_rate_id          BIGINT UNSIGNED   DEFAULT NULL COMMENT 'rate_card_rates.id of the vendor''s price',
  dlr_sell_rate_id     BIGINT UNSIGNED   DEFAULT NULL COMMENT 'the selling price of one receipt, fixed at acceptance',
  hlr_sell_rate_id     BIGINT UNSIGNED   DEFAULT NULL COMMENT 'the selling price of one lookup, fixed at acceptance',
  fx_rate              DECIMAL(24,12)    DEFAULT NULL COMMENT 'selling card currency -> company currency; NULL = the same',
  created_at           DATETIME          NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_msg (msg_id),
  KEY idx_acct_at (account_id, accepted_at),
  KEY idx_dest_at (destination, accepted_at),
  KEY idx_client_ref (account_id, client_ref),
  KEY idx_request (request_id),
  KEY idx_vendor_msg (vendor_id, vendor_msg_id),
  KEY idx_drill (accepted_at, vendor_id),
  KEY idx_campaign_hash (campaign_hash),
  KEY idx_campaign_id (campaign_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS tpl_day_sub (
  id              BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT,
  msg_id          CHAR(26)          NOT NULL,
  account_id      BIGINT UNSIGNED   DEFAULT NULL,
  part            SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  parts           SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  attempt         SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  vendor_id       BIGINT UNSIGNED   DEFAULT NULL,
  vendor_msg_id   VARCHAR(64)       DEFAULT NULL,
  vendor_protocol ENUM('smpp','http','soap','blackhole') DEFAULT NULL,
  command_status  INT UNSIGNED      DEFAULT NULL,
  http_status     SMALLINT UNSIGNED DEFAULT NULL,
  result          ENUM('accepted','refused','not_now','timeout') NOT NULL,
  code            VARCHAR(32)       DEFAULT NULL,
  words           VARCHAR(255)      DEFAULT NULL,
  sent_at         DATETIME(3)       DEFAULT NULL,
  answered_at     DATETIME(3)       DEFAULT NULL,
  deployment_id   VARCHAR(64)       DEFAULT NULL,
  session         VARCHAR(64)       DEFAULT NULL,
  created_at      DATETIME          NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_answer (msg_id, part, attempt, result),
  KEY idx_vendor_msg (vendor_id, vendor_msg_id),
  KEY idx_at (sent_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS tpl_day_dlr (
  id            BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT,
  msg_id        CHAR(26)          NOT NULL,
  account_id    BIGINT UNSIGNED   DEFAULT NULL,
  vendor_id     BIGINT UNSIGNED   DEFAULT NULL,
  vendor_msg_id VARCHAR(64)       DEFAULT NULL,
  part          SMALLINT UNSIGNED DEFAULT NULL,
  status        VARCHAR(12)       DEFAULT NULL,
  stat          VARCHAR(8)        DEFAULT NULL,
  error_code    VARCHAR(16)       DEFAULT NULL,
  raw           VARCHAR(512)      DEFAULT NULL,
  final_of      CHAR(26)          DEFAULT NULL COMMENT 'the message this receipt closed; NULL on every other receipt',
  delivered_at  DATETIME(3)       DEFAULT NULL COMMENT 'when the receipt reached us',
  done_date     DATETIME          DEFAULT NULL COMMENT 'when the vendor says the message reached the phone',
  deployment_id VARCHAR(64)       DEFAULT NULL,
  sell_rate_id  BIGINT UNSIGNED   DEFAULT NULL COMMENT 'the receipt fee, when the customer was charged for this receipt',
  buy_rate_id   BIGINT UNSIGNED   DEFAULT NULL COMMENT 'the vendor''s receipt cost, when charged',
  fx_rate       DECIMAL(24,12)    DEFAULT NULL,
  created_at    DATETIME          NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_final (final_of),
  KEY idx_msg (msg_id),
  KEY idx_vendor_msg (vendor_id, vendor_msg_id),
  KEY idx_at (delivered_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS tpl_day_mo (
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
  vendor_protocol ENUM('smpp','http','soap','blackhole') DEFAULT NULL COMMENT 'how it reached us',
  mcc             VARCHAR(3)      DEFAULT NULL,
  mnc             VARCHAR(3)      DEFAULT NULL,
  error_code      VARCHAR(32)     DEFAULT NULL,
  received_at     DATETIME(3)     DEFAULT NULL,
  mask_body       TINYINT(1)      DEFAULT NULL,
  queue_owner     VARCHAR(64)     DEFAULT NULL,
  deployment_id   VARCHAR(64)     DEFAULT NULL,
  sell_rate_id    BIGINT UNSIGNED DEFAULT NULL,
  buy_rate_id     BIGINT UNSIGNED DEFAULT NULL,
  fx_rate         DECIMAL(24,12)  DEFAULT NULL,
  created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_msg (msg_id),
  KEY idx_acct_at (account_id, received_at),
  KEY idx_dest_at (destination, received_at),
  KEY idx_sender_at (sender, received_at),
  KEY idx_received (received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS tpl_day_mo_out (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  msg_id          CHAR(26)        NOT NULL,
  account_id      BIGINT UNSIGNED DEFAULT NULL,
  status          VARCHAR(12)     NOT NULL COMMENT 'delivered | failed | expired',
  error_code      VARCHAR(32)     DEFAULT NULL,
  client_protocol ENUM('http','smpp') DEFAULT NULL COMMENT 'smpp = a bind, http = the webhook',
  deployment_id   VARCHAR(64)     DEFAULT NULL,
  at              DATETIME(3)     NOT NULL,
  created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_msg (msg_id),
  KEY idx_at (at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS tpl_day_hlr (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  msg_id        CHAR(26)        DEFAULT NULL COMMENT 'NULL = a lookup with no message (GET /sms/v4/operator)',
  account_id    BIGINT UNSIGNED DEFAULT NULL,
  destination   VARCHAR(20)     NOT NULL,
  source        VARCHAR(8)      NOT NULL COMMENT 'live | dip | dip_miss | db | miss | range | client | country',
  mcc           VARCHAR(3)      DEFAULT NULL,
  mnc           VARCHAR(3)      DEFAULT NULL,
  ported        TINYINT(1)      NOT NULL DEFAULT 0,
  sell_rate_id  BIGINT UNSIGNED DEFAULT NULL COMMENT 'the lookup''s selling price, fixed at acceptance',
  fx_rate       DECIMAL(24,12)  DEFAULT NULL,
  looked_up_at  DATETIME(3)     NOT NULL,
  deployment_id VARCHAR(64)     DEFAULT NULL,
  created_at    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_acct_at (account_id, looked_up_at),
  KEY idx_dest_at (destination, looked_up_at),
  KEY idx_msg (msg_id),
  KEY idx_at (looked_up_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS tpl_day_rejects (
  id               BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT,
  msg_id           CHAR(26)          NOT NULL,
  rejected_stage   ENUM('ACCEPTANCE','SUBMISSION') NOT NULL DEFAULT 'ACCEPTANCE',
  status           VARCHAR(12)       NOT NULL DEFAULT 'rejected' COMMENT 'rejected (at acceptance) | failed | expired',
  rejected_at      DATETIME(3)       NOT NULL,
  account_id       BIGINT UNSIGNED   DEFAULT NULL,
  client_protocol  ENUM('http','smpp') DEFAULT NULL,
  client_msg_id    VARCHAR(64)       DEFAULT NULL COMMENT 'the client''s own reference',
  request_id       VARCHAR(64)       DEFAULT NULL,
  sender_id        VARCHAR(32)       DEFAULT NULL,
  msisdn           VARCHAR(20)       DEFAULT NULL,
  parts            SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  sent_parts       SMALLINT UNSIGNED DEFAULT NULL,
  label            VARCHAR(32)       DEFAULT NULL,
  reason_code      VARCHAR(191)      NOT NULL,
  reason_text      VARCHAR(512)      DEFAULT NULL,
  reason_detail    VARCHAR(128)      DEFAULT NULL,
  reason_audit     VARCHAR(190)      DEFAULT NULL,
  ip_address       VARCHAR(64)       DEFAULT NULL,
  vendor_id        BIGINT UNSIGNED   DEFAULT NULL,
  vendor_protocol  ENUM('smpp','http','soap','blackhole') DEFAULT NULL,
  route_id         BIGINT UNSIGNED   DEFAULT NULL,
  attempts         SMALLINT UNSIGNED DEFAULT NULL,
  mcc              VARCHAR(3)        DEFAULT NULL,
  mnc              VARCHAR(3)        DEFAULT NULL,
  queue_owner      VARCHAR(64)       DEFAULT NULL,
  deployment_id    VARCHAR(64)       DEFAULT NULL,
  body             TEXT,
  body_enc         ENUM('plain_text','pii_masked','all_encrypted') NOT NULL DEFAULT 'plain_text',
  sell_rate_id     BIGINT UNSIGNED   DEFAULT NULL,
  buy_rate_id      BIGINT UNSIGNED   DEFAULT NULL,
  dlr_sell_rate_id BIGINT UNSIGNED   DEFAULT NULL,
  hlr_sell_rate_id BIGINT UNSIGNED   DEFAULT NULL,
  fx_rate          DECIMAL(24,12)    DEFAULT NULL,
  created_at       DATETIME          NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_msg (msg_id),
  KEY idx_acct (account_id, rejected_at),
  KEY idx_client (account_id, client_msg_id),
  KEY idx_at (rejected_at),
  KEY idx_reason_at (reason_code, rejected_at),
  KEY idx_msisdn_at (msisdn, rejected_at),
  KEY idx_stage_at (rejected_stage, rejected_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS tpl_day_orphans (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  kind          ENUM('dlr','mo') NOT NULL,
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
  received_at   DATETIME(3)     NOT NULL,
  PRIMARY KEY (id),
  KEY idx_kind_at (kind, received_at),
  KEY idx_vendor_msg (vendor_id, vendor_msg_id),
  KEY idx_destination (destination),
  KEY idx_received (received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- An orphan that was applied to its message after all. The orphan row stays
-- (logs are never changed); this work table says it is done, so it is not
-- applied twice and the Orphans screen can show it as matched. Rows older
-- than the hot store's days are pruned by retention.
CREATE TABLE IF NOT EXISTS orphans_matched (
  orphan_table VARCHAR(64)     NOT NULL,
  orphan_id    BIGINT UNSIGNED NOT NULL,
  msg_id       CHAR(26)        DEFAULT NULL,
  matched_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (orphan_table, orphan_id),
  KEY idx_matched (matched_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
