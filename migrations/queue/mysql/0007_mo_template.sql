-- ============================================================================
-- 0007_mo_template.sql — MO inbound staging queue template, v2 (2026-07-17)
-- Replaces the process-3-era mo_template (0002_process3.sql; empty pre-alpha
-- table) with the PO-decided MO lifecycle shape (see
-- products_docs/06-processes/process-6/README.md):
--   vendor connection -> direction='MO' route match -> staged here ->
--   delivered to client's MO channel (bound SMPP receiver / accounts mo_url)
--   -> removed from staging -> finalized into records_* with direction='MO'.
--
-- Merged shape: canonical ids + claim/lease columns (same pattern as
-- queue_template) PLUS the SMPP capture fields the vendor edge produces
-- (tlvs/raw/session_id). sender = originating subscriber MSISDN; destination =
-- the client's shortcode/sender-id. route_id = winning MO route (audit).
-- Clone unit (mo_<account> vs shared) is a Process-6 scope decision.
-- ============================================================================

DROP TABLE IF EXISTS mo_template;   -- pre-alpha reshape; table was empty

CREATE TABLE mo_template (
  id              BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  msg_id          CHAR(26) NOT NULL,
  trace_id        CHAR(26) NOT NULL,
  vendor_id       BIGINT UNSIGNED NOT NULL,
  vendor_msg_id   VARCHAR(64) NULL,
  session_id      VARCHAR(64) NULL,      -- SMPP bind the deliver_sm arrived on
  account_id      BIGINT UNSIGNED NOT NULL,
  route_id        BIGINT UNSIGNED NULL,  -- winning direction='MO' route (audit)
  sender          VARCHAR(32) NOT NULL,  -- originating subscriber MSISDN
  destination     VARCHAR(32) NOT NULL,  -- client shortcode / sender-id
  body            TEXT NOT NULL,
  encoding        ENUM('gsm7','ucs2') NOT NULL DEFAULT 'gsm7',
  segments        INT NOT NULL DEFAULT 1,
  tlvs            JSON NULL,             -- optional SMPP TLVs as received
  raw             MEDIUMTEXT NULL,       -- raw vendor payload (http callback body / pdu hex)
  mcc             VARCHAR(3) NULL,
  mnc             VARCHAR(3) NULL,
  status          ENUM('queued','delivered','failed','expired') NOT NULL DEFAULT 'queued',
  priority        INT NOT NULL DEFAULT 9,
  attempts        INT NOT NULL DEFAULT 0,
  next_attempt_at DATETIME NOT NULL,
  valid_until     DATETIME NULL,
  received_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- when the vendor handed it to us
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_claim (status, next_attempt_at, id),
  KEY idx_msg (msg_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
