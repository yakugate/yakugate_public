-- ============================================================================
-- 0002  Schema v2 consolidation — HOT store (MySQL)
-- Matches database-structure.docx v2.0 (2026-07-13): records_template (full
-- lifecycle incl. rejected/discarded, three delivery timestamps, body privacy)
-- + record_index (persistent per-message locator; distinct from the queue-store
-- delivery_reports transient inbox). Pre-alpha: DROP+CREATE to final shape.
-- ============================================================================

DROP TABLE IF EXISTS records_template;
DROP TABLE IF EXISTS record_index;

-- 3.1 records_template -> records_<account>_<YYYYMM> ---------------------------
CREATE TABLE records_template (
  id                   BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  msg_id               CHAR(26) NOT NULL UNIQUE,
  trace_id             CHAR(26) NULL,
  request_id           CHAR(26) NULL,
  account_id           BIGINT UNSIGNED,
  sender               VARCHAR(32),
  destination          VARCHAR(15),
  body                 TEXT NULL,
  body_enc             ENUM('plain_text','pii_masked','all_encrypted') NOT NULL DEFAULT 'plain_text',
  campaign_hash        CHAR(64) NULL,
  encoding             ENUM('gsm7','ucs2'),
  segments             INT,
  channel              VARCHAR(32) NULL,
  priority             INT,
  protocol             ENUM('http','smpp'),
  ip_address           VARCHAR(64) NULL,
  mcc                  VARCHAR(3),
  mnc                  VARCHAR(3),
  vendor_id            BIGINT UNSIGNED NULL,
  transport            ENUM('smpp','http') NULL,
  route_id             BIGINT UNSIGNED NULL,
  template_id          BIGINT UNSIGNED NULL,
  status               ENUM('queued','sent','delivered','failed','expired','rejected','discarded') NOT NULL,
  reject_reason        VARCHAR(190) NULL,
  error_code           VARCHAR(16) NULL,
  vendor_msg_id        VARCHAR(64) NULL,
  raw_dlr              JSON NULL,
  accepted_at          DATETIME,
  delivered_network_at DATETIME NULL,
  delivered_handset_at DATETIME NULL,
  rate_cards           JSON NULL,
  created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_msg (msg_id),
  KEY idx_vendor_msg (vendor_id, vendor_msg_id),
  KEY idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3.2 record_index (persistent per-message locator) ----------------------------
CREATE TABLE record_index (
  msg_id        CHAR(26) PRIMARY KEY,
  account_id    BIGINT UNSIGNED NOT NULL,
  ym            CHAR(6) NOT NULL,
  vendor_id     BIGINT UNSIGNED NULL,
  vendor_msg_id VARCHAR(64) NULL,
  status        VARCHAR(12),
  KEY idx_vendor_msg (vendor_msg_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
