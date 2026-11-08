-- ============================================================================
-- 0002  Process 3 (SMPP Vendor Sender) — QUEUE / HLR store additions (MySQL)
-- New: delivery_reports (raw DLR holding, read by Process 5) and mo_template
-- (per-user MO holding: mo_<username>, read by Process 6). Plus a claim lease
-- on the pending queue so stuck (lease-expired) rows are reclaimable.
-- ============================================================================

-- Raw delivery receipts awaiting correlation by Process 5.
-- Process 3 writes here (all receipt fields + TLVs), acks the SMSC AFTER this
-- durable write, and Process 5 reads/correlates (normalizing vendor_msg_id per
-- the vendor's rule — see config.vendors.msgid_transform).
CREATE TABLE delivery_reports (
  id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  vendor_id     BIGINT UNSIGNED NOT NULL,
  vendor_msg_id VARCHAR(128) NOT NULL,               -- id parsed from the receipt (raw, verbatim)
  stat          VARCHAR(24),                          -- DELIVRD / UNDELIV / EXPIRED / REJECTD / ...
  err_code      VARCHAR(16),
  submit_date   DATETIME NULL,
  done_date     DATETIME NULL,
  receipt_text  TEXT,                                 -- full short_message receipt text
  tlvs          JSON,                                 -- all TLVs: receipted_message_id(0x001E), message_state(0x0427), ...
  raw           MEDIUMTEXT,                           -- raw deliver_sm params (hex/base64) for diagnostics
  session_id    VARCHAR(64),                          -- session that received it
  received_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  processed     TINYINT(1) NOT NULL DEFAULT 0,        -- Process 5 sets 1 after correlation
  attempts      INT NOT NULL DEFAULT 0,
  KEY idx_corr (vendor_id, vendor_msg_id),            -- correlation lookup
  KEY idx_unprocessed (processed, received_at)        -- Process 5 scan
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-user inbound MO holding. Cloned to mo_<username> per user (like queue_<cid>);
-- Process 6 reads and routes to the client (accounts.mo_url).
CREATE TABLE mo_template (
  id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id    BIGINT UNSIGNED,
  username      VARCHAR(64),
  vendor_id     BIGINT UNSIGNED,
  source        VARCHAR(32) NOT NULL,                 -- originating MSISDN
  destination   VARCHAR(32) NOT NULL,                 -- our shortcode/number that received it
  body          TEXT,
  encoding      ENUM('gsm7','ucs2') NOT NULL DEFAULT 'gsm7',
  tlvs          JSON,                                 -- all TLVs / params
  raw           MEDIUMTEXT,
  session_id    VARCHAR(64),
  received_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  delivered     TINYINT(1) NOT NULL DEFAULT 0,        -- Process 6 sets 1 after routing to client
  attempts      INT NOT NULL DEFAULT 0,
  KEY idx_undelivered (delivered, received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Claim lease so Process 3 (and other senders) never double-send; a claimed row
-- whose lease_until has passed is treated as stuck and reclaimed.
ALTER TABLE queue_template
  MODIFY status ENUM('queued','claimed','sent','failed','expired') NOT NULL DEFAULT 'queued',
  ADD COLUMN lease_until DATETIME NULL AFTER next_attempt_at,   -- visibility lease
  ADD COLUMN session_id  VARCHAR(64) NULL AFTER lease_until;    -- claiming session
-- Re-point the claim index to include the lease.
ALTER TABLE queue_template
  ADD KEY idx_claim_lease (status, lease_until, next_attempt_at, id);
