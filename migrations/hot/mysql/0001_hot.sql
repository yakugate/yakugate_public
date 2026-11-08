-- ============================================================================
-- STORE 3: HOT BACKUP — MySQL.  PER-TENANT physical isolation.
-- MDRs are written to a per-tenant, per-MONTH table cloned from records_template:
--     records_<account_id>_<YYYYMM>     (e.g. records_42_202607)
-- The month is the account's timezone month (see accounts.timezone). Cold
-- archival exports a table's rows to per-day files and drops the table.
--
-- ONE table template only. There is exactly ONE shape in the hot store —
-- records_template. The former relationship tables (record_index, dlr_log) are
-- REMOVED; their data is folded into the record itself:
--   * messageids  JSON — every id for the message (gateway ULID, client
--                        messageId, and the per-part vendor message ids) with
--                        part-wise sequencing (seq / total).
--   * raw_dlr     JSON — an array of the raw delivery receipts, ONE ENTRY PER
--                        PART, each tagged with its segment seq. Holds multiple
--                        DLRs for a single logical message (multi-segment SMS).
-- Cross-tenant DLR correlation now runs through the queue/HLR store
-- (delivery_reports, Process 5); once correlated, the Finalizer (Process 8)
-- knows account + service month and updates the owning record, appending the
-- receipt to raw_dlr. No separate global index/table is needed.
-- ============================================================================

-- Shape for per-tenant-month tables. NOT written to directly; cloned per tenant-month.
CREATE TABLE records_template (
  id               BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  msg_id           CHAR(26) NOT NULL,                    -- gateway ULID (primary id)
  client_msg_id    VARCHAR(64) NULL,                     -- client idempotency messageId
  messageids       JSON,                                 -- all ids + part sequencing (see below)
  account_id       BIGINT UNSIGNED,
  username         VARCHAR(64),
  sender           VARCHAR(32),
  destination      VARCHAR(20),
  operator_id      INT,
  vendor_id        BIGINT UNSIGNED,
  transport        ENUM('smpp','http'),
  channel          VARCHAR(32) NULL,
  segments         INT,                                  -- total parts (sequencing total)
  status           ENUM('queued','sent','delivered','partial','failed','expired') NOT NULL,
  error_code       VARCHAR(16),
  raw_dlr          JSON,                                 -- array of per-part raw DLRs (see below)
  service_day      DATE,                                 -- account-timezone calendar day
  submit_date      DATETIME,
  delivery_date    DATETIME,                             -- final part's done date (aggregate)
  vendor_msg_id    VARCHAR(128),                         -- first/primary part vendor id (also in messageids)
  original_message TEXT,
  charge           DECIMAL(12,4) NULL,                   -- rated cost (billing on the record)
  rate_cards       JSON,
  created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_msg (msg_id),
  KEY idx_client_msg (account_id, client_msg_id),        -- client status lookup
  KEY idx_service_day (service_day),
  KEY idx_vendor_msg (vendor_id, vendor_msg_id)          -- primary-part DLR correlation
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- messageids JSON shape (multiple ids + part-wise sequencing):
--   {
--     "gateway": "01J29XYZ8QK4T2R7VMBGA9WNPE",       -- our ULID (== msg_id)
--     "client":  "order-7842-otp-1",                 -- client messageId (== client_msg_id)
--     "parts": [
--       { "seq": 1, "total": 3, "vendor_msg_id": "9f2c.1" },
--       { "seq": 2, "total": 3, "vendor_msg_id": "9f2c.2" },
--       { "seq": 3, "total": 3, "vendor_msg_id": "9f2c.3" }
--     ]
--   }
--
-- raw_dlr JSON shape (one entry per part; multiple DLRs per logical message):
--   [
--     { "seq": 1, "direction": "esme", "vendor_msg_id": "9f2c.1",
--       "stat": "DELIVRD", "err": "000", "done_date": "2607081230",
--       "raw": "id:9f2c.1 sub:001 dlvrd:001 ... stat:DELIVRD err:000 text:...",
--       "tlv": { "message_state": 2, "receipted_message_id": "9f2c.1" },
--       "received_at": "2026-07-08T12:30:05Z" },
--     { "seq": 2, "direction": "esme", "vendor_msg_id": "9f2c.2", "stat": "DELIVRD", ... },
--     { "seq": 3, "direction": "esme", "vendor_msg_id": "9f2c.3", "stat": "UNDELIV", ... }
--   ]
--   direction: "esme" = receipt received from the vendor SMSC (Yakugate as ESME);
--              "smsc"  = receipt Yakugate owes/pushed to the client (adds "delivered_at").
--   status = 'delivered' when every part is DELIVRD; 'partial' when some parts
--            delivered and others failed/undelivered; 'failed'/'expired' otherwise.
-- ----------------------------------------------------------------------------
