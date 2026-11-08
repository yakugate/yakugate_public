-- Signature pass, hot-store half (D-081 Appendix A + pinned DLR/vendor-ack
-- semantics + D-083 supersede rulings). Creates the PO-supplied summary
-- schema, drops the superseded hot/0014 tables and the rejectedlog_* family.
--
-- Deviations from Appendix A, each deliberate and small:
--  * summary_mt gains the vendor-ack event columns pinned 2026-07-30 (the
--    PO's "on-submit DLR" = submit_sm_resp, tracked as its own event with a
--    fourth latency clock).
--  * rejects_template.reason_code is VARCHAR(32), not 16 — our live reason
--    vocabulary (RATE_LIMIT_EXCEEDED, INSUFFICIENT_BALANCE...) exceeds 16.
--  * bucket_start comments say UTC explicitly — "gateway TZ" IS UTC (D-028).

CREATE TABLE summary_mt (
  bucket_start          DATETIME NOT NULL,                 -- 15-min, UTC, SUBMIT clock
  account_id            BIGINT UNSIGNED NOT NULL,          -- 0 = pre-auth reject
  vendor_id             INT UNSIGNED NOT NULL DEFAULT 0,
  mcc                   VARCHAR(3)  NOT NULL DEFAULT '',
  mnc                   VARCHAR(3)  NOT NULL DEFAULT '',
  sender_id             VARCHAR(20) NOT NULL DEFAULT '',
  sell_card_id          INT UNSIGNED NOT NULL DEFAULT 0,   -- versioned rate_card_rates.id (D-082); 0 = unrated
  buy_card_id           INT UNSIGNED NOT NULL DEFAULT 0,
  submitted_parts       INT UNSIGNED NOT NULL DEFAULT 0,
  source_rejected_parts INT UNSIGNED NOT NULL DEFAULT 0,   -- refused at gate; evidence in rejects_YYYYMM
  dlr_requested_parts   INT UNSIGNED NOT NULL DEFAULT 0,   -- FINAL receipts expected only (pinned 2026-07-30)
  delivered_parts       INT UNSIGNED NOT NULL DEFAULT 0,   -- booked at correlation, credited to submit bucket
  undeliv_parts         INT UNSIGNED NOT NULL DEFAULT 0,
  expired_parts         INT UNSIGNED NOT NULL DEFAULT 0,
  rejected_parts        INT UNSIGNED NOT NULL DEFAULT 0,   -- vendor-side REJECTD receipt
  vendor_accepted_parts INT UNSIGNED NOT NULL DEFAULT 0,   -- submit_sm_resp ROK / HTTP accepted (vendor-ack event)
  vendor_rejected_parts INT UNSIGNED NOT NULL DEFAULT 0,   -- submit_sm_resp error / HTTP refused
  sum_vendor_ack_sec    BIGINT UNSIGNED NOT NULL DEFAULT 0,-- 4th clock: submit -> vendor ack
  max_vendor_ack_sec    INT UNSIGNED NOT NULL DEFAULT 0,
  sum_latency_sec       BIGINT UNSIGNED NOT NULL DEFAULT 0,-- submit -> receipt received
  sum_handset_sec       BIGINT UNSIGNED NOT NULL DEFAULT 0,-- submit -> done_date
  sum_forward_lag_sec   BIGINT UNSIGNED NOT NULL DEFAULT 0,-- done_date -> received
  max_forward_lag_sec   INT UNSIGNED NOT NULL DEFAULT 0,
  neg_lag_parts         INT UNSIGNED NOT NULL DEFAULT 0,
  no_done_date_parts    INT UNSIGNED NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq (bucket_start, account_id, vendor_id, mcc, mnc, sender_id, sell_card_id, buy_card_id),
  KEY idx_vendor  (vendor_id, bucket_start),
  KEY idx_account (account_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE summary_dlr_flow (
  bucket_start        DATETIME NOT NULL,                   -- 15-min, UTC, ARRIVAL clock
  vendor_id           INT UNSIGNED NOT NULL,
  sender_id           VARCHAR(20) NOT NULL DEFAULT '',
  received_parts      INT UNSIGNED NOT NULL DEFAULT 0,
  delivered_parts     INT UNSIGNED NOT NULL DEFAULT 0,
  undeliv_parts       INT UNSIGNED NOT NULL DEFAULT 0,
  expired_parts       INT UNSIGNED NOT NULL DEFAULT 0,
  rejected_parts      INT UNSIGNED NOT NULL DEFAULT 0,
  orphan_parts        INT UNSIGNED NOT NULL DEFAULT 0,
  sum_forward_lag_sec BIGINT UNSIGNED NOT NULL DEFAULT 0,
  max_forward_lag_sec INT UNSIGNED NOT NULL DEFAULT 0,
  neg_lag_parts       INT UNSIGNED NOT NULL DEFAULT 0,
  no_done_date_parts  INT UNSIGNED NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq (bucket_start, vendor_id, sender_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE summary_mo (
  bucket_start           DATETIME NOT NULL,                -- 15-min, UTC, ARRIVAL clock
  account_id             BIGINT UNSIGNED NOT NULL,
  vendor_id              INT UNSIGNED NOT NULL,
  mcc                    VARCHAR(3)  NOT NULL DEFAULT '',
  mnc                    VARCHAR(3)  NOT NULL DEFAULT '',
  sender_id              VARCHAR(20) NOT NULL DEFAULT '',  -- shortcode the MO arrived on
  sell_card_id           INT UNSIGNED NOT NULL DEFAULT 0,
  buy_card_id            INT UNSIGNED NOT NULL DEFAULT 0,
  received_parts         INT UNSIGNED NOT NULL DEFAULT 0,
  client_delivered_parts INT UNSIGNED NOT NULL DEFAULT 0,
  client_failed_parts    INT UNSIGNED NOT NULL DEFAULT 0,
  client_retry_parts     INT UNSIGNED NOT NULL DEFAULT 0,
  sum_client_latency_sec BIGINT UNSIGNED NOT NULL DEFAULT 0,
  max_client_latency_sec INT UNSIGNED NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq (bucket_start, account_id, vendor_id, mcc, mnc, sender_id, sell_card_id, buy_card_id),
  KEY idx_account (account_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE summary_lookup (
  bucket_start   DATETIME NOT NULL,                        -- 15-min, UTC, REQUEST clock; NOT rebuildable (primary count)
  service        ENUM('hlr','crp') NOT NULL,
  account_id     BIGINT UNSIGNED NOT NULL,
  vendor_id      INT UNSIGNED NOT NULL DEFAULT 0,          -- 0 = served from cache; else http_endpoints id
  mcc            VARCHAR(3) NOT NULL DEFAULT '',
  mnc            VARCHAR(3) NOT NULL DEFAULT '',
  sell_card_id   INT UNSIGNED NOT NULL DEFAULT 0,          -- 0 until lookups are client-billed (D-082 default_hlr card)
  buy_card_id    INT UNSIGNED NOT NULL DEFAULT 0,
  requests       INT UNSIGNED NOT NULL DEFAULT 0,
  cache_hits     INT UNSIGNED NOT NULL DEFAULT 0,
  found          INT UNSIGNED NOT NULL DEFAULT 0,
  not_found      INT UNSIGNED NOT NULL DEFAULT 0,
  ported_parts   INT UNSIGNED NOT NULL DEFAULT 0,
  failed         INT UNSIGNED NOT NULL DEFAULT 0,
  sum_latency_ms BIGINT UNSIGNED NOT NULL DEFAULT 0,
  max_latency_ms INT UNSIGNED NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq (bucket_start, service, account_id, vendor_id, mcc, mnc, sell_card_id, buy_card_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE summary_err_daily (
  day       DATE NOT NULL,
  service   ENUM('mt','mo','hlr','crp') NOT NULL,
  vendor_id INT UNSIGNED NOT NULL DEFAULT 0,               -- 0 = the gateway itself (source-reject reasons)
  mcc       VARCHAR(3) NOT NULL DEFAULT '',
  mnc       VARCHAR(3) NOT NULL DEFAULT '',
  err_code  VARCHAR(16) NOT NULL DEFAULT '',               -- top-N per vendor, tail bucketed 'other'; DLR errs under 'mt'
  parts     BIGINT UNSIGNED NOT NULL DEFAULT 0,
  UNIQUE KEY uq (day, service, vendor_id, mcc, mnc, err_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE summary_live (
  minute_start DATETIME NOT NULL,                          -- MINUTE grain, self-pruning 48h
  service      ENUM('mt','mo','dlr','hlr','crp') NOT NULL,
  vendor_id    INT UNSIGNED NOT NULL,
  in_parts     INT UNSIGNED NOT NULL DEFAULT 0,
  ok_parts     INT UNSIGNED NOT NULL DEFAULT 0,
  failed_parts INT UNSIGNED NOT NULL DEFAULT 0,
  UNIQUE KEY uq (minute_start, service, vendor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- rejects_YYYYMM (GLOBAL monthly, D-083): accepted = record; refused = reject.
-- No body, never charged, never cold-exported.
CREATE TABLE rejects_template (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  rejected_at   DATETIME(3) NOT NULL,
  account_id    BIGINT UNSIGNED NULL,                      -- NULL = refused before auth
  channel       ENUM('http','smpp') NOT NULL,
  client_msg_id VARCHAR(64) NULL,
  sender_id     VARCHAR(20) NULL,
  msisdn        VARCHAR(20) NULL,
  parts         SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  reason_code   VARCHAR(32) NOT NULL,
  reason_detail VARCHAR(128) NULL,
  KEY idx_acct   (account_id, rejected_at),
  KEY idx_client (account_id, client_msg_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Drill-down contract (Appendix A #8, adapted: the live record family is
-- mtlog_*, its submit-time column is accepted_at; template only per the PO's
-- own note — existing clones get the fan-out step later).
ALTER TABLE mtlog_template ADD KEY idx_drill (accepted_at, vendor_id);

-- Vendor-ack timestamp (pinned 2026-07-30) on the MT log — template + every
-- live clone (per-account tables can receive traffic before their next
-- daily rollover).
ALTER TABLE mtlog_template ADD COLUMN vendor_ack_at DATETIME(3) DEFAULT NULL AFTER accepted_at;
ALTER TABLE mtlog_aracct_27_07_2026 ADD COLUMN vendor_ack_at DATETIME(3) DEFAULT NULL AFTER accepted_at;
ALTER TABLE mtlog_demo_26_07_2026 ADD COLUMN vendor_ack_at DATETIME(3) DEFAULT NULL AFTER accepted_at;
ALTER TABLE mtlog_demo_27_07_2026 ADD COLUMN vendor_ack_at DATETIME(3) DEFAULT NULL AFTER accepted_at;
ALTER TABLE mtlog_sidacct_26_07_2026 ADD COLUMN vendor_ack_at DATETIME(3) DEFAULT NULL AFTER accepted_at;
ALTER TABLE mtlog_sidacct_27_07_2026 ADD COLUMN vendor_ack_at DATETIME(3) DEFAULT NULL AFTER accepted_at;

-- done_date parsed onto receipts (pinned 2026-07-30) — template + clones.
ALTER TABLE dlrlog_template ADD COLUMN done_date DATETIME DEFAULT NULL AFTER delivered_at;
ALTER TABLE dlrlog_aracct_27_07_2026 ADD COLUMN done_date DATETIME DEFAULT NULL AFTER delivered_at;
ALTER TABLE dlrlog_demo_26_07_2026 ADD COLUMN done_date DATETIME DEFAULT NULL AFTER delivered_at;
ALTER TABLE dlrlog_demo_27_07_2026 ADD COLUMN done_date DATETIME DEFAULT NULL AFTER delivered_at;
ALTER TABLE dlrlog_sidacct_26_07_2026 ADD COLUMN done_date DATETIME DEFAULT NULL AFTER delivered_at;
ALTER TABLE dlrlog_sidacct_27_07_2026 ADD COLUMN done_date DATETIME DEFAULT NULL AFTER delivered_at;

-- record_index.updated_at: the reconcile job's dirty-bucket watermark.
ALTER TABLE record_index
  ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ADD KEY idx_record_index_updated (updated_at);

-- Supersedes (D-081/D-083, PO-confirmed drops): the six schema-only hot/0014
-- summary tables, and the per-tenant rejectedlog_* family (replaced by the
-- global monthly rejects_YYYYMM; the reject writer repoints in this same
-- change — internal/store/mysql).
DROP TABLE IF EXISTS mt_billing_summary;
DROP TABLE IF EXISTS mo_billing_summary;
DROP TABLE IF EXISTS dlr_summary;
DROP TABLE IF EXISTS rejected_summary;
DROP TABLE IF EXISTS billing_summary;
DROP TABLE IF EXISTS hlr_billing_summary;
DROP TABLE IF EXISTS rejectedlog_template;
DROP TABLE IF EXISTS rejectedlog_countryacct_26_07_2026;
DROP TABLE IF EXISTS rejectedlog_demo_27_07_2026;
DROP TABLE IF EXISTS rejectedlog_demo_28_07_2026;
DROP TABLE IF EXISTS rejectedlog_demo_29_07_2026;
DROP TABLE IF EXISTS rejectedlog_demo_30_07_2026;
DROP TABLE IF EXISTS rejectedlog_ipacct_28_07_2026;
DROP TABLE IF EXISTS rejectedlog_prepaidacct_27_07_2026;
DROP TABLE IF EXISTS rejectedlog_prepaidacct_28_07_2026;
DROP TABLE IF EXISTS rejectedlog_quietacct_28_07_2026;
