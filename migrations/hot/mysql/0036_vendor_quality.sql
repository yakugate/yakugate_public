-- 0036_vendor_quality.sql — quality routing per vendor AND network, from
-- counts (PO 2026-09-14, routing rules review).
--
-- route_type='quality' used to score a vendor with one SQL count over
-- record_index every 30 seconds in every sending process, blended across all
-- networks. Now the processes that see a verdict (receipts, vendor refusals,
-- dead-letters, expiry) count it in memory and add the counts here once a few
-- seconds, per minute, vendor and network. The senders read the last hour back.
-- With Redis on, the same counts go to Redis instead and this table stays empty.
--
-- record_index.mcc/mnc: a receipt reaches the gateway with only the vendor's
-- id for the message; this locator row is all that is left to say which
-- network the message went to — the same reason bind_slot and no_receipt ride
-- here. NULL on rows written before this migration.
ALTER TABLE record_index
  ADD COLUMN mcc VARCHAR(3) NULL COMMENT 'destination network at send, for quality counts',
  ADD COLUMN mnc VARCHAR(3) NULL COMMENT 'destination network at send, for quality counts';

CREATE TABLE IF NOT EXISTS vendor_quality_minute (
  minute    DATETIME        NOT NULL COMMENT 'UTC minute the verdicts arrived in',
  vendor_id BIGINT UNSIGNED NOT NULL,
  mcc       VARCHAR(3)      NOT NULL DEFAULT '',
  mnc       VARCHAR(3)      NOT NULL DEFAULT '' COMMENT 'normalised: 03, not 003',
  delivered INT UNSIGNED    NOT NULL DEFAULT 0,
  judged    INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'delivered + failed + expired',
  PRIMARY KEY (minute, vendor_id, mcc, mnc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
