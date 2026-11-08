-- 0025_orphan_tables.sql — keep the traffic we cannot place (PO 2026-08-31:
-- "we need to store un routed mo as well, into an orphan table, dlr can also be
-- orphan an needs to saved").
--
-- Two things were being ACKed to the vendor and then dropped with only a log
-- line:
--
--   ORPHAN MO  — an inbound message for a shortcode no MO route owns. The
--                vendor is told 200, the subscriber is charged by their
--                operator, and nothing on our side remembers it existed.
--                (P6-B8.)
--   ORPHAN DLR — a receipt whose vendor_msg_id resolves to no message of ours:
--                foreign, stale, or one that simply lost a race with its own
--                record. Two of the three code paths that hit this did not even
--                log it.
--
-- Both are now recorded. PO ruling: RECORD ONLY — never replayed, never
-- re-ingested. They are evidence of what arrived and could not be placed, not a
-- queue. And no volume cap (PO ruling): a vendor sending endless unmatchable
-- traffic writes endless rows, bounded only by retention. That is a deliberate
-- choice; the writes are best-effort and off the critical path so a flood
-- cannot stall receipt handling or MO intake.
--
-- Monthly, template-cloned, exactly like mo_log — same retention machinery, no
-- new lifecycle to own.
CREATE TABLE IF NOT EXISTS orphan_mo_template (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  vendor_id    BIGINT UNSIGNED DEFAULT NULL,
  source       VARCHAR(32) DEFAULT NULL,   -- subscriber MSISDN
  destination  VARCHAR(32) DEFAULT NULL,   -- the shortcode nobody owns
  body         TEXT,
  encoding     VARCHAR(8) DEFAULT NULL,
  mcc          VARCHAR(3) DEFAULT NULL,
  mnc          VARCHAR(3) DEFAULT NULL,
  reason       VARCHAR(64) DEFAULT NULL,   -- why it could not be placed
  received_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_destination (destination),
  KEY idx_received (received_at),
  KEY idx_vendor (vendor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS orphan_dlr_template (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  vendor_id     BIGINT UNSIGNED DEFAULT NULL,
  vendor_msg_id VARCHAR(64) DEFAULT NULL,
  status        VARCHAR(16) DEFAULT NULL,  -- the vendor's raw status token
  error_code    VARCHAR(32) DEFAULT NULL,
  raw           TEXT,                      -- the receipt as it arrived
  reason        VARCHAR(64) DEFAULT NULL,
  received_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_vendor_msg (vendor_id, vendor_msg_id),
  KEY idx_received (received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
