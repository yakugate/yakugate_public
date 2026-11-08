-- ============================================================================
-- 0007_concat_parts.sql — per-part DLR correlation for concatenated SMS (2026-07-20)
-- D-037: a multi-part message is N submit_sm PDUs, so the vendor issues N message
-- ids and emits N receipts. record_index keys on msg_id (one row per message), so
-- it can only hold ONE vendor id — the other parts' receipts had nowhere to
-- correlate and were silently dropped, letting a message report 'delivered' on
-- the strength of part 1 alone while later parts were unverified (or failed).
--
-- This table is the (vendor_id, vendor_msg_id) -> msg_id alias for EVERY part,
-- carrying each part's own receipt status so the group status can be aggregated:
--   all parts delivered -> delivered;  any part failed/expired -> failed;
--   otherwise still waiting (leave the record 'sent').
-- ============================================================================

CREATE TABLE concat_parts (
  vendor_id     BIGINT UNSIGNED NOT NULL,
  vendor_msg_id VARCHAR(64) NOT NULL,
  msg_id        CHAR(26) NOT NULL,
  part_seq      INT NOT NULL,
  parts_total   INT NOT NULL,
  status        VARCHAR(12) NULL,          -- this part's own receipt status
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (vendor_id, vendor_msg_id),
  KEY idx_msg (msg_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
