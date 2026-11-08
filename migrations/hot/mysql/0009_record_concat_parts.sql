-- ============================================================================
-- 0009_record_concat_parts.sql — fold completed concat groups onto the record
-- (2026-07-20)
--
-- D-040: `concat_parts` is a WORKING table — it exists so a receipt for any PDU
-- of a multi-part send can be correlated and the group status aggregated. Once
-- the group reaches its verdict that job is done, but the rows were never
-- cleaned up: the table grew unbounded and, because P8 retention only archives
-- records_*, it sat entirely outside the storage lifecycle.
--
-- On completion the per-PDU outcome is now folded into this column and the
-- working rows deleted, so the record is self-contained:
--   [{"seq":1,"vendor_msg_id":"748208537","status":"delivered"}, ...]
-- That keeps per-part forensics queryable without a join, and carries it into
-- the cold archive with the rest of the record.
-- ============================================================================

ALTER TABLE records_template
  ADD COLUMN concat_parts JSON NULL AFTER raw_dlr;
