-- ============================================================================
-- 0008_udh_concat.sql — SMPP concatenation group (2026-07-18)
-- Per-part model (PO decision 2026-07-18): each UDH part stays its own message
-- with its own msg_id; these columns link the parts of one long SMS so vendor
-- egress can re-emit the UDH and DLRs correlate. 0 = single-part (not concat).
-- Template only: live queue_<vendor> tables have dynamic names — upgrade manually:
--   ALTER TABLE queue_<vendor>
--     ADD COLUMN udh_ref   INT NOT NULL DEFAULT 0 AFTER segments,
--     ADD COLUMN udh_total INT NOT NULL DEFAULT 0 AFTER udh_ref,
--     ADD COLUMN udh_seq   INT NOT NULL DEFAULT 0 AFTER udh_total;
-- New vendor queues clone the template and are born correct.
-- ============================================================================

ALTER TABLE queue_template
  ADD COLUMN udh_ref   INT NOT NULL DEFAULT 0 AFTER segments,
  ADD COLUMN udh_total INT NOT NULL DEFAULT 0 AFTER udh_ref,
  ADD COLUMN udh_seq   INT NOT NULL DEFAULT 0 AFTER udh_total;
