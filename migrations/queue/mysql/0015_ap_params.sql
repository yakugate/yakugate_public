-- ============================================================================
-- 0015_ap_params.sql — custom pass-through parameters (2026-07-22)
--
-- PO decision 2026-07-22: adopt ap1/ap2/apg1/apg2 from the published Hadal
-- contract (scope Draft 4.0 item S-06, previously deferred — reinstated because
-- they appear in the customer-facing API docs, so migrating clients send them).
--
-- Semantics per the published spec:
--   ap1, ap2   — custom metadata, carried to the message record (logs only)
--   apg1, apg2 — custom metadata, carried to the record AND intended as
--                grouping dimensions for the reporting rollups
--
-- They ride the queue row because the MDR is written at terminal state (D-030)
-- by the vendor sender, which rebuilds the message from the queue — anything
-- not on the queue row is lost between accept and record.
--
-- NOTE: ALTERs the template. Existing queue_<cid> clones are reconciled
-- automatically at startup by bootstrap.syncCloneColumns.
-- ============================================================================

ALTER TABLE queue_template
  ADD COLUMN ap1  VARCHAR(50) NULL AFTER channel,
  ADD COLUMN ap2  VARCHAR(50) NULL AFTER ap1,
  ADD COLUMN apg1 VARCHAR(50) NULL AFTER ap2,
  ADD COLUMN apg2 VARCHAR(50) NULL AFTER apg1;
