-- ============================================================================
-- 0011_ap_params.sql — custom pass-through parameters on the MDR (2026-07-22)
--
-- Destination for the ap1/ap2/apg1/apg2 values carried on the queue row
-- (queue/0015_ap_params.sql). apg1/apg2 are indexed because the published
-- contract describes them as grouping dimensions ("included in logs AND
-- summaries") — they are what the reporting rollups aggregate by.
--
-- NOTE: ALTERs the template. Existing records_<tenant>_<date> partitions are
-- reconciled automatically at startup by bootstrap.syncCloneColumns.
-- ============================================================================

ALTER TABLE records_template
  ADD COLUMN ap1  VARCHAR(50) NULL AFTER channel,
  ADD COLUMN ap2  VARCHAR(50) NULL AFTER ap1,
  ADD COLUMN apg1 VARCHAR(50) NULL AFTER ap2,
  ADD COLUMN apg2 VARCHAR(50) NULL AFTER apg1,
  ADD KEY idx_apg (apg1, apg2);
