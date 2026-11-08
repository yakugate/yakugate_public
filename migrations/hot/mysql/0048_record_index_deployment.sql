-- ============================================================================
-- 0048_record_index_deployment.sql — filter the message log by gateway
--
-- PO 2026-09-24, with hot/0046.
--
-- hot/0046 put the deployment on the log partitions, which is where it
-- belongs: that is the row that records what happened. But the message-log
-- SCREEN reads record_index first — it is the locator, and the partition is
-- only opened for the detail — so "show me everything that went through
-- Lahore" could not be asked without opening every partition in range.
--
-- One column on the locator makes it an ordinary predicate. It is the
-- ACCEPTING deployment, matching mtlog.deployment_id; which box sent it stays
-- on the partition row, where the rest of the vendor-side truth lives.
--
-- Deliberately NOT indexed. Every log query already narrows by account and
-- date first, and record_index carries the hot path's write cost — an index
-- here would be paid on every message to speed up a filter that is always
-- applied second.
-- ============================================================================
ALTER TABLE record_index
  ADD COLUMN deployment_id VARCHAR(64) NULL
    COMMENT 'YAKU_DEPLOYMENT_ID of the gateway that accepted this message';
