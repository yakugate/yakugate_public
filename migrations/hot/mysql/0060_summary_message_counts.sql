-- ============================================================================
-- 0060_summary_message_counts.sql — the dashboard counts messages from the
-- summaries, not from the records (Saleem 2026-10-13: "dashboard is stuck at
-- loading when choosing all time, even takes long for today")
--
-- summary_mt and summary_mo count PARTS. The dashboard shows MESSAGES and
-- their outcome, so it counted them from the records themselves:
--   SELECT status, COUNT(*) FROM record_index … GROUP BY status
--       all time = the whole table: 14 million rows, 63 seconds on the dev box
--   SELECT COUNT(*), SUM(segments) FROM mo_log_<month> WHERE received_at …
--       the whole month for ANY period: mo_log has no key on received_at
--
-- Message counts are added beside the part counts. The billing role fills
-- them in the same statement that fills the parts (one bucket = one GROUP BY
-- over that bucket's messages), and fills the buckets written before this
-- migration once, in the background.
--
--   summary_mt.submitted_msgs   messages in the bucket
--   summary_mt.delivered_msgs   … whose record is `delivered`
--   summary_mt.failed_msgs      … `failed`
--   summary_mt.expired_msgs     … `expired`   (the rest are still on their way)
--   summary_mo.received_msgs    incoming messages in the bucket
--
-- NULL = not counted yet (a bucket from before this migration that the
-- background fill has not reached). Never 0 for that: 0 is a count.
--
-- mologtemplate.idx_received: an incoming-message bucket is rebuilt by its
-- receive time. Without a key every rebuild read the whole month. Months that
-- already exist gain the key after start, in the background (bootstrap
-- latekeys.go).
--
-- One column, one key per statement: the bootstrap treats "already there" as
-- done and skips the WHOLE statement.
-- ============================================================================
ALTER TABLE summary_mt ADD COLUMN submitted_msgs INT UNSIGNED NULL;
ALTER TABLE summary_mt ADD COLUMN delivered_msgs INT UNSIGNED NULL;
ALTER TABLE summary_mt ADD COLUMN failed_msgs INT UNSIGNED NULL;
ALTER TABLE summary_mt ADD COLUMN expired_msgs INT UNSIGNED NULL;
ALTER TABLE summary_mo ADD COLUMN received_msgs INT UNSIGNED NULL;
ALTER TABLE mologtemplate ADD KEY idx_received (received_at);
