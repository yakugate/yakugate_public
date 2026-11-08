-- ============================================================================
-- 0053_monitor_push_sec.sql — each sample says how often its role reports
-- (Saleem 2026-10-05, D-126)
--
-- YAKU_STATUS_PUSH_SEC (default 5, at least 5) is a gateway setting, read by
-- each role at start. The monitoring screens no longer have an interval of
-- their own: they read the samples and say how far apart they are, so the
-- figure travels with the data instead of being guessed. Rows written before
-- this say 5, which is what every gateway used until now.
--
-- tps_rejected is the refused-message rate beside tps (accepted), for the
-- "refusals jumped" event: the counters alone are lifetime totals.
-- One column per ALTER (the bootstrap's rule since 2026-09-24).
-- ============================================================================
ALTER TABLE monitor_samples
  ADD COLUMN push_sec SMALLINT UNSIGNED NOT NULL DEFAULT 5
    COMMENT 'YAKU_STATUS_PUSH_SEC of the reporting process: seconds between its samples';

ALTER TABLE monitor_samples
  ADD COLUMN tps_rejected DECIMAL(10,2) NOT NULL DEFAULT 0
    COMMENT 'messages refused at acceptance, per second (60-second rate)';
