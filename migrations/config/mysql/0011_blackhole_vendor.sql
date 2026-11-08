-- ============================================================================
-- 0011_blackhole_vendor.sql — blackhole (simulator) vendor type (2026-07-19)
-- A blackhole vendor is a full vendor SIMULATOR: messages route + enqueue to it
-- normally, records + DLRs are written exactly as a real vendor, but the message
-- is NEVER transmitted onward. dlr_config carries the outcome split, e.g.
--   {"delivered_pct":90,"failed_pct":5,"nodlr_pct":5}
-- The dedicated `blackhole` worker claims its queue and applies the outcomes.
-- ============================================================================

ALTER TABLE vendors
  MODIFY COLUMN transport ENUM('smpp','http','blackhole') NOT NULL;
