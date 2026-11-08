-- ============================================================================
-- 0005_hlr_cache.sql — HLR in-memory cache support (2026-07-17)
-- last_check becomes the change-tracking column: a self-updating TIMESTAMP
-- (any INSERT/UPDATE bumps it) with an index so the 5-minute delta refresh
-- (WHERE last_check >= @last_sync) is cheap. is_deleted enables soft deletes:
-- the cache evicts the entry and lookups skip it (rows are never hard-deleted
-- so the delta sync can observe the removal).
--
-- Applies to hlr_template only. hlr_<first4> shards have dynamic names, so the
-- engine upgrades existing shards at startup (refdata.MySQL.Prepare); new
-- shards clone the template and are born correct.
-- ============================================================================

ALTER TABLE hlr_template
  MODIFY COLUMN last_check TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ADD COLUMN is_deleted TINYINT(1) NOT NULL DEFAULT 0,
  ADD INDEX idx_last_check (last_check);
