-- ============================================================================
-- 0004_blackhole_transport.sql — allow 'blackhole' on MDR records (2026-07-19)
-- Records for messages routed to a blackhole (simulator) vendor carry
-- transport='blackhole' (and, until Write is split, protocol mirrors it).
-- Applies to the template; live records_<account>_<YYYYMM> tables clone it, so
-- pre-existing tenant tables must be dropped (they re-clone) or altered:
--   ALTER TABLE records_<acct>_<ym> MODIFY transport ENUM('smpp','http','blackhole') NOT NULL,
--     MODIFY protocol ENUM('smpp','http','blackhole') NOT NULL;
-- ============================================================================

ALTER TABLE records_template
  MODIFY COLUMN transport ENUM('smpp','http','blackhole') NOT NULL,
  MODIFY COLUMN protocol  ENUM('smpp','http','blackhole') NOT NULL;
