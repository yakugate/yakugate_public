-- ============================================================================
-- 0010_blackhole_transport.sql — allow 'blackhole' transport on queue (2026-07-19)
-- A blackhole (simulator) vendor stamps transport='blackhole' onto its queue rows.
-- Template only: live queue_<vendor> tables have dynamic names — upgrade manually:
--   ALTER TABLE queue_<vendor> MODIFY transport ENUM('smpp','http','blackhole') NOT NULL;
-- ============================================================================

ALTER TABLE queue_template
  MODIFY COLUMN transport ENUM('smpp','http','blackhole') NOT NULL;
