-- ============================================================================
-- 0006_route_id.sql — routing audit trail (2026-07-17)
-- route_id records WHICH route won for each queued message (routes.id in the
-- config store; NULL for legacy rows). Template only: live queue_<vendor>
-- tables have dynamic names — upgrade them manually:
--   ALTER TABLE queue_<vendor> ADD COLUMN route_id BIGINT UNSIGNED NULL AFTER vendor_id;
-- New vendor queues clone the template and are born correct.
-- ============================================================================

ALTER TABLE queue_template
  ADD COLUMN route_id BIGINT UNSIGNED NULL AFTER vendor_id;
