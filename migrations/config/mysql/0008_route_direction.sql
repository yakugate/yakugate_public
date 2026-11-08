-- ============================================================================
-- 0008_route_direction.sql — route direction (2026-07-17)
-- direction: MT (outbound, client -> vendor; the P1 submit path) or
-- MO (inbound, vendor -> client; consumed by the future MO receiver process).
-- The submit pipeline's route stage only considers MT routes; MO routes are
-- invisible to it. Default MT keeps all existing routes live.
-- ============================================================================

ALTER TABLE routes
  ADD COLUMN direction ENUM('MT','MO') NOT NULL DEFAULT 'MT' AFTER route_type;
