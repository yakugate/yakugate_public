-- ============================================================================
-- 0010_route_weight.sql — restore per-route weight for loadbalance (2026-07-19)
-- weight was dropped in 0007 when loadbalance was unimplemented. route_type
-- selection (PO decisions 2026-07-19): priority tiers; within a loadbalance tier
-- routes are chosen round-robin proportional to weight. Higher weight = more share.
-- ============================================================================

ALTER TABLE routes
  ADD COLUMN weight INT NOT NULL DEFAULT 100 AFTER route_type;
