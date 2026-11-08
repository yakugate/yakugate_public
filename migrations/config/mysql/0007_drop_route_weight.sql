-- ============================================================================
-- 0007_drop_route_weight.sql — remove routes.weight (2026-07-17)
-- PO decision: weight was loaded but never used by the route stage (no weighted
-- distribution is designed); drop it rather than carry a dead knob. If weighted
-- routing ever lands, it returns as a new column with real semantics.
-- ============================================================================

ALTER TABLE routes DROP COLUMN weight;
