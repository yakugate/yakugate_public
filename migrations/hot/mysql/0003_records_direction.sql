-- ============================================================================
-- 0003_records_direction.sql — record direction (2026-07-17)
-- PO decision: MT and MO history share the per-account record tables
-- (records_<account>_<YYYYMM>, cloned from records_template), distinguished by
-- direction. MT = outbound submits (default, matches all existing rows);
-- MO = inbound messages finalized by the MO receiver (Process 6) after client
-- handoff. New per-account tables clone the template and inherit the column;
-- no live records_* tables existed when this was applied.
-- ============================================================================

ALTER TABLE records_template
  ADD COLUMN direction ENUM('MT','MO') NOT NULL DEFAULT 'MT' AFTER account_id;
