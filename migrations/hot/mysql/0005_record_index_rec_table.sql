-- ============================================================================
-- 0005_record_index_rec_table.sql — store the resolved MDR table name (2026-07-19)
-- P8/D-030: hot MDR tables become daily-per-username (records_<user>_<DD_MM_YYYY>).
-- record_index now records the exact table a message landed in, so UpdateStatus /
-- DLR correlation locate the row by lookup instead of re-deriving the naming
-- scheme. `ym` is kept for reporting; legacy records_<account_id>_<YYYYMM> rows
-- (rec_table NULL) still resolve via the old scheme.
-- ============================================================================

ALTER TABLE record_index
  ADD COLUMN rec_table VARCHAR(96) NULL AFTER ym;
