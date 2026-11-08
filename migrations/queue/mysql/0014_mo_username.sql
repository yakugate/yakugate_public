-- ============================================================================
-- 0014_mo_username.sql — carry the owning client's username on MO rows (2026-07-19)
-- P6/D-032: MO records finalize into the client's daily-per-username hot table
-- (records_<username>_<DD_MM_YYYY>, D-030). The MO route resolves the owning
-- account; its username rides the staging row so the delivery consumer files the
-- record under the right table. Template + shared mo_queue both get the column.
-- ============================================================================

ALTER TABLE mo_template
  ADD COLUMN username VARCHAR(64) NULL AFTER account_id;
