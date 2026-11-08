-- ============================================================================
-- 0068_rejects_lookup_receipt.sql — a refusal's row says how its number's
-- network was found and whether its client asked for a receipt (the harness
-- pass of 27 Oct 2026 on the day tables, issues 14 and 25).
--
-- A message refused at submission is counted in summary_lookup by how its
-- network was found, and may have had its receipt fee booked: neither could
-- be seen on its row, so a reconcile could not explain either. The columns
-- are those of mt_ (tpl_day_mt). Existing rejects_<company>_<day> tables get
-- them at start (bootstrap syncCloneColumns). One column per ALTER.
-- ============================================================================
ALTER TABLE tpl_day_rejects ADD COLUMN hlr_source VARCHAR(8) NOT NULL DEFAULT '';
ALTER TABLE tpl_day_rejects ADD COLUMN no_receipt TINYINT(1) DEFAULT NULL;
ALTER TABLE tpl_day_rejects ADD COLUMN failure_receipt_only TINYINT(1) DEFAULT NULL;
