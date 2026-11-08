-- ============================================================================
-- 0040_hlr_source.sql — a queued message carries how its network was found
-- (D-136, found by the first run without Redis, 2026-10-14)
--
-- The accept pipeline stamps hlr_source (db, dip, dip_miss, miss, range, …)
-- and hlr_ported on the message; the record of the message keeps them, the
-- HLR log (hlr_<company>_<YYYYMM>) is written from them and summary_lookup
-- counts them. A Redis queue carries the whole message; a MySQL queue row had
-- no place for the two, so with queues in MySQL every record said "" and the
-- HLR summary and log stayed empty. NULL = a row from before this column.
-- One column per ALTER (the bootstrap rule).
-- ============================================================================
ALTER TABLE queue_template ADD COLUMN hlr_source VARCHAR(8) NULL;
ALTER TABLE queue_template ADD COLUMN hlr_ported TINYINT(1) NULL;
