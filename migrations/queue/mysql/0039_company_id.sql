-- ============================================================================
-- 0039_company_id.sql — a queued message carries its company (Saleem
-- 2026-10-14, D-136: records are filed by company and month)
--
-- The record of a message is written to mt_<company>_<YYYYMM> by whichever
-- role sends it, often a gateway that never saw the account. The accept
-- pipeline therefore puts the account's company on the message (as it puts
-- its price, 0038), and the queue keeps it. NULL = a row from before this
-- column: the writer then files the record under company 0.
-- ============================================================================
ALTER TABLE queue_template ADD COLUMN company_id BIGINT UNSIGNED NULL;
ALTER TABLE mo_queue ADD COLUMN company_id BIGINT UNSIGNED NULL;
