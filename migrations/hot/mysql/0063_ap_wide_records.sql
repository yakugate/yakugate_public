-- ============================================================================
-- 0063_ap_wide_records.sql — ap1/ap2/apg1/apg2 hold 255 characters on the
-- record tables by company and month (D-136), as hot/0060_ap_wide made them
-- on the old mtlog family (QA templates API-013, 2026-10-14)
--
-- tpl_mt (hot/0062) took the old mtlog widths, VARCHAR(50). The queue row
-- carries 255 characters (queue/0039_ap_wide), so a longer value reached the
-- record and its write failed. Existing mt_<company>_<YYYYMM> tables are
-- widened at start by bootstrap.widenCloneColumns; a table still 50 wide
-- when a long value arrives is widened by the write itself (apwide.go,
-- writeBatch). One column per statement.
-- ============================================================================
ALTER TABLE tpl_mt MODIFY ap1 VARCHAR(255) NULL;
ALTER TABLE tpl_mt MODIFY ap2 VARCHAR(255) NULL;
ALTER TABLE tpl_mt MODIFY apg1 VARCHAR(255) NULL;
ALTER TABLE tpl_mt MODIFY apg2 VARCHAR(255) NULL;
