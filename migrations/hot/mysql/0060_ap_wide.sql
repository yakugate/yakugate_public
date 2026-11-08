-- ============================================================================
-- 0060_ap_wide.sql — ap1/ap2/apg1/apg2 hold 255 characters on the MT record
-- (QA templates API-013, 2026-10-14)
--
-- The twin of queue/0039: the queue row carries 255 characters now, and the
-- record written from it at the end must too (hot/0013 made them
-- VARCHAR(50)).
--
-- ALTERs the template only. Existing mtlog_<user>_<date> clones are widened
-- at start by bootstrap.widenCloneColumns, and one still 50 wide when a long
-- value arrives is widened by the store on that write (internal/store/mysql
-- apwide.go). records_template (the legacy family, written by nothing now) is
-- left as it is. One column per statement.
-- ============================================================================
ALTER TABLE mtlog_template MODIFY ap1 VARCHAR(255) NULL;
ALTER TABLE mtlog_template MODIFY ap2 VARCHAR(255) NULL;
ALTER TABLE mtlog_template MODIFY apg1 VARCHAR(255) NULL;
ALTER TABLE mtlog_template MODIFY apg2 VARCHAR(255) NULL;
