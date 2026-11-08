-- BUG fix (2026-08-09): add 'latin1' to the hot-store encoding enum.
--
-- Records / traffic-log templates carried encoding enum('gsm7','ucs2'); a
-- Latin-1 message (HTTP encoding="latin1" or SMPP data_coding=3) would fail its
-- MDR / traffic-log write. Widen the templates so every table cloned from here
-- accepts latin1. (Already-materialized dated mtlog_*/records_* tables inherit
-- the fix at the next month's clone; widen out-of-band if a live latin1 write is
-- needed against an existing month.) Companion to queue/0021_encoding_latin1.sql.
ALTER TABLE records_template MODIFY encoding ENUM('gsm7','ucs2','latin1') NULL;
ALTER TABLE mtlog_template MODIFY encoding ENUM('gsm7','ucs2','latin1') NULL;
ALTER TABLE mologtemplate MODIFY encoding ENUM('gsm7','ucs2','latin1') NULL;
