-- Process 13 — drop `max_files` from log_config (PO 2026-09-04:
-- "there should not be any max files per day, only per file size and
-- retention days").
--
-- 0081 shipped four settings; this leaves three. The cap was only ever
-- enforceable by deleting the EARLIEST parts of a day that is still being
-- written — throwing away this morning's records while the day is in
-- progress, which is the opposite of what a log is for. Disk use is bounded
-- by retention_days instead, so it stays proportional to real traffic rather
-- than to an arbitrary per-day ceiling.
--
-- A log file that passes max_size_mb still continues in a new part
-- (account_demo_04_09_2026.1.log and so on); there is simply no limit on how
-- many parts one day may have. Whole days are removed once past retention.
--
-- Applied migrations are immutable, so 0081 keeps its four-key default and
-- this migration narrows it.

-- 1. New shape of the column DEFAULT, for rows created from here on.
ALTER TABLE sms_service_config
  MODIFY COLUMN log_config JSON NOT NULL
    DEFAULT (JSON_OBJECT('level', 'info', 'max_size_mb', 100, 'retention_days', 30))
    COMMENT 'Process 13: level + max file size + retention for this account''s log files';

ALTER TABLE vendors
  MODIFY COLUMN log_config JSON NOT NULL
    DEFAULT (JSON_OBJECT('level', 'info', 'max_size_mb', 100, 'retention_days', 30))
    COMMENT 'Process 13: level + max file size + retention for this vendor''s log files';

-- 2. Remove the key from every row that 0081 wrote it into. A row an operator
--    has since edited keeps its own level, size and retention untouched.
UPDATE sms_service_config SET log_config = JSON_REMOVE(log_config, '$.max_files')
 WHERE JSON_CONTAINS_PATH(log_config, 'one', '$.max_files');

UPDATE vendors SET log_config = JSON_REMOVE(log_config, '$.max_files')
 WHERE JSON_CONTAINS_PATH(log_config, 'one', '$.max_files');
