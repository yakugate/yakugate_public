-- Process 13 — ONE place for every logging setting (PO 2026-09-04:
-- "do one thing, save all log info into log config, log_level as well now").
--
-- This supersedes the split this morning's 0080 set up. The level was going
-- to be its own column on both tables; the PO's later ruling is that level,
-- rotation size, file count and retention all live together in one JSON
-- object, so there is exactly one thing to read, write and show on a form.
--
-- Applied migrations are immutable, so 0080 stays in the file list and its
-- column is dropped here instead of being edited away. Nothing is lost: that
-- column was added the same day and no vendor ever carried a value in it.
--
-- Shape of log_config, on BOTH tables:
--
--   {
--     "level":          "info",   -- debug | info | warn | error | none
--     "max_size_mb":    100,      -- start a new part when today's file passes this
--     "max_files":      10,       -- most parts kept for any one day
--     "retention_days": 30        -- delete files older than this
--   }
--
-- The PO's ruling "should be forced on creation of vendor/account" is why
-- these are NOT NULL with a real DEFAULT: a new account or vendor can never
-- exist without a complete logging config, whatever inserts the row — the
-- Admin API, a bulk import, or a hand-written SQL script. Same reasoning as
-- trg_accounts_after_insert, which guarantees every account has a dialect row
-- no matter who created it.

-- 1. Accounts gain the column (vendors have had one since 0006).
ALTER TABLE sms_service_config
  ADD COLUMN log_config JSON NOT NULL
    DEFAULT (JSON_OBJECT('level', 'info', 'max_size_mb', 100, 'max_files', 10, 'retention_days', 30))
    COMMENT 'Process 13: level + rotation + retention for this account''s log files'
    AFTER log_level;

-- 2. Carry every existing account's level across before the old column goes.
--    Levels are stored lower case from here on; the live data holds 'NONE'.
UPDATE sms_service_config
   SET log_config = JSON_SET(log_config, '$.level', LOWER(log_level))
 WHERE log_level IS NOT NULL AND log_level <> '';

-- 3. The old column has no readers left.
ALTER TABLE sms_service_config DROP COLUMN log_level;

-- 4. Vendors: same defaults, and every existing row filled in. Their
--    log_config has been NOT NULL since 0006 but every row holds '{}', so
--    JSON_MERGE_PATCH fills the missing keys without disturbing anything an
--    operator may already have put there.
ALTER TABLE vendors
  MODIFY COLUMN log_config JSON NOT NULL
    DEFAULT (JSON_OBJECT('level', 'info', 'max_size_mb', 100, 'max_files', 10, 'retention_days', 30))
    COMMENT 'Process 13: level + rotation + retention for this vendor''s log files';

UPDATE vendors
   SET log_config = JSON_MERGE_PATCH(
         JSON_OBJECT('level', 'info', 'max_size_mb', 100, 'max_files', 10, 'retention_days', 30),
         COALESCE(log_config, JSON_OBJECT()));

-- 5. Fold 0080's level into the JSON for any vendor that had one, then drop it.
UPDATE vendors
   SET log_config = JSON_SET(log_config, '$.level', LOWER(log_level))
 WHERE log_level IS NOT NULL AND log_level <> '';

ALTER TABLE vendors DROP COLUMN log_level;
