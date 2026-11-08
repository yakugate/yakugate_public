-- BUG-1 fix (2026-08-09): restore next_attempt_at to NULLABLE on the queue.
--
-- Schema v2 (0004_schema_v2.sql) redefined next_attempt_at as DATETIME NOT NULL,
-- but the store INSERTs NULL for every immediate-claim message (mysql.go
-- Enqueue -> nzTime(zero) -> NULL) and Claim's gate "next_attempt_at IS NULL OR
-- <= NOW()" DEPENDS on NULL meaning "claimable now". The NOT NULL therefore made
-- EVERY MySQL-backend enqueue fail with 1048/RMSGQFUL -> the whole MySQL queue
-- backend (the Phase-1 queue) could not accept traffic. MSSQL never had this
-- regression (its 0001 keeps next_attempt_at NULL), so this is MySQL-only.
--
-- Mirrors 0018_action_guards.sql: fix the template (for future vendor tables)
-- and every existing per-vendor queue table.
ALTER TABLE queue_template
  MODIFY next_attempt_at DATETIME NULL;

ALTER TABLE queue_testvendor
  MODIFY next_attempt_at DATETIME NULL;
