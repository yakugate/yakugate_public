-- ============================================================================
-- 0052_account_default_ttl.sql — account-level TTL default (TC-P7-09, 2026-07-30)
-- D-026 named three layers: request-supplied validity -> account default ->
-- global default (YAKU_DEFAULT_TTL_SEC). Only the outer two existed; this adds
-- the missing middle layer. Same precedence pattern as accounts.dedupe_ttl_sec
-- -> YAKU_DEDUPE_TTL_SEC (migration config/0027, D-069) — NULL = no override,
-- fall through to the process-wide default.
-- ============================================================================

ALTER TABLE sms_service_config
  ADD COLUMN default_ttl_sec INT UNSIGNED DEFAULT NULL AFTER dedupe_ttl_sec;
