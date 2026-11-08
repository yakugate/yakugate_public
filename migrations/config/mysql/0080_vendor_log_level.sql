-- Process 13 — per-vendor log level (PO 2026-09-04).
--
-- The PO's ruling: "each vendor and account have separate log levels and so
-- the system — debug, info, error, warn and none". Accounts already had a
-- place for this: `sms_service_config.log_level` (config/0031), which landed
-- schema-only during the Hadal reconciliation and had no reader until now.
-- Vendors had nothing equivalent, so this adds the matching column under the
-- SAME name, letting one piece of Go read both sides.
--
-- Deliberately a column, not a key inside the existing `vendors.log_config`
-- JSON (PO's choice, 2026-09-04): the level is the one setting the portal
-- needs to list and filter vendors by, and SQL cannot index a JSON key as
-- cheaply. `log_config` stays where it is and still holds the rotation, size
-- and retention settings when those land.
--
-- NULL means "no level of its own" and falls back to the process-wide
-- YAKU_LOG_LEVEL (default info) — the same precedence shape as
-- sms_service_config.dedupe_ttl_sec -> YAKU_DEDUPE_TTL_SEC (D-069), reused
-- rather than reinvented. Every existing vendor is therefore unchanged in
-- behaviour by this migration.
ALTER TABLE vendors
  ADD COLUMN log_level VARCHAR(32) DEFAULT NULL
    COMMENT 'Process 13 log level for this vendor: debug|info|warn|error|none. NULL = follow YAKU_LOG_LEVEL'
    AFTER log_config;
