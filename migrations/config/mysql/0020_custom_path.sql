-- ============================================================================
-- 0020_custom_path.sql — per-account custom submit URL (2026-07-22)
--
-- PO decisions 2026-07-22:
--   * authored in the SAME place as the dialect config (accounts.api_dialect,
--     top-level "custom_path" field) — one config surface per account
--   * once an account has a custom path, /sms/v3/send is no longer valid FOR
--     THAT ACCOUNT (replaces, not additive) — enforced in internal/dialect and
--     internal/process/httpserver, not at the schema layer
--   * a different account authenticating against someone else's custom path is
--     rejected 404 (ownership mismatch), also enforced in the engine
--
-- custom_send_path is a STORED generated column, not a hand-written one: it
-- lets one JSON blob stay the single authoring surface while the database
-- still enforces global uniqueness (two accounts cannot claim the same path)
-- and gives refdata a plain column to select without parsing JSON per row.
-- MySQL treats every NULL as distinct in a UNIQUE index, so the overwhelming
-- majority of accounts (no custom path configured) never collide.
-- ============================================================================

ALTER TABLE accounts
  ADD COLUMN custom_send_path VARCHAR(190)
    GENERATED ALWAYS AS (NULLIF(JSON_UNQUOTE(JSON_EXTRACT(api_dialect, '$.custom_path')), ''))
    STORED,
  ADD UNIQUE KEY uq_custom_send_path (custom_send_path);
