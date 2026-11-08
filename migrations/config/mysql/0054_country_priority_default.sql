-- 0054_country_priority_default.sql — country-rule priority + per-account
-- default rule (P1 scope finalize 2026-08-09).
--
-- Two additive changes to quota_by_countries:
--   1. `priority` — lower is evaluated first. The pipeline walks an account's
--      own rules in priority order (longest-prefix tiebreak within a priority),
--      first match wins; only if none match are the global rules consulted.
--   2. destination_country_code becomes NULLABLE — a row with a NULL prefix is
--      the account's DEFAULT rule (catch-all): mode=allow => allow-all,
--      mode=deny => deny-all. This expresses blocklist OR allowlist behaviour
--      as data, with no separate mode flag. One default row per account is the
--      intent (enforced in the app; MySQL treats NULLs as distinct in the
--      existing UNIQUE(account_id, destination_country_code), so the loader's
--      last-write-wins is the practical guard).
ALTER TABLE quota_by_countries
  ADD COLUMN priority INT NOT NULL DEFAULT 0 AFTER mode;

ALTER TABLE quota_by_countries
  MODIFY COLUMN destination_country_code VARCHAR(6) NULL;
