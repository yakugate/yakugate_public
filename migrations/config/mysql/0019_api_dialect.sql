-- ============================================================================
-- 0019_api_dialect.sql — per-account API dialect + password auth (2026-07-22)
--
-- PO decisions 2026-07-22 (see process-1-dialect-design.md):
--   * accept username/password alongside JWT, on BOTH GET and POST
--   * password stored ONE-WAY (bcrypt) — deliberately NOT the reference's
--     reversible AES, so a database or backup leak cannot yield plaintext
--     customer API passwords. The portal can reset a password, never show it.
--   * per-account request/response field mapping, status-code override,
--     per-reason-code error overrides, and json|text|xml response formats
--
-- api_dialect NULL = the native V3 contract with zero overhead, so every
-- existing account is unaffected.
-- ============================================================================

ALTER TABLE accounts
  ADD COLUMN api_password VARCHAR(100) NULL AFTER password_hash,
  ADD COLUMN api_dialect  JSON         NULL AFTER jwt;
