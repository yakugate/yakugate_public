-- ============================================================================
-- 0021_account_dialects.sql — dialect/URL/auth moves to its own table (2026-07-23)
--
-- PO decision 2026-07-23: no account should rely on an IMPLICIT "no dialect
-- row = native" state — every account gets an EXPLICIT row here from the
-- moment it exists. Two things make that true:
--   1. this migration backfills a row for every account that exists today;
--   2. trg_accounts_after_insert (below) inserts a row for every account
--      created from now on — fires no matter WHAT inserts the accounts row
--      (the Admin API's generic CRUD factory, a future bulk import, a manual
--      SQL script), which app-level provisioning code cannot guarantee.
--
-- /sms/v3/send remains a real, SHARED value — not an implicit fallback. Many
-- accounts' rows legitimately hold it at once. A genuinely custom submit path
-- stays exclusive to one account: url_exclusive is NULL whenever url equals
-- the shared default (so those rows never collide with each other in the
-- UNIQUE key), and holds the real value otherwise — the same technique
-- 0020_custom_path.sql used for the column this table now replaces.
--
-- Retires accounts.api_dialect / accounts.api_password / the generated
-- accounts.custom_send_path column from 0019/0020 — this table replaces all
-- three. Confirmed via full-codebase grep (2026-07-23) that nothing else,
-- Go or the Node Admin API, references those three columns.
-- ============================================================================

CREATE TABLE account_dialects (
  id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id    BIGINT UNSIGNED NOT NULL,
  url           VARCHAR(190) NOT NULL DEFAULT '/sms/v3/send',
  url_exclusive VARCHAR(190) GENERATED ALWAYS AS (NULLIF(url, '/sms/v3/send')) STORED,
  api_password  VARCHAR(100) NULL,        -- bcrypt; NULL = password auth disabled for this account
  request_map   JSON NULL,                -- dialect.Config.Request
  response_map  JSON NULL,                -- dialect.Config.Response
  keep_unmapped TINYINT(1) NULL,          -- dialect.Config.KeepUnmapped; NULL = default true
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_account (account_id),
  UNIQUE KEY uq_url_exclusive (url_exclusive),
  CONSTRAINT fk_account_dialects_account FOREIGN KEY (account_id)
    REFERENCES accounts (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Backfill: every existing account gets an explicit row, carrying forward
-- whatever it already had (custom path / password / field mapping) from the
-- JSON column, defaulting to the shared native path where it had nothing.
INSERT INTO account_dialects (account_id, url, api_password, request_map, response_map, keep_unmapped)
SELECT
  a.id,
  COALESCE(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(a.api_dialect, '$.custom_path')), ''), '/sms/v3/send'),
  a.api_password,
  JSON_EXTRACT(a.api_dialect, '$.request'),
  JSON_EXTRACT(a.api_dialect, '$.response'),
  CASE JSON_UNQUOTE(JSON_EXTRACT(a.api_dialect, '$.keep_unmapped'))
    WHEN 'true' THEN 1
    WHEN 'false' THEN 0
    ELSE NULL
  END
FROM accounts a
WHERE NOT EXISTS (SELECT 1 FROM account_dialects d WHERE d.account_id = a.id);

-- Auto-provision every future account — the actual guarantee behind
-- "automatically added when we add a user." Requires the migration
-- connection's MultiStatements=true (already set in bootstrap.go for exactly
-- this reason); verified live 2026-07-23 that a BEGIN/END trigger body parses
-- correctly through the same db.ExecContext(whole-file-as-one-query) path
-- every other migration already uses — no DELIMITER directive needed (that is
-- a mysql-CLI-only convention, not real SQL; the server's own parser handles
-- the nested semicolon natively once MultiStatements allows the batch at all).
CREATE TRIGGER trg_accounts_after_insert
AFTER INSERT ON accounts
FOR EACH ROW
BEGIN
  INSERT INTO account_dialects (account_id, url) VALUES (NEW.id, '/sms/v3/send');
END;

ALTER TABLE accounts
  DROP COLUMN custom_send_path,
  DROP COLUMN api_dialect,
  DROP COLUMN api_password;
