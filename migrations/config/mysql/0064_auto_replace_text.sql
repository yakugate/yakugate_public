-- GW-04 (PO approved 2026-08-27, option 1): auto_replace_text MOVES to the
-- Config store. It is configuration, not queue data — and the P12 invariant
-- ("the Admin API never touches the Queue store") meant it was unmanageable
-- from the portal while it lived there. The engine's loader now reads THIS
-- table; the queue-store copy is retired (left in place, no longer read —
-- queue migrations are additions-only, so it is not dropped there).
--
-- Existing deployments carry their rows over once, by hand or script:
--   INSERT INTO yakugate_config.auto_replace_text
--     (account_id, replace_regex, seq, is_disabled, created_by_user_id)
--   SELECT account_id, replace_regex, seq, is_disabled, created_by_user_id
--     FROM yakugate_queue.auto_replace_text;
-- (Not done in-migration: a fresh install has no queue DB to read from, and
-- cross-schema names are deployment-specific.)
CREATE TABLE IF NOT EXISTS auto_replace_text (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  account_id         BIGINT UNSIGNED NOT NULL,
  replace_regex      JSON NOT NULL,
  seq                INT NOT NULL,
  is_disabled        TINYINT(1) NOT NULL DEFAULT 0,
  created_by_user_id VARCHAR(64) NULL,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_account_seq (account_id, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
