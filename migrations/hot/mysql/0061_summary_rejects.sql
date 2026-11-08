-- ============================================================================
-- 0061_summary_rejects.sql — a summary of the refusals (Saleem 2026-10-13:
-- "we need summaries of each, mt, mo, dlr, hlr, rejected and all dashboards be
-- generated from them")
--
-- Outgoing messages and their delivery outcome are in summary_mt, incoming in
-- summary_mo, number lookups in summary_lookup. Refusals had none: every
-- dashboard and the Rejected screen's counts read rejects_<month> itself
-- (6.7 million rows on the dev box).
--
-- summary_rejects: refused messages per 15 minutes (UTC, by rejected_at), per
-- account (0 = refused before the account was known), per door and per reason.
--
-- It is kept by ADDING, not by rebuilding: a refusal is written once and never
-- changed, so the billing role reads only the rows that came since its last
-- look — by the table's own id — and adds them. Where it has read up to is
-- kept in summary_cursor, one row a rejects month ("rej:rejects_202610"), and
-- is moved in the same transaction as the counts: a row is counted once.
--   pos     the last id counted
--   at_end  when the count last reached the end of the table (NULL = it has
--           never: the month is still being counted from its start, and a
--           reader must count the refusals themselves meanwhile)
-- ============================================================================
CREATE TABLE IF NOT EXISTS summary_rejects (
  bucket_start   DATETIME        NOT NULL,
  account_id     BIGINT UNSIGNED NOT NULL DEFAULT 0,
  channel        VARCHAR(8)      NOT NULL DEFAULT '',
  reason_code    VARCHAR(191)    NOT NULL DEFAULT '',
  rejected_msgs  INT UNSIGNED    NOT NULL DEFAULT 0,
  rejected_parts INT UNSIGNED    NOT NULL DEFAULT 0,
  updated_at     DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq (bucket_start, account_id, channel, reason_code),
  KEY idx_account (account_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS summary_cursor (
  name       VARCHAR(64)     NOT NULL PRIMARY KEY,
  pos        BIGINT UNSIGNED NOT NULL DEFAULT 0,
  at_end     DATETIME        NULL,
  updated_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
