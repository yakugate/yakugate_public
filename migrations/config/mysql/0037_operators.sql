-- Hadal reconciliation, operators group (PO 2026-07-27, Salman): net-new
-- reference table — a real gap, not a reconciliation. We had nothing like
-- this before: hlr_<first4> shards cache PER-MSISDN lookup results, but
-- nothing maps MCC/MNC -> country/operator name, or owns number-range data
-- (from_series/to_series) for offline MNP resolution without a live HLR dip.
--
-- Adopts Hadal's `operators` shape (well-designed on his side — real PK, real
-- indexes, unlike the `prefixes`/`operators_1`/`operators_new`/
-- `test_operators` junk this migration deliberately does NOT adopt), with two
-- renames to avoid colliding with words Yakugate already uses heavily:
--   operator          -> operator_name   (operator is already the P1 request
--                                         field name / splitOperator())
--   redis_shard_key   -> hlr_shard_key   (we use this "first 4 digits" shard
--                                         key for MySQL/Mongo table names too,
--                                         not just Redis)
--
-- redis_shard_key/hlr_shard_key uses literally the same "first 4 digits" key
-- our hlr_<first4> sharding already does — independent validation of that
-- design choice, not a new idea to evaluate.
--
-- Data is NOT backfilled by this migration (a cross-server copy from the
-- live Hadal DB can't run inside a local schema migration) — see the
-- one-time import script this migration's own discussion produced.
--
-- REPLACES the existing `operators` table (migration config/0005), whose
-- (mcc,mnc) composite PK can only hold ONE row per operator and cannot
-- represent Hadal's real data (an operator can own several non-contiguous
-- number-range blocks under the same mcc/mnc). Safe: confirmed zero Go code
-- reads this table today (0005's own comment already says so — the
-- /sms/v4/operator endpoint is still a stub), and it holds only 4 placeholder
-- seed rows. Direct precedent: 0005 itself already dropped-and-replaced an
-- even older operators shape for the identical reason ("no engine code read
-- the legacy shape... the legacy table is dropped, not migrated").
DROP TABLE IF EXISTS operators;

CREATE TABLE operators (
  id             BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  ccc            VARCHAR(10) DEFAULT NULL,
  mcc            VARCHAR(10) DEFAULT NULL,
  mnc            VARCHAR(10) DEFAULT NULL,
  mccmnc         VARCHAR(20) DEFAULT NULL,
  operator_name  VARCHAR(100) DEFAULT NULL,
  country        VARCHAR(100) DEFAULT NULL,
  region         VARCHAR(100) DEFAULT NULL,
  allow_mcc_only TINYINT(1) NOT NULL DEFAULT 0,
  from_series    VARCHAR(20) DEFAULT NULL,
  to_series      VARCHAR(20) DEFAULT NULL,
  hlr_shard_key  VARCHAR(20) DEFAULT NULL,
  created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_operators_range (ccc, mcc, mnc, from_series, to_series),
  KEY idx_operators_mcc (mcc),
  KEY idx_operators_mnc (mnc),
  KEY idx_operators_mccmnc (mccmnc),
  KEY idx_operators_country (country),
  KEY idx_operators_series (from_series, to_series),
  KEY idx_operators_shard (hlr_shard_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
