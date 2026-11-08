-- 0022_vendor_sessions.sql — shared vendor session/token store (PO 2026-08-10:
-- "this session token will be used in any process that is using that vendor /
-- id. so we need to store it in mysql as well").
--
-- One row per vendor: the auth-extracted variables (session id, bearer token,
-- ...) a Telenor-style login/token flow produced, shared by EVERY process and
-- instance driving that vendor — replacing the old per-adapter in-memory cache
-- that was also thrown away on every 30s vendor reload. expires_at is written
-- and compared in Go (driver UTC round-trip) — deliberately never against SQL
-- NOW()/UTC_TIMESTAMP(), per the 2026-08-10 next_attempt_at clock-mixing fix.
--
-- Lives in the QUEUE store: runtime state beside the queues its senders drain,
-- present as a MySQL handle in every egress process even under the Redis gate.
CREATE TABLE IF NOT EXISTS vendor_sessions (
  vendor_id  BIGINT       NOT NULL PRIMARY KEY,
  cid        VARCHAR(64)  NOT NULL,
  vars       JSON         NOT NULL,
  expires_at DATETIME(3)  NOT NULL,
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
