-- PO 2026-07-27: durable, cross-instance dedupe backend for deployments that
-- don't run Redis. A duplicate lock here uses an explicit expires_at (an
-- already-expired row is reclaimed in place by dedupe.MySQL.Check rather than
-- requiring a sweep for correctness) instead of Redis's native key EXPIRE.
CREATE TABLE dedupe_keys (
  dkey        VARCHAR(191) NOT NULL PRIMARY KEY,
  fingerprint VARCHAR(64) NOT NULL,
  msg_id      CHAR(26) NOT NULL,
  expires_at  DATETIME NOT NULL,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_expires_at (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
