-- The HLR store (Saleem 2026-10-02, D-124): the gateway's data that is not a
-- queue — the HLR shards, HTTP vendor login sessions and JWT revocations —
-- in its own database, YAKU_HLR_DSN, so YAKU_QUEUE_DSN is about queues only.
--
-- These tables lived in the queue database from the start (queue/0002, 0005,
-- 0022 …) and still do on every install that has not set YAKU_HLR_DSN: this
-- set runs only on a SEPARATE HLR database, and creates the tables in the
-- shapes they have reached there. The gateway then copies what the queue
-- database held into them, once, in the background (bootstrap/hlrcopy.go);
-- the old tables are left where they are.

-- hlr_template: the shape every hlr_<first4> shard is cloned from (LIKE), by
-- the Admin API's import and by the engine when it keeps a live lookup.
CREATE TABLE IF NOT EXISTS hlr_template (
  msisdn      VARCHAR(15) NOT NULL,
  mcc         VARCHAR(3)  NOT NULL,
  mnc         VARCHAR(3)  NOT NULL,
  ported      TINYINT(1)  NOT NULL DEFAULT 0,
  last_check  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  is_deleted  TINYINT(1)  NOT NULL DEFAULT 0,
  PRIMARY KEY (msisdn),
  KEY idx_network (mcc, mnc),
  KEY idx_last_check (last_check)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- vendor_sessions: one shared login session per HTTP vendor (D-103), reused
-- by every gateway process; expiry compared in Go, never with SQL NOW().
CREATE TABLE IF NOT EXISTS vendor_sessions (
  vendor_id   BIGINT       NOT NULL,
  cid         VARCHAR(64)  NOT NULL,
  vars        JSON         NOT NULL,
  expires_at  DATETIME(3)  NOT NULL,
  updated_at  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (vendor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- jwt_revocations: JWT ids refused before their expiry.
CREATE TABLE IF NOT EXISTS jwt_revocations (
  jti         VARCHAR(128)    NOT NULL,
  account_id  BIGINT UNSIGNED DEFAULT NULL,
  expires_at  DATETIME        NOT NULL,
  revoked_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  reason      VARCHAR(64)     DEFAULT NULL,
  PRIMARY KEY (jti),
  KEY idx_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
