-- ============================================================================
-- STORE 1: QUEUE / HLR  (hot, high-throughput operational)  — MySQL
-- Holds per-vendor queues, HLR, and the auth-token secrets (fast hot-path auth).
-- Dedup + rate limiting are IN-MEMORY (not persisted). Backends: mysql/mssql/redis.
-- ============================================================================

-- Auth secrets: token -> account. Hot-path lookup on every request; PK is the
-- hash so lookup is O(1). (Kept in this store for performance; Redis-friendly.)
CREATE TABLE api_tokens (
  token_hash   CHAR(64) PRIMARY KEY,                  -- sha-256 of the API key; never the raw key
  account_id   BIGINT UNSIGNED NOT NULL,
  username     VARCHAR(64) NOT NULL,                  -- denormalized for hot auth (no cross-store join)
  issued_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at   DATETIME,
  revoked      TINYINT(1) NOT NULL DEFAULT 0,
  KEY idx_account (account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-vendor pending queue: cloned to queue_<cid> per vendor (PENDING ONLY).
CREATE TABLE queue_template (
  id              BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  msg_id          CHAR(26) NOT NULL,                   -- ULID
  account_id      BIGINT UNSIGNED NOT NULL,
  vendor_id       BIGINT UNSIGNED NOT NULL,
  sender          VARCHAR(32),
  destination     VARCHAR(20) NOT NULL,
  body            TEXT NOT NULL,
  encoding        ENUM('gsm7','ucs2') NOT NULL DEFAULT 'gsm7',
  gsm7            TINYINT NULL,                         -- 1=force / 0=ucs2 / NULL=auto
  segments        INT NOT NULL DEFAULT 1,
  channel         VARCHAR(32) NULL,
  template_id     BIGINT UNSIGNED,
  transport       ENUM('smpp','http') NOT NULL,
  mcc             VARCHAR(3),                           -- resolved country code
  mnc             VARCHAR(3),                           -- resolved network code
  status          ENUM('queued','sent','failed','expired') NOT NULL DEFAULT 'queued',
  priority        TINYINT NOT NULL DEFAULT 5,           -- OTP=1, Txn=5, Default=9
  idempotency_key VARCHAR(80),
  request_id      CHAR(36),                             -- batch id (multi-recipient)
  valid_until     DATETIME,
  attempts        INT NOT NULL DEFAULT 0,               -- retry backoff (attempts/next_attempt_at)
  next_attempt_at DATETIME,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_claim (status, next_attempt_at, id),          -- SKIP LOCKED claim index
  KEY idx_msg (msg_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- HLR / MNP source of truth (also cached hot in memory).
-- HLR is SHARDED by the first 4 digits of the MSISDN: hlr_<first4> (e.g. hlr_9230),
-- each cloned from this template. Lookups compute hlr_ + msisdn[:4].
CREATE TABLE hlr_template (
  msisdn        VARCHAR(20) PRIMARY KEY,
  mcc           VARCHAR(3) NOT NULL,                    -- mobile country code
  mnc           VARCHAR(3) NOT NULL,                    -- mobile network code
  ported        TINYINT(1) NOT NULL DEFAULT 0,          -- MNP-ported
  block_senders VARCHAR(25) NOT NULL DEFAULT '',        -- '*' = blocked for ALL senders; else a sender mask
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_network (mcc, mnc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
