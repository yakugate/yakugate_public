-- 0135_singleton_holders.sql — who runs each gateway-wide role (Saleem,
-- 2026-10-01: "don't start, auto takeover").
--
-- billing, retention, backup and abuse-monitor run in ONE deployment at a
-- time: each holds a MySQL advisory lock (GET_LOCK 'yaku_role_<role>',
-- internal/singleton). The lock says THAT somebody holds it, not who. A role
-- writes this row when it takes the lock, so a deployment refused it can say
-- where the role runs — the console's Processes screen shows "runs on lhr-1".
--
-- The lock is the truth, never this row: a row is believed only while its
-- conn_id is the session IS_USED_LOCK names. A holder that died leaves its row
-- behind with a conn_id nobody holds, which reads as "another deployment"
-- until the next holder overwrites it.
CREATE TABLE IF NOT EXISTS singleton_holders (
  role          VARCHAR(64)     NOT NULL COMMENT 'billing | retention | backup | abuse-monitor',
  deployment_id VARCHAR(64)     NOT NULL DEFAULT '' COMMENT 'the holder''s YAKU_DEPLOYMENT_ID (its hostname when unset)',
  host          VARCHAR(255)    NOT NULL DEFAULT '' COMMENT 'the holder''s hostname',
  pid           INT             NOT NULL DEFAULT 0  COMMENT 'the holder''s process id on that host',
  conn_id       BIGINT UNSIGNED NOT NULL DEFAULT 0  COMMENT 'CONNECTION_ID() of the session holding the lock',
  since         DATETIME        NOT NULL COMMENT 'UTC: when it took the lock',
  updated_at    TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (role)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
