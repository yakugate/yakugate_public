-- 0139_singleton_claims.sql — a gateway-wide role goes back to its home
-- (Saleem, 2026-10-05: billing ran on tw-1, where the register expects it,
-- moved to tw-2 and never came back).
--
-- billing, retention, backup and abuse-monitor each run in ONE deployment at a
-- time, whoever takes the MySQL lock first (internal/singleton). Another
-- deployment taking over when the first goes away is the point; keeping the
-- role for ever after is not. The register says where each is EXPECTED
-- (deployment_roles, is_singleton = 1, one deployment only), and that is its
-- home: when the home finds the role running somewhere else it writes this
-- row, and the holder away from home sees it and hands the role back.
--
-- A claim counts for 3 minutes, and the home asks again each time it looks
-- (the supervisor starts it every minute), so a home that goes away again
-- stops holding the role back by itself. The lock is still the truth: this
-- row only says who is asking.
CREATE TABLE IF NOT EXISTS singleton_claims (
  role          VARCHAR(64)  NOT NULL COMMENT 'billing | retention | backup | abuse-monitor',
  deployment_id VARCHAR(64)  NOT NULL COMMENT 'the home deployment asking for it back',
  host          VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'the asking process''s hostname',
  pid           INT          NOT NULL DEFAULT 0  COMMENT 'the asking process id',
  claimed_at    DATETIME(3)  NOT NULL COMMENT 'UTC: when it last asked',
  PRIMARY KEY (role)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
