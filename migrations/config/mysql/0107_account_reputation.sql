-- 0107_account_reputation.sql — each account's reputation score (P10
-- register "Sender/account reputation score": ruled 2026-09-09 shown only,
-- never feeds an action; built 2026-09-15, PO "yes, as proposed").
--
-- One row per account, rewritten once a day by the abuse-monitor role from
-- the day's evidence (abuse.Reputation): 0-100 and the reasons behind it.
-- Operators see it on Accounts. NOTHING in the engine reads it to act.
CREATE TABLE IF NOT EXISTS account_reputation (
  account_id   BIGINT       NOT NULL COMMENT 'sms_service_config.id',
  score        TINYINT UNSIGNED NOT NULL COMMENT '0-100; starts at 100 and loses points for evidence',
  reasons      JSON         NOT NULL COMMENT '[{text, points}] — why, in order',
  inputs       JSON         NULL     COMMENT 'the counts it was computed from',
  computed_at  DATETIME     NOT NULL COMMENT 'UTC',
  PRIMARY KEY (account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
