-- 0106_abuse_rule_overrides.sql — one customer's own abuse thresholds
-- (PO 2026-09-15: "per account + Abuse Rules screen").
--
-- abuse_rules holds ONE row per signal for the whole gateway. The weighted
-- score ruling of 2026-09-09 assumed per-account weights — "how a trusted
-- customer gets more rope" — which never existed for any signal. An override
-- row here changes a signal for one account:
--
--   params       keys merged over the gateway row's params for this account
--                only (a bank's OTP traffic may spike 20x, a new reseller
--                warns early). Thresholds, weights, actions and cooldown may
--                be overridden; the time windows and sample sizes may not
--                (window_min, baseline_hours, scan_limit, sample_msgs,
--                prefix_len): the monitor measures each signal over one
--                window for every account in one pass. The Admin API refuses
--                them and the engine ignores them.
--   is_disabled  1 = this signal does not score this account at all.
--
-- No row = the gateway default, exactly as before.
CREATE TABLE IF NOT EXISTS abuse_rule_overrides (
  id           INT UNSIGNED NOT NULL AUTO_INCREMENT,
  signal_name  VARCHAR(32)  NOT NULL COMMENT 'abuse_rules.signal_name it overrides',
  account_id   BIGINT       NOT NULL COMMENT 'sms_service_config.id',
  params       JSON         NULL     COMMENT 'keys merged over the gateway params for this account; NULL = none',
  is_disabled  TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '1 = this signal does not score this account',
  note         VARCHAR(190) NULL     COMMENT 'why, for the next operator',
  created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_abuse_override (signal_name, account_id),
  KEY idx_abuse_override_account (account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
