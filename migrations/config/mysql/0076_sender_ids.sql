-- RENUMBERED 0062 -> 0076 on the main-salman merge (2026-08-31).
-- This file was written as config/0062_* on a branch taken before main
-- reached 0062; main already had a different migration at that number, so two
-- files would have shared it. A deployment that applied the OLD name needs its
-- schema_migrations row updated to this one, or the migration re-runs and fails.
-- 0062_sender_ids.sql — sender-ID registration workflow (P1 scope finalize
-- 2026-08-09 §13): a user CREATES a sender ID → it is ASSIGNED to an account
-- → MASTER ADMIN (operator) APPROVES it before use. Until now the only
-- mechanism was the free-form sms_service_config.allowed_sender_ids JSON,
-- which the operator edited directly with no lifecycle.
--
-- The engine treats this table as ADDITIVE to the legacy JSON list: the
-- refdata loader unions rows with status='approved' AND a non-NULL
-- account_id into that account's allowed set, so existing accounts keep
-- working and the workflow can be adopted sender-by-sender. The '*'
-- any-sender wildcard stays a JSON-list-only concept (test/unregulated
-- accounts) — it cannot be created through the workflow.
CREATE TABLE IF NOT EXISTS sender_ids (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  sender             VARCHAR(20) NOT NULL,               -- the mask: numeric ≤15 or GSM-7 alpha ≤11
  company_id         BIGINT UNSIGNED NULL,               -- creating/owning tenant (NULL = operator-created)
  account_id         BIGINT UNSIGNED NULL,               -- assignment target; NULL until assigned
  status             ENUM('pending','approved','rejected') NOT NULL DEFAULT 'pending',
  approved_by_user_id VARCHAR(64) NULL,                  -- master admin who ruled
  approved_at        DATETIME NULL,
  rejected_reason    VARCHAR(255) NULL,
  created_by_user_id VARCHAR(64) NULL,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_sender_account (sender, account_id),
  KEY idx_sender_status (status),
  KEY idx_sender_account (account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
