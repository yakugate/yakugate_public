-- GW-29 (PO 2026-08-28): low-balance alerting, multi-threshold.
--
-- companies.low_balance_alert was a single stored-but-never-consumed value.
-- This table holds ANY number of thresholds per company, each carrying its
-- own alert state: `armed` = will fire on the next downward crossing; the
-- Admin API's poller disarms a row when it alerts and re-arms it once the
-- balance recovers above the threshold (once-per-dip policy, PO ruled).
-- low_balance_email is the dedicated recipient list (comma-separated, like
-- the sibling email fields); empty falls back to billing_email.
CREATE TABLE IF NOT EXISTS balance_alert_thresholds (
  id                 BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  company_id         BIGINT UNSIGNED NOT NULL,
  threshold          DECIMAL(14,4)   NOT NULL,
  is_disabled        TINYINT(1)      NOT NULL DEFAULT 0,
  armed              TINYINT(1)      NOT NULL DEFAULT 1,
  last_alerted_at    DATETIME        NULL,
  last_balance_seen  DECIMAL(14,4)   NULL,
  created_by_user_id VARCHAR(64)     NULL,
  created_at         DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_company_threshold (company_id, threshold),
  KEY idx_company (company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE companies ADD COLUMN low_balance_email VARCHAR(190) NULL AFTER rates_email;

-- Backfill: each company's legacy single threshold becomes its first row.
-- The old column stays for read-compat; the portal now edits the list.
INSERT INTO balance_alert_thresholds (company_id, threshold)
  SELECT id, low_balance_alert FROM companies
  WHERE low_balance_alert IS NOT NULL AND low_balance_alert > 0;
