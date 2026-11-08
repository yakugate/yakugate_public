-- ============================================================================
-- 0017_companies.sql — three-level tenant hierarchy (2026-07-22)
--
-- PO decision 2026-07-22 (after reviewing the Hadal reference DB): adopt the
-- company -> user -> API-client hierarchy instead of the flat accounts model.
--
--   companies : the TENANT. Owns the money (balance, currency) and the
--               contractual ceilings. One company, many accounts.
--   accounts  : the API CLIENT identity the engine authenticates (unchanged
--               role). Gains company_id.
--   admin_users : the PEOPLE who log into the portal (already exists, owned by
--               the Node Admin API). Gains company_id there, not here.
--
-- Balance moves UP to the company so several API clients (prod/test, or a
-- bank's per-branch senders) draw on one contractual balance. accounts.balance
-- is kept as a legacy fallback for rows with no company_id so the change is
-- non-breaking; the engine prefers the company balance when linked.
-- ============================================================================

CREATE TABLE companies (
  id                BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  company_id        CHAR(36)     NOT NULL UNIQUE,          -- external/stable id
  name              VARCHAR(128) NOT NULL,
  type              VARCHAR(32)  NULL,                     -- direct | reseller | internal
  currency          CHAR(3)      NOT NULL DEFAULT 'USD',
  prepaid_billing   TINYINT(1)   NOT NULL DEFAULT 0,
  balance           DECIMAL(14,4) NOT NULL DEFAULT 0,
  low_balance_alert DECIMAL(14,4) NULL,                    -- notify below this
  is_disabled       TINYINT(1)   NOT NULL DEFAULT 0,       -- suspends every account under it
  max_throughput    INT          NOT NULL DEFAULT 0,       -- 0 = unlimited (contract ceiling)
  timezone          VARCHAR(64)  NOT NULL DEFAULT 'UTC',
  billing_email     VARCHAR(190) NULL,
  support_email     VARCHAR(190) NULL,
  address           VARCHAR(255) NULL,
  created_by_user_id VARCHAR(64) NULL,
  created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_disabled (is_disabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE accounts
  ADD COLUMN company_id BIGINT UNSIGNED NULL AFTER customer_id,
  ADD KEY idx_company (company_id);

-- Backfill: every existing account becomes its own single-account company, so
-- behaviour is identical on day one and the hierarchy can be reorganised later
-- from the portal. UUID via the account's existing customer_id keeps it stable.
INSERT INTO companies (company_id, name, currency, prepaid_billing, balance, is_disabled, max_throughput, timezone)
SELECT a.customer_id,
       COALESCE(NULLIF(a.name,''), a.username),
       'USD',
       a.prepaid_billing,
       a.balance,
       a.is_disabled,
       COALESCE(a.throughput,0),
       COALESCE(NULLIF(a.timezone,''),'UTC')
FROM accounts a
WHERE NOT EXISTS (SELECT 1 FROM companies c WHERE c.company_id = a.customer_id);

UPDATE accounts a
  JOIN companies c ON c.company_id = a.customer_id
   SET a.company_id = c.id
 WHERE a.company_id IS NULL;

-- The ledger settles at company level from now on. account_id is retained so an
-- entry still records which API client caused the charge.
ALTER TABLE balance_history
  ADD COLUMN company_id BIGINT UNSIGNED NULL AFTER account_id,
  ADD KEY idx_company (company_id, created_at);

UPDATE balance_history bh
  JOIN accounts a ON a.id = bh.account_id
   SET bh.company_id = a.company_id
 WHERE bh.company_id IS NULL;
