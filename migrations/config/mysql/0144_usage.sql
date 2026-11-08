-- ============================================================================
-- 0144_usage.sql — usage without a ledger row for each message
-- (Saleem 2026-10-13: "we don't need to record per message"; balance_history
-- holds top-ups, adjustments and refunds only)
--
-- The billing role charges in batches. Each batch writes, in the SAME
-- transaction as the balance it moves:
--
--   usage_batches   one row for the batch and the company. It is the guard
--                   against charging twice: a batch that is tried again (the
--                   role stopped between charging and marking the records)
--                   finds its row and changes nothing. Rows older than 90
--                   days are removed by the retention role.
--   usage_daily     what each account used that day, by kind and by how the
--                   company paid. Invoices and the billing reports read it:
--                   it takes the place of adding the per-message rows up.
--
--   day      UTC day the batch was charged on (as a ledger row's created_at
--            was): what is billed in a period is what was charged in it.
--   kind     mt | mo | dlr | hlr
--   model    prepaid (taken from the balance) | postpaid (to be invoiced)
--   items    how many messages, receipts or lookups
--   amount   in the company's currency, positive = charged
-- ============================================================================
CREATE TABLE IF NOT EXISTS usage_batches (
  batch_id   CHAR(26)  NOT NULL,
  company_id BIGINT    NOT NULL,
  created_at DATETIME  NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (batch_id, company_id),
  KEY idx_usage_batches_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS usage_daily (
  day        DATE          NOT NULL,
  company_id BIGINT        NOT NULL,
  account_id BIGINT        NOT NULL,
  kind       VARCHAR(8)    NOT NULL,
  model      VARCHAR(8)    NOT NULL,
  items      BIGINT        NOT NULL DEFAULT 0,
  amount     DECIMAL(18,4) NOT NULL DEFAULT 0,
  updated_at DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (day, company_id, account_id, kind, model),
  KEY idx_usage_daily_company (company_id, day)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
