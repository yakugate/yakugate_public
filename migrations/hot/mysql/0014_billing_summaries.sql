-- ============================================================================
-- Hadal reconciliation, summary/rollup group (PO 2026-07-27): the one
-- genuinely NEW subsystem in this whole reconciliation — Yakugate had zero
-- rollup tables before this, so P12 would otherwise aggregate over millions
-- of live mtlog/dlrlog/mologtemplate/rejectedlog rows for every portal chart.
--
-- Adapted, not copied verbatim, for two reasons the original Hadal review
-- itself already found:
--   1. Money as `double` everywhere in his summaries (mt/mo/dlr/rejected/
--      billing/hlr_billing) is exactly the anti-pattern flagged in
--      saleem-hadal-db-review.md §6.4 — DECIMAL here instead.
--   2. `summary_hash` as a secondary UNIQUE KEY with a separate auto-
--      increment PRIMARY KEY is exactly the anti-pattern flagged in §6.7 (his
--      mt_billing_summary: AUTO_INCREMENT=84.3M for 23.6k real rows — every
--      idempotent re-aggregation burns an id). summary_hash is the PRIMARY
--      KEY here, generated+stored so no application code has to compute it.
--
-- Also simplified: dropped the full 3-tier (ratecard/company/base) x 3-way
-- (buying/selling/reselling) x per-currency-conversion-rate matrix — Yakugate
-- hasn't built multi-currency conversion or reseller billing LOGIC yet (only
-- the reseller schema flags from earlier in this reconciliation), so that
-- much unused dimensionality would just be more "columns nothing reads yet"
-- debt. One buy/sell amount + tax + currency per row; extend when the FX/
-- reseller features actually land.
-- ============================================================================

CREATE TABLE mt_billing_summary (
  account_id      BIGINT UNSIGNED NOT NULL,
  company_id      BIGINT UNSIGNED DEFAULT NULL,
  hour            DATETIME NOT NULL,
  sender          VARCHAR(32) DEFAULT NULL,
  mcc             VARCHAR(3) DEFAULT NULL,
  mnc             VARCHAR(3) DEFAULT NULL,
  template_id     BIGINT UNSIGNED DEFAULT NULL,
  route_id        BIGINT UNSIGNED DEFAULT NULL,
  charging_mode   VARCHAR(32) DEFAULT NULL,
  total_sms       INT NOT NULL DEFAULT 0,
  total_segments  INT NOT NULL DEFAULT 0,
  buy_amount      DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  buy_tax         DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  sell_amount     DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  sell_tax        DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  currency        CHAR(3) DEFAULT NULL,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  summary_hash CHAR(32) GENERATED ALWAYS AS
    (MD5(CONCAT_WS('|', account_id, hour, IFNULL(mcc,''), IFNULL(mnc,''), IFNULL(template_id,0), IFNULL(charging_mode,'')))) STORED,
  PRIMARY KEY (summary_hash),
  KEY idx_mt_summary_account_hour (account_id, hour),
  KEY idx_mt_summary_company_hour (company_id, hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE mo_billing_summary (
  account_id      BIGINT UNSIGNED NOT NULL,
  company_id      BIGINT UNSIGNED DEFAULT NULL,
  hour            DATETIME NOT NULL,
  receiver        VARCHAR(32) DEFAULT NULL,
  mcc             VARCHAR(3) DEFAULT NULL,
  mnc             VARCHAR(3) DEFAULT NULL,
  route_id        BIGINT UNSIGNED DEFAULT NULL,
  total_sms       INT NOT NULL DEFAULT 0,
  total_segments  INT NOT NULL DEFAULT 0,
  buy_amount      DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  buy_tax         DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  sell_amount     DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  sell_tax        DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  currency        CHAR(3) DEFAULT NULL,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  summary_hash CHAR(32) GENERATED ALWAYS AS
    (MD5(CONCAT_WS('|', account_id, hour, IFNULL(mcc,''), IFNULL(mnc,'')))) STORED,
  PRIMARY KEY (summary_hash),
  KEY idx_mo_summary_account_hour (account_id, hour),
  KEY idx_mo_summary_company_hour (company_id, hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE dlr_summary (
  account_id      BIGINT UNSIGNED NOT NULL,
  company_id      BIGINT UNSIGNED DEFAULT NULL,
  hour            DATETIME NOT NULL,
  vendor_id       BIGINT UNSIGNED DEFAULT NULL,
  mcc             VARCHAR(3) DEFAULT NULL,
  mnc             VARCHAR(3) DEFAULT NULL,
  delivery_status VARCHAR(12) DEFAULT NULL,
  total_count     INT NOT NULL DEFAULT 0,
  buy_amount      DECIMAL(14,4) NOT NULL DEFAULT 0.0000, -- operator_bills_failure: a failed delivery can still cost
  currency        CHAR(3) DEFAULT NULL,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  summary_hash CHAR(32) GENERATED ALWAYS AS
    (MD5(CONCAT_WS('|', account_id, hour, IFNULL(vendor_id,0), IFNULL(delivery_status,'')))) STORED,
  PRIMARY KEY (summary_hash),
  KEY idx_dlr_summary_account_hour (account_id, hour),
  KEY idx_dlr_summary_company_hour (company_id, hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE rejected_summary (
  account_id      BIGINT UNSIGNED NOT NULL,
  company_id      BIGINT UNSIGNED DEFAULT NULL,
  hour            DATETIME NOT NULL,
  sender          VARCHAR(32) DEFAULT NULL,
  template_id     BIGINT UNSIGNED DEFAULT NULL,
  route_id        BIGINT UNSIGNED DEFAULT NULL,
  status_code     INT DEFAULT NULL,
  reject_reason   VARCHAR(64) DEFAULT NULL,
  total_count     INT NOT NULL DEFAULT 0,
  is_charged      TINYINT(1) DEFAULT NULL,
  charged_amount  DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  currency        CHAR(3) DEFAULT NULL,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  summary_hash CHAR(32) GENERATED ALWAYS AS
    (MD5(CONCAT_WS('|', account_id, hour, IFNULL(status_code,0), IFNULL(reject_reason,'')))) STORED,
  PRIMARY KEY (summary_hash),
  KEY idx_rejected_summary_account_hour (account_id, hour),
  KEY idx_rejected_summary_company_hour (company_id, hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Company-level rollup across MT+MO+DLR — what invoices actually total against.
CREATE TABLE billing_summary (
  company_id      BIGINT UNSIGNED NOT NULL,
  day             DATE NOT NULL,
  transaction_type VARCHAR(16) DEFAULT NULL, -- 'mt' | 'mo' | 'dlr'
  total_count     INT NOT NULL DEFAULT 0,
  tax_amount      DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  total_amount    DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  currency        CHAR(3) DEFAULT NULL,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  summary_hash CHAR(32) GENERATED ALWAYS AS
    (MD5(CONCAT_WS('|', company_id, day, IFNULL(transaction_type,'')))) STORED,
  PRIMARY KEY (summary_hash),
  KEY idx_billing_summary_company_day (company_id, day)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- HLR lookups as a billable product (backlog S-10, still deferred as a
-- feature — table lands now, same "column/table now, feature later" pattern
-- as the rest of this reconciliation). provider_id references the
-- hlr_providers catalog built earlier in this reconciliation (migration
-- config/0030).
CREATE TABLE hlr_billing_summary (
  account_id      BIGINT UNSIGNED NOT NULL,
  company_id      BIGINT UNSIGNED DEFAULT NULL,
  provider_id     BIGINT UNSIGNED DEFAULT NULL,
  hour            DATETIME NOT NULL,
  total_count     INT NOT NULL DEFAULT 0,
  buy_amount      DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  sell_amount     DECIMAL(14,4) NOT NULL DEFAULT 0.0000,
  currency        CHAR(3) DEFAULT NULL,
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  summary_hash CHAR(32) GENERATED ALWAYS AS
    (MD5(CONCAT_WS('|', account_id, hour, IFNULL(provider_id,0)))) STORED,
  PRIMARY KEY (summary_hash),
  KEY idx_hlr_summary_account_hour (account_id, hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
