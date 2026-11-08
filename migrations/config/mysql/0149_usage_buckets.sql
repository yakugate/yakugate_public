-- Usage in 15-minute buckets, both sides (issues 3 and 18; Saleem, 26 Oct 2026).
--
-- Money is booked from a billing queue: every amount is an event, made at the
-- moment it is decided — the balance deduction, or for postpaid wherever the
-- amount applies — and the DB worker adds a batch of events up here. A row's
-- bucket is that moment, not the moment anything was written. Customer
-- invoices read the sell side; vendor bills read the buy side.
--
--   side        sell = what the customer pays us; buy = what we pay the vendor
--   company_id  the CUSTOMER's company (whose message it was) on both sides;
--               a vendor's own company is read from vendors.company_id
--   account_id  the customer's account (both sides)
--   vendor_id   the vendor that carried it (0 when none did, e.g. a lookup)
--   kind        mt | mo | dlr | hlr
--   model       sell: prepaid | postpaid; buy: ''
--   currency    sell: the company's currency; buy: the buying card's
--   items       messages (receipts, lookups); 0 on a parts correction
--   parts       message parts
--   amount      in `currency`; positive = owed, negative = given back
CREATE TABLE IF NOT EXISTS usage_buckets (
  bucket_start DATETIME      NOT NULL COMMENT 'UTC start of the 15-minute bucket the amount was decided in',
  side         VARCHAR(4)    NOT NULL COMMENT 'sell | buy',
  company_id   BIGINT        NOT NULL COMMENT 'the customer company',
  account_id   BIGINT        NOT NULL DEFAULT 0,
  vendor_id    BIGINT        NOT NULL DEFAULT 0,
  kind         VARCHAR(8)    NOT NULL COMMENT 'mt | mo | dlr | hlr',
  model        VARCHAR(8)    NOT NULL DEFAULT '' COMMENT 'sell: prepaid | postpaid; buy: empty',
  currency     VARCHAR(8)    NOT NULL DEFAULT '',
  items        BIGINT        NOT NULL DEFAULT 0,
  parts        BIGINT        NOT NULL DEFAULT 0,
  amount       DECIMAL(18,4) NOT NULL DEFAULT 0,
  updated_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (bucket_start, side, company_id, account_id, vendor_id, kind, model, currency),
  KEY idx_usage_buckets_company (company_id, bucket_start),
  KEY idx_usage_buckets_vendor (vendor_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- When the events began. Usage before this moment is what the billing role
-- booked per day (copied below at 00:00 of its day, sell side only); vendor
-- bills before it are filled once from the summaries by the Admin API.
CREATE TABLE IF NOT EXISTS usage_state (
  name       VARCHAR(64)  NOT NULL PRIMARY KEY,
  at         DATETIME     NOT NULL,
  note       VARCHAR(255) NULL,
  updated_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO usage_state (name, at, note)
VALUES ('events_since', UTC_TIMESTAMP(), 'usage booked from the billing queue from this moment');

-- The days booked so far, as the sell side. usage_daily is not written or
-- read again; its rows stay.
INSERT IGNORE INTO usage_buckets
  (bucket_start, side, company_id, account_id, vendor_id, kind, model, currency, items, parts, amount)
SELECT CAST(u.day AS DATETIME), 'sell', u.company_id, u.account_id, 0, u.kind, u.model,
       COALESCE(c.currency, ''), u.items, 0, u.amount
  FROM usage_daily u
  LEFT JOIN companies c ON c.id = u.company_id;
