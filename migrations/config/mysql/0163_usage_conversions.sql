-- Every conversion kept in the usage buckets (Saleem, 27 Oct 2026: card ->
-- company -> base, fixed at booking; one bill per vendor company with every
-- currency in it). Each money event carries its conversions (billing.Conv);
-- the DB worker adds them up here.
--
--   card_currency     the card's currency (sell: the selling card's; buy: the
--                     buying card's, which is `currency` on a buy row)
--   company_currency  sell: the customer company's (= `currency`); buy: the
--                     vendor's own company's (the card's for a vendor in no
--                     company)
--   fx_rate           card -> company rate, rounded to 6 significant digits
--                     so near-identical provider fetches share a row; 0 = not
--                     known
--   card_amount       the amount in card_currency
--   company_amount    the amount in company_currency
--   base_amount       the amount in the system base currency
--   conv_missing      events of the row whose company amount was not known
--   base_missing      events of the row with no rate to the base currency
--
-- A row booked before this has '' / 0 in the new key columns and NULL
-- amounts: "before conversions were kept". Additions only, one column per
-- ALTER; the primary key gains the three conversion columns (one statement:
-- the table is never without a key).
ALTER TABLE usage_buckets ADD COLUMN card_currency VARCHAR(8) NOT NULL DEFAULT '' AFTER currency;
ALTER TABLE usage_buckets ADD COLUMN company_currency VARCHAR(8) NOT NULL DEFAULT '' AFTER card_currency;
ALTER TABLE usage_buckets ADD COLUMN fx_rate DECIMAL(24,12) NOT NULL DEFAULT 0 AFTER company_currency;
ALTER TABLE usage_buckets ADD COLUMN card_amount DECIMAL(18,4) NULL AFTER amount;
ALTER TABLE usage_buckets ADD COLUMN company_amount DECIMAL(18,4) NULL AFTER card_amount;
ALTER TABLE usage_buckets ADD COLUMN base_amount DECIMAL(18,4) NULL AFTER company_amount;
ALTER TABLE usage_buckets ADD COLUMN conv_missing BIGINT NOT NULL DEFAULT 0 AFTER base_amount;
ALTER TABLE usage_buckets ADD COLUMN base_missing BIGINT NOT NULL DEFAULT 0 AFTER conv_missing;
ALTER TABLE usage_buckets DROP PRIMARY KEY, ADD PRIMARY KEY (bucket_start, side, company_id, account_id, vendor_id, kind, model, currency, card_currency, company_currency, fx_rate);
