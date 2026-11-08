-- Hadal architecture reconciliation (PO 2026-07-27, Salman + Saleem): companies
-- gains Hadal's remaining fields wholesale — "don't skip fields, we will be
-- needing them later." Purely additive; every existing row gets sane defaults.
-- base_conversion_rate/base_thirdparty_crp_id are FX columns (ties to the
-- still-deferred fx_rates proposal) — the column lands now, the feature stays
-- deferred until that proposal is built.
ALTER TABLE companies
  ADD COLUMN company_email VARCHAR(190) DEFAULT NULL AFTER support_email,
  ADD COLUMN rates_email VARCHAR(190) DEFAULT NULL AFTER company_email,
  ADD COLUMN billing_cycle_day INT NOT NULL DEFAULT 1 AFTER rates_email,
  ADD COLUMN base_conversion_rate DECIMAL(20,10) NOT NULL DEFAULT 1.0000000000 AFTER currency,
  ADD COLUMN base_thirdparty_crp_id VARCHAR(255) DEFAULT NULL AFTER base_conversion_rate,
  ADD COLUMN logo_url VARCHAR(512) DEFAULT NULL AFTER address,
  ADD COLUMN theme VARCHAR(255) DEFAULT NULL AFTER logo_url;
