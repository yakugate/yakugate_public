-- Hadal reconciliation, rate-card group (PO 2026-07-27, Salman): "we will
-- adopt all fields, but two tables structure is fine for versioning that we
-- will be needing later" — rate_cards + rate_card_rates (normalized, immutable
-- versioning) stay as two tables rather than adopting Hadal's single JSON
-- rate_plan blob per card; everything else discussed in this round is
-- adopted as new columns.
--
-- rate_cards.type tags which traffic direction a card is priced for (his
-- cards are literally named ZONG_MT/ZONG_MO/ZONG_DLR) — matches the 3-way
-- default_mt_rate_card_id/_mo_/_dlr_ split already added to
-- sms_service_config (migration 0031). company_id is the same per-tenant
-- rate-card ownership fork already raised on `connectors` (migration 0032's
-- discussion) — added here as a nullable column (NULL = shared/global card,
-- today's behavior for every existing row) rather than forcing a ruling on
-- that fork before this migration can land.
ALTER TABLE rate_cards
  ADD COLUMN type ENUM('MT','MO','DLR') DEFAULT NULL AFTER kind,
  ADD COLUMN company_id BIGINT UNSIGNED DEFAULT NULL AFTER currency,
  ADD KEY idx_rate_cards_company (company_id);

-- Real gap (S-05, tax in the cost-disclosure response): tax_percentage,
-- costing_unit (per-segment vs per-message — today's engine always assumes
-- per-segment, price_per_segment's own name bakes that in), and the FX pair
-- (conversion_rate/thirdparty_crp_id, same deferred-feature/landed-column
-- pattern as companies.base_conversion_rate in migration 0029).
ALTER TABLE rate_card_rates
  ADD COLUMN tax_percentage DECIMAL(5,2) NOT NULL DEFAULT 0.00 AFTER price_per_segment,
  ADD COLUMN costing_unit VARCHAR(32) NOT NULL DEFAULT 'segment' AFTER tax_percentage,
  ADD COLUMN conversion_rate DECIMAL(20,10) NOT NULL DEFAULT 1.0000000000 AFTER costing_unit,
  ADD COLUMN thirdparty_crp_id VARCHAR(255) DEFAULT NULL AFTER conversion_rate;
