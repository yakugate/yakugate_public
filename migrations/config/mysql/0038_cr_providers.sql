-- Hadal reconciliation, provider-catalog group (PO 2026-07-27, Salman:
-- "implement all"): cr_providers (currency-rate providers) — completes the
-- provider-catalog trio alongside hlr_providers/kms_providers (migration
-- 0030). Two earlier additions already pointed at this concept with nothing
-- behind it: companies.base_thirdparty_crp_id (migration 0029) and
-- rate_card_rates.thirdparty_crp_id (migration 0034) — both were landed
-- ahead of this table, same "column/table now, FX feature later" pattern
-- used throughout this reconciliation (the FX feature itself stays deferred,
-- per the still-open fx_rates proposal in CHANGELOG).
--
-- cache_ttl (how long to cache a fetched conversion rate before re-querying)
-- is a genuinely useful detail neither hlr_providers nor kms_providers
-- needed — kept as Hadal has it.
CREATE TABLE cr_providers (
  id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name          VARCHAR(128) NOT NULL,
  provider_type VARCHAR(64) NOT NULL,
  config        JSON NULL,
  cache_ttl_sec INT NOT NULL DEFAULT 0,
  is_active     TINYINT(1) NOT NULL DEFAULT 1,
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_cr_providers_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- hlr_providers gains company_id (kms_providers deliberately does NOT — his
-- own model keeps KMS global/shared even though HLR/rate providers are
-- per-company; a meaningful signal given the still-open company-ownership
-- fork on connectors/rate_cards/templates/hlr_providers). NULL = shared
-- catalog entry, today's behavior for every existing row.
ALTER TABLE hlr_providers
  ADD COLUMN company_id BIGINT UNSIGNED DEFAULT NULL AFTER provider_type,
  ADD KEY idx_hlr_providers_company (company_id);
