-- ============================================================================
-- 0024_country_rate_limit.sql — per-country segments/sec rate limit (2026-07-26)
--
-- PO decision 2026-07-26: rate limits are configured per destination country
-- in the countries table (quota_by_countries — this schema's countries table,
-- renamed from country_rules in 0006). rate_limit is a SEGMENTS-per-second
-- cap shared by ALL traffic to that country (longest-prefix match, one bucket
-- per matched prefix); 0 = unlimited. Enforced by the pipeline's rate-limit
-- stage alongside the company-shared throughput cap.
-- ============================================================================

ALTER TABLE quota_by_countries
  ADD COLUMN rate_limit INT NOT NULL DEFAULT 0 AFTER quota;
