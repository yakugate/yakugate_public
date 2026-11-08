-- Consolidate quota/balance/retention onto companies (PO 2026-07-28): "data
-- retention policy json must be in companies along with quota, balance,
-- prepaid billing json, country wise quota." Companion migrations 0045
-- (sms_service_config trim) and 0046 (dialect cost/balance move) complete
-- the same request.
--
-- balance/low_balance_alert STAY plain DECIMAL columns (unchanged) rather
-- than folding into JSON -- this is the one actively-mutated financial field
-- in the whole schema (P1 synchronous reservation, P9 settlement, the Admin
-- API's balance-adjust endpoint, the Redis prepaid offload's atomic Lua
-- decrement all assume a plain numeric column with row-level locking /
-- native EXPIRE-style atomicity). Folding it into JSON would have made every
-- one of those sites rewrite arithmetic as JSON_SET(...,JSON_EXTRACT(...)+x),
-- a real regression in a financially-sensitive path for no asked-for benefit
-- -- "prepaid billing json" is read here as the boolean prepaid/postpaid
-- FLAG becoming JSON-shaped (room to grow later), not the money itself.
--
-- "country wise quota" gets a new company-level JSON column (country_quota)
-- as the forward-looking home, but the EXISTING quota_by_countries table
-- (still account-scoped) is left as-is -- re-plumbing its rate-limit
-- pipeline bucket key from account to company is a separate, larger change
-- not attempted in this pass. Flagged, not done.
ALTER TABLE companies
  ADD COLUMN quota JSON DEFAULT NULL AFTER max_throughput,
  ADD COLUMN data_retention JSON DEFAULT NULL AFTER quota,
  ADD COLUMN country_quota JSON DEFAULT NULL AFTER data_retention,
  ADD COLUMN billing_config JSON DEFAULT NULL AFTER prepaid_billing;

-- Backfill EXISTING companies (already linked to >=1 account today) from one
-- representative linked account's current per-account values -- moving to a
-- company-wide policy is inherently lossy when several accounts under one
-- company previously held different quota/retention settings; this is the
-- accepted tradeoff of the consolidation itself, not a migration bug.
UPDATE companies c
JOIN (
  SELECT s1.company_id, s1.quota, s1.portal_data_retention, s1.archival_data_retention
  FROM sms_service_config s1
  JOIN (SELECT company_id, MIN(id) AS min_id FROM sms_service_config
        WHERE company_id IS NOT NULL GROUP BY company_id) first
    ON first.company_id = s1.company_id AND first.min_id = s1.id
) rep ON rep.company_id = c.id
SET c.quota = rep.quota,
    c.data_retention = JSON_OBJECT('portal_days', rep.portal_data_retention,
                                    'archival_days', rep.archival_data_retention);

-- Unlinked accounts (company_id NULL) each get their OWN dedicated new
-- company, seeded from their current values, so nothing is silently lost --
-- matches the design CLAUDE.md already documents ("every account is
-- auto-backfilled into its own single-account company") but which evidently
-- never ran for every account in this environment.
INSERT INTO companies (company_id, name, prepaid_billing, balance, timezone,
                        quota, data_retention, billing_config, created_at, updated_at)
SELECT UUID(), CONCAT(s.username, '-company'), s.prepaid_billing, s.balance, s.timezone,
       s.quota, JSON_OBJECT('portal_days', s.portal_data_retention,
                             'archival_days', s.archival_data_retention),
       JSON_OBJECT('prepaid', IF(s.prepaid_billing = 1, TRUE, FALSE)),
       NOW(), NOW()
FROM sms_service_config s
WHERE s.company_id IS NULL;

UPDATE sms_service_config s
JOIN companies c ON c.name = CONCAT(s.username, '-company')
SET s.company_id = c.id
WHERE s.company_id IS NULL;

-- billing_config for the pre-existing companies (the newly INSERTed ones
-- above already got it set directly).
UPDATE companies SET billing_config = JSON_OBJECT('prepaid', IF(prepaid_billing = 1, TRUE, FALSE))
WHERE billing_config IS NULL;

-- Any company still missing quota/data_retention (one that had never had ANY
-- linked account, e.g. created standalone) gets the same defaults every
-- column already defaulted to.
UPDATE companies
   SET quota = JSON_OBJECT('daily_quota', NULL, 'daily_used', 0, 'monthly_quota', NULL, 'monthly_used', 0)
 WHERE quota IS NULL;
UPDATE companies
   SET data_retention = JSON_OBJECT('portal_days', 90, 'archival_days', 365)
 WHERE data_retention IS NULL;

ALTER TABLE companies DROP COLUMN prepaid_billing;
