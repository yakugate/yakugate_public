-- ============================================================================
-- Four more shapes the Admin API used to add to the engine's own tables
-- (PO 2026-09-18, the same ruling as config/0087a).
--
-- The engine owns the config store. The Admin API had been reshaping five of
-- its tables from its own migration set, which is why a fresh database could
-- not be built by running one side and then the other: each runner kept
-- reaching a file that needed a table the other had not created yet.
-- config/0087a settled `audit_log`. These are the rest:
--
--   admin/0009  rate_card_rates.effective_from becomes DATETIME
--   admin/0010  templates.template_group_id + its index
--   admin/0011  sms_service_config: index the user, drop the customer unique
--   admin/0015  templates.account_id becomes nullable
--
-- The DATA halves of those files do NOT move here and are not needed on a new
-- database: admin/0010 backfills a company id and a group id on templates that
-- already exist, admin/0012 stamps every account's SMPP system_id from its
-- username, and admin/0015 keeps one row per template group. A table the
-- engine has just created has no rows for any of that to fix. On an existing
-- deployment those files ran years of migrations ago and are recorded as
-- applied; nothing re-runs them.
--
-- WHY THE END OF THE SET, UNLIKE 0087a. 0087a had to land before 0088, whose
-- trigger reads `actor_name`. Nothing in the engine's own set reads any of the
-- four below, so they sit at the end where a new migration normally goes.
--
-- Each statement is separate so a deployment holding some of them settles the
-- rest. An ALTER whose column or key is already there is treated as satisfied
-- by the bootstrap, so this file is a no-op wherever the Admin API did it
-- first.
-- ============================================================================

-- admin/0009 — a rate may take effect at a time of day, not just on a date.
ALTER TABLE rate_card_rates MODIFY COLUMN effective_from DATETIME NULL;

-- admin/0010 — one template authored by a company, fanned out per account:
-- the group id ties the copies together.
ALTER TABLE templates ADD COLUMN template_group_id CHAR(36) NULL AFTER company_id;
CREATE INDEX idx_templates_group ON templates (template_group_id);

-- admin/0015 — a company-level template row belongs to no single account.
ALTER TABLE templates MODIFY account_id BIGINT UNSIGNED NULL;

-- admin/0011 — a portal user may hold several accounts, so the old unique on
-- customer_id (config/0031) has to go, and the user column is worth an index.
ALTER TABLE sms_service_config ADD INDEX idx_ssc_user_id (user_id);
ALTER TABLE sms_service_config DROP INDEX uq_ssc_customer_id;
