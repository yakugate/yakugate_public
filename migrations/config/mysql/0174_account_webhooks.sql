-- One MO or DLR webhook may serve many accounts (Saleem, 6 Nov 2026: "one
-- dlr or mo endpoint can be assigned to many accounts at same time"). The
-- twin of migrations/config/mssql/0174_account_webhooks.sql.
--
-- The ACCOUNT now names its webhook of each kind; any number of accounts may
-- name the same http_endpoints row. Before this the row named its one
-- account (http_endpoints.account_id), and the dlr and mo roles found an
-- account's webhook through it. That column stays, read by nothing for
-- type dlr/mo (bookkeeping only, as vendor_id is for vendor calls).
--
-- Each account keeps its own push lane at the webhook's rate_limit and
-- submit_concurrency (Saleem: each account gets it, not shared).
--
-- NULL = no webhook of that kind. No foreign key: a webhook is never deleted
-- while an account names it (both doors refuse, naming the accounts), as a
-- vendor's calls are kept by its adapter_config references.
ALTER TABLE sms_service_config ADD COLUMN dlr_webhook_id BIGINT UNSIGNED NULL;
ALTER TABLE sms_service_config ADD COLUMN mo_webhook_id BIGINT UNSIGNED NULL;

-- What each account has today: the row that names it, a switched-on one
-- first, then the newest (the roles read every row and the last one read,
-- the newest, won).
--
-- Written for production (6 Nov 2026): no LIMIT (MySQL logs that as unsafe
-- for statement replication; MAX is the same answer every time), only
-- accounts still NULL are filled (safe to run again: it is also the check
-- run after a rollout, for links an older Admin API made meanwhile), and
-- updated_at is kept: nothing about the account changed. Both ADDs are a
-- NULL column at the end, which MySQL 8 adds in place without copying the
-- table.
UPDATE sms_service_config a
   SET a.dlr_webhook_id = COALESCE(
         (SELECT MAX(e.id) FROM http_endpoints e
           WHERE e.type = 'dlr' AND e.account_id = a.id AND e.is_active = 1),
         (SELECT MAX(e.id) FROM http_endpoints e
           WHERE e.type = 'dlr' AND e.account_id = a.id)),
       a.updated_at = a.updated_at
 WHERE a.dlr_webhook_id IS NULL;
UPDATE sms_service_config a
   SET a.mo_webhook_id = COALESCE(
         (SELECT MAX(e.id) FROM http_endpoints e
           WHERE e.type = 'mo' AND e.account_id = a.id AND e.is_active = 1),
         (SELECT MAX(e.id) FROM http_endpoints e
           WHERE e.type = 'mo' AND e.account_id = a.id)),
       a.updated_at = a.updated_at
 WHERE a.mo_webhook_id IS NULL;
