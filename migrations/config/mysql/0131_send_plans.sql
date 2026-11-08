-- ============================================================================
-- 0131_send_plans.sql — routing rules with Main and backups (Saleem, 2026-09-30)
--
-- A rule's "Where to send" becomes one JSON plan (internal/sendplan, mirrored
-- by p13-backend/src/services/sendPlan.js — change together):
--
--   {"levels":[{"method":"direct|cheap|best|smart|least","share":false,
--               "vendors":[{"vendor_id":35,"pct":0,
--                           "buy_rate_card_id":0,"sell_rate_card_id":0}]}, …],
--    "exceptions":{"41004":[ …levels… ]}}
--
-- levels[0] is Main, the rest Backup 1, Backup 2… A vendor sits in one level
-- only. A level may share by % (pct adds up to 100); a vendor may carry its
-- own buy and sell card for this rule (0 = the vendor's default buy card /
-- the rule's sell card). Exceptions give one network its own levels.
--
-- NULL send_plan = an old rule: vendor_id + route_type, read as a one-level
-- plan, so nothing already configured changes. New rules keep vendor_id set
-- to Main's first vendor, for the older readers of that column.
--
-- Retries per rule: a round is Main then every backup; a round in which no
-- vendor took the message sends it back to wait, and the next round starts
-- again from Main. NULL = the gateway's default (5 retries, 30 s, doubling,
-- at most 30 min).
--
-- One column per statement: the bootstrap skips a whole statement whose first
-- column already exists.
-- ============================================================================
ALTER TABLE routes ADD COLUMN send_plan JSON NULL;
ALTER TABLE routes ADD COLUMN retry_rounds INT NULL;
ALTER TABLE routes ADD COLUMN retry_first_sec INT NULL;
ALTER TABLE routes ADD COLUMN retry_backoff VARCHAR(8) NULL;
ALTER TABLE routes ADD COLUMN retry_max_wait_sec INT NULL;
